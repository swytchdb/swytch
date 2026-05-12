/*
 * Copyright 2026 Swytch Labs BV
 *
 * This file is part of Swytch.
 *
 * Swytch is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Swytch is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Swytch. If not, see <https://www.gnu.org/licenses/>.
 */

package bitmap

import (
	"math"
	"math/bits"
	"strconv"
	"strings"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

// Bitmap command implementations
// Bitmaps are stored as KEYED collections where each element ID is the
// stringified bit offset. This makes concurrent SETBITs on different bits
// commutative — they merge correctly without data loss.

// getBitmapSnapshot returns the snapshot for a bitmap key.
// Returns (snapshot, tips, exists, wrongType, error).
func getBitmapSnapshot(cmd *shared.Command, key string) (snap *pb.ReducedEffect, tips []effects.Tip, exists bool, wrongType bool, err error) {
	snap, tips, err = cmd.Context.GetSnapshot(key)
	if err != nil {
		return nil, nil, false, false, err
	}
	if snap == nil || snap.Op == pb.EffectOp_REMOVE_OP {
		return nil, tips, false, false, nil
	}
	if snap.Collection != pb.CollectionKind_KEYED && snap.Collection != pb.CollectionKind_SCALAR {
		return nil, tips, true, true, nil
	}
	return snap, tips, true, false, nil
}

// getScalarBit reads a single bit from raw SCALAR bytes.
func getScalarBit(data []byte, offset int64) int64 {
	byteIndex := offset / 8
	bitIndex := 7 - (offset % 8)
	if int64(len(data)) <= byteIndex {
		return 0
	}
	return int64((data[byteIndex] >> bitIndex) & 1)
}

// getBit reads a single bit from a bitmap snapshot.
func getBit(snap *pb.ReducedEffect, offset int64) int64 {
	if snap == nil {
		return 0
	}
	if snap.Collection == pb.CollectionKind_KEYED {
		baseBit := int64(0)
		if snap.Scalar != nil {
			baseBit = getScalarBit(snap.Scalar.GetRaw(), offset)
		}
		elem, exists := snap.NetAdds[strconv.FormatInt(offset, 10)]
		if exists {
			if snap.Scalar != nil {
				// XOR mode: IntVal=1 means toggle from base
				if elem.Data.GetIntVal() == 1 {
					return 1 - baseBit
				}
				return baseBit
			}
			// Pure KEYED: IntVal is the actual bit value
			return elem.Data.GetIntVal()
		}
		return baseBit
	}
	// Pure SCALAR bitmap
	return getScalarBit(snap.Scalar.GetRaw(), offset)
}

// reconstructBitmapBytes rebuilds a byte array from a bitmap snapshot.
// For KEYED collections with a SCALAR base, KEYED elements act as toggles
// (XOR) on the base bytes — this allows SETBIT on SCALAR strings without
// a costly bulk conversion.
func reconstructBitmapBytes(snap *pb.ReducedEffect) []byte {
	if snap == nil {
		return nil
	}
	// Pure SCALAR path
	if snap.Collection == pb.CollectionKind_SCALAR {
		return snap.Scalar.GetRaw()
	}

	// Collect overlay offsets.  Every entry in NetAdds contributes to the
	// string length (Redis SETBIT always extends), but only entries with
	// IntVal=1 actually set a bit.
	type bitEntry struct {
		offset int64
		set    bool // IntVal == 1
	}
	entries := make([]bitEntry, 0, len(snap.NetAdds))
	var maxOffset int64
	for key, elem := range snap.NetAdds {
		off, _ := strconv.ParseInt(key, 10, 64)
		if off > maxOffset {
			maxOffset = off
		}
		entries = append(entries, bitEntry{off, elem.Data.GetIntVal() == 1})
	}

	// Start from SCALAR base or empty
	var data []byte
	if snap.Scalar != nil {
		base := snap.Scalar.GetRaw()
		data = make([]byte, len(base))
		copy(data, base)
		// Extend if overlays go beyond SCALAR length
		if len(entries) > 0 {
			needed := maxOffset/8 + 1
			if needed > int64(len(data)) {
				newData := make([]byte, needed)
				copy(newData, data)
				data = newData
			}
		}
		// Apply toggles (XOR) only for IntVal=1 entries
		for _, e := range entries {
			if e.set {
				byteIdx := e.offset / 8
				bitIdx := 7 - (e.offset % 8)
				data[byteIdx] ^= 1 << uint(bitIdx)
			}
		}
		return data
	}

	// Pure KEYED (no SCALAR base)
	if len(entries) == 0 {
		return nil
	}
	data = make([]byte, maxOffset/8+1)
	for _, e := range entries {
		if e.set {
			byteIdx := e.offset / 8
			bitIdx := 7 - (e.offset % 8)
			data[byteIdx] |= 1 << uint(bitIdx)
		}
	}
	return data
}

// emitSetBit emits a single KEYED element for one bit.
// Always uses INSERT so the offset appears in NetAdds — this is critical
// because Redis SETBIT extends the string to accommodate the offset even
// when setting a bit to 0.  IntVal carries the actual bit value (0 or 1).
func emitSetBit(cmd *shared.Command, key string, offset int64, value int64) error {
	id := []byte(strconv.FormatInt(offset, 10))
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Id:         id,
			Value:      &pb.DataEffect_IntVal{IntVal: value},
		}},
	})
}

// emitBitmapBulk replaces all bits in a bitmap key with the given data.
// When oldSnap has a SCALAR base, emits toggles (XOR) relative to the base.
// Otherwise emits absolute set bits. Used by BITFIELD.
// oldSnap is the current snapshot (may be nil for new keys).
// tips should be passed for read-modify-write operations.
func emitBitmapBulk(cmd *shared.Command, key string, data []byte, oldSnap *pb.ReducedEffect, tips []effects.Tip) error {
	// Determine SCALAR base for overlay-aware emit
	var baseData []byte
	if oldSnap != nil && oldSnap.Scalar != nil {
		baseData = oldSnap.Scalar.GetRaw()
	}

	// Build set of bit offsets that need a KEYED element (toggle/set)
	needInsert := make(map[string]struct{})
	maxLen := max(len(baseData), len(data))
	for byteIdx := range maxLen {
		var dataByte, baseByte byte
		if byteIdx < len(data) {
			dataByte = data[byteIdx]
		}
		if byteIdx < len(baseData) {
			baseByte = baseData[byteIdx]
		}
		// With a SCALAR base: INSERT where bits differ from base (toggles)
		// Without a base: INSERT where bits are set (baseByte is 0)
		diff := dataByte ^ baseByte
		for bitIdx := 7; bitIdx >= 0; bitIdx-- {
			if (diff>>uint(bitIdx))&1 == 1 {
				offset := int64(byteIdx)*8 + int64(7-bitIdx)
				needInsert[strconv.FormatInt(offset, 10)] = struct{}{}
			}
		}
	}

	first := true
	emit := func(eff *pb.Effect) error {
		var t []effects.Tip
		if first {
			t = tips
			first = false
		}
		return cmd.Context.Emit(eff, t)
	}

	// REMOVE old overlay elements no longer needed
	if oldSnap != nil && oldSnap.Collection == pb.CollectionKind_KEYED {
		for id := range oldSnap.NetAdds {
			if _, keep := needInsert[id]; !keep {
				if err := emit(&pb.Effect{
					Key: []byte(key),
					Kind: &pb.Effect_Data{Data: &pb.DataEffect{
						Op:         pb.EffectOp_REMOVE_OP,
						Collection: pb.CollectionKind_KEYED,
						Id:         []byte(id),
					}},
				}); err != nil {
					return err
				}
			}
		}
	}

	// INSERT elements that differ from base
	for id := range needInsert {
		if err := emit(&pb.Effect{
			Key: []byte(key),
			Kind: &pb.Effect_Data{Data: &pb.DataEffect{
				Op:         pb.EffectOp_INSERT_OP,
				Merge:      pb.MergeRule_LAST_WRITE_WINS,
				Collection: pb.CollectionKind_KEYED,
				Id:         []byte(id),
				Value:      &pb.DataEffect_IntVal{IntVal: 1},
			}},
		}); err != nil {
			return err
		}
	}

	return nil
}

// handleSetBit implements SETBIT key offset value
// Sets or clears the bit at offset in the string value stored at key
// Returns the original bit value at that position
func handleSetBit(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("setbit")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	offset, ok := shared.ParseInt64(cmd.Args[1])
	if !ok || offset < 0 || offset >= 4*1024*1024*1024 {
		w.WriteError("ERR bit offset is not an integer or out of range")
		return
	}

	value, ok := shared.ParseInt64(cmd.Args[2])
	if !ok || (value != 0 && value != 1) {
		w.WriteError("ERR bit is not an integer or out of range")
		return
	}

	valid = true
	runner = func() {
		snap, _, _, wrongType, err := getBitmapSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		oldBit := getBit(snap, offset)

		// When the key doesn't exist and value is 0, Redis still creates
		// the key as a zero-filled string. Emit a SCALAR to establish it.
		if snap == nil && value == 0 {
			byteLen := (offset / 8) + 1
			zeroData := make([]byte, byteLen)
			if err := cmd.Context.Emit(&pb.Effect{
				Key: []byte(key),
				Kind: &pb.Effect_Data{Data: &pb.DataEffect{
					Op:         pb.EffectOp_INSERT_OP,
					Merge:      pb.MergeRule_LAST_WRITE_WINS,
					Collection: pb.CollectionKind_SCALAR,
					Value:      &pb.DataEffect_Raw{Raw: zeroData},
				}},
			}); err != nil {
				w.WriteError(err.Error())
				return
			}
			if err := cmd.Context.Emit(&pb.Effect{
				Key:  []byte(key),
				Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_BITMAP}},
			}); err != nil {
				w.WriteError(err.Error())
				return
			}
			w.WriteInteger(0)
			return
		}

		// Determine the emit value for the overlay model.
		// If there's a SCALAR base, KEYED elements are toggles (XOR):
		//   INSERT = "this bit differs from base"
		//   REMOVE = "this bit matches base"
		emitValue := value
		if snap != nil && snap.Scalar != nil {
			baseBit := getScalarBit(snap.Scalar.GetRaw(), offset)
			if value != baseBit {
				emitValue = 1 // INSERT (toggle from base)
			} else {
				emitValue = 0 // REMOVE (matches base)
			}
		}

		if err := emitSetBit(cmd, key, offset, emitValue); err != nil {
			w.WriteError(err.Error())
			return
		}

		// Tag as bitmap on first write
		if snap == nil {
			if err := cmd.Context.Emit(&pb.Effect{
				Key:  []byte(key),
				Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_BITMAP}},
			}); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteInteger(oldBit)
	}
	return
}

// handleGetBit implements GETBIT key offset
// Returns the bit value at offset in the string value stored at key
func handleGetBit(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 2 {
		w.WriteWrongNumArguments("getbit")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	offset, ok := shared.ParseInt64(cmd.Args[1])
	if !ok || offset < 0 {
		w.WriteError("ERR bit offset is not an integer or out of range")
		return
	}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getBitmapSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteInteger(0)
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		w.WriteInteger(getBit(snap, offset))
	}
	return
}

// handleBitCount implements BITCOUNT key [start end [BYTE|BIT]]
// Counts the number of set bits (1s) in a string
func handleBitCount(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("bitcount")
		return
	}

	// Validate arguments first (before checking key existence)
	// BITCOUNT key [start end [BYTE|BIT]]
	// Valid arg counts: 1 (just key), 3 (key start end), 4 (key start end BYTE|BIT)
	if len(cmd.Args) == 2 {
		w.WriteSyntaxError()
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	// Default values
	var start, end int64 = 0, -1
	useBitIndex := false
	hasRange := len(cmd.Args) >= 3

	// Parse optional range arguments before checking key existence
	if hasRange {
		var ok bool
		start, ok = shared.ParseInt64(cmd.Args[1])
		if !ok {
			w.WriteNotInteger()
			return
		}
		end, ok = shared.ParseInt64(cmd.Args[2])
		if !ok {
			w.WriteNotInteger()
			return
		}

		// Check for BYTE|BIT modifier (Redis 7.0+)
		if len(cmd.Args) >= 4 {
			modifier := strings.ToUpper(string(cmd.Args[3]))
			switch modifier {
			case "BIT":
				useBitIndex = true
			case "BYTE":
				useBitIndex = false
			default:
				w.WriteSyntaxError()
				return
			}
		}
	}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getBitmapSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteInteger(0)
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		// Fast path: no range, pure KEYED (no SCALAR base) — count set bits
		if !hasRange && snap.Collection == pb.CollectionKind_KEYED && snap.Scalar == nil {
			count := int64(0)
			for _, elem := range snap.NetAdds {
				if elem.Data.GetIntVal() == 1 {
					count++
				}
			}
			w.WriteInteger(count)
			return
		}

		// Reconstruct byte array for range queries and legacy SCALAR
		data := reconstructBitmapBytes(snap)
		dataLen := int64(len(data))

		// Apply defaults if no range was specified
		actualEnd := end
		if !hasRange {
			actualEnd = dataLen - 1
		}

		var count int64

		if useBitIndex {
			bitLen := dataLen * 8
			actualStart := start
			if actualStart < 0 {
				actualStart = bitLen + actualStart
			}
			if actualEnd < 0 {
				actualEnd = bitLen + actualEnd
			}
			if actualStart < 0 {
				actualStart = 0
			}
			if actualEnd >= bitLen {
				actualEnd = bitLen - 1
			}
			if actualStart > actualEnd || actualStart >= bitLen {
				w.WriteInteger(0)
				return
			}
			for i := actualStart; i <= actualEnd; i++ {
				byteIdx := i / 8
				bitIdx := 7 - (i % 8)
				if (data[byteIdx]>>bitIdx)&1 == 1 {
					count++
				}
			}
		} else {
			actualStart := start
			if actualStart < 0 {
				actualStart = dataLen + actualStart
			}
			if actualEnd < 0 {
				actualEnd = dataLen + actualEnd
			}
			if actualStart < 0 {
				actualStart = 0
			}
			if actualEnd >= dataLen {
				actualEnd = dataLen - 1
			}
			if actualStart > actualEnd || actualStart >= dataLen {
				w.WriteInteger(0)
				return
			}
			for i := actualStart; i <= actualEnd; i++ {
				count += int64(bits.OnesCount8(data[i]))
			}
		}

		w.WriteInteger(count)
	}
	return
}

// handleBitPos implements BITPOS key bit [start [end [BYTE|BIT]]]
// Returns the position of the first bit set to 0 or 1 in a string
func handleBitPos(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("bitpos")
		return
	}

	// Validate arguments first (before checking key existence)
	// BITPOS key bit [start [end [BYTE|BIT]]]
	// Max 5 arguments
	if len(cmd.Args) > 5 {
		w.WriteSyntaxError()
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	searchBit, ok := shared.ParseInt64(cmd.Args[1])
	if !ok || (searchBit != 0 && searchBit != 1) {
		w.WriteError("ERR bit is not an integer or out of range")
		return
	}

	// Default range values
	var start, end int64 = 0, -1
	useBitIndex := false
	hasEnd := false

	// Check for BYTE|BIT modifier FIRST (before parsing integers)
	// This ensures invalid modifiers return syntax error, not integer error
	if len(cmd.Args) >= 5 {
		modifier := strings.ToUpper(string(cmd.Args[4]))
		switch modifier {
		case "BIT":
			useBitIndex = true
		case "BYTE":
			useBitIndex = false
		default:
			w.WriteSyntaxError()
			return
		}
	}

	// Parse optional start
	if len(cmd.Args) >= 3 {
		start, ok = shared.ParseInt64(cmd.Args[2])
		if !ok {
			w.WriteNotInteger()
			return
		}
	}

	// Parse optional end
	if len(cmd.Args) >= 4 {
		end, ok = shared.ParseInt64(cmd.Args[3])
		if !ok {
			w.WriteNotInteger()
			return
		}
		hasEnd = true
	}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getBitmapSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			if searchBit == 0 {
				w.WriteInteger(0)
			} else {
				w.WriteInteger(-1)
			}
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		data := reconstructBitmapBytes(snap)
		dataLen := int64(len(data))

		if dataLen == 0 {
			if searchBit == 0 {
				w.WriteInteger(0)
			} else {
				w.WriteInteger(-1)
			}
			return
		}

		// Apply default end if not specified
		actualEnd := end
		if !hasEnd {
			actualEnd = dataLen - 1
		}

		if useBitIndex {
			// BIT indexing mode
			bitLen := dataLen * 8

			// Handle negative indices
			actualStart := start
			if actualStart < 0 {
				actualStart = bitLen + actualStart
			}
			if actualEnd < 0 {
				actualEnd = bitLen + actualEnd
			}

			// Clamp start
			if actualStart < 0 {
				actualStart = 0
			}
			if !hasEnd {
				actualEnd = bitLen - 1
			}
			if actualEnd >= bitLen {
				actualEnd = bitLen - 1
			}

			if actualStart > actualEnd || actualStart >= bitLen {
				w.WriteInteger(-1)
				return
			}

			// Search for bit
			for i := actualStart; i <= actualEnd; i++ {
				byteIdx := i / 8
				bitIdx := 7 - (i % 8)
				bit := (data[byteIdx] >> bitIdx) & 1
				if int64(bit) == searchBit {
					w.WriteInteger(i)
					return
				}
			}

			w.WriteInteger(-1)
		} else {
			// BYTE indexing mode
			// Handle negative indices
			actualStart := start
			if actualStart < 0 {
				actualStart = dataLen + actualStart
			}
			if actualEnd < 0 {
				actualEnd = dataLen + actualEnd
			}

			// Clamp start
			if actualStart < 0 {
				actualStart = 0
			}
			if !hasEnd {
				actualEnd = dataLen - 1
			}
			if actualEnd >= dataLen {
				actualEnd = dataLen - 1
			}

			if actualStart > actualEnd || actualStart >= dataLen {
				w.WriteInteger(-1)
				return
			}

			// Search for bit in byte range
			for byteIdx := actualStart; byteIdx <= actualEnd; byteIdx++ {
				b := data[byteIdx]
				for bitIdx := int64(7); bitIdx >= 0; bitIdx-- {
					bit := (b >> bitIdx) & 1
					if int64(bit) == searchBit {
						pos := byteIdx*8 + (7 - bitIdx)
						w.WriteInteger(pos)
						return
					}
				}
			}

			// Special case: searching for 0 with no end specified
			// If all bits are 1, return the first bit position after the string
			if searchBit == 0 && !hasEnd {
				w.WriteInteger(dataLen * 8)
				return
			}

			w.WriteInteger(-1)
		}
	}
	return
}

// handleBitOp implements BITOP operation destkey key [key ...]
// Performs bitwise operations between strings and stores the result
func handleBitOp(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("bitop")
		return
	}

	op := strings.ToUpper(string(cmd.Args[0]))
	destKey := string(cmd.Args[1])
	srcKeys := make([]string, len(cmd.Args)-2)
	for i := 2; i < len(cmd.Args); i++ {
		srcKeys[i-2] = string(cmd.Args[i])
	}

	// Validate operation
	switch op {
	case "AND", "OR", "XOR", "ONE":
		// These require at least 1 source key (already validated above)
	case "NOT":
		if len(srcKeys) != 1 {
			w.WriteError("ERR BITOP NOT requires one and only one key")
			return
		}
	case "DIFF":
		// DIFF requires at least 2 source keys: X and at least one Y
		if len(srcKeys) < 2 {
			w.WriteError("ERR BITOP DIFF requires at least two keys")
			return
		}
	case "DIFF1":
		// DIFF1 requires at least 2 source keys: X and at least one Y
		if len(srcKeys) < 2 {
			w.WriteError("ERR BITOP DIFF1 requires at least two keys")
			return
		}
	case "ANDOR":
		// ANDOR requires at least 2 source keys: X and at least one Y
		if len(srcKeys) < 2 {
			w.WriteError("ERR BITOP ANDOR requires at least two keys")
			return
		}
	default:
		w.WriteError("ERR syntax error")
		return
	}

	// Build keys list: destKey first, then all source keys
	keys = append([]string{destKey}, srcKeys...)

	valid = true
	runner = func() {
		// Read all source values via effects engine
		sources := make([][]byte, len(srcKeys))
		maxLen := 0
		for i, srcKey := range srcKeys {
			snap, _, _, wrongType, err := getBitmapSnapshot(cmd, srcKey)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if wrongType {
				w.WriteWrongType()
				return
			}
			srcData := reconstructBitmapBytes(snap)
			sources[i] = srcData
			if len(srcData) > maxLen {
				maxLen = len(srcData)
			}
		}

		// If all sources are empty/missing, result is empty
		if maxLen == 0 {
			w.WriteInteger(0)
			return
		}

		// Create result buffer
		result := make([]byte, maxLen)

		// Perform bitwise operation
		switch op {
		case "AND":
			// Initialize with all 1s, then AND each source
			for i := range result {
				result[i] = 0xFF
			}
			for _, src := range sources {
				for i := 0; i < maxLen; i++ {
					if i < len(src) {
						result[i] &= src[i]
					} else {
						result[i] = 0 // Missing bytes are treated as 0x00
					}
				}
			}

		case "OR":
			// Initialize with 0s, then OR each source
			for _, src := range sources {
				for i := range src {
					result[i] |= src[i]
				}
			}

		case "XOR":
			// Initialize with 0s, then XOR each source
			for _, src := range sources {
				for i := range src {
					result[i] ^= src[i]
				}
			}

		case "NOT":
			src := sources[0]
			if src == nil {
				src = []byte{}
			}
			for i := 0; i < len(src); i++ {
				result[i] = ^src[i]
			}
			// NOT on empty string produces empty result
			if len(src) == 0 {
				result = []byte{}
			}

		case "ONE":
			// A bit is set if it's set in exactly one source key
			for i := 0; i < maxLen; i++ {
				count := byte(0)
				for _, src := range sources {
					if i < len(src) {
						count ^= src[i] // XOR counts odd occurrences per bit
					}
				}
				// For "exactly one", we need to check each bit individually
				// XOR gives us bits that appear an odd number of times
				// But we need exactly one, so we also need to exclude bits appearing 3+ times
				// For most cases with 1-2 sources, XOR is correct
				// For 3+ sources, we need more careful counting
				if len(sources) <= 2 {
					result[i] = count
				} else {
					// Count bits that appear in exactly one source
					for bit := range 8 {
						bitCount := 0
						for _, src := range sources {
							if i < len(src) && (src[i]>>bit)&1 == 1 {
								bitCount++
							}
						}
						if bitCount == 1 {
							result[i] |= (1 << bit)
						}
					}
				}
			}

		case "DIFF":
			// A bit is set if it's in X (first key) but not in any of Y1, Y2, ...
			if len(sources) == 0 {
				break
			}
			x := sources[0]
			if x == nil {
				x = []byte{}
			}
			copy(result, x)
			// Remove bits that appear in any Y key
			for j := 1; j < len(sources); j++ {
				y := sources[j]
				if y == nil {
					continue
				}
				for i := 0; i < len(y) && i < len(result); i++ {
					result[i] &^= y[i] // AND NOT: clear bits that are set in y
				}
			}

		case "DIFF1":
			// A bit is set if it's in one or more of Y1, Y2, ... but not in X
			if len(sources) == 0 {
				break
			}
			x := sources[0]
			if x == nil {
				x = []byte{}
			}
			// First, OR all Y keys together
			yUnion := make([]byte, maxLen)
			for j := 1; j < len(sources); j++ {
				y := sources[j]
				if y == nil {
					continue
				}
				for i := range y {
					yUnion[i] |= y[i]
				}
			}
			// Then remove bits that are in X
			for i := 0; i < maxLen; i++ {
				if i < len(x) {
					result[i] = yUnion[i] &^ x[i]
				} else {
					result[i] = yUnion[i]
				}
			}

		case "ANDOR":
			// A bit is set if it's in X and also in one or more of Y1, Y2, ...
			if len(sources) == 0 {
				break
			}
			x := sources[0]
			if x == nil {
				x = []byte{}
			}
			// First, OR all Y keys together
			yUnion := make([]byte, maxLen)
			for j := 1; j < len(sources); j++ {
				y := sources[j]
				if y == nil {
					continue
				}
				for i := range y {
					yUnion[i] |= y[i]
				}
			}
			// Then AND with X
			for i := 0; i < len(x); i++ {
				result[i] = x[i] & yUnion[i]
			}
		}

		// Store result as a SCALAR string (like Redis does).
		// This preserves exact byte length including trailing zero bytes.
		destSnap, destTips, _, _, _ := getBitmapSnapshot(cmd, destKey)
		var tips []effects.Tip
		if destTips != nil {
			tips = destTips
		}
		if err := cmd.Context.Emit(&pb.Effect{
			Key: []byte(destKey),
			Kind: &pb.Effect_Data{Data: &pb.DataEffect{
				Op:         pb.EffectOp_INSERT_OP,
				Merge:      pb.MergeRule_LAST_WRITE_WINS,
				Collection: pb.CollectionKind_SCALAR,
				Value:      &pb.DataEffect_Raw{Raw: result},
			}},
		}, tips); err != nil {
			w.WriteError(err.Error())
			return
		}

		// Tag as string if dest was a different type or new
		if destSnap == nil || (destSnap.TypeTag != pb.ValueType_TYPE_STRING && destSnap.TypeTag != pb.ValueType_TYPE_UNSPECIFIED) {
			if err := cmd.Context.Emit(&pb.Effect{
				Key:  []byte(destKey),
				Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_STRING}},
			}); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteInteger(int64(len(result)))
	}
	return
}

// overflowBehavior defines how BITFIELD handles integer overflow
type overflowBehavior int

const (
	overflowWrap overflowBehavior = iota // Default: wraparound
	overflowSat                          // Saturate at min/max
	overflowFail                         // Return nil and don't modify
)

// handleBitField implements BITFIELD key [GET type offset] [SET type offset value]
//
//	[INCRBY type offset increment] [OVERFLOW WRAP|SAT|FAIL]
//
// Performs arbitrary bitfield integer operations on strings
func handleBitField(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("bitfield")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	// Parse subcommands
	type subCommand struct {
		op        string // GET, SET, INCRBY
		signed    bool
		bits      int
		offset    int64
		value     int64 // For SET and INCRBY
		overflow  overflowBehavior
		offsetMul bool // true if offset was specified with #
	}

	var subCmds []subCommand
	currentOverflow := overflowWrap

	i := 1
	for i < len(cmd.Args) {
		op := strings.ToUpper(string(cmd.Args[i]))

		switch op {
		case "GET":
			if i+2 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			signed, bitWidth, ok := parseBitfieldType(cmd.Args[i+1])
			if !ok {
				w.WriteError("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
				return
			}
			offset, offsetMul, ok := parseBitfieldOffset(cmd.Args[i+2], bitWidth)
			if !ok {
				w.WriteError("ERR bit offset is not an integer or out of range")
				return
			}
			subCmds = append(subCmds, subCommand{
				op:        "GET",
				signed:    signed,
				bits:      bitWidth,
				offset:    offset,
				overflow:  currentOverflow,
				offsetMul: offsetMul,
			})
			i += 3

		case "SET":
			if i+3 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			signed, bitWidth, ok := parseBitfieldType(cmd.Args[i+1])
			if !ok {
				w.WriteError("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
				return
			}
			offset, offsetMul, ok := parseBitfieldOffset(cmd.Args[i+2], bitWidth)
			if !ok {
				w.WriteError("ERR bit offset is not an integer or out of range")
				return
			}
			value, ok := shared.ParseInt64(cmd.Args[i+3])
			if !ok {
				w.WriteNotInteger()
				return
			}
			subCmds = append(subCmds, subCommand{
				op:        "SET",
				signed:    signed,
				bits:      bitWidth,
				offset:    offset,
				value:     value,
				overflow:  currentOverflow,
				offsetMul: offsetMul,
			})
			i += 4

		case "INCRBY":
			if i+3 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			signed, bitWidth, ok := parseBitfieldType(cmd.Args[i+1])
			if !ok {
				w.WriteError("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
				return
			}
			offset, offsetMul, ok := parseBitfieldOffset(cmd.Args[i+2], bitWidth)
			if !ok {
				w.WriteError("ERR bit offset is not an integer or out of range")
				return
			}
			increment, ok := shared.ParseInt64(cmd.Args[i+3])
			if !ok {
				w.WriteNotInteger()
				return
			}
			subCmds = append(subCmds, subCommand{
				op:        "INCRBY",
				signed:    signed,
				bits:      bitWidth,
				offset:    offset,
				value:     increment,
				overflow:  currentOverflow,
				offsetMul: offsetMul,
			})
			i += 4

		case "OVERFLOW":
			if i+1 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			behavior := strings.ToUpper(string(cmd.Args[i+1]))
			switch behavior {
			case "WRAP":
				currentOverflow = overflowWrap
			case "SAT":
				currentOverflow = overflowSat
			case "FAIL":
				currentOverflow = overflowFail
			default:
				w.WriteError("ERR Invalid OVERFLOW type (should be one of WRAP, SAT, FAIL)")
				return
			}
			i += 2

		default:
			w.WriteSyntaxError()
			return
		}
	}

	if len(subCmds) == 0 {
		// No operations, return empty array
		valid = true
		runner = func() {
			w.WriteArray(0)
		}
		return
	}

	valid = true
	runner = func() {
		lock := cmd.Runtime.GetLock(key)
		lock.Lock()
		defer lock.Unlock()

		snap, tips, _, wrongType, err := getBitmapSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}
		rawData := reconstructBitmapBytes(snap)
		// Copy data for mutation
		data := append([]byte{}, rawData...)

		// Execute subcommands and collect results
		results := make([]any, len(subCmds))
		modified := false

		for idx, sc := range subCmds {
			switch sc.op {
			case "GET":
				value := getBitfieldInt(data, sc.offset, sc.bits, sc.signed)
				results[idx] = value

			case "SET":
				oldValue := getBitfieldInt(data, sc.offset, sc.bits, sc.signed)
				newValue := sc.value

				// Check for overflow
				if checkBitfieldOverflow(newValue, sc.bits, sc.signed) && sc.overflow == overflowFail {
					results[idx] = nil
					continue
				}

				// Apply overflow handling (WRAP or SAT)
				newValue = applyOverflow(newValue, sc.bits, sc.signed, sc.overflow)

				// Ensure data is large enough
				requiredBytes := (sc.offset + int64(sc.bits) + 7) / 8
				if int64(len(data)) < requiredBytes {
					newData := make([]byte, requiredBytes)
					copy(newData, data)
					data = newData
				}

				setBitfieldInt(data, sc.offset, sc.bits, newValue)
				results[idx] = oldValue
				modified = true

			case "INCRBY":
				oldValue := getBitfieldInt(data, sc.offset, sc.bits, sc.signed)

				// Check for arithmetic overflow before adding
				if sc.overflow == overflowFail && checkAdditionOverflow(oldValue, sc.value) {
					results[idx] = nil
					continue
				}

				newValue := oldValue + sc.value

				// Check for type overflow
				if checkBitfieldOverflow(newValue, sc.bits, sc.signed) && sc.overflow == overflowFail {
					results[idx] = nil
					continue
				}

				newValue = applyOverflow(newValue, sc.bits, sc.signed, sc.overflow)

				// Ensure data is large enough
				requiredBytes := (sc.offset + int64(sc.bits) + 7) / 8
				if int64(len(data)) < requiredBytes {
					newData := make([]byte, requiredBytes)
					copy(newData, data)
					data = newData
				}

				setBitfieldInt(data, sc.offset, sc.bits, newValue)
				results[idx] = newValue
				modified = true
			}
		}

		// Store back if modified — emit changed bits as KEYED elements
		if modified {
			if err := emitBitmapBulk(cmd, key, data, snap, tips); err != nil {
				w.WriteError(err.Error())
				return
			}
			// Tag as bitmap on first write or when converting from SCALAR
			if snap == nil || snap.Collection == pb.CollectionKind_SCALAR {
				if err := cmd.Context.Emit(&pb.Effect{
					Key:  []byte(key),
					Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_BITMAP}},
				}); err != nil {
					w.WriteError(err.Error())
					return
				}
			}
		}

		// Write results array
		w.WriteArray(len(results))
		for _, r := range results {
			if r == nil {
				w.WriteNullBulkString()
			} else {
				w.WriteInteger(r.(int64))
			}
		}
	}
	return
}

// handleBitFieldRO implements BITFIELD_RO key [GET encoding offset ...]
// Read-only variant of BITFIELD that only supports GET subcommands
func handleBitFieldRO(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("bitfield_ro")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	// Parse subcommands - only GET is allowed
	type getCommand struct {
		signed bool
		bits   int
		offset int64
	}

	var getCmds []getCommand

	i := 1
	for i < len(cmd.Args) {
		op := strings.ToUpper(string(cmd.Args[i]))

		switch op {
		case "GET":
			if i+2 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			signed, bitWidth, ok := parseBitfieldType(cmd.Args[i+1])
			if !ok {
				w.WriteError("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")
				return
			}
			offset, _, ok := parseBitfieldOffset(cmd.Args[i+2], bitWidth)
			if !ok {
				w.WriteError("ERR bit offset is not an integer or out of range")
				return
			}
			getCmds = append(getCmds, getCommand{
				signed: signed,
				bits:   bitWidth,
				offset: offset,
			})
			i += 3

		default:
			// BITFIELD_RO only supports GET
			w.WriteError("ERR BITFIELD_RO only supports the GET subcommand")
			return
		}
	}

	if len(getCmds) == 0 {
		valid = true
		runner = func() {
			w.WriteArray(0)
		}
		return
	}

	valid = true
	runner = func() {
		snap, _, _, wrongType, err := getBitmapSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}
		data := reconstructBitmapBytes(snap)

		// Execute GET subcommands and collect results
		w.WriteArray(len(getCmds))
		for _, gc := range getCmds {
			value := getBitfieldInt(data, gc.offset, gc.bits, gc.signed)
			w.WriteInteger(value)
		}
	}
	return
}

// parseBitfieldType parses a bitfield type like "i8", "u16", etc.
// Returns (signed, bits, ok)
func parseBitfieldType(b []byte) (bool, int, bool) {
	if len(b) < 2 {
		return false, 0, false
	}

	signed := false
	switch b[0] {
	case 'i', 'I':
		signed = true
	case 'u', 'U':
		signed = false
	default:
		return false, 0, false
	}

	bits, err := strconv.Atoi(string(b[1:]))
	if err != nil || bits < 1 || bits > 64 {
		return false, 0, false
	}

	// u64 is not supported (would need uint64 handling)
	if !signed && bits == 64 {
		return false, 0, false
	}

	return signed, bits, true
}

// parseBitfieldOffset parses a bitfield offset
// Can be a plain number or #N (multiply by type width)
func parseBitfieldOffset(b []byte, bitWidth int) (int64, bool, bool) {
	if len(b) == 0 {
		return 0, false, false
	}

	if b[0] == '#' {
		// Multiply by type width
		idx, ok := shared.ParseInt64(b[1:])
		if !ok || idx < 0 {
			return 0, false, false
		}
		return idx * int64(bitWidth), true, true
	}

	offset, ok := shared.ParseInt64(b)
	if !ok || offset < 0 {
		return 0, false, false
	}
	return offset, false, true
}

// getBitfieldInt reads an integer from a bitfield
func getBitfieldInt(data []byte, bitOffset int64, bits int, signed bool) int64 {
	if len(data) == 0 {
		return 0
	}

	var value int64
	for i := range bits {
		bitPos := bitOffset + int64(i)
		byteIdx := bitPos / 8
		bitIdx := 7 - (bitPos % 8)

		if byteIdx < int64(len(data)) {
			bit := (data[byteIdx] >> bitIdx) & 1
			value = (value << 1) | int64(bit)
		} else {
			value = value << 1 // Pad with zeros
		}
	}

	// Sign extend if signed
	if signed && bits < 64 {
		signBit := int64(1) << (bits - 1)
		if value&signBit != 0 {
			// Negative number, sign extend
			value = value - (int64(1) << bits)
		}
	}

	return value
}

// setBitfieldInt writes an integer to a bitfield
func setBitfieldInt(data []byte, bitOffset int64, bits int, value int64) {
	// Mask to the appropriate number of bits
	var mask int64
	if bits == 64 {
		mask = -1 // All bits set
	} else {
		mask = (int64(1) << bits) - 1
	}
	value &= mask

	// Write bits from MSB to LSB
	for i := range bits {
		bitPos := bitOffset + int64(i)
		byteIdx := bitPos / 8
		bitIdx := 7 - (bitPos % 8)

		if byteIdx >= int64(len(data)) {
			continue
		}

		// Get the bit value (from MSB of value)
		bitValue := (value >> (bits - 1 - i)) & 1

		if bitValue == 1 {
			data[byteIdx] |= (1 << bitIdx)
		} else {
			data[byteIdx] &^= (1 << bitIdx)
		}
	}
}

// checkBitfieldOverflow checks if a value overflows the given bit width
func checkBitfieldOverflow(value int64, bits int, signed bool) bool {
	if signed {
		if bits == 64 {
			return false // i64 can hold any int64 value
		}
		minVal := -(int64(1) << (bits - 1))
		maxVal := (int64(1) << (bits - 1)) - 1
		return value < minVal || value > maxVal
	}
	// unsigned
	maxVal := (int64(1) << bits) - 1
	return value < 0 || value > maxVal
}

// checkAdditionOverflow checks if a + b overflows int64
// Returns true if overflow occurs
func checkAdditionOverflow(a, b int64) bool {
	if b > 0 && a > math.MaxInt64-b {
		return true // positive overflow
	}
	if b < 0 && a < math.MinInt64-b {
		return true // negative overflow
	}
	return false
}

// applyOverflow applies overflow behavior to a value
func applyOverflow(value int64, bits int, signed bool, behavior overflowBehavior) int64 {
	var minVal, maxVal int64

	// Handle i64 specially - shifting by 64 bits overflows to 0
	if signed && bits == 64 {
		minVal = math.MinInt64
		maxVal = math.MaxInt64
	} else if signed {
		minVal = -(int64(1) << (bits - 1))
		maxVal = (int64(1) << (bits - 1)) - 1
	} else {
		minVal = 0
		maxVal = (int64(1) << bits) - 1
	}

	switch behavior {
	case overflowWrap:
		// Wraparound
		if signed {
			// For i64, the value already wraps naturally in Go's int64
			if bits == 64 {
				// No wrapping needed - int64 arithmetic already wraps
				return value
			}
			// Signed wraparound for smaller bit widths
			range_ := int64(1) << bits
			value = ((value - minVal) % range_)
			if value < 0 {
				value += range_
			}
			value += minVal
		} else {
			// Unsigned wraparound
			value = value & maxVal
		}
	case overflowSat:
		// Saturate at boundaries
		if value < minVal {
			value = minVal
		} else if value > maxVal {
			value = maxVal
		}
	case overflowFail:
		// Caller handles this - just clamp for safety
		if value < minVal {
			value = minVal
		} else if value > maxVal {
			value = maxVal
		}
	}

	return value
}
