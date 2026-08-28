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

package stream

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
	"strconv"
	"time"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/swytch/redis/shared"
)

// Stream command implementations

// handleXAdd handles XADD key [NOMKSTREAM] [KEEPREF|DELREF|ACKED] [MAXLEN|MINID [=|~] threshold [LIMIT count]] [IDMP producer iid | IDMPAUTO producer] *|id field value [field value ...]
func handleXAdd(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("xadd")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	i := 1

	// Parse options
	var noMkStream bool
	var trimOpts shared.TrimOptions
	var idmpProducer string
	var idmpIID string
	var idmpAuto bool
	var hasIDMP bool

	for i < len(cmd.Args) {
		opt := shared.ToUpper(cmd.Args[i])
		switch string(opt) {
		case "NOMKSTREAM":
			noMkStream = true
			i++
		case "KEEPREF":
			trimOpts.RefMode = shared.RefModeKeepRef
			i++
		case "DELREF":
			trimOpts.RefMode = shared.RefModeDelRef
			i++
		case "ACKED":
			trimOpts.RefMode = shared.RefModeAcked
			i++
		case "IDMP":
			if hasIDMP {
				w.WriteError("ERR IDMP/IDMPAUTO specified multiple times")
				return
			}
			hasIDMP = true
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			idmpProducer = string(cmd.Args[i])
			if idmpProducer == "" {
				w.WriteError("ERR IDMP requires a non-empty producer ID")
				return
			}
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			idmpIID = string(cmd.Args[i])
			if idmpIID == "" {
				w.WriteError("ERR IDMP requires a non-empty idempotent ID")
				return
			}
			i++
		case "IDMPAUTO":
			if hasIDMP {
				w.WriteError("ERR IDMP/IDMPAUTO specified multiple times")
				return
			}
			hasIDMP = true
			idmpAuto = true
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			idmpProducer = string(cmd.Args[i])
			if idmpProducer == "" {
				w.WriteError("ERR IDMPAUTO requires a non-empty producer ID")
				return
			}
			i++
		case "MAXLEN":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			// Check for = or ~
			next := string(cmd.Args[i])
			switch next {
			case "=":
				i++
			case "~":
				trimOpts.Approx = true
				i++
			}
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			maxLen, err := strconv.ParseInt(string(cmd.Args[i]), 10, 64)
			if err != nil || maxLen < 0 {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			trimOpts.MaxLen = maxLen
			trimOpts.HasMaxLen = true
			i++
			// Check for LIMIT
			if i < len(cmd.Args) && string(shared.ToUpper(cmd.Args[i])) == "LIMIT" {
				i++
				if i >= len(cmd.Args) {
					w.WriteError("ERR syntax error")
					return
				}
				limit, err := strconv.ParseInt(string(cmd.Args[i]), 10, 64)
				if err != nil || limit < 0 {
					w.WriteError("ERR value is not an integer or out of range")
					return
				}
				trimOpts.Limit = limit
				trimOpts.HasExplicitLimit = true
				i++
			}
		case "MINID":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			// Check for = or ~
			next := string(cmd.Args[i])
			switch next {
			case "=":
				i++
			case "~":
				trimOpts.Approx = true
				i++
			}
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			minID, _, _, err := shared.ParseStreamID(string(cmd.Args[i]))
			if err != nil {
				w.WriteError("ERR Invalid stream ID specified as stream command argument")
				return
			}
			trimOpts.MinID = &minID
			i++
			// Check for LIMIT
			if i < len(cmd.Args) && string(shared.ToUpper(cmd.Args[i])) == "LIMIT" {
				i++
				if i >= len(cmd.Args) {
					w.WriteError("ERR syntax error")
					return
				}
				limit, err := strconv.ParseInt(string(cmd.Args[i]), 10, 64)
				if err != nil || limit < 0 {
					w.WriteError("ERR value is not an integer or out of range")
					return
				}
				trimOpts.Limit = limit
				trimOpts.HasExplicitLimit = true
				i++
			}
		default:
			goto parseID
		}
	}

parseID:
	if i >= len(cmd.Args) {
		w.WriteError("ERR syntax error")
		return
	}

	// Parse ID
	idStr := string(cmd.Args[i])
	i++

	// Validate IDMP requires auto-generated IDs
	if hasIDMP && idStr != "*" {
		// First check if it's a valid stream ID at all
		_, _, _, parseErr := shared.ParseStreamID(idStr)
		if parseErr != nil {
			w.WriteError("ERR Invalid stream ID specified as stream command argument")
			return
		}
		w.WriteError("ERR IDMP/IDMPAUTO can be used only with auto-generated IDs")
		return
	}

	// Check for fields (must be even number remaining)
	remaining := cmd.Args[i:]
	if len(remaining) == 0 || len(remaining)%2 != 0 {
		w.WriteWrongNumArguments("xadd")
		return
	}

	// For IDMPAUTO, compute iid from field-value pairs hash (order-independent)
	if idmpAuto {
		// Collect field-value pairs and sort by field name for order independence
		type fvPair struct{ field, value []byte }
		pairs := make([]fvPair, 0, len(remaining)/2)
		for j := 0; j < len(remaining)-1; j += 2 {
			pairs = append(pairs, fvPair{remaining[j], remaining[j+1]})
		}
		sort.Slice(pairs, func(a, b int) bool {
			return string(pairs[a].field) < string(pairs[b].field)
		})
		h := sha256.New()
		for _, p := range pairs {
			h.Write(p.field)
			h.Write([]byte{0})
			h.Write(p.value)
			h.Write([]byte{0})
		}
		idmpIID = hex.EncodeToString(h.Sum(nil))
	}

	// Copy fields
	fields := make([][]byte, len(remaining))
	for j, f := range remaining {
		fields[j] = make([]byte, len(f))
		copy(fields[j], f)
	}

	valid = true
	runner = func() {
		snap, tips, exists, wrongType, err := getStreamSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if exists && wrongType {
			w.WriteWrongType()
			return
		}

		// NOMKSTREAM: don't create if doesn't exist
		if noMkStream && !exists {
			w.WriteNullBulkString()
			return
		}

		// Read IDMP config once (before any transaction) to avoid stale reads
		var idmpGen int64
		var idmpDuration, idmpMaxsize int64
		if hasIDMP {
			idmpGen = getIdmpGeneration(cmd, key)
			var durationExplicit bool
			idmpDuration, idmpMaxsize, durationExplicit = getStreamConfig(cmd, key)
			if !durationExplicit {
				idmpDuration = 0 // don't expire with default duration
			}
		}

		// Producer dedup check: if this iid was already recorded, return the existing ID.
		// Only check if the stream exists — if it was DELeted, stale mappings must be ignored.
		if hasIDMP && idmpIID != "" && exists {
			if existingID, found := lookupProducerSeqWithConfig(cmd, key, idmpProducer, idmpIID, idmpGen, idmpDuration); found {
				incrementIidsDuplicates(cmd, key)
				w.WriteBulkStringStr(existingID.String())
				return
			}
		}

		// Get current lastID from metadata or entries
		lastID, _, _, _ := streamMetadata(snap)
		if lastID.IsZero() && exists {
			entries := snapshotToEntries(snap)
			lastID = streamLastID(entries)
		}

		// Generate or parse ID
		var id shared.StreamID
		if idStr == "*" {
			id = generateStreamID(snap)
			if id.IsZero() {
				w.WriteError("ERR The ID specified in XADD is equal or smaller than the target stream top item")
				return
			}
		} else {
			var isSpecial bool
			id, _, isSpecial, err = shared.ParseStreamID(idStr)
			if err != nil {
				w.WriteError("ERR Invalid stream ID specified as stream command argument")
				return
			}
			if isSpecial {
				if len(idStr) > 2 && idStr[len(idStr)-2:] == "-*" {
					id, err = generateStreamIDWithMs(snap, id.Ms)
					if err != nil {
						w.WriteError("ERR The ID specified in XADD is equal or smaller than the target stream top item")
						return
					}
				} else {
					w.WriteError("ERR Invalid stream ID specified as stream command argument")
					return
				}
			}
		}

		// Validate ID > LastID
		if id.Compare(lastID) <= 0 {
			w.WriteError("ERR The ID specified in XADD is equal or smaller than the target stream top item")
			return
		}
		if id.IsZero() {
			w.WriteError("ERR The ID specified in XADD must be greater than 0-0")
			return
		}

		// Begin transaction for read-modify-write
		cmd.Context.BeginTx()

		// Noop with tips for dependency tracking
		if len(tips) > 0 {
			if err := cmd.Context.Emit(&pb.Effect{
				Key:  []byte(key),
				Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
			}, tips); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		// Emit the entry insert (ORDERED, PLACE_TAIL)
		if err := emitStreamInsert(cmd, key, id, fields); err != nil {
			w.WriteError(err.Error())
			return
		}

		// Increment entries-added counter
		if err := incrementStreamEntriesAdded(cmd, key); err != nil {
			w.WriteError(err.Error())
			return
		}

		// Type tag on first write
		if !exists {
			if err := emitStreamTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		// Record producer dedup mapping (in same transaction)
		if hasIDMP && idmpIID != "" {
			if err := emitProducerSeqWithConfig(cmd, key, idmpProducer, idmpIID, id, idmpGen, idmpMaxsize); err != nil {
				w.WriteError(err.Error())
				return
			}
			if err := incrementIidsAdded(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		// Handle trimming
		if trimOpts.HasMaxLen || trimOpts.MinID != nil {
			trimOpts.NodeSize = shared.GetStreamNodeMaxEntries()
			entries := snapshotToEntries(snap)
			// Add our new entry to the list for trim calculations
			entries = append(entries, &shared.StreamEntry{ID: id, Fields: fields})

			trimEntries(cmd, key, entries, trimOpts, snap)
		}

		w.WriteBulkStringStr(id.String())
	}
	return
}

// trimEntries performs stream trimming by emitting REMOVE effects.
func trimEntries(cmd *shared.Command, key string, entries []*shared.StreamEntry, opts shared.TrimOptions, snap *pb.ReducedEffect) {
	if len(entries) == 0 {
		return
	}

	// Determine effective limit
	limit := opts.Limit
	if limit == 0 {
		if opts.Approx && opts.NodeSize > 0 {
			limit = 100 * opts.NodeSize
		} else {
			limit = int64(len(entries))
		}
	}

	// For ACKED mode, we need consumer group info
	canTrim := func(id shared.StreamID) bool {
		if opts.RefMode != shared.RefModeAcked {
			return true
		}
		// Check all groups
		groupNames := listGroupNames(cmd, key)
		for _, gName := range groupNames {
			gSnap, _, _ := getGroupSnapshot(cmd, key, gName)
			lastDelivered, _, _, _ := getGroupMeta(gSnap)
			if id.Compare(lastDelivered) > 0 {
				return false
			}
			pelSnap, _, _ := getPELSnapshot(cmd, key, gName)
			if pelSnap != nil && pelSnap.NetAdds != nil {
				elemID := string(encodeStreamElementID(id))
				if _, pending := pelSnap.NetAdds[elemID]; pending {
					return false
				}
			}
		}
		return true
	}

	// For DELREF mode, also remove PEL entries for trimmed entries
	handleTrim := func(id shared.StreamID) {
		if opts.RefMode != shared.RefModeDelRef {
			return
		}
		groupNames := listGroupNames(cmd, key)
		for _, gName := range groupNames {
			pelSnap, _, _ := getPELSnapshot(cmd, key, gName)
			if pelSnap != nil && pelSnap.NetAdds != nil {
				elemID := string(encodeStreamElementID(id))
				if _, pending := pelSnap.NetAdds[elemID]; pending {
					emitPELRemove(cmd, key, gName, id)
				}
			}
		}
	}

	trimCount := int64(0)

	// Trim by MINID
	if opts.MinID != nil {
		for _, entry := range entries {
			if trimCount >= limit {
				break
			}
			if entry.ID.Compare(*opts.MinID) >= 0 {
				break
			}
			if !canTrim(entry.ID) {
				continue
			}
			emitStreamRemove(cmd, key, entry.ID)
			handleTrim(entry.ID)
			trimCount++
		}
	}

	// Trim by MAXLEN
	if opts.HasMaxLen {
		effectiveMaxLen := opts.MaxLen
		if opts.Approx && opts.NodeSize > 0 {
			effectiveMaxLen = ((opts.MaxLen + opts.NodeSize - 1) / opts.NodeSize) * opts.NodeSize
		}

		currentLen := int64(len(entries)) - trimCount
		toTrim := currentLen - effectiveMaxLen
		if toTrim > 0 {
			if toTrim > limit-trimCount {
				toTrim = limit - trimCount
			}
			if opts.Approx && opts.HasExplicitLimit && opts.NodeSize > 0 {
				toTrim = (toTrim / opts.NodeSize) * opts.NodeSize
			}
		}

		if toTrim > 0 {
			trimmedHere := int64(0)
			for _, entry := range entries {
				if trimmedHere >= toTrim {
					break
				}
				if !canTrim(entry.ID) {
					continue
				}
				// Check if already trimmed by MINID
				if opts.MinID != nil && entry.ID.Compare(*opts.MinID) < 0 {
					continue // already counted
				}
				emitStreamRemove(cmd, key, entry.ID)
				handleTrim(entry.ID)
				trimmedHere++
			}
		}
	}
}

// handleXLen handles XLEN key
func handleXLen(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("xlen")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getStreamSnapshot(cmd, key)
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

		w.WriteInteger(int64(streamLen(snap)))
	}
	return
}

// handleXRange handles XRANGE key start end [COUNT count]
func handleXRange(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("xrange")
		return
	}

	key := string(cmd.Args[0])
	startStr := string(cmd.Args[1])
	endStr := string(cmd.Args[2])
	keys = []string{key}

	// Parse COUNT option
	count := 0
	if len(cmd.Args) >= 5 {
		if string(shared.ToUpper(cmd.Args[3])) == "COUNT" {
			c, err := strconv.Atoi(string(cmd.Args[4]))
			if err != nil || c < 0 {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			count = c
		} else {
			w.WriteError("ERR syntax error")
			return
		}
	}

	// Parse start and end IDs
	start, startExcl, err := shared.ParseStreamIDForRange(startStr, false)
	if err != nil {
		w.WriteError("ERR Invalid stream ID specified as stream command argument")
		return
	}

	end, endExcl, err := shared.ParseStreamIDForRange(endStr, true)
	if err != nil {
		w.WriteError("ERR Invalid stream ID specified as stream command argument")
		return
	}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getStreamSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteArray(0)
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		entries := snapshotToEntries(snap)
		result := rangeEntries(entries, start, end, startExcl, endExcl, count)
		writeStreamEntries(w, result)
	}
	return
}

// handleXRevRange handles XREVRANGE key end start [COUNT count]
func handleXRevRange(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("xrevrange")
		return
	}

	key := string(cmd.Args[0])
	// Note: XREVRANGE has end before start
	endStr := string(cmd.Args[1])
	startStr := string(cmd.Args[2])
	keys = []string{key}

	// Parse COUNT option
	count := 0
	if len(cmd.Args) >= 5 {
		if string(shared.ToUpper(cmd.Args[3])) == "COUNT" {
			c, err := strconv.Atoi(string(cmd.Args[4]))
			if err != nil || c < 0 {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			count = c
		} else {
			w.WriteError("ERR syntax error")
			return
		}
	}

	// Parse start and end IDs
	start, startExcl, err := shared.ParseStreamIDForRange(startStr, false)
	if err != nil {
		w.WriteError("ERR Invalid stream ID specified as stream command argument")
		return
	}

	end, endExcl, err := shared.ParseStreamIDForRange(endStr, true)
	if err != nil {
		w.WriteError("ERR Invalid stream ID specified as stream command argument")
		return
	}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getStreamSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteArray(0)
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		entries := snapshotToEntries(snap)
		result := rangeEntriesReverse(entries, start, end, startExcl, endExcl, count)
		writeStreamEntries(w, result)
	}
	return
}

// handleXRead handles XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
func handleXRead(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
	// In script context, use non-blocking behavior (same as transactions)
	if conn.InScript {
		handleXReadNonBlocking(cmd, w, db)
		return
	}

	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("xread")
		return
	}

	var count int
	var block float64 = -1 // -1 means no blocking
	i := 0

	// Parse options
	for i < len(cmd.Args) {
		opt := shared.ToUpper(cmd.Args[i])
		switch string(opt) {
		case "COUNT":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			c, err := strconv.Atoi(string(cmd.Args[i]))
			if err != nil || c < 0 {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			count = c
			i++
		case "BLOCK":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			b, err := strconv.ParseFloat(string(cmd.Args[i]), 64)
			if err != nil || b < 0 {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			block = b / 1000.0 // Convert ms to seconds
			i++
		case "STREAMS":
			i++
			goto parseStreams
		case "CLAIM":
			w.WriteError("ERR The CLAIM option is only supported when using the GROUP option")
			return
		default:
			w.WriteError("ERR syntax error")
			return
		}
	}

	w.WriteError("ERR syntax error")
	return

parseStreams:
	// Remaining args are: key [key ...] id [id ...]
	remaining := cmd.Args[i:]
	if len(remaining) < 2 || len(remaining)%2 != 0 {
		w.WriteError("ERR Unbalanced 'xread' list of streams: for each stream key an ID, '+', or '$' must be specified.")
		return
	}

	numStreams := len(remaining) / 2
	streamKeys := make([]string, numStreams)
	ids := make([]shared.StreamID, numStreams)
	useLastID := make([]bool, numStreams)
	useLastEntry := make([]bool, numStreams)

	for j := range numStreams {
		streamKeys[j] = string(remaining[j])
		idStr := string(remaining[numStreams+j])

		switch idStr {
		case "$":
			useLastID[j] = true
		case "+":
			useLastEntry[j] = true
		default:
			id, _, isSpecial, err := shared.ParseStreamID(idStr)
			if err != nil {
				w.WriteError("ERR Invalid stream ID specified as stream command argument")
				return
			}
			if isSpecial && idStr != "-" {
				w.WriteError("ERR Invalid stream ID specified as stream command argument")
				return
			}
			ids[j] = id
		}
	}

	// Try to read streams
	tryReadStreams := func() ([]streamResult, error) {
		var results []streamResult

		for j, sKey := range streamKeys {
			snap, _, exists, wrongType, err := getStreamSnapshot(cmd, sKey)
			if err != nil {
				return nil, err
			}
			if !exists || wrongType {
				continue
			}

			entries := snapshotToEntries(snap)

			// Handle + special ID: return the last entry
			if useLastEntry[j] {
				if len(entries) > 0 {
					results = append(results, streamResult{key: sKey, entries: entries[len(entries)-1:]})
				}
				continue
			}

			var startID shared.StreamID
			if useLastID[j] {
				lastID, _, _, found := streamMetadata(snap)
				if !found {
					lastID = streamLastID(entries)
				}
				startID = lastID
			} else {
				startID = ids[j]
			}

			// Get entries after startID
			filtered := rangeEntries(entries, startID, shared.MaxStreamID, true, false, count)
			if len(filtered) > 0 {
				results = append(results, streamResult{key: sKey, entries: filtered})
			}
		}

		return results, nil
	}

	// Non-blocking case
	if block < 0 {
		results, err := tryReadStreams()
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if len(results) == 0 {
			w.WriteNullArray()
			return
		}
		writeXReadResults(w, results)
		return
	}

	// Blocking case
	// First resolve $ IDs to current LastID
	for j, sKey := range streamKeys {
		if useLastID[j] {
			snap, _, exists, _, err := getStreamSnapshot(cmd, sKey)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if exists {
				lastID, _, _, found := streamMetadata(snap)
				if !found {
					entries := snapshotToEntries(snap)
					lastID = streamLastID(entries)
				}
				ids[j] = lastID
			}
			useLastID[j] = false
		}
	}

	// Setup blocking context
	ctx, cleanup := conn.Block(block)
	defer cleanup()

	// Register for notifications on all keys
	keyBytes := make([][]byte, len(streamKeys))
	for j, k := range streamKeys {
		keyBytes[j] = []byte(k)
	}
	reg, _ := db.Manager().Subscriptions.Register(ctx, keyBytes...)
	defer reg.Cancel()

	// Blocking loop
	for {
		results, err := tryReadStreams()
		if err != nil {
			reg.Cancel()
			w.WriteError(err.Error())
			return
		}
		if len(results) > 0 {
			reg.Cancel()
			writeXReadResults(w, results)
			return
		}

		// Wait for notification or timeout
		if _, err := reg.Wait(); err != nil {
			if conn.WasUnblockedWithError() {
				w.WriteError("UNBLOCKED client unblocked via CLIENT UNBLOCK")
				return
			}
			w.WriteNullArray()
			return
		}
	}
}

// handleXReadNonBlocking is the transaction-safe variant of XREAD
func handleXReadNonBlocking(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("xread")
		return
	}

	var count int
	i := 0

	// Parse options
	for i < len(cmd.Args) {
		opt := shared.ToUpper(cmd.Args[i])
		switch string(opt) {
		case "COUNT":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			c, err := strconv.Atoi(string(cmd.Args[i]))
			if err != nil || c < 0 {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			count = c
			i++
		case "BLOCK":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			// Parse but ignore in non-blocking mode
			_, err := strconv.ParseFloat(string(cmd.Args[i]), 64)
			if err != nil {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			i++
		case "STREAMS":
			i++
			goto parseNBStreams
		case "CLAIM":
			w.WriteError("ERR The CLAIM option is only supported when using the GROUP option")
			return
		default:
			w.WriteError("ERR syntax error")
			return
		}
	}

	w.WriteError("ERR syntax error")
	return

parseNBStreams:
	remaining := cmd.Args[i:]
	if len(remaining) < 2 || len(remaining)%2 != 0 {
		w.WriteError("ERR Unbalanced 'xread' list of streams: for each stream key an ID, '+', or '$' must be specified.")
		return
	}

	numStreams := len(remaining) / 2
	streamKeys := make([]string, numStreams)
	ids := make([]shared.StreamID, numStreams)
	useLastID := make([]bool, numStreams)
	useLastEntry := make([]bool, numStreams)

	for j := range numStreams {
		streamKeys[j] = string(remaining[j])
		idStr := string(remaining[numStreams+j])

		switch idStr {
		case "$":
			useLastID[j] = true
		case "+":
			useLastEntry[j] = true
		default:
			id, _, isSpecial, err := shared.ParseStreamID(idStr)
			if err != nil {
				w.WriteError("ERR Invalid stream ID specified as stream command argument")
				return
			}
			if isSpecial && idStr != "-" {
				w.WriteError("ERR Invalid stream ID specified as stream command argument")
				return
			}
			ids[j] = id
		}
	}

	keys = streamKeys
	valid = true
	runner = func() {
		var results []streamResult

		for j, sKey := range streamKeys {
			snap, _, exists, wrongType, err := getStreamSnapshot(cmd, sKey)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if !exists || wrongType {
				continue
			}

			entries := snapshotToEntries(snap)

			if useLastEntry[j] {
				if len(entries) > 0 {
					results = append(results, streamResult{key: sKey, entries: entries[len(entries)-1:]})
				}
				continue
			}

			var startID shared.StreamID
			if useLastID[j] {
				lastID, _, _, found := streamMetadata(snap)
				if !found {
					lastID = streamLastID(entries)
				}
				startID = lastID
			} else {
				startID = ids[j]
			}

			filtered := rangeEntries(entries, startID, shared.MaxStreamID, true, false, count)
			if len(filtered) > 0 {
				results = append(results, streamResult{key: sKey, entries: filtered})
			}
		}

		if len(results) == 0 {
			w.WriteNullArray()
			return
		}
		writeXReadResults(w, results)
	}
	return
}

// streamResult holds entries for a single stream in XREAD results
type streamResult struct {
	key     string
	entries []*shared.StreamEntry
}

// streamResultWithClaim includes additional claim metadata
type streamResultWithClaim struct {
	key     string
	entries []claimEntry
}

// claimEntry represents an entry with claim metadata
type claimEntry struct {
	entry         *shared.StreamEntry
	idleTime      int64 // milliseconds since last delivery
	deliveryCount int64
}

// writeStreamEntries writes an array of stream entries
func writeStreamEntries(w *shared.Writer, entries []*shared.StreamEntry) {
	w.WriteArray(len(entries))
	for _, entry := range entries {
		w.WriteArray(2)
		w.WriteBulkStringStr(entry.ID.String())
		w.WriteArray(len(entry.Fields))
		for _, f := range entry.Fields {
			w.WriteBulkString(f)
		}
	}
}

// writeStreamEntriesWithClaim writes an array of stream entries with claim metadata
func writeStreamEntriesWithClaim(w *shared.Writer, entries []claimEntry) {
	w.WriteArray(len(entries))
	for _, ce := range entries {
		w.WriteArray(4)
		w.WriteBulkStringStr(ce.entry.ID.String())
		w.WriteArray(len(ce.entry.Fields))
		for _, f := range ce.entry.Fields {
			w.WriteBulkString(f)
		}
		w.WriteInteger(ce.idleTime)
		w.WriteInteger(ce.deliveryCount)
	}
}

// writeXReadResults writes XREAD results
func writeXReadResults(w *shared.Writer, results []streamResult) {
	w.WriteArray(len(results))
	for _, r := range results {
		w.WriteArray(2)
		w.WriteBulkStringStr(r.key)
		writeStreamEntries(w, r.entries)
	}
}

// writeXReadGroupHistoryResults writes XREADGROUP history results.
// For history queries (non->), each stream is always included in the output,
// with an empty entry list if there are no pending entries.
func writeXReadGroupHistoryResults(w *shared.Writer, results []streamResult, streamKeys []string) {
	// Build a map of results by key
	resultMap := make(map[string]*streamResult)
	for i := range results {
		resultMap[results[i].key] = &results[i]
	}

	w.WriteArray(len(streamKeys))
	for _, sk := range streamKeys {
		w.WriteArray(2)
		w.WriteBulkStringStr(sk)
		if r, ok := resultMap[sk]; ok && len(r.entries) > 0 {
			writeStreamEntries(w, r.entries)
		} else {
			w.WriteArray(0)
		}
	}
}

// writeXReadResultsWithClaim writes XREAD results with claim metadata
func writeXReadResultsWithClaim(w *shared.Writer, results []streamResultWithClaim) {
	w.WriteArray(len(results))
	for _, r := range results {
		w.WriteArray(2)
		w.WriteBulkStringStr(r.key)
		writeStreamEntriesWithClaim(w, r.entries)
	}
}

// handleXDel handles XDEL key id [id ...]
func handleXDel(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("xdel")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	// Parse all IDs to delete
	ids := make([]shared.StreamID, 0, len(cmd.Args)-1)
	for i := 1; i < len(cmd.Args); i++ {
		id, _, isSpecial, err := shared.ParseStreamID(string(cmd.Args[i]))
		if err != nil || isSpecial {
			w.WriteError("ERR Invalid stream ID specified as stream command argument")
			return
		}
		ids = append(ids, id)
	}

	valid = true
	runner = func() {
		snap, tips, exists, wrongType, err := getStreamSnapshot(cmd, key)
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

		// Check which IDs exist
		entries := snapshotToEntries(snap)
		entrySet := make(map[string]bool, len(entries))
		for _, e := range entries {
			entrySet[string(encodeStreamElementID(e.ID))] = true
		}

		deleted := int64(0)
		var toDelete []shared.StreamID
		for _, id := range ids {
			elemID := string(encodeStreamElementID(id))
			if entrySet[elemID] {
				toDelete = append(toDelete, id)
				deleted++
			}
		}

		if deleted == 0 {
			w.WriteInteger(0)
			return
		}

		cmd.Context.BeginTx()
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			w.WriteError(err.Error())
			return
		}

		for _, id := range toDelete {
			if err := emitStreamRemove(cmd, key, id); err != nil {
				w.WriteError(err.Error())
				return
			}
			// Emit a second remove so the ID lands in NetRemoves (the first
			// remove pulls it from OrderedElements; the second, finding it
			// absent, records it as a tombstone for MaxDeletedID tracking).
			if err := emitStreamRemove(cmd, key, id); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteInteger(deleted)
	}
	return
}

// handleXInfo handles XINFO subcommands
func handleXInfo(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteError("ERR wrong number of arguments for 'xinfo' command")
		return
	}

	subCmd := string(shared.ToUpper(cmd.Args[0]))

	if subCmd == "STREAM" || subCmd == "GROUPS" || subCmd == "CONSUMERS" {
		if len(cmd.Args) < 2 {
			w.WriteError("ERR wrong number of arguments for 'xinfo|" + string(cmd.Args[0]) + "' command")
			return
		}
		keys = []string{string(cmd.Args[1])}
	}

	valid = true
	runner = func() {
		switch subCmd {
		case "STREAM":
			handleXInfoStream(cmd, w, db)
		case "GROUPS":
			handleXInfoGroups(cmd, w, db)
		case "CONSUMERS":
			handleXInfoConsumers(cmd, w, db)
		case "HELP":
			if len(cmd.Args) > 1 {
				w.WriteError("ERR wrong number of arguments for 'xinfo|help' command")
				return
			}
			handleXInfoHelp(w)
		default:
			w.WriteError("ERR unknown subcommand '" + string(cmd.Args[0]) + "'. Try XINFO HELP.")
		}
	}
	return
}

// handleXInfoStream handles XINFO STREAM key [FULL [COUNT count]]
func handleXInfoStream(cmd *shared.Command, w *shared.Writer, db *shared.Database) {
	if len(cmd.Args) < 2 {
		w.WriteError("ERR wrong number of arguments for 'xinfo|stream' command")
		return
	}

	key := string(cmd.Args[1])
	full := false
	count := int64(10)

	for i := 2; i < len(cmd.Args); i++ {
		opt := string(shared.ToUpper(cmd.Args[i]))
		switch opt {
		case "FULL":
			full = true
		case "COUNT":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			c, err := strconv.ParseInt(string(cmd.Args[i]), 10, 64)
			if err != nil || c < 0 {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			count = c
		default:
			w.WriteError("ERR syntax error")
			return
		}
	}

	snap, _, exists, wrongType, err := getStreamSnapshot(cmd, key)
	if err != nil {
		w.WriteError(err.Error())
		return
	}
	if !exists {
		w.WriteError("ERR no such key")
		return
	}
	if wrongType {
		w.WriteWrongType()
		return
	}

	entries := snapshotToEntries(snap)
	lastID, entriesAdded, maxDeletedID, _ := streamMetadata(snap)

	// Override entries-added from cfg key if available
	if cfgEA := getStreamEntriesAdded(cmd, key); cfgEA >= 0 {
		entriesAdded = cfgEA
	}
	if lastID.IsZero() {
		lastID = streamLastID(entries)
	}

	firstID := streamFirstID(entries)

	// Get group count
	groupNames := listGroupNames(cmd, key)
	groupCount := len(groupNames)

	// Get producer count and total IID count (filtered by current generation)
	producerNames := listProducerNames(cmd, key)
	currentGen := getIdmpGeneration(cmd, key)
	producerCount := 0
	iidsTracked := 0
	for _, pName := range producerNames {
		snap, _, err := cmd.Context.GetSnapshot(producerKey(key, pName))
		if err == nil && snap != nil && snap.NetAdds != nil {
			count := 0
			for _, elem := range snap.NetAdds {
				r := elem.Data.Decompress()
				if len(r) >= 32 {
					entryGen := int64(binary.LittleEndian.Uint64(r[24:32]))
					if entryGen == currentGen {
						count++
					}
				} else if len(r) >= 16 {
					// Old format entries (no generation) — only count if gen is 0
					if currentGen == 0 {
						count++
					}
				}
			}
			if count > 0 {
				producerCount++
			}
			iidsTracked += count
		}
	}

	// Get IDMP config and lifetime counters
	idmpDuration, idmpMaxsize, _ := getStreamConfig(cmd, key)
	iidsAdded := getIidsAdded(cmd, key)
	iidsDuplicates := getIidsDuplicates(cmd, key)

	if full {
		writeXInfoStreamFull(w, entries, lastID, entriesAdded, maxDeletedID, firstID, groupCount, producerCount, iidsTracked, iidsAdded, iidsDuplicates, idmpDuration, idmpMaxsize, count, cmd, key, groupNames)
	} else {
		writeXInfoStreamBasic(w, entries, lastID, entriesAdded, maxDeletedID, firstID, groupCount, producerCount, iidsTracked, iidsAdded, iidsDuplicates, idmpDuration, idmpMaxsize)
	}
}

func writeXInfoStreamBasic(w *shared.Writer, entries []*shared.StreamEntry, lastID shared.StreamID, entriesAdded int64, maxDeletedID, firstID shared.StreamID, groupCount int, producerCount int, iidsTracked int, iidsAdded int64, iidsDuplicates int64, idmpDuration int64, idmpMaxsize int64) {
	w.WriteArray(32)

	w.WriteBulkStringStr("length")
	w.WriteInteger(int64(len(entries)))

	w.WriteBulkStringStr("radix-tree-keys")
	w.WriteInteger(int64(len(entries) / 4))

	w.WriteBulkStringStr("radix-tree-nodes")
	w.WriteInteger(2)

	w.WriteBulkStringStr("last-generated-id")
	w.WriteBulkStringStr(lastID.String())

	w.WriteBulkStringStr("max-deleted-entry-id")
	w.WriteBulkStringStr(maxDeletedID.String())

	w.WriteBulkStringStr("entries-added")
	w.WriteInteger(entriesAdded)

	w.WriteBulkStringStr("recorded-first-entry-id")
	w.WriteBulkStringStr(firstID.String())

	w.WriteBulkStringStr("groups")
	w.WriteInteger(int64(groupCount))

	w.WriteBulkStringStr("pids-tracked")
	w.WriteInteger(int64(producerCount))

	w.WriteBulkStringStr("iids-tracked")
	w.WriteInteger(int64(iidsTracked))

	w.WriteBulkStringStr("iids-added")
	w.WriteInteger(iidsAdded)

	w.WriteBulkStringStr("iids-duplicates")
	w.WriteInteger(iidsDuplicates)

	w.WriteBulkStringStr("idmp-duration")
	w.WriteInteger(idmpDuration)

	w.WriteBulkStringStr("idmp-maxsize")
	w.WriteInteger(idmpMaxsize)

	w.WriteBulkStringStr("first-entry")
	if len(entries) > 0 {
		writeStreamEntry(w, entries[0])
	} else {
		w.WriteNullArray()
	}

	w.WriteBulkStringStr("last-entry")
	if len(entries) > 0 {
		writeStreamEntry(w, entries[len(entries)-1])
	} else {
		w.WriteNullArray()
	}
}

func writeXInfoStreamFull(w *shared.Writer, entries []*shared.StreamEntry, lastID shared.StreamID, entriesAdded int64, maxDeletedID, firstID shared.StreamID, groupCount int, producerCount int, iidsTracked int, iidsAdded int64, iidsDuplicates int64, idmpDuration int64, idmpMaxsize int64, count int64, cmd *shared.Command, key string, groupNames []string) {
	w.WriteArray(30)

	w.WriteBulkStringStr("length")
	w.WriteInteger(int64(len(entries)))

	w.WriteBulkStringStr("radix-tree-keys")
	w.WriteInteger(int64(len(entries) / 4))

	w.WriteBulkStringStr("radix-tree-nodes")
	w.WriteInteger(2)

	w.WriteBulkStringStr("last-generated-id")
	w.WriteBulkStringStr(lastID.String())

	w.WriteBulkStringStr("max-deleted-entry-id")
	w.WriteBulkStringStr(maxDeletedID.String())

	w.WriteBulkStringStr("entries-added")
	w.WriteInteger(entriesAdded)

	w.WriteBulkStringStr("recorded-first-entry-id")
	w.WriteBulkStringStr(firstID.String())

	w.WriteBulkStringStr("entries")
	numEntries := int64(len(entries))
	if count > 0 && count < numEntries {
		numEntries = count
	}
	w.WriteArray(int(numEntries))
	for i := int64(0); i < numEntries && i < int64(len(entries)); i++ {
		writeStreamEntry(w, entries[i])
	}

	w.WriteBulkStringStr("pids-tracked")
	w.WriteInteger(int64(producerCount))

	w.WriteBulkStringStr("iids-tracked")
	w.WriteInteger(int64(iidsTracked))

	w.WriteBulkStringStr("iids-added")
	w.WriteInteger(iidsAdded)

	w.WriteBulkStringStr("iids-duplicates")
	w.WriteInteger(iidsDuplicates)

	w.WriteBulkStringStr("idmp-duration")
	w.WriteInteger(idmpDuration)

	w.WriteBulkStringStr("idmp-maxsize")
	w.WriteInteger(idmpMaxsize)

	w.WriteBulkStringStr("groups")
	w.WriteArray(len(groupNames))
	for _, name := range groupNames {
		writeXInfoGroupFull(w, cmd, key, name, entriesAdded, int64(len(entries)), count)
	}
}

func writeXInfoGroupFull(w *shared.Writer, cmd *shared.Command, key, groupName string, entriesAdded int64, streamLength int64, count int64) {
	gSnap, _, _ := getGroupSnapshot(cmd, key, groupName)
	lastDelivered, entriesRead, _, _ := getGroupMeta(gSnap)

	pelSnap, _, _ := getPELSnapshot(cmd, key, groupName)
	consumers := getConsumersFromGroup(gSnap)

	w.WriteArray(14)

	w.WriteBulkStringStr("name")
	w.WriteBulkStringStr(groupName)

	w.WriteBulkStringStr("last-delivered-id")
	w.WriteBulkStringStr(lastDelivered.String())

	w.WriteBulkStringStr("entries-read")
	if entriesRead >= 0 {
		w.WriteInteger(entriesRead)
	} else {
		w.WriteNullBulkString()
	}

	w.WriteBulkStringStr("lag")
	if entriesRead >= 0 {
		lag := max(entriesAdded-entriesRead, 0)
		w.WriteInteger(lag)
	} else {
		w.WriteInteger(streamLength)
	}

	pelEntries := pelToEntries(pelSnap)
	w.WriteBulkStringStr("pel-count")
	w.WriteInteger(int64(len(pelEntries)))

	w.WriteBulkStringStr("pending")
	displayPEL := pelEntries
	if count > 0 && int64(len(displayPEL)) > count {
		displayPEL = displayPEL[:count]
	}
	w.WriteArray(len(displayPEL))
	for _, pe := range displayPEL {
		w.WriteArray(4)
		w.WriteBulkStringStr(pe.ID.String())
		w.WriteBulkStringStr(pe.Consumer)
		w.WriteInteger(pe.DeliveryTime)
		w.WriteInteger(pe.DeliveryCount)
	}

	w.WriteBulkStringStr("consumers")
	if len(consumers) == 0 {
		w.WriteArray(0)
	} else {
		consumerNames := make([]string, 0, len(consumers))
		for name := range consumers {
			consumerNames = append(consumerNames, name)
		}
		sort.Strings(consumerNames)
		w.WriteArray(len(consumerNames))
		for _, name := range consumerNames {
			writeXInfoConsumerFull(w, name, pelEntries, consumers, count)
		}
	}
}

func writeXInfoConsumerFull(w *shared.Writer, name string, pelEntries []*shared.PendingEntry, consumers map[string]*pb.ReducedElement, count int64) {
	w.WriteArray(10)

	w.WriteBulkStringStr("name")
	w.WriteBulkStringStr(name)

	seenTime, activeTime := int64(0), int64(0)
	if elem, ok := consumers[name]; ok {
		seenTime, activeTime = decodeConsumerMeta(elem.Data.Decompress())
	}

	w.WriteBulkStringStr("seen-time")
	w.WriteInteger(seenTime)

	w.WriteBulkStringStr("active-time")
	w.WriteInteger(activeTime)

	// Count pending for this consumer
	var pending int64
	var consumerPEL []*shared.PendingEntry
	for _, pe := range pelEntries {
		if pe.Consumer == name {
			pending++
			consumerPEL = append(consumerPEL, pe)
		}
	}

	w.WriteBulkStringStr("pel-count")
	w.WriteInteger(pending)

	displayPEL := consumerPEL
	if count > 0 && int64(len(displayPEL)) > count {
		displayPEL = displayPEL[:count]
	}
	w.WriteBulkStringStr("pending")
	w.WriteArray(len(displayPEL))
	for _, pe := range displayPEL {
		w.WriteArray(3)
		w.WriteBulkStringStr(pe.ID.String())
		w.WriteInteger(pe.DeliveryTime)
		w.WriteInteger(pe.DeliveryCount)
	}
}

// writeStreamEntry writes a single stream entry as [id, [field, value, ...]]
func writeStreamEntry(w *shared.Writer, entry *shared.StreamEntry) {
	w.WriteArray(2)
	w.WriteBulkStringStr(entry.ID.String())
	w.WriteArray(len(entry.Fields))
	for _, f := range entry.Fields {
		w.WriteBulkString(f)
	}
}

// handleXInfoGroups handles XINFO GROUPS key
func handleXInfoGroups(cmd *shared.Command, w *shared.Writer, db *shared.Database) {
	if len(cmd.Args) < 2 {
		w.WriteError("ERR wrong number of arguments for 'xinfo|groups' command")
		return
	}

	key := string(cmd.Args[1])

	snap, _, exists, wrongType, err := getStreamSnapshot(cmd, key)
	if err != nil {
		w.WriteError(err.Error())
		return
	}
	if !exists {
		w.WriteError("ERR no such key")
		return
	}
	if wrongType {
		w.WriteWrongType()
		return
	}

	entries := snapshotToEntries(snap)
	_, entriesAdded, _, _ := streamMetadata(snap)
	if cfgEA := getStreamEntriesAdded(cmd, key); cfgEA >= 0 {
		entriesAdded = cfgEA
	}

	groupNames := listGroupNames(cmd, key)
	if len(groupNames) == 0 {
		w.WriteArray(0)
		return
	}

	w.WriteArray(len(groupNames))
	for _, name := range groupNames {
		gSnap, _, _ := getGroupSnapshot(cmd, key, name)
		lastDelivered, entriesRead, _, _ := getGroupMeta(gSnap)
		pelSnap, _, _ := getPELSnapshot(cmd, key, name)
		consumers := getConsumersFromGroup(gSnap)

		pelCount := 0
		if pelSnap != nil && pelSnap.NetAdds != nil {
			pelCount = len(pelSnap.NetAdds)
		}
		consumerCount := len(consumers)

		w.WriteArray(12)

		w.WriteBulkStringStr("name")
		w.WriteBulkStringStr(name)

		w.WriteBulkStringStr("consumers")
		w.WriteInteger(int64(consumerCount))

		w.WriteBulkStringStr("pending")
		w.WriteInteger(int64(pelCount))

		w.WriteBulkStringStr("last-delivered-id")
		w.WriteBulkStringStr(lastDelivered.String())

		w.WriteBulkStringStr("entries-read")
		if entriesRead >= 0 {
			w.WriteInteger(entriesRead)
		} else {
			w.WriteNullBulkString()
		}

		w.WriteBulkStringStr("lag")
		if entriesRead >= 0 {
			lag := max(entriesAdded-entriesRead, 0)
			w.WriteInteger(lag)
		} else {
			w.WriteInteger(int64(len(entries)))
		}
	}
}

// handleXInfoConsumers handles XINFO CONSUMERS key group
func handleXInfoConsumers(cmd *shared.Command, w *shared.Writer, db *shared.Database) {
	if len(cmd.Args) < 3 {
		w.WriteError("ERR wrong number of arguments for 'xinfo|consumers' command")
		return
	}

	key := string(cmd.Args[1])
	groupName := string(cmd.Args[2])

	_, _, exists, wrongType, err := getStreamSnapshot(cmd, key)
	if err != nil {
		w.WriteError(err.Error())
		return
	}
	if !exists {
		w.WriteError("ERR no such key")
		return
	}
	if wrongType {
		w.WriteWrongType()
		return
	}

	if !groupExists(cmd, key, groupName) {
		w.WriteError("NOGROUP No such consumer group '" + groupName + "' for key name '" + key + "'")
		return
	}

	gSnap, _, _ := getGroupSnapshot(cmd, key, groupName)
	consumers := getConsumersFromGroup(gSnap)
	pelSnap, _, _ := getPELSnapshot(cmd, key, groupName)

	if len(consumers) == 0 {
		w.WriteArray(0)
		return
	}

	now := time.Now().UnixMilli()

	consumerNames := make([]string, 0, len(consumers))
	for name := range consumers {
		consumerNames = append(consumerNames, name)
	}
	sort.Strings(consumerNames)

	w.WriteArray(len(consumerNames))
	for _, name := range consumerNames {
		elem := consumers[name]
		seenTime, activeTime := decodeConsumerMeta(elem.Data.Decompress())
		pendingCount := consumerPendingCount(pelSnap, name)

		w.WriteArray(8)

		w.WriteBulkStringStr("name")
		w.WriteBulkStringStr(name)

		w.WriteBulkStringStr("pending")
		w.WriteInteger(pendingCount)

		w.WriteBulkStringStr("idle")
		idle := max(now-seenTime, 0)
		w.WriteInteger(idle)

		w.WriteBulkStringStr("inactive")
		if activeTime == 0 {
			w.WriteInteger(-1)
		} else {
			w.WriteInteger(max(now-activeTime, 0))
		}
	}
}

// handleXInfoHelp handles XINFO HELP
func handleXInfoHelp(w *shared.Writer) {
	help := []string{
		"XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
		"CONSUMERS <key> <groupname>",
		"    Show consumers of <groupname>.",
		"GROUPS <key>",
		"    Show the stream consumer groups.",
		"STREAM <key> [FULL [COUNT <count>]]",
		"    Show information about the stream.",
		"HELP",
		"    Print this help.",
	}
	w.WriteArray(len(help))
	for _, line := range help {
		w.WriteBulkStringStr(line)
	}
}

// handleXTrim handles XTRIM key <MAXLEN|MINID> [=|~] threshold [LIMIT count] [KEEPREF|DELREF|ACKED]
func handleXTrim(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("xtrim")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	i := 1

	var trimOpts shared.TrimOptions
	hasExplicitLimit := false

	strategy := string(shared.ToUpper(cmd.Args[i]))
	switch strategy {
	case "MAXLEN":
		i++
		if i >= len(cmd.Args) {
			w.WriteError("ERR syntax error")
			return
		}
		next := string(cmd.Args[i])
		switch next {
		case "=":
			i++
		case "~":
			trimOpts.Approx = true
			i++
		}
		if i >= len(cmd.Args) {
			w.WriteError("ERR syntax error")
			return
		}
		maxLen, err := strconv.ParseInt(string(cmd.Args[i]), 10, 64)
		if err != nil || maxLen < 0 {
			w.WriteError("ERR value is not an integer or out of range")
			return
		}
		trimOpts.MaxLen = maxLen
		trimOpts.HasMaxLen = true
		i++
	case "MINID":
		i++
		if i >= len(cmd.Args) {
			w.WriteError("ERR syntax error")
			return
		}
		next := string(cmd.Args[i])
		switch next {
		case "=":
			i++
		case "~":
			trimOpts.Approx = true
			i++
		}
		if i >= len(cmd.Args) {
			w.WriteError("ERR syntax error")
			return
		}
		minID, _, _, err := shared.ParseStreamID(string(cmd.Args[i]))
		if err != nil {
			w.WriteError("ERR Invalid stream ID specified as stream command argument")
			return
		}
		trimOpts.MinID = &minID
		i++
	default:
		w.WriteError("ERR syntax error, MAXLEN or MINID must be specified")
		return
	}

	if i < len(cmd.Args) && string(shared.ToUpper(cmd.Args[i])) == "LIMIT" {
		i++
		if i >= len(cmd.Args) {
			w.WriteError("ERR syntax error")
			return
		}
		if !trimOpts.Approx {
			w.WriteError("ERR syntax error, LIMIT cannot be used without the special ~ option")
			return
		}
		limit, err := strconv.ParseInt(string(cmd.Args[i]), 10, 64)
		if err != nil || limit < 0 {
			w.WriteError("ERR value is not an integer or out of range")
			return
		}
		trimOpts.Limit = limit
		hasExplicitLimit = true
		i++
	}

	if i < len(cmd.Args) {
		opt := string(shared.ToUpper(cmd.Args[i]))
		switch opt {
		case "KEEPREF":
			trimOpts.RefMode = shared.RefModeKeepRef
			i++
		case "DELREF":
			trimOpts.RefMode = shared.RefModeDelRef
			i++
		case "ACKED":
			trimOpts.RefMode = shared.RefModeAcked
			i++
		default:
			w.WriteError("ERR syntax error")
			return
		}
	}

	if i < len(cmd.Args) {
		w.WriteError("ERR syntax error")
		return
	}

	trimOpts.HasExplicitLimit = hasExplicitLimit

	valid = true
	runner = func() {
		snap, tips, exists, wrongType, err := getStreamSnapshot(cmd, key)
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

		entries := snapshotToEntries(snap)
		beforeLen := len(entries)

		trimOpts.NodeSize = shared.GetStreamNodeMaxEntries()

		cmd.Context.BeginTx()
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			w.WriteError(err.Error())
			return
		}

		// Use StreamValue.Trim for accurate trimming logic
		sv := entriesToStreamValue(entries, snap)

		// Populate consumer groups for ACKED/DELREF ref modes
		if trimOpts.RefMode == shared.RefModeAcked || trimOpts.RefMode == shared.RefModeDelRef {
			populateStreamGroups(cmd, key, sv)
		}

		trimmed := sv.Trim(trimOpts)

		// Emit removes for trimmed entries
		if trimmed > 0 {
			// Identify which entries were trimmed
			remainingMap := make(map[string]bool)
			sv.ForEach(func(entry *shared.StreamEntry) bool {
				remainingMap[entry.ID.Key()] = true
				return true
			})

			for _, entry := range entries {
				if !remainingMap[entry.ID.Key()] {
					emitStreamRemove(cmd, key, entry.ID)
				}
			}

			// For DELREF: emit PEL removes for trimmed entries
			if trimOpts.RefMode == shared.RefModeDelRef {
				groupNames := listGroupNames(cmd, key)
				for _, gName := range groupNames {
					pelSnap, _, _ := getPELSnapshot(cmd, key, gName)
					if pelSnap == nil || pelSnap.NetAdds == nil {
						continue
					}
					for _, entry := range entries {
						if remainingMap[entry.ID.Key()] {
							continue
						}
						pelElemID := string(encodeStreamElementID(entry.ID))
						if _, hasPEL := pelSnap.NetAdds[pelElemID]; hasPEL {
							emitPELRemove(cmd, key, gName, entry.ID)
						}
					}
				}
			}
		}

		_ = beforeLen
		w.WriteInteger(trimmed)
	}
	return
}

// entriesToStreamValue converts snapshot entries to a StreamValue for trim logic reuse.
// populateStreamGroups loads consumer group data from the effects system into a StreamValue.
func populateStreamGroups(cmd *shared.Command, key string, sv *shared.StreamValue) {
	groupNames := listGroupNames(cmd, key)
	for _, gName := range groupNames {
		gSnap, _, _ := getGroupSnapshot(cmd, key, gName)
		lastDelivered, entriesRead, _, _ := getGroupMeta(gSnap)

		group := &shared.ConsumerGroup{
			Name:            gName,
			LastDeliveredID: lastDelivered,
			EntriesRead:     entriesRead,
			Pending:         make(map[shared.StreamID]*shared.PendingEntry),
			Consumers:       make(map[string]*shared.Consumer),
		}

		// Load PEL
		pelSnap, _, _ := getPELSnapshot(cmd, key, gName)
		if pelSnap != nil && pelSnap.NetAdds != nil {
			for elemID, elem := range pelSnap.NetAdds {
				if len(elemID) == 16 {
					id := decodeStreamElementID([]byte(elemID))
					raw := elem.Data.Decompress()
					if len(raw) >= 24 {
						consumer := string(raw[24:])
						group.Pending[id] = &shared.PendingEntry{
							ID:       id,
							Consumer: consumer,
						}
					}
				}
			}
		}

		sv.Groups[gName] = group
	}
}

func entriesToStreamValue(entries []*shared.StreamEntry, snap *pb.ReducedEffect) *shared.StreamValue {
	sv := shared.NewStreamValue()
	lastID, entriesAdded, maxDeletedID, _ := streamMetadata(snap)
	sv.LastID = lastID
	sv.EntriesAdded = entriesAdded
	sv.MaxDeletedID = maxDeletedID

	for _, e := range entries {
		sv.Entries[e.ID.Key()] = e
		sv.Index.Insert(e.ID.Key(), nil, nil)
	}
	if len(entries) > 0 {
		sv.FirstID = entries[0].ID
	}
	return sv
}

// handleXDelEx handles XDELEX key [KEEPREF | DELREF | ACKED] IDS numids id [id ...]
func handleXDelEx(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("xdelex")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	i := 1

	refMode := shared.RefModeKeepRef
	if i < len(cmd.Args) {
		opt := string(shared.ToUpper(cmd.Args[i]))
		switch opt {
		case "KEEPREF":
			refMode = shared.RefModeKeepRef
			i++
		case "DELREF":
			refMode = shared.RefModeDelRef
			i++
		case "ACKED":
			refMode = shared.RefModeAcked
			i++
		case "IDS":
			// Not a mode option, fall through
		default:
			w.WriteError("ERR syntax error")
			return
		}
	}

	if i >= len(cmd.Args) || string(shared.ToUpper(cmd.Args[i])) != "IDS" {
		w.WriteError("ERR syntax error")
		return
	}
	i++

	if i >= len(cmd.Args) {
		w.WriteError("ERR syntax error")
		return
	}

	numIds, err := strconv.Atoi(string(cmd.Args[i]))
	if err != nil || numIds < 1 {
		w.WriteError("ERR Number of IDs must be a positive integer")
		return
	}
	i++

	if i+numIds != len(cmd.Args) {
		if i+numIds > len(cmd.Args) {
			w.WriteError("ERR The `numids` parameter must match the number of arguments")
		} else {
			w.WriteError("ERR syntax error")
		}
		return
	}

	ids := make([]shared.StreamID, numIds)
	for j := range numIds {
		id, _, isSpecial, err := shared.ParseStreamID(string(cmd.Args[i+j]))
		if err != nil || isSpecial {
			w.WriteError("ERR Invalid stream ID specified as stream command argument")
			return
		}
		ids[j] = id
	}

	valid = true
	runner = func() {
		snap, tips, exists, wrongType, err := getStreamSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteArray(numIds)
			for range numIds {
				w.WriteInteger(-1)
			}
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		cmd.Context.BeginTx()
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			w.WriteError(err.Error())
			return
		}

		// Build set of existing entries
		entries := snapshotToEntries(snap)
		entrySet := make(map[string]bool, len(entries))
		for _, e := range entries {
			entrySet[string(encodeStreamElementID(e.ID))] = true
		}

		results := make([]int64, numIds)
		for j, id := range ids {
			elemID := string(encodeStreamElementID(id))
			if !entrySet[elemID] {
				results[j] = -1
				continue
			}

			// ACKED mode check
			if refMode == shared.RefModeAcked {
				canDelete := true
				groupNames := listGroupNames(cmd, key)
				for _, gName := range groupNames {
					gSnap, _, _ := getGroupSnapshot(cmd, key, gName)
					lastDelivered, _, _, _ := getGroupMeta(gSnap)
					if id.Compare(lastDelivered) > 0 {
						canDelete = false
						break
					}
					pelSnap, _, _ := getPELSnapshot(cmd, key, gName)
					if pelSnap != nil && pelSnap.NetAdds != nil {
						pelElemID := string(encodeStreamElementID(id))
						if _, pending := pelSnap.NetAdds[pelElemID]; pending {
							canDelete = false
							break
						}
					}
				}
				if !canDelete {
					results[j] = 2
					continue
				}
			}

			// Delete
			if err := emitStreamRemove(cmd, key, id); err != nil {
				w.WriteError(err.Error())
				return
			}

			// DELREF mode - remove from PELs
			if refMode == shared.RefModeDelRef {
				groupNames := listGroupNames(cmd, key)
				for _, gName := range groupNames {
					pelSnap, _, _ := getPELSnapshot(cmd, key, gName)
					if pelSnap != nil && pelSnap.NetAdds != nil {
						pelElemID := string(encodeStreamElementID(id))
						if _, pending := pelSnap.NetAdds[pelElemID]; pending {
							emitPELRemove(cmd, key, gName, id)
						}
					}
				}
			}

			results[j] = 1
		}

		w.WriteArray(numIds)
		for _, r := range results {
			w.WriteInteger(r)
		}
	}
	return
}

// handleXSetId handles XSETID key last-id [ENTRIESADDED entries-added] [MAXDELETEDID max-deleted-id]
func handleXSetId(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("xsetid")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	lastIDStr := string(cmd.Args[1])

	lastID, _, isSpecial, err := shared.ParseStreamID(lastIDStr)
	if err != nil || isSpecial {
		w.WriteError("ERR Invalid stream ID specified as stream command argument")
		return
	}

	var entriesAddedOpt int64 = -1
	var maxDeletedIDOpt *shared.StreamID
	i := 2

	for i < len(cmd.Args) {
		opt := string(shared.ToUpper(cmd.Args[i]))
		switch opt {
		case "ENTRIESADDED":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			ea, err := strconv.ParseInt(string(cmd.Args[i]), 10, 64)
			if err != nil {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			if ea < 0 {
				w.WriteError("ERR entries_added must be positive")
				return
			}
			entriesAddedOpt = ea
			i++
		case "MAXDELETEDID":
			i++
			if i >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			mdID, _, isSpecial, err := shared.ParseStreamID(string(cmd.Args[i]))
			if err != nil || isSpecial {
				w.WriteError("ERR Invalid stream ID specified as stream command argument")
				return
			}
			maxDeletedIDOpt = &mdID
			i++
		default:
			w.WriteError("ERR syntax error")
			return
		}
	}

	valid = true
	runner = func() {
		snap, tips, exists, wrongType, err := getStreamSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteError("ERR no such key")
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		curLastID, _, curMaxDeletedID, _ := streamMetadata(snap)
		entries := snapshotToEntries(snap)
		if curLastID.IsZero() {
			curLastID = streamLastID(entries)
		}

		// Validate: last-id must be >= current max_deleted_entry_id
		if lastID.Compare(curMaxDeletedID) < 0 {
			w.WriteError("ERR The ID specified in XSETID is smaller than current max_deleted_entry_id")
			return
		}

		// Validate: if maxDeletedID specified, must be <= lastID
		if maxDeletedIDOpt != nil && maxDeletedIDOpt.Compare(lastID) > 0 {
			w.WriteError("ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id")
			return
		}

		// If stream has entries, validate ID >= top entry
		if len(entries) > 0 {
			topEntry := entries[len(entries)-1]
			if lastID.Compare(topEntry.ID) < 0 {
				w.WriteError("ERR The ID specified in XSETID is smaller than the target stream top item")
				return
			}

			if entriesAddedOpt >= 0 && int64(len(entries)) > entriesAddedOpt {
				w.WriteError("ERR The entries_added specified in XSETID is smaller than the target stream length")
				return
			}
		}

		// For XSETID with ORDERED collections, we emit a dummy entry at the specified lastID
		// to establish the lastID cursor, then immediately remove it.
		// This preserves the "lastID" semantics without a separate metadata key.
		cmd.Context.BeginTx()
		if len(tips) > 0 {
			if err := cmd.Context.Emit(&pb.Effect{
				Key:  []byte(key),
				Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
			}, tips); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		// If the requested lastID is beyond the current tail, insert a sentinel and remove it
		// to advance the ordered elements' last known ID via NetRemoves.
		if lastID.Compare(curLastID) > 0 {
			if err := emitStreamInsert(cmd, key, lastID, nil); err != nil {
				w.WriteError(err.Error())
				return
			}
			// First remove pulls from OrderedElements; second ensures NetRemoves tombstone.
			if err := emitStreamRemove(cmd, key, lastID); err != nil {
				w.WriteError(err.Error())
				return
			}
			if err := emitStreamRemove(cmd, key, lastID); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		// If maxDeletedIDOpt is specified, record it in NetRemoves via sentinel
		if maxDeletedIDOpt != nil && maxDeletedIDOpt.Compare(curMaxDeletedID) > 0 {
			if err := emitStreamInsert(cmd, key, *maxDeletedIDOpt, nil); err != nil {
				w.WriteError(err.Error())
				return
			}
			if err := emitStreamRemove(cmd, key, *maxDeletedIDOpt); err != nil {
				w.WriteError(err.Error())
				return
			}
			if err := emitStreamRemove(cmd, key, *maxDeletedIDOpt); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		if entriesAddedOpt >= 0 {
			if err := setStreamEntriesAdded(cmd, key, entriesAddedOpt); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteOK()
	}
	return
}

// handleXIdmpRecord handles XIDMPRECORD key producer iid stream-id
// Records a producer dedup mapping for a given producer/iid → stream-id
func handleXIdmpRecord(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 4 {
		w.WriteWrongNumArguments("xidmprecord")
		return
	}

	key := string(cmd.Args[0])
	producer := string(cmd.Args[1])
	iid := string(cmd.Args[2])
	idStr := string(cmd.Args[3])

	if producer == "" {
		w.WriteError("ERR producer ID must be non-empty")
		return
	}
	if iid == "" {
		w.WriteError("ERR idempotent ID must be non-empty")
		return
	}

	id, _, _, err := shared.ParseStreamID(idStr)
	if err != nil {
		w.WriteError("ERR Invalid stream ID specified as stream command argument")
		return
	}

	keys = []string{key}
	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getStreamSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteError("ERR no such key")
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		// Verify the stream ID exists in the stream
		entries := snapshotToEntries(snap)
		found := false
		for _, e := range entries {
			if e.ID.Compare(id) == 0 {
				found = true
				break
			}
		}
		if !found {
			w.WriteError("ERR No such message in stream")
			return
		}

		// Check if already recorded with same iid
		if existingID, exists := lookupProducerSeq(cmd, key, producer, iid); exists {
			if existingID.Compare(id) != 0 {
				w.WriteError("ERR IID already exists for this producer with a different stream ID")
				return
			}
			// Idempotent: same mapping already exists
			w.WriteOK()
			return
		}

		cmd.Context.BeginTx()

		if err := emitProducerSeq(cmd, key, producer, iid, id); err != nil {
			w.WriteError(err.Error())
			return
		}

		if err := incrementIidsAdded(cmd, key); err != nil {
			w.WriteError(err.Error())
			return
		}

		w.WriteOK()
	}
	return
}

// handleXCfgSet handles XCFGSET key [IDMP-DURATION duration] [IDMP-MAXSIZE maxsize]
func handleXCfgSet(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("xcfgset")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	if len(cmd.Args) < 2 {
		w.WriteError("ERR At least one parameter (IDMP-DURATION or IDMP-MAXSIZE) is required")
		return
	}

	// Parse and validate config options
	cfgValues := make(map[string]int64)
	for i := 1; i < len(cmd.Args); i += 2 {
		opt := string(shared.ToUpper(cmd.Args[i]))
		if i+1 >= len(cmd.Args) {
			w.WriteError("ERR syntax error")
			return
		}
		valStr := string(cmd.Args[i+1])
		val, err := strconv.ParseInt(valStr, 10, 64)
		if err != nil || (len(valStr) > 1 && valStr[0] == '0') {
			w.WriteError("ERR value is not an integer or out of range")
			return
		}
		switch opt {
		case "IDMP-DURATION":
			if val < 1 || val > 86400 {
				w.WriteError("ERR IDMP-DURATION must be between 1 and 86400")
				return
			}
			cfgValues["idmp-duration"] = val
		case "IDMP-MAXSIZE":
			if val < 1 || val > 10000 {
				w.WriteError("ERR IDMP-MAXSIZE must be between 1 and 10000")
				return
			}
			cfgValues["idmp-maxsize"] = val
		default:
			w.WriteError("ERR syntax error")
			return
		}
	}

	valid = true
	runner = func() {
		_, _, exists, wrongType, err := getStreamSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteError("ERR no such key")
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		// Check if any value actually changed — if so, clear IDMP history
		oldDuration, oldMaxsize, _ := getStreamConfig(cmd, key)
		changed := false
		if v, ok := cfgValues["idmp-duration"]; ok && v != oldDuration {
			changed = true
		}
		if v, ok := cfgValues["idmp-maxsize"]; ok && v != oldMaxsize {
			changed = true
		}

		for field, val := range cfgValues {
			if err := emitCfgSet(cmd, key, field, val); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		if changed {
			clearAllProducerKeys(cmd, key)
		}

		w.WriteOK()
	}
	return
}
