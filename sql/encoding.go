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

package sql

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"zombiezen.com/go/sqlite"
)

// Canonical SQLite column affinities we store in schema metadata. We
// match SQLite's own affinity rules (https://sqlite.org/datatype3.html
// §3.1.1) but narrow the final bucket to one of these five strings so
// encoding/decoding is unambiguous.
const (
	affinityInteger = "INTEGER"
	affinityReal    = "REAL"
	affinityText    = "TEXT"
	affinityBlob    = "BLOB"
	affinityNumeric = "NUMERIC"
)

// sqliteAffinity returns the affinity bucket for a declared SQLite
// column type following rule 2 in §3.1.1 of the SQLite type docs.
func sqliteAffinity(declType string) string {
	upper := strings.ToUpper(declType)
	switch {
	case strings.Contains(upper, "INT"):
		return affinityInteger
	case strings.Contains(upper, "CHAR"),
		strings.Contains(upper, "CLOB"),
		strings.Contains(upper, "TEXT"):
		return affinityText
	case strings.Contains(upper, "BLOB"), upper == "":
		return affinityBlob
	case strings.Contains(upper, "REAL"),
		strings.Contains(upper, "FLOA"),
		strings.Contains(upper, "DOUB"):
		return affinityReal
	}
	return affinityNumeric
}

// setEncodedValue writes v into eff.Value in the appropriate oneof
// variant. Returns true if the value was non-NULL and something was
// written; false for NULL (in which case the caller should skip the
// effect entirely — an absent field is the swytch representation of a
// NULL column).
//
// The proto package's oneof wrapper interface is unexported, so we set
// the field here where we have access to the concrete types rather
// than bubbling it up.
func setEncodedValue(eff *pb.DataEffect, v sqlite.Value) bool {
	switch v.Type() {
	case sqlite.TypeInteger:
		eff.Value = &pb.DataEffect_IntVal{IntVal: v.Int64()}
	case sqlite.TypeFloat:
		eff.Value = &pb.DataEffect_FloatVal{FloatVal: v.Float()}
	case sqlite.TypeText:
		eff.Value = &pb.DataEffect_Raw{Raw: []byte(v.Text())}
	case sqlite.TypeBlob:
		eff.Value = &pb.DataEffect_Raw{Raw: v.Blob()}
	case sqlite.TypeNull:
		return false
	default:
		return false
	}
	return true
}

// decodeValue reconstructs a SQLite value from a stored DataEffect
// given the column's affinity (which disambiguates TEXT/BLOB since
// both share the Raw variant).
func decodeValue(eff *pb.DataEffect, affinity string) sqlite.Value {
	if eff == nil || eff.Value == nil {
		return sqlite.Value{}
	}
	switch v := eff.Value.(type) {
	case *pb.DataEffect_IntVal:
		return sqlite.IntegerValue(v.IntVal)
	case *pb.DataEffect_FloatVal:
		return sqlite.FloatValue(v.FloatVal)
	case *pb.DataEffect_Raw:
		if affinity == affinityBlob {
			return sqlite.BlobValue(v.Raw)
		}
		return sqlite.TextValue(string(v.Raw))
	}
	return sqlite.Value{}
}

// encodePK produces the bytes that serve as the row-key suffix for
// a primary key. Composite PKs concatenate component encodings
// using an order-preserving tuple format (see encodeTupleComponent)
// so lex ordering of the byte string matches tuple ordering
// left-to-right.
//
// For single-column PKs this degenerates to the component encoding
// alone — a text PK stays raw text, an integer PK stays 8 bytes of
// sign-flipped BE — preserving everything existing tests assume.
//
// The returned string is a byte container; the bytes are NOT
// required to be printable or valid UTF-8.
func encodePK(values []sqlite.Value, affinities []string) (string, error) {
	if len(values) != len(affinities) {
		return "", fmt.Errorf("encodePK: %d values for %d affinities", len(values), len(affinities))
	}
	if len(values) == 1 {
		switch affinities[0] {
		case affinityInteger, affinityReal:
			return string(encodeIndexValue(values[0], affinities[0])), nil
		case affinityText:
			return values[0].Text(), nil
		case affinityBlob:
			return string(values[0].Blob()), nil
		default:
			return values[0].Text(), nil
		}
	}
	var buf []byte
	for i, v := range values {
		buf = append(buf, encodeTupleComponent(v, affinities[i])...)
	}
	return string(buf), nil
}

// decodePK reverses encodePK. For single-column PKs it returns a
// slice of one value; for composite PKs it walks the tuple format.
func decodePK(pk string, affinities []string) []sqlite.Value {
	if len(affinities) == 1 {
		return []sqlite.Value{decodeSinglePK(pk, affinities[0])}
	}
	out := make([]sqlite.Value, 0, len(affinities))
	buf := []byte(pk)
	for _, aff := range affinities {
		val, rest, ok := decodeTupleComponent(buf, aff)
		if !ok {
			// Truncated / malformed — append NULL and stop.
			for len(out) < len(affinities) {
				out = append(out, sqlite.Value{})
			}
			return out
		}
		out = append(out, val)
		buf = rest
	}
	return out
}

// decodeSinglePK is the hot-path single-column decode. Composite
// PKs use decodeTupleComponent instead.
func decodeSinglePK(pk, affinity string) sqlite.Value {
	switch affinity {
	case affinityInteger:
		if len(pk) != 8 {
			return sqlite.TextValue(pk)
		}
		u := binary.BigEndian.Uint64([]byte(pk)) ^ (1 << 63)
		return sqlite.IntegerValue(int64(u))
	case affinityReal:
		if len(pk) != 8 {
			return sqlite.TextValue(pk)
		}
		bits := binary.BigEndian.Uint64([]byte(pk))
		if bits&(1<<63) != 0 {
			bits ^= 1 << 63
		} else {
			bits = ^bits
		}
		return sqlite.FloatValue(math.Float64frombits(bits))
	case affinityBlob:
		return sqlite.BlobValue([]byte(pk))
	default:
		return sqlite.TextValue(pk)
	}
}

// encodeTupleComponent emits one tuple component with an
// order-preserving self-terminating encoding:
//
//   - INTEGER / REAL: the existing 8-byte fixed form. Length is
//     implicit, nothing follows inside the component itself.
//   - TEXT / BLOB:  every 0x00 in the value is escaped to 0x00 0x01;
//     the component is terminated with 0x00 0x00. This makes the
//     encoded bytes self-delimiting AND order-preserving —
//     0x00 0x00 sorts below 0x00 0x01, so "abc\x00\x00" compared
//     byte-wise against "abcd\x00\x00" takes the expected shorter-
//     first outcome. Empty string encodes to just "\x00\x00".
//
// This matches the well-known FoundationDB tuple-layer trick.
func encodeTupleComponent(v sqlite.Value, affinity string) []byte {
	switch affinity {
	case affinityInteger, affinityReal:
		return encodeIndexValue(v, affinity)
	case affinityText:
		return encodeTupleBytes([]byte(v.Text()))
	case affinityBlob:
		return encodeTupleBytes(v.Blob())
	default:
		return encodeTupleBytes([]byte(v.Text()))
	}
}

// decodeTupleComponent reads one component from the head of buf,
// returning the decoded value and the remaining buffer. ok=false
// indicates a truncated or malformed input.
func decodeTupleComponent(buf []byte, affinity string) (sqlite.Value, []byte, bool) {
	switch affinity {
	case affinityInteger:
		if len(buf) < 8 {
			return sqlite.Value{}, nil, false
		}
		u := binary.BigEndian.Uint64(buf[:8]) ^ (1 << 63)
		return sqlite.IntegerValue(int64(u)), buf[8:], true
	case affinityReal:
		if len(buf) < 8 {
			return sqlite.Value{}, nil, false
		}
		bits := binary.BigEndian.Uint64(buf[:8])
		if bits&(1<<63) != 0 {
			bits ^= 1 << 63
		} else {
			bits = ^bits
		}
		return sqlite.FloatValue(math.Float64frombits(bits)), buf[8:], true
	case affinityText, affinityBlob:
		raw, rest, ok := decodeTupleBytes(buf)
		if !ok {
			return sqlite.Value{}, nil, false
		}
		if affinity == affinityBlob {
			return sqlite.BlobValue(raw), rest, true
		}
		return sqlite.TextValue(string(raw)), rest, true
	default:
		raw, rest, ok := decodeTupleBytes(buf)
		if !ok {
			return sqlite.Value{}, nil, false
		}
		return sqlite.TextValue(string(raw)), rest, true
	}
}

// encodeTupleBytes applies the 0x00 → 0x00 0x01 escape and appends a
// 0x00 0x00 terminator. Order-preserving on the escaped output.
func encodeTupleBytes(raw []byte) []byte {
	out := make([]byte, 0, len(raw)+2)
	for _, b := range raw {
		if b == 0x00 {
			out = append(out, 0x00, 0x01)
			continue
		}
		out = append(out, b)
	}
	return append(out, 0x00, 0x00)
}

// decodeTupleBytes is the inverse of encodeTupleBytes. Stops at the
// first unescaped 0x00 0x00.
func decodeTupleBytes(buf []byte) (raw []byte, rest []byte, ok bool) {
	out := make([]byte, 0, len(buf))
	for i := 0; i < len(buf); i++ {
		b := buf[i]
		if b != 0x00 {
			out = append(out, b)
			continue
		}
		if i+1 >= len(buf) {
			return nil, nil, false
		}
		next := buf[i+1]
		switch next {
		case 0x00:
			return out, buf[i+2:], true
		case 0x01:
			out = append(out, 0x00)
			i++ // skip escape byte
		default:
			return nil, nil, false
		}
	}
	return nil, nil, false
}

// encodeIndexKeyTuple builds the ordered byte encoding that serves
// as the "value" portion of an index-entry key for one index write.
// For a single-column index this is exactly encodeIndexValue (no
// trailing bytes). For a composite index each component uses the
// tuple-layer encoding so concatenated ordering matches per-column
// ordering — see encodeTupleComponent.
func encodeIndexKeyTuple(vals []sqlite.Value, affinities []string) []byte {
	if len(vals) == 1 {
		return encodeIndexValue(vals[0], affinities[0])
	}
	var buf []byte
	for i, v := range vals {
		buf = append(buf, encodeTupleComponent(v, affinities[i])...)
	}
	return buf
}

// encodeIndexKeyPrefix is the composite-index counterpart of
// encodeIndexKeyTuple for a left-anchored prefix: the first
// `prefixCount` tuple components are encoded in full, and any
// remaining components are omitted. Used for range scans that
// constrain a prefix of the index columns.
func encodeIndexKeyPrefix(vals []sqlite.Value, affinities []string, prefixCount int) []byte {
	if prefixCount <= 0 {
		return nil
	}
	if prefixCount > len(vals) {
		prefixCount = len(vals)
	}
	if prefixCount == 1 && len(affinities) == 1 {
		return encodeIndexValue(vals[0], affinities[0])
	}
	var buf []byte
	for i := 0; i < prefixCount; i++ {
		buf = append(buf, encodeTupleComponent(vals[i], affinities[i])...)
	}
	return buf
}

// encodeIndexValue turns a column value into bytes whose lex ordering
// matches the column's natural ordering. It is the basis for range
// scans on indexed columns: we append the returned bytes to the
// "i/<index>/" prefix, and keytrie's RangeFrom then walks entries in
// natural column order.
//
// Encoding per affinity:
//
//   - INTEGER: 8-byte big-endian with the sign bit flipped. After the
//     XOR, int64 values compare correctly as unsigned bytes —
//     MinInt64 becomes 0x0000…, 0 becomes 0x8000…, MaxInt64 becomes
//     0xFFFF….
//   - REAL:    IEEE 754 big-endian. For positive floats flip just the
//     sign bit; for negative floats flip every bit. This preserves
//     numeric order including across the sign boundary.
//   - TEXT:    raw UTF-8 bytes (lex order == standard string order).
//   - BLOB:    raw bytes.
//
// NULL values in an indexed column are not inserted into the index —
// SQL's standard behaviour treats NULLs as distinct from every value
// in WHERE comparisons, so they're not reachable via indexed lookups.
// Callers should skip emission when the value is NULL.
func encodeIndexValue(v sqlite.Value, affinity string) []byte {
	switch affinity {
	case affinityInteger:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v.Int64())^(1<<63))
		return buf
	case affinityReal:
		bits := math.Float64bits(v.Float())
		if bits&(1<<63) != 0 {
			// Negative: flip every bit so more-negative sorts lower.
			bits = ^bits
		} else {
			// Positive (including +0): flip only the sign bit so
			// positives sort above flipped-negatives.
			bits ^= 1 << 63
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, bits)
		return buf
	case affinityText:
		return []byte(v.Text())
	case affinityBlob:
		return append([]byte(nil), v.Blob()...)
	default:
		// NUMERIC / unknown: fall back to text representation.
		return []byte(v.Text())
	}
}
