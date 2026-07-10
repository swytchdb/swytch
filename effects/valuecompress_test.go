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

package effects

import (
	"bytes"
	"math/rand"
	"testing"

	pb "github.com/swytchdb/swytch/cluster/proto"
)

// compressibleValue is an alphabet-cycle payload, the same shape trace-bench
// fills with — compresses to a tiny fraction of its size.
func compressibleValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('A' + i%26)
	}
	return b
}

func rawScalar(v []byte) *pb.DataEffect {
	return &pb.DataEffect{
		Op:         pb.EffectOp_INSERT_OP,
		Merge:      pb.MergeRule_LAST_WRITE_WINS,
		Collection: pb.CollectionKind_SCALAR,
		Value:      &pb.DataEffect_Raw{Raw: v},
	}
}

// TestCompressDataValueRoundTrip pins the whole arm contract: a compressible
// value moves to the compressed arm (raw reads as nil — the loud failure a
// stale GetRaw consumer gets), records its inflated length, shrinks, and
// Decompress restores the exact original bytes.
func TestCompressDataValueRoundTrip(t *testing.T) {
	val := compressibleValue(4096)
	d := rawScalar(bytes.Clone(val))
	compressDataValue(d)

	cv := d.GetCompressed()
	if cv == nil {
		t.Fatal("compressible value did not move to the compressed arm")
	}
	if d.GetRaw() != nil {
		t.Fatal("raw arm still readable after compression")
	}
	if cv.GetRawLen() != uint64(len(val)) {
		t.Fatalf("raw_len = %d, want %d", cv.GetRawLen(), len(val))
	}
	if len(cv.GetData()) >= len(val) {
		t.Fatalf("stored %d bytes for a %d-byte compressible value", len(cv.GetData()), len(val))
	}
	if !bytes.Equal(d.Decompress(), val) {
		t.Fatal("Decompress did not restore the original bytes")
	}
}

// TestCompressStoreIfSmaller: incompressible bytes stay on the raw arm — the
// codec ran and lost, so the plain form is stored.
func TestCompressStoreIfSmaller(t *testing.T) {
	v := make([]byte, 4096)
	rand.New(rand.NewSource(42)).Read(v)
	d := rawScalar(v)
	compressDataValue(d)
	if d.GetCompressed() != nil {
		t.Fatal("incompressible value was stored compressed")
	}
	if !bytes.Equal(d.GetRaw(), v) {
		t.Fatal("raw value changed")
	}
}

// TestCompressSkipsSmallValues: below the floor, framing dominates — don't
// bother.
func TestCompressSkipsSmallValues(t *testing.T) {
	d := rawScalar(compressibleValue(compressMinBytes - 1))
	compressDataValue(d)
	if d.GetCompressed() != nil {
		t.Fatal("sub-threshold value was compressed")
	}
}

// TestCompressReducedStateNeverMutatesShared: snapshot state may share element
// pointers with cached reduced state, so compression must swap in fresh
// structs — the input state must come back byte-identical.
func TestCompressReducedStateNeverMutatesShared(t *testing.T) {
	big := compressibleValue(1024)
	small := []byte("tiny")
	elem := func(v []byte) *pb.ReducedElement {
		return &pb.ReducedElement{Data: rawScalar(bytes.Clone(v))}
	}
	orig := &pb.ReducedEffect{
		Scalar: rawScalar(bytes.Clone(big)),
		NetAdds: map[string]*pb.ReducedElement{
			"big":  elem(big),
			"tiny": elem(small),
		},
		OrderedElements: []*pb.ReducedElement{elem(big)},
	}

	out := compressReducedState(orig)

	if orig.Scalar.GetCompressed() != nil || orig.NetAdds["big"].Data.GetCompressed() != nil ||
		orig.OrderedElements[0].Data.GetCompressed() != nil {
		t.Fatal("compression wrote through the shared input state")
	}
	if out.Scalar.GetCompressed() == nil || out.NetAdds["big"].Data.GetCompressed() == nil ||
		out.OrderedElements[0].Data.GetCompressed() == nil {
		t.Fatal("compressible values not compressed in the output state")
	}
	if out.NetAdds["tiny"].Data.GetCompressed() != nil {
		t.Fatal("sub-threshold element compressed")
	}
	for name, got := range map[string][]byte{
		"scalar":  out.Scalar.Decompress(),
		"big":     out.NetAdds["big"].Data.Decompress(),
		"ordered": out.OrderedElements[0].Data.Decompress(),
	} {
		if !bytes.Equal(got, big) {
			t.Fatalf("%s did not round-trip", name)
		}
	}
}

// TestReduceOverCompressedEffects: reduced state carries the compressed arm
// through reduction untouched, and RMW merges (LWW string + additive int)
// inflate the base on demand.
func TestReduceOverCompressedEffects(t *testing.T) {
	val := compressibleValue(2048)
	set := &pb.Effect{Key: []byte("k"), Kind: &pb.Effect_Data{Data: rawScalar(bytes.Clone(val))}}
	CompressEffectValues(set)
	if set.GetData().GetCompressed() == nil {
		t.Fatal("setup: value did not compress")
	}

	r := ReduceBranch([]*pb.Effect{set})
	if r.Scalar.GetCompressed() == nil {
		t.Fatal("reduction inflated the value; reduced state should stay compressed")
	}
	if !bytes.Equal(r.Scalar.Decompress(), val) {
		t.Fatal("reduced value did not round-trip")
	}

	// An additive-int on top of a compressed LWW base must inflate the base
	// to parse it — a zero-padded number is long enough to compress and
	// still parses as an int64.
	digits := append(bytes.Repeat([]byte("0"), 126), []byte("42")...)
	setNum := &pb.Effect{Key: []byte("n"), Kind: &pb.Effect_Data{Data: rawScalar(digits)}}
	CompressEffectValues(setNum)
	if setNum.GetData().GetCompressed() == nil {
		t.Fatal("setup: numeric value did not compress")
	}
	incr := &pb.Effect{Key: []byte("n"), Kind: &pb.Effect_Data{Data: &pb.DataEffect{
		Op:         pb.EffectOp_INSERT_OP,
		Merge:      pb.MergeRule_ADDITIVE_INT,
		Collection: pb.CollectionKind_SCALAR,
		Value:      &pb.DataEffect_IntVal{IntVal: 1},
	}}}
	rn := ReduceBranch([]*pb.Effect{setNum, incr})
	if rn.Scalar.GetIntVal() != 43 {
		t.Fatalf("INCR over compressed base = %d, want 43", rn.Scalar.GetIntVal())
	}
}

// TestEngineCompressValuesEndToEnd: with CompressValues on, an emitted value
// rides the DAG compressed — the reconstructed state still holds the
// compressed arm — and Decompress hands back the user's bytes.
func TestEngineCompressValuesEndToEnd(t *testing.T) {
	e := NewEngine(EngineConfig{NodeID: 1, CompressValues: true})
	defer func() {
		if err := e.Close(); err != nil {
			t.Errorf("engine close: %v", err)
		}
	}()

	val := compressibleValue(8192)
	ctx := e.NewContext()
	if err := ctx.Emit(&pb.Effect{Key: []byte("k"), Kind: &pb.Effect_Data{Data: rawScalar(bytes.Clone(val))}}); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if err := ctx.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	if r == nil || r.Scalar == nil {
		t.Fatal("no reduced state")
	}
	if r.Scalar.GetCompressed() == nil {
		t.Fatal("value not stored compressed under CompressValues")
	}
	if !bytes.Equal(r.Scalar.Decompress(), val) {
		t.Fatal("read did not round-trip the value")
	}
}

// TestInflatedSizeDelta: billing's declared size must grow by exactly what
// compression hid — for both a data effect and a snapshot's state.
func TestInflatedSizeDelta(t *testing.T) {
	plain := &pb.Effect{Kind: &pb.Effect_Data{Data: rawScalar(compressibleValue(512))}}
	if got := InflatedSizeDelta(plain); got != 0 {
		t.Fatalf("uncompressed delta = %d, want 0", got)
	}

	eff := &pb.Effect{Kind: &pb.Effect_Data{Data: rawScalar(compressibleValue(4096))}}
	CompressEffectValues(eff)
	cv := eff.GetData().GetCompressed()
	want := cv.GetRawLen() - uint64(len(cv.GetData()))
	if got := InflatedSizeDelta(eff); got != want {
		t.Fatalf("data delta = %d, want %d", got, want)
	}

	snap := &pb.Effect{Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
		State: &pb.ReducedEffect{
			Scalar: rawScalar(compressibleValue(2048)),
			NetAdds: map[string]*pb.ReducedElement{
				"e": {Data: rawScalar(compressibleValue(1024))},
			},
		},
	}}}
	CompressEffectValues(snap)
	st := snap.GetSnapshot().State
	scv, ecv := st.Scalar.GetCompressed(), st.NetAdds["e"].Data.GetCompressed()
	want = scv.GetRawLen() - uint64(len(scv.GetData())) + ecv.GetRawLen() - uint64(len(ecv.GetData()))
	if got := InflatedSizeDelta(snap); got != want {
		t.Fatalf("snapshot delta = %d, want %d", got, want)
	}
}
