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
	"encoding/binary"
	"testing"

	pb "github.com/swytchdb/swytch/cluster/proto"
)

// TestMarshalEffect_BinaryNetAddsKey is the regression test for the bug where
// snapshots with non-UTF-8 element IDs (e.g. beacon membership using
// little-endian uint64 nodeIDs) silently failed proto.Marshal, producing
// orphaned snapshot effects that vanished from the DAG. The wire wrappers
// hex-encode keys so the marshal succeeds and the in-memory map keys
// survive a round-trip unchanged.
func TestMarshalEffect_BinaryNetAddsKey(t *testing.T) {
	idBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(idBytes, 7636774211719507549)

	state := &pb.ReducedEffect{
		Collection: pb.CollectionKind_KEYED,
		NetAdds: map[string]*pb.ReducedElement{
			string(idBytes): {
				Data: &pb.DataEffect{
					Op:    pb.EffectOp_INSERT_OP,
					Value: &pb.DataEffect_Raw{Raw: []byte("127.0.0.1:9000")},
				},
			},
		},
	}

	eff := &pb.Effect{
		Key: []byte("__swytch:members"),
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
			Collection: pb.CollectionKind_KEYED,
			State:      state,
		}},
	}

	data, err := MarshalEffect(eff)
	if err != nil {
		t.Fatalf("MarshalEffect failed on binary NetAdds key: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty marshalled bytes")
	}

	out := &pb.Effect{}
	if err := UnmarshalEffect(data, out); err != nil {
		t.Fatalf("UnmarshalEffect failed: %v", err)
	}

	snap := out.GetSnapshot()
	if snap == nil {
		t.Fatal("decoded effect lost its Snapshot kind")
	}
	if snap.State == nil {
		t.Fatal("decoded snapshot lost its state")
	}
	if len(snap.State.NetAdds) != 1 {
		t.Fatalf("expected 1 NetAdds entry, got %d", len(snap.State.NetAdds))
	}
	got, ok := snap.State.NetAdds[string(idBytes)]
	if !ok {
		t.Fatalf("decoded NetAdds key not equal to original binary bytes\nkeys: %q",
			keysOf(snap.State.NetAdds))
	}
	if string(got.Data.GetRaw()) != "127.0.0.1:9000" {
		t.Fatalf("value lost in round-trip: got %q", got.Data.GetRaw())
	}
}

// TestMarshalEffect_OriginalNotMutated guards against the wrapper mutating
// the caller's effect. Sanitization must happen on a clone.
func TestMarshalEffect_OriginalNotMutated(t *testing.T) {
	idBytes := []byte{0xff, 0xfe, 0xfd}
	state := &pb.ReducedEffect{
		Collection: pb.CollectionKind_KEYED,
		NetAdds: map[string]*pb.ReducedElement{
			string(idBytes): {Data: &pb.DataEffect{Op: pb.EffectOp_INSERT_OP}},
		},
		NetRemoves: map[string]bool{
			string([]byte{0xff, 0xff}): true,
		},
	}
	eff := &pb.Effect{
		Key: []byte("k"),
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
			Collection: pb.CollectionKind_KEYED,
			State:      state,
		}},
	}

	if _, err := MarshalEffect(eff); err != nil {
		t.Fatalf("MarshalEffect: %v", err)
	}

	// Original NetAdds key must still be the raw binary string, not hex.
	if _, ok := state.NetAdds[string(idBytes)]; !ok {
		t.Fatalf("MarshalEffect mutated original NetAdds keys: %q", keysOf(state.NetAdds))
	}
	if _, ok := state.NetRemoves[string([]byte{0xff, 0xff})]; !ok {
		t.Fatal("MarshalEffect mutated original NetRemoves keys")
	}
}

// TestMarshalEffect_NoSnapshotShortCircuit verifies non-snapshot effects
// don't pay the clone cost.
func TestMarshalEffect_NoSnapshotShortCircuit(t *testing.T) {
	eff := &pb.Effect{
		Key: []byte("k"),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:    pb.EffectOp_INSERT_OP,
			Value: &pb.DataEffect_Raw{Raw: []byte("v")},
		}},
	}
	data, err := MarshalEffect(eff)
	if err != nil {
		t.Fatal(err)
	}
	out := &pb.Effect{}
	if err := UnmarshalEffect(data, out); err != nil {
		t.Fatal(err)
	}
	if out.GetData() == nil || string(out.GetData().GetRaw()) != "v" {
		t.Fatalf("data effect did not round-trip: %+v", out)
	}
}

func keysOf(m map[string]*pb.ReducedElement) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
