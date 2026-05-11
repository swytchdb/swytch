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

package beacon

import (
	"encoding/binary"
	"testing"

	pb "github.com/swytchdb/cache/cluster/proto"
)

func TestNodeIDBytes_Roundtrip(t *testing.T) {
	ids := []uint64{0, 1, 42, 1<<32 | 0xDEADBEEF, ^uint64(0)}
	for _, id := range ids {
		b := nodeIDBytes(id)
		if len(b) != 8 {
			t.Fatalf("expected 8 bytes, got %d", len(b))
		}
		got := nodeIDFromBytes(b)
		if got != id {
			t.Fatalf("roundtrip failed: %d -> %d", id, got)
		}
	}
}

func TestNodeIDFromBytes_InvalidLength(t *testing.T) {
	if nodeIDFromBytes(nil) != 0 {
		t.Fatal("expected 0 for nil")
	}
	if nodeIDFromBytes([]byte{1, 2, 3}) != 0 {
		t.Fatal("expected 0 for wrong length")
	}
}

func TestBuildMemberInsert(t *testing.T) {
	eff := buildMemberInsert(12345, "10.0.0.1:7379")
	if string(eff.Key) != MembershipKey {
		t.Fatalf("expected key %q, got %q", MembershipKey, eff.Key)
	}

	data := eff.GetData()
	if data == nil {
		t.Fatal("expected DataEffect")
	}
	if data.Op != pb.EffectOp_INSERT_OP {
		t.Fatalf("expected INSERT_OP, got %v", data.Op)
	}
	if data.Merge != pb.MergeRule_LAST_WRITE_WINS {
		t.Fatalf("expected LAST_WRITE_WINS, got %v", data.Merge)
	}
	if data.Collection != pb.CollectionKind_KEYED {
		t.Fatalf("expected KEYED, got %v", data.Collection)
	}

	gotID := binary.LittleEndian.Uint64(data.Id)
	if gotID != 12345 {
		t.Fatalf("expected nodeID 12345, got %d", gotID)
	}

	raw := data.GetRaw()
	if string(raw) != "10.0.0.1:7379" {
		t.Fatalf("expected addr 10.0.0.1:7379, got %q", raw)
	}
}

func TestBuildMemberRemove(t *testing.T) {
	eff := buildMemberRemove(999)
	data := eff.GetData()
	if data == nil {
		t.Fatal("expected DataEffect")
	}
	if data.Op != pb.EffectOp_REMOVE_OP {
		t.Fatalf("expected REMOVE_OP, got %v", data.Op)
	}
	if data.Collection != pb.CollectionKind_KEYED {
		t.Fatalf("expected KEYED, got %v", data.Collection)
	}
	gotID := binary.LittleEndian.Uint64(data.Id)
	if gotID != 999 {
		t.Fatalf("expected nodeID 999, got %d", gotID)
	}
}

func TestParseMembership_Nil(t *testing.T) {
	members := parseMembership(nil)
	if len(members) != 0 {
		t.Fatalf("expected empty, got %d members", len(members))
	}
}

func TestParseMembership_Empty(t *testing.T) {
	reduced := &pb.ReducedEffect{
		NetAdds: map[string]*pb.ReducedElement{},
	}
	members := parseMembership(reduced)
	if len(members) != 0 {
		t.Fatalf("expected empty, got %d members", len(members))
	}
}

func TestParseMembership(t *testing.T) {
	// Build a ReducedEffect matching what the engine would produce
	// for two HSET-like entries on __swytch:members.
	node1ID := nodeIDBytes(100)
	node2ID := nodeIDBytes(200)

	reduced := &pb.ReducedEffect{
		Collection: pb.CollectionKind_KEYED,
		NetAdds: map[string]*pb.ReducedElement{
			string(node1ID): {
				Data: &pb.DataEffect{
					Op:         pb.EffectOp_INSERT_OP,
					Merge:      pb.MergeRule_LAST_WRITE_WINS,
					Collection: pb.CollectionKind_KEYED,
					Id:         node1ID,
					Value:      &pb.DataEffect_Raw{Raw: []byte("10.0.0.1:7379")},
				},
			},
			string(node2ID): {
				Data: &pb.DataEffect{
					Op:         pb.EffectOp_INSERT_OP,
					Merge:      pb.MergeRule_LAST_WRITE_WINS,
					Collection: pb.CollectionKind_KEYED,
					Id:         node2ID,
					Value:      &pb.DataEffect_Raw{Raw: []byte("10.0.0.2:7379")},
				},
			},
		},
	}

	members := parseMembership(reduced)
	if len(members) != 2 {
		t.Fatalf("expected 2 members, got %d", len(members))
	}

	byID := map[uint64]string{}
	for _, m := range members {
		byID[m.NodeID] = m.Addr
	}

	if byID[100] != "10.0.0.1:7379" {
		t.Fatalf("expected node 100 at 10.0.0.1:7379, got %q", byID[100])
	}
	if byID[200] != "10.0.0.2:7379" {
		t.Fatalf("expected node 200 at 10.0.0.2:7379, got %q", byID[200])
	}
}

func TestMembersEqual(t *testing.T) {
	a := []Member{{NodeID: 1, Addr: "a"}, {NodeID: 2, Addr: "b"}}
	b := []Member{{NodeID: 2, Addr: "b"}, {NodeID: 1, Addr: "a"}}

	if !membersEqual(a, b) {
		t.Fatal("expected equal")
	}

	c := []Member{{NodeID: 1, Addr: "a"}, {NodeID: 3, Addr: "c"}}
	if membersEqual(a, c) {
		t.Fatal("expected not equal (different IDs)")
	}

	d := []Member{{NodeID: 1, Addr: "a"}}
	if membersEqual(a, d) {
		t.Fatal("expected not equal (different lengths)")
	}

	e := []Member{{NodeID: 1, Addr: "changed"}, {NodeID: 2, Addr: "b"}}
	if membersEqual(a, e) {
		t.Fatal("expected not equal (different addrs)")
	}
}
