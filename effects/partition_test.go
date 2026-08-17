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
	"testing"

	pb "github.com/swytchdb/swytch/cluster/proto"
)

// virt sets the virtual partition key on an effect (fluent helper).
func virt(e *pb.Effect, v string) *pb.Effect {
	e.Virtual = []byte(v)
	return e
}

func pScalarRaw(raw string, fch byte) *pb.ReducedEffect {
	return &pb.ReducedEffect{
		Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_LAST_WRITE_WINS,
		Collection: pb.CollectionKind_SCALAR, Commutative: false,
		Hlc: rTs(int64(fch) + 1), NodeId: 1,
		Scalar: &pb.DataEffect{
			Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR, Value: &pb.DataEffect_Raw{Raw: []byte(raw)},
		},
		ForkChoiceHash: []byte{fch},
	}
}

// branchWithPartitions builds a minimal root-object branch carrying partitions.
func branchWithPartitions(fch byte, parts map[string]*pb.ReducedEffect) *pb.ReducedEffect {
	return &pb.ReducedEffect{
		Op: pb.EffectOp_INSERT_OP, Collection: pb.CollectionKind_KEYED, Commutative: false,
		Hlc: rTs(int64(fch) + 1), NodeId: 1,
		NetAdds: map[string]*pb.ReducedElement{
			"m": {Data: &pb.DataEffect{Value: &pb.DataEffect_Raw{Raw: []byte("marker")}}, Hlc: rTs(int64(fch) + 1)},
		},
		ForkChoiceHash: []byte{fch},
		Partitions:     parts,
	}
}

// A flat chain (no virtual effects, no seed partitions) must produce no
// partitions — the fast path, byte-identical to the pre-nesting engine.
func TestReduce_FlatHasNoPartitions(t *testing.T) {
	r := ReduceBranch([]*pb.Effect{
		makeDataEffect("k", 1, 1, scalarInsertRaw([]byte("hello"))),
	})
	if r == nil || r.Scalar.GetRaw() == nil {
		t.Fatalf("flat scalar lost: %v", r)
	}
	if len(r.Partitions) != 0 {
		t.Fatalf("flat key grew partitions: %v", r.Partitions)
	}
}

// Writes to disjoint virtual partitions land in separate partition entries and
// do not interfere; the root partition reduces normally alongside them.
func TestReduce_DisjointPartitions(t *testing.T) {
	root := makeDataEffect("k", 0, 1, keyedInsertRaw("a", []byte("marker")))
	ea := virt(makeDataEffect("k", 1, 1, scalarInsertRaw([]byte("1"))), "a")
	eb := virt(makeDataEffect("k", 2, 1, scalarInsertRaw([]byte("2"))), "b")

	r := ReduceBranch([]*pb.Effect{root, ea, eb})
	if r == nil || r.Collection != pb.CollectionKind_KEYED {
		t.Fatalf("root not KEYED: %v", r)
	}
	if r.NetAdds["a"] == nil {
		t.Fatalf("root membership for a missing")
	}
	if got := r.Partitions["a"].GetScalar().GetRaw(); string(got) != "1" {
		t.Fatalf("partition a = %q, want 1", got)
	}
	if got := r.Partitions["b"].GetScalar().GetRaw(); string(got) != "2" {
		t.Fatalf("partition b = %q, want 2", got)
	}
}

// Sequential writes to the same partition reduce like a flat chain — last
// (topo-order) write wins under LWW.
func TestReduce_SamePartitionOverwrite(t *testing.T) {
	p1 := virt(makeDataEffect("k", 1, 1, scalarInsertRaw([]byte("1"))), "a")
	p2 := virt(makeDataEffect("k", 2, 1, scalarInsertRaw([]byte("2"))), "a")
	r := ReduceBranch([]*pb.Effect{p1, p2})
	if got := r.Partitions["a"].GetScalar().GetRaw(); string(got) != "2" {
		t.Fatalf("partition a = %q, want 2 (overwrite)", got)
	}
}

// A root key-level DEL wipes all prior partition state (it deletes the whole
// key); partitions written after the DEL survive.
func TestReduce_RootKeyDelWipesPartitions(t *testing.T) {
	// DEL with nothing after it: the key (and its partitions) is gone.
	gone := ReduceBranch([]*pb.Effect{
		virt(makeDataEffect("k", 1, 1, scalarInsertRaw([]byte("1"))), "a"),
		makeDataEffect("k", 2, 1, scalarRemove()),
	})
	if gone == nil || gone.Op != pb.EffectOp_REMOVE_OP {
		t.Fatalf("expected REMOVE tombstone, got %v", gone)
	}
	if len(gone.Partitions) != 0 {
		t.Fatalf("DEL did not wipe partitions: %v", gone.Partitions)
	}

	// DEL then recreate root + a new value in partition "a": only the post-DEL
	// state survives.
	recreated := ReduceBranch([]*pb.Effect{
		virt(makeDataEffect("k", 1, 1, scalarInsertRaw([]byte("old"))), "a"),
		makeDataEffect("k", 2, 1, scalarRemove()),
		makeDataEffect("k", 3, 1, keyedInsertRaw("a", []byte("marker"))),
		virt(makeDataEffect("k", 4, 1, scalarInsertRaw([]byte("new"))), "a"),
	})
	if recreated.Op == pb.EffectOp_REMOVE_OP {
		t.Fatalf("recreated key should not be a tombstone: %v", recreated)
	}
	if got := recreated.Partitions["a"].GetScalar().GetRaw(); string(got) != "new" {
		t.Fatalf("partition a = %q, want new (pre-DEL value wiped)", got)
	}
}

// Seed partitions (from a merge point / snapshot) that receive no new effects
// in this chain are carried through; new effects on other partitions add to them.
func TestReduce_SeedPartitionsCarried(t *testing.T) {
	seed := ReduceBranch([]*pb.Effect{
		makeDataEffect("k", 0, 1, keyedInsertRaw("a", []byte("marker"))),
		virt(makeDataEffect("k", 1, 1, scalarInsertRaw([]byte("av"))), "a"),
	})
	if len(seed.Partitions) == 0 {
		t.Fatalf("seed has no partitions")
	}

	r := ReduceChain(seed, []*pb.Effect{
		virt(makeDataEffect("k", 2, 1, scalarInsertRaw([]byte("bv"))), "b"),
	})
	if got := r.Partitions["a"].GetScalar().GetRaw(); string(got) != "av" {
		t.Fatalf("carried partition a = %q, want av", got)
	}
	if got := r.Partitions["b"].GetScalar().GetRaw(); string(got) != "bv" {
		t.Fatalf("new partition b = %q, want bv", got)
	}
}

// cloneReduced makes the partitions map independent — mutating the clone must
// not corrupt the original (snapshots in the cache rely on this).
func TestCloneReduced_ClonesPartitions(t *testing.T) {
	orig := branchWithPartitions(0x01, map[string]*pb.ReducedEffect{
		"a": pScalarRaw("1", 0x01),
	})
	c := cloneReduced(orig)
	c.Partitions["b"] = pScalarRaw("2", 0x02)
	c.Partitions["a"] = pScalarRaw("mutated", 0x03)

	if _, leaked := orig.Partitions["b"]; leaked {
		t.Fatalf("clone leaked partition b into original")
	}
	if got := orig.Partitions["a"].GetScalar().GetRaw(); string(got) != "1" {
		t.Fatalf("original partition a mutated to %q", got)
	}
}
