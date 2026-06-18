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

func pScalarInt(v int64, merge pb.MergeRule, fch byte) *pb.ReducedEffect {
	return &pb.ReducedEffect{
		Op: pb.EffectOp_INSERT_OP, Merge: merge,
		Collection: pb.CollectionKind_SCALAR, Commutative: isCommutative(merge),
		Hlc: rTs(int64(fch) + 1), NodeId: 1,
		Scalar: &pb.DataEffect{
			Op: pb.EffectOp_INSERT_OP, Merge: merge,
			Collection: pb.CollectionKind_SCALAR, Value: &pb.DataEffect_IntVal{IntVal: v},
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

// Merge of two branches with disjoint partitions unions them.
func TestMergePartitions_DisjointUnion(t *testing.T) {
	a := branchWithPartitions(0x01, map[string]*pb.ReducedEffect{"x": pScalarRaw("vx", 0x01)})
	b := branchWithPartitions(0x02, map[string]*pb.ReducedEffect{"y": pScalarRaw("vy", 0x02)})
	r := Merge2(a, b)
	if got := r.Partitions["x"].GetScalar().GetRaw(); string(got) != "vx" {
		t.Fatalf("partition x = %q, want vx", got)
	}
	if got := r.Partitions["y"].GetScalar().GetRaw(); string(got) != "vy" {
		t.Fatalf("partition y = %q, want vy", got)
	}
}

// Same partition written non-commutatively on both branches: lower fork-choice
// hash wins, per partition, just like a flat scalar.
func TestMergePartitions_SamePartitionForkChoice(t *testing.T) {
	lo := branchWithPartitions(0x01, map[string]*pb.ReducedEffect{"x": pScalarRaw("lo", 0x01)})
	hi := branchWithPartitions(0x02, map[string]*pb.ReducedEffect{"x": pScalarRaw("hi", 0x02)})
	// Merge both orders — partition fork-choice must be commutative.
	for _, r := range []*pb.ReducedEffect{Merge2(lo, hi), Merge2(hi, lo)} {
		if got := r.Partitions["x"].GetScalar().GetRaw(); string(got) != "lo" {
			t.Fatalf("partition x = %q, want lo (lower hash wins)", got)
		}
	}
}

// A partition seeing commutative writes on both branches accumulates — proving
// partitions merge with the canonical MergeN logic, not a naive overwrite.
func TestMergePartitions_CommutativeAccumulate(t *testing.T) {
	a := branchWithPartitions(0x01, map[string]*pb.ReducedEffect{"n": pScalarInt(5, pb.MergeRule_ADDITIVE_INT, 0x01)})
	b := branchWithPartitions(0x02, map[string]*pb.ReducedEffect{"n": pScalarInt(3, pb.MergeRule_ADDITIVE_INT, 0x02)})
	r := Merge2(a, b)
	if got := r.Partitions["n"].GetScalar().GetIntVal(); got != 8 {
		t.Fatalf("partition n = %d, want 8 (5+3)", got)
	}
}

// MergeN merges partitions across all branches deterministically regardless of
// branch order.
func TestMergePartitionsN_OrderInvariant(t *testing.T) {
	mk := func() []*pb.ReducedEffect {
		return []*pb.ReducedEffect{
			branchWithPartitions(0x01, map[string]*pb.ReducedEffect{"a": pScalarInt(1, pb.MergeRule_ADDITIVE_INT, 0x01)}),
			branchWithPartitions(0x02, map[string]*pb.ReducedEffect{"a": pScalarInt(2, pb.MergeRule_ADDITIVE_INT, 0x02)}),
			branchWithPartitions(0x03, map[string]*pb.ReducedEffect{"a": pScalarInt(4, pb.MergeRule_ADDITIVE_INT, 0x03)}),
		}
	}
	fwd := MergeN(mk())
	br := mk()
	rev := MergeN([]*pb.ReducedEffect{br[2], br[1], br[0]})
	if fwd.Partitions["a"].GetScalar().GetIntVal() != 7 || rev.Partitions["a"].GetScalar().GetIntVal() != 7 {
		t.Fatalf("partition a sum not order-invariant: fwd=%d rev=%d",
			fwd.Partitions["a"].GetScalar().GetIntVal(), rev.Partitions["a"].GetScalar().GetIntVal())
	}
}
