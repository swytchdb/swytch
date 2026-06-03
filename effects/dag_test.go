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
	"github.com/swytchdb/swytch/keytrie"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func orderedAppend(log *snapshotLog, hlcNanos int64, nodeID uint64, id string, value []byte, deps ...Tip) Tip {
	hlc := timestamppb.New(sTs(hlcNanos).AsTime())
	var pbDeps []*pb.EffectRef
	for _, d := range deps {
		pbDeps = append(pbDeps, toPbRef(d))
	}
	return log.putEffect(&pb.Effect{
		Key:            []byte("k"),
		Hlc:            hlc,
		NodeId:         nodeID,
		ForkChoiceHash: ComputeForkChoiceHash(pb.NodeID(nodeID), hlc),
		Deps:           pbDeps,
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_ORDERED,
			Placement:  pb.Placement_PLACE_TAIL,
			Id:         []byte(id),
			Value:      &pb.DataEffect_Raw{Raw: value},
		}},
	})
}

func orderedValues(r *pb.ReducedEffect) []string {
	if r == nil {
		return nil
	}
	var vals []string
	for _, elem := range r.OrderedElements {
		vals = append(vals, string(elem.Data.GetRaw()))
	}
	return vals
}

func strSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func positionMap(vals []string) map[string]int {
	m := make(map[string]int, len(vals))
	for i, v := range vals {
		m[v] = i
	}
	return m
}

func isContiguous(result []string, branch []string) bool {
	if len(branch) == 0 {
		return true
	}
	branchSet := make(map[string]bool, len(branch))
	for _, v := range branch {
		branchSet[v] = true
	}
	start := -1
	for i, v := range result {
		if branchSet[v] {
			start = i
			break
		}
	}
	if start == -1 {
		return false
	}
	idx := 0
	for i := start; i < len(result) && idx < len(branch); i++ {
		if branchSet[result[i]] {
			idx++
		} else {
			return false
		}
	}
	return idx == len(branch)
}

// --- Ordered list merge behaviors ---

func TestOrdered_LinearChain(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	a := orderedAppend(log, 10, 1, "id-a", []byte("a"))
	b := orderedAppend(log, 20, 1, "id-b", []byte("b"), a)
	c := orderedAppend(log, 30, 1, "id-c", []byte("c"), b)
	e.index.Insert("k", nil, keytrie.NewTipSet(c))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	got := orderedValues(r)
	want := []string{"a", "b", "c"}
	if !strSliceEqual(got, want) {
		t.Fatalf("linear chain: got %v, want %v", got, want)
	}
}

func TestOrdered_ForkPreservesCausalOrder(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	a := orderedAppend(log, 10, 1, "id-a", []byte("a"))
	b := orderedAppend(log, 20, 1, "id-b", []byte("b"), a)
	c := orderedAppend(log, 30, 1, "id-c", []byte("c"), b)
	d := orderedAppend(log, 40, 2, "id-d", []byte("d"), c)

	// Concurrent branch from b
	ee := orderedAppend(log, 35, 3, "id-e", []byte("e"), b)

	e.index.Insert("k", nil, keytrie.NewTipSet(d, ee))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	got := orderedValues(r)
	if len(got) != 5 {
		t.Fatalf("expected 5 elements, got %v", got)
	}

	pos := positionMap(got)
	if pos["a"] >= pos["b"] {
		t.Errorf("'a' must be before 'b'; got %v", got)
	}
	if pos["b"] >= pos["c"] {
		t.Errorf("'b' must be before 'c'; got %v", got)
	}
	if pos["c"] >= pos["d"] {
		t.Errorf("'c' must be before 'd'; got %v", got)
	}
}

func TestOrdered_StableUnderGrowth(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	a := orderedAppend(log, 10, 1, "id-a", []byte("a"))
	b := orderedAppend(log, 20, 2, "id-b", []byte("b"), a)
	c := orderedAppend(log, 30, 1, "id-c", []byte("c"), b)

	// Initial: linear [a, b, c]
	r1, _, err := e.reconstruct("k", []Tip{c}, "", false)
	if err != nil {
		t.Fatal(err)
	}
	got1 := orderedValues(r1)
	want1 := []string{"a", "b", "c"}
	if !strSliceEqual(got1, want1) {
		t.Fatalf("initial: got %v, want %v", got1, want1)
	}

	// Add concurrent branch from a
	d := orderedAppend(log, 15, 3, "id-d", []byte("d"), a)
	ee := orderedAppend(log, 25, 3, "id-e", []byte("e"), d)

	// Grown: same effects plus concurrent branch, tips = [c, ee]
	r2, _, err := e.reconstruct("k", []Tip{c, ee}, "", false)
	if err != nil {
		t.Fatal(err)
	}
	got2 := orderedValues(r2)
	if len(got2) != 5 {
		t.Fatalf("grown: expected 5 elements, got %v", got2)
	}

	// a, b, c must maintain relative ordering
	pos := positionMap(got2)
	if pos["a"] >= pos["b"] {
		t.Errorf("grown: 'a' must be before 'b'; got %v", got2)
	}
	if pos["b"] >= pos["c"] {
		t.Errorf("grown: 'b' must be before 'c'; got %v", got2)
	}
	if pos["d"] >= pos["e"] {
		t.Errorf("grown: 'd' must be before 'e'; got %v", got2)
	}
}

func TestOrdered_DeterministicAcrossViews(t *testing.T) {
	log := newSnapshotLog()

	a := orderedAppend(log, 10, 1, "id-a", []byte("a"))
	b := orderedAppend(log, 20, 1, "id-b", []byte("b"), a)
	c := orderedAppend(log, 30, 1, "id-c", []byte("c"), b)
	d := orderedAppend(log, 40, 2, "id-d", []byte("d"), c)
	ee := orderedAppend(log, 35, 3, "id-e", []byte("e"), b)
	f := orderedAppend(log, 45, 3, "id-f", []byte("f"), ee)

	// View 1: tips = [d, f]
	e1 := newSnapshotEngine(log)
	e1.index.Insert("k", nil, keytrie.NewTipSet(d, f))
	r1, _, _, err := e1.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	got1 := orderedValues(r1)

	// View 2: same effects, different tip set (subscription merges them)
	sub := log.putEffect(&pb.Effect{
		Key:    []byte("k"),
		Hlc:    sTs(50),
		NodeId: 4,
		Deps:   []*pb.EffectRef{toPbRef(d), toPbRef(f)},
		Kind:   &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 4}},
	})
	e2 := newSnapshotEngine(log)
	e2.index.Insert("k", nil, keytrie.NewTipSet(sub))
	r2, _, _, err := e2.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	got2 := orderedValues(r2)

	if !strSliceEqual(got1, got2) {
		t.Fatalf("different views produced different orderings:\n  view1: %v\n  view2: %v", got1, got2)
	}
}

func TestOrdered_BranchContiguity_TwoBranches(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	x := orderedAppend(log, 10, 1, "id-x", []byte("x"))
	a1 := orderedAppend(log, 20, 1, "id-a1", []byte("a1"), x)
	a2 := orderedAppend(log, 30, 1, "id-a2", []byte("a2"), a1)
	a3 := orderedAppend(log, 40, 1, "id-a3", []byte("a3"), a2)

	b1 := orderedAppend(log, 21, 2, "id-b1", []byte("b1"), x)
	b2 := orderedAppend(log, 31, 2, "id-b2", []byte("b2"), b1)
	b3 := orderedAppend(log, 41, 2, "id-b3", []byte("b3"), b2)

	e.index.Insert("k", nil, keytrie.NewTipSet(a3, b3))
	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	got := orderedValues(r)
	if len(got) != 7 {
		t.Fatalf("expected 7 elements, got %v", got)
	}
	if got[0] != "x" {
		t.Fatalf("expected 'x' first, got %v", got)
	}
	if !isContiguous(got, []string{"a1", "a2", "a3"}) {
		t.Errorf("branch A not contiguous in %v", got)
	}
	if !isContiguous(got, []string{"b1", "b2", "b3"}) {
		t.Errorf("branch B not contiguous in %v", got)
	}
	pos := positionMap(got)
	if pos["a1"] >= pos["a2"] || pos["a2"] >= pos["a3"] {
		t.Errorf("branch A order broken in %v", got)
	}
	if pos["b1"] >= pos["b2"] || pos["b2"] >= pos["b3"] {
		t.Errorf("branch B order broken in %v", got)
	}
}

func TestOrdered_BranchContiguity_ThreeBranches(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	x := orderedAppend(log, 10, 1, "id-x", []byte("x"))
	a1 := orderedAppend(log, 20, 1, "id-a1", []byte("a1"), x)
	a2 := orderedAppend(log, 30, 1, "id-a2", []byte("a2"), a1)
	b1 := orderedAppend(log, 21, 2, "id-b1", []byte("b1"), x)
	b2 := orderedAppend(log, 31, 2, "id-b2", []byte("b2"), b1)
	c1 := orderedAppend(log, 22, 3, "id-c1", []byte("c1"), x)
	c2 := orderedAppend(log, 32, 3, "id-c2", []byte("c2"), c1)

	e.index.Insert("k", nil, keytrie.NewTipSet(a2, b2, c2))
	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	got := orderedValues(r)
	if len(got) != 7 {
		t.Fatalf("expected 7 elements, got %v", got)
	}
	if !isContiguous(got, []string{"a1", "a2"}) {
		t.Errorf("branch A not contiguous in %v", got)
	}
	if !isContiguous(got, []string{"b1", "b2"}) {
		t.Errorf("branch B not contiguous in %v", got)
	}
	if !isContiguous(got, []string{"c1", "c2"}) {
		t.Errorf("branch C not contiguous in %v", got)
	}
}

func TestOrdered_MergeTipDoesNotChangeOrdering(t *testing.T) {
	log := newSnapshotLog()

	x := orderedAppend(log, 10, 1, "id-x", []byte("x"))
	a1 := orderedAppend(log, 20, 1, "id-a1", []byte("a1"), x)
	a2 := orderedAppend(log, 30, 1, "id-a2", []byte("a2"), a1)
	b1 := orderedAppend(log, 21, 2, "id-b1", []byte("b1"), x)
	b2 := orderedAppend(log, 31, 2, "id-b2", []byte("b2"), b1)

	// Without merge node: multi-tip
	e1 := newSnapshotEngine(log)
	e1.index.Insert("k", nil, keytrie.NewTipSet(a2, b2))
	r1, _, _, err := e1.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	got1 := orderedValues(r1)

	// With merge node: subscription merges both tips
	merge := log.putEffect(&pb.Effect{
		Key:    []byte("k"),
		Hlc:    sTs(50),
		NodeId: 3,
		Deps:   []*pb.EffectRef{toPbRef(a2), toPbRef(b2)},
		Kind:   &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 3}},
	})
	e2 := newSnapshotEngine(log)
	e2.index.Insert("k", nil, keytrie.NewTipSet(merge))
	r2, _, _, err := e2.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	got2 := orderedValues(r2)

	if !strSliceEqual(got1, got2) {
		t.Fatalf("merge tip changed ordering:\n  without: %v\n  with:    %v", got1, got2)
	}
}

// --- Merge commutativity ---

func TestOrdered_MergeIsCommutative(t *testing.T) {
	branchA := &pb.ReducedEffect{
		Op:         pb.EffectOp_INSERT_OP,
		Collection: pb.CollectionKind_ORDERED,
		Hlc:        sTs(30),
		NodeId:     1,
		OrderedElements: []*pb.ReducedElement{
			{Data: &pb.DataEffect{Id: []byte("id-a"), Value: &pb.DataEffect_Raw{Raw: []byte("a")}},
				ForkChoiceHash: ComputeForkChoiceHash(1, sTs(10))},
			{Data: &pb.DataEffect{Id: []byte("id-b"), Value: &pb.DataEffect_Raw{Raw: []byte("b")}},
				ForkChoiceHash: ComputeForkChoiceHash(1, sTs(20))},
		},
		ForkChoiceHash: ComputeForkChoiceHash(1, sTs(10)),
		Commutative:    true,
	}
	branchB := &pb.ReducedEffect{
		Op:         pb.EffectOp_INSERT_OP,
		Collection: pb.CollectionKind_ORDERED,
		Hlc:        sTs(35),
		NodeId:     2,
		OrderedElements: []*pb.ReducedElement{
			{Data: &pb.DataEffect{Id: []byte("id-c"), Value: &pb.DataEffect_Raw{Raw: []byte("c")}},
				ForkChoiceHash: ComputeForkChoiceHash(2, sTs(15))},
			{Data: &pb.DataEffect{Id: []byte("id-d"), Value: &pb.DataEffect_Raw{Raw: []byte("d")}},
				ForkChoiceHash: ComputeForkChoiceHash(2, sTs(25))},
		},
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(15)),
		Commutative:    true,
	}

	r1 := Merge2(branchA, branchB)
	r2 := Merge2(branchB, branchA)

	got1 := orderedValues(r1)
	got2 := orderedValues(r2)
	if !strSliceEqual(got1, got2) {
		t.Fatalf("Merge2 not commutative:\n  Merge2(A,B): %v\n  Merge2(B,A): %v", got1, got2)
	}

	pos := positionMap(got1)
	if pos["a"] >= pos["b"] {
		t.Errorf("branch A order broken in %v", got1)
	}
	if pos["c"] >= pos["d"] {
		t.Errorf("branch B order broken in %v", got1)
	}
}

// --- Transaction filtering ---

func TestCrossTxnDepsMustNotConfirm(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	// Non-transactional root
	root := log.putEffect(&pb.Effect{
		Key:            []byte("k"),
		Hlc:            sTs(10),
		NodeId:         1,
		ForkChoiceHash: ComputeForkChoiceHash(1, sTs(10)),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Collection: pb.CollectionKind_ORDERED,
			Placement:  pb.Placement_PLACE_TAIL,
			Id:         []byte("root"),
			Value:      &pb.DataEffect_Raw{Raw: []byte("root")},
		}},
	})

	// Transaction A writes (tentative — no bind)
	log.putEffect(&pb.Effect{
		Key:            []byte("k"),
		Hlc:            sTs(20),
		NodeId:         2,
		TxnId:          "txn-A",
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(20)),
		Deps:           []*pb.EffectRef{toPbRef(root)},
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Collection: pb.CollectionKind_ORDERED,
			Placement:  pb.Placement_PLACE_TAIL,
			Id:         []byte("a-val"),
			Value:      &pb.DataEffect_Raw{Raw: []byte("from-A")},
		}},
	})

	// Transaction B writes, depending on A's tentative write
	txBWrite := log.putEffect(&pb.Effect{
		Key:            []byte("k"),
		Hlc:            sTs(30),
		NodeId:         3,
		TxnId:          "txn-B",
		ForkChoiceHash: ComputeForkChoiceHash(3, sTs(30)),
		Deps:           []*pb.EffectRef{toPbRef(root)},
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Collection: pb.CollectionKind_ORDERED,
			Placement:  pb.Placement_PLACE_TAIL,
			Id:         []byte("b-val"),
			Value:      &pb.DataEffect_Raw{Raw: []byte("from-B")},
		}},
	})

	// Transaction B's bind (confirmed)
	txBBind := log.putEffect(&pb.Effect{
		Key:            []byte("k"),
		Hlc:            sTs(40),
		NodeId:         3,
		TxnId:          "txn-B",
		ForkChoiceHash: ComputeForkChoiceHash(3, sTs(40)),
		Deps:           []*pb.EffectRef{toPbRef(txBWrite)},
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			Keys: []*pb.TransactionalBindEffect_KeyBind{{
				Key:          []byte("k"),
				ConsumedTips: []*pb.EffectRef{toPbRef(root)},
				NewTip:       toPbRef(txBWrite),
			}},
			OriginatorNodeId: 3,
			TxnHlc:           sTs(30),
		}},
	})

	e.index.Insert("k", nil, keytrie.NewTipSet(txBBind))
	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	got := orderedValues(r)

	// "from-A" must NOT appear — txn A has no bind
	for _, v := range got {
		if v == "from-A" {
			t.Fatalf("tentative effect from txn-A was visible; got %v", got)
		}
	}
	// "root" and "from-B" must appear
	hasRoot, hasB := false, false
	for _, v := range got {
		if v == "root" {
			hasRoot = true
		}
		if v == "from-B" {
			hasB = true
		}
	}
	if !hasRoot || !hasB {
		t.Fatalf("expected [root, from-B], got %v", got)
	}
}

// TestLCA_SnapshotOnOneBranchMustNotSkipOtherBranch verifies that a
// snapshot reachable from multiple paths doesn't prematurely terminate
// the BFS when another path bypasses it through effects the snapshot
// doesn't cover.
//
// DAG (two tips):
//
//	R(+10) → Y(+7) → B(+2) → tip2 (deps=[A, B])
//	   └──→ S(snap=10) → A(+1) ↗
//	                  └→ tip1 (+3)
//
// S covers only R (state=10). Y is concurrent with S, on the R→B path.
// The true LCA is R, not S. If S is selected as LCA, Y is lost.
// Correct: 10 + 7 + 2 + 1 + 3 = 23
func TestLCA_SnapshotOnOneBranchMustNotSkipOtherBranch(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	root := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 10)},
	})

	// S(snapshot, state=10, deps=[R])
	snap := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(root)},
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
			Collection: pb.CollectionKind_SCALAR,
			State: &pb.ReducedEffect{
				Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_ADDITIVE_INT,
				Collection: pb.CollectionKind_SCALAR, Commutative: true,
				Hlc: sTs(10), NodeId: 1,
				Scalar: &pb.DataEffect{
					Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_ADDITIVE_INT,
					Value: &pb.DataEffect_IntVal{IntVal: 10},
				},
			},
		}},
	})

	// tip1(+3, deps=[S])
	tip1 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(30), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(snap)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 3)},
	})

	// A(+1, deps=[S])
	effA := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(31), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(snap)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	// Y(+7, deps=[R]) — concurrent with S, not covered by S
	effY := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(25), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(root)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 7)},
	})

	// B(+2, deps=[Y])
	effB := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(35), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(effY)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 2)},
	})

	// tip2(+0, deps=[A, B]) — merge node
	tip2 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(40), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(effA), toPbRef(effB)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 0)},
	})

	e.index.Insert("k", nil, keytrie.NewTipSet(tip1, tip2))
	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	// R(10) + Y(7) + A(1) + B(2) + tip1(3) + tip2(0) = 23
	if r.Scalar.GetIntVal() != 23 {
		t.Fatalf("expected 23, got %d (snapshot on one branch skipped Y on the other)", r.Scalar.GetIntVal())
	}
}

// TestCompetingBinds_FirstWins verifies that when two transactions compete
// (share consumed tips on the same key), the first in fork-choice order
// (lower hash) wins and the loser's effects are not visible.
//
// DAG:
//
//	root(+10)
//	├── txA_write(+1, txn=A) ── bindA(txn=A, consumed=[root], newTip=txA_write)
//	└── txB_write(+2, txn=B) ── bindB(txn=B, consumed=[root], newTip=txB_write)
//
// Both transactions consume the same tip (root). Only one should win.
// The winner's effects should be visible; the loser's should not.
func TestCompetingBinds_FirstWins(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	root := log.putEffect(&pb.Effect{
		Key:            []byte("k"),
		Hlc:            sTs(10),
		NodeId:         1,
		ForkChoiceHash: ComputeForkChoiceHash(1, sTs(10)),
		Kind:           &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 10)},
	})

	// Transaction A: write +1
	txAWrite := log.putEffect(&pb.Effect{
		Key:            []byte("k"),
		Hlc:            sTs(20),
		NodeId:         2,
		TxnId:          "txn-A",
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(20)),
		Deps:           []*pb.EffectRef{toPbRef(root)},
		Kind:           &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	bindA := log.putEffect(&pb.Effect{
		Key:            []byte("k"),
		Hlc:            sTs(25),
		NodeId:         2,
		TxnId:          "txn-A",
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(25)),
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			Keys: []*pb.TransactionalBindEffect_KeyBind{{
				Key:          []byte("k"),
				ConsumedTips: []*pb.EffectRef{toPbRef(root)},
				NewTip:       toPbRef(txAWrite),
			}},
			OriginatorNodeId: 2,
			TxnHlc:           sTs(20),
		}},
	})

	// Transaction B: write +2
	txBWrite := log.putEffect(&pb.Effect{
		Key:            []byte("k"),
		Hlc:            sTs(30),
		NodeId:         3,
		TxnId:          "txn-B",
		ForkChoiceHash: ComputeForkChoiceHash(3, sTs(30)),
		Deps:           []*pb.EffectRef{toPbRef(root)},
		Kind:           &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 2)},
	})
	bindB := log.putEffect(&pb.Effect{
		Key:            []byte("k"),
		Hlc:            sTs(35),
		NodeId:         3,
		TxnId:          "txn-B",
		ForkChoiceHash: ComputeForkChoiceHash(3, sTs(35)),
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			Keys: []*pb.TransactionalBindEffect_KeyBind{{
				Key:          []byte("k"),
				ConsumedTips: []*pb.EffectRef{toPbRef(root)},
				NewTip:       toPbRef(txBWrite),
			}},
			OriginatorNodeId: 3,
			TxnHlc:           sTs(30),
		}},
	})

	e.index.Insert("k", nil, keytrie.NewTipSet(bindA, bindB))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}

	// Determine which transaction wins fork-choice (lower hash)
	hashA := ComputeForkChoiceHash(2, sTs(25))
	hashB := ComputeForkChoiceHash(3, sTs(35))
	var expectedVal int64
	if ForkChoiceLess(hashA, hashB) {
		expectedVal = 11 // root(10) + txA(1)
	} else {
		expectedVal = 12 // root(10) + txB(2)
	}

	if r.Scalar.GetIntVal() != expectedVal {
		t.Fatalf("expected %d (only winner visible), got %d (both txns applied = %d)",
			expectedVal, r.Scalar.GetIntVal(), 10+1+2)
	}
}

// TestCompetingBinds_LoserDependentsInvisible verifies that non-txn effects
// written after the losing transaction are still visible (they depend on the
// bind structurally but are independent data writes).
//
// DAG:
//
//	root(+10)
//	├── txA(+1, txn=A) ── bindA ── afterA(+100, non-txn)
//	└── txB(+2, txn=B) ── bindB ── afterB(+200, non-txn)
//
// Only the winning bind's txn effects should be visible. Both afterA and
// afterB are non-transactional and should be visible regardless.
func TestCompetingBinds_LoserDependentsInvisible(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	root := log.putEffect(&pb.Effect{
		Key:            []byte("k"),
		Hlc:            sTs(10),
		NodeId:         1,
		ForkChoiceHash: ComputeForkChoiceHash(1, sTs(10)),
		Kind:           &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 10)},
	})

	// Transaction A: write +1, bind, then non-txn +100 after bind
	txAWrite := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 2, TxnId: "txn-A",
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(20)),
		Deps:           []*pb.EffectRef{toPbRef(root)},
		Kind:           &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	bindA := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(25), NodeId: 2, TxnId: "txn-A",
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(25)),
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			Keys: []*pb.TransactionalBindEffect_KeyBind{{
				Key:          []byte("k"),
				ConsumedTips: []*pb.EffectRef{toPbRef(root)},
				NewTip:       toPbRef(txAWrite),
			}},
			OriginatorNodeId: 2, TxnHlc: sTs(20),
		}},
	})
	afterA := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(26), NodeId: 2,
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(26)),
		Deps:           []*pb.EffectRef{toPbRef(bindA)},
		Kind:           &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 100)},
	})

	// Transaction B: write +2, bind, then non-txn +200 after bind
	txBWrite := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(30), NodeId: 3, TxnId: "txn-B",
		ForkChoiceHash: ComputeForkChoiceHash(3, sTs(30)),
		Deps:           []*pb.EffectRef{toPbRef(root)},
		Kind:           &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 2)},
	})
	bindB := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(35), NodeId: 3, TxnId: "txn-B",
		ForkChoiceHash: ComputeForkChoiceHash(3, sTs(35)),
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			Keys: []*pb.TransactionalBindEffect_KeyBind{{
				Key:          []byte("k"),
				ConsumedTips: []*pb.EffectRef{toPbRef(root)},
				NewTip:       toPbRef(txBWrite),
			}},
			OriginatorNodeId: 3, TxnHlc: sTs(30),
		}},
	})
	afterB := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(36), NodeId: 3,
		ForkChoiceHash: ComputeForkChoiceHash(3, sTs(36)),
		Deps:           []*pb.EffectRef{toPbRef(bindB)},
		Kind:           &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 200)},
	})

	e.index.Insert("k", nil, keytrie.NewTipSet(afterA, afterB))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}

	// Winner's txn effect + both non-txn effects should be visible
	hashA := ComputeForkChoiceHash(2, sTs(25))
	hashB := ComputeForkChoiceHash(3, sTs(35))
	var expectedVal int64
	if ForkChoiceLess(hashA, hashB) {
		expectedVal = 10 + 1 + 100 + 200 // root + txA(winner) + afterA + afterB
	} else {
		expectedVal = 10 + 2 + 100 + 200 // root + txB(winner) + afterA + afterB
	}

	if r.Scalar.GetIntVal() != expectedVal {
		t.Fatalf("expected %d, got %d", expectedVal, r.Scalar.GetIntVal())
	}
}

// TestCompetingBinds_NonOverlappingPredicatesBothVisible verifies that two
// transactions sharing consumed tips but with non-overlapping predicates
// both survive reconstruction. Predicate refinement at commit time allows
// both to coexist; the read path must agree.
//
// TxA observes column 0 == 1 and writes column 0 = 10.
// TxB observes column 0 == 2 and writes column 0 = 20.
// Their predicates don't intersect, so both should be visible.
func TestCompetingBinds_NonOverlappingPredicatesBothVisible(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	root := log.putEffect(&pb.Effect{
		Key:            []byte("k"),
		Hlc:            sTs(10),
		NodeId:         1,
		ForkChoiceHash: ComputeForkChoiceHash(1, sTs(10)),
		Kind:           &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 10)},
	})

	// Transaction A: observation + row-write + data + bind
	txAObs := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 2, TxnId: "txn-A",
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(20)),
		Deps:           []*pb.EffectRef{toPbRef(root)},
		Kind: &pb.Effect_Observation{Observation: &pb.ObservationEffect{
			Predicate: &pb.Predicate{
				Kind:    pb.Predicate_CMP,
				Col:     0,
				Op:      pb.PredicateCmpOp_CMP_EQ,
				Literal: &pb.TypedValue{Kind: pb.TypedValue_INT, IntVal: 1},
			},
		}},
	})
	txARW := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(21), NodeId: 2, TxnId: "txn-A",
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(21)),
		Deps:           []*pb.EffectRef{toPbRef(txAObs)},
		Kind: &pb.Effect_RowWrite{RowWrite: &pb.RowWriteEffect{
			Kind:    pb.RowWriteEffect_INSERT,
			Columns: []*pb.TypedValue{{Kind: pb.TypedValue_INT, IntVal: 10}},
		}},
	})
	txAData := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(22), NodeId: 2, TxnId: "txn-A",
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(22)),
		Deps:           []*pb.EffectRef{toPbRef(txARW)},
		Kind:           &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	bindA := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(23), NodeId: 2, TxnId: "txn-A",
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(23)),
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			Keys: []*pb.TransactionalBindEffect_KeyBind{{
				Key:          []byte("k"),
				ConsumedTips: []*pb.EffectRef{toPbRef(root)},
				NewTip:       toPbRef(txAData),
			}},
			OriginatorNodeId: 2, TxnHlc: sTs(20),
		}},
	})

	// Transaction B: observation + row-write + data + bind
	// Different predicate: observes col 0 == 2 (non-overlapping with A's col 0 == 1)
	txBObs := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(30), NodeId: 3, TxnId: "txn-B",
		ForkChoiceHash: ComputeForkChoiceHash(3, sTs(30)),
		Deps:           []*pb.EffectRef{toPbRef(root)},
		Kind: &pb.Effect_Observation{Observation: &pb.ObservationEffect{
			Predicate: &pb.Predicate{
				Kind:    pb.Predicate_CMP,
				Col:     0,
				Op:      pb.PredicateCmpOp_CMP_EQ,
				Literal: &pb.TypedValue{Kind: pb.TypedValue_INT, IntVal: 2},
			},
		}},
	})
	txBRW := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(31), NodeId: 3, TxnId: "txn-B",
		ForkChoiceHash: ComputeForkChoiceHash(3, sTs(31)),
		Deps:           []*pb.EffectRef{toPbRef(txBObs)},
		Kind: &pb.Effect_RowWrite{RowWrite: &pb.RowWriteEffect{
			Kind:    pb.RowWriteEffect_INSERT,
			Columns: []*pb.TypedValue{{Kind: pb.TypedValue_INT, IntVal: 20}},
		}},
	})
	txBData := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(32), NodeId: 3, TxnId: "txn-B",
		ForkChoiceHash: ComputeForkChoiceHash(3, sTs(32)),
		Deps:           []*pb.EffectRef{toPbRef(txBRW)},
		Kind:           &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 2)},
	})
	bindB := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(33), NodeId: 3, TxnId: "txn-B",
		ForkChoiceHash: ComputeForkChoiceHash(3, sTs(33)),
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			Keys: []*pb.TransactionalBindEffect_KeyBind{{
				Key:          []byte("k"),
				ConsumedTips: []*pb.EffectRef{toPbRef(root)},
				NewTip:       toPbRef(txBData),
			}},
			OriginatorNodeId: 3, TxnHlc: sTs(30),
		}},
	})

	e.index.Insert("k", nil, keytrie.NewTipSet(bindA, bindB))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}

	// Both transactions should be visible: root(10) + txA(1) + txB(2) = 13
	if r.Scalar.GetIntVal() != 13 {
		t.Fatalf("expected 13 (both txns visible, non-overlapping predicates), got %d",
			r.Scalar.GetIntVal())
	}
}

// TestCompaction_SnapshotAppearsInDAG verifies that after enough effects
// are emitted to trigger compaction, a snapshot effect actually appears
// in the DAG and the walker finds it as the LCA on the next read.
func TestCompaction_SnapshotAppearsInDAG(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	// Emit 60 effects via Context to guarantee compaction triggers
	// (threshold is 20+rand(31), max 50).
	for i := range 60 {
		ctx := e.NewContext()
		if err := ctx.Emit(&pb.Effect{
			Key:  []byte("k"),
			Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
		}); err != nil {
			t.Fatalf("emit %d: %v", i, err)
		}
		if err := ctx.Flush(); err != nil {
			t.Fatalf("flush %d: %v", i, err)
		}
	}

	// Next GetSnapshot should trigger compaction and emit a snapshot.
	ctx := e.NewContext()
	r, _, err := ctx.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	if r == nil || r.Scalar.GetIntVal() != 60 {
		t.Fatalf("expected 60, got %v", r)
	}
	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	// Verify the snapshot is in the DAG by walking from the current tips.
	// The walker should find it as LCA.
	tips := e.index.Contains("k")
	if tips == nil {
		t.Fatal("no tips in index")
	}
	d := newDag(e, "k", "")
	d.visited = make(map[Tip]bool, 64)
	d.nodes = make(map[Tip]*pb.Effect, 64)
	if err := d.bfs(tips.Tips(), &d.lcaTip); err != nil {
		t.Fatal(err)
	}

	// Check that we collected a snapshot node
	hasSnapshot := false
	for _, eff := range d.nodes {
		if eff.GetSnapshot() != nil {
			hasSnapshot = true
			break
		}
	}
	if !hasSnapshot {
		t.Fatal("no snapshot found in collected nodes after compaction")
	}

	var zero Tip
	if d.lcaTip == zero {
		// Dump the encoded DAG for debugging
		t.Fatalf("snapshot exists in DAG but was not detected as LCA\nencoded: %s", d.encode(tips.Tips()))
	}

	t.Logf("LCA found at %v, collected %d nodes", d.lcaTip, len(d.nodes))
}

func TestCompaction_BeaconPattern(t *testing.T) {

	log := newSnapshotLog()
	e := newSnapshotEngine(log)

	nodeIDBytes := func(id uint64) []byte {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, id)
		return b
	}

	key := "__swytch:members"

	// Initial registration: KEYED insert + type tag + TTL meta
	{
		ctx := e.NewContext()
		if err := ctx.Emit(&pb.Effect{
			Key: []byte(key),
			Kind: &pb.Effect_Data{Data: &pb.DataEffect{
				Op:         pb.EffectOp_INSERT_OP,
				Merge:      pb.MergeRule_LAST_WRITE_WINS,
				Collection: pb.CollectionKind_KEYED,
				Id:         nodeIDBytes(1),
				Value:      &pb.DataEffect_Raw{Raw: []byte("127.0.0.1:9000")},
			}},
		}); err != nil {
			t.Fatal(err)
		}
		if err := ctx.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_HASH}},
		}); err != nil {
			t.Fatal(err)
		}
		if err := ctx.Emit(&pb.Effect{
			Key: []byte(key),
			Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{
				ElementId: nodeIDBytes(1),
				ExpiresAt: sTs(999999999),
			}},
		}); err != nil {
			t.Fatal(err)
		}
		if err := ctx.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	// Beacon refresh loop: GetSnapshot → meta TTL refresh → flush
	for i := range 80 {
		ctx := e.NewContext()
		_, _, err := ctx.GetSnapshot(key)
		if err != nil {
			t.Fatalf("tick %d GetSnapshot: %v", i, err)
		}
		if err := ctx.Emit(&pb.Effect{
			Key: []byte(key),
			Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{
				ElementId: nodeIDBytes(1),
				ExpiresAt: sTs(999999999),
			}},
		}); err != nil {
			t.Fatalf("tick %d Emit: %v", i, err)
		}
		if err := ctx.Flush(); err != nil {
			t.Fatalf("tick %d Flush: %v", i, err)
		}
	}

	// Plant a no-deps subscription effect from a "remote" node directly
	// into log + index AFTER all the ticks. It lands as a sibling tip
	// alongside the data chain — the same shape we get when a peer's
	// first subscription arrives via HandleRemote at a moment when the
	// local context isn't mid-flight to consume it on the next emit.
	{
		eff := &pb.Effect{
			Key:    []byte(key),
			Hlc:    sTs(1),
			NodeId: 2,
			Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
				SubscriberNodeId: 2,
			}},
		}
		eff.ForkChoiceHash = ComputeForkChoiceHash(2, eff.Hlc)
		oldNID := log.nodeID
		log.nodeID = 2
		subTip := log.putEffect(eff)
		log.nodeID = oldNID
		mu := e.GetLock(key)
		mu.Lock()
		e.updateIndex(key, nil, subTip)
		mu.Unlock()
		t.Logf("planted remote subscription at %v after %d ticks", subTip, 80)
	}

	// Check: is a snapshot in the DAG?
	tips := e.index.Contains(key)
	if tips == nil {
		t.Fatal("no tips in index")
	}
	d := newDag(e, key, "")
	d.visited = make(map[Tip]bool, 256)
	d.nodes = make(map[Tip]*pb.Effect, 256)
	if err := d.bfs(tips.Tips(), &d.lcaTip); err != nil {
		t.Fatal(err)
	}

	hasSnapshot := false
	for _, eff := range d.nodes {
		if eff.GetSnapshot() != nil {
			hasSnapshot = true
			break
		}
	}

	var zero Tip
	if !hasSnapshot {
		t.Fatalf("no snapshot in DAG after 80 ticks\ntips: %v\nnodes: %d\nencoded: %s",
			tips.Tips(), len(d.nodes), d.encode(tips.Tips()))
	}
	if d.lcaTip == zero {
		t.Fatalf("snapshot exists but not detected as LCA\nnodes: %d\nencoded: %s",
			len(d.nodes), d.encode(tips.Tips()))
	}
	t.Logf("LCA at %v, %d nodes", d.lcaTip, len(d.nodes))
}
