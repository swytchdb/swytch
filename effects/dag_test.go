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

	pb "github.com/swytchdb/cache/cluster/proto"
	"github.com/swytchdb/cache/keytrie"
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
	e := newSnapshotEngine(log, nil)

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
	e := newSnapshotEngine(log, nil)

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
	e := newSnapshotEngine(log, nil)

	a := orderedAppend(log, 10, 1, "id-a", []byte("a"))
	b := orderedAppend(log, 20, 2, "id-b", []byte("b"), a)
	c := orderedAppend(log, 30, 1, "id-c", []byte("c"), b)

	// Initial: linear [a, b, c]
	r1, _, err := e.reconstruct("k", []Tip{c})
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
	r2, _, err := e.reconstruct("k", []Tip{c, ee})
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
	e1 := newSnapshotEngine(log, nil)
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
	e2 := newSnapshotEngine(log, nil)
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
	e := newSnapshotEngine(log, nil)

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
	e := newSnapshotEngine(log, nil)

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
	e1 := newSnapshotEngine(log, nil)
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
	e2 := newSnapshotEngine(log, nil)
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
	e := newSnapshotEngine(log, nil)

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
