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
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	clox "github.com/swytchdb/swytch/cache"
	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/crdt"
	"github.com/swytchdb/swytch/keytrie"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func sTs(nanos int64) *timestamppb.Timestamp {
	return timestamppb.New(time.Unix(0, nanos))
}

// --- mock cache ---

type mockCache struct {
	data map[string]*pb.ReducedEffect
}

func newMockCache() *mockCache {
	return &mockCache{data: make(map[string]*pb.ReducedEffect)}
}

func (c *mockCache) Get(key string) (*pb.ReducedEffect, bool) {
	v, ok := c.data[key]
	return v, ok
}

func (c *mockCache) Put(key string, value *pb.ReducedEffect) {
	c.data[key] = value
}

func (c *mockCache) Evict(key string) {
	delete(c.data, key)
}

// --- snapshotLog: test helper for pre-populating the effect DAG ---

type snapshotLog struct {
	effectCache *clox.CloxCache[Tip, *pb.Effect]
	entries     map[Tip][]byte // raw proto bytes, for tests that inspect wire data
	nextOff     uint64
	nodeID      uint64
}

func newSnapshotLog() *snapshotLog {
	return &snapshotLog{
		effectCache: clox.NewCloxCache[Tip, *pb.Effect](clox.ConfigFromMemorySize(1024 * 1024)),
		entries:     make(map[Tip][]byte),
		nextOff:     100,
		nodeID:      1,
	}
}

// putEffect serialises eff, stores it in both the raw entries map and the
// effectCache, advances nextOff by 100, and returns the Tip assigned.
func (l *snapshotLog) putEffect(eff *pb.Effect) Tip {
	if len(eff.ForkChoiceHash) == 0 {
		eff.ForkChoiceHash = ComputeForkChoiceHash(pb.NodeID(eff.NodeId), eff.Hlc)
	}
	off := l.nextOff
	l.nextOff += 100
	t := Tip{l.nodeID, off}

	data, _ := proto.Marshal(eff)
	l.entries[t] = data
	l.effectCache.Put(t, proto.Clone(eff).(*pb.Effect))
	return t
}

// --- helpers ---

func newSnapshotEngine(log *snapshotLog, cache StateCache) *Engine {
	var ec *clox.CloxCache[Tip, *pb.Effect]
	if log != nil {
		ec = log.effectCache
	} else {
		ec = clox.NewCloxCache[Tip, *pb.Effect](clox.ConfigFromMemorySize(1024 * 1024))
	}
	e := &Engine{
		index:             keytrie.New(),
		cache:             cache,
		effectCache:       ec,
		nodeID:            1,
		clock:             crdt.NewHLC(),
		subscriptions:     xsync.NewMap[string, *subscriptionState](),
		pendingTxns:       xsync.NewMap[Tip, *pendingTxn](),
		pendingTxTips:     xsync.NewMap[Tip, []Tip](),
		txAbortCounts:     xsync.NewMap[string, *atomic.Int32](),
		pendingBootstraps: xsync.NewMap[string, *bootstrapCollector](),
		unsubInFlight:     xsync.NewMap[string, struct{}](),
		voidedBinds:       clox.NewCloxCache[string, struct{}](clox.ConfigFromCapacity(256)),
	}
	e.safety.Store(&safetyMap{defaultMode: UnsafeMode})
	return e
}

// --- GetSnapshot tests ---

func TestGetSnapshot_MissReturnsNil(t *testing.T) {
	e := newSnapshotEngine(nil, nil)

	r, _, _, err := e.GetSnapshot("missing")
	if err != nil {
		t.Fatal(err)
	}
	if r != nil {
		t.Fatal("expected nil for missing key")
	}
}

func TestGetSnapshot_SingleEffect(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	off := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("hello"))},
	})
	e.index.Insert("k", nil, keytrie.NewTipSet(off))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("expected non-nil")
	}
	if string(r.Scalar.GetRaw()) != "hello" {
		t.Fatalf("expected 'hello', got %q", r.Scalar.GetRaw())
	}
}

func TestGetSnapshot_LinearChain(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	off1 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("first"))},
	})
	off2 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(off1)},
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("second"))},
	})
	off3 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(30), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(off2)},
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("third"))},
	})
	e.index.Insert("k", nil, keytrie.NewTipSet(off3))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	if string(r.Scalar.GetRaw()) != "third" {
		t.Fatalf("expected 'third', got %q", r.Scalar.GetRaw())
	}
}

func TestGetSnapshot_LinearAdditiveChain(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	off1 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 5)},
	})
	off2 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(off1)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 3)},
	})
	off3 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(30), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(off2)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 2)},
	})
	e.index.Insert("k", nil, keytrie.NewTipSet(off3))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	if r.Scalar.GetIntVal() != 10 {
		t.Fatalf("expected 10, got %d", r.Scalar.GetIntVal())
	}
}

func TestGetSnapshot_ForkLWW(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	// root → branch A (HLC=20) and branch B (HLC=30)
	root := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("base"))},
	})
	tipA := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(root)},
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("branchA"))},
	})
	tipB := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(30), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(root)},
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("branchB"))},
	})
	e.index.Insert("k", nil, keytrie.NewTipSet(tipA, tipB))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	// Higher fork_choice_hash wins (applied last in linearized reduction)
	hashA := ComputeForkChoiceHash(1, sTs(20))
	hashB := ComputeForkChoiceHash(2, sTs(30))
	expected := "branchB"
	if ForkChoiceLess(hashB, hashA) {
		expected = "branchA"
	}
	if string(r.Scalar.GetRaw()) != expected {
		t.Fatalf("expected %q (higher hash), got %q", expected, r.Scalar.GetRaw())
	}
}

func TestGetSnapshot_ForkAdditiveCorrect(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	// root(+5) → branchA(+3) and branchB(+2)
	// Correct result: 5 + 3 + 2 = 10 (NOT 8+7=15 from naive approach)
	root := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 5)},
	})
	tipA := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(root)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 3)},
	})
	tipB := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(30), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(root)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 2)},
	})
	e.index.Insert("k", nil, keytrie.NewTipSet(tipA, tipB))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	if r.Scalar.GetIntVal() != 10 {
		t.Fatalf("expected 10, got %d", r.Scalar.GetIntVal())
	}
}

func TestGetSnapshot_ForkKeyedUnion(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	// root: HSET f0=v0 → branchA: HSET f1=v1, branchB: HSET f2=v2
	root := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: keyedInsertRaw("f0", []byte("v0"))},
	})
	tipA := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(root)},
		Kind: &pb.Effect_Data{Data: keyedInsertRaw("f1", []byte("v1"))},
	})
	tipB := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(30), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(root)},
		Kind: &pb.Effect_Data{Data: keyedInsertRaw("f2", []byte("v2"))},
	})
	e.index.Insert("k", nil, keytrie.NewTipSet(tipA, tipB))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	// Should have all 3 fields
	if len(r.NetAdds) != 3 {
		t.Fatalf("expected 3 fields, got %d", len(r.NetAdds))
	}
	for _, f := range []string{"f0", "f1", "f2"} {
		if _, ok := r.NetAdds[f]; !ok {
			t.Fatalf("missing field %s", f)
		}
	}
}

func TestGetSnapshot_MergePointInChain(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	// root → A, root → B, C(deps:[A,B]) → D [single tip]
	// Tests that a resolved fork within a single-tip chain works.
	root := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	a := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(root)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 2)},
	})
	b := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(25), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(root)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 3)},
	})
	// C resolves the fork (depends on both A and B)
	c := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(30), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(a), toPbRef(b)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 4)},
	})
	// D continues linearly
	d := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(40), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(c)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 5)},
	})
	e.index.Insert("k", nil, keytrie.NewTipSet(d))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	// root(1) + branches(A=2, B=3 merged via MergeN) + C(4) + D(5)
	// reduceNode(root)=1. reduceNode(A)=chain(1,[A])=3. reduceNode(B)=chain(1,[B])=4.
	// reduceNode(C): MergeN([3,4])=7, chain(7,[C])=11.
	// reduceNode(D): chain(11,[D])=16.
	// Wait: MergeN of two additive branches: 3+4=7. Then chain(7,[+4])=11. chain(11,[+5])=16.
	// But correct should be 1+2+3+4+5=15.
	// The issue: MergeN([3,4]) sums the fully-reduced branches which include root.
	// reduceNode(A)=ReduceChain(reduceNode(root),[A])=ReduceChain(1,[+2])=3
	// reduceNode(B)=ReduceChain(reduceNode(root),[B])=ReduceChain(1,[+3])=4
	// MergeN([3,4])=7. But root is counted twice.
	// reduceNode handles this through the multi-dep path which merges concurrent branches.
	// The concurrent branches ARE A and B, both of which include the root.
	// This is the "double-counting" issue for additive forks within a single tip.
	// The correct result would require LCA-aware reduction, but reduceNode
	// at the merge point just MergeN's the deps.
	//
	// For this test: since this is a merge point within a single-tip chain,
	// reduceNode processes it. The result is 7+4+5=16, which double-counts root.
	// This is a known limitation for additive values at internal merge points.
	// LWW values don't have this issue since the winner overwrites.
	//
	// Adjusting the test to use LWW to verify correctness:
	_ = d // suppress unused
	// Actually, let me test what we get and document it.
	// The internal reduceNode merges A and B as concurrent branches,
	// which sums their full reductions (including shared root).
	// Correct: root(1) + A(2) + B(3) + C(4) + D(5) = 15
	// mergeDeps uses LCA + inclusion-exclusion to avoid double-counting root.
	if r.Scalar.GetIntVal() != 15 {
		t.Fatalf("expected 15, got %d", r.Scalar.GetIntVal())
	}
}

func TestGetSnapshot_MergePointLWW(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	// root → A, root → B, C(deps:[A,B]) → D [single tip]
	// LWW: merge picks HLC winner, no double-counting issue.
	root := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("root"))},
	})
	a := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(root)},
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("A"))},
	})
	b := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(25), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(root)},
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("B"))},
	})
	c := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(30), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(a), toPbRef(b)},
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("C"))},
	})
	d := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(40), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(c)},
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("D"))},
	})
	e.index.Insert("k", nil, keytrie.NewTipSet(d))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	if string(r.Scalar.GetRaw()) != "D" {
		t.Fatalf("expected 'D', got %q", r.Scalar.GetRaw())
	}
}

func TestGetSnapshot_SnapshotEffectStopsWalk(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	// snapshot(state="snap") → A("after")
	snapOff := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
			Collection: pb.CollectionKind_SCALAR,
			State: &pb.ReducedEffect{
				Op:         pb.EffectOp_INSERT_OP,
				Merge:      pb.MergeRule_LAST_WRITE_WINS,
				Collection: pb.CollectionKind_SCALAR,
				Hlc:        sTs(10), NodeId: 1,
				Scalar: &pb.DataEffect{
					Op:    pb.EffectOp_INSERT_OP,
					Merge: pb.MergeRule_LAST_WRITE_WINS,
					Value: &pb.DataEffect_Raw{Raw: []byte("snap")},
				},
			},
		}},
	})
	tip := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(snapOff)},
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("after"))},
	})
	e.index.Insert("k", nil, keytrie.NewTipSet(tip))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	// "after" overwrites "snap" via LWW (higher HLC)
	if string(r.Scalar.GetRaw()) != "after" {
		t.Fatalf("expected 'after', got %q", r.Scalar.GetRaw())
	}
}

// TestGetSnapshot_LinearSnapshotSeed tests that a snapshot on a linear chain
// is used as the seed for reduction. Without this, stopping the walk at the
// snapshot means the effects behind it are lost and additive counters under-count.
//
// DAG (linear, single tip):
//
//	E1(+1) → E2(+1) → E3(+1) → snapshot(state=3) → E4(+1) → E5(+1)
//
// Correct: 3 (snapshot) + 1 (E4) + 1 (E5) = 5
// Bug (no seed): 0 + 1 (E4) + 1 (E5) = 2
func TestGetSnapshot_LinearSnapshotSeed(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	// Build prefix: E1(+1) → E2(+1) → E3(+1)
	e1 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	e2 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e1)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	e3 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(30), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e2)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	// Snapshot materializing E1+E2+E3 = 3
	snap := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(31), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e3)},
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
			Collection: pb.CollectionKind_SCALAR,
			State: &pb.ReducedEffect{
				Op:          pb.EffectOp_INSERT_OP,
				Merge:       pb.MergeRule_ADDITIVE_INT,
				Collection:  pb.CollectionKind_SCALAR,
				Commutative: true,
				Hlc:         sTs(30), NodeId: 1,
				Scalar: &pb.DataEffect{
					Op:    pb.EffectOp_INSERT_OP,
					Merge: pb.MergeRule_ADDITIVE_INT,
					Value: &pb.DataEffect_IntVal{IntVal: 3},
				},
			},
		}},
	})

	// E4(+1) → E5(+1) on top of snapshot
	e4 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(40), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(snap)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	tip := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(50), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e4)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	e.index.Insert("k", nil, keytrie.NewTipSet(tip))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	if r.Scalar.GetIntVal() != 5 {
		t.Fatalf("expected 5, got %d", r.Scalar.GetIntVal())
	}
}

func TestGetSnapshot_CacheHit(t *testing.T) {
	log := newSnapshotLog()
	cache := newMockCache()
	e := newSnapshotEngine(log, cache)

	cached := &pb.ReducedEffect{
		Op:         pb.EffectOp_INSERT_OP,
		Merge:      pb.MergeRule_LAST_WRITE_WINS,
		Collection: pb.CollectionKind_SCALAR,
		Scalar:     &pb.DataEffect{Value: &pb.DataEffect_Raw{Raw: []byte("cached")}},
	}
	cache.Put("k", cached)

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	if string(r.Scalar.GetRaw()) != "cached" {
		t.Fatalf("expected 'cached', got %q", r.Scalar.GetRaw())
	}
}

func TestGetSnapshot_CachePopulated(t *testing.T) {
	log := newSnapshotLog()
	cache := newMockCache()
	e := newSnapshotEngine(log, cache)

	off := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("val"))},
	})
	e.index.Insert("k", nil, keytrie.NewTipSet(off))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("expected non-nil")
	}

	// Check cache was populated
	if _, ok := cache.Get("k"); !ok {
		t.Fatal("expected cache to be populated after GetSnapshot")
	}
}

func TestGetSnapshot_ExpiredKeyReturnsNil(t *testing.T) {
	log := newSnapshotLog()
	cache := newMockCache()
	e := newSnapshotEngine(log, cache)

	// Cached value with past expiry
	cached := &pb.ReducedEffect{
		Op:        pb.EffectOp_INSERT_OP,
		ExpiresAt: sTs(1), // expired (1 ns since epoch)
		Scalar:    &pb.DataEffect{Value: &pb.DataEffect_Raw{Raw: []byte("old")}},
	}
	cache.Put("k", cached)

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	if r != nil {
		t.Fatal("expected nil for expired key")
	}
	// Should have been evicted from cache
	if _, ok := cache.Get("k"); ok {
		t.Fatal("expected expired key to be evicted from cache")
	}
}

func TestGetSnapshot_ExpiredReconstructedStillReturnsState(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	// Data effect + meta with past expiry. GetSnapshot returns the raw
	// causal state — expiry filtering is a presentation concern handled
	// by the protocol layer, not reconstruction.
	off1 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("val"))},
	})
	off2 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(off1)},
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{ExpiresAt: sTs(1)}},
	})
	e.index.Insert("k", nil, keytrie.NewTipSet(off2))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("expected non-nil — raw causal state should be returned regardless of expiry")
	}
}

func TestGetSnapshot_SubscriptionTracked(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	// GetSnapshot should track the subscription
	_, _, _, _ = e.GetSnapshot("newkey")

	if _, ok := e.subscriptions.Load("newkey"); !ok {
		t.Fatal("expected subscription to be recorded")
	}
}

func TestGetSnapshot_SubscriptionBroadcast(t *testing.T) {
	log := newSnapshotLog()
	bc := &mockBroadcaster{peerIDs: []pb.NodeID{10, 20}}
	e := newSnapshotEngine(log, nil)
	e.broadcaster = bc
	bc.nackTarget = e // wire up so bootstrap NACKs arrive and don't block

	_, _, _, _ = e.GetSnapshot("newkey")

	// ensureSubscribed sends ReplicateTo to each peer
	if len(bc.replicateToPeers) != 2 {
		t.Fatalf("expected 2 ReplicateTo calls, got %d", len(bc.replicateToPeers))
	}
	peerSet := map[pb.NodeID]bool{bc.replicateToPeers[0]: true, bc.replicateToPeers[1]: true}
	if !peerSet[10] || !peerSet[20] {
		t.Fatalf("expected ReplicateTo to peers 10 and 20, got %v", bc.replicateToPeers)
	}
}

func TestGetSnapshot_MetaWithData(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	off1 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("hello"))},
	})
	off2 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(off1)},
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{
			TypeTag:   pb.ValueType_TYPE_STRING,
			ExpiresAt: sTs(int64(uint64(1) << 62)), // far future
		}},
	})
	e.index.Insert("k", nil, keytrie.NewTipSet(off2))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	if string(r.Scalar.GetRaw()) != "hello" {
		t.Fatalf("expected 'hello', got %q", r.Scalar.GetRaw())
	}
	if r.TypeTag != pb.ValueType_TYPE_STRING {
		t.Fatalf("expected TYPE_STRING, got %v", r.TypeTag)
	}
}

func TestGetSnapshot_HashMultiField(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	off1 := log.putEffect(&pb.Effect{
		Key: []byte("h"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: keyedInsertRaw("f1", []byte("v1"))},
	})
	off2 := log.putEffect(&pb.Effect{
		Key: []byte("h"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(off1)},
		Kind: &pb.Effect_Data{Data: keyedInsertRaw("f2", []byte("v2"))},
	})
	off3 := log.putEffect(&pb.Effect{
		Key: []byte("h"), Hlc: sTs(30), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(off2)},
		Kind: &pb.Effect_Data{Data: keyedInsertRaw("f1", []byte("v1-updated"))},
	})
	e.index.Insert("h", nil, keytrie.NewTipSet(off3))

	r, _, _, err := e.GetSnapshot("h")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.NetAdds) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(r.NetAdds))
	}
	if string(r.NetAdds["f1"].Data.GetRaw()) != "v1-updated" {
		t.Fatalf("expected f1=v1-updated, got %q", r.NetAdds["f1"].Data.GetRaw())
	}
}

func TestGetSnapshot_OrderedList(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	off1 := log.putEffect(&pb.Effect{
		Key: []byte("l"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "e1", []byte("a"))},
	})
	off2 := log.putEffect(&pb.Effect{
		Key: []byte("l"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(off1)},
		Kind: &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "e2", []byte("b"))},
	})
	off3 := log.putEffect(&pb.Effect{
		Key: []byte("l"), Hlc: sTs(30), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(off2)},
		Kind: &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_HEAD, "e3", []byte("c"))},
	})
	e.index.Insert("l", nil, keytrie.NewTipSet(off3))

	r, _, _, err := e.GetSnapshot("l")
	if err != nil {
		t.Fatal(err)
	}
	if len(r.OrderedElements) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(r.OrderedElements))
	}
	got := make([]string, 3)
	for i, elem := range r.OrderedElements {
		got[i] = string(elem.Data.GetRaw())
	}
	// RPUSH a, RPUSH b, LPUSH c → [c, a, b]
	want := []string{"c", "a", "b"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
}

func TestGetSnapshot_DELReturnsRemoveOp(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	off1 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("val"))},
	})
	off2 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(off1)},
		Kind: &pb.Effect_Data{Data: scalarRemove()},
	})
	e.index.Insert("k", nil, keytrie.NewTipSet(off2))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	if r == nil || r.Op != pb.EffectOp_REMOVE_OP {
		t.Fatal("expected REMOVE_OP after DEL")
	}
}

// --- ReduceChain tests ---

func TestReduceChain_NilSeedDelegatesToReduceBranch(t *testing.T) {
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, scalarInsertRaw([]byte("hello"))),
	}
	r := ReduceChain(nil, effects)
	if r == nil || string(r.Scalar.GetRaw()) != "hello" {
		t.Fatal("ReduceChain(nil, ...) should behave like ReduceBranch")
	}
}

func TestReduceChain_SeedWithEffects(t *testing.T) {
	seed := &pb.ReducedEffect{
		Op:          pb.EffectOp_INSERT_OP,
		Merge:       pb.MergeRule_ADDITIVE_INT,
		Collection:  pb.CollectionKind_SCALAR,
		Commutative: true,
		Scalar: &pb.DataEffect{
			Op:    pb.EffectOp_INSERT_OP,
			Merge: pb.MergeRule_ADDITIVE_INT,
			Value: &pb.DataEffect_IntVal{IntVal: 10},
		},
	}
	effects := []*pb.Effect{
		makeDataEffect("k", 20, 1, scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 5)),
	}
	r := ReduceChain(seed, effects)
	if r.Scalar.GetIntVal() != 15 {
		t.Fatalf("expected 15, got %d", r.Scalar.GetIntVal())
	}
}

func TestReduceChain_SeedNoEffects(t *testing.T) {
	seed := &pb.ReducedEffect{
		Op:     pb.EffectOp_INSERT_OP,
		Scalar: &pb.DataEffect{Value: &pb.DataEffect_Raw{Raw: []byte("seed")}},
	}
	r := ReduceChain(seed, nil)
	if r == nil || string(r.Scalar.GetRaw()) != "seed" {
		t.Fatal("ReduceChain(seed, nil) should return clone of seed")
	}
}

func TestReduceChain_MetaOnTopOfSeed(t *testing.T) {
	seed := &pb.ReducedEffect{
		Op:         pb.EffectOp_INSERT_OP,
		Merge:      pb.MergeRule_LAST_WRITE_WINS,
		Collection: pb.CollectionKind_SCALAR,
		Scalar: &pb.DataEffect{
			Op:    pb.EffectOp_INSERT_OP,
			Merge: pb.MergeRule_LAST_WRITE_WINS,
			Value: &pb.DataEffect_Raw{Raw: []byte("data")},
		},
	}
	effects := []*pb.Effect{
		makeMetaEffect("k", 20, 1, &pb.MetaEffect{ExpiresAt: sTs(9999)}),
	}
	r := ReduceChain(seed, effects)
	if string(r.Scalar.GetRaw()) != "data" {
		t.Fatalf("expected seed data preserved, got %q", r.Scalar.GetRaw())
	}
	if r.GetExpiresAt().AsTime().UnixNano() != 9999 {
		t.Fatalf("expected ExpiresAt=9999ns, got %v", r.ExpiresAt)
	}
}

// --- Integration: Emit + Flush + GetSnapshot ---

func TestEmitFlushGetSnapshot(t *testing.T) {
	log := newSnapshotLog()
	cache := newMockCache()
	e := newSnapshotEngine(log, cache)

	ctx := e.NewContext()
	if err := ctx.Emit(&pb.Effect{
		Key: []byte("mykey"),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: []byte("myval")},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	r, _, _, err := e.GetSnapshot("mykey")
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("expected non-nil snapshot after Emit+Flush")
	}
	if string(r.Scalar.GetRaw()) != "myval" {
		t.Fatalf("expected 'myval', got %q", r.Scalar.GetRaw())
	}
}

func TestEmitFlushGetSnapshot_MultipleWrites(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	// First write
	ctx := e.NewContext()
	if err := ctx.Emit(&pb.Effect{
		Key: []byte("k"),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: []byte("v1")},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	// Second write (new context, same connection pattern)
	ctx2 := e.NewContext()
	if err := ctx2.Emit(&pb.Effect{
		Key: []byte("k"),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: []byte("v2")},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ctx2.Flush(); err != nil {
		t.Fatal(err)
	}

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	if string(r.Scalar.GetRaw()) != "v2" {
		t.Fatalf("expected 'v2', got %q", r.Scalar.GetRaw())
	}
}

func TestGetSnapshot_ReturnsTips(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	off1 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("base"))},
	})
	off2 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(off1)},
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("branch"))},
	})
	e.index.Insert("k", nil, keytrie.NewTipSet(off1, off2))

	_, tips, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	if len(tips) != 2 {
		t.Fatalf("expected 2 tips, got %d", len(tips))
	}
	tipSet := map[Tip]bool{tips[0]: true, tips[1]: true}
	if !tipSet[off1] || !tipSet[off2] {
		t.Fatalf("expected tips {%v, %v}, got %v", off1, off2, tips)
	}
}

// TestEmitWithSnapshotTips_PreventsStaleDepRace verifies that passing
// snapshot tips to Emit prevents the race where HandleRemote updates
// the index between GetSnapshot and Emit.
//
// Scenario:
//  1. GetSnapshot reads tips {A, B}, returns snapshot + tips
//  2. HandleRemote arrives with C→{A,B}, index becomes {C}
//  3. Emit(eff, tips) uses {A, B} as deps (from snapshot)
//  4. Flush correctly creates a fork {C, D} instead of D→C
func TestEmitWithSnapshotTips_PreventsStaleDepRace(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	// Set up initial state: key "k" with tips {A, B} (forked)
	offA := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("A"))},
	})
	offB := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(offA)},
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("B"))},
	})
	e.index.Insert("k", nil, keytrie.NewTipSet(offA, offB))

	// Step 1: GetSnapshot reads tips {A, B}
	_, tips, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}

	// Step 2: Simulate HandleRemote: C depends on {A,B}, resolves the fork
	effC := &pb.Effect{
		Key: []byte("k"), Hlc: sTs(30), NodeId: 3, Deps: []*pb.EffectRef{toPbRef(offA), toPbRef(offB)},
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("C"))},
	}
	offC := log.putEffect(effC)
	// Update index: consume {A,B}, replace with {C}
	old := e.index.Contains("k")
	e.index.Insert("k", old, keytrie.NewTipSet(offC))

	// Step 3: Emit with the snapshot tips from step 1
	ctx := e.NewContext()
	err = ctx.Emit(&pb.Effect{
		Key:  []byte("k"),
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("D"))},
	}, tips)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the emitted effect depends on {A, B}, not {C}
	lastCall := log.entries[Tip{uint64(e.nodeID), log.nextOff - 100}]
	var emittedEff pb.Effect
	if err := proto.Unmarshal(lastCall, &emittedEff); err != nil {
		t.Fatal(err)
	}
	if len(emittedEff.Deps) != 2 {
		t.Fatalf("expected 2 deps (snapshot tips), got %v", emittedEff.Deps)
	}
	depSet := map[Tip]bool{r(emittedEff.Deps[0]): true, r(emittedEff.Deps[1]): true}
	if !depSet[offA] || !depSet[offB] {
		t.Fatalf("expected deps {%v, %v}, got %v", offA, offB, emittedEff.Deps)
	}

	// Step 4: Flush should create a fork {C, D}
	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}
	finalTips := e.index.Contains("k")
	if finalTips == nil {
		t.Fatal("expected tips after flush")
	}
	// D's initialTips were {A,B}. Index has {C}. A and B are not in
	// current index, so they don't get removed. Result: {C, D_offset}.
	if finalTips.Len() != 2 {
		t.Fatalf("expected 2 tips (fork: C + D), got %d: %v", finalTips.Len(), finalTips.Tips())
	}
	if !finalTips.Contains(offC) {
		t.Fatalf("expected fork to contain C (%v), got %v", offC, finalTips.Tips())
	}
}

// TestEmitWithoutSnapshotTips_ReadsIndex verifies that Emit without
// snapshot tips reads deps from the index (backward compat for pure writes).
func TestEmitWithoutSnapshotTips_ReadsIndex(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	off := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("old"))},
	})
	e.index.Insert("k", nil, keytrie.NewTipSet(off))

	// Emit without snapshot tips — should read from index
	ctx := e.NewContext()
	err := ctx.Emit(&pb.Effect{
		Key:  []byte("k"),
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("new"))},
	})
	if err != nil {
		t.Fatal(err)
	}

	// After Emit, the new effect should be in the effectCache with deps pointing to off
	tips := e.index.Contains("k")
	if tips == nil {
		// Not flushed yet — check the context's pending effects instead.
		// The emitted effect lives in ctx.effects; inspect its Deps directly.
	}
	// Flush to commit
	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}
	tips = e.index.Contains("k")
	if tips == nil || tips.Len() != 1 {
		t.Fatalf("expected 1 tip after flush, got %v", tips)
	}
	newOff := tips.Tips()[0]
	cached, ok := e.effectCache.Get(newOff, 0)
	if !ok {
		t.Fatal("emitted effect not in effectCache")
	}
	if len(cached.Deps) != 1 || r(cached.Deps[0]) != off {
		t.Fatalf("expected deps [%v], got %v", off, cached.Deps)
	}
}

// --- Subscription bootstrapping tests ---

// bootstrapBroadcaster simulates the bootstrap protocol: when ReplicateTo
// is called, it processes the subscription effect and sends NACKs with tips
// back through the engine's HandleNack path (as the real cluster would).
type bootstrapBroadcaster struct {
	mockBroadcaster
	remoteEngine *Engine // the "remote" engine that has data
}

func (b *bootstrapBroadcaster) ReplicateTo(notify *pb.OffsetNotify, wireData []byte, targetNodeID pb.NodeID) ([]*pb.NackNotify, error) {
	b.replicateToPeers = append(b.replicateToPeers, targetNodeID)

	// Simulate the remote side: process the SubscriptionEffect via HandleRemote.
	// HandleRemote returns NACKs synchronously — forward them to the caller
	// so ensureSubscribed can collect bootstrap tips.
	if b.remoteEngine != nil {
		nacks, err := b.remoteEngine.HandleRemote(notify)
		return nacks, err
	}
	return nil, nil
}

func (b *bootstrapBroadcaster) FetchFromAny(ref *pb.EffectRef) ([]byte, error) {
	if b.remoteEngine == nil {
		return nil, nil
	}
	offset := r(ref)
	// Read from remote engine's effect cache
	cached, ok := b.remoteEngine.effectCache.Get(offset, 0)
	if !ok {
		return nil, fmt.Errorf("effect %v not found in remote cache", offset)
	}
	// Reconstruct wire format: [4-byte LE keyLen][key][protoData]
	protoData, err := proto.Marshal(cached)
	if err != nil {
		return nil, err
	}
	keyBytes := cached.Key
	wire := make([]byte, 4+len(keyBytes)+len(protoData))
	binary.LittleEndian.PutUint32(wire[:4], uint32(len(keyBytes)))
	copy(wire[4:4+len(keyBytes)], keyBytes)
	copy(wire[4+len(keyBytes):], protoData)
	return wire, nil
}

func TestSubscriptionBootstrap_FetchesRemoteState(t *testing.T) {
	// Set up "remote" engine (node 2) with pre-existing data.
	// Use a different offset range to avoid collision with local log.
	remoteLog := newSnapshotLog()
	remoteLog.nextOff = 10000
	remoteEngine := &Engine{
		effectCache:       remoteLog.effectCache,
		index:             keytrie.New(),
		nodeID:            2,
		clock:             crdt.NewHLC(),
		subscriptions:     xsync.NewMap[string, *subscriptionState](),
		pendingTxns:       xsync.NewMap[Tip, *pendingTxn](),
		pendingTxTips:     xsync.NewMap[Tip, []Tip](),
		txAbortCounts:     xsync.NewMap[string, *atomic.Int32](),
		pendingBootstraps: xsync.NewMap[string, *bootstrapCollector](),
		unsubInFlight:     xsync.NewMap[string, struct{}](),
		voidedBinds:       clox.NewCloxCache[string, struct{}](clox.ConfigFromCapacity(256)),
	}
	remoteEngine.safety.Store(&safetyMap{defaultMode: UnsafeMode})

	// Write data on remote node
	remoteCtx := remoteEngine.NewContext()
	if err := remoteCtx.Emit(&pb.Effect{
		Key: []byte("shared-key"),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: []byte("remote-value")},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := remoteCtx.Flush(); err != nil {
		t.Fatal(err)
	}

	// Set up "local" engine (node 1) with no data
	localLog := newSnapshotLog()

	// Create the bootstrap broadcaster that connects local and remote
	bc := &bootstrapBroadcaster{
		mockBroadcaster: mockBroadcaster{peerIDs: []pb.NodeID{2}, allRegionPeersReachable: true},
	}
	// The remote engine's broadcaster needs to forward NACKs to the local engine
	// We'll set this up after creating the local engine

	localEngine := &Engine{
		effectCache:       localLog.effectCache,
		index:             keytrie.New(),
		broadcaster:       bc,
		nodeID:            1,
		clock:             crdt.NewHLC(),
		subscriptions:     xsync.NewMap[string, *subscriptionState](),
		pendingTxns:       xsync.NewMap[Tip, *pendingTxn](),
		pendingTxTips:     xsync.NewMap[Tip, []Tip](),
		txAbortCounts:     xsync.NewMap[string, *atomic.Int32](),
		pendingBootstraps: xsync.NewMap[string, *bootstrapCollector](),
		unsubInFlight:     xsync.NewMap[string, struct{}](),
		voidedBinds:       clox.NewCloxCache[string, struct{}](clox.ConfigFromCapacity(256)),
	}
	localEngine.safety.Store(&safetyMap{defaultMode: UnsafeMode})

	// Wire up: remote engine sends NACKs through the local engine's HandleNack
	nackForwarder := &nackForwardBroadcaster{target: localEngine}
	remoteEngine.broadcaster = nackForwarder
	bc.remoteEngine = remoteEngine

	// Now the local engine reads "shared-key" for the first time
	r, _, _, err := localEngine.GetSnapshot("shared-key")
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("expected non-nil snapshot after subscription bootstrap")
	}
	if string(r.Scalar.GetRaw()) != "remote-value" {
		t.Fatalf("expected 'remote-value', got %q", r.Scalar.GetRaw())
	}
}

// nackForwardBroadcaster forwards SendNack calls to a target engine's HandleNack.
type nackForwardBroadcaster struct {
	mockBroadcaster
	target *Engine
}

func (b *nackForwardBroadcaster) SendNack(nack *pb.NackNotify, targetNodeID pb.NodeID) {
	b.sentNacks = append(b.sentNacks, nack)
	_ = b.target.HandleNack(nack)
}

func TestSubscriptionBootstrap_NoPeers_NoBlock(t *testing.T) {
	log := newSnapshotLog()
	bc := &mockBroadcaster{peerIDs: nil} // no peers
	e := newSnapshotEngine(log, nil)
	e.broadcaster = bc

	// Should not block even with no peers
	r, _, _, err := e.GetSnapshot("key")
	if err != nil {
		t.Fatal(err)
	}
	if r != nil {
		t.Fatal("expected nil for key with no data and no peers")
	}
}

func TestSubscriptionBootstrap_AllPeersEmpty(t *testing.T) {
	log := newSnapshotLog()
	bc := &mockBroadcaster{peerIDs: []pb.NodeID{10, 20}, allRegionPeersReachable: true}
	e := newSnapshotEngine(log, nil)
	e.broadcaster = bc
	bc.nackTarget = e // wire up so empty NACKs arrive

	// Peers exist but have no data for this key — empty NACKs
	r, _, _, err := e.GetSnapshot("missing-key")
	if err != nil {
		t.Fatal(err)
	}
	if r != nil {
		t.Fatal("expected nil when all peers have no data")
	}
	// Should have sent ReplicateTo to both peers twice (two rounds)
	if len(bc.replicateToPeers) != 4 {
		t.Fatalf("expected 4 ReplicateTo calls (2 peers x 2 rounds), got %d", len(bc.replicateToPeers))
	}
}

func TestHandleRemote_SubscriptionEffect_SendsNack(t *testing.T) {
	log := newSnapshotLog()
	bc := &mockBroadcaster{}
	e := &Engine{
		effectCache:       log.effectCache,
		index:             keytrie.New(),
		broadcaster:       bc,
		nodeID:            1,
		clock:             crdt.NewHLC(),
		subscriptions:     xsync.NewMap[string, *subscriptionState](),
		pendingTxns:       xsync.NewMap[Tip, *pendingTxn](),
		pendingTxTips:     xsync.NewMap[Tip, []Tip](),
		txAbortCounts:     xsync.NewMap[string, *atomic.Int32](),
		pendingBootstraps: xsync.NewMap[string, *bootstrapCollector](),
		unsubInFlight:     xsync.NewMap[string, struct{}](),
		voidedBinds:       clox.NewCloxCache[string, struct{}](clox.ConfigFromCapacity(256)),
	}
	e.safety.Store(&safetyMap{defaultMode: UnsafeMode})

	// Pre-populate: node 1 has data for "k"
	off := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("data"))},
	})
	e.index.Insert("k", nil, keytrie.NewTipSet(off))

	// Simulate remote node 2 subscribing to "k"
	subEff := &pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 2,
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(20)),
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
			SubscriberNodeId: 2,
		}},
	}
	subData, _ := proto.Marshal(subEff)
	notify := BuildOffsetNotify(2, Tip{2, 5000}, subEff, subData, nil)

	nacks, err := e.HandleRemote(notify)
	if err != nil {
		t.Fatal(err)
	}

	// Should have returned a NACK with the existing tip
	if len(nacks) != 1 {
		t.Fatalf("expected 1 NACK returned, got %d", len(nacks))
	}
	nack := nacks[0]
	if string(nack.Key) != "k" {
		t.Fatalf("expected NACK for key 'k', got %q", nack.Key)
	}
	if len(nack.Tips) != 1 || r(nack.Tips[0]) != off {
		t.Fatalf("expected NACK tips [%v], got %v", off, nack.Tips)
	}
}

// TestHandleRemote_SubscriptionEffect_NoAuthority_EmptyAck asserts that a
// subscription from a peer for a key this node has no authority over
// (not subscribed AND no tips in the index) produces an empty ACK
// rather than the normal authority-gate drop. The bootstrap subscribe
// flow on the sender side waits on ReplicateTo for an ACK from every
// reachable peer; suppressing the wire response there would prevent
// any new key from being bootstrapped on a cluster where no peer yet
// has authority. The receiver still does not index the remote
// subscription — index cleanliness is preserved.
func TestHandleRemote_SubscriptionEffect_NoAuthority_EmptyAck(t *testing.T) {
	log := newSnapshotLog()
	bc := &mockBroadcaster{}
	e := &Engine{
		effectCache:       log.effectCache,
		index:             keytrie.New(),
		broadcaster:       bc,
		nodeID:            1,
		clock:             crdt.NewHLC(),
		subscriptions:     xsync.NewMap[string, *subscriptionState](),
		pendingTxns:       xsync.NewMap[Tip, *pendingTxn](),
		pendingTxTips:     xsync.NewMap[Tip, []Tip](),
		txAbortCounts:     xsync.NewMap[string, *atomic.Int32](),
		pendingBootstraps: xsync.NewMap[string, *bootstrapCollector](),
		unsubInFlight:     xsync.NewMap[string, struct{}](),
		voidedBinds:       clox.NewCloxCache[string, struct{}](clox.ConfigFromCapacity(256)),
	}
	e.safety.Store(&safetyMap{defaultMode: UnsafeMode})

	// No data for "newkey", and the engine has not subscribed to it.
	subEff := &pb.Effect{
		Key: []byte("newkey"), Hlc: sTs(20), NodeId: 2,
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(20)),
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
			SubscriberNodeId: 2,
		}},
	}
	subData, _ := proto.Marshal(subEff)
	notify := BuildOffsetNotify(2, Tip{2, 5000}, subEff, subData, nil)

	nacks, err := e.HandleRemote(notify)
	if err != nil {
		t.Fatalf("expected nil err (empty ACK), got %v", err)
	}
	if len(nacks) != 0 {
		t.Fatalf("expected 0 NACKs from a no-authority key; got %d", len(nacks))
	}
	// And we should not have acquired any index entry from the peer's
	// subscription either — the no-authority short-circuit must skip
	// indexing even though it now returns an ACK.
	if e.index.Contains("newkey") != nil {
		t.Fatal("no-authority short-circuit let the peer's subscription tip enter our index")
	}
}

// TestHandleRemote_FlushKey_BypassesAuthorityGate asserts that an
// inbound flush-all effect propagates even though the receiver has no
// prior subscription or index entry for FlushKey. Pre-fix FlushKey was
// "\x00" which did not match the __swytch: system-key prefix, so the
// authority gate dropped inbound flushes with ErrAuthorityDropped and
// cluster-wide FLUSHDB/FLUSHALL stopped at any peer that hadn't
// independently observed a prior flush. Moving FlushKey into the
// __swytch: namespace bypasses the gate via isSystemKey and lets the
// flush handler downstream wipe the local index.
func TestHandleRemote_FlushKey_BypassesAuthorityGate(t *testing.T) {
	log := newSnapshotLog()
	bc := &mockBroadcaster{}
	e := &Engine{
		effectCache:       log.effectCache,
		index:             keytrie.New(),
		broadcaster:       bc,
		nodeID:            1,
		clock:             crdt.NewHLC(),
		subscriptions:     xsync.NewMap[string, *subscriptionState](),
		pendingTxns:       xsync.NewMap[Tip, *pendingTxn](),
		pendingTxTips:     xsync.NewMap[Tip, []Tip](),
		txAbortCounts:     xsync.NewMap[string, *atomic.Int32](),
		pendingBootstraps: xsync.NewMap[string, *bootstrapCollector](),
		unsubInFlight:     xsync.NewMap[string, struct{}](),
		voidedBinds:       clox.NewCloxCache[string, struct{}](clox.ConfigFromCapacity(256)),
	}
	e.safety.Store(&safetyMap{defaultMode: UnsafeMode})

	e.updateIndex("alpha", nil, Tip{1, 100})
	e.updateIndex("beta", nil, Tip{1, 101})
	if e.index.Contains("alpha") == nil || e.index.Contains("beta") == nil {
		t.Fatal("test setup: seeded keys not in index")
	}

	flushFired := false
	e.OnFlushAll = func() { flushFired = true }

	flushEff := &pb.Effect{
		Key: []byte(FlushKey), Hlc: sTs(30), NodeId: 2,
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(30)),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
		}},
	}
	flushData, _ := proto.Marshal(flushEff)
	notify := BuildOffsetNotify(2, Tip{2, 6000}, flushEff, flushData, nil)

	nacks, err := e.HandleRemote(notify)
	if err != nil {
		t.Fatalf("expected nil err (gate must bypass FlushKey), got %v", err)
	}
	if len(nacks) != 0 {
		t.Fatalf("expected 0 NACKs from flush, got %d", len(nacks))
	}
	if e.index.Contains("alpha") != nil || e.index.Contains("beta") != nil {
		t.Fatal("FlushIndex did not run; seeded keys remain in the index")
	}
	if !flushFired {
		t.Fatal("OnFlushAll callback did not fire")
	}
}

// TestHandleRemote_TxnBind_AuthorityViaNonCanonicalKey asserts that a
// remote bind passes the authority gate when the receiver has authority
// over any touched key, not only the canonical first key. flushTx sets
// bindEff.Key = bind.Keys[0].Key but collectSubscribers replicates the
// bind to peers subscribed to any touched key; pre-fix the gate checked
// only eff.Key, so a peer subscribed to a later key but not the
// canonical key dropped the bind and missed committed transactions for
// keys it was authoritative over.
func TestHandleRemote_TxnBind_AuthorityViaNonCanonicalKey(t *testing.T) {
	log := newSnapshotLog()
	bc := &mockBroadcaster{}
	e := &Engine{
		effectCache:       log.effectCache,
		index:             keytrie.New(),
		broadcaster:       bc,
		nodeID:            1,
		clock:             crdt.NewHLC(),
		subscriptions:     xsync.NewMap[string, *subscriptionState](),
		pendingTxns:       xsync.NewMap[Tip, *pendingTxn](),
		pendingTxTips:     xsync.NewMap[Tip, []Tip](),
		txAbortCounts:     xsync.NewMap[string, *atomic.Int32](),
		pendingBootstraps: xsync.NewMap[string, *bootstrapCollector](),
		unsubInFlight:     xsync.NewMap[string, struct{}](),
		voidedBinds:       clox.NewCloxCache[string, struct{}](clox.ConfigFromCapacity(256)),
	}
	e.safety.Store(&safetyMap{defaultMode: UnsafeMode})

	// Subscribed only to "B" (non-canonical). No subscription, no
	// index entry for "A" (canonical) — pre-fix this configuration
	// triggers the gate drop.
	e.subscriptions.Store("B", &subscriptionState{ready: make(chan struct{})})

	bindOffset := Tip{2, 7000}
	bind := &pb.TransactionalBindEffect{
		TxnHlc:           sTs(40),
		OriginatorNodeId: 2,
		Keys: []*pb.TransactionalBindEffect_KeyBind{
			{Key: []byte("A"), NewTip: &pb.EffectRef{NodeId: 2, Offset: 6998}},
			{Key: []byte("B"), NewTip: &pb.EffectRef{NodeId: 2, Offset: 6999}},
		},
	}
	bindEff := &pb.Effect{
		Key:            []byte("A"),
		Hlc:            sTs(40),
		NodeId:         2,
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(40)),
		TxnId:          "2:40:1",
		Kind:           &pb.Effect_TxnBind{TxnBind: bind},
	}
	data, _ := proto.Marshal(bindEff)
	notify := BuildOffsetNotify(2, bindOffset, bindEff, data, nil)

	nacks, err := e.HandleRemote(notify)
	if err != nil {
		t.Fatalf("expected gate to bypass via non-canonical key authority, got %v", err)
	}
	if len(nacks) != 0 {
		t.Fatalf("expected 0 NACKs (no divergence), got %d", len(nacks))
	}

	// The bind must be indexed under "B" — the key we have authority
	// over and the reason we received the bind in the first place.
	tipsB := e.index.Contains("B")
	if tipsB == nil {
		t.Fatal("bind not indexed under 'B' (the authoritative key)")
	}
	if !slices.Contains(tipsB.Tips(), bindOffset) {
		t.Fatalf("bind offset %v missing from 'B' tips: %v", bindOffset, tipsB.Tips())
	}
}

// TestGetSnapshot_ForkWithSnapshotEffect verifies that a SnapshotEffect on one
// branch doesn't cause double-counting of the shared prefix during forked
// reconstruction. This was the root cause of the Jepsen counter bug: the
// SnapshotEffect embeds an absolute value that includes the shared prefix,
// and if the LCA isn't found correctly (because collectAncestors stopped at
// the snapshot), the prefix is counted in both the base and the branch delta.
//
// DAG:
//
//	root(+1) → E2(+1) → E3(+1) = common prefix (value 3)
//	                     ↓
//	               snapshot(state=3, deps=[E3]) → E4(+1) = branch 1 (tip)
//	                     ↓
//	                    E5(+1) = branch 2 (tip, depends on E3 directly)
//
// Correct: 3 (base) + 1 (E4 delta) + 1 (E5 delta) = 5
// Bug:     0 (LCA=0) + 4 (snapshot+E4) + 3 (E1+E2+E3+E5) = double-count
func TestGetSnapshot_ForkWithSnapshotEffect(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	// Build common prefix: E1(+1) → E2(+1) → E3(+1)
	e1 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	e2 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e1)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	e3 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(30), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e2)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	// Branch 1: snapshot(state=3, deps=[E3]) → E4(+1)
	snap := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(31), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e3)},
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
			Collection: pb.CollectionKind_SCALAR,
			State: &pb.ReducedEffect{
				Op:          pb.EffectOp_INSERT_OP,
				Merge:       pb.MergeRule_ADDITIVE_INT,
				Collection:  pb.CollectionKind_SCALAR,
				Commutative: true,
				Hlc:         sTs(30), NodeId: 1,
				Scalar: &pb.DataEffect{
					Op:    pb.EffectOp_INSERT_OP,
					Merge: pb.MergeRule_ADDITIVE_INT,
					Value: &pb.DataEffect_IntVal{IntVal: 3},
				},
			},
		}},
	})
	tipA := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(40), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(snap)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	// Branch 2: E5(+1), forked from E3 directly (no snapshot)
	tipB := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(50), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(e3)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	e.index.Insert("k", nil, keytrie.NewTipSet(tipA, tipB))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	// 3 (common: E1+E2+E3) + 1 (branch1: E4) + 1 (branch2: E5) = 5
	if r.Scalar.GetIntVal() != 5 {
		t.Fatalf("expected 5, got %d (double-counting bug if >5)", r.Scalar.GetIntVal())
	}
}

// TestGetSnapshot_ForkBothBranchesHaveSnapshots tests the case where BOTH
// branches have SnapshotEffects hiding the common ancestor. The LCA must
// be discovered by following SnapshotEffect deps as offset numbers even
// when the target offset isn't in the effects map yet.
func TestGetSnapshot_ForkBothBranchesHaveSnapshots(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	// Common prefix: E1(+10)
	e1 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 10)},
	})

	// Branch 1: snapshot(state=10, deps=[E1]) → E2(+1)
	snap1 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(11), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e1)},
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
	tipA := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(snap1)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	// Branch 2: snapshot(state=10, deps=[E1]) → E3(+2)
	snap2 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(12), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(e1)},
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
	tipB := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(30), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(snap2)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 2)},
	})

	e.index.Insert("k", nil, keytrie.NewTipSet(tipA, tipB))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	// 10 (common: E1) + 1 (branch1: E2) + 2 (branch2: E3) = 13
	if r.Scalar.GetIntVal() != 13 {
		t.Fatalf("expected 13, got %d (double-counting bug if >13)", r.Scalar.GetIntVal())
	}
}

// TestGetSnapshot_CrossBranchSnapshot reproduces the Jepsen counter bug.
// A snapshot is created during a forked read, so its deps span BOTH branches.
// When a new fork later happens above this snapshot, the reconstruction must
// not double-count effects that are reachable from multiple branches through
// the cross-branch snapshot's deps.
//
// DAG (what actually happens during Jepsen):
//
//  1. Common prefix: E1(+1) → E2(+1) → E3(+1)  [value=3]
//  2. Fork: E4(+1, deps=[E3]) on node1, E5(+1, deps=[E3]) on node2
//  3. Node1 sees both tips [E4,E5], does a read → compaction:
//     S(snapshot, state=5, deps=[E4,E5])   [cross-branch!]
//  4. Node1 does INCR: E6(+1, deps=[S])
//  5. Meanwhile node2 does INCR: E7(+1, deps=[E5])
//  6. Tips are now [E6, E7]. Correct value = 7 (E1+E2+E3+E4+E5+E6+E7)
func TestGetSnapshot_CrossBranchSnapshot(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	// Common prefix
	e1 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	e2 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e1)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	e3 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(30), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e2)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	// First fork: E4 on node1, E5 on node2, both from E3
	e4 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(40), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e3)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	e5 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(41), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(e3)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	// Cross-branch snapshot: node1 sees [E4,E5], compacts to snapshot
	snap := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(50), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e4), toPbRef(e5)},
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
			Collection: pb.CollectionKind_SCALAR,
			State: &pb.ReducedEffect{
				Op:          pb.EffectOp_INSERT_OP,
				Merge:       pb.MergeRule_ADDITIVE_INT,
				Collection:  pb.CollectionKind_SCALAR,
				Commutative: true,
				Hlc:         sTs(41), NodeId: 2,
				Scalar: &pb.DataEffect{
					Op:    pb.EffectOp_INSERT_OP,
					Merge: pb.MergeRule_ADDITIVE_INT,
					Value: &pb.DataEffect_IntVal{IntVal: 5}, // E1+E2+E3+E4+E5
				},
			},
		}},
	})

	// E6: node1 INCRs on top of the snapshot
	tipA := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(60), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(snap)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	// E7: node2 INCRs on top of E5 (hasn't seen the snapshot)
	tipB := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(61), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(e5)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	e.index.Insert("k", nil, keytrie.NewTipSet(tipA, tipB))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	// E1+E2+E3+E4+E5+E6+E7 = 7
	if r.Scalar.GetIntVal() != 7 {
		t.Fatalf("expected 7, got %d", r.Scalar.GetIntVal())
	}
}

// TestGetSnapshot_ForkMultiEffectBranches tests forked reconstruction with
// multiple effects per branch and no snapshots — the basic Jepsen scenario.
// This catches the doubling bug that occurs with chains longer than 1 per branch.
//
// DAG:
//
//	E1(+1) → E2(+1) → E3(+1) = common prefix [value=3]
//	                     ↓
//	                    E4(+1) → E5(+1) → E6(+1) = branch A [3 effects]
//	                     ↓
//	                    E7(+1) → E8(+1) = branch B [2 effects]
//
// Correct: 3 + 3 + 2 = 8
func TestGetSnapshot_ForkMultiEffectBranches(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	// Common prefix
	e1 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	e2 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e1)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	e3 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(30), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e2)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	// Branch A: 3 effects
	e4 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(40), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e3)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	e5 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(50), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e4)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	tipA := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(60), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e5)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	// Branch B: 2 effects
	e7 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(41), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(e3)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	tipB := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(51), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(e7)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	e.index.Insert("k", nil, keytrie.NewTipSet(tipA, tipB))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	// 3 (common) + 3 (branch A) + 2 (branch B) = 8
	if r.Scalar.GetIntVal() != 8 {
		t.Fatalf("expected 8, got %d", r.Scalar.GetIntVal())
	}
}

// TestGetSnapshot_ForkAfterMerge tests reconstruction when a fork occurs
// after a previous fork was already merged. This is the common Jepsen pattern:
// fork → merge → fork → merge, with the counter doubling at each merge.
//
// DAG:
//
//	E1(+1) → E2(+1) = common [value=2]
//	           ↓
//	          E3(+1) on node1
//	           ↓
//	          E4(+1) on node2
//	           ↓
//	          E5(+1, deps=[E3,E4]) = merge point [value should be 5]
//	           ↓
//	          E6(+1) on node1
//	           ↓
//	          E7(+1) on node2
//	Tips: [E6, E7]. Correct: 7
func TestGetSnapshot_ForkAfterMerge(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	// Common prefix
	e1 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	e2 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e1)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	// First fork
	e3 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(30), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e2)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	e4 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(31), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(e2)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	// Merge point: node1 sees both E3 and E4, does INCR
	e5 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(40), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e3), toPbRef(e4)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	// Second fork from the merge point
	tipA := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(50), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e5)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	tipB := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(51), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(e5)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	e.index.Insert("k", nil, keytrie.NewTipSet(tipA, tipB))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	// E1+E2+E3+E4+E5+E6+E7 = 7
	if r.Scalar.GetIntVal() != 7 {
		t.Fatalf("expected 7, got %d", r.Scalar.GetIntVal())
	}
}

// TestGetSnapshot_ForkWithPartialReplication tests the case where one branch
// has received some (but not all) effects from the other branch before forking.
// This is the most common Jepsen scenario: node2 receives E3 from node1,
// then a partition happens, and both continue independently.
//
// DAG:
//
//	E1(+1, node1) → E2(+1, node1) → E3(+1, node1)
//	                                   ↓
//	                 E4(+1, node2, deps=[E3])  ← node2 received E3 before partition
//	                                   ↓
//	                 E5(+1, node1, deps=[E3])  ← node1 continues after E3
//	Tips: [E4, E5]. Both depend on E3. Correct: 5
func TestGetSnapshot_ForkWithPartialReplication(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	e1 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	e2 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e1)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	e3 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(30), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e2)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	// Node2 received E3, does its own INCR
	tipA := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(40), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(e3)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	// Node1 continues from E3
	tipB := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(41), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e3)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	e.index.Insert("k", nil, keytrie.NewTipSet(tipA, tipB))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	// E1+E2+E3+E4+E5 = 5
	if r.Scalar.GetIntVal() != 5 {
		t.Fatalf("expected 5, got %d", r.Scalar.GetIntVal())
	}
}

// TestGetSnapshot_JepsenDAG14 is a real DAG captured from a Jepsen run.
// 14 nodes, 2 tips, 5 roots. 8 data effects (+1 each), so correct = 8.
//
// Encoded: 14 T10,6 R9,7,4,0,11 0:U:0: 1:D:1:4,7,9,11 2:M:0:1 3:D:1:5,13
//
//	4:U:0: 5:D:1:0,2,7,9,11 6:D:1:3,8 7:U:0: 8:D:1:5,13
//	9:U:0: 10:D:1:0,4,5,7,11,13 11:U:0: 12:D:1:0,4,7,9 13:M:0:12
func TestGetSnapshot_JepsenDAG14(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	// Node IDs map to offsets. Using sequential offsets.
	off := make([]Tip, 14)

	// nodeIDs for each effect slot (determined by the effects below)
	nodeIDs := []uint64{1, 1, 1, 2, 2, 1, 2, 3, 2, 4, 1, 5, 1, 1}

	for i := range off {
		off[i] = Tip{nodeIDs[i], uint64((i + 1) * 100)}
	}

	putN := func(id int, eff *pb.Effect) {
		eff.Hlc = sTs(int64(id + 1))
		if len(eff.ForkChoiceHash) == 0 {
			eff.ForkChoiceHash = ComputeForkChoiceHash(pb.NodeID(eff.NodeId), eff.Hlc)
		}
		data, _ := proto.Marshal(eff)
		log.entries[off[id]] = data
		log.effectCache.Put(off[id], proto.Clone(eff).(*pb.Effect))
	}

	depsOf := func(ids ...int) []*pb.EffectRef {
		d := make([]*pb.EffectRef, len(ids))
		for i, id := range ids {
			d[i] = toPbRef(off[id])
		}
		return d
	}

	// 0:U (subscription, root)
	putN(0, &pb.Effect{Key: []byte("k"), NodeId: 1, Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 1}}})
	// 1:D:1 deps=[4,7,9,11]
	putN(1, &pb.Effect{Key: []byte("k"), NodeId: 1, Deps: depsOf(4, 7, 9, 11), Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)}})
	// 2:M deps=[1]
	putN(2, &pb.Effect{Key: []byte("k"), NodeId: 1, Deps: depsOf(1), Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_STRING}}})
	// 3:D:1 deps=[5,13]
	putN(3, &pb.Effect{Key: []byte("k"), NodeId: 2, Deps: depsOf(5, 13), Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)}})
	// 4:U (subscription, root)
	putN(4, &pb.Effect{Key: []byte("k"), NodeId: 2, Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 2}}})
	// 5:D:1 deps=[0,2,7,9,11]
	putN(5, &pb.Effect{Key: []byte("k"), NodeId: 1, Deps: depsOf(0, 2, 7, 9, 11), Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)}})
	// 6:D:1 deps=[3,8] — TIP
	putN(6, &pb.Effect{Key: []byte("k"), NodeId: 2, Deps: depsOf(3, 8), Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)}})
	// 7:U (subscription, root)
	putN(7, &pb.Effect{Key: []byte("k"), NodeId: 3, Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 3}}})
	// 8:D:1 deps=[5,13]
	putN(8, &pb.Effect{Key: []byte("k"), NodeId: 2, Deps: depsOf(5, 13), Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)}})
	// 9:U (subscription, root)
	putN(9, &pb.Effect{Key: []byte("k"), NodeId: 4, Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 4}}})
	// 10:D:1 deps=[0,4,5,7,11,13] — TIP
	putN(10, &pb.Effect{Key: []byte("k"), NodeId: 1, Deps: depsOf(0, 4, 5, 7, 11, 13), Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)}})
	// 11:U (subscription, root)
	putN(11, &pb.Effect{Key: []byte("k"), NodeId: 5, Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 5}}})
	// 12:D:1 deps=[0,4,7,9]
	putN(12, &pb.Effect{Key: []byte("k"), NodeId: 1, Deps: depsOf(0, 4, 7, 9), Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)}})
	// 13:M deps=[12]
	putN(13, &pb.Effect{Key: []byte("k"), NodeId: 1, Deps: depsOf(12), Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_STRING}}})

	e.index.Insert("k", nil, keytrie.NewTipSet(off[10], off[6]))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	// 7 data effects (nodes 1,3,5,6,8,10,12), each +1, correct = 7
	if r.Scalar.GetIntVal() != 7 {
		t.Fatalf("expected 7, got %d", r.Scalar.GetIntVal())
	}
}

// TestGetSnapshot_ThreeWayMergeUnequalLCA tests the N>2 inclusion-exclusion bug.
// When 3 branches merge but two of them share a deeper common ancestor than
// the third, the global LCA formula sum(fulls)-(N-1)*base over-subtracts
// for the shallow pair and under-subtracts for the deep pair.
//
// DAG:
//
//	E1(+1) = root [value=1]
//	  ↓
//	E2(+1, deps=[E1]) = deeper common ancestor of A and B [value=2]
//	  ↓                    ↓
//	E3(+1, deps=[E2])    E4(+1, deps=[E2])    E5(+1, deps=[E1]) ← only shares E1
//
// Tips: [E3, E4, E5]. Global LCA = E1 (value=1).
// But E3 and E4 share LCA=E2 (value=2).
//
// Correct: E1+E2+E3+E4+E5 = 5
// Formula with global LCA: (3+3+2) - 2*1 = 6 ← WRONG (over by 1)
// Because E2 is counted in both E3's and E4's full values, but the
// formula only subtracts E1 (the global LCA), not E2.
func TestGetSnapshot_ThreeWayMergeUnequalLCA(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	e1 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	e2 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e1)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	// Branches A and B fork from E2 (share deeper ancestry)
	tipA := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(30), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e2)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	tipB := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(31), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(e2)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	// Branch C forks from E1 (only shares the root)
	tipC := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(32), NodeId: 3, Deps: []*pb.EffectRef{toPbRef(e1)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	e.index.Insert("k", nil, keytrie.NewTipSet(tipA, tipB, tipC))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	// E1(1) + E2(1) + E3(1) + E4(1) + E5(1) = 5
	if r.Scalar.GetIntVal() != 5 {
		t.Fatalf("expected 5, got %d", r.Scalar.GetIntVal())
	}
}

// TestGetSnapshot_FourWayMergeFromJepsen reproduces the Jepsen pattern:
// 4 nodes fork from a common point, two pairs share deeper ancestry.
//
// DAG:
//
//	E1(+1) = root
//	  ↓
//	E2(+1, deps=[E1]) = shared by nodes 1,2
//	  ↓           ↓
//	E3(+1)      E4(+1)     E5(+1, deps=[E1])  E6(+1, deps=[E1])
//	(node1)     (node2)    (node3)              (node4)
//
// Tips: [E3, E4, E5, E6]. Global LCA = E1.
// E3,E4 share LCA=E2. E5,E6 share LCA=E1.
// Correct: 1+1+1+1+1+1 = 6
func TestGetSnapshot_FourWayMergeFromJepsen(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	e1 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	e2 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e1)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	// Nodes 1,2 fork from E2
	tipA := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(30), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e2)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	tipB := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(31), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(e2)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	// Nodes 3,4 fork from E1
	tipC := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(32), NodeId: 3, Deps: []*pb.EffectRef{toPbRef(e1)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	tipD := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(33), NodeId: 4, Deps: []*pb.EffectRef{toPbRef(e1)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	e.index.Insert("k", nil, keytrie.NewTipSet(tipA, tipB, tipC, tipD))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	// E1(1) + E2(1) + E3(1) + E4(1) + E5(1) + E6(1) = 6
	if r.Scalar.GetIntVal() != 6 {
		t.Fatalf("expected 6, got %d", r.Scalar.GetIntVal())
	}
}

// TestGetSnapshot_ChainedCompactionSnapshots simulates the Jepsen counter pattern:
// repeated fork → compaction snapshot → new increments → fork again.
// Each compaction snapshot captures the merged value and becomes the base
// for subsequent increments. Chained snapshots must not lose increments.
//
// Round 1: 10 increments on node1, 10 on node2, fork from root.
// Compaction: snapshot S1 captures value=20, deps=[tip1, tip2].
// Round 2: 5 more increments on node1 (deps=[S1]), 5 on node2 (deps=[S1]).
// Compaction: snapshot S2 captures value=30, deps=[tip3, tip4].
// Round 3: 3 more on node1, 3 on node2, fork from S2.
// Final tips: [tip5, tip6]. Correct: 36.
func TestGetSnapshot_ChainedCompactionSnapshots(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)
	hlc := int64(0)
	nextHLC := func() int64 { hlc++; return hlc }

	// Round 1: 10 increments per node from root
	var prev1, prev2 Tip
	for range 10 {
		deps := []*pb.EffectRef{}
		if prev1 != (Tip{}) {
			deps = []*pb.EffectRef{toPbRef(prev1)}
		}
		prev1 = log.putEffect(&pb.Effect{
			Key: []byte("k"), Hlc: sTs(nextHLC()), NodeId: 1, Deps: deps,
			Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
		})
	}
	for range 10 {
		deps := []*pb.EffectRef{}
		if prev2 != (Tip{}) {
			deps = []*pb.EffectRef{toPbRef(prev2)}
		}
		prev2 = log.putEffect(&pb.Effect{
			Key: []byte("k"), Hlc: sTs(nextHLC()), NodeId: 2, Deps: deps,
			Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
		})
	}

	// Compaction snapshot S1: merges both branches, value=20
	s1 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(nextHLC()), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(prev1), toPbRef(prev2)},
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
			Collection: pb.CollectionKind_SCALAR,
			State: &pb.ReducedEffect{
				Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_ADDITIVE_INT,
				Collection: pb.CollectionKind_SCALAR, Commutative: true,
				Hlc: sTs(hlc), NodeId: 1,
				Scalar: &pb.DataEffect{
					Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_ADDITIVE_INT,
					Value: &pb.DataEffect_IntVal{IntVal: 20},
				},
			},
		}},
	})

	// Round 2: 5 more per node, both starting from S1
	prev1 = s1
	prev2 = s1
	for range 5 {
		prev1 = log.putEffect(&pb.Effect{
			Key: []byte("k"), Hlc: sTs(nextHLC()), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(prev1)},
			Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
		})
	}
	for range 5 {
		prev2 = log.putEffect(&pb.Effect{
			Key: []byte("k"), Hlc: sTs(nextHLC()), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(prev2)},
			Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
		})
	}

	// Compaction snapshot S2: merges both branches, value=30
	s2 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(nextHLC()), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(prev1), toPbRef(prev2)},
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
			Collection: pb.CollectionKind_SCALAR,
			State: &pb.ReducedEffect{
				Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_ADDITIVE_INT,
				Collection: pb.CollectionKind_SCALAR, Commutative: true,
				Hlc: sTs(hlc), NodeId: 1,
				Scalar: &pb.DataEffect{
					Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_ADDITIVE_INT,
					Value: &pb.DataEffect_IntVal{IntVal: 30},
				},
			},
		}},
	})

	// Round 3: 3 more per node from S2
	prev1 = s2
	prev2 = s2
	for range 3 {
		prev1 = log.putEffect(&pb.Effect{
			Key: []byte("k"), Hlc: sTs(nextHLC()), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(prev1)},
			Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
		})
	}
	for range 3 {
		prev2 = log.putEffect(&pb.Effect{
			Key: []byte("k"), Hlc: sTs(nextHLC()), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(prev2)},
			Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
		})
	}

	e.index.Insert("k", nil, keytrie.NewTipSet(prev1, prev2))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	// 10+10 + 5+5 + 3+3 = 36
	if r.Scalar.GetIntVal() != 36 {
		t.Fatalf("expected 36, got %d", r.Scalar.GetIntVal())
	}
}

// TestGetSnapshot_SnapshotChainWithPartialVisibility simulates when node2
// hasn't seen a compaction snapshot but continues incrementing from an
// older branch point, while node1 increments on top of the snapshot.
//
// E1(+1)→...→E10(+1) = 10 on node1
// E11(+1)→...→E20(+1) = 10 on node2, forked from root
// S1(snapshot, value=20, deps=[E10, E20])  ← node1 saw both
// E21(+1, deps=[S1]) on node1
// E22(+1, deps=[E20]) on node2  ← hasn't seen S1!
// E23(+1, deps=[E21]) on node1
// E24(+1, deps=[E22]) on node2
// Tips: [E23, E24]. Correct: 24
func TestGetSnapshot_SnapshotChainWithPartialVisibility(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)
	hlc := int64(0)
	nextHLC := func() int64 { hlc++; return hlc }

	// 10 increments on node1
	var prev1 Tip
	for range 10 {
		deps := []*pb.EffectRef{}
		if prev1 != (Tip{}) {
			deps = []*pb.EffectRef{toPbRef(prev1)}
		}
		prev1 = log.putEffect(&pb.Effect{
			Key: []byte("k"), Hlc: sTs(nextHLC()), NodeId: 1, Deps: deps,
			Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
		})
	}

	// 10 increments on node2 (independent, no shared root)
	var prev2 Tip
	for range 10 {
		deps := []*pb.EffectRef{}
		if prev2 != (Tip{}) {
			deps = []*pb.EffectRef{toPbRef(prev2)}
		}
		prev2 = log.putEffect(&pb.Effect{
			Key: []byte("k"), Hlc: sTs(nextHLC()), NodeId: 2, Deps: deps,
			Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
		})
	}

	// Snapshot S1 on node1: saw both branches
	s1 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(nextHLC()), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(prev1), toPbRef(prev2)},
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
			Collection: pb.CollectionKind_SCALAR,
			State: &pb.ReducedEffect{
				Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_ADDITIVE_INT,
				Collection: pb.CollectionKind_SCALAR, Commutative: true,
				Hlc: sTs(hlc), NodeId: 1,
				Scalar: &pb.DataEffect{
					Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_ADDITIVE_INT,
					Value: &pb.DataEffect_IntVal{IntVal: 20},
				},
			},
		}},
	})

	// Node1 continues from snapshot
	e21 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(nextHLC()), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(s1)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	tip1 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(nextHLC()), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(e21)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	// Node2 continues from E20 (hasn't seen S1)
	e22 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(nextHLC()), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(prev2)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})
	tip2 := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(nextHLC()), NodeId: 2, Deps: []*pb.EffectRef{toPbRef(e22)},
		Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
	})

	e.index.Insert("k", nil, keytrie.NewTipSet(tip1, tip2))

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	// 10+10 + 2+2 = 24
	if r.Scalar.GetIntVal() != 24 {
		t.Fatalf("expected 24, got %d", r.Scalar.GetIntVal())
	}
}

// TestGetSnapshot_ManyForksAndMerges simulates rapid fork-merge cycles
// like Jepsen produces under partition/heal. 100 total increments across
// 5 nodes with merge points where nodes observe each other's effects.
func TestGetSnapshot_ManyForksAndMerges(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)
	hlc := int64(0)
	nextHLC := func() int64 { hlc++; return hlc }

	const nNodes = 5
	const incrsPerRound = 4
	const rounds = 5

	tips := make([]Tip, nNodes)
	totalIncrements := 0

	for round := range rounds {
		// Each node does incrsPerRound increments from its last tip
		for node := range uint32(nNodes) {
			for range incrsPerRound {
				deps := []*pb.EffectRef{}
				if tips[node] != (Tip{}) {
					deps = []*pb.EffectRef{toPbRef(tips[node])}
				}
				tips[node] = log.putEffect(&pb.Effect{
					Key: []byte("k"), Hlc: sTs(nextHLC()), NodeId: uint64(node) + 1, Deps: deps,
					Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
				})
				totalIncrements++
			}
		}

		// After each round, node 0 merges: creates an effect depending on ALL tips
		if round < rounds-1 {
			mergeEff := log.putEffect(&pb.Effect{
				Key: []byte("k"), Hlc: sTs(nextHLC()), NodeId: 1, Deps: toPbRefs(tips[:]),
				Kind: &pb.Effect_Data{Data: scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 1)},
			})
			totalIncrements++
			// All nodes start next round from the merge point
			for i := range tips {
				tips[i] = mergeEff
			}
		}
	}

	// Final tips: each node's last effect from the final round
	ts := keytrie.NewTipSet(tips[:]...)
	e.index.Insert("k", nil, ts)

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	if r.Scalar.GetIntVal() != int64(totalIncrements) {
		t.Fatalf("expected %d, got %d", totalIncrements, r.Scalar.GetIntVal())
	}
}

// TestGetSnapshot_AbortedCrossKeyBind_JepsenG1aRepro reproduces the first G1a
// anomaly from Jepsen run 25887292245.
//
// Reader 10.0.0.69 reconstructed el-3 at 21:44:18.732866 and returned
// [48, 53] — but the writer of `53` had already aborted (watch-conflict)
// because its bind lost a fork-choice race on el-2 against a concurrent
// bind from a third node. The aborted bind's effects MUST NOT surface in
// the reader's reconstruction; the verdict must be derivable from DAG +
// fork-choice alone (no abort propagation).
//
// Setup mirrors the reader's DAG at the moment of the dirty read:
//
//	el-3 tip: bind X = {…81326252873, 28}
//	  ↳ X.deps include xDataEl3 = {…81326252873, 27} ("53")
//	  ↳ chain → xPrev = {…81326252873, 21} ("48", pre-X commit)
//	el-2 tips: { bind X, bind Y }
//	  bind Y = {…83178718270, 20} (competitor, lower hash → wins)
//	  bind X's NewTip on el-2 = xDataEl2 = {…81326252873, 26} ("52")
//
// bindKeyClosure(el-3) must expand to {el-3, el-2} (bind X touches both).
// losersOnKey(el-2) must pair-wise hash-compare X vs Y; X loses.
// Atomic-across-keys: X is in the loser set on el-3 too → its data
// effects (including "53") are skipped.
//
// Expected reconstruction of el-3: ["48"], not ["48", "53"].
func TestGetSnapshot_AbortedCrossKeyBind_JepsenG1aRepro(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	// Node IDs lifted verbatim from the failing run.
	const (
		nodeWriter     uint64 = 7639866581326252873 // 10.0.0.206 — emits bind X
		nodeCompetitor uint64 = 7639866583178718270 // 10.0.2.228 — emits bind Y
	)

	putAt := func(tip Tip, eff *pb.Effect) Tip {
		if len(eff.ForkChoiceHash) == 0 {
			eff.ForkChoiceHash = ComputeForkChoiceHash(pb.NodeID(eff.NodeId), eff.Hlc)
		}
		data, _ := proto.Marshal(eff)
		log.entries[tip] = data
		log.effectCache.Put(tip, proto.Clone(eff).(*pb.Effect))
		return tip
	}

	// Hashes chosen so Y < X (so X loses fork choice on el-2).
	xHash := []byte{0xff, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	yHash := []byte{0x00, 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	if !ForkChoiceLess(yHash, xHash) {
		t.Fatalf("test setup: expected Y < X by hash")
	}

	// Pre-existing committed element "48" on el-3 (a normal, non-tx data
	// effect). This is the value reads should still see after X aborts.
	xPrev := putAt(Tip{nodeWriter, 21}, &pb.Effect{
		Key: []byte("el-3"), Hlc: sTs(100), NodeId: nodeWriter,
		Kind: &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "el-3:48", []byte("48"))},
	})

	// Bind X's tx-marked data effects.
	txnX := "txn-X"
	xDataEl2 := putAt(Tip{nodeWriter, 26}, &pb.Effect{
		Key:   []byte("el-2"),
		Hlc:   sTs(200),
		NodeId: nodeWriter,
		TxnId: txnX,
		Kind:  &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "el-2:52", []byte("52"))},
	})
	xDataEl3 := putAt(Tip{nodeWriter, 27}, &pb.Effect{
		Key:   []byte("el-3"),
		Hlc:   sTs(210),
		NodeId: nodeWriter,
		TxnId: txnX,
		Deps:  []*pb.EffectRef{toPbRef(xPrev)},
		Kind:  &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "el-3:53", []byte("53"))},
	})

	// Bind X itself. NewTip per key: el-2 → xDataEl2, el-3 → xDataEl3.
	// Indexed under both keys; first key (el-3) is canonical.
	xBind := putAt(Tip{nodeWriter, 28}, &pb.Effect{
		Key:            []byte("el-3"),
		Hlc:            sTs(220),
		NodeId:         nodeWriter,
		TxnId:          txnX,
		ForkChoiceHash: xHash,
		Deps:           []*pb.EffectRef{toPbRef(xDataEl2), toPbRef(xDataEl3)},
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			TxnHlc:           sTs(220),
			OriginatorNodeId: nodeWriter,
			Keys: []*pb.TransactionalBindEffect_KeyBind{
				{Key: []byte("el-2"), NewTip: toPbRef(xDataEl2)},
				{Key: []byte("el-3"), NewTip: toPbRef(xDataEl3)},
			},
		}},
	})

	// Bind Y: concurrent with X on el-2 (no DAG dep either direction),
	// lower hash so wins fork-choice on el-2.
	txnY := "txn-Y"
	yBind := putAt(Tip{nodeCompetitor, 20}, &pb.Effect{
		Key:            []byte("el-2"),
		Hlc:            sTs(150),
		NodeId:         nodeCompetitor,
		TxnId:          txnY,
		ForkChoiceHash: yHash,
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			TxnHlc:           sTs(150),
			OriginatorNodeId: nodeCompetitor,
			Keys: []*pb.TransactionalBindEffect_KeyBind{
				{Key: []byte("el-2"), NewTip: &pb.EffectRef{NodeId: nodeCompetitor, Offset: 20}},
			},
		}},
	})

	// Index reflects the reader's state at .732866:
	//   el-3 tip: xBind (bind X)
	//   el-2 tips: {xBind (cross-key indexed), yBind}
	e.index.Insert("el-3", nil, keytrie.NewTipSet(xBind))
	e.index.Insert("el-2", nil, keytrie.NewTipSet(xBind, yBind))

	r, _, _, err := e.GetSnapshot("el-3")
	if err != nil {
		t.Fatal(err)
	}

	got := orderedValues(r)
	if slices.Contains(got, "53") {
		t.Fatalf("G1a: aborted bind X's effect '53' visible in el-3 reconstruction: %v", got)
	}
	if !slices.Contains(got, "48") {
		t.Fatalf("expected baseline '48' to be visible in el-3 reconstruction, got %v", got)
	}
}

// TestLosersOnKey_TwoConcurrentBindsLowerHashWins is the unit-level
// counterpart of TestGetSnapshot_AbortedCrossKeyBind_JepsenG1aRepro: it
// pokes losersOnKey directly with two concurrent binds on a single key
// and asserts the higher-hash bind is in the loser set.
func TestLosersOnKey_TwoConcurrentBindsLowerHashWins(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	const (
		nodeA uint64 = 7639866581326252873
		nodeB uint64 = 7639866583178718270
	)

	putAt := func(tip Tip, eff *pb.Effect) Tip {
		if len(eff.ForkChoiceHash) == 0 {
			eff.ForkChoiceHash = ComputeForkChoiceHash(pb.NodeID(eff.NodeId), eff.Hlc)
		}
		data, _ := proto.Marshal(eff)
		log.entries[tip] = data
		log.effectCache.Put(tip, proto.Clone(eff).(*pb.Effect))
		return tip
	}

	hashHigh := []byte{0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	hashLow := []byte{0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}

	bindHi := putAt(Tip{nodeA, 28}, &pb.Effect{
		Key:            []byte("el-2"),
		Hlc:            sTs(220),
		NodeId:         nodeA,
		TxnId:          "txn-hi",
		ForkChoiceHash: hashHigh,
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			TxnHlc:           sTs(220),
			OriginatorNodeId: nodeA,
			Keys: []*pb.TransactionalBindEffect_KeyBind{
				{Key: []byte("el-2"), NewTip: &pb.EffectRef{NodeId: nodeA, Offset: 28}},
			},
		}},
	})
	bindLo := putAt(Tip{nodeB, 20}, &pb.Effect{
		Key:            []byte("el-2"),
		Hlc:            sTs(150),
		NodeId:         nodeB,
		TxnId:          "txn-lo",
		ForkChoiceHash: hashLow,
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			TxnHlc:           sTs(150),
			OriginatorNodeId: nodeB,
			Keys: []*pb.TransactionalBindEffect_KeyBind{
				{Key: []byte("el-2"), NewTip: &pb.EffectRef{NodeId: nodeB, Offset: 20}},
			},
		}},
	})

	e.index.Insert("el-2", nil, keytrie.NewTipSet(bindHi, bindLo))

	losers, _ := e.losersOnKey("el-2", nil)
	if _, ok := losers["txn-hi"]; !ok {
		t.Fatalf("expected higher-hash bind 'txn-hi' to be in losers, got %v", losers)
	}
	if _, ok := losers["txn-lo"]; ok {
		t.Fatalf("lower-hash bind 'txn-lo' should not be in losers, got %v", losers)
	}
}

// TestLosersOnKey_XDependsOnYsAncestorsButNotNewTip reproduces the
// el-2 chain shape from Jepsen run 25887292245 with the actual deps:
//
//	Y (node nodeB): 9 → 10(tx) → 11(tx) → 12(N) → 14(tx) → 16(tx) → 17(tx) → bind 20
//	  Y.NewTip on el-2 = {nodeB, 17}
//	X (node nodeA): 19(N, deps include nodeB:{7,14,16}) → 22(tx) → 25(tx) → 26(tx) → bind 28
//	  X.NewTip on el-2 = {nodeA, 26}
//
// X's chain transitively pulls in some of Y's tx data effects (14, 16)
// via X's 19 deps, but never reaches Y.NewTip=17. The reaches() check
// in losersOnKey should therefore correctly classify X and Y as
// concurrent on el-2, and pairwise fork-choice (X.hash > Y.hash)
// should mark X as a loser.
//
// Production observed: 53 (X's append on el-3) appears in every read
// after the dirty read. If this test passes (X marked loser), the bug
// is NOT in the algorithm we modeled — it's in the data the production
// reader had vs. what we assumed.
func TestLosersOnKey_XDependsOnYsAncestorsButNotNewTip(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	const (
		nodeA uint64 = 7639866581326252873 // writer / X
		nodeB uint64 = 7639866583178718270 // competitor / Y
	)

	putAt := func(tip Tip, eff *pb.Effect) Tip {
		if len(eff.ForkChoiceHash) == 0 {
			eff.ForkChoiceHash = ComputeForkChoiceHash(pb.NodeID(eff.NodeId), eff.Hlc)
		}
		data, _ := proto.Marshal(eff)
		log.entries[tip] = data
		log.effectCache.Put(tip, proto.Clone(eff).(*pb.Effect))
		return tip
	}

	// Y's chain on el-2 (originator: nodeB).
	yRoot := putAt(Tip{nodeB, 9}, &pb.Effect{
		Key: []byte("el-2"), Hlc: sTs(50), NodeId: nodeB,
		Kind: &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "y-root", []byte("y-root"))},
	})
	y10 := putAt(Tip{nodeB, 10}, &pb.Effect{
		Key: []byte("el-2"), Hlc: sTs(51), NodeId: nodeB,
		TxnId: "txn-Y", Deps: []*pb.EffectRef{toPbRef(yRoot)},
		Kind: &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "y-10", []byte("y-10"))},
	})
	y11 := putAt(Tip{nodeB, 11}, &pb.Effect{
		Key: []byte("el-2"), Hlc: sTs(52), NodeId: nodeB,
		TxnId: "txn-Y", Deps: []*pb.EffectRef{toPbRef(y10)},
		Kind: &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "y-11", []byte("y-11"))},
	})
	y12 := putAt(Tip{nodeB, 12}, &pb.Effect{
		Key: []byte("el-2"), Hlc: sTs(53), NodeId: nodeB,
		Deps: []*pb.EffectRef{toPbRef(y11)},
		Kind: &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "y-12", []byte("y-12"))},
	})
	y14 := putAt(Tip{nodeB, 14}, &pb.Effect{
		Key: []byte("el-2"), Hlc: sTs(54), NodeId: nodeB,
		TxnId: "txn-Y", Deps: []*pb.EffectRef{toPbRef(y12)},
		Kind: &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "y-14", []byte("y-14"))},
	})
	y16 := putAt(Tip{nodeB, 16}, &pb.Effect{
		Key: []byte("el-2"), Hlc: sTs(55), NodeId: nodeB,
		TxnId: "txn-Y", Deps: []*pb.EffectRef{toPbRef(y14)},
		Kind: &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "y-16", []byte("y-16"))},
	})
	y17 := putAt(Tip{nodeB, 17}, &pb.Effect{
		Key: []byte("el-2"), Hlc: sTs(56), NodeId: nodeB,
		TxnId: "txn-Y", Deps: []*pb.EffectRef{toPbRef(y16)},
		Kind: &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "y-17", []byte("y-17"))},
	})

	// Bind Y: NewTip on el-2 = y17. Deps = [y17] (and Y's other-key
	// effects, omitted since they don't affect el-2's reachability).
	yHash := []byte{0x00, 0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	yBind := putAt(Tip{nodeB, 20}, &pb.Effect{
		Key: []byte("el-2"), Hlc: sTs(60), NodeId: nodeB,
		TxnId:          "txn-Y",
		ForkChoiceHash: yHash,
		Deps:           []*pb.EffectRef{toPbRef(y17)},
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			TxnHlc:           sTs(60),
			OriginatorNodeId: nodeB,
			Keys: []*pb.TransactionalBindEffect_KeyBind{
				{Key: []byte("el-2"), NewTip: toPbRef(y17)},
			},
		}},
	})

	// X's chain on el-2: 19(N, deps spanning {nodeB:7,14,16}) → 22(tx) → 25(tx) → 26(tx).
	// y7 needs to exist as a referenced effect for BFS to not error.
	y7 := putAt(Tip{nodeB, 7}, &pb.Effect{
		Key: []byte("el-2"), Hlc: sTs(40), NodeId: nodeB,
		Kind: &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "y-7", []byte("y-7"))},
	})
	x19 := putAt(Tip{nodeA, 19}, &pb.Effect{
		Key: []byte("el-2"), Hlc: sTs(70), NodeId: nodeA,
		Deps: []*pb.EffectRef{toPbRef(y7), toPbRef(y14), toPbRef(y16)},
		Kind: &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "x-19", []byte("x-19"))},
	})
	x22 := putAt(Tip{nodeA, 22}, &pb.Effect{
		Key: []byte("el-2"), Hlc: sTs(71), NodeId: nodeA,
		TxnId: "txn-X", Deps: []*pb.EffectRef{toPbRef(x19)},
		Kind: &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "x-22", []byte("x-22"))},
	})
	x25 := putAt(Tip{nodeA, 25}, &pb.Effect{
		Key: []byte("el-2"), Hlc: sTs(72), NodeId: nodeA,
		TxnId: "txn-X", Deps: []*pb.EffectRef{toPbRef(x22)},
		Kind: &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "x-25", []byte("x-25"))},
	})
	x26 := putAt(Tip{nodeA, 26}, &pb.Effect{
		Key: []byte("el-2"), Hlc: sTs(73), NodeId: nodeA,
		TxnId: "txn-X", Deps: []*pb.EffectRef{toPbRef(x25)},
		Kind: &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "x-26-52", []byte("52"))},
	})

	// Bind X: NewTip on el-2 = x26, NewTip on el-3 (placeholder) omitted
	// since this test only inspects el-2.
	xHash := []byte{0xff, 0x00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	if !ForkChoiceLess(yHash, xHash) {
		t.Fatalf("test setup: expected Y.hash < X.hash")
	}
	xBind := putAt(Tip{nodeA, 28}, &pb.Effect{
		Key: []byte("el-2"), Hlc: sTs(80), NodeId: nodeA,
		TxnId:          "txn-X",
		ForkChoiceHash: xHash,
		Deps:           []*pb.EffectRef{toPbRef(x26)},
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			TxnHlc:           sTs(80),
			OriginatorNodeId: nodeA,
			Keys: []*pb.TransactionalBindEffect_KeyBind{
				{Key: []byte("el-2"), NewTip: toPbRef(x26)},
			},
		}},
	})

	// Tips on el-2 mirror what the reader had at .732928:
	// {nodeB:10, nodeB:20=yBind, nodeA:26=x26 (still a tip — bind 28 didn't
	// consume it because X.ConsumedTips on el-2 didn't include it), nodeA:28=xBind}.
	e.index.Insert("el-2", nil, keytrie.NewTipSet(Tip{nodeB, 10}, yBind, x26, xBind))

	losers, _ := e.losersOnKey("el-2", nil)
	t.Logf("losersOnKey(el-2) returned: %v", losers)
	if _, ok := losers["txn-X"]; !ok {
		t.Fatalf("expected X (txn-X) to be marked as loser; production observed 53 surfacing on el-3 means X must be a loser on el-2. Got losers=%v", losers)
	}
	if _, ok := losers["txn-Y"]; ok {
		t.Fatalf("Y (txn-Y) should NOT be a loser (it has the lower hash). Got losers=%v", losers)
	}
}

// TestLosersOnKey_SharedAncestorConcurrentBinds reproduces the second
// G1a from Jepsen run 25890153673.
//
// Reader 10.0.0.208 reconstructed el-1 and the new `losersOnKey: verdict`
// debug log showed 7 binds visible on el-1, all with hash bytes from the
// run, and losers=[] — i.e. no bind is marked a loser. The aborted writer
// X (txn 7639884449200700087:…:6, hash bea34ea4) was in `binds_seen` but
// not in losers. Its value 180 surfaced on the reader.
//
// Tracing the deps in logs: X.NewTip on el-1 = [449200700087, 41], which
// reaches [450658684150, 43] via `41 → 40 → 39 → 43`. Offset 43 was
// emitted with `tx:false` (non-tx) and kind Noop — a committed phantom-
// write marker, NOT part of Y's commit branch (Y.TxnId differs; Y.NewTip
// is offset 51, not 43).
//
// Y's NewTip on el-1 = [450658684150, 51]. Y's causal past reaches 43
// via 51 → 48 → … → 44 → 43.
//
// So X and Y are concurrent siblings around shared ancestor 43. Neither
// causal past contains the other's NewTip. `losersOnKey` MUST classify
// them as concurrent and mark X (higher hash) as a loser.
//
// This test reproduces the minimal shape: shared non-tx Noop ancestor
// with two concurrent binds branching off.
func TestLosersOnKey_SharedAncestorConcurrentBinds(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	const (
		nodeX uint64 = 7639884449200700087 // X's writer
		nodeY uint64 = 7639884450658684150 // Y's writer (and ancestor's node)
		nodeM uint64 = 7639884447148611983 // root effect's node
	)

	putAt := func(tip Tip, eff *pb.Effect) Tip {
		if len(eff.ForkChoiceHash) == 0 {
			eff.ForkChoiceHash = ComputeForkChoiceHash(pb.NodeID(eff.NodeId), eff.Hlc)
		}
		data, _ := proto.Marshal(eff)
		log.entries[tip] = data
		log.effectCache.Put(tip, proto.Clone(eff).(*pb.Effect))
		return tip
	}

	// Root committed data effect on el-1 (any predecessor).
	root := putAt(Tip{nodeM, 28}, &pb.Effect{
		Key: []byte("el-1"), Hlc: sTs(10), NodeId: nodeM,
		Kind: &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "root", []byte("root"))},
	})

	// Shared ancestor: non-tx Noop on el-1 (phantom-write marker), the
	// kind production offset 43 is.
	sharedAncestor := putAt(Tip{nodeY, 43}, &pb.Effect{
		Key: []byte("el-1"), Hlc: sTs(20), NodeId: nodeY,
		Deps: []*pb.EffectRef{toPbRef(root)},
		Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	})

	// X's tx-data chain depends on sharedAncestor.
	xData := putAt(Tip{nodeX, 41}, &pb.Effect{
		Key: []byte("el-1"), Hlc: sTs(60), NodeId: nodeX,
		TxnId: "txn-X",
		Deps:  []*pb.EffectRef{toPbRef(sharedAncestor)},
		Kind:  &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "X-180", []byte("180"))},
	})

	// Y's tx-data chain also depends on sharedAncestor, with one extra hop.
	yIntermediate := putAt(Tip{nodeY, 48}, &pb.Effect{
		Key: []byte("el-1"), Hlc: sTs(40), NodeId: nodeY,
		TxnId: "txn-Y",
		Deps:  []*pb.EffectRef{toPbRef(sharedAncestor)},
		Kind:  &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "Y-pre", []byte("Y-pre"))},
	})
	yData := putAt(Tip{nodeY, 51}, &pb.Effect{
		Key: []byte("el-1"), Hlc: sTs(45), NodeId: nodeY,
		TxnId: "txn-Y",
		Deps:  []*pb.EffectRef{toPbRef(yIntermediate)},
		Kind:  &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "Y-data", []byte("Y-data"))},
	})

	// Hashes chosen so Y < X (matches production: 15d2c1af < bea34ea4).
	yHash := []byte{0x15, 0xd2, 0xc1, 0xaf, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	xHash := []byte{0xbe, 0xa3, 0x4e, 0xa4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	if !ForkChoiceLess(yHash, xHash) {
		t.Fatalf("test setup: expected Y < X by hash")
	}

	xBind := putAt(Tip{nodeX, 42}, &pb.Effect{
		Key: []byte("el-1"), Hlc: sTs(70), NodeId: nodeX,
		TxnId:          "txn-X",
		ForkChoiceHash: xHash,
		Deps:           []*pb.EffectRef{toPbRef(xData)},
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			TxnHlc: sTs(70), OriginatorNodeId: nodeX,
			Keys: []*pb.TransactionalBindEffect_KeyBind{
				{Key: []byte("el-1"), NewTip: toPbRef(xData)},
			},
		}},
	})

	yBind := putAt(Tip{nodeY, 53}, &pb.Effect{
		Key: []byte("el-1"), Hlc: sTs(55), NodeId: nodeY,
		TxnId:          "txn-Y",
		ForkChoiceHash: yHash,
		Deps:           []*pb.EffectRef{toPbRef(yData)},
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			TxnHlc: sTs(55), OriginatorNodeId: nodeY,
			Keys: []*pb.TransactionalBindEffect_KeyBind{
				{Key: []byte("el-1"), NewTip: toPbRef(yData)},
			},
		}},
	})

	e.index.Insert("el-1", nil, keytrie.NewTipSet(xBind, yBind))

	losers, _ := e.losersOnKey("el-1", nil)
	t.Logf("losersOnKey(el-1) returned: %v", losers)
	if _, ok := losers["txn-X"]; !ok {
		t.Fatalf("BUG: expected X (higher hash) to be marked as loser. X and Y are concurrent siblings around shared non-tx ancestor — neither reaches the other. Got losers=%v", losers)
	}
	if _, ok := losers["txn-Y"]; ok {
		t.Fatalf("Y (lower hash) should NOT be a loser. Got losers=%v", losers)
	}
}

// TestLosersOnKey_SideChannelMakesSequential reproduces the *actual*
// production scenario from Jepsen run 25890153673 — once we trace the
// real Deps fields from the reader's `dag.walk` log:
//
// The writer X's chain on el-1 reaches Y not through any shared
// ancestor, but through a *side-channel* effect on a third node:
//
//	X.NewTip = [449, 41] → 40 → 39 → {…, 453:70, …}
//	453:70 is a non-tx Noop emitted on node 453 with deps that include
//	[450, 51] (Y.NewTip!) and [450, 53] (Y.bind).
//
// So X is downstream of Y in the DAG. From any reader's reconstruct
// perspective, X is sequential-after-Y. losersOnKey correctly returns
// `losers=[]` (no fork-choice needed — they coexist).
//
// This means the BUG IS NOT in losersOnKey. It's in isRealConflict on
// the writer, which didn't check reaches() at NACK time, hashed-out,
// and aborted X — even though X had already observed Y transitively
// via the side-channel noop. Reads include X.value (180). Jepsen
// reports G1a because writer said :fail but value persists.
//
// This test asserts losersOnKey returns empty given the side-channel
// shape — i.e., the algorithm correctly identifies the sequential
// relationship. A separate test (or production fix) needs to make
// isRealConflict match this behavior.
func TestLosersOnKey_SideChannelMakesSequential(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	const (
		nodeX     uint64 = 7639884449200700087 // X's writer
		nodeY     uint64 = 7639884450658684150 // Y's writer
		nodeThird uint64 = 7639884453256734367 // node hosting the side-channel noop
	)

	putAt := func(tip Tip, eff *pb.Effect) Tip {
		if len(eff.ForkChoiceHash) == 0 {
			eff.ForkChoiceHash = ComputeForkChoiceHash(pb.NodeID(eff.NodeId), eff.Hlc)
		}
		data, _ := proto.Marshal(eff)
		log.entries[tip] = data
		log.effectCache.Put(tip, proto.Clone(eff).(*pb.Effect))
		return tip
	}

	// Root effect on el-1.
	root := putAt(Tip{nodeY, 19}, &pb.Effect{
		Key: []byte("el-1"), Hlc: sTs(5), NodeId: nodeY,
		Kind: &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "root", []byte("root"))},
	})

	// Y's tx-data and bind on el-1 — committed earlier.
	yData := putAt(Tip{nodeY, 51}, &pb.Effect{
		Key: []byte("el-1"), Hlc: sTs(30), NodeId: nodeY,
		TxnId: "txn-Y",
		Deps:  []*pb.EffectRef{toPbRef(root)},
		Kind:  &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "Y", []byte("Y"))},
	})
	yHash := []byte{0x15, 0xd2, 0xc1, 0xaf, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	yBind := putAt(Tip{nodeY, 53}, &pb.Effect{
		Key: []byte("el-1"), Hlc: sTs(35), NodeId: nodeY,
		TxnId:          "txn-Y",
		ForkChoiceHash: yHash,
		Deps:           []*pb.EffectRef{toPbRef(yData)},
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			TxnHlc: sTs(35), OriginatorNodeId: nodeY,
			Keys: []*pb.TransactionalBindEffect_KeyBind{
				{Key: []byte("el-1"), NewTip: toPbRef(yData)},
			},
		}},
	})

	// Side-channel: a non-tx Noop on a third node whose deps include
	// Y.NewTip and Y.bind. This is the kind of effect that gets emitted
	// when some other operation reads el-1 after Y commits — its
	// observation marker references Y.
	sideChannel := putAt(Tip{nodeThird, 70}, &pb.Effect{
		Key: []byte("el-1"), Hlc: sTs(50), NodeId: nodeThird,
		Deps: []*pb.EffectRef{toPbRef(yData), toPbRef(yBind)},
		Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	})

	// X's chain — emitted AFTER the side-channel exists. X's intermediate
	// effect 39 has the side-channel as a dep, mirroring production where
	// 39.deps = [450:43, 453:69, 453:70, 449:38].
	x39 := putAt(Tip{nodeX, 39}, &pb.Effect{
		Key: []byte("el-1"), Hlc: sTs(60), NodeId: nodeX,
		TxnId: "txn-X",
		Deps:  []*pb.EffectRef{toPbRef(sideChannel)},
		Kind:  &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	})
	x40 := putAt(Tip{nodeX, 40}, &pb.Effect{
		Key: []byte("el-1"), Hlc: sTs(61), NodeId: nodeX,
		TxnId: "txn-X", Deps: []*pb.EffectRef{toPbRef(x39)},
		Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	})
	xData := putAt(Tip{nodeX, 41}, &pb.Effect{
		Key: []byte("el-1"), Hlc: sTs(62), NodeId: nodeX,
		TxnId: "txn-X", Deps: []*pb.EffectRef{toPbRef(x40)},
		Kind: &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "X-180", []byte("180"))},
	})
	xHash := []byte{0xbe, 0xa3, 0x4e, 0xa4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	if !ForkChoiceLess(yHash, xHash) {
		t.Fatalf("test setup: expected Y < X by hash")
	}
	xBind := putAt(Tip{nodeX, 42}, &pb.Effect{
		Key: []byte("el-1"), Hlc: sTs(70), NodeId: nodeX,
		TxnId:          "txn-X",
		ForkChoiceHash: xHash,
		Deps:           []*pb.EffectRef{toPbRef(xData)},
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			TxnHlc: sTs(70), OriginatorNodeId: nodeX,
			Keys: []*pb.TransactionalBindEffect_KeyBind{
				{Key: []byte("el-1"), NewTip: toPbRef(xData)},
			},
		}},
	})

	e.index.Insert("el-1", nil, keytrie.NewTipSet(xBind, yBind))

	losers, _ := e.losersOnKey("el-1", nil)
	t.Logf("losersOnKey(el-1) returned: %v", losers)
	if len(losers) != 0 {
		t.Fatalf("X reaches Y via side-channel noop — they are sequential, not concurrent. losersOnKey must return no losers. Got %v", losers)
	}
}

// TestBindKeyClosure_ExpandsViaBindKeysField asserts bindKeyClosure
// follows a bind's Keys list, so a reconstruction on one key picks up
// loser verdicts on every key the bind touches.
func TestBindKeyClosure_ExpandsViaBindKeysField(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log, nil)

	const nodeA uint64 = 7639866581326252873

	putAt := func(tip Tip, eff *pb.Effect) Tip {
		if len(eff.ForkChoiceHash) == 0 {
			eff.ForkChoiceHash = ComputeForkChoiceHash(pb.NodeID(eff.NodeId), eff.Hlc)
		}
		data, _ := proto.Marshal(eff)
		log.entries[tip] = data
		log.effectCache.Put(tip, proto.Clone(eff).(*pb.Effect))
		return tip
	}

	// Bind X touches el-2, el-3, el-4 (the actual Jepsen txn shape).
	bind := putAt(Tip{nodeA, 28}, &pb.Effect{
		Key:    []byte("el-3"),
		Hlc:    sTs(220),
		NodeId: nodeA,
		TxnId:  "txn-X",
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			TxnHlc:           sTs(220),
			OriginatorNodeId: nodeA,
			Keys: []*pb.TransactionalBindEffect_KeyBind{
				{Key: []byte("el-2"), NewTip: &pb.EffectRef{NodeId: nodeA, Offset: 26}},
				{Key: []byte("el-3"), NewTip: &pb.EffectRef{NodeId: nodeA, Offset: 27}},
				{Key: []byte("el-4"), NewTip: &pb.EffectRef{NodeId: nodeA, Offset: 23}},
			},
		}},
	})
	e.index.Insert("el-3", nil, keytrie.NewTipSet(bind))

	closure, _, err := e.bindKeyClosure("el-3")
	if err != nil {
		t.Fatalf("bindKeyClosure: %v", err)
	}
	for _, want := range []string{"el-2", "el-3", "el-4"} {
		if _, ok := closure[want]; !ok {
			t.Fatalf("bindKeyClosure(el-3) missing %q; got %v", want, closure)
		}
	}
}
