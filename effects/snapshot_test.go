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
