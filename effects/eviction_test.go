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
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/keytrie"
)

// TestHandleEviction_SystemKey_NoOp asserts that handleEviction is a
// no-op for "__swytch:" keys. Reaching this path indicates a violated
// pin invariant (the cache's evictDecider should have refused), so the
// engine logs a warning but does not drop any state — system keys are
// load-bearing for cluster operations and must not be torn down
// silently.
func TestHandleEviction_SystemKey_NoOp(t *testing.T) {
	bc := &mockBroadcaster{}
	e := newTestEngine(bc)

	// Pre-populate state for the "system" key as if a subscription
	// already exists. This is what handleEviction would tear down for
	// a user key.
	const key = "__swytch:members"
	e.subscriptions.Store(key, &subscriptionState{ready: make(chan struct{})})
	tip := Tip{1, 42}
	e.index.Insert(key, nil, keytrie.NewTipSet(tip))

	eff := &pb.Effect{Key: []byte(key)}
	e.handleEviction(tip, eff)

	if _, ok := e.subscriptions.Load(key); !ok {
		t.Fatal("handleEviction dropped subscription for a __swytch: key; pin invariant should keep it")
	}
	if e.index.Contains(key) == nil {
		t.Fatal("handleEviction dropped index entry for a __swytch: key")
	}
	if len(bc.broadcasts) != 0 {
		t.Fatalf("handleEviction broadcast for a __swytch: key; expected 0 messages, got %d", len(bc.broadcasts))
	}
}

// TestHandleEviction_UserKey_DropsAndUnsubs covers the production path:
// the cache lost the bytes for a user-key effect, so handleEviction
// must drop the key from the local index, drop the subscription
// state, and broadcast a wire-level unsubscribe so peers know we no
// longer have authority.
func TestHandleEviction_UserKey_DropsAndUnsubs(t *testing.T) {
	bc := &mockBroadcaster{}
	e := newTestEngine(bc)

	const key = "user:k"
	e.subscriptions.Store(key, &subscriptionState{ready: make(chan struct{})})
	tip := Tip{1, 42}
	e.index.Insert(key, nil, keytrie.NewTipSet(tip))

	eff := &pb.Effect{Key: []byte(key)}
	e.handleEviction(tip, eff)

	if _, ok := e.subscriptions.Load(key); ok {
		t.Fatal("handleEviction did not drop subscription state")
	}
	if e.index.Contains(key) != nil {
		t.Fatal("handleEviction did not drop the index entry")
	}
	if len(bc.broadcasts) != 1 {
		t.Fatalf("expected 1 wire broadcast (the unsubscribe); got %d", len(bc.broadcasts))
	}
	// The broadcast must carry a SubscriptionEffect with Unsubscribe=true
	// on the right key. Round-trip through the wire format to confirm.
	got := bc.broadcasts[0]
	if string(got.Key) != key {
		t.Fatalf("broadcast key = %q, want %q", got.Key, key)
	}
	// The notify's EffectData is wire-format [keyLen][key][protoData].
	protoStart := 4 + len(got.Key)
	if len(got.EffectData) <= protoStart {
		t.Fatal("broadcast EffectData truncated")
	}
	parsed := &pb.Effect{}
	if err := UnmarshalEffect(got.EffectData[protoStart:], parsed); err != nil {
		t.Fatalf("re-parse broadcast effect: %v", err)
	}
	sub := parsed.GetSubscription()
	if sub == nil {
		t.Fatal("broadcast was not a SubscriptionEffect")
	}
	if !sub.Unsubscribe {
		t.Fatal("SubscriptionEffect.Unsubscribe = false; expected true")
	}
	// DAG correctness: the unsub references the prior tip as a Dep so
	// peers reduce us out of the subscribers map. Without this the
	// effect is an orphan leaf that never overrides the prior
	// subscription.
	if len(parsed.Deps) != 1 || r(parsed.Deps[0]) != tip {
		t.Fatalf("unsub Deps = %v; want [%v] to causally consume the prior subscription tip", parsed.Deps, tip)
	}
	// Offset comes from nextOffset (monotonic sequence in our nodeID
	// namespace), not a synthetic hlc-derived value.
	gotOff := r(got.Origin)
	if gotOff[0] != uint64(e.nodeID) {
		t.Fatalf("unsub offset NodeID = %d; want %d (this engine's nodeID)", gotOff[0], e.nodeID)
	}
	if gotOff[1] == 0 {
		t.Fatal("unsub offset sequence is 0; expected nextOffset to have produced a non-zero seq")
	}
}

// TestHandleEviction_ShutdownShortCircuit asserts that a callback
// firing during engine shutdown does not perform any state changes —
// the broadcaster may already be torn down, and any local mutation
// is wasted work since the engine is going away.
func TestHandleEviction_ShutdownShortCircuit(t *testing.T) {
	bc := &mockBroadcaster{}
	e := newTestEngine(bc)

	const key = "user:k"
	e.subscriptions.Store(key, &subscriptionState{ready: make(chan struct{})})
	e.index.Insert(key, nil, keytrie.NewTipSet(Tip{1, 42}))
	e.closed.Store(true)

	e.handleEviction(Tip{1, 42}, &pb.Effect{Key: []byte(key)})

	if _, ok := e.subscriptions.Load(key); !ok {
		t.Fatal("shutdown short-circuit failed; subscription was dropped")
	}
	if e.index.Contains(key) == nil {
		t.Fatal("shutdown short-circuit failed; index entry was dropped")
	}
	if len(bc.broadcasts) != 0 {
		t.Fatalf("shutdown short-circuit failed; expected 0 broadcasts, got %d", len(bc.broadcasts))
	}
}

// TestNewEngine_PinsSystemKeysInCache verifies the end-to-end wiring:
// constructing an Engine via NewEngine installs the cache decider so
// that "__swytch:" effects survive memory pressure while user-key
// effects are evicted. This is the regression test for the original
// bootstrap-incomplete bug.
func TestNewEngine_PinsSystemKeysInCache(t *testing.T) {
	bc := &mockBroadcaster{}
	e := NewEngine(EngineConfig{
		NodeID:      1,
		Index:       keytrie.New(),
		Broadcaster: bc,
		DefaultMode: UnsafeMode,
		MemoryLimit: 4096, // tiny — easy to overflow
	})
	defer func() { _ = e.Close() }()

	// Seed a membership-style effect.
	systemKey := "__swytch:members"
	systemEff := &pb.Effect{
		Key:            []byte(systemKey),
		Hlc:            sTs(1),
		NodeId:         1,
		ForkChoiceHash: ComputeForkChoiceHash(1, sTs(1)),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte("node-1"),
			Value:      &pb.DataEffect_Raw{Raw: []byte("10.0.0.1:7379")},
		}},
	}
	systemTip := Tip{1, 1}
	e.effectCache.Put(systemTip, systemEff)
	// Bump frequency so the protected-freq fallback would also try to
	// evict it — the decider must override regardless.
	for range 5 {
		_, _ = e.effectCache.Get(systemTip, 0)
	}

	// Overflow with user-key effects. The exact payload size doesn't
	// matter — we keep going until evictions happen.
	for i := range uint64(5000) {
		userEff := &pb.Effect{
			Key:            []byte("user:k"),
			Hlc:            sTs(int64(i + 2)),
			NodeId:         1,
			ForkChoiceHash: ComputeForkChoiceHash(1, sTs(int64(i+2))),
			Kind: &pb.Effect_Data{Data: &pb.DataEffect{
				Op:         pb.EffectOp_INSERT_OP,
				Merge:      pb.MergeRule_LAST_WRITE_WINS,
				Collection: pb.CollectionKind_SCALAR,
				Value:      &pb.DataEffect_Raw{Raw: make([]byte, 256)},
			}},
		}
		e.effectCache.Put(Tip{1, 1000 + i}, userEff)
	}

	// Allow any async handleEviction callbacks a moment to settle.
	time.Sleep(50 * time.Millisecond)

	// The system key's effect must still be in the cache.
	if _, ok := e.effectCache.Get(systemTip, 0); !ok {
		t.Fatal("system __swytch:members effect was evicted; pin failed")
	}
	// And evictions must have occurred (otherwise our pressure
	// assertion didn't actually exercise the decider).
	_, _, evictions := e.effectCache.Stats()
	if evictions == 0 {
		t.Fatal("no evictions occurred; test did not exercise the decider path")
	}
}

// TestHandleEviction_SerializesWithConcurrentLocalWrites guards the
// race that motivated taking the per-key striped lock in
// handleEviction: a local emit and an eviction running concurrently
// on the same key. Without the lock, the eviction could snapshot
// tips before the emit's updateIndex landed, broadcast an unsub
// referencing a stale tip set, then Delete the index — silently
// dropping the new tip locally even though peers think the key was
// torn down. With the lock, the emit either fully precedes (its
// tip is in the unsub's Deps and gone from the index after Delete)
// or fully follows (its tip is added after Delete, so the index
// reflects the new authoritative state).
func TestHandleEviction_SerializesWithConcurrentLocalWrites(t *testing.T) {
	bc := &mockBroadcaster{}
	e := newTestEngine(bc)

	const key = "user:contended"
	const iterations = 200

	for i := range iterations {
		// Pre-seed a tip so handleEviction has something to drop.
		seedTip := Tip{1, uint64(1000 + i)}
		e.subscriptions.Store(key, &subscriptionState{ready: make(chan struct{})})
		e.index.Insert(key, nil, keytrie.NewTipSet(seedTip))

		// Concurrent: handleEviction tears down; an Emit-style write
		// adds a fresh tip via the same per-key lock the eviction
		// path now takes.
		newTip := Tip{1, uint64(2000 + i)}
		evictionDone := make(chan struct{})
		writeDone := make(chan struct{})

		go func() {
			defer close(evictionDone)
			e.handleEviction(seedTip, &pb.Effect{Key: []byte(key)})
		}()
		go func() {
			defer close(writeDone)
			mu := e.GetLock(key)
			mu.Lock()
			e.updateIndex(key, nil, newTip)
			mu.Unlock()
		}()

		<-evictionDone
		<-writeDone

		// Post-condition: index must NOT be in a torn state. Either:
		//   (a) eviction ran first → write's newTip is the sole tip
		//       (eviction deleted, write re-added under the same key).
		//   (b) write ran first → eviction's snapshot saw both
		//       seedTip and newTip, broadcast an unsub consuming
		//       both, then deleted. Index has no entry.
		// What MUST NOT happen: eviction snapshots only seedTip,
		// emit lands newTip while eviction holds nothing, eviction
		// Deletes the key, and newTip is silently lost.
		tips := e.index.Contains(key)
		if tips == nil {
			// case (b): both consumed by the unsub before Delete. Fine.
			continue
		}
		got := tips.Tips()
		// case (a): only newTip should remain.
		if len(got) != 1 || got[0] != newTip {
			t.Fatalf("iter %d: torn index after race; expected [%v] or nil, got %v",
				i, newTip, got)
		}
	}
}
