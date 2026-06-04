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
	e.handleEviction(tip, eff, e.index.Contains(key))

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
	e.handleEviction(tip, eff, e.index.Contains(key))

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

	e.handleEviction(Tip{1, 42}, &pb.Effect{Key: []byte(key)}, e.index.Contains(key))

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

// TestNewEngine_PinsSystemKeysInCache verifies that the memory-pressure
// sweep pins "__swytch:" effects while evicting user-key effects that are
// not in any active path. This is the regression test for the original
// bootstrap-incomplete bug (a membership effect must never be swept).
func TestNewEngine_PinsSystemKeysInCache(t *testing.T) {
	bc := &mockBroadcaster{}
	e := NewEngine(EngineConfig{
		NodeID:      1,
		Broadcaster: bc,
		DefaultMode: UnsafeMode,
	})
	defer func() { _ = e.Close() }()

	// Seed a membership-style effect. It has no index entry, so it is not
	// in any active path — only the system-key pin should keep it.
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

	// User-key effects with no index entry — below-LCA / orphan from the
	// sweep's perspective, so all should be evicted.
	for i := range uint64(50) {
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

	// Run the sweep directly (the governor fires it on its tick under
	// memory pressure; here we drive it deterministically).
	e.sweepBelowLCA()

	// The system key's effect must survive the sweep.
	if _, ok := e.effectCache.Get(systemTip, 0); !ok {
		t.Fatal("system __swytch:members effect was evicted; pin failed")
	}
	// User effects must have been evicted.
	_, _, evictions := e.effectCache.Stats()
	if evictions == 0 {
		t.Fatal("no evictions occurred; sweep did not exercise victim selection")
	}
	if _, ok := e.effectCache.Get(Tip{1, 1000}, 0); ok {
		t.Fatal("user-key effect survived the sweep; victim selection failed")
	}
}

// TestHandleEviction_CASProtectsConcurrentWrites verifies that
// handleEviction's CAS-based DeleteAndSnapshot backs off when a
// concurrent write changes the tip set between snapshot and delete.
func TestHandleEviction_CASProtectsConcurrentWrites(t *testing.T) {
	bc := &mockBroadcaster{}
	e := newTestEngine(bc)

	const key = "user:contended"
	const iterations = 200

	for i := range iterations {
		seedTip := Tip{1, uint64(1000 + i)}
		seedEff := &pb.Effect{Key: []byte(key)}
		e.subscriptions.Store(key, &subscriptionState{ready: make(chan struct{})})
		e.index.Insert(key, nil, keytrie.NewTipSet(seedTip))
		e.effectCache.Put(seedTip, seedEff)

		newTip := Tip{1, uint64(2000 + i)}
		evictionDone := make(chan struct{})
		writeDone := make(chan struct{})

		triggerTips := e.index.Contains(key)
		go func() {
			defer close(evictionDone)
			e.handleEviction(seedTip, &pb.Effect{Key: []byte(key)}, triggerTips)
		}()
		go func() {
			defer close(writeDone)
			e.updateIndex(key, nil, newTip)
		}()

		<-evictionDone
		<-writeDone

		tips := e.index.Contains(key)
		if tips == nil {
			// Delete won — key is gone. Verify no torn state:
			// the leaf must not be reachable with nil tips.
			continue
		}
		if !tips.Contains(newTip) {
			t.Fatalf("iter %d: key exists but newTip %v missing; got %v — torn state",
				i, newTip, tips.Tips())
		}

		// Clean up for next iteration: remove whatever's there.
		e.index.DeleteAndSnapshot(key, tips)
	}
}
