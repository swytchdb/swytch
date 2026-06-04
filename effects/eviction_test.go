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
	"sync"
	"testing"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/keytrie"
)

// TestEvictBounded_PinsSystemKey asserts the eviction sweep never selects a
// "__swytch:" key as a victim (the registered decider pins it), while a
// user key with the same frequency is evicted.
func TestEvictBounded_PinsSystemKey(t *testing.T) {
	bc := &mockBroadcaster{}
	e := newTestEngine(bc)
	e.index.SetEvictHooks(
		func(key string) bool { return !isSystemKey([]byte(key)) },
		e.onLeafEvicted,
	)

	const sys = "__swytch:members"
	const usr = "user:k"
	e.index.Insert(sys, nil, keytrie.NewTipSet(Tip{1, 1}))
	e.index.Insert(usr, nil, keytrie.NewTipSet(Tip{1, 2}))

	for i := 0; i < 10 && e.index.Contains(usr) != nil; i++ {
		e.index.EvictBounded(64)
	}

	if e.index.Contains(sys) == nil {
		t.Fatal("system __swytch:members key was evicted; decider pin failed")
	}
	if e.index.Contains(usr) != nil {
		t.Fatal("user key was not evicted by the sweep")
	}
}

// TestFlushIndex_ReleasesTipRefs verifies the trie-owned refcount: FlushIndex
// deletes keys via the trie's Delete, which fires refDelta so the index-tip
// reference is released and reclaimUnreferenced can free the vertex. Before the
// trie-owned-refcount refactor, Delete bypassed the manual refcount protocol
// and the vertex leaked permanently.
func TestFlushIndex_ReleasesTipRefs(t *testing.T) {
	e := newTestEngine(&mockBroadcaster{})
	e.index.SetRefDelta(e.applyRefDelta)

	const key = "user:k"
	tip := Tip{1, 5}
	// Resident + indexed: PutSized makes the vertex present (incref/decref no-op
	// on absent tips), Insert fires refDelta to incref the tip (refs=1).
	e.effectCache.PutSized(tip, &pb.Effect{Key: []byte(key)}, 10)
	e.index.Insert(key, nil, keytrie.NewTipSet(tip))

	// While it is a live index tip, reclaim must not free it.
	e.reclaimUnreferenced()
	if _, ok := e.effectCache.Get(tip); !ok {
		t.Fatal("referenced index tip was reclaimed")
	}

	e.FlushIndex()
	e.reclaimUnreferenced()
	if _, ok := e.effectCache.Get(tip); ok {
		t.Fatal("FlushIndex did not release the tip refcount; vertex leaked")
	}
}

// TestRemoveTips_ReleasesPrunedRef verifies reconstruct's stale-tip pruning path
// now releases refs: RemoveTips fires refDelta for the dropped tips while the
// kept tips stay referenced. Before the refactor RemoveTips skipped the decref
// and the pruned vertex leaked.
func TestRemoveTips_ReleasesPrunedRef(t *testing.T) {
	e := newTestEngine(&mockBroadcaster{})
	e.index.SetRefDelta(e.applyRefDelta)

	const key = "user:k"
	keep := Tip{1, 5}
	stale := Tip{1, 6}
	e.effectCache.PutSized(keep, &pb.Effect{Key: []byte(key)}, 10)
	e.effectCache.PutSized(stale, &pb.Effect{Key: []byte(key)}, 10)
	e.index.Insert(key, nil, keytrie.NewTipSet(keep, stale))

	e.index.RemoveTips(key, []Tip{stale})
	e.reclaimUnreferenced()

	if _, ok := e.effectCache.Get(keep); !ok {
		t.Fatal("kept index tip was wrongly reclaimed")
	}
	if _, ok := e.effectCache.Get(stale); ok {
		t.Fatal("RemoveTips did not release the pruned tip; vertex leaked")
	}
}

// TestBroadcastUnsubscribe_EmitsUnsub covers the eviction teardown: dropping a
// key emits a wire-level unsubscribe (so peers reduce us out of the subscriber
// set) as a proper DAG element consuming the prior tip, and drops local
// subscription state.
func TestBroadcastUnsubscribe_EmitsUnsub(t *testing.T) {
	bc := &mockBroadcaster{}
	e := newTestEngine(bc)

	const key = "user:k"
	e.subscriptions.Store(key, &subscriptionState{ready: make(chan struct{})})
	tip := Tip{1, 42}

	e.broadcastUnsubscribe(key, []Tip{tip})

	if _, ok := e.subscriptions.Load(key); ok {
		t.Fatal("did not drop subscription state")
	}
	if len(bc.broadcasts) != 1 {
		t.Fatalf("expected 1 wire broadcast (the unsubscribe); got %d", len(bc.broadcasts))
	}
	got := bc.broadcasts[0]
	if string(got.Key) != key {
		t.Fatalf("broadcast key = %q, want %q", got.Key, key)
	}
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
	// DAG correctness: the unsub references the prior tip as a Dep so peers
	// reduce us out of the subscribers map.
	if len(parsed.Deps) != 1 || r(parsed.Deps[0]) != tip {
		t.Fatalf("unsub Deps = %v; want [%v] to causally consume the prior tip", parsed.Deps, tip)
	}
	gotOff := r(got.Origin)
	if gotOff[0] != uint64(e.nodeID) {
		t.Fatalf("unsub offset NodeID = %d; want %d", gotOff[0], e.nodeID)
	}
	if gotOff[1] == 0 {
		t.Fatal("unsub offset sequence is 0; expected nextOffset to produce a non-zero seq")
	}
}

// TestBroadcastUnsubscribe_ShutdownShortCircuit asserts teardown during
// shutdown performs no state changes — the broadcaster may already be gone.
func TestBroadcastUnsubscribe_ShutdownShortCircuit(t *testing.T) {
	bc := &mockBroadcaster{}
	e := newTestEngine(bc)

	const key = "user:k"
	e.subscriptions.Store(key, &subscriptionState{ready: make(chan struct{})})
	e.closed.Store(true)

	e.broadcastUnsubscribe(key, []Tip{{1, 42}})

	if _, ok := e.subscriptions.Load(key); !ok {
		t.Fatal("shutdown short-circuit failed; subscription was dropped")
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

	// Run reclamation directly (the governor fires it on its tick under
	// memory pressure; here we drive it deterministically). These effects
	// have no index tip and no subdag, so their refcount is zero.
	e.reclaimUnreferenced()

	// The system key's effect must survive the sweep.
	if _, ok := e.effectCache.Get(systemTip); !ok {
		t.Fatal("system __swytch:members effect was evicted; pin failed")
	}
	// User effects must have been evicted.
	_, _, evictions := e.effectCache.Stats()
	if evictions == 0 {
		t.Fatal("no evictions occurred; sweep did not exercise victim selection")
	}
	if _, ok := e.effectCache.Get(Tip{1, 1000}); ok {
		t.Fatal("user-key effect survived the sweep; victim selection failed")
	}
}

// TestEvictBounded_CASProtectsConcurrentWrites verifies that the eviction
// sweep's soft-delete and a concurrent updateIndex don't produce torn state:
// either the evict wins (key gone) or the write wins (key present with the new
// tip), never a half-applied mix.
func TestEvictBounded_CASProtectsConcurrentWrites(t *testing.T) {
	bc := &mockBroadcaster{}
	e := newTestEngine(bc)
	e.index.SetEvictHooks(func(string) bool { return true }, e.onLeafEvicted)

	const key = "user:contended"
	const iterations = 200

	for i := range iterations {
		seedTip := Tip{1, uint64(1000 + i)}
		e.index.Insert(key, nil, keytrie.NewTipSet(seedTip))
		e.effectCache.Put(seedTip, &pb.Effect{Key: []byte(key)})

		newTip := Tip{1, uint64(2000 + i)}
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			e.index.EvictBounded(64)
		}()
		go func() {
			defer wg.Done()
			e.updateIndex(key, nil, newTip)
		}()
		wg.Wait()

		tips := e.index.Contains(key)
		if tips == nil {
			continue // evict won — key gone, no torn state
		}
		if !tips.Contains(newTip) {
			t.Fatalf("iter %d: key exists but newTip %v missing; got %v — torn state",
				i, newTip, tips.Tips())
		}
		e.index.DeleteAndSnapshot(key, tips)
	}
}
