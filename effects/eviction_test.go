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
	"fmt"
	"sync"
	"testing"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/keytrie"
)

// TestReclaim_KeepsUnreadKeyValue is the regression test for the data-loss bug: a
// key that is written but never read has no subdag, so a model that only pins the
// index-tip would leave the value-carrying data effect (which sits below the
// type-tag tip) at refs==0 and reclaimUnreferenced would delete it — losing the
// value. With creation refs every ingested vertex (the data effect included) is
// born at refs==1, so reclaim leaves it alone and the later read still returns the
// value. The ref is released only when the key is evicted.
func TestReclaim_KeepsUnreadKeyValue(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	dataOff := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(10), NodeId: 1,
		Kind: &pb.Effect_Data{Data: scalarInsertRaw([]byte("hello"))},
	})
	tagOff := log.putEffect(&pb.Effect{
		Key: []byte("k"), Hlc: sTs(20), NodeId: 1, Deps: []*pb.EffectRef{toPbRef(dataOff)},
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_STRING}},
	})
	e.index.Insert("k", nil, keytrie.NewTipSet(tagOff)) // tip = type-tag, value sits below it

	e.reclaimUnreferenced() // key never read → no subdag; must not shred the chain

	r, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	if r == nil || r.Scalar == nil || string(r.Scalar.GetRaw()) != "hello" {
		t.Fatalf("value lost to reclamation: got %v — data effect was reclaimed out from under the tip", r)
	}
}

// TestRefcount_ConcurrentReadsVsReclaim stresses the race between reads that
// incref (publishSubdag / reduced memo) and the governor's reclaimUnreferenced
// running concurrently. reclaim checks refs==0 then deletes in two steps; if a
// read increfs in that window, reclaim frees a vertex that is now referenced —
// the pool stops counting it while the subdag/reduced still holds it (an
// uncounted leak, Bytes() < truth). After the stress, evicting every key must
// still leave the pool empty; a leftover vertex is a vertex the pool lost track
// of. Run under -race to also surface the data race directly.
func TestRefcount_ConcurrentReadsVsReclaim(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	e.index.SetEvictHooks(
		func(key string) bool { return !isSystemKey([]byte(key)) },
		e.onLeafEvicted,
	)

	const keys = 64
	names := make([]string, keys)
	for k := range keys {
		key := fmt.Sprintf("user:c%d", k)
		names[k] = key
		off := log.putEffect(&pb.Effect{
			Key: []byte(key), Hlc: sTs(int64(k + 1)), NodeId: 1,
			Kind: &pb.Effect_Data{Data: scalarInsertRaw(fmt.Appendf(nil, "v%d", k))},
		})
		e.index.Insert(key, nil, keytrie.NewTipSet(off))
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				_, _, _, _ = e.GetSnapshot(names[(i+g)%keys])
			}
		}(g)
	}
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			e.reclaimUnreferenced()
		}
	})

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	for iter := 0; e.index.Size() > 0; iter++ {
		if e.index.EvictBatch(256) == 0 {
			break
		}
		if iter > keys*2 {
			t.Fatalf("eviction stuck: %d keys still indexed", e.index.Size())
		}
	}
	e.reclaimUnreferenced()

	if cnt := e.effectCache.EntryCount(); cnt != 0 {
		t.Fatalf("concurrent reads vs reclaim: pool not empty after evict-all: %d vertices / %d bytes "+
			"(reclaim freed a vertex a concurrent incref had just referenced)", cnt, e.effectCache.Bytes())
	}
}

// TestRefcount_SubscriptionLifecycleEvictAll probes the subscription subsystem —
// the second-largest per-key consumer in production heaps and the one the other
// refcount tests never touch (engine.GetSnapshot doesn't subscribe). Each key is
// subscribed via the real ensureSubscribed path (which pools + indexes a
// SubscriptionEffect and adds an e.subscriptions entry) and read. After evicting
// every key, both the pool and the subscriptions map must be empty: a leftover
// pool vertex means the subscription effect's ref wasn't released; a leftover
// subscriptions entry means eviction's teardown didn't delete it.
func TestRefcount_SubscriptionLifecycleEvictAll(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	e.index.SetEvictHooks(
		func(key string) bool { return !isSystemKey([]byte(key)) },
		e.onLeafEvicted,
	)

	const n = 200
	for i := range n {
		key := fmt.Sprintf("user:s%d", i)
		if err := e.ensureSubscribed(key); err != nil {
			t.Fatalf("ensureSubscribed(%s): %v", key, err)
		}
		if _, _, _, err := e.GetSnapshot(key); err != nil {
			t.Fatalf("GetSnapshot(%s): %v", key, err)
		}
	}
	if sz := e.subscriptions.Size(); sz != n {
		t.Fatalf("expected %d subscriptions, got %d", n, sz)
	}

	for iter := 0; e.index.Size() > 0; iter++ {
		if e.index.EvictBatch(256) == 0 {
			break
		}
		if iter > n*2 {
			t.Fatalf("eviction stuck: %d keys still indexed", e.index.Size())
		}
	}
	e.reclaimUnreferenced()

	if cnt := e.effectCache.EntryCount(); cnt != 0 {
		t.Fatalf("pool not empty after evicting all subscribed keys: %d vertices / %d bytes",
			cnt, e.effectCache.Bytes())
	}
	// broadcastUnsubscribe (which deletes the subscriptions entry) runs async off
	// onLeafEvicted; give it a bounded window to drain.
	deadline := time.Now().Add(2 * time.Second)
	for e.subscriptions.Size() > 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if sz := e.subscriptions.Size(); sz != 0 {
		t.Fatalf("subscriptions map leaked %d entries after evicting every key", sz)
	}
}

// TestRefcount_ChurnEvictAllReclaimsPool is the adversarial version: many keys,
// each re-written several times through the real write path (updateIndex, which
// fires the refDelta hook to incref the new tip and decref the consumed one)
// with a read after every write (rebuilding the subdag and reduced memo, which
// re-incref). This is the production shape — repeated writes growing per-key
// chains, repeated reads churning the caches. After evicting every key and
// reclaiming, the pool must be empty: dropping a leaf has to release its tip,
// subdag, and reduced-memo refs no matter how long the chain or how many times
// the caches were rebuilt. A non-empty pool is a missing decref somewhere on the
// write/read/evict cycle.
func TestRefcount_ChurnEvictAllReclaimsPool(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	e.index.SetEvictHooks(
		func(key string) bool { return !isSystemKey([]byte(key)) },
		e.onLeafEvicted,
	)

	const keys = 100
	const rewrites = 10
	for k := range keys {
		key := fmt.Sprintf("user:k%d", k)
		var curTip Tip
		hasCur := false
		for w := range rewrites {
			eff := &pb.Effect{
				Key: []byte(key), Hlc: sTs(int64(k*1000 + w + 1)), NodeId: 1,
				Kind: &pb.Effect_Data{Data: scalarInsertRaw(fmt.Appendf(nil, "v%d-%d", k, w))},
			}
			if hasCur {
				eff.Deps = []*pb.EffectRef{toPbRef(curTip)}
			}
			off := log.putEffect(eff)
			var consumed *keytrie.TipSet
			if hasCur {
				consumed = keytrie.NewTipSet(curTip)
			}
			e.updateIndex(key, consumed, off)
			curTip = off
			hasCur = true
			if _, _, _, err := e.GetSnapshot(key); err != nil {
				t.Fatalf("GetSnapshot(%s) w=%d: %v", key, w, err)
			}
		}
	}

	for iter := 0; e.index.Size() > 0; iter++ {
		if e.index.EvictBatch(256) == 0 {
			break
		}
		if iter > keys*2 {
			t.Fatalf("eviction stuck: %d keys still indexed", e.index.Size())
		}
	}
	e.reclaimUnreferenced()

	if cnt := e.effectCache.EntryCount(); cnt != 0 {
		t.Fatalf("churn: pool not empty after evict-all + reclaim: %d vertices / %d bytes still resident "+
			"(missing decref on the write/read/evict cycle)", cnt, e.effectCache.Bytes())
	}
}

// TestReleaseQueue_ZeroesDrainedEntries pins the release buffer's retention
// contract: after drainPendingReleases, no drained evictedKey may remain
// reachable through either buffer's backing array. A retained entry pins its
// leafState → cached subdag → effect payloads, so a queue that holds drained
// items (the failure mode of xsync's UMPSCQueue, which never zeroes dequeued
// slots and pools consumed segments fully populated) keeps gigabytes of
// evicted state live and the memory governor permanently over target.
func TestReleaseQueue_ZeroesDrainedEntries(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	e.index.SetEvictHooks(
		func(key string) bool { return !isSystemKey([]byte(key)) },
		e.onLeafEvicted,
	)

	const keys = 50
	for k := range keys {
		key := fmt.Sprintf("user:k%d", k)
		off := log.putEffect(&pb.Effect{
			Key: []byte(key), Hlc: sTs(int64(k + 1)), NodeId: 1,
			Kind: &pb.Effect_Data{Data: scalarInsertRaw(fmt.Appendf(nil, "v%d", k))},
		})
		e.updateIndex(key, nil, off)
		if _, _, _, err := e.GetSnapshot(key); err != nil {
			t.Fatalf("GetSnapshot(%s): %v", key, err)
		}
	}
	for e.index.Size() > 0 {
		if e.index.EvictBatch(256) == 0 {
			break
		}
	}
	if got := e.ReleaseQueueDepth(); got == 0 {
		t.Fatal("expected pending releases after evict-all")
	}

	e.drainPendingReleases()

	if got := e.ReleaseQueueDepth(); got != 0 {
		t.Fatalf("ReleaseQueueDepth = %d after drain; want 0", got)
	}
	var zero evictedKey
	for _, buf := range [][]evictedKey{
		e.releaseItems[:cap(e.releaseItems)],
		e.releaseSpare[:cap(e.releaseSpare)],
	} {
		for i, ek := range buf {
			if ek.key != zero.key || ek.tips != nil || ek.ls != nil {
				t.Fatalf("drained entry %d still populated (key=%q tips=%v ls=%p); "+
					"the buffer must be cleared or it pins the evicted chain", i, ek.key, ek.tips, ek.ls)
			}
		}
	}
}

// TestRefcount_EvictAllReclaimsPool is the load-bearing invariant test for the
// vertex-pool refcount protocol. Every per-key structure that pins a vertex —
// the index tip, the cached subdag (publishSubdag), and the reduced-result memo
// (GetSnapshot) — must release its ref when the leaf is dropped. Build N keys
// through the real read path (Insert + GetSnapshot, which publishes the subdag
// and stores the reduced memo, both incref'ing vertices), then evict every key
// and reclaim. If a single incref lacks a matching decref, the stranded vertex
// stays at refs>0 and survives reclamation — so the pool is non-empty, which is
// exactly the leak. A correct protocol leaves the pool completely empty.
func TestRefcount_EvictAllReclaimsPool(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngine(log)
	e.index.SetEvictHooks(
		func(key string) bool { return !isSystemKey([]byte(key)) },
		e.onLeafEvicted,
	)

	const n = 300
	for i := range n {
		key := fmt.Sprintf("user:k%d", i)
		off := log.putEffect(&pb.Effect{
			Key: []byte(key), Hlc: sTs(int64(10 + i)), NodeId: 1,
			Kind: &pb.Effect_Data{Data: scalarInsertRaw(fmt.Appendf(nil, "value-%d", i))},
		})
		e.index.Insert(key, nil, keytrie.NewTipSet(off))
		// Read it so the subdag and reduced memo are built and pin their vertices.
		if _, _, _, err := e.GetSnapshot(key); err != nil {
			t.Fatalf("GetSnapshot(%s): %v", key, err)
		}
	}

	if got := e.effectCache.EntryCount(); got < n {
		t.Fatalf("expected at least %d pooled effects after %d writes, got %d", n, n, got)
	}

	// Evict every key. EvictBatch fires the trie's refDelta (decref) synchronously
	// for each dropped leaf, releasing its tip + subdag + reduced-memo refs.
	for iter := 0; e.index.Size() > 0; iter++ {
		if e.index.EvictBatch(512) == 0 {
			break
		}
		if iter > n {
			t.Fatalf("eviction made no progress: %d keys still indexed", e.index.Size())
		}
	}
	e.reclaimUnreferenced()

	if cnt := e.effectCache.EntryCount(); cnt != 0 {
		t.Fatalf("pool not empty after evict-all + reclaim: %d vertices still resident — "+
			"a vertex stranded at refs>0 means an incref without a matching decref (the leak)", cnt)
	}
}

// TestDrainReclaim_QueueDrivenReclaim covers the governor's per-tick reclaim: a
// vertex that reaches refs==0 (a dropped adoption, a born-at-zero cache fetch)
// is freed by drainReclaim alone — the queue is the whole mechanism, there is no
// full-map scan behind it — while a still-referenced vertex and a pinned system
// key survive.
func TestDrainReclaim_QueueDrivenReclaim(t *testing.T) {
	e := newTestEngine(&mockBroadcaster{})
	p := e.effectCache

	// Born-at-zero cache fetch: enqueued at birth, freed by the queue drain.
	cacheTip := Tip{1, 1}
	p.PutSizedCache(cacheTip, &pb.Effect{Key: []byte("user:c")}, 10)

	// System-key cache entry: also enqueued, but pinned — drainReclaim must skip it.
	sysTip := Tip{1, 2}
	p.PutSizedCache(sysTip, &pb.Effect{Key: []byte("__swytch:members")}, 10)

	// Owned vertex (creation ref): never enqueued, must survive the drain.
	ownedTip := Tip{1, 3}
	p.PutSized(ownedTip, &pb.Effect{Key: []byte("user:o")}, 10)

	// Adopted then dropped: incref over the creation ref, then two decrefs drive
	// it back to 0 — only the last transition enqueues it.
	dropTip := Tip{1, 4}
	p.PutSized(dropTip, &pb.Effect{Key: []byte("user:d")}, 10) // refs=1 (creation)
	p.incref(dropTip)                                          // refs=2 (adoption)
	p.decref(dropTip)                                          // refs=1, no enqueue
	p.decref(dropTip)                                          // refs=0, enqueue

	if freed := p.drainReclaim(); freed == 0 {
		t.Fatal("drainReclaim freed nothing; queued refs==0 candidates were not reclaimed")
	}
	if _, ok := p.Get(cacheTip); ok {
		t.Fatal("born-at-zero cache entry survived drainReclaim")
	}
	if _, ok := p.Get(dropTip); ok {
		t.Fatal("dropped (refs→0) vertex survived drainReclaim")
	}
	if _, ok := p.Get(sysTip); !ok {
		t.Fatal("pinned system-key entry was freed by drainReclaim")
	}
	if _, ok := p.Get(ownedTip); !ok {
		t.Fatal("owned (refs>0) vertex was freed by drainReclaim")
	}

	// A second drain with nothing queued must be a no-op (and must not block on
	// the empty UMPSCQueue).
	if freed := p.drainReclaim(); freed != 0 {
		t.Fatalf("second drainReclaim freed %d bytes; expected an empty-queue no-op", freed)
	}
}

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

// TestFlushIndex_ReleasesTipRefs verifies FlushIndex releases creation refs:
// Delete bypasses the eviction notify, so FlushIndex itself walks each key's
// chain (releaseChainRefs) to decref the creation ref, after which
// reclaimUnreferenced frees the vertex. Without that release the vertex would
// leak permanently — Delete touches only the index, never the pool.
func TestFlushIndex_ReleasesTipRefs(t *testing.T) {
	e := newTestEngine(&mockBroadcaster{})
	const key = "user:k"
	tip := Tip{1, 5}
	// Resident + indexed: PutSized seeds the creation ref (refs=1) for the one
	// tipset this effect occupies; Insert only updates the index, not refs.
	e.effectCache.PutSized(tip, &pb.Effect{Key: []byte(key)}, 10)
	e.index.Insert(key, nil, keytrie.NewTipSet(tip))

	// While it is a live, reachable tip, reclaim must not free it.
	e.reclaimUnreferenced()
	if _, ok := e.effectCache.Get(tip); !ok {
		t.Fatal("referenced index tip was reclaimed")
	}

	e.FlushIndex()
	e.reclaimUnreferenced()
	if _, ok := e.effectCache.Get(tip); ok {
		t.Fatal("FlushIndex did not release the creation refcount; vertex leaked")
	}
}

// TestPutSized_PromotesCacheEntryToOwned covers the cache→owned upgrade: an effect
// first pooled as a cross-key cache fetch (PutSizedCache, refs==0) and then
// re-ingested as owned (PutSized, because we now serve its key) must gain the
// creation ref it lacked. Without the promotion it stays refs==0 and reclaim drops
// a vertex we now own. A plain cache entry (no owned put) must still be reclaimed.
func TestPutSized_PromotesCacheEntryToOwned(t *testing.T) {
	e := newTestEngine(&mockBroadcaster{})
	eff := &pb.Effect{Key: []byte("user:k")}

	owned := Tip{1, 7}
	e.effectCache.PutSizedCache(owned, eff, 10) // first seen as cross-key cache
	e.effectCache.PutSized(owned, eff, 10)      // now served → promote to owned

	cacheOnly := Tip{1, 8}
	e.effectCache.PutSizedCache(cacheOnly, eff, 10) // stays a cache entry

	e.reclaimUnreferenced()

	if _, ok := e.effectCache.Get(owned); !ok {
		t.Fatal("owned re-put of a cache entry was reclaimed; PutSized did not promote refs 0→creation")
	}
	if _, ok := e.effectCache.Get(cacheOnly); ok {
		t.Fatal("plain cache entry survived reclaim; it carries no creation ref and must be freed")
	}
}

// TestRemoveTips_KeepsReachableAncestor verifies the new-model semantics of
// stale-tip pruning. RemoveTips drops a redundant ancestor tip from the frontier
// but does NOT release its creation ref: the ancestor is still reachable from the
// kept descendant tip, and reconstruct still walks through it, so it must stay
// resident. The ref is released only when the key is removed for good (here via
// FlushIndex), whose chain walk frees the whole lineage.
func TestRemoveTips_KeepsReachableAncestor(t *testing.T) {
	e := newTestEngine(&mockBroadcaster{})
	const key = "user:k"
	ancestor := Tip{1, 5}
	head := Tip{1, 6}
	e.effectCache.PutSized(ancestor, &pb.Effect{Key: []byte(key)}, 10)
	e.effectCache.PutSized(head, &pb.Effect{Key: []byte(key), Deps: []*pb.EffectRef{toPbRef(ancestor)}}, 10)
	e.index.Insert(key, nil, keytrie.NewTipSet(ancestor, head))

	// Prune the redundant ancestor from the frontier — it stays reachable via head.
	e.index.RemoveTips(key, []Tip{ancestor})
	e.reclaimUnreferenced()

	if _, ok := e.effectCache.Get(head); !ok {
		t.Fatal("head tip was wrongly reclaimed")
	}
	if _, ok := e.effectCache.Get(ancestor); !ok {
		t.Fatal("pruned ancestor was reclaimed while still reachable from the head tip")
	}

	// Removing the key for good releases the whole chain.
	e.FlushIndex()
	e.reclaimUnreferenced()
	if _, ok := e.effectCache.Get(head); ok {
		t.Fatal("FlushIndex did not release the head tip")
	}
	if _, ok := e.effectCache.Get(ancestor); ok {
		t.Fatal("FlushIndex did not release the pruned ancestor; chain walk missed it")
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
	// Cache residency (no creation ref): these are unindexed, never reached from a
	// served tip — exactly how a cross-key fetch or an un-served orphan is pooled.
	// The system key must survive on the isSystemKey pin alone, not a refcount.
	e.effectCache.PutSizedCache(systemTip, systemEff, 64)

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
		e.effectCache.PutSizedCache(Tip{1, 1000 + i}, userEff, 256)
	}

	// Run reclamation directly (the governor fires it on its tick under
	// memory pressure; here we drive it deterministically). These cache effects
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
