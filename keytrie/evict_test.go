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

package keytrie

import (
	"container/list"
	"fmt"
	"math/rand"
	"runtime"
	"testing"
)

func tip(seq uint64) EffectRef { return EffectRef{1, seq} }

// TestEvictBounded_EvictsColdNotProtected verifies the protected-freq policy:
// keys accessed enough to exceed k are protected, and the bounded sweep evicts
// the cold (low-freq) key instead.
func TestEvictBounded_EvictsColdNotProtected(t *testing.T) {
	c := NewCritbit[struct{}]()
	var evicted []string
	c.SetEvictHooks(
		func(string) bool { return true },
		func(k string, _ []EffectRef, _ *struct{}) { evicted = append(evicted, k) },
		func() bool { return true },
	)

	for i, k := range []string{"hot-a", "hot-b", "cold-c"} {
		c.Insert(k, nil, NewTipSet(tip(uint64(i+1))))
	}
	// Protect the two hot keys by accessing them past k (=2).
	for range 5 {
		c.LoadOrStoreData("hot-a", &struct{}{})
		c.LoadOrStoreData("hot-b", &struct{}{})
	}

	if !c.EvictBounded(64) {
		t.Fatal("EvictBounded reported no eviction; expected the cold key to go")
	}
	if len(evicted) != 1 || evicted[0] != "cold-c" {
		t.Fatalf("evicted = %v; want [cold-c]", evicted)
	}
	if c.Contains("cold-c") != nil {
		t.Fatal("cold-c still present after eviction")
	}
	if c.Contains("hot-a") == nil || c.Contains("hot-b") == nil {
		t.Fatal("a protected hot key was evicted")
	}
}

// TestEvictBounded_GhostPromoteOnReinsert verifies that an evicted key leaves a
// ghost retaining its frequency, and re-inserting it promotes the ghost back
// to a live leaf (warm restart) rather than starting cold.
func TestEvictBounded_GhostPromoteOnReinsert(t *testing.T) {
	c := NewCritbit[struct{}]()
	c.SetEvictHooks(func(string) bool { return true }, func(string, []EffectRef, *struct{}) {}, func() bool { return true })

	c.Insert("x", nil, NewTipSet(tip(1)))
	if !c.EvictBounded(64) {
		t.Fatal("expected x to be evicted")
	}
	if c.Contains("x") != nil {
		t.Fatal("x should be absent after eviction")
	}
	if g := c.ghostCount.Load(); g != 1 {
		t.Fatalf("ghostCount = %d; want 1 (the evicted key is a ghost)", g)
	}

	// Re-insert: the ghost must be promoted, not double-counted.
	c.Insert("x", nil, NewTipSet(tip(2)))
	if c.Contains("x") == nil {
		t.Fatal("x should be live again after re-insert")
	}
	if g := c.ghostCount.Load(); g != 0 {
		t.Fatalf("ghostCount = %d; want 0 after promotion", g)
	}
}

// TestEvictBatch_EvictsOldestColdInOnePass verifies batched eviction: one sweep
// reclaims many victims, and it takes the LRU (oldest) cold leaves, sparing the
// most-recently-written ones (read-your-writes).
func TestEvictBatch_EvictsOldestColdInOnePass(t *testing.T) {
	c := NewCritbit[struct{}]()
	c.SetEvictHooks(func(string) bool { return true }, func(string, []EffectRef, *struct{}) {}, func() bool { return true })

	const n = 100
	keys := make([]string, n)
	for i := range n {
		keys[i] = fmt.Sprintf("k-%04d", i) // inserted in order → ascending lastAccess
		c.Insert(keys[i], nil, NewTipSet(tip(uint64(i+1))))
	}

	if got := c.EvictBatch(30); got != 30 {
		t.Fatalf("EvictBatch(30) = %d; want 30 evicted in one pass", got)
	}

	for i, k := range keys {
		present := c.Contains(k) != nil
		if i < 30 && present {
			t.Fatalf("key %s is among the 30 oldest and should be evicted", k)
		}
		if i >= 30 && !present {
			t.Fatalf("key %s is recent and should be spared", k)
		}
	}
}

// TestEvictBatch_SkipsDynamicallyPinned verifies the Pin/Unpin eviction hold:
// a pinned key survives a sweep that takes everything around it (the cloud
// outbox pins keys whose uploads haven't been acked — evicting one hides
// cluster-durable data), and unpinning makes it evictable again.
func TestEvictBatch_SkipsDynamicallyPinned(t *testing.T) {
	c := NewCritbit[struct{}]()
	c.SetEvictHooks(func(string) bool { return true }, func(string, []EffectRef, *struct{}) {}, func() bool { return true })

	const n = 10
	keys := make([]string, n)
	for i := range n {
		keys[i] = fmt.Sprintf("k-%04d", i)
		c.Insert(keys[i], nil, NewTipSet(tip(uint64(i+1))))
	}
	const held = "k-0000" // the oldest — first in line for eviction
	if !c.Pin(held) {
		t.Fatal("Pin on a live key must succeed")
	}
	if c.Pin("nonexistent") {
		t.Fatal("Pin on a missing key must report no hold taken")
	}

	if got := c.EvictBatch(n); got != n-1 {
		t.Fatalf("EvictBatch(%d) = %d; want %d (everything but the pinned key)", n, got, n-1)
	}
	if c.Contains(held) == nil {
		t.Fatal("pinned key was evicted")
	}

	if !c.Unpin(held) {
		t.Fatal("Unpin on a live key must succeed")
	}
	if got := c.EvictBatch(1); got != 1 {
		t.Fatalf("EvictBatch(1) after Unpin = %d; want 1", got)
	}
	if c.Contains(held) != nil {
		t.Fatal("unpinned key must be evictable again")
	}
}

// TestEvictBounded_ReclaimsGhostBeforeHotKey verifies the ghost-as-fallback
// policy: when the sweep finds no cold (unprotected) live leaf but ghosts exist,
// eviction reclaims the oldest ghost instead of evicting a hot (protected) key.
func TestEvictBounded_ReclaimsGhostBeforeHotKey(t *testing.T) {
	c := NewCritbit[struct{}]()
	c.SetEvictHooks(func(string) bool { return true }, func(string, []EffectRef, *struct{}) {}, func() bool { return true })
	// The scenario needs a protected class; the default k=0 is pure LRU.
	c.evictK.Store(2)

	all := []string{"cold-1", "cold-2", "hot-1", "hot-2"}
	for _, k := range all {
		c.Insert(k, nil, NewTipSet(tip(1)))
	}
	// Drive the hot keys above k (=2) so they're protected.
	for range 5 {
		c.LoadOrStoreData("hot-1", &struct{}{})
		c.LoadOrStoreData("hot-2", &struct{}{})
	}
	// Evict the two cold keys — each is the LRU unprotected leaf, so it becomes a ghost.
	c.EvictBounded(64)
	c.EvictBounded(64)
	ghostsBefore := c.ghostCount.Load()
	if ghostsBefore < 1 {
		t.Fatalf("expected ghosts after evicting cold keys, got %d", ghostsBefore)
	}

	// Now only protected keys are live. The next eviction must reclaim a ghost.
	if !c.EvictBounded(64) {
		t.Fatal("EvictBounded returned false; expected a ghost reclaim")
	}
	if got := c.ghostCount.Load(); got >= ghostsBefore {
		t.Fatalf("ghostCount %d -> %d; expected a ghost to be reclaimed", ghostsBefore, got)
	}
	for _, k := range []string{"hot-1", "hot-2"} {
		if c.Contains(k) == nil {
			t.Fatalf("hot key %s was evicted; a ghost should have been reclaimed instead", k)
		}
	}
}

// TestReap_QueueDrainUnlinks verifies the queue-driven reap: deleting a key
// enqueues its leaf, and draining the queue unlinks it via a direct (search-free)
// splice, leaving deletedCount at zero and survivors intact. Reaping is async
// (Delete may also kick a background drain), so we drain-and-yield until settled.
func TestReap_QueueDrainUnlinks(t *testing.T) {
	c := NewCritbit[struct{}]()
	keys := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"}
	tips := make([]*TipSet, len(keys))
	for i, k := range keys {
		tips[i] = NewTipSet(tip(uint64(i + 1)))
		c.Insert(k, nil, tips[i])
	}
	for i := range 4 {
		c.Delete(keys[i], tips[i])
	}

	settled := false
	for range 10000 {
		for c.drainReap() > 0 {
		}
		if c.deletedCount.Load() == 0 {
			settled = true
			break
		}
		runtime.Gosched()
	}
	if !settled {
		t.Fatalf("deletedCount = %d after draining; want 0 (all queued leaves unlinked)", c.deletedCount.Load())
	}

	for i, k := range keys {
		got := c.Contains(k)
		if i < 4 && got != nil {
			t.Fatalf("deleted key %s still present after queue drain", k)
		}
		if i >= 4 && got == nil {
			t.Fatalf("surviving key %s missing after queue drain", k)
		}
	}
}

// TestChunkReclaim_DropsFullyDeadChunk verifies that once every leaf in an arena
// chunk has been deleted and reaped, the chunk is reclaimed: a fresh array takes
// its place and its slot range goes to the free list, so a subsequent insert
// reuses a recycled slot instead of growing the arena. Leaves in other chunks
// survive. Single-threaded, so leaf allocation is sequential: the first
// chunkSize distinct keys land in leaf chunk 0.
func TestChunkReclaim_DropsFullyDeadChunk(t *testing.T) {
	c := NewCritbit[struct{}]()
	const extra = 500
	n := defaultCritbitArenaChunkSize + extra

	keys := make([]string, n)
	tips := make([]*TipSet, n)
	for i := range n {
		keys[i] = fmt.Sprintf("key-%08d", i)
		tips[i] = NewTipSet(tip(uint64(i + 1)))
		c.Insert(keys[i], nil, tips[i])
	}

	if got := len(c.leafArena.list.Load().chunks); got < 2 {
		t.Fatalf("expected at least 2 leaf chunks after %d inserts, got %d", n, got)
	}

	// Delete every key whose leaf lives in chunk 0, then reap to completion.
	for i := range defaultCritbitArenaChunkSize {
		c.Delete(keys[i], tips[i])
	}
	for c.reap() > 0 {
	}

	// Chunk 0 was recycled: a fresh array is in place and every one of its slots
	// waits on the free list.
	list := c.leafArena.list.Load()
	if list.chunks[0] == nil {
		t.Fatal("leaf chunk 0 was nil-dropped; the free list had room, so it should have been recycled")
	}
	if got := list.outstanding[0].Load(); got != int64(defaultCritbitArenaChunkSize) {
		t.Fatalf("chunk 0 outstanding = %d; want %d (all recycled slots unallocated)",
			got, defaultCritbitArenaChunkSize)
	}
	if got := c.leafArena.freeCount.Load(); got != int64(defaultCritbitArenaChunkSize) {
		t.Fatalf("leaf free list holds %d slots; want %d", got, defaultCritbitArenaChunkSize)
	}

	// A new insert must reuse a recycled slot, not grow the monotonic tail.
	nextBefore := c.leafArena.next.Load()
	c.Insert("fresh-after-reclaim", nil, NewTipSet(tip(9999)))
	if got := c.leafArena.next.Load(); got != nextBefore {
		t.Fatalf("insert after reclaim grew the arena tail (%d -> %d); want a recycled slot", nextBefore, got)
	}
	if c.Contains("fresh-after-reclaim") == nil {
		t.Fatal("key inserted into a recycled slot is unreadable")
	}

	// Survivors (chunk 1+) must remain readable, and the sweep must tolerate the
	// recycled chunk.
	for i := defaultCritbitArenaChunkSize; i < n; i++ {
		if c.Contains(keys[i]) == nil {
			t.Fatalf("surviving key %s missing after chunk reclaim", keys[i])
		}
	}
	c.SetEvictHooks(func(string) bool { return true }, func(string, []EffectRef, *struct{}) {}, func() bool { return true })
	c.EvictBounded(64) // must not panic on the recycled chunk
}

// TestEvictBounded_GhostNotReaped verifies the reaper leaves ghost leaves in
// place (their frequency memory is the point) while still pruning a normal
// deleted leaf.
func TestEvictBounded_GhostNotReaped(t *testing.T) {
	c := NewCritbit[struct{}]()
	c.SetEvictHooks(func(string) bool { return true }, func(string, []EffectRef, *struct{}) {}, func() bool { return true })

	c.Insert("ghost", nil, NewTipSet(tip(1)))
	c.Insert("normal", nil, NewTipSet(tip(2)))

	if !c.EvictBounded(64) {
		t.Fatal("expected an eviction")
	}
	// Force a reap pass; the ghost must survive it.
	c.reap()
	if g := c.ghostCount.Load(); g < 1 {
		t.Fatalf("ghostCount = %d; a ghost should have survived reap", g)
	}
}

// TestGraduationGatedByPressure exercises the underPressure gate in bumpAccess:
// a leaf crossing k into protected status counts toward ReachedProtected only
// while the consumer is over its eviction target. The other eviction tests pin
// underPressure to always-true, so this is the only coverage of the gated-off
// branch — counting graduations while not under pressure would rail adaptive k.
func TestGraduationGatedByPressure(t *testing.T) {
	c := NewCritbit[struct{}]()
	pressure := false
	c.SetEvictHooks(
		func(string) bool { return true },
		func(string, []EffectRef, *struct{}) {},
		func() bool { return pressure },
	)

	// Walk a fresh leaf's freq past k (=2) with no pressure: the crossing must
	// not be counted.
	c.Insert("cold", nil, NewTipSet(tip(1)))
	for range 5 {
		c.LoadOrStoreData("cold", &struct{}{})
	}
	if got := c.EvictStats().ReachedProtected; got != 0 {
		t.Fatalf("ReachedProtected = %d with underPressure=false; want 0 (gated off)", got)
	}

	// Now under pressure, a different fresh leaf's crossing is counted (the gate
	// fires once per leaf at f==k, so a new key is required).
	pressure = true
	c.Insert("warm", nil, NewTipSet(tip(2)))
	for range 5 {
		c.LoadOrStoreData("warm", &struct{}{})
	}
	if got := c.EvictStats().ReachedProtected; got == 0 {
		t.Fatal("ReachedProtected = 0 with underPressure=true; want the crossing counted")
	}
}

// findLeaf scans the leaf arena for a leaf carrying key, ghosts included
// (Contains hides deleted leaves). Test-only; not concurrency-safe.
func findLeaf[T any](c *Critbit[T], key string) *critNode[T] {
	for _, chunk := range c.leafArena.list.Load().chunks {
		for i := range chunk {
			if n := &chunk[i]; n.key == key {
				return n
			}
		}
	}
	return nil
}

// TestEvictBatch_GhostTrimOrder pins the ghost-trim order: at ghost capacity,
// the oldest EXISTING ghost is trimmed to make room before the fresh victim is
// ghosted. The inverted order (ghost first, then trim the LRU ghost) is
// refactor-bait that survives review: the victim was chosen as LRU, so its
// recency is near-oldest, and the just-created ghost is almost always the one
// trimmed — ghost memory freezes on a stale cohort, every returning key
// re-admits at freq 1, and the policy quietly becomes LRU-with-overhead.
func TestEvictBatch_GhostTrimOrder(t *testing.T) {
	c := NewCritbit[struct{}]()
	c.SetEvictHooks(func(string) bool { return true }, func(string, []EffectRef, *struct{}) {}, func() bool { return true })

	// The victim-to-be goes in first, so its lastAccess predates every ghost's.
	// Pin it so the fodder churn below can't take it early.
	c.Insert("victim", nil, NewTipSet(tip(1)))
	if !c.Pin("victim") {
		t.Fatal("Pin on a live key must succeed")
	}

	// Fill the ghost cache to capacity with fodder evictions.
	ghostCap := int(c.ghostCap())
	for i := range ghostCap {
		c.Insert(fmt.Sprintf("fodder-%04d", i), nil, NewTipSet(tip(uint64(i+2))))
	}
	for range ghostCap {
		if !c.EvictBounded(64) {
			t.Fatal("fodder eviction failed")
		}
	}
	if got := c.ghostCount.Load(); got != int64(ghostCap) {
		t.Fatalf("ghostCount = %d; want %d (at capacity)", got, ghostCap)
	}

	// Unpin and evict once: the victim is the LRU cold leaf and its recency is
	// older than all existing ghosts — the doc scenario where the buggy order
	// trims the fresh ghost.
	if !c.Unpin("victim") {
		t.Fatal("Unpin on a live key must succeed")
	}
	if !c.EvictBounded(64) {
		t.Fatal("expected the victim to be evicted")
	}

	if got := c.ghostCount.Load(); got != int64(ghostCap) {
		t.Fatalf("ghostCount = %d after trim+ghost; want %d (net unchanged)", got, ghostCap)
	}
	if n := findLeaf(c, "victim"); n == nil || n.freq.Load() >= 0 {
		t.Fatal("fresh victim must survive as a ghost")
	}
	if n := findLeaf(c, "fodder-0000"); n != nil && n.freq.Load() < 0 {
		t.Fatal("oldest existing ghost must be the one trimmed")
	}
	if n := findLeaf(c, "fodder-0001"); n == nil || n.freq.Load() >= 0 {
		t.Fatal("younger ghosts must survive the trim")
	}
}

// TestDecayStepsLeafFreqDown verifies eviction-driven frequency decay: a leaf
// that saturated freq while hot steps down by 1 per elapsed decay epoch as the
// sweep visits it, and an idle policy (no reclaims) decays nothing.
func TestDecayStepsLeafFreqDown(t *testing.T) {
	c := NewCritbit[struct{}]()
	c.SetDecayInterval(100)
	c.SetEvictHooks(func(string) bool { return true }, func(string, []EffectRef, *struct{}) {}, func() bool { return true })
	// Pin a protected class so the hot leaf is never a victim; the default
	// k=0 (pure LRU) would evict it as the oldest leaf during the churn.
	c.evictK.Store(2)

	c.Insert("hot", nil, NewTipSet(tip(1)))
	for range 30 {
		c.LoadData("hot") // Contains doesn't record accesses; LoadData does
	}
	if got := findLeaf(c, "hot").freq.Load(); got != maxLeafFreq {
		t.Fatalf("hot freq = %d after saturation; want %d", got, maxLeafFreq)
	}

	// 150 reclaims cross one decay interval (100) exactly once: the hot leaf —
	// protected, never a victim — must step 15 → 14, no further.
	for i := range 150 {
		c.Insert(fmt.Sprintf("fodder-%04d", i), nil, NewTipSet(tip(uint64(i+2))))
		if !c.EvictBounded(64) {
			t.Fatalf("eviction %d failed", i)
		}
	}
	if got := findLeaf(c, "hot").freq.Load(); got != maxLeafFreq-1 {
		t.Fatalf("hot freq = %d after one decay epoch; want %d", got, maxLeafFreq-1)
	}
}

// TestSlidingWindowDriftParity is the squatter detector: a working set that
// drifts across the keyspace. Undecayed protection collapses here (measured
// 50.8%% hit vs plain LRU's 89.9%% on the colibri traces) because saturated
// leaves keep their slots after the workload moves on; with eviction-driven
// decay the policy must track LRU. Retention mistakes are invisible to
// eviction metrics — squatters are never evicted — so hit-rate parity against
// an LRU replay of the same trace is the only signal that catches this.
func TestSlidingWindowDriftParity(t *testing.T) {
	const (
		capacity   = 128
		windowKeys = 160 // capacity/working-set ≈ 0.8: the band where policy differences show
		ops        = 60000
		slideEvery = 40 // window advances one key every N ops
	)
	rng := rand.New(rand.NewSource(42))
	trace := make([]int, ops)
	for i := range trace {
		trace[i] = i/slideEvery + rng.Intn(windowKeys)
	}
	key := func(id int) string { return fmt.Sprintf("key-%06d", id) }

	// Plain-LRU baseline over the identical trace.
	lru := list.New()
	inLRU := make(map[int]*list.Element, capacity)
	lruHits := 0
	for _, id := range trace {
		if el, ok := inLRU[id]; ok {
			lruHits++
			lru.MoveToFront(el)
			continue
		}
		if lru.Len() >= capacity {
			back := lru.Back()
			delete(inLRU, back.Value.(int))
			lru.Remove(back)
		}
		inLRU[id] = lru.PushFront(id)
	}

	c := NewCritbit[struct{}]()
	c.SetEvictHooks(
		func(string) bool { return true },
		func(string, []EffectRef, *struct{}) {},
		func() bool { return c.Size() >= capacity },
	)
	payload := struct{}{}
	trieHits := 0
	for _, id := range trace {
		// LoadData is the probe that records the access for the policy
		// (Contains doesn't); the payload install below makes a resident key
		// distinguishable from a miss.
		if c.LoadData(key(id)) != nil {
			trieHits++
			continue
		}
		c.Insert(key(id), nil, NewTipSet(tip(uint64(id))))
		c.LoadOrStoreData(key(id), &payload)
		for c.Size() > capacity {
			if c.EvictBatch(int(c.Size())-capacity) == 0 {
				break
			}
		}
	}

	t.Logf("sliding-window drift: LRU hits %d (%.1f%%), trie hits %d (%.1f%%)",
		lruHits, 100*float64(lruHits)/ops, trieHits, 100*float64(trieHits)/ops)
	if float64(trieHits) < 0.9*float64(lruHits) {
		t.Fatalf("drift parity broken: trie hits %d < 90%% of LRU's %d — protection is squatting (decay regression?)",
			trieHits, lruHits)
	}
}
