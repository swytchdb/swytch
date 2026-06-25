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
	"fmt"
	"runtime"
	"sync"
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

// TestEvictBounded_ReclaimsGhostBeforeHotKey verifies the ghost-as-fallback
// policy: when the sweep finds no cold (unprotected) live leaf but ghosts exist,
// eviction reclaims the oldest ghost instead of evicting a hot (protected) key.
func TestEvictBounded_ReclaimsGhostBeforeHotKey(t *testing.T) {
	c := NewCritbit[struct{}]()
	c.SetEvictHooks(func(string) bool { return true }, func(string, []EffectRef, *struct{}) {}, func() bool { return true })

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

// TestChunkReclaim_RecyclesFullyDeadChunk verifies that once every leaf in an
// arena chunk has been deleted and reaped, the chunk is recycled: its dead array
// is replaced by a fresh empty one and its slots return to the free list, so the
// arena's footprint tracks live keys instead of the all-time high-water mark.
// Leaves in other chunks survive, and the next allocations reuse the recycled
// slots rather than bumping next. Single-threaded, so leaf allocation is
// sequential: the first chunkSize distinct keys land in leaf chunk 0.
func TestChunkReclaim_RecyclesFullyDeadChunk(t *testing.T) {
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

	chunksBefore := len(c.leafArena.list.Load().chunks)
	if chunksBefore < 2 {
		t.Fatalf("expected at least 2 leaf chunks after %d inserts, got %d", n, chunksBefore)
	}
	nextBefore := c.leafArena.next.Load()

	// Delete every key whose leaf lives in chunk 0, then reap to completion.
	for i := range defaultCritbitArenaChunkSize {
		c.Delete(keys[i], tips[i])
	}
	for c.reap() > 0 {
	}

	// Chunk 0 is recycled (a fresh, non-nil empty chunk), not nil'd, and its slots
	// are back on the free list.
	if chunks := c.leafArena.list.Load().chunks; chunks[0] == nil {
		t.Fatal("leaf chunk 0 should be recycled to a fresh chunk, not nil")
	}
	if fc := c.leafArena.freeCount.Load(); fc < int64(defaultCritbitArenaChunkSize) {
		t.Fatalf("expected >= %d recycled slots on the free list, got %d", defaultCritbitArenaChunkSize, fc)
	}

	// Survivors (chunk 1+) must remain readable.
	for i := defaultCritbitArenaChunkSize; i < n; i++ {
		if c.Contains(keys[i]) == nil {
			t.Fatalf("surviving key %s missing after chunk recycle", keys[i])
		}
	}

	// Reuse: inserting chunkSize fresh keys must drain the free list instead of
	// growing the arena — next and the chunk count stay put.
	for i := range defaultCritbitArenaChunkSize {
		k := fmt.Sprintf("reuse-%08d", i)
		c.Insert(k, nil, NewTipSet(tip(uint64(n+i+1))))
	}
	if got := c.leafArena.next.Load(); got != nextBefore {
		t.Fatalf("arena grew via next (%d -> %d) instead of reusing recycled slots", nextBefore, got)
	}
	if got := len(c.leafArena.list.Load().chunks); got != chunksBefore {
		t.Fatalf("chunk count grew (%d -> %d) instead of reusing recycled slots", chunksBefore, got)
	}

	// Sweep must tolerate the recycled chunk.
	c.SetEvictHooks(func(string) bool { return true }, func(string, []EffectRef, *struct{}) {}, func() bool { return true })
	c.EvictBounded(64) // must not panic
}

// TestChunkReclaim_ArenaBoundedUnderChurn compresses the weeks-of-churn scenario:
// it inserts and fully evicts a working set many times over — far exceeding the
// arena's lifetime allocation count — and asserts the arena high-water mark
// (next) stays bounded near one working set. Without recycling, next would grow
// with the cumulative insert count (the unbounded-arena bug that makes the
// eviction sweep walk an ever-larger field of dead slots).
func TestChunkReclaim_ArenaBoundedUnderChurn(t *testing.T) {
	c := NewCritbit[struct{}]()
	const working = defaultCritbitArenaChunkSize + 256 // spans into a 2nd chunk
	const rounds = 24

	for round := range rounds {
		keys := make([]string, working)
		tips := make([]*TipSet, working)
		for i := range working {
			keys[i] = fmt.Sprintf("r%03d-k%08d", round, i)
			tips[i] = NewTipSet(tip(uint64(round*working + i + 1)))
			c.Insert(keys[i], nil, tips[i])
		}
		for i := range working {
			c.Delete(keys[i], tips[i])
		}
		for c.reap() > 0 {
		}
	}

	// Cumulative inserts ≈ rounds*working, but recycling reuses dead slots, so the
	// high-water mark stays near one working set (plus partial-chunk slack), not
	// the cumulative count.
	const bound = 3 * working
	if got := c.leafArena.next.Load(); got > uint64(bound) {
		t.Fatalf("leaf arena next=%d after churning %d keys; recycling should bound the high-water mark near %d (<= %d)",
			got, rounds*working, working, bound)
	}
}

// TestChunkReclaim_ConcurrentChurnReuse races the recycle path (dropChunk
// installing a fresh chunk and enqueuing its slots) against concurrent allocators
// (alloc dequeuing recycled slots), readers, and the reaper. Each writer slides a
// small live window across many unique keys, so chunks fully die and recycle
// while inserts are still flowing. Run under -race to surface any torn read of a
// recycled slot or a stale free-list index; the post-run Range==Size check
// asserts the tree stays structurally consistent.
func TestChunkReclaim_ConcurrentChurnReuse(t *testing.T) {
	c := NewCritbit[struct{}]()

	const writers = 8
	const perWriter = 6000 // 8*6000 = 48k churned keys, well past arena capacity
	const window = 64       // each writer keeps ~window keys live at once

	var writersWg sync.WaitGroup
	for w := range writers {
		writersWg.Go(func() {
			live := make(map[int]*TipSet, window+1)
			for i := range perWriter {
				key := fmt.Sprintf("w%02d-k%08d", w, i)
				ts := NewTipSet(tip(uint64(w)*1_000_000 + uint64(i) + 1))
				c.Insert(key, nil, ts)
				live[i] = ts
				if old := i - window; old >= 0 {
					c.Delete(fmt.Sprintf("w%02d-k%08d", w, old), live[old])
					delete(live, old)
				}
			}
		})
	}

	stop := make(chan struct{})
	var helpersWg sync.WaitGroup
	helpersWg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				c.reap()
			}
		}
	})
	helpersWg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				for i := range 200 {
					c.Contains(fmt.Sprintf("w%02d-k%08d", i%writers, i))
				}
			}
		}
	})

	writersWg.Wait()
	close(stop)
	helpersWg.Wait()
	for c.reapRunning.Load() {
		runtime.Gosched()
	}

	count := int64(0)
	c.Range(func(string) bool { count++; return true })
	if count != c.Size() {
		t.Errorf("Range count %d != Size %d after concurrent churn — recycling corrupted the tree", count, c.Size())
	}
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

// TestReapBackoff_AIMD verifies the adaptive pacing controller: clean cycles
// raise the backoff additively up to the max, overflow halves it, and both
// directions saturate at their bounds.
func TestReapBackoff_AIMD(t *testing.T) {
	c := NewCritbit[struct{}]()
	if got := c.reapBackoff.Load(); got != reapBackoffInit {
		t.Fatalf("initial backoff = %d; want %d", got, reapBackoffInit)
	}

	// A clean cycle increases the factor by exactly one.
	c.adaptReapBackoff(false)
	if got := c.reapBackoff.Load(); got != reapBackoffInit+1 {
		t.Fatalf("after one clean cycle = %d; want %d", got, reapBackoffInit+1)
	}

	// Many clean cycles climb to, but never past, the max.
	for range 100 {
		c.adaptReapBackoff(false)
	}
	if got := c.reapBackoff.Load(); got != reapBackoffMax {
		t.Fatalf("after many clean cycles = %d; want max %d", got, reapBackoffMax)
	}

	// Overflow halves the factor (multiplicative decrease).
	c.adaptReapBackoff(true)
	if got := c.reapBackoff.Load(); got != reapBackoffMax/2 {
		t.Fatalf("after overflow = %d; want %d", got, reapBackoffMax/2)
	}

	// Repeated overflow floors at the min, never below.
	for range 100 {
		c.adaptReapBackoff(true)
	}
	if got := c.reapBackoff.Load(); got != reapBackoffMin {
		t.Fatalf("after many overflows = %d; want min %d", got, reapBackoffMin)
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
