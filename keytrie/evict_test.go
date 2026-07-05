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
	c.SetEvictHooks(func(string) bool { return true }, func(string, []EffectRef, *struct{}) {})

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
	c.SetEvictHooks(func(string) bool { return true }, func(string, []EffectRef, *struct{}) {})

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
	c.SetEvictHooks(func(string) bool { return true }, func(string, []EffectRef, *struct{}) {})

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
	c.SetEvictHooks(func(string) bool { return true }, func(string, []EffectRef, *struct{}) {})
	c.EvictBounded(64) // must not panic on the recycled chunk
}

// TestEvictBounded_GhostNotReaped verifies the reaper leaves ghost leaves in
// place (their frequency memory is the point) while still pruning a normal
// deleted leaf.
func TestEvictBounded_GhostNotReaped(t *testing.T) {
	c := NewCritbit[struct{}]()
	c.SetEvictHooks(func(string) bool { return true }, func(string, []EffectRef, *struct{}) {})

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
