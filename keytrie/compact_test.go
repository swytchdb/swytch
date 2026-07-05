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
	"sync"
	"testing"
	"unsafe"
)

// drainAllReaps runs the reap machinery to quiescence synchronously (the
// production path is the async maybeReap goroutine).
func drainAllReaps[T any](c *Critbit[T]) {
	for c.drainReap() > 0 {
	}
	if c.reapDropped.Swap(0) > 0 {
		for c.reap() > 0 {
		}
	}
}

// TestCompact_EvacuatesSparseLeafChunk: a chunk pinned by a handful of live
// leaves is evacuated — survivors relocate with tips and payload intact, the
// chunk drops, and ArenaBytes shrinks.
func TestCompact_EvacuatesSparseLeafChunk(t *testing.T) {
	c := NewCritbit[int]()
	n := defaultCritbitArenaChunkSize + 100 // fill chunk 0, spill into chunk 1
	keys := make([]string, n)
	for i := range n {
		keys[i] = fmt.Sprintf("k-%05d", i)
		c.Insert(keys[i], nil, NewTipSet(tip(uint64(i+1))))
	}
	// Give the chunk-0 survivors a payload to carry across the move.
	payload := 42
	survivors := keys[:10]
	for _, k := range survivors {
		c.LoadOrStoreData(k, &payload)
	}
	// Kill the rest of chunk 0 (sequential inserts → the first chunkSize
	// allocations are chunk 0).
	for _, k := range keys[10:defaultCritbitArenaChunkSize] {
		if c.DeleteAndSnapshot(k, c.Contains(k)) == nil {
			t.Fatalf("delete %s failed", k)
		}
	}
	drainAllReaps(c)

	before := c.ArenaBytes()
	if got := c.compactLeafChunks(16); got < 1 {
		t.Fatalf("compactLeafChunks dropped %d chunks; want >= 1", got)
	}
	if after := c.ArenaBytes(); after >= before {
		t.Fatalf("ArenaBytes did not shrink: %d -> %d", before, after)
	}

	for i, k := range survivors {
		ts := c.Contains(k)
		if ts == nil {
			t.Fatalf("survivor %s lost by relocation", k)
		}
		if !ts.Contains(tip(uint64(i + 1))) {
			t.Fatalf("survivor %s has wrong tips after relocation: %v", k, ts.Tips())
		}
		data, loaded := c.LoadOrStoreData(k, new(int))
		if !loaded || data != &payload {
			t.Fatalf("survivor %s lost its payload pointer across relocation", k)
		}
	}
	// The relocated leaves must remain fully mutable through the tree.
	for i, k := range survivors {
		if _, ok := c.Insert(k, c.Contains(k), NewTipSet(tip(uint64(1000 + i)))); !ok {
			t.Fatalf("post-compaction tip CAS on %s failed", k)
		}
		c.RemoveTips(k, []EffectRef{tip(uint64(1000 + i))})
		if ts := c.Contains(k); ts == nil || len(ts.Tips()) != 0 {
			t.Fatalf("post-compaction RemoveTips on %s left %v", k, ts.Tips())
		}
	}
	// Keys outside the evacuated chunk are untouched.
	for _, k := range keys[defaultCritbitArenaChunkSize:] {
		if c.Contains(k) == nil {
			t.Fatalf("key %s outside the victim chunk went missing", k)
		}
	}
}

// TestCompact_InternalChunksFollowLeaves: after a mass delete the internal
// arena is as sparse as the leaf arena; the internal pass relocates the few
// linked internals and drops the chunk, leaving the tree fully readable.
func TestCompact_InternalChunksFollowLeaves(t *testing.T) {
	c := NewCritbit[struct{}]()
	n := defaultCritbitArenaChunkSize + 100
	keys := make([]string, n)
	for i := range n {
		keys[i] = fmt.Sprintf("k-%05d", i)
		c.Insert(keys[i], nil, NewTipSet(tip(uint64(i+1))))
	}
	for _, k := range keys[10:] {
		if c.DeleteAndSnapshot(k, c.Contains(k)) == nil {
			t.Fatalf("delete %s failed", k)
		}
	}
	drainAllReaps(c)

	before := c.internalArena.bytes()
	if got := c.compactInternalChunks(16); got < 1 {
		t.Fatalf("compactInternalChunks dropped %d chunks; want >= 1", got)
	}
	if after := c.internalArena.bytes(); after >= before {
		t.Fatalf("internal arena did not shrink: %d -> %d", before, after)
	}

	live := 0
	c.Range(func(string) bool { live++; return true })
	if live != 10 {
		t.Fatalf("Range sees %d keys after internal compaction; want 10", live)
	}
	for _, k := range keys[:10] {
		if c.Contains(k) == nil {
			t.Fatalf("key %s unreachable after internal relocation", k)
		}
	}
}

// TestCompact_GhostsForfeited: ghosts stranded in a victim chunk are unlinked
// (their warm-restart hint is forfeited) so the chunk can drop, with ghost and
// deleted accounting staying balanced.
func TestCompact_GhostsForfeited(t *testing.T) {
	c := NewCritbit[struct{}]()
	c.SetEvictHooks(func(string) bool { return true }, func(string, []EffectRef, *struct{}) {})
	n := defaultCritbitArenaChunkSize + 100
	keys := make([]string, n)
	for i := range n {
		keys[i] = fmt.Sprintf("k-%05d", i)
		c.Insert(keys[i], nil, NewTipSet(tip(uint64(i+1))))
	}
	// Evict the oldest keys — they become ghosts (cap is size/8, well above 50).
	if got := c.EvictBatch(50); got != 50 {
		t.Fatalf("EvictBatch(50) = %d; want 50", got)
	}
	ghostsBefore := c.ghostCount.Load()
	if ghostsBefore == 0 {
		t.Fatal("expected ghosts after eviction")
	}
	// Delete everything still live in chunk 0 so only ghosts pin it.
	for _, k := range keys[:defaultCritbitArenaChunkSize] {
		if ts := c.Contains(k); ts != nil {
			c.DeleteAndSnapshot(k, ts)
		}
	}
	drainAllReaps(c)

	if got := c.compactLeafChunks(16); got < 1 {
		t.Fatalf("compactLeafChunks dropped %d chunks; want >= 1 (ghosts must not pin)", got)
	}
	if g := c.ghostCount.Load(); g >= ghostsBefore {
		t.Fatalf("ghostCount = %d; want < %d (victim-chunk ghosts forfeited)", g, ghostsBefore)
	}
	if d := c.deletedCount.Load(); d != 0 {
		t.Fatalf("deletedCount = %d; want 0 (accounting must balance)", d)
	}
	// A forfeited ghost's key re-inserts cleanly as a cold fresh key.
	if _, ok := c.Insert(keys[0], nil, NewTipSet(tip(7777))); !ok {
		t.Fatalf("re-insert of forfeited ghost key failed")
	}
	if ts := c.Contains(keys[0]); ts == nil || !ts.Contains(tip(7777)) {
		t.Fatal("re-inserted key not readable")
	}
}

// TestOrphanSlots_DontPinChunks: concurrent inserts race structural CASes and
// orphan arena slots. With orphans accounted as reaped at abandonment, a fully
// deleted keyspace must release every non-tail chunk through the ordinary
// unlink path — no compaction involved.
func TestOrphanSlots_DontPinChunks(t *testing.T) {
	c := NewCritbit[struct{}]()
	const workers = 8
	const perWorker = 8192
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWorker {
				k := fmt.Sprintf("w%02d-%05d", w, i)
				c.Insert(k, nil, NewTipSet(tip(uint64(i+1))))
			}
		}()
	}
	wg.Wait()

	for w := range workers {
		for i := range perWorker {
			k := fmt.Sprintf("w%02d-%05d", w, i)
			if ts := c.Contains(k); ts != nil {
				c.DeleteAndSnapshot(k, ts)
			}
		}
	}
	drainAllReaps(c)

	// Every chunk below the tail is fully dead (unlinked or orphaned) and must
	// have dropped. The tail chunk of each arena may survive.
	var node critNode[struct{}]
	nodeSize := int64(unsafe.Sizeof(node))
	maxLeft := 2 * int64(defaultCritbitArenaChunkSize) * nodeSize // one tail chunk per arena
	if got := c.ArenaBytes(); got > maxLeft {
		t.Fatalf("ArenaBytes = %d after full delete+reap; want <= %d (chunks pinned — orphan accounting broken?)", got, maxLeft)
	}
}

// TestCompact_UnderConcurrentTraffic hammers inserts, reads, tip removals, and
// deletes while compaction runs repeatedly, then verifies the surviving
// keyspace exactly. Run with -race to exercise the locking contract.
func TestCompact_UnderConcurrentTraffic(t *testing.T) {
	c := NewCritbit[int]()
	const workers = 4
	const perWorker = 6000

	stop := make(chan struct{})
	compactorDone := make(chan struct{})
	// Compactor loop: force passes regardless of the waste gate.
	go func() {
		defer close(compactorDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			drainAllReaps(c)
			c.compactLeafChunks(64)
			c.compactInternalChunks(64)
		}
	}()

	var writers sync.WaitGroup
	for w := range workers {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for i := range perWorker {
				k := fmt.Sprintf("w%d-%05d", w, i)
				ts := NewTipSet(tip(uint64(i + 1)))
				c.Insert(k, nil, ts)
				c.LoadOrStoreData(k, new(int))
				if i%2 == 0 {
					// Even keys die; via RemoveTips first on every fourth to
					// exercise that path too.
					if i%4 == 0 {
						c.RemoveTips(k, []EffectRef{tip(uint64(i + 1))})
					}
					if cur := c.Contains(k); cur != nil {
						c.DeleteAndSnapshot(k, cur)
					}
				}
			}
		}()
	}
	writers.Wait()
	close(stop)
	<-compactorDone

	for w := range workers {
		for i := range perWorker {
			k := fmt.Sprintf("w%d-%05d", w, i)
			ts := c.Contains(k)
			if i%2 == 0 {
				if ts != nil {
					t.Fatalf("deleted key %s still present", k)
				}
				continue
			}
			if ts == nil {
				t.Fatalf("live key %s lost under concurrent compaction", k)
			}
			if !ts.Contains(tip(uint64(i + 1))) {
				t.Fatalf("key %s has wrong tips: %v", k, ts.Tips())
			}
		}
	}
}
