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

package cache

import (
	"fmt"
	"testing"
)

// findNode scans every shard chain for key, ghosts included (Get hides them).
// Test-only; not concurrency-safe.
func findNode[K Key, V any](c *CloxCache[K, V], key K) *recordNode[K, V] {
	for si := range c.shards {
		sh := &c.shards[si]
		for i := range sh.slots {
			for n := sh.slots[i].Load(); n != nil; n = n.next.Load() {
				if n.key == key {
					return n
				}
			}
		}
	}
	return nil
}

// TestEvictGhostTrimOrder pins the ghost-trim order: at ghost capacity, the
// oldest EXISTING ghost is trimmed to make room before the fresh victim is
// ghosted. The inverted order (ghost first, then trim the LRU ghost) is
// refactor-bait that survives review: the victim was chosen as LRU, so its
// recency is near-oldest, and the just-created ghost is almost always the one
// trimmed — ghost memory freezes on a stale early cohort, every returning key
// re-admits at freq 1 like a stranger, and the policy quietly becomes
// LRU-with-overhead (the colibri C port shipped exactly this inversion).
func TestEvictGhostTrimOrder(t *testing.T) {
	// 1 shard, 8 slots, capacity 4 → ghostCapacity = min(8-4, 4) = 4.
	c := NewCloxCache[string, int](Config{
		NumShards: 1, SlotsPerShard: 8, Capacity: 4,
		SweepPercent: 100, DecayInterval: -1,
	})
	defer c.Close()

	// The victim-to-be goes in first so its recency predates every ghost's;
	// the decider pins it while the fodder churn fills the ghost cache.
	pinned := "victim"
	c.SetEvictDecider(func(k string, _ int) bool { return k != pinned })
	c.Put("victim", 0)

	// f0..f2 fill the shard; f3..f6 each evict the LRU fodder into a ghost.
	for i := range 7 {
		c.Put(fmt.Sprintf("fodder-%d", i), i)
	}
	if got := c.shards[0].ghostCount.Load(); got != 4 {
		t.Fatalf("ghostCount = %d; want 4 (at capacity)", got)
	}

	// Unpin and insert once more: the victim is the LRU live entry and its
	// recency is older than all existing ghosts — the exact scenario where the
	// buggy order trims the fresh ghost instead of the oldest one.
	pinned = ""
	c.Put("fodder-7", 7)

	if got := c.shards[0].ghostCount.Load(); got != 4 {
		t.Fatalf("ghostCount = %d after trim+ghost; want 4 (net unchanged)", got)
	}
	if n := findNode(c, "victim"); n == nil || n.freq.Load() >= 0 {
		t.Fatal("fresh victim must survive as a ghost")
	}
	if n := findNode(c, "fodder-0"); n != nil {
		t.Fatal("oldest existing ghost must be the one trimmed")
	}
	for i := 1; i <= 3; i++ {
		if n := findNode(c, fmt.Sprintf("fodder-%d", i)); n == nil || n.freq.Load() >= 0 {
			t.Fatalf("younger ghost fodder-%d must survive the trim", i)
		}
	}
}

// TestEvictionDrivenDecayStepsFreqDown verifies eviction-driven frequency
// decay: an entry that saturated freq while hot steps down by 1 per elapsed
// decay epoch as the eviction scan visits it, and decay disabled leaves freq
// untouched. The clock is evictions, so an idle cache forgets nothing.
func TestEvictionDrivenDecayStepsFreqDown(t *testing.T) {
	run := func(decayInterval int) int32 {
		c := NewCloxCache[string, int](Config{
			NumShards: 1, SlotsPerShard: 512, Capacity: 16,
			SweepPercent: 100, DecayInterval: decayInterval,
		})
		defer c.Close()
		// Pin a protected class so the hot entry is never a victim; the
		// default k=0 (pure LRU) would evict it as the oldest entry.
		c.shards[0].k.Store(2)

		c.Put("hot", 0)
		for range 30 {
			c.Get("hot", 0)
		}
		if got := findNode(c, "hot").freq.Load(); got != maxFrequency {
			t.Fatalf("hot freq = %d after saturation; want %d", got, maxFrequency)
		}

		// 150 one-shot puts drive ~135 evictions: with interval 100 that is one
		// decay epoch; the hot entry — protected, never a victim — is visited by
		// every full-shard scan and must absorb exactly one −1 step.
		for i := range 150 {
			c.Put(fmt.Sprintf("churn-%d", i), i)
		}
		return findNode(c, "hot").freq.Load()
	}

	if got := run(100); got != maxFrequency-1 {
		t.Fatalf("hot freq = %d after one decay epoch; want %d", got, maxFrequency-1)
	}
	if got := run(-1); got != maxFrequency {
		t.Fatalf("hot freq = %d with decay disabled; want %d untouched", got, maxFrequency)
	}
}
