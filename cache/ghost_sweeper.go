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
	"log/slog"
	"time"
)

// StartGhostSweeper starts a background goroutine that periodically scans all
// shards and unlinks ghost entries. While the CLOCK hand does eventually sweep
// the entire shard, with large shards and small sweep percentages it can take
// many eviction cycles before the hand rotates back to a ghost's slot. During
// that time, ghosts hold their values in memory unnecessarily.
//
// The sweeper respects ghost capacity under normal conditions, keeping useful
// ghosts for frequency-boosted re-insertion. Under memory pressure (when
// EnforceMemoryTarget is active and process memory exceeds the target), it
// culls ALL ghosts to reclaim memory immediately rather than waiting for
// the CLOCK hand to reach them naturally.
//
// interval controls how often the sweeper runs. A typical value is 1-5 seconds.
// The goroutine stops when Close() is called.
func (c *CloxCache[K, V]) StartGhostSweeper(interval time.Duration) {
	if !c.ghostSweeperRunning.CompareAndSwap(false, true) {
		return // already running
	}

	if interval <= 0 {
		interval = time.Second
	}

	c.wg.Add(1)
	go c.ghostSweeperLoop(interval)
}

func (c *CloxCache[K, V]) ghostSweeperLoop(interval time.Duration) {
	defer c.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			c.sweepGhosts()
		}
	}
}

// sweepGhosts scans all shards and unlinks excess ghosts.
// Under memory pressure (memoryLimit set and process RSS exceeds it), culls ALL ghosts.
// Otherwise, culls ghosts that exceed the per-shard ghost capacity.
func (c *CloxCache[K, V]) sweepGhosts() {
	// Determine if we're under memory pressure
	memoryLimit := c.memoryLimit.Load()
	underPressure := false
	if memoryLimit > 0 {
		processRSS := int64(GetProcessRSS())
		underPressure = processRSS > memoryLimit
	}

	totalCulled := 0

	for shardID := range c.shards {
		shard := &c.shards[shardID]

		ghostCount := shard.ghostCount.Load()
		if ghostCount == 0 {
			continue
		}

		// How many ghosts to remove from this shard?
		var cullTarget int64
		if underPressure {
			// Under memory pressure: remove ALL ghosts
			cullTarget = ghostCount
		} else {
			// Normal mode: only cull ghosts exceeding capacity
			excess := ghostCount - shard.ghostCapacity
			if excess <= 0 {
				continue
			}
			cullTarget = excess
		}

		culled := c.sweepShardGhosts(shardID, cullTarget)
		totalCulled += culled
	}

	if totalCulled > 0 {
		slog.Debug("ghost sweeper culled entries",
			"culled", totalCulled,
			"under_pressure", underPressure)
	}
}

// sweepBatchSize is the number of slots to process per lock acquisition.
// Smaller batches reduce write latency at the cost of more lock round-trips.
const sweepBatchSize = 64

// sweepShardGhosts removes up to cullTarget ghost entries from a single shard.
// It processes slots in small batches, acquiring and releasing the shard lock
// between batches so that Put operations are not blocked for the entire sweep.
// Gets are lock-free and never blocked.
// Returns the number of ghosts actually removed.
func (c *CloxCache[K, V]) sweepShardGhosts(shardID int, cullTarget int64) int {
	shard := &c.shards[shardID]
	slotsPerShard := len(shard.slots)
	var culled int64

	for batchStart := 0; batchStart < slotsPerShard && culled < cullTarget; batchStart += sweepBatchSize {
		batchEnd := min(batchStart+sweepBatchSize, slotsPerShard)

		shard.mu.Lock()
		for slotID := batchStart; slotID < batchEnd && culled < cullTarget; slotID++ {
			slot := &shard.slots[slotID]
			node := slot.Load()
			var prev *recordNode[K, V]

			for node != nil {
				if culled >= cullTarget {
					break
				}

				freq := node.freq.Load()
				if freq <= 0 {
					// Ghost found — unlink it
					next := node.next.Load()
					if prev == nil {
						slot.Store(next)
					} else {
						prev.next.Store(next)
					}
					shard.ghostCount.Add(-1)
					culled++
					// Don't advance prev — it's still the same node
					node = next
					continue
				}

				prev = node
				node = node.next.Load()
			}
		}
		shard.mu.Unlock()
	}

	return int(culled)
}
