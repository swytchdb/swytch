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
	"sync/atomic"
	"unsafe"
)

// windowShards is how many cache lines the adapt-window hit/op counters are
// spread across. CloxCache got this spreading for free: windowHits/windowOps
// live in its per-shard struct, one set per hash shard, so every Get's
// increment landed on its own shard's line. The single-domain trie has no shard
// array, so a plain atomic.Uint64 would fold all of them onto one global line —
// the read path's last every-access contention point. 64 covers typical core
// counts; the counters are summed only on the cold adapt path.
const windowShards = 64

// windowCounter is a cache-line-sharded uint64 sum. The shard is chosen from
// the accessing leaf's address, so a given key maps to a stable shard — the
// same contention profile as CloxCache's per-key-hash shard, restored without a
// shard array. Reads (load) and the per-window reset run on the eviction/adapt
// path, which can afford to touch every shard.
type windowCounter struct {
	shards [windowShards]struct {
		v atomic.Uint64
		_ [56]byte // pad to a full 64-byte line: no false sharing between shards
	}
}

// add increments the shard selected by p, the accessing leaf's address. Bits 6+
// vary per arena-allocated leaf (stride > one cache line), so >>6 spreads
// adjacent leaves across shards.
func (w *windowCounter) add(p unsafe.Pointer, delta uint64) {
	w.shards[(uintptr(p)>>6)&(windowShards-1)].v.Add(delta)
}

// load sums every shard. Eviction/adapt path only.
func (w *windowCounter) load() uint64 {
	var total uint64
	for i := range w.shards {
		total += w.shards[i].v.Load()
	}
	return total
}

// reset zeroes every shard at the end of an adapt window. Eviction path only.
func (w *windowCounter) reset() {
	for i := range w.shards {
		w.shards[i].v.Store(0)
	}
}
