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
	"log/slog"
	"unsafe"
)

// Arena compaction. Arena slots are never reused in place — a chunk is
// reclaimed (and its slot range recycled through the free list) only when every
// one of its 2048 slots has individually died. Under evict/reinsert churn
// survivors scatter, so one live leaf (or ghost) pins 160KB indefinitely and
// the arena high-water ratchets at the churn rate. Compaction evacuates sparse
// chunks: live nodes are copied to fresh slots and spliced in under the reap
// write lock, dead ones are unlinked, and every disposed slot is accounted
// through recordReaped so the chunk's counter — the single drop trigger —
// fires exactly once when the last slot dies. Compaction never drops a chunk
// directly: a force-drop racing the counter would recycle a chunk while its
// old generation was still being accounted, corrupting the new generation's
// counter and double-enqueuing free-list indices.
//
// Relocation safety rests on the existing lock discipline:
//   - Every leaf-field mutator (Insert, RemoveTips, LoadOrStoreData,
//     DeleteAndSnapshot, the eviction sweep) holds the reap read-lock for its
//     whole critical section, so the write lock excludes all of them.
//   - Splices are identity-checked by descent, exactly like unlinkLeaf: a slot
//     whose key path resolves to a different node is an orphan husk and is
//     skipped, and stale reapQueue entries no-op the same way.
//   - Pure readers (Contains, Range) are lock-free and stale-tolerant: the
//     husk's fields are left intact — deleted flag, tips, and payload — so a
//     reader that captured the old pointer mid-descent finishes with a correct
//     answer, and GC keeps the dropped chunk's memory alive until the last
//     such reader lets go (the same grace dropChunk has always relied on).
//   - TipSets and leaf payloads move by pointer, so escaped *T references and
//     tip refcounts are untouched; no refDelta fires for a relocation.
const (
	// compactOccupancyPct: a chunk whose linked slots are at or below this
	// percentage of chunkSize is an evacuation candidate. Evacuating costs one
	// fresh slot per survivor, so the threshold is the worst-case copy overhead
	// (25% → move ≤512 nodes to free 2048 slots).
	compactOccupancyPct = 25

	// compactMaxChunksPerPass bounds one pass's evacuation work per arena so a
	// badly fragmented trie converges over several reap cycles instead of one
	// long stall.
	compactMaxChunksPerPass = 256
)

// compactMinWasteBytes gates compaction entirely while the reclaimable waste
// is small — scanning the arenas for victims is not free. A var so tests can
// lower it.
var compactMinWasteBytes = int64(32 << 20)

// maybeCompact runs a bounded compaction pass when the arenas hold
// substantially more slots than the tree has linked nodes. Called from the
// reap goroutine (serialized by reapRunning), so it never runs concurrently
// with itself, drainReap, or the BFS reap.
func (c *Critbit[T]) maybeCompact() {
	if c.closed.Load() {
		return
	}
	var node critNode[T]
	nodeSize := int64(unsafe.Sizeof(node))
	arenaBytes := c.leafArena.bytes() + c.internalArena.bytes()
	// Linked estimate: live + deleted-pending + ghost leaves, and one internal
	// per leaf (a crit-bit tree has exactly leaves-1 internals).
	linked := 2 * (c.size.Load() + c.deletedCount.Load() + c.ghostCount.Load()) * nodeSize
	waste := arenaBytes - linked
	if waste < compactMinWasteBytes || waste*4 < arenaBytes {
		return
	}
	leafDropped := c.compactLeafChunks(compactMaxChunksPerPass)
	internalDropped := c.compactInternalChunks(compactMaxChunksPerPass)
	if leafDropped+internalDropped > 0 {
		slog.Debug("keytrie: compacted arenas",
			"leaf_chunks", leafDropped, "internal_chunks", internalDropped,
			"arena_bytes", c.leafArena.bytes()+c.internalArena.bytes())
	}
}

// compactLeafChunks evacuates up to maxChunks sparse leaf chunks and returns
// how many were dropped. Victim selection is slot-driven (a leaf's liveness is
// readable off the slot) and advisory — occupancy is re-read under the write
// lock by the per-slot handling, so a chunk that gained live leaves since the
// scan is still evacuated correctly, just less profitably.
func (c *Critbit[T]) compactLeafChunks(maxChunks int) int {
	list := c.leafArena.list.Load()
	chunks, chunkSize := list.chunks, c.leafArena.chunkSize
	// The tail chunk is still receiving allocations (and is where evacuated
	// survivors land); never evacuate it.
	tail := int(c.leafArena.next.Load() / chunkSize)
	threshold := int(chunkSize) * compactOccupancyPct / 100
	evacuated := 0
	for ci := 0; ci < len(chunks) && ci < tail && evacuated < maxChunks; ci++ {
		chunk := chunks[ci]
		if chunk == nil {
			continue
		}
		// A recycled chunk with slots still waiting in the free list looks empty
		// (pristine slots are born deleted) but is about to be refilled by
		// allocations — evacuating it would relocate its few residents for
		// nothing, and its counter can't fill until every recycled slot has been
		// handed out anyway.
		if list.outstanding[ci].Load() > 0 {
			continue
		}
		live := 0
		for i := range chunk {
			if !chunk[i].deleted.Load() {
				live++
			}
		}
		if live > threshold {
			continue
		}
		if c.evacuateLeafChunk(chunk) {
			evacuated++
		}
	}
	return evacuated
}

// evacuateLeafChunk empties one leaf chunk under a single reap write-lock hold.
// Live leaves are relocated, dead-pending leaves are unlinked, ghosts forfeit
// their warm-restart hint (a ghost stranded in a near-empty chunk is stale
// anyway), and orphan husks need nothing — the identity-checked descent skips
// them, and their slots were already accounted at abandonment. Every disposal
// here flows through recordReaped (unlinkLeaf does it on success, relocateLeaf
// on splice), so the chunk's counter reaches chunkSize exactly when the last
// slot dies and dropChunk fires from inside recordReaped — possibly mid-loop,
// which is safe: a full counter means every remaining slot is an
// already-accounted husk whose disposal below is a no-op.
func (c *Critbit[T]) evacuateLeafChunk(chunk []critNode[T]) bool {
	c.reapMu.Lock()
	defer c.reapMu.Unlock()
	if c.closed.Load() {
		return false
	}
	for i := range chunk {
		n := &chunk[i]
		if n.deleted.Load() {
			if n.freq.Load() < 0 {
				// Ghost: shed ghost status (unlinkLeaf refuses freq < 0), then
				// unlink. Ghosts were never counted in deletedCount, so balance
				// unlinkLeaf's decrement up front and revert if it wasn't linked.
				c.deletedCount.Add(1)
				n.freq.Store(0)
				if c.unlinkLeaf(n) {
					c.ghostCount.Add(-1)
				} else {
					c.deletedCount.Add(-1)
				}
			} else {
				// Deleted-pending (its reapQueue entry will no-op later) or an
				// orphan (identity check fails, no-op now).
				c.unlinkLeaf(n)
			}
			continue
		}
		c.relocateLeaf(n)
	}
	return true
}

// relocateLeaf copies a live leaf into a fresh slot and splices it in place of
// the original. Caller holds the reap write lock. The husk is left untouched —
// still live-looking, sharing the same TipSet and payload pointers — so a
// stale lock-free reader finishes through it correctly; it is unreachable from
// the tree the moment the splice lands, and its slot is accounted right here so
// the chunk's counter can fire the drop. Returns false when the slot isn't
// actually linked (an orphan husk, accounted at abandonment).
func (c *Critbit[T]) relocateLeaf(old *critNode[T]) bool {
	root := c.root.Load()
	if root == nil {
		return false
	}
	var p *critNode[T]
	var pDir int
	cur := root
	for !cur.isLeaf {
		dir := getDirection(old.key, cur.bytePos, cur.otherbits)
		p, pDir = cur, dir
		next := cur.child[dir].Load()
		if next == nil {
			return false
		}
		cur = next
	}
	if cur != old {
		return false // an orphan husk; a different leaf owns this key path
	}
	n2, ci := c.leafArena.alloc() // born deleted=true via the arena init hook
	n2.chunkIdx = ci
	n2.key = old.key
	n2.tips.Store(old.tips.Load())
	n2.data.Store(old.data.Load())
	n2.freq.Store(old.freq.Load())
	n2.lastAccess.Store(old.lastAccess.Load())
	// Publish before splicing: unreachable until the splice, and the write lock
	// excludes the sweep, so no scanner can see it early — while a lock-free
	// reader arriving through the new parent pointer must find deleted=false.
	n2.deleted.Store(false)
	if p == nil {
		c.root.Store(n2)
	} else {
		p.child[pDir].Store(n2)
	}
	c.leafArena.recordReaped(old.chunkIdx)
	return true
}

// compactInternalChunks evacuates up to maxChunks sparse internal chunks.
// Internal nodes carry no key, so victims are found by walking the tree:
// one shared-lock DFS counts linked internals per chunk (advisory), then one
// write-lock DFS collects the victims' members with their parents and
// relocates them. Splice order is handled by a replacement map, so a victim
// child spliced after its victim parent lands on the parent's clone. Each
// relocated husk is accounted through recordReaped, which fires the drop when
// the victim's last slot dies — compaction never drops a chunk directly.
func (c *Critbit[T]) compactInternalChunks(maxChunks int) int {
	list := c.internalArena.list.Load()
	chunks, chunkSize := list.chunks, c.internalArena.chunkSize
	tail := int(c.internalArena.next.Load() / chunkSize)
	if tail == 0 {
		return 0
	}

	// Pass 1 (shared lock, concurrent with traffic): count linked internals
	// per chunk.
	counts := make([]int, len(chunks))
	rt := c.reapMu.RLock()
	root := c.root.Load()
	if root == nil {
		c.reapMu.RUnlock(rt)
		return 0
	}
	stack := make([]*critNode[T], 0, 64)
	stack = append(stack, root)
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if n.isLeaf {
			continue
		}
		if int(n.chunkIdx) < len(counts) {
			counts[n.chunkIdx]++
		}
		for dir := range 2 {
			if ch := n.child[dir].Load(); ch != nil && !ch.isLeaf {
				stack = append(stack, ch)
			}
		}
	}
	c.reapMu.RUnlock(rt)

	threshold := int(chunkSize) * compactOccupancyPct / 100
	victims := make(map[uint32]bool)
	for ci := 0; ci < len(chunks) && ci < tail && len(victims) < maxChunks; ci++ {
		if chunks[ci] == nil {
			continue
		}
		// Recycled chunks awaiting re-allocation look sparse but are refilling;
		// see the same skip in compactLeafChunks.
		if list.outstanding[ci].Load() > 0 {
			continue
		}
		if counts[ci] <= threshold {
			victims[uint32(ci)] = true
		}
	}
	if len(victims) == 0 {
		return 0
	}

	// Pass 2 (write lock): collect victim members with parents, relocate, drop.
	c.reapMu.Lock()
	defer c.reapMu.Unlock()
	if c.closed.Load() {
		return 0
	}
	root = c.root.Load()
	if root == nil {
		return 0
	}
	type frame struct {
		node   *critNode[T]
		parent *critNode[T]
		dir    int
	}
	var moves []frame
	fstack := make([]frame, 0, 64)
	fstack = append(fstack, frame{node: root})
	for len(fstack) > 0 {
		f := fstack[len(fstack)-1]
		fstack = fstack[:len(fstack)-1]
		if f.node.isLeaf {
			continue
		}
		if victims[f.node.chunkIdx] {
			// DFS is pre-order, so an ancestor always precedes its descendants
			// in moves — the replacement map below can therefore always resolve
			// a relocated parent before its child splices through it.
			moves = append(moves, f)
		}
		for dir := range 2 {
			if ch := f.node.child[dir].Load(); ch != nil && !ch.isLeaf {
				fstack = append(fstack, frame{node: ch, parent: f.node, dir: dir})
			}
		}
	}
	replaced := make(map[*critNode[T]]*critNode[T], len(moves))
	for _, m := range moves {
		n2, ci := c.internalArena.alloc()
		n2.chunkIdx = ci
		n2.bytePos = m.node.bytePos
		n2.otherbits = m.node.otherbits
		n2.child[0].Store(m.node.child[0].Load())
		n2.child[1].Store(m.node.child[1].Load())
		if m.parent == nil {
			c.root.Store(n2)
		} else {
			p := m.parent
			if rp, ok := replaced[p]; ok {
				p = rp
			}
			p.child[m.dir].Store(n2)
		}
		replaced[m.node] = n2
		c.internalArena.recordReaped(m.node.chunkIdx)
	}
	return len(victims)
}
