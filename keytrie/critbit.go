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

/*
 * This file is part of CloxCache.
 *
 * CloxCache is licensed under the MIT License.
 * See LICENSE file for details.
 */

package keytrie

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/puzpuzpuz/xsync/v4"
)

const (
	defaultCritbitArenaChunkSize = 2048

	// maxLeafFreq caps the per-leaf access-frequency counter. The eviction
	// policy protects leaves whose freq exceeds the shard's adaptive
	// threshold; saturating keeps a hot key from running away.
	maxLeafFreq = 15

	// reapQueueCapacity bounds the pending-unlink queue. Deleted/evicted leaves
	// are pushed here at delete time (we already know which node went dead, so
	// there is no need to BFS the tree to rediscover them); a background worker
	// drains it in small batches under brief write-lock holds. On overflow a push
	// is dropped and a full-BFS reap is scheduled as a backstop, so nothing leaks.
	reapQueueCapacity = 1 << 16

	// reapBatchSize is how many queued leaves the worker unlinks per write-lock
	// acquisition — small, so inserts interleave between batches instead of
	// stalling behind a whole-tree pass.
	reapBatchSize = 64
)

// critNode is a flattened node that serves as both internal and leaf.
// Hot-path fields (child, bytePos, otherbits, isLeaf) are in the first 22 bytes.
// Leaf fields start at offset 24.
//
// The type parameter T is the consumer-owned leaf payload. The trie itself
// never inspects it — it only stores the pointer on each leaf so a consumer
// (the effects engine) can attach a per-key cached subdag without leaking its
// types into keytrie. Plain tip indexes use T = struct{} and never touch it.
type critNode[T any] struct {
	child     [2]atomic.Pointer[critNode[T]] // 16 bytes, offset 0  (internal)
	bytePos   uint32                         // 4 bytes,  offset 16 (internal)
	otherbits uint8                          // 1 byte,   offset 20 (internal)
	isLeaf    bool                           // 1 byte,   offset 21
	// pinned caches the eviction decider's verdict (the system-key pin), computed
	// once when the leaf is created, so the sweep reads a bit instead of calling
	// the decider — a func-pointer indirect plus a per-key prefix check — on every
	// leaf it visits. The verdict is a pure function of the immutable key; key
	// updates reuse the leaf, so it never needs recomputing. Written once before
	// publish, so no atomic is needed (same discipline as key).
	pinned bool    // 1 byte,   offset 22 (leaf)
	_      [1]byte //           offset 23
	key       string                         // 16 bytes, offset 24 (leaf)
	tips      atomic.Pointer[TipSet]         // 8 bytes,  offset 40 (leaf)
	deleted   atomic.Bool                    // 4 bytes,  offset 44 (leaf)
	// Eviction metadata (leaf). A leaf spans a second cache line now;
	// these are written on the hot read path (freq bump) so they sit
	// apart from the structural fields above. freq < 0 marks a ghost:
	// a soft-deleted leaf retaining |freq| so a returning key warms back.
	freq       atomic.Int32  // 4 bytes,  offset 48
	chunkIdx   uint32        // 4 bytes,  offset 52 (arena chunk, for reclamation)
	lastAccess atomic.Uint64 // 8 bytes,  offset 56 (LRU tiebreak)
	// pins counts dynamic do-not-evict holds (Pin/Unpin). The cloud outbox
	// holds one while any of the key's effects await a durability ack:
	// evicting then would free almost nothing — the outbox already pins the
	// effect bytes — while making the key invisible cluster-wide (this node
	// unsubscribes and drops its tips, and the cloud has no tip markers yet),
	// so every read of the key anywhere misses data the cluster durably
	// holds. Unlike the static pinned bit this changes at runtime, so the
	// sweep pays one atomic load per visited leaf.
	pins atomic.Int32      // 4 bytes,  offset 64
	data atomic.Pointer[T] // 8 bytes,  offset 72 (leaf payload, 8-aligned)
}

func noop() {}

func (n *critNode[T]) isDeleted() bool {
	return n.deleted.Load()
}

// Critbit is a crit-bit trie for storing string keys, generic over the
// per-leaf payload type T (see critNode).
type Critbit[T any] struct {
	root   atomic.Pointer[critNode[T]]
	size   atomic.Int64
	closed atomic.Bool

	deletedCount atomic.Int64 // deleted-but-linked leaves still in the tree
	reapRunning  atomic.Bool  // prevents concurrent reap goroutines

	// reapQueue holds leaves that have gone dead (deleted/evicted) and need to be
	// unlinked from the tree. Populated at delete time, drained by a background
	// worker (see drainReap) so the unlink never rides the hot path. reapDropped
	// counts pushes lost to a full queue; a non-zero value schedules a full-BFS
	// reap as a backstop.
	reapQueue   *xsync.MPMCQueue[*critNode[T]]
	reapDropped atomic.Int64

	// Eviction policy (lifted from CloxCache as a single bounded domain;
	// see evict.go). Sweeps run serialized under evictMu — a cold path —
	// while the read path only bumps per-leaf freq/lastAccess + the window
	// counters. A sweep advances a CLOCK hand over the leaf arena's slots
	// (CloxCache's slot scan, adapted to the trie), reading them sequentially
	// rather than by per-sample tree descent. evictHand is the hand position,
	// mutated only under evictMu (no atomics needed).
	evictMu        sync.Mutex
	evictHand      uint64
	evictK         atomic.Int32  // protected-freq threshold (adaptive)
	ghostCount     atomic.Int64  // soft-deleted leaves retained for warm restart
	windowHits     windowCounter // live accesses in the current adapt window (sharded)
	windowOps      windowCounter // total accesses (live + ghost) in the window (sharded)
	evictedProt    atomic.Uint64 // evicted with freq > k (protected fallback)
	evictedUnprot  atomic.Uint64 // evicted with freq <= k (unprotected)
	reachedProt    atomic.Uint64 // leaves whose freq crossed k under pressure
	prevHitRate    atomic.Uint64 // last window hit rate * 10000 (gradient input)
	rateLow        atomic.Uint32 // adaptive low graduation threshold * 10000
	rateHigh       atomic.Uint32 // adaptive high graduation threshold * 10000
	lastKDir       atomic.Int32  // direction of the last k change (+1/-1/0)
	lastAdaptCheck atomic.Uint64 // eviction count at the last adapt check
	evictDecider   func(key string) bool
	evictNotify    func(key string, tips []EffectRef, data *T)
	// underPressure reports whether the consumer is currently over its eviction
	// target — the keytrie analogue of CloxCache's instantaneous
	// entryCount >= capacity check. A graduation (a leaf's freq crossing k) is
	// counted only while this is true, so adaptive-k measures the current
	// eviction episode instead of accumulating graduations across idle pauses.
	underPressure func() bool
	evictActive   atomic.Bool // true once eviction hooks are installed

	// refDelta, if set, is called on every leaf tip-set transition so the
	// consumer can maintain per-tip refcounts without re-deriving them at each
	// call site. added/removed are the tip-set delta; droppedData is the leaf
	// payload released by a delete or eviction (nil for a plain tip change), so
	// the consumer can release references the payload held. The trie owns the
	// "when"; the consumer owns the "what". Runs without any trie lock the
	// consumer could re-enter.
	refDelta func(added, removed []EffectRef, droppedData *T)

	initOnce sync.Once
	// Two arenas of the same node type — leaves and internal nodes never migrate
	// between roles (isLeaf is set at alloc and never flipped), so a node born in
	// one arena dies there. Splitting them lets the eviction sweep scan only the
	// leaf arena (dense, no internal-node skipping) and lets the sweep reclaim a
	// whole leaf chunk once every slot in it is dead. Individual slots are never
	// reused in place (that would require synchronizing every lock-free reader
	// against reinitialization); reclamation is at chunk granularity, and a
	// reclaimed chunk's slot range is recycled through a fresh, never-published
	// array via the arena free list (see dropChunk).
	leafArena     critbitArena[critNode[T]]
	internalArena critbitArena[critNode[T]]

	// reapMu coordinates structural modifications. Inserts take a
	// read-lock (concurrent inserts proceed in parallel). The reaper
	// takes a write-lock so no insert can be mid-CAS on a child
	// pointer while the reaper unlinks a subtree.
	reapMu xsync.RBMutex
}

// NewCritbit creates a new crit-bit trie with leaf payload type T.
func NewCritbit[T any]() *Critbit[T] {
	c := &Critbit[T]{}
	c.ensureInit()
	return c
}

// freeSlotCapacity bounds the per-arena recycled-slot free list. Recycling that
// would overflow it falls back to dropping the dead chunk to nil (the old
// behavior), so this only caps how much eviction churn the allocator can buffer,
// never correctness. 64Ki slots ≈ 32 chunks.
const freeSlotCapacity = 1 << 16

// arenaChunkList holds an immutable snapshot of the chunk slice plus, per chunk,
// a count of how many of its nodes have been unlinked (reaped) and how many of
// its slots are still waiting in the free list (outstanding). Swapped atomically
// so readers never see a partially-grown slice. A fully-reaped chunk is either
// recycled — replaced with a fresh empty chunk whose slots return to the free
// list — or, if the free list is saturated, dropped to a nil entry (indices stay
// stable; GC reclaims the old array once no stale reader still points into it).
// reaped/outstanding entries are *atomic.Int64 so the copy-on-write swap shares
// the live counters.
type arenaChunkList[T any] struct {
	chunks      [][]T
	reaped      []*atomic.Int64
	outstanding []*atomic.Int64
}

type critbitArena[T any] struct {
	next      atomic.Uint64
	chunkSize uint64
	growMu    sync.Mutex // only held when allocating a new chunk (every chunkSize allocs)
	list      atomic.Pointer[arenaChunkList[T]]
	// freeList recycles the slots of fully-reaped chunks so the arena's footprint
	// tracks the live working set instead of the all-time high-water mark — without
	// it, monotonic next climbs forever under key churn and the eviction sweep
	// walks an ever-larger arena of dead slots. It holds reusable slot indices;
	// alloc drains it before bumping next. freeCount mirrors its occupancy (it has
	// no Size) so dropChunk can pre-check room for a whole chunk before committing
	// a recycle. MPMC: many allocators dequeue, dropChunk enqueues under growMu.
	// Reuse is safe because recycling installs a brand-new chunk array — its slots
	// have never been published, so no lock-free reader can hold a pointer into
	// them, while the dead array goes to GC exactly as the nil path does.
	freeList  *xsync.MPMCQueue[uint64]
	freeCount atomic.Int64
	freeCap   uint64
	// initNode, if set, runs on every node of a freshly-grown chunk before that
	// chunk is published in list. The leaf arena uses it to stamp deleted=true on
	// every slot, so the eviction sweep (which scans the arena directly) skips a
	// slot until allocLeafNode + Insert have written and published a real leaf.
	initNode func(*T)
}

func (a *critbitArena[T]) init(chunkSize int, initNode func(*T)) {
	if chunkSize < 1 {
		chunkSize = 1
	}
	a.chunkSize = uint64(chunkSize)
	a.initNode = initNode
	a.freeCap = freeSlotCapacity
	a.freeList = xsync.NewMPMCQueue[uint64](freeSlotCapacity)
	a.list.Store(&arenaChunkList[T]{})
}

// chunkSnapshot returns the current chunk slice and chunk size for a direct
// arena scan (the eviction sweep). The slice is immutable once published, so a
// concurrent grow can't tear it.
func (a *critbitArena[T]) chunkSnapshot() ([][]T, uint64) {
	return a.list.Load().chunks, a.chunkSize
}

func (a *critbitArena[T]) alloc() (*T, uint32) {
	// Reuse a recycled slot first, so the arena's footprint tracks live keys
	// rather than the all-time high-water mark. A recycled index points into a
	// fresh chunk dropChunk installed (and published in list before enqueuing the
	// index), so the load below sees it. Every allocator holds the trie's reap
	// lock (read or write) from claim through publish, so a compaction pass —
	// which skips chunks with outstanding recycled slots — can never evacuate the
	// chunk out from under a half-initialized slot.
	if idx, ok := a.freeList.TryDequeue(); ok {
		a.freeCount.Add(-1)
		chunkIdx := idx / a.chunkSize
		list := a.list.Load()
		list.outstanding[chunkIdx].Add(-1)
		return &list.chunks[chunkIdx][idx%a.chunkSize], uint32(chunkIdx)
	}

	// Fast path: atomically claim a fresh slot index. No lock needed.
	idx := a.next.Add(1) - 1
	chunkIdx := idx / a.chunkSize
	offset := idx % a.chunkSize

	list := a.list.Load()
	if chunkIdx >= uint64(len(list.chunks)) {
		// Slow path: need a new chunk. Only one goroutine allocates;
		// others wait on the mutex then find the chunk already exists.
		a.grow(chunkIdx)
		list = a.list.Load()
	}

	return &list.chunks[chunkIdx][offset], uint32(chunkIdx)
}

// recordReaped notes that one node in chunkIdx is permanently dead — unlinked
// from the tree by the reaper (write lock) or orphaned by a lost insert race
// (read lock). Each slot is accounted at most once: unlink is identity-checked
// and an orphan is counted exactly where it is abandoned. When every slot in
// the chunk is accounted, the chunk is dropped so GC can reclaim it. The drop
// is safe from either lock: the counter is shared across list snapshots, grow
// and drop serialize on growMu, and a concurrent sweep iterating a dropped
// chunk's memory sees only deleted slots (a droppable chunk holds no ghosts —
// ghosts are never accounted).
func (a *critbitArena[T]) recordReaped(chunkIdx uint32) {
	list := a.list.Load()
	if int(chunkIdx) >= len(list.reaped) || list.reaped[chunkIdx] == nil {
		return
	}
	if list.reaped[chunkIdx].Add(1) == int64(a.chunkSize) {
		a.dropChunk(chunkIdx)
	}
}

// dropChunk handles a fully-unlinked chunk. The dead array is always released to
// GC (it is no longer referenced by list once we swap), so a stale lock-free
// reader still pointing into it stays valid until it drops its own pointer.
//
// When the free list has room for a whole chunk, it RECYCLES: a fresh empty chunk
// takes the slot's place and its pristine slots return to the free list for
// reuse, so the arena stops growing under churn. The fresh array's slots have
// never been published, so no reader can be mid-read on them — reuse is safe. The
// reaped counter is reset for the new generation, and outstanding is armed so
// compaction skips the chunk until every recycled slot has been re-allocated.
// Otherwise it falls back to the old behavior: a nil placeholder (indices stay
// stable; monotonic next never revisits a nil'd chunk).
//
// Its sole caller is recordReaped's counter hitting chunkSize — every drop is
// counter-driven, and per-slot at-most-once accounting means it fires exactly
// once per chunk generation. growMu serializes concurrent drops of different
// chunks, so the free list has a single logical enqueuer and the room pre-check
// below guarantees every TryEnqueue succeeds (freeCount is decremented by
// allocators only after removal, so it never understates occupancy).
func (a *critbitArena[T]) dropChunk(chunkIdx uint32) {
	a.growMu.Lock()
	defer a.growMu.Unlock()
	list := a.list.Load()
	if int(chunkIdx) >= len(list.chunks) || list.chunks[chunkIdx] == nil {
		return
	}
	newChunks := make([][]T, len(list.chunks))
	copy(newChunks, list.chunks)

	if a.freeCount.Load()+int64(a.chunkSize) <= int64(a.freeCap) {
		fresh := make([]T, a.chunkSize)
		if a.initNode != nil {
			for i := range fresh {
				a.initNode(&fresh[i])
			}
		}
		newChunks[chunkIdx] = fresh
		list.reaped[chunkIdx].Store(0) // fresh generation: nothing reaped yet
		list.outstanding[chunkIdx].Store(int64(a.chunkSize))
		// Publish the fresh chunk before its slots become reachable via the free
		// list, so a concurrent alloc that dequeues an index sees the new array.
		a.list.Store(&arenaChunkList[T]{chunks: newChunks, reaped: list.reaped, outstanding: list.outstanding})
		base := uint64(chunkIdx) * a.chunkSize
		for i := uint64(0); i < a.chunkSize; i++ {
			a.freeList.TryEnqueue(base + i) // room pre-checked: cannot fail
		}
		a.freeCount.Add(int64(a.chunkSize))
		return
	}

	newChunks[chunkIdx] = nil
	a.list.Store(&arenaChunkList[T]{chunks: newChunks, reaped: list.reaped, outstanding: list.outstanding})
}

func (a *critbitArena[T]) grow(needed uint64) {
	a.growMu.Lock()
	defer a.growMu.Unlock()

	list := a.list.Load()
	for uint64(len(list.chunks)) <= needed {
		chunk := make([]T, a.chunkSize)
		if a.initNode != nil {
			for i := range chunk {
				a.initNode(&chunk[i])
			}
		}
		newChunks := make([][]T, len(list.chunks)+1)
		copy(newChunks, list.chunks)
		newChunks[len(list.chunks)] = chunk
		newReaped := make([]*atomic.Int64, len(list.reaped)+1)
		copy(newReaped, list.reaped)
		newReaped[len(list.reaped)] = &atomic.Int64{}
		newOutstanding := make([]*atomic.Int64, len(list.outstanding)+1)
		copy(newOutstanding, list.outstanding)
		newOutstanding[len(list.outstanding)] = &atomic.Int64{}
		list = &arenaChunkList[T]{chunks: newChunks, reaped: newReaped, outstanding: newOutstanding}
		a.list.Store(list)
	}
}

func (a *critbitArena[T]) clear() {
	a.growMu.Lock()
	a.list.Store(&arenaChunkList[T]{})
	a.next.Store(0)
	// Drain stale recycled indices: they point into chunks the reset just dropped,
	// so a later alloc must not hand them out. clear assumes no concurrent alloc
	// (same as the list/next reset above).
	if a.freeList != nil {
		for {
			if _, ok := a.freeList.TryDequeue(); !ok {
				break
			}
		}
		a.freeCount.Store(0)
	}
	a.growMu.Unlock()
}

func (c *Critbit[T]) ensureInit() {
	c.initOnce.Do(func() {
		// Leaf slots are born deleted=true (and isLeaf=true) so the eviction sweep
		// skips a slot until allocLeafNode has written it and Insert has published
		// it (deleted=false) after linking. See the sweep in evict.go.
		c.leafArena.init(defaultCritbitArenaChunkSize, func(n *critNode[T]) {
			n.isLeaf = true
			n.deleted.Store(true)
		})
		c.internalArena.init(defaultCritbitArenaChunkSize, nil)
		c.reapQueue = xsync.NewMPMCQueue[*critNode[T]](reapQueueCapacity)
		c.evictK.Store(defaultProtectedFreqThreshold)
		c.rateLow.Store(defaultRateLow)
		c.rateHigh.Store(defaultRateHigh)
	})
}

// enqueueReap queues a leaf that just went dead for background unlinking. A drop
// (full queue) bumps reapDropped so maybeReap falls back to a full BFS, which
// rediscovers stragglers — nothing is leaked, the fast path is just bypassed.
func (c *Critbit[T]) enqueueReap(leaf *critNode[T]) {
	if c.reapQueue == nil || !c.reapQueue.TryEnqueue(leaf) {
		c.reapDropped.Add(1)
	}
}

// allocLeafNode returns a fresh leaf from the leaf arena. The node is born
// deleted=true (the arena init hook) and stays so while its fields are written,
// so the eviction sweep — which scans the leaf arena directly — skips it; the
// caller flips deleted=false only after linking it into the tree, which is also
// when its fields become visible to readers (release).
func (c *Critbit[T]) allocLeafNode(key string, ts *TipSet) *critNode[T] {
	n, ci := c.leafArena.alloc() // born deleted=true via the arena init hook
	n.chunkIdx = ci
	n.key = key
	// A decider returning false means "do not evict" → pinned. When no decider is
	// installed, no sweep ever runs, so the default false is inert.
	n.pinned = c.evictDecider != nil && !c.evictDecider(key)
	n.tips.Store(ts)
	n.freq.Store(1) // start warm enough to survive one sweep
	n.lastAccess.Store(monotonic())
	return n
}

func (c *Critbit[T]) allocInternalNode(bytePos uint32, otherbits uint8) *critNode[T] {
	n, ci := c.internalArena.alloc()
	n.chunkIdx = ci
	n.bytePos = bytePos
	n.otherbits = otherbits
	return n
}

func highestBit(b byte) uint8 {
	if b == 0 {
		return 0
	}
	b |= b >> 1
	b |= b >> 2
	b |= b >> 4
	return b ^ (b >> 1)
}

func findCritBit(a, b string) (bytePos uint32, otherbits uint8) {
	minLen := min(len(b), len(a))

	for i := range minLen {
		if a[i] != b[i] {
			return uint32(i), highestBit(a[i]^b[i]) ^ 0xFF
		}
	}

	if len(a) != len(b) {
		bytePos = uint32(minLen)
		var diffByte byte
		if len(a) > minLen {
			diffByte = a[minLen]
		} else {
			diffByte = b[minLen]
		}
		hb := highestBit(diffByte)
		if hb == 0 {
			return bytePos, 0xFF // length-discrimination sentinel
		}
		return bytePos, hb ^ 0xFF
	}
	return 0, 0
}

// getDirection uses branchless arithmetic matching the reference C implementation.
// otherbits has all bits set except the critical bit.
// (1 + uint16(otherbits | c)) >> 8 yields 1 when the critical bit is set in c, 0 otherwise.
func getDirection(key string, bytePos uint32, otherbits uint8) int {
	if int(bytePos) >= len(key) {
		return 0
	}
	c := key[bytePos]
	return int(1+uint16(otherbits|c)) >> 8
}

// Insert attempts to store a TipSet for the given key using CAS.
//
// For new keys (old is nil): CAS retry loop for tree structural insertion.
// For existing keys: single CAS comparing old to the current pointer.
//
// On success: returns (nil, true).
// On CAS failure: returns (currentTips, false) — the conflicting tip set.
func (c *Critbit[T]) Insert(key string, old *TipSet, new *TipSet) (*TipSet, bool) {
	if c.closed.Load() {
		return nil, false
	}
	c.ensureInit()

	for {
		if c.closed.Load() {
			return nil, false
		}

		rt := c.reapMu.RLock()

		// The root MUST be loaded under the read lock: the reaper and the
		// compactor (write lock) detach nodes whose child pointers remain
		// valid-looking and writable. A root captured before the lock can name
		// a path through a detached node, and a structural CAS landing on such
		// a node succeeds — silently linking the new leaf into an unreachable
		// subtree. Under the lock, every node reachable from root is linked.
		rootNode := c.root.Load()

		if rootNode == nil {
			newNode := c.allocLeafNode(key, new) // deleted=true, pop under the read lock
			if c.root.CompareAndSwap(nil, newNode) {
				newNode.deleted.Store(false) // publish only after linking
				c.reapMu.RUnlock(rt)
				c.size.Add(1)
				c.fireTipDelta(old, new, nil)
				return nil, true
			}
			c.reapMu.RUnlock(rt)
			// Lost the CAS: newNode is an unlinked orphan, still deleted=true, so
			// the sweep skips it. Its slot is never reused; account it reaped now,
			// or its chunk could never reach a full count and would be pinned from
			// reclamation forever by this one dead slot.
			c.leafArena.recordReaped(newNode.chunkIdx)
			continue
		}

		bestLeaf := c.findBestMatch(rootNode, key)
		if bestLeaf == nil {
			c.reapMu.RUnlock(rt)
			continue
		}

		if bestLeaf.key == key {
			// Existing key — single CAS attempt.
			// If the leaf is deleted, the caller may pass old=nil (from Contains
			// returning nil). Accept that by swapping old to the actual current
			// pointer so the CAS can succeed.
			current := bestLeaf.tips.Load()
			if old == nil && bestLeaf.isDeleted() {
				old = current
			}
			if bestLeaf.tips.CompareAndSwap(old, new) {
				if bestLeaf.deleted.CompareAndSwap(true, false) {
					c.size.Add(1)
					// A ghost (freq < 0) was never counted as a deleted leaf;
					// promote it instead of decrementing deletedCount.
					if bestLeaf.freq.Load() < 0 {
						c.promoteIfGhost(bestLeaf)
					} else {
						c.deletedCount.Add(-1)
					}
				}
				c.reapMu.RUnlock(rt)
				c.fireTipDelta(old, new, nil)
				return nil, true
			}
			tips := bestLeaf.tips.Load()
			c.reapMu.RUnlock(rt)
			return tips, false
		}

		bytePos, otherbits := findCritBit(key, bestLeaf.key)
		if otherbits == 0 {
			current := bestLeaf.tips.Load()
			if old == nil && bestLeaf.isDeleted() {
				old = current
			}
			if bestLeaf.tips.CompareAndSwap(old, new) {
				if bestLeaf.deleted.CompareAndSwap(true, false) {
					c.size.Add(1)
					// A ghost (freq < 0) was never counted as a deleted leaf;
					// promote it instead of decrementing deletedCount.
					if bestLeaf.freq.Load() < 0 {
						c.promoteIfGhost(bestLeaf)
					} else {
						c.deletedCount.Add(-1)
					}
				}
				c.reapMu.RUnlock(rt)
				c.fireTipDelta(old, new, nil)
				return nil, true
			}
			tips := bestLeaf.tips.Load()
			c.reapMu.RUnlock(rt)
			return tips, false
		}

		newLeafNode := c.allocLeafNode(key, new) // deleted=true
		newInternal := c.allocInternalNode(bytePos, otherbits)

		newDir := getDirection(key, bytePos, otherbits)
		oldDir := 1 - newDir

		ok := c.insertNode(rootNode, newInternal, newLeafNode, newDir, oldDir, bytePos, otherbits)
		if ok {
			newLeafNode.deleted.Store(false) // publish after linking, before releasing the read lock
			c.reapMu.RUnlock(rt)
			c.size.Add(1)
			c.fireTipDelta(old, new, nil)
			return nil, true
		}
		c.reapMu.RUnlock(rt)
		// Lost: newLeafNode/newInternal are unlinked orphans (leaf still
		// deleted=true, so the sweep skips it). Account both slots reaped so
		// they don't pin their chunks from reclamation, then retry.
		c.leafArena.recordReaped(newLeafNode.chunkIdx)
		c.internalArena.recordReaped(newInternal.chunkIdx)
	}
}

func (c *Critbit[T]) findBestMatch(node *critNode[T], key string) *critNode[T] {
	if node == nil {
		return nil
	}
	current := node
	for !current.isLeaf {
		dir := getDirection(key, current.bytePos, current.otherbits)
		next := current.child[dir].Load()
		if next == nil {
			return nil
		}
		current = next
	}
	return current
}

func (c *Critbit[T]) insertNode(rootNode *critNode[T], newInternal *critNode[T], newLeafNode *critNode[T], newDir, oldDir int, bytePos uint32, otherbits uint8) bool {
	if rootNode.isLeaf {
		newInternal.child[newDir].Store(newLeafNode)
		newInternal.child[oldDir].Store(rootNode)
		return c.root.CompareAndSwap(rootNode, newInternal)
	}

	if shouldInsertBefore(bytePos, otherbits, rootNode.bytePos, rootNode.otherbits) {
		newInternal.child[newDir].Store(newLeafNode)
		newInternal.child[oldDir].Store(rootNode)
		return c.root.CompareAndSwap(rootNode, newInternal)
	}

	return c.walkAndInsert(rootNode, newInternal, newLeafNode, newDir, oldDir, bytePos, otherbits)
}

func shouldInsertBefore(newBytePos uint32, newOtherbits uint8, curBytePos uint32, curOtherbits uint8) bool {
	if newBytePos < curBytePos {
		return true
	}
	if newBytePos > curBytePos {
		return false
	}
	// Lower otherbits = more significant bit (comparison flips vs bitMask)
	return newOtherbits < curOtherbits
}

func (c *Critbit[T]) walkAndInsert(rootNode *critNode[T], newInternal *critNode[T], newLeafNode *critNode[T], newDir, oldDir int, bytePos uint32, otherbits uint8) bool {
	current := rootNode

	for {
		// The new node's bit must be strictly less significant than the
		// current parent's bit. A concurrent insert may have spliced a
		// new internal node with the same (or more-significant) bit into
		// the path we are walking, which would create a duplicate or
		// out-of-order bit position if we insert here.
		if !shouldInsertBefore(current.bytePos, current.otherbits, bytePos, otherbits) {
			return false
		}

		dir := getDirection(newLeafNode.key, current.bytePos, current.otherbits)
		childNode := current.child[dir].Load()

		if childNode == nil {
			return false
		}

		if childNode.isLeaf {
			// Verify the precomputed crit bit is still valid for this leaf.
			// A concurrent insert may have replaced the original leaf with a
			// different one, making the crit bit (computed against the old
			// best-match leaf) incorrect for this pairing.
			actualBytePos, actualOtherbits := findCritBit(newLeafNode.key, childNode.key)
			if actualBytePos != bytePos || actualOtherbits != otherbits {
				return false // tree changed; retry from Insert's outer loop
			}
			newInternal.child[newDir].Store(newLeafNode)
			newInternal.child[oldDir].Store(childNode)
			return current.child[dir].CompareAndSwap(childNode, newInternal)
		}

		if shouldInsertBefore(bytePos, otherbits, childNode.bytePos, childNode.otherbits) {
			newInternal.child[newDir].Store(newLeafNode)
			newInternal.child[oldDir].Store(childNode)
			return current.child[dir].CompareAndSwap(childNode, newInternal)
		}

		current = childNode
	}
}

// Pin adds a dynamic do-not-evict hold on key's live leaf, reporting whether
// one was taken (false: no live leaf — nothing to protect). Taken under the
// reap read lock, writer discipline: chunk compaction copies leaf fields under
// the write lock, so a hold can never land on a husk the copy already left.
func (c *Critbit[T]) Pin(key string) bool {
	if c.closed.Load() {
		return false
	}
	rt := c.reapMu.RLock()
	defer c.reapMu.RUnlock(rt)
	root := c.root.Load()
	if root == nil {
		return false
	}
	leaf := c.findBestMatch(root, key)
	if leaf == nil || leaf.key != key || leaf.isDeleted() {
		return false
	}
	leaf.pins.Add(1)
	// Dekker partner of EvictBatch's post-claim pins re-check: if the sweep
	// claimed this leaf between our liveness check and the Add, exactly one of
	// us observes the other — undo so the hold is never stranded on an evicted
	// leaf (a ghost promotion would resurrect it as a permanent pin).
	if leaf.isDeleted() {
		leaf.pins.Add(-1)
		return false
	}
	return true
}

// Unpin releases one dynamic hold on key's live leaf. A missing or deleted
// leaf is a no-op (the key was explicitly deleted or the index flushed while
// held — the hold died with the leaf). A negative count on a live leaf is an
// unpin without a matching pin: a protocol bug that would let the sweep evict
// a key another holder still protects, so it panics like a refcount underflow.
func (c *Critbit[T]) Unpin(key string) bool {
	if c.closed.Load() {
		return false
	}
	rt := c.reapMu.RLock()
	defer c.reapMu.RUnlock(rt)
	root := c.root.Load()
	if root == nil {
		return false
	}
	leaf := c.findBestMatch(root, key)
	if leaf == nil || leaf.key != key || leaf.isDeleted() {
		return false
	}
	if n := leaf.pins.Add(-1); n < 0 {
		panic(fmt.Sprintf("keytrie: pin underflow on %q (unpin without matching pin)", key))
	}
	return true
}

// Contains checks if a key exists and returns its TipSet.
// Returns nil if the key doesn't exist or is deleted.
func (c *Critbit[T]) Contains(key string) *TipSet {
	if c.closed.Load() {
		return nil
	}
	rootNode := c.root.Load()
	if rootNode == nil {
		return nil
	}
	leaf := c.findBestMatch(rootNode, key)
	if leaf == nil || leaf.key != key || leaf.isDeleted() {
		return nil
	}
	return leaf.tips.Load()
}

// bumpAccess records an access to leaf: it raises the frequency counter
// (saturating at maxLeafFreq) and stamps lastAccess from the monotonic clock.
// Lock-free; called on the hot read path. A ghost (freq < 0) is left alone —
// it is promoted explicitly when its key is re-inserted.
func (c *Critbit[T]) bumpAccess(leaf *critNode[T]) {
	// When eviction is inactive (no memory governor) nothing consumes the
	// freq/LRU/window data, so skip all of it — this keeps the read path free
	// of trie-global atomic contention in the default, no-eviction case.
	if !c.evictActive.Load() {
		return
	}
	for {
		f := leaf.freq.Load()
		if f < 0 || f >= maxLeafFreq {
			break
		}
		if leaf.freq.CompareAndSwap(f, f+1) {
			// Count a leaf crossing into protected status, but only while
			// instantaneously over the eviction target — CloxCache's
			// entryCount >= capacity guard. The graduation rate is
			// graduated/evictions; counting graduations while not under pressure
			// (cold start, or an idle pause after the governor brought us back
			// under target) inflates the numerator against a frozen denominator
			// and rails k. underPressure must be the *current* over-target
			// condition, not a "have we ever evicted" latch — a latch never falls
			// back to false, so the rate accumulates across pauses. The predicate
			// runs only at the f==k crossing (once per leaf lifetime), not on
			// every read.
			if f == c.evictK.Load() && c.underPressure != nil && c.underPressure() {
				c.reachedProt.Add(1)
			}
			break
		}
	}
	leaf.lastAccess.Store(monotonic())
	// Window accounting for adaptive-k: a live-leaf access is a hit. Sharded by
	// leaf address so the per-access increment doesn't serialize on one line.
	p := unsafe.Pointer(leaf)
	c.windowHits.add(p, 1)
	c.windowOps.add(p, 1)
}

// LoadOrStoreData returns the leaf payload for key, installing def if none is
// present yet (CAS, so concurrent callers agree on a single payload). Returns
// (nil, false) if the key is missing/deleted. The bool reports whether an
// existing payload was loaded (true) rather than def being stored (false).
//
// Locating the payload counts as an access: it bumps the leaf's frequency and
// last-access stamp, which is the signal the eviction policy learns from. The
// read path (reconstruct) calls this on every read, hit or miss.
//
// The install CAS runs under the reap read-lock like every other leaf-field
// mutation: compaction (write lock) relocates leaves, and a CAS landing on a
// relocated husk would be silently lost.
func (c *Critbit[T]) LoadOrStoreData(key string, def *T) (*T, bool) {
	if c.closed.Load() {
		return nil, false
	}
	rt := c.reapMu.RLock()
	defer c.reapMu.RUnlock(rt)
	rootNode := c.root.Load()
	if rootNode == nil {
		return nil, false
	}
	leaf := c.findBestMatch(rootNode, key)
	if leaf == nil || leaf.key != key || leaf.isDeleted() {
		return nil, false
	}
	c.bumpAccess(leaf)
	if cur := leaf.data.Load(); cur != nil {
		return cur, true
	}
	if leaf.data.CompareAndSwap(nil, def) {
		return def, false
	}
	return leaf.data.Load(), true
}

// tipSetDiff returns the tips added (in new, not old) and removed (in old, not
// new) for an old→new transition. Tip sets are small, so a linear scan is cheap.
func tipSetDiff(old, new *TipSet) (added, removed []EffectRef) {
	if new != nil {
		for _, t := range new.tips {
			if old == nil || !old.Contains(t) {
				added = append(added, t)
			}
		}
	}
	if old != nil {
		for _, t := range old.tips {
			if new == nil || !new.Contains(t) {
				removed = append(removed, t)
			}
		}
	}
	return added, removed
}

// fireTipDelta reports a leaf tip-set transition to the refDelta hook (if any).
// droppedData is the leaf payload released by a delete/eviction, else nil.
func (c *Critbit[T]) fireTipDelta(old, new *TipSet, droppedData *T) {
	if c.refDelta == nil {
		return
	}
	added, removed := tipSetDiff(old, new)
	if len(added) > 0 || len(removed) > 0 || droppedData != nil {
		c.refDelta(added, removed, droppedData)
	}
}

// RemoveTips drops refs from key's tip set (CAS retry). The CAS runs under the
// reap read-lock like every other leaf-field mutation: compaction (write lock)
// relocates leaves, and a CAS landing on a relocated husk would be silently
// lost. The delta fires outside the lock (the hook may re-enter the trie).
func (c *Critbit[T]) RemoveTips(key string, refs []EffectRef) {
	if c.closed.Load() || len(refs) == 0 {
		return
	}

	removeSet := make(map[EffectRef]bool, len(refs))
	for _, r := range refs {
		removeSet[r] = true
	}

	rt := c.reapMu.RLock()
	rootNode := c.root.Load()
	if rootNode == nil {
		c.reapMu.RUnlock(rt)
		return
	}
	leaf := c.findBestMatch(rootNode, key)
	if leaf == nil || leaf.key != key || leaf.isDeleted() {
		c.reapMu.RUnlock(rt)
		return
	}

	for {
		current := leaf.tips.Load()
		if current == nil {
			c.reapMu.RUnlock(rt)
			return
		}
		var kept []EffectRef
		for _, tip := range current.Tips() {
			if !removeSet[tip] {
				kept = append(kept, tip)
			}
		}
		if len(kept) == len(current.Tips()) {
			c.reapMu.RUnlock(rt)
			return // nothing to remove
		}
		newTips := NewTipSet(kept...)
		if leaf.tips.CompareAndSwap(current, newTips) {
			c.reapMu.RUnlock(rt)
			c.fireTipDelta(current, newTips, nil)
			return
		}
		// CAS failed, retry
	}
}

func (c *Critbit[T]) Delete(key string, old *TipSet) bool {
	return c.DeleteAndSnapshot(key, old) != nil
}

// DeleteAndSnapshot removes a key only if its current tips match old
// (CAS). Returns the previous tip set on success, nil on failure.
func (c *Critbit[T]) DeleteAndSnapshot(key string, old *TipSet) *TipSet {
	if c.closed.Load() {
		return nil
	}

	rt := c.reapMu.RLock()

	rootNode := c.root.Load()
	if rootNode == nil {
		c.reapMu.RUnlock(rt)
		return nil
	}
	leaf := c.findBestMatch(rootNode, key)
	if leaf == nil || leaf.key != key {
		c.reapMu.RUnlock(rt)
		return nil
	}
	if !leaf.deleted.CompareAndSwap(false, true) {
		c.reapMu.RUnlock(rt)
		return nil
	}
	if !leaf.tips.CompareAndSwap(old, nil) {
		leaf.deleted.Store(false)
		c.reapMu.RUnlock(rt)
		return nil
	}
	// Drop the leaf payload too — a deleted key's cached state is stale.
	droppedData := leaf.data.Load()
	leaf.data.Store(nil)
	c.size.Add(-1)

	c.reapMu.RUnlock(rt)

	// Release the tips and the payload's references — DeleteAndSnapshot bypasses
	// the eviction notify, so this is the only refcount signal for FlushIndex.
	c.fireTipDelta(old, nil, droppedData)

	deleted := c.deletedCount.Add(1)
	c.enqueueReap(leaf)
	live := c.size.Load()
	if deleted > 0 && deleted > (live+deleted)/10 {
		c.maybeReap()
	}

	return old
}

func (c *Critbit[T]) Size() int64 {
	if c.closed.Load() {
		return 0
	}
	return c.size.Load()
}

// bytes returns the backing-array footprint of the arena: the sum of the
// node-struct bytes across every live (non-dropped) chunk. This is the
// contiguous slot memory only — the key strings and TipSets the nodes point
// at live on the heap and are accounted elsewhere. A dropped chunk is a nil
// entry and contributes nothing, so this shrinks exactly when dropChunk fires.
func (a *critbitArena[T]) bytes() int64 {
	list := a.list.Load()
	if list == nil {
		return 0
	}
	var node T
	nodeSize := int64(unsafe.Sizeof(node))
	var total int64
	for _, chunk := range list.chunks {
		if chunk != nil {
			total += int64(len(chunk)) * nodeSize
		}
	}
	return total
}

// ArenaBytes returns the combined slot-array footprint of the leaf and internal
// arenas. Append-only growth means this tracks the high-water mark of allocated
// nodes and only drops when a whole chunk is reclaimed — so it is the direct
// measure of trie-skeleton memory, distinct from the vertex pool's effect bytes.
func (c *Critbit[T]) ArenaBytes() int64 {
	if c.closed.Load() {
		return 0
	}
	return c.leafArena.bytes() + c.internalArena.bytes()
}

func (c *Critbit[T]) Range(fn func(key string) bool) {
	if c.closed.Load() || fn == nil {
		return
	}
	rootNode := c.root.Load()
	if rootNode != nil {
		c.rangeNode(rootNode, fn)
	}
}

func (c *Critbit[T]) rangeNode(node *critNode[T], fn func(key string) bool) bool {
	if node == nil {
		return true
	}
	if node.isLeaf {
		if !node.isDeleted() {
			return fn(node.key)
		}
		return true
	}
	if left := node.child[0].Load(); left != nil {
		if !c.rangeNode(left, fn) {
			return false
		}
	}
	if right := node.child[1].Load(); right != nil {
		return c.rangeNode(right, fn)
	}
	return true
}

func (c *Critbit[T]) RangeFrom(after string, fn func(key string) bool) {
	if c.closed.Load() || fn == nil {
		return
	}
	if after == "" {
		c.Range(fn)
		return
	}
	rootNode := c.root.Load()
	if rootNode != nil {
		c.rangeFromNode(rootNode, after, fn)
	}
}

func (c *Critbit[T]) rangeFromNode(node *critNode[T], after string, fn func(key string) bool) bool {
	if node == nil {
		return true
	}
	if node.isLeaf {
		if !node.isDeleted() && node.key > after {
			return fn(node.key)
		}
		return true
	}
	if left := node.child[0].Load(); left != nil {
		if !c.rangeFromNode(left, after, fn) {
			return false
		}
	}
	if right := node.child[1].Load(); right != nil {
		return c.rangeFromNode(right, after, fn)
	}
	return true
}

func (c *Critbit[T]) RangePrefix(prefix string, fn func(key string) bool) {
	if c.closed.Load() || fn == nil {
		return
	}
	rootNode := c.root.Load()
	if rootNode == nil {
		return
	}
	subtree := c.findPrefixSubtree(rootNode, prefix)
	if subtree != nil {
		c.rangePrefixNode(subtree, prefix, fn)
	}
}

func (c *Critbit[T]) findPrefixSubtree(node *critNode[T], prefix string) *critNode[T] {
	current := node
	for current != nil && !current.isLeaf {
		if int(current.bytePos) >= len(prefix) {
			return current
		}
		dir := getDirection(prefix, current.bytePos, current.otherbits)
		current = current.child[dir].Load()
	}
	return current
}

func (c *Critbit[T]) rangePrefixNode(node *critNode[T], prefix string, fn func(key string) bool) bool {
	if node == nil {
		return true
	}
	if node.isLeaf {
		if !node.isDeleted() {
			if len(node.key) >= len(prefix) && node.key[:len(prefix)] == prefix {
				return fn(node.key)
			}
		}
		return true
	}
	if left := node.child[0].Load(); left != nil {
		if !c.rangePrefixNode(left, prefix, fn) {
			return false
		}
	}
	if right := node.child[1].Load(); right != nil {
		return c.rangePrefixNode(right, prefix, fn)
	}
	return true
}

func (c *Critbit[T]) Keys() []string {
	if c.closed.Load() {
		return nil
	}
	result := make([]string, 0, c.size.Load())
	c.Range(func(key string) bool {
		result = append(result, key)
		return true
	})
	return result
}

func (c *Critbit[T]) MatchPattern(pattern string) []string {
	if c.closed.Load() {
		return nil
	}
	if pattern == "*" {
		return c.Keys()
	}
	allKeys := c.Keys()
	result := make([]string, 0)
	for _, key := range allKeys {
		if matchGlob(key, pattern) {
			result = append(result, key)
		}
	}
	return result
}

func (c *Critbit[T]) FirstWithPrefix(prefix string, claim bool) (string, bool, ReleaseClaimFunc) {
	if c.closed.Load() {
		return "", false, nil
	}
	rootNode := c.root.Load()
	if rootNode == nil {
		return "", false, nil
	}
	subtree := c.findPrefixSubtree(rootNode, prefix)
	if subtree == nil {
		return "", false, nil
	}
	key, _, found := c.findLeftmost(subtree, prefix)
	if !found {
		return "", false, nil
	}
	if claim {
		return key, true, noop
	}
	return key, true, nil
}

func (c *Critbit[T]) findLeftmost(node *critNode[T], prefix string) (string, *critNode[T], bool) {
	if node == nil {
		return "", nil, false
	}
	if node.isLeaf {
		if node.isDeleted() {
			return "", nil, false
		}
		if len(node.key) >= len(prefix) && node.key[:len(prefix)] == prefix {
			return node.key, node, true
		}
		return "", nil, false
	}
	if left := node.child[0].Load(); left != nil {
		if key, leaf, found := c.findLeftmost(left, prefix); found {
			return key, leaf, true
		}
	}
	if right := node.child[1].Load(); right != nil {
		return c.findLeftmost(right, prefix)
	}
	return "", nil, false
}

func (c *Critbit[T]) LastWithPrefix(prefix string, claim bool) (string, bool, ReleaseClaimFunc) {
	if c.closed.Load() {
		return "", false, nil
	}
	rootNode := c.root.Load()
	if rootNode == nil {
		return "", false, nil
	}
	subtree := c.findPrefixSubtree(rootNode, prefix)
	if subtree == nil {
		return "", false, nil
	}
	key, _, found := c.findRightmost(subtree, prefix)
	if !found {
		return "", false, nil
	}
	if claim {
		return key, true, noop
	}
	return key, true, nil
}

func (c *Critbit[T]) findRightmost(node *critNode[T], prefix string) (string, *critNode[T], bool) {
	if node == nil {
		return "", nil, false
	}
	if node.isLeaf {
		if node.isDeleted() {
			return "", nil, false
		}
		if len(node.key) >= len(prefix) && node.key[:len(prefix)] == prefix {
			return node.key, node, true
		}
		return "", nil, false
	}
	if right := node.child[1].Load(); right != nil {
		if key, leaf, found := c.findRightmost(right, prefix); found {
			return key, leaf, true
		}
	}
	if left := node.child[0].Load(); left != nil {
		return c.findRightmost(left, prefix)
	}
	return "", nil, false
}

type critbitPathEntry[T any] struct {
	node *critNode[T]
	dir  int
}

func (c *Critbit[T]) NextWithPrefix(prefix, after string, claim bool) (string, bool, ReleaseClaimFunc) {
	if c.closed.Load() {
		return "", false, nil
	}
	if len(after) < len(prefix) || after[:len(prefix)] != prefix {
		return c.FirstWithPrefix(prefix, claim)
	}

	for {
		rootNode := c.root.Load()
		if rootNode == nil {
			return "", false, nil
		}

		path := make([]critbitPathEntry[T], 0, 64)
		current := rootNode

		for current != nil && !current.isLeaf {
			dir := getDirection(after, current.bytePos, current.otherbits)
			path = append(path, critbitPathEntry[T]{node: current, dir: dir})
			current = current.child[dir].Load()
		}

		var resultKey string
		found := false

		// Check if the leaf we landed on is > after (handles case when after doesn't exist)
		if current != nil && current.isLeaf && !current.isDeleted() {
			leafKey := current.key
			if len(leafKey) >= len(prefix) && leafKey[:len(prefix)] == prefix && leafKey > after {
				resultKey, found = leafKey, true
			}
		}

		// If not found yet, walk back up looking for right branches
		if !found {
			for i := len(path) - 1; i >= 0; i-- {
				entry := path[i]
				if entry.dir == 0 {
					rightNode := entry.node.child[1].Load()
					if rightNode == nil {
						continue
					}
					if int(entry.node.bytePos) < len(prefix) {
						if getDirection(prefix, entry.node.bytePos, entry.node.otherbits) == 0 {
							continue
						}
					}
					if key, _, ok := c.findLeftmost(rightNode, prefix); ok && key > after {
						resultKey, found = key, true
						break
					}
				}
			}
		}

		if !found {
			return "", false, nil
		}
		if claim {
			return resultKey, true, noop
		}
		return resultKey, true, nil
	}
}

func (c *Critbit[T]) PrevWithPrefix(prefix, before string, claim bool) (string, bool, ReleaseClaimFunc) {
	if c.closed.Load() {
		return "", false, nil
	}
	if len(before) < len(prefix) || before[:len(prefix)] != prefix {
		return c.LastWithPrefix(prefix, claim)
	}

	for {
		rootNode := c.root.Load()
		if rootNode == nil {
			return "", false, nil
		}

		path := make([]critbitPathEntry[T], 0, 64)
		current := rootNode

		for current != nil && !current.isLeaf {
			dir := getDirection(before, current.bytePos, current.otherbits)
			path = append(path, critbitPathEntry[T]{node: current, dir: dir})
			current = current.child[dir].Load()
		}

		var resultKey string
		found := false

		for i := len(path) - 1; i >= 0; i-- {
			entry := path[i]
			if entry.dir == 1 {
				leftNode := entry.node.child[0].Load()
				if leftNode == nil {
					continue
				}
				if int(entry.node.bytePos) < len(prefix) {
					if getDirection(prefix, entry.node.bytePos, entry.node.otherbits) == 1 {
						continue
					}
				}
				if key, _, ok := c.findRightmost(leftNode, prefix); ok && key < before {
					resultKey, found = key, true
					break
				}
			}
		}

		if !found {
			return "", false, nil
		}
		if claim {
			return resultKey, true, noop
		}
		return resultKey, true, nil
	}
}

func (c *Critbit[T]) TryClaimKey(key string) (exists bool, release ReleaseClaimFunc) {
	if c.closed.Load() {
		return false, nil
	}
	rootNode := c.root.Load()
	if rootNode == nil {
		return false, nil
	}
	leaf := c.findBestMatch(rootNode, key)
	if leaf == nil || leaf.key != key || leaf.isDeleted() {
		return false, nil
	}
	return true, noop
}

func (c *Critbit[T]) GetHeadHint(prefix string) string      { return "" }
func (c *Critbit[T]) SetHeadHint(prefix string, key string) {}
func (c *Critbit[T]) GetTailHint(prefix string) string      { return "" }
func (c *Critbit[T]) SetTailHint(prefix string, key string) {}

// Snapshot returns a frozen copy of the index. The new Critbit is independent —
// mutations to either copy don't affect the other. TipSets are immutable so
// only pointers are copied. Leaf payloads (T) are NOT copied: a snapshot is a
// tip-only view used for SSI reads. O(n) in key count.
func (c *Critbit[T]) Snapshot() KeyIndex {
	snap := NewCritbit[T]()
	c.Range(func(key string) bool {
		tips := c.Contains(key)
		if tips != nil {
			snap.Insert(key, nil, tips)
		}
		return true
	})
	return snap
}

func (c *Critbit[T]) maybeReap() {
	if !c.reapRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer c.reapRunning.Store(false)
		// Fast path: unlink the leaves we already know are dead, in small batches.
		for c.drainReap() > 0 {
		}
		// Backstop: if any pushes were dropped on a full queue, a full BFS pass
		// rediscovers those stragglers so nothing lingers linked forever.
		if c.reapDropped.Swap(0) > 0 {
			for c.reap() > 0 {
			}
		}
		// Unlinking alone frees a chunk only when every one of its slots died;
		// under churn, survivors fragment across chunks and the arenas ratchet.
		// Compaction evacuates sparse chunks so they can drop (see compact.go).
		c.maybeCompact()
	}()
}

// drainReap pops up to reapBatchSize queued leaves and unlinks them under a
// single short write-lock hold. Returns the number popped (0 when the queue is
// empty), so the caller can loop until drained. Each unlink re-validates the
// leaf is still dead and still the leaf for its key, so a revived or
// already-unlinked entry is harmlessly skipped.
func (c *Critbit[T]) drainReap() int {
	if c.closed.Load() || c.reapQueue == nil {
		return 0
	}
	var batch [reapBatchSize]*critNode[T]
	n := 0
	for n < reapBatchSize {
		leaf, ok := c.reapQueue.TryDequeue()
		if !ok {
			break
		}
		batch[n] = leaf
		n++
	}
	if n == 0 {
		return 0
	}
	c.reapMu.Lock()
	for i := range n {
		c.unlinkLeaf(batch[i])
	}
	c.reapMu.Unlock()
	return n
}

// unlinkLeaf removes one dead leaf (and the internal node it collapses) from the
// tree, given the leaf directly — no search. The caller must hold the reap write
// lock. It re-validates under the lock: the leaf must still be deleted, non-ghost
// (freq >= 0), and still reachable as the leaf for its key; otherwise it was
// revived or already unlinked and is skipped. Returns true if it unlinked.
func (c *Critbit[T]) unlinkLeaf(leaf *critNode[T]) bool {
	if leaf == nil || !leaf.isDeleted() || leaf.freq.Load() < 0 {
		return false
	}
	root := c.root.Load()
	if root == nil {
		return false
	}
	if root == leaf {
		c.root.Store(nil)
		c.deletedCount.Add(-1)
		c.leafArena.recordReaped(leaf.chunkIdx)
		return true
	}

	// Descend to leaf.key, tracking the parent (p) and grandparent (gp) so we can
	// splice the leaf's sibling up into p's slot, dropping both leaf and p.
	key := leaf.key
	var gp, p *critNode[T]
	var gpDir, pDir int
	cur := root
	for !cur.isLeaf {
		dir := getDirection(key, cur.bytePos, cur.otherbits)
		gp, gpDir = p, pDir
		p, pDir = cur, dir
		next := cur.child[dir].Load()
		if next == nil {
			return false
		}
		cur = next
	}
	if cur != leaf {
		return false // a different leaf occupies this key path now
	}

	sibling := p.child[1-pDir].Load()
	if sibling == nil {
		return false
	}
	if gp == nil {
		c.root.Store(sibling)
	} else {
		gp.child[gpDir].Store(sibling)
	}
	c.deletedCount.Add(-1)
	c.leafArena.recordReaped(leaf.chunkIdx)
	c.internalArena.recordReaped(p.chunkIdx)
	return true
}

// reap does a single BFS walk pruning every deleted leaf it finds.
// Takes the write-lock on reapMu so no structural insert can race.
// Returns the number of deleted leaves unlinked.
func (c *Critbit[T]) reap() int {
	if c.closed.Load() {
		return 0
	}

	c.reapMu.Lock()
	defer c.reapMu.Unlock()

	root := c.root.Load()
	if root == nil {
		return 0
	}
	if root.isLeaf {
		// Ghosts (deleted, freq < 0) are retained for warm restart.
		if root.isDeleted() && root.freq.Load() >= 0 {
			c.root.Store(nil)
			c.deletedCount.Add(-1)
			c.leafArena.recordReaped(root.chunkIdx)
			return 1
		}
		return 0
	}

	type bfsEntry struct {
		node          *critNode[T]
		parent        *critNode[T]
		dirFromParent int
	}

	reaped := 0
	queue := make([]bfsEntry, 1, 64)
	queue[0] = bfsEntry{node: root}

	for i := 0; i < len(queue); i++ {
		e := queue[i]
		node := e.node

		pruned := false
		for dir := range 2 {
			child := node.child[dir].Load()
			// Skip live leaves, internal nodes, and ghosts (freq < 0).
			if child == nil || !child.isLeaf || !child.isDeleted() || child.freq.Load() < 0 {
				continue
			}

			sibling := node.child[1-dir].Load()
			if sibling == nil {
				continue
			}

			if e.parent == nil {
				c.root.Store(sibling)
			} else {
				e.parent.child[e.dirFromParent].Store(sibling)
			}
			c.deletedCount.Add(-1)
			reaped++
			pruned = true

			// Both the deleted leaf and the internal node it collapsed are now
			// unlinked; tell each arena so it can drop a fully-dead chunk.
			c.leafArena.recordReaped(child.chunkIdx)
			c.internalArena.recordReaped(node.chunkIdx)

			if !sibling.isLeaf {
				queue = append(queue, bfsEntry{
					node:          sibling,
					parent:        e.parent,
					dirFromParent: e.dirFromParent,
				})
			}
			break
		}

		if !pruned {
			for dir := range 2 {
				child := node.child[dir].Load()
				if child != nil && !child.isLeaf {
					queue = append(queue, bfsEntry{
						node:          child,
						parent:        node,
						dirFromParent: dir,
					})
				}
			}
		}
	}

	return reaped
}

// Closed reports whether Close has been called. Mutations on a closed trie
// fail unconditionally, so CAS retry loops must check this to avoid spinning
// forever during shutdown.
func (c *Critbit[T]) Closed() bool {
	return c.closed.Load()
}

func (c *Critbit[T]) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.root.Store(nil)
	c.size.Store(0)
	c.leafArena.clear()
	c.internalArena.clear()
	return nil
}
