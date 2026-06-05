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

// Package cache provides a lock-free adaptive in-memory cache implementation.
// CloxCache uses protected-freq eviction: items with high frequency are protected,
// with LRU as a tiebreaker among same-frequency items.
package cache

import (
	"log/slog"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/puzpuzpuz/xsync/v4"
)

const (
	// maxFrequency is the maximum value for the frequency counter (0-15 range)
	maxFrequency = 15

	// initialFreq is the starting frequency for new keys
	initialFreq = 1

	// defaultProtectedFreqThreshold - items with freq > this are protected from eviction.
	// Analysis shows freq>=3 items are likely to return
	defaultProtectedFreqThreshold = 2

	// adaptiveCheckInterval - check graduation rate every N evictions
	adaptiveCheckInterval = 1000

	// Default graduation rate thresholds (will be learned per-shard)
	defaultRateLow  = 2500 // 0.25 * 10000
	defaultRateHigh = 5000 // 0.50 * 10000

	// Bounds for learned thresholds
	minRateLow  = 500  // 0.05 - minimum low threshold
	maxRateLow  = 4000 // 0.40 - maximum low threshold
	minRateHigh = 3000 // 0.30 - minimum high threshold
	maxRateHigh = 8000 // 0.80 - maximum high threshold

	// Learning rate for threshold adjustment (how much to nudge per feedback)
	thresholdLearningRate = 1000 // 0.10 per adjustment (aggressive)

	// Window size for measuring hit rate effect of k changes
	hitRateWindowSize = 2000 // smaller window = faster feedback

)

// Key is a type constraint for cache keys.
type Key = comparable

// Cache is the common interface for cache implementations.
type Cache[K Key, V any] interface {
	// Get retrieves a value by key
	Get(key K, offset uint64) (V, bool)

	// Put stores a value by key
	Put(key K, value V) (success bool, evictedKey K, evictedOffset, putOffset uint64)

	// PutBack updates an existing value that was modified in place.
	// Unlike Put, it skips eviction logic since the item already exists.
	// oldSize is the size of the value before modification (for accurate byte tracking).
	// Returns (success, offset) - offset is the L2 storage location.
	PutBack(key K, value V, oldSize int64) (success bool, offset uint64)

	// CompareAndSwap atomically swaps the value if it matches expected (pointer equality).
	// Returns (swapped, currentValue, exists, offset).
	CompareAndSwap(key K, expected, new V) (swapped bool, current V, exists bool, offset uint64)

	// Stats returns hit, miss, and eviction counts
	Stats() (hits, misses, evictions uint64)

	// GetAdaptiveStats returns per-shard adaptive statistics
	GetAdaptiveStats() []AdaptiveStats

	// EntryCount returns the number of entries in the cache
	EntryCount() int

	// Bytes returns the current memory usage in bytes (requires SetSizeFunc)
	Bytes() int64

	// MemoryLimit returns the configured memory limit in bytes (0 = unlimited)
	MemoryLimit() int64

	// Evict removes an entry from L1 cache only.
	// The entry is tombstoned in place (not unlinked) for efficiency.
	// Returns true if the key was found and evicted.
	Evict(key K) bool

	// Close releases resources
	Close()
}

// CloxCache is a lock-free adaptive in-memory cache.
// It stores generic keys of type K (string or []byte) and values of type V.
type CloxCache[K Key, V any] struct {
	shards    []shard[K, V]
	numShards int
	shardBits int

	// Configuration
	collectStats bool
	sweepPercent int                        // Percentage of shard to scan during eviction (1-100)
	sizeFunc     func(key K, value V) int64 // optional byte size calculator
	evictDecider func(key K, value V) bool  // optional eviction veto (true = may evict, false = pinned)
	evictNotify  func(key K, value V)       // optional post-eviction notification (ghost-conversion or full unlink)

	// Metrics (only updated when collectStats is true)
	hits        atomic.Uint64
	misses      atomic.Uint64
	evictions   atomic.Uint64
	bytes       atomic.Int64 // current bytes used (requires sizeFunc)
	memoryLimit atomic.Int64 // configured memory limit (0 = unlimited)

	// Lifecycle management
	stop                chan struct{}
	wg                  sync.WaitGroup
	closeOnce           sync.Once
	ghostSweeperRunning atomic.Bool
}

// shard contains a portion of the cache slots with minimal lock contention.
// Each field group is padded to 64 bytes (one cache line) to prevent false sharing.
type shard[K Key, V any] struct {
	// cache lines 0-1: cold fields (init-time, rarely written)
	slots         []atomic.Pointer[recordNode[K, V]] // 24 bytes
	mu            xsync.RBMutex                      // 80 bytes — reader-biased lock for insertions and sweeper unlink
	ghostCapacity int64                              // 8 bytes — max ghosts = slotsPerShard - capacity
	_             [16]byte                           // pad to 128 (2 cache lines)

	// cache line 1: entry book keeping (insert/evict)
	entryCount atomic.Int64 // live entries in this shard
	capacity   atomic.Int64 // max live entries for this shard
	ghostCount atomic.Int64 // ghost entries in this shard
	_          [40]byte     // pad to 64

	// cache line 2: clock hand (eviction only)
	hand atomic.Uint64 // per-shard CLOCK hand position
	_    [56]byte      // pad to 64

	// cache line 3: hit tracking + access timestamp (all hot-path writes)
	windowHits atomic.Uint64 // hits in current measurement window
	windowOps  atomic.Uint64 // total ops in current measurement window
	timestamp  atomic.Uint64 // per-shard monotonic counter for LRU ordering
	_          [40]byte      // pad to 64

	// cache line 4: adaptive k — threshold tracking (per-shard, no global contention)
	k                  atomic.Int32  // current protection threshold for this shard
	lastKDirection     atomic.Int32  // +1 if k increased, -1 if decreased, 0 if no change
	rateLow            atomic.Uint32 // adaptive low threshold * 10000
	rateHigh           atomic.Uint32 // adaptive high threshold * 10000
	evictedUnprotected atomic.Uint64 // evicted with freq <= k (unprotected)
	evictedProtected   atomic.Uint64 // evicted with freq > k (protected, fallback)
	reachedProtected   atomic.Uint64 // items whose freq crossed the shard's current k (graduated)
	lastAdaptCheck     atomic.Uint64 // eviction count at last adaptation check
	prevHitRate        atomic.Uint64 // previous window hit rate * 10000 (gradient descent on hit rate)
	_                  [8]byte       // pad to 64
}

// Compile-time assertion: shard size must be a multiple of 64 bytes (cache line).
// This is generic-struct-aware: we check a concrete instantiation.
var _ [0]struct{} = [unsafe.Sizeof(shard[string, any]{}) % 64]struct{}{}

// recordNode is a cache entry with collision chaining
// When freq > 0: live entry with that frequency
// When freq <= 0: ghost entry, |freq| is the remembered frequency
type recordNode[K Key, V any] struct {
	value      atomic.Value                     // value stored (stale for ghosts)
	next       atomic.Pointer[recordNode[K, V]] // chain traversal
	keyHash    uint64                           // fast hash comparison
	freq       atomic.Int32                     // access frequency (negative = ghost)
	lastAccess atomic.Uint64                    // per-shard monotonic timestamp for LRU tiebreaking
	key        K
}

// SizeFunc returns the size in bytes of a key-value pair.
// Used for byte tracking when provided in Config.
type SizeFunc[K Key, V any] func(key K, value V) int64

// Config holds CloxCache configuration
type Config struct {
	NumShards     int  // Must be power of 2
	SlotsPerShard int  // Must be power of 2
	Capacity      int  // Max entries (0 = use SlotsPerShard * NumShards as default)
	CollectStats  bool // Enable hit/miss/eviction counters
	// (recommend: 15 for temporal workloads and low latency)
	SweepPercent int // Percentage of shard to scan during eviction
}

// NewCloxCache creates a new cache with the given configuration
func NewCloxCache[K Key, V any](cfg Config) *CloxCache[K, V] {
	// Validate positive values
	if cfg.NumShards <= 0 {
		panic("NumShards must be positive")
	}
	if cfg.SlotsPerShard <= 0 {
		panic("SlotsPerShard must be positive")
	}

	// Validate power-of-2 requirements
	if cfg.NumShards&(cfg.NumShards-1) != 0 {
		panic("NumShards must be a power of 2")
	}
	if cfg.SlotsPerShard&(cfg.SlotsPerShard-1) != 0 {
		panic("SlotsPerShard must be a power of 2")
	}

	sweepPercent := cfg.SweepPercent
	if sweepPercent <= 0 {
		sweepPercent = 15
	} else if sweepPercent > 100 {
		sweepPercent = 100
	}

	c := &CloxCache[K, V]{
		numShards:    cfg.NumShards,
		shardBits:    bits.Len(uint(cfg.NumShards - 1)),
		shards:       make([]shard[K, V], cfg.NumShards),
		stop:         make(chan struct{}),
		collectStats: cfg.CollectStats,
		sweepPercent: sweepPercent,
	}

	totalCapacity := cfg.Capacity
	if totalCapacity <= 0 {
		totalCapacity = cfg.NumShards * cfg.SlotsPerShard
	}
	perShardCapacity := max(int64(totalCapacity/cfg.NumShards), 1)

	// Ghost capacity uses unused slot space, capped at 100% of live capacity
	ghostCapacity := min(max(int64(cfg.SlotsPerShard)-perShardCapacity, 0), perShardCapacity)

	for i := range c.shards {
		c.shards[i].slots = make([]atomic.Pointer[recordNode[K, V]], cfg.SlotsPerShard)
		c.shards[i].capacity.Store(perShardCapacity)
		c.shards[i].ghostCapacity = ghostCapacity
		c.shards[i].k.Store(defaultProtectedFreqThreshold)
		// Initialize self-tuning threshold learning
		c.shards[i].rateLow.Store(defaultRateLow)
		c.shards[i].rateHigh.Store(defaultRateHigh)
	}

	return c
}

// Close stops background goroutines and waits for them to exit.
// Safe to call multiple times.
func (c *CloxCache[K, V]) Close() {
	c.closeOnce.Do(func() {
		close(c.stop)
	})
	c.wg.Wait()
}

func keysEqual[K Key](a, b K) bool {
	return a == b
}

func copyKey[K Key](key K) K {
	return key
}

// Get retrieves a value from the cache (lock-free)
func (c *CloxCache[K, V]) Get(key K, offset uint64) (V, bool) {
	var zero V

	if bypass() {
		return zero, false
	}

	hash := hashKey(key)
	shardID := hash & uint64(c.numShards-1)
	slotID := (hash >> c.shardBits) & uint64(len(c.shards[0].slots)-1)

	shard := &c.shards[shardID]
	sl := &shard.slots[slotID]

	// Track ops for hit rate learning (always, even if collectStats is false)
	shard.windowOps.Add(1)

	node := sl.Load()
	for node != nil {
		if node.keyHash == hash && keysEqual(node.key, key) {
			f := node.freq.Load()
			// Skip ghosts (freq <= 0)
			if f <= 0 {
				node = node.next.Load()
				continue
			}

			// Bump frequency (saturating at 15)
			// If already at max, skip all updates - the item is clearly hot
			if f < maxFrequency {
				if node.freq.CompareAndSwap(f, f+1) {
					// Track when items cross into protected status (freq > k)
					// This happens when freq goes from k to k+1
					// Only count when at capacity (under eviction pressure)
					if f == shard.k.Load() && shard.entryCount.Load() >= shard.capacity.Load() {
						shard.reachedProtected.Add(1)
					}
					// Only update timestamp when we successfully bumped freq
					// This amortises the cost, and hot items skip updates entirely
					node.lastAccess.Store(shard.timestamp.Add(1))
				}
			}

			// Track hits for hit rate learning
			shard.windowHits.Add(1)

			if c.collectStats {
				c.hits.Add(1)
			}
			return node.value.Load().(V), true
		}
		node = node.next.Load()
	}

	if c.collectStats {
		c.misses.Add(1)
	}
	return zero, false
}

// Put inserts or updates a value in the cache
func (c *CloxCache[K, V]) Put(key K, value V) (success bool, evictedKey K, evictedOffset, putOffset uint64) {
	hash := hashKey(key)
	shardID := hash & uint64(c.numShards-1)
	slotID := (hash >> c.shardBits) & uint64(len(c.shards[0].slots)-1)

	shard := &c.shards[shardID]
	slot := &shard.slots[slotID]
	var k K

	// First, try to update the existing key (lock-free)
	node := slot.Load()
	for node != nil {
		if node.keyHash == hash {
			if keysEqual(node.key, key) {
				f := node.freq.Load()
				// Skip ghosts - we'll handle them under lock
				if f <= 0 {
					node = node.next.Load()
					continue
				}
				// Update existing - bump frequency and update access time
				// Track byte change if sizeFunc is set
				if c.sizeFunc != nil {
					oldValue := node.value.Load().(V)
					c.bytes.Add(c.sizeFunc(key, value) - c.sizeFunc(key, oldValue))
				}
				node.value.Store(value)
				node.lastAccess.Store(shard.timestamp.Add(1))
				for {
					f = node.freq.Load()
					if f >= maxFrequency || f < 1 {
						// already at max-freq or became a ghost while we were putting
						break
					}
					if node.freq.CompareAndSwap(f, f+1) {
						break
					}
				}
				return true, k, 0, 0
			}
		}
		node = node.next.Load()
	}

	// Allocate new node with a copied key to prevent caller mutations
	newNode := &recordNode[K, V]{
		keyHash: hash,
		key:     copyKey(key),
	}
	newNode.value.Store(value)
	newNode.freq.Store(initialFreq)
	newNode.lastAccess.Store(shard.timestamp.Add(1))

	// Try CAS onto head
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Re-check for an existing key under lock (including ghosts)
	node = slot.Load()
	for node != nil {
		if node.keyHash == hash {
			if keysEqual(node.key, key) {
				f := node.freq.Load()
				if f <= 0 {
					// Found a ghost - promote it! Use remembered freq + 1
					promotedFreq := max(min(-f+1, maxFrequency), initialFreq)
					// Track bytes for promoted ghost (ghost had stale value, add new size)
					if c.sizeFunc != nil {
						c.bytes.Add(c.sizeFunc(key, value))
					}
					node.value.Store(value)
					node.freq.Store(promotedFreq)
					node.lastAccess.Store(shard.timestamp.Add(1))
					shard.ghostCount.Add(-1)
					shard.entryCount.Add(1)
					return true, k, 0, 0
				}
				// Someone else inserted it - update value and access time
				// Track byte change if sizeFunc is set
				if c.sizeFunc != nil {
					oldValue := node.value.Load().(V)
					c.bytes.Add(c.sizeFunc(key, value) - c.sizeFunc(key, oldValue))
				}
				node.value.Store(value)
				node.lastAccess.Store(shard.timestamp.Add(1))
				return true, k, 0, 0
			}
		}
		node = node.next.Load()
	}

	// Evict from this shard if over capacity
	for shard.entryCount.Load() >= shard.capacity.Load() {
		evicted := c.evictFromShard(int(shardID), len(shard.slots))
		if evicted == 0 {
			// Couldn't evict anything, break to avoid infinite loop
			return false, k, 0, 0
		}
	}

	// Insert at head
	head := slot.Load()
	newNode.next.Store(head)
	slot.Store(newNode)
	shard.entryCount.Add(1)

	// Track bytes for new insertion
	if c.sizeFunc != nil {
		c.bytes.Add(c.sizeFunc(key, value))
	}

	return true, k, 0, 0
}

// PutBack updates an existing value that was modified in place.
// Unlike Put, it skips eviction logic since the item already has a slot.
// oldSize is the size of the value before modification (for accurate byte tracking).
// Returns (success, slot, offset) - for CloxCache, slot/offset are always 0 (no L2).
func (c *CloxCache[K, V]) PutBack(key K, value V, oldSize int64) (success bool, offset uint64) {
	hash := hashKey(key)
	shardID := hash & uint64(c.numShards-1)
	slotID := (hash >> c.shardBits) & uint64(len(c.shards[0].slots)-1)

	shard := &c.shards[shardID]
	sl := &shard.slots[slotID]

	// Find the existing node (lock-free traversal)
	node := sl.Load()
	for node != nil {
		if node.keyHash == hash && keysEqual(node.key, key) {
			f := node.freq.Load()
			// Skip ghosts
			if f <= 0 {
				node = node.next.Load()
				continue
			}

			// Update byte tracking with correct delta
			if c.sizeFunc != nil {
				newSize := c.sizeFunc(key, value)
				c.bytes.Add(newSize - oldSize)
			}

			// The value was modified in place, so node.value already points to it.
			// We still store it to ensure memory visibility across goroutines.
			node.value.Store(value)

			// Update last access time
			node.lastAccess.Store(shard.timestamp.Add(1))

			// Bump frequency (saturating at maxFrequency)
			for {
				f = node.freq.Load()
				if f >= maxFrequency || f <= 0 {
					break
				}
				if node.freq.CompareAndSwap(f, f+1) {
					// Track graduation if crossing threshold
					if f == shard.k.Load() && shard.entryCount.Load() >= shard.capacity.Load() {
						shard.reachedProtected.Add(1)
					}
					break
				}
			}

			return true, 0
		}
		node = node.next.Load()
	}

	return false, 0
}

// CompareAndSwap atomically swaps the value if it matches expected.
// Returns (swapped, currentValue, exists, offset). If swapped is false and exists is true,
// currentValue contains the actual value so the caller can retry.
func (c *CloxCache[K, V]) CompareAndSwap(key K, expected, new V) (swapped bool, current V, exists bool, offset uint64) {
	var zero V

	hash := hashKey(key)
	shardID := hash & uint64(c.numShards-1)
	slotID := (hash >> c.shardBits) & uint64(len(c.shards[0].slots)-1)

	shard := &c.shards[shardID]
	sl := &shard.slots[slotID]

	node := sl.Load()
	for node != nil {
		if node.keyHash == hash && keysEqual(node.key, key) {
			f := node.freq.Load()
			if f > 0 {
				// Use atomic CAS - truly atomic, no interleaving
				if node.value.CompareAndSwap(expected, new) {
					node.lastAccess.Store(shard.timestamp.Add(1))
					return true, new, true, 0
				}
				// CAS failed - return current value for retry
				return false, node.value.Load().(V), true, 0
			}
		}
		node = node.next.Load()
	}

	return false, zero, false, 0
}

// Evict removes an entry from L1 cache by tombstoning it in place.
// The entry is made unfindable (null-prefixed key), set to low priority (freq=1),
// and its value is cleared. This avoids expensive chain unlinking while
// maintaining eviction pressure so the slot gets reclaimed.
// Returns true if the key was found and evicted.
func (c *CloxCache[K, V]) Evict(key K) bool {
	hash := hashKey(key)
	shardID := hash & uint64(c.numShards-1)
	slotID := (hash >> c.shardBits) & uint64(len(c.shards[0].slots)-1)

	shard := &c.shards[shardID]
	slot := &shard.slots[slotID]

	node := slot.Load()
	for node != nil {
		if node.keyHash == hash && keysEqual(node.key, key) {
			f := node.freq.Load()
			if f <= 0 {
				node = node.next.Load()
				continue
			}

			// Decrement bytes before tombstoning
			if c.sizeFunc != nil {
				oldValue := node.value.Load().(V)
				c.bytes.Add(-c.sizeFunc(key, oldValue))
			}

			// Convert to ghost: negate freq to preserve remembered frequency
			for {
				f := node.freq.Load()
				if f <= 0 {
					break // already a ghost
				}
				if node.freq.CompareAndSwap(f, -f) {
					shard.entryCount.Add(-1)
					shard.ghostCount.Add(1)
					break
				}
			}
			return true
		}
		node = node.next.Load()
	}
	return false
}

// evictFromShard uses protected-freq eviction with LRU tiebreaking.
// Called during Put when shard is over capacity. Caller must hold shard lock.
// Returns the number of entries evicted (0 or 1).
//
// Algorithm:
// - Scans a portion of the shard (sweepPercent)
// - Finds LRU item among low-frequency items (freq <= k)
// - Falls back to any LRU item if no low-freq items are found
// - Low-freq items become ghosts (freq negated) instead of being removed
// - Adapts k based on graduation rate
func (c *CloxCache[K, V]) evictFromShard(shardID, slotsPerShard int) int {
	shard := &c.shards[shardID]
	k := shard.k.Load()

	// Calculate scan range
	maxScan := max(slotsPerShard*c.sweepPercent/100, 1)

	// Advance CLOCK hand
	advance := (maxScan + 1) / 2
	startSlot := int(shard.hand.Add(uint64(advance)) % uint64(slotsPerShard))

	// Track the best victims: low-freq preferred, any as fallback
	// Also track oldest ghost for eviction when ghost capacity is full
	var lowFreqVictim, lowFreqPrev *recordNode[K, V]
	var lowFreqSlot *atomic.Pointer[recordNode[K, V]]
	lowFreqAccess := uint64(^uint64(0))

	var fallbackVictim, fallbackPrev *recordNode[K, V]
	var fallbackSlot *atomic.Pointer[recordNode[K, V]]
	fallbackAccess := uint64(^uint64(0))

	var oldestGhost, oldestGhostPrev *recordNode[K, V]
	var oldestGhostSlot *atomic.Pointer[recordNode[K, V]]
	oldestGhostAccess := uint64(^uint64(0))

	// Walks one slot's chain and updates the captured victim/ghost trackers.
	// Returns true if a live (non-ghost, non-pinned) eviction candidate
	// has been recorded so far in this shard scan.
	scanSlot := func(slot *atomic.Pointer[recordNode[K, V]]) bool {
		node := slot.Load()
		var prev *recordNode[K, V]

		for node != nil {
			access := node.lastAccess.Load()
			freq := node.freq.Load()

			// Skip ghosts for live eviction, but track oldest ghost
			if freq <= 0 {
				if access < oldestGhostAccess {
					oldestGhost = node
					oldestGhostPrev = prev
					oldestGhostSlot = slot
					oldestGhostAccess = access
				}
				prev = node
				node = node.next.Load()
				continue
			}

			// Pin check: skip live entries the decider rejects so they
			// never become an eviction candidate.
			if c.evictDecider != nil && !c.evictDecider(node.key, node.value.Load().(V)) {
				prev = node
				node = node.next.Load()
				continue
			}

			// Track LRU among low-freq items (freq <= k, unprotected)
			if freq <= k && access < lowFreqAccess {
				lowFreqVictim = node
				lowFreqPrev = prev
				lowFreqSlot = slot
				lowFreqAccess = access
			}

			// Track LRU overall (fallback)
			if access < fallbackAccess {
				fallbackVictim = node
				fallbackPrev = prev
				fallbackSlot = slot
				fallbackAccess = access
			}

			prev = node
			node = node.next.Load()
		}

		return lowFreqVictim != nil || fallbackVictim != nil
	}

	for scanned := range maxScan {
		slotID := (startSlot + scanned) % slotsPerShard
		scanSlot(&shard.slots[slotID])
	}

	// If pin pressure happened to concentrate in the partial-sweep
	// window, the trackers can be empty even when evictable entries
	// exist elsewhere in the shard. Returning 0 here would make Put
	// fail and leave the caller indexing an effect whose bytes were
	// never cached. Extend the scan across the remaining slots and
	// stop as soon as a candidate appears — keeps the common-case
	// cost at sweepPercent and only pays full-shard when forced.
	if lowFreqVictim == nil && fallbackVictim == nil && maxScan < slotsPerShard {
		for scanned := maxScan; scanned < slotsPerShard; scanned++ {
			slotID := (startSlot + scanned) % slotsPerShard
			if scanSlot(&shard.slots[slotID]) {
				break
			}
		}
	}

	// Choose a victim: prefer low-freq, protect high-freq items
	var victim, victimPrev *recordNode[K, V]
	var victimSlot *atomic.Pointer[recordNode[K, V]]
	isUnprotected := false

	if lowFreqVictim != nil {
		shard.evictedUnprotected.Add(1) // evicting low-freq (unprotected) item
		victim = lowFreqVictim
		victimPrev = lowFreqPrev
		victimSlot = lowFreqSlot
		isUnprotected = true
	} else if fallbackVictim != nil {
		shard.evictedProtected.Add(1) // forced to evict high-freq (protected) item
		victim = fallbackVictim
		victimPrev = fallbackPrev
		victimSlot = fallbackSlot
	}

	if victim == nil {
		return 0
	}

	// Check if we can convert to ghost (only for unprotected items with ghost capacity)
	// Disable ghosts when sizeFunc is set - ghosts hold values but aren't tracked in bytes,
	// and the partial-scan eviction can't reliably evict ghosts from un-scanned slots
	canGhost := c.sizeFunc == nil && isUnprotected && shard.ghostCapacity > 0 && shard.ghostCount.Load() < shard.ghostCapacity

	// If ghost capacity is full, evict oldest ghost first to make room
	if isUnprotected && shard.ghostCapacity > 0 && !canGhost && oldestGhost != nil {
		// Remove oldest ghost
		next := oldestGhost.next.Load()
		if oldestGhostPrev == nil {
			oldestGhostSlot.Store(next)
		} else {
			oldestGhostPrev.next.Store(next)
		}
		shard.ghostCount.Add(-1)
		canGhost = true
	}

	// Subtract bytes for evicted entry (before eviction, value still valid)
	if c.sizeFunc != nil {
		victimValue := victim.value.Load().(V)
		c.bytes.Add(-c.sizeFunc(victim.key, victimValue))
	}

	// Capture the value before mutation so evictNotify sees the
	// effect as it was when the cache held it. After ghost conversion
	// the value is still in the node but Get treats it as absent;
	// after full unlink the node is gone.
	var notifyValue V
	if c.evictNotify != nil {
		notifyValue = victim.value.Load().(V)
	}

	if canGhost {
		// Convert to ghost: atomically negate freq to claim victim and preserve frequency.
		// CAS ensures we capture the correct freq even if concurrent Gets bump it.
		// We explicitly don't clear the value: this prevents a race with concurrent Gets.
		for {
			f := victim.freq.Load()
			if victim.freq.CompareAndSwap(f, -f) {
				shard.entryCount.Add(-1)
				shard.ghostCount.Add(1)
				break
			}
			// CAS failed - freq was bumped by concurrent access, retry with fresh value
		}
	} else {
		// Fully evict: unlink from chain
		c.evictions.Add(1) // Always track evictions (infrequent, no hot-path impact)
		shard.entryCount.Add(-1)

		next := victim.next.Load()
		if victimPrev == nil {
			victimSlot.Store(next)
		} else {
			victimPrev.next.Store(next)
		}
	}

	if c.evictNotify != nil {
		c.evictNotify(victim.key, notifyValue)
	}

	// Periodically adapt k based on graduation rate
	totalEvictions := shard.evictedUnprotected.Load() + shard.evictedProtected.Load()
	lastCheck := shard.lastAdaptCheck.Load()
	if totalEvictions-lastCheck >= adaptiveCheckInterval {
		if shard.lastAdaptCheck.CompareAndSwap(lastCheck, totalEvictions) {
			c.adaptThreshold(shard)
		}
	}

	return 1
}

// adaptThreshold adjusts the per-shard k based on graduation rate.
// Also implements self-tuning: adjusts the rate thresholds based on whether
// k changes actually improved hit rate (gradient descent on hit rate).
// Called periodically during eviction.
func (c *CloxCache[K, V]) adaptThreshold(shard *shard[K, V]) {
	graduated := shard.reachedProtected.Load()
	totalEvictions := shard.evictedUnprotected.Load() + shard.evictedProtected.Load()

	if totalEvictions == 0 {
		// Still decay graduated to prevent accumulation when evictions are slow
		if graduated > 100 {
			shard.reachedProtected.Store(graduated / 2)
		}
		return
	}

	// First, check if we have enough data to evaluate the effect of the last k change
	windowOps := shard.windowOps.Load()
	if windowOps >= hitRateWindowSize {
		windowHits := shard.windowHits.Load()
		currentHitRate := uint64(float64(windowHits) / float64(windowOps) * 10000)
		prevHitRate := shard.prevHitRate.Load()
		lastDirection := shard.lastKDirection.Load()

		// Only learn if we have a previous measurement and made a change
		if prevHitRate > 0 && lastDirection != 0 {
			hitRateImproved := currentHitRate > prevHitRate
			rateLow := shard.rateLow.Load()
			rateHigh := shard.rateHigh.Load()

			if lastDirection > 0 {
				// We increased k last time
				if hitRateImproved {
					// Increasing k helped - make it easier to increase (lower highThreshold)
					if rateHigh > minRateHigh+thresholdLearningRate {
						shard.rateHigh.Store(rateHigh - thresholdLearningRate)
					}
				} else {
					// Increasing k hurt - make it harder to increase (raise highThreshold)
					if rateHigh < maxRateHigh-thresholdLearningRate {
						shard.rateHigh.Store(rateHigh + thresholdLearningRate)
					}
				}
			} else {
				// We decreased k last time
				if hitRateImproved {
					// Decreasing k helped - make it easier to decrease (raise lowThreshold)
					if rateLow < maxRateLow-thresholdLearningRate {
						shard.rateLow.Store(rateLow + thresholdLearningRate)
					}
				} else {
					// Decreasing k hurt - make it harder to decrease (lower lowThreshold)
					if rateLow > minRateLow+thresholdLearningRate {
						shard.rateLow.Store(rateLow - thresholdLearningRate)
					}
				}
			}
		}

		// Save current hit rate and reset window
		shard.prevHitRate.Store(currentHitRate)
		shard.windowHits.Store(0)
		shard.windowOps.Store(0)
	}

	// Graduation rate = items that crossed threshold k / total evictions
	// This tells us: "what fraction of items survived long enough to become protected?"
	rate := float64(graduated) / float64(totalEvictions)
	currentK := shard.k.Load()

	// Use the learned thresholds (convert from uint32 * 10000 back to float)
	rateLow := float64(shard.rateLow.Load()) / 10000.0
	rateHigh := float64(shard.rateHigh.Load()) / 10000.0

	var kDirection int32 = 0
	if rate < rateLow && currentK > 1 {
		// Very few items graduating - protection isn't helping
		// Lower k, but never below 1 (need freq>=2 protection to allow graduation)
		shard.k.Store(currentK - 1)
		kDirection = -1
	} else if rate > rateHigh && currentK < maxFrequency-1 {
		// Many items graduating - protection is working, can raise k
		// Cap at maxFrequency-1 so there's always room to reach protected status
		shard.k.Store(currentK + 1)
		kDirection = 1
	}
	shard.lastKDirection.Store(kDirection)

	// Decay counters to weight recent behavior (but keep minimum for signal)
	if graduated > 100 {
		shard.reachedProtected.Store(graduated / 2)
	}
	if totalEvictions > 100 {
		shard.evictedUnprotected.Store(shard.evictedUnprotected.Load() / 2)
		shard.evictedProtected.Store(shard.evictedProtected.Load() / 2)
	}
}

// Stats return cache statistics
func (c *CloxCache[K, V]) Stats() (hits, misses, evictions uint64) {
	return c.hits.Load(), c.misses.Load(), c.evictions.Load()
}

// SetSizeFunc sets the function used to calculate byte size of entries.
// When set, the cache tracks total bytes and updates on Put/eviction.
// Must be called before any Put operations for accurate tracking.
func (c *CloxCache[K, V]) SetSizeFunc(fn func(key K, value V) int64) {
	c.sizeFunc = fn
}

// SetEvictDecider installs an optional veto for eviction victim selection.
// The function is called under the shard mutex during evictFromShard for
// each candidate. Return true to allow eviction, false to pin (the scan
// will try a different victim within the same maxScan budget). If every
// candidate in a sweep is pinned, the eviction returns no victim and the
// caller's "couldn't evict" path runs. Keep the function cheap — no I/O,
// no locks that could re-enter the cache.
func (c *CloxCache[K, V]) SetEvictDecider(fn func(key K, value V) bool) {
	c.evictDecider = fn
}

// SetEvictNotify installs an optional post-eviction callback, invoked
// once per evicted entry after the victim is unlinked from its chain
// or converted to a ghost. The callback is called under the shard
// mutex; it must return promptly and must not re-enter the cache.
// Callers that need to do real work (broadcast, take other locks)
// should dispatch the work on a goroutine.
func (c *CloxCache[K, V]) SetEvictNotify(fn func(key K, value V)) {
	c.evictNotify = fn
}

// Bytes returns the current estimated bytes used by cached entries.
// Returns 0 if no SizeFunc was set.
func (c *CloxCache[K, V]) Bytes() int64 {
	return c.bytes.Load()
}

// MemoryLimit returns the configured memory limit in bytes.
// Returns 0 if no limit was set via EnforceMemoryCapacity.
func (c *CloxCache[K, V]) MemoryLimit() int64 {
	return c.memoryLimit.Load()
}

// SetCapacity adjusts the total cache capacity.
// The new capacity is distributed evenly across shards.
// Reducing capacity may trigger evictions on subsequent operations.
func (c *CloxCache[K, V]) SetCapacity(totalCapacity int) {
	if totalCapacity < c.numShards {
		totalCapacity = c.numShards // At least 1 per shard
	}
	// Round up to ensure we don't lose capacity to integer division truncation
	perShardCapacity := max((int64(totalCapacity)+int64(c.numShards)-1)/int64(c.numShards), 1)
	for i := range c.shards {
		c.shards[i].capacity.Store(perShardCapacity)
	}
}

// Capacity returns the current total capacity (sum of all shard capacities).
func (c *CloxCache[K, V]) Capacity() int {
	var total int64
	for i := range c.shards {
		total += c.shards[i].capacity.Load()
	}
	return int(total)
}

// EntryCount returns the current number of live entries in the cache.
func (c *CloxCache[K, V]) EntryCount() int {
	var total int64
	for i := range c.shards {
		total += c.shards[i].entryCount.Load()
	}
	return int(total)
}

// AdaptiveStats returns per-shard adaptive threshold statistics
type AdaptiveStats struct {
	ShardID            int
	K                  int32   // current protection threshold for this shard
	GraduationRate     float64 // fraction of items whose freq crossed the shard's k
	EvictedUnprotected uint64  // items evicted with freq <= k
	EvictedProtected   uint64  // items evicted with freq > k (fallback)
	ReachedProtected   uint64  // items whose freq crossed the shard's current k
	// Learned thresholds (self-tuning)
	LearnedRateLow  float64 // learned low threshold (rate below which k decreases)
	LearnedRateHigh float64 // learned high threshold (rate above which k increases)
	WindowHitRate   float64 // current window hit rate
}

// GetAdaptiveStats returns adaptive threshold stats for all shards
func (c *CloxCache[K, V]) GetAdaptiveStats() []AdaptiveStats {
	stats := make([]AdaptiveStats, c.numShards)
	for i := range c.shards {
		shard := &c.shards[i]
		graduated := shard.reachedProtected.Load()
		evictedU := shard.evictedUnprotected.Load()
		evictedP := shard.evictedProtected.Load()
		total := evictedU + evictedP

		var rate float64
		if total > 0 {
			rate = float64(graduated) / float64(total)
		}

		// Calculate current window hit rate
		windowOps := shard.windowOps.Load()
		var windowHitRate float64
		if windowOps > 0 {
			windowHitRate = float64(shard.windowHits.Load()) / float64(windowOps)
		}

		stats[i] = AdaptiveStats{
			ShardID:            i,
			K:                  shard.k.Load(),
			GraduationRate:     rate,
			EvictedUnprotected: evictedU,
			EvictedProtected:   evictedP,
			ReachedProtected:   graduated,
			LearnedRateLow:     float64(shard.rateLow.Load()) / 10000.0,
			LearnedRateHigh:    float64(shard.rateHigh.Load()) / 10000.0,
			WindowHitRate:      windowHitRate,
		}
	}
	return stats
}

// AverageK returns the average protection threshold across all shards
func (c *CloxCache[K, V]) AverageK() float64 {
	var sum int32
	for i := range c.shards {
		sum += c.shards[i].k.Load()
	}
	return float64(sum) / float64(c.numShards)
}

// AverageLearnedThresholds returns the average learned rate thresholds across all shards
func (c *CloxCache[K, V]) AverageLearnedThresholds() (rateLow, rateHigh float64) {
	var sumLow, sumHigh uint32
	for i := range c.shards {
		sumLow += c.shards[i].rateLow.Load()
		sumHigh += c.shards[i].rateHigh.Load()
	}
	rateLow = float64(sumLow) / float64(c.numShards) / 10000.0
	rateHigh = float64(sumHigh) / float64(c.numShards) / 10000.0
	return
}

// EnforceMemoryCapacity starts a background goroutine that periodically adjusts
// cache capacity to stay within the specified memory limit. The avgItemSize is
// the estimated average memory usage per item (including overhead like keys and
// cache node metadata). The goroutine runs every second and stops when Close()
// is called.
//
// This approach avoids cache-line ping-pong that would occur if memory were
// tracked on every Put operation.
func (c *CloxCache[K, V]) EnforceMemoryCapacity(memoryLimit, avgItemSize int64) {
	if memoryLimit <= 0 || avgItemSize <= 0 {
		return
	}

	c.memoryLimit.Store(memoryLimit)
	c.wg.Add(1)
	go c.memoryEnforcerLoop(memoryLimit, avgItemSize)
}

func (c *CloxCache[K, V]) memoryEnforcerLoop(memoryLimit, avgItemSize int64) {
	defer c.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			c.adjustCapacityForMemory(memoryLimit, avgItemSize)
		}
	}
}

func (c *CloxCache[K, V]) adjustCapacityForMemory(memoryLimit, avgItemSize int64) {
	entryCount := int64(c.EntryCount())
	if entryCount == 0 {
		return
	}

	// Use actual bytes if available and non-zero, otherwise estimate
	var currentMemory int64
	if c.sizeFunc != nil {
		currentMemory = c.bytes.Load()
	}
	// Fall back to estimate if bytes not tracked or zero
	if currentMemory <= 0 {
		currentMemory = entryCount * avgItemSize
	}

	// Calculate actual average item size for capacity planning
	actualAvgSize := avgItemSize
	if currentMemory > 0 && entryCount > 0 {
		actualAvgSize = max(currentMemory/entryCount,
			// minimum reasonable size
			64)
	}

	// Calculate target capacity based on memory limit
	targetCapacity := max(memoryLimit/actualAvgSize, int64(c.numShards))

	currentCapacity := int64(c.Capacity())

	// Get runtime memory stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Debug output
	slog.Debug("memory stats",
		"entries", entryCount,
		"tracked_mb", currentMemory/1024/1024,
		"heap_alloc_mb", m.HeapAlloc/1024/1024,
		"heap_sys_mb", m.HeapSys/1024/1024,
		"limit_mb", memoryLimit/1024/1024,
		"avg_size", actualAvgSize,
		"capacity", currentCapacity,
		"target_capacity", targetCapacity)

	// Only adjust if significantly different (>10% change needed)
	if currentMemory > memoryLimit {
		// Over limit - reduce capacity to force evictions
		if targetCapacity < currentCapacity*9/10 {
			slog.Debug("reducing capacity", "from", currentCapacity, "to", targetCapacity)
			c.SetCapacity(int(targetCapacity))
		}
	} else if currentMemory < memoryLimit*8/10 && targetCapacity > currentCapacity {
		// Under 80% and can grow - increase capacity gradually (max 10% per second)
		maxIncrease := currentCapacity * 11 / 10
		if targetCapacity > maxIncrease {
			targetCapacity = maxIncrease
		}
		slog.Debug("increasing capacity", "from", currentCapacity, "to", targetCapacity)
		c.SetCapacity(int(targetCapacity))
	}
}
