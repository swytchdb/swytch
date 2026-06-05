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

import "math"

// Eviction policy constants, lifted from CloxCache. The trie runs the policy
// as a single bounded domain (see Critbit.EvictBounded) rather than over
// fixed-size shards, so a sweep's cost is bounded by the absolute scan budget
// instead of a fraction of a shard.
const (
	// defaultProtectedFreqThreshold: leaves with freq above this are
	// protected from eviction; below-or-equal are eviction candidates.
	defaultProtectedFreqThreshold = 2

	// adaptiveCheckInterval: re-evaluate k every N evictions.
	adaptiveCheckInterval = 1000

	// Default graduation-rate thresholds (× 10000), learned per domain.
	defaultRateLow  = 2500
	defaultRateHigh = 5000

	// Bounds for the learned thresholds.
	minRateLow  = 500
	maxRateLow  = 4000
	minRateHigh = 3000
	maxRateHigh = 8000

	// thresholdLearningRate: how far to nudge a rate threshold per feedback.
	thresholdLearningRate = 1000

	// hitRateWindowSize: accesses per adaptation window.
	hitRateWindowSize = 2000

	// defaultEvictScan: leaves scanned per bounded sweep when the caller
	// passes 0. Bounds sweep latency independent of keyspace size.
	defaultEvictScan = 64
)

// SetEvictHooks installs the consumer callbacks the eviction sweep uses.
// decider vetoes a key as an eviction victim (return false to pin it, e.g.
// system keys). notify fires after a victim leaf is soft-deleted, handing the
// consumer the dropped key's tips and leaf payload so it can release refs and
// tear down (e.g. unsubscribe). Both run without any trie lock the consumer
// could re-enter; keep them prompt.
func (c *Critbit[T]) SetEvictHooks(decider func(key string) bool, notify func(key string, tips []EffectRef, data *T)) {
	c.evictDecider = decider
	c.evictNotify = notify
	// Hooks are only ever consumed by EvictBounded, so their presence marks
	// eviction as active: bumpAccess then maintains the freq/LRU/adaptive-k
	// bookkeeping. Without them, reads skip that bookkeeping entirely.
	c.evictActive.Store(true)
}

// SetRefDelta installs the per-tip refcount hook, fired on every leaf tip-set
// transition (insert, remove, delete, eviction). See the refDelta field.
func (c *Critbit[T]) SetRefDelta(fn func(added, removed []EffectRef, droppedData *T)) {
	c.refDelta = fn
}

// EvictBounded runs one bounded eviction sweep. It samples at most maxScan
// leaves by random root-to-leaf descent, selects the least-recently-used
// unprotected (freq <= k) leaf among them — falling back to the overall LRU
// sample when every candidate is protected — soft-deletes it, and notifies the
// consumer. Returns true if a leaf was evicted.
//
// Latency is bounded by maxScan regardless of keyspace size, and victim quality
// doesn't depend on key-order locality: the samples are spread across the
// keyspace the way CloxCache's hash-distributed slot scan was, so the LRU of
// the sample tracks the global LRU rather than the LRU of a lexicographic
// neighborhood.
// EvictK returns the current protected-frequency eviction threshold (the
// adaptive SLRU promotion bar maybeAdaptK tunes). It is the trie's analogue
// of CloxCache's per-shard k; telemetry reports it as average_k. Returns the
// default threshold until the eviction hooks are installed and adaptation runs.
func (c *Critbit[T]) EvictK() float64 {
	return float64(c.evictK.Load())
}

func (c *Critbit[T]) EvictBounded(maxScan int) bool {
	if c.closed.Load() {
		return false
	}
	if maxScan <= 0 {
		maxScan = defaultEvictScan
	}

	c.evictMu.Lock()
	defer c.evictMu.Unlock()

	k := c.evictK.Load()

	var (
		lowVictim *critNode[T]
		lowAccess = uint64(math.MaxUint64)
		fbVictim  *critNode[T]
		fbAccess  = uint64(math.MaxUint64)
	)

	c.sampleLeaves(maxScan, func(leaf *critNode[T]) {
		if leaf.isDeleted() { // already evicted / ghost
			return
		}
		if c.evictDecider != nil && !c.evictDecider(leaf.key) {
			return // pinned
		}
		access := leaf.lastAccess.Load()
		f := leaf.freq.Load()
		if f <= k && access < lowAccess {
			lowVictim, lowAccess = leaf, access
		}
		if access < fbAccess {
			fbVictim, fbAccess = leaf, access
		}
	})

	victim := lowVictim
	protected := false
	if victim == nil {
		victim, protected = fbVictim, true
	}
	if victim == nil {
		return false
	}

	// Soft-delete under the reap read-lock (mirrors DeleteAndSnapshot) so the
	// reaper can't unlink the leaf mid-mutation.
	rt := c.reapMu.RLock()
	if victim.isDeleted() || !victim.deleted.CompareAndSwap(false, true) {
		c.reapMu.RUnlock(rt)
		return false
	}
	tips := victim.tips.Load()
	data := victim.data.Load()
	key := victim.key
	victim.tips.Store(nil)
	victim.data.Store(nil)
	c.size.Add(-1)

	// Ghost an unprotected victim — retain its frequency (negated) so a
	// returning key warms back instead of restarting cold — when under the
	// ghost budget; otherwise fully delete it. A ghost is a deleted leaf the
	// reaper skips (freq < 0); a full delete (freq >= 0) gets unlinked.
	ghosted := false
	if !protected && c.ghostCount.Load() < c.ghostCap() {
		for {
			f := victim.freq.Load()
			nf := -f
			if nf >= 0 {
				nf = -1
			}
			if victim.freq.CompareAndSwap(f, nf) {
				break
			}
		}
		c.ghostCount.Add(1)
		ghosted = true
	} else {
		c.deletedCount.Add(1)
	}
	c.reapMu.RUnlock(rt)

	if protected {
		c.evictedProt.Add(1)
	} else {
		c.evictedUnprot.Add(1)
	}

	// Release the evicted leaf's tip and payload references; evictNotify is
	// then only responsible for the teardown side effects (unsubscribe).
	c.fireTipDelta(tips, nil, data)

	if c.evictNotify != nil {
		var tipRefs []EffectRef
		if tips != nil {
			tipRefs = tips.Tips()
		}
		c.evictNotify(key, tipRefs, data)
	}

	c.maybeAdaptK()
	if !ghosted {
		c.maybeReap()
	}
	return true
}

// ghostCap bounds how many ghosts (freq-only memory of evicted keys) the trie
// retains, roughly the live key count — ghost leaves carry no tips/data, just
// the node, so this keeps freq memory proportional to the working set.
func (c *Critbit[T]) ghostCap() int64 {
	if n := c.size.Load(); n > 16 {
		return n
	}
	return 16
}

// promoteIfGhost reactivates a ghost leaf (freq < 0) into a live one with its
// remembered frequency + 1, and accounts the reactivation as a window miss
// (the key returned after we evicted it). No-op if the leaf is not a ghost.
func (c *Critbit[T]) promoteIfGhost(leaf *critNode[T]) {
	for {
		f := leaf.freq.Load()
		if f >= 0 {
			return
		}
		nf := min(-f+1, maxLeafFreq)
		if leaf.freq.CompareAndSwap(f, nf) {
			c.ghostCount.Add(-1)
			c.windowOps.Add(1)
			return
		}
	}
}

// maybeAdaptK re-tunes the protected-freq threshold k every
// adaptiveCheckInterval evictions, ported from CloxCache's adaptThreshold.
// It runs under evictMu (the only caller is EvictBounded).
//
// Two feedback loops: (1) the graduation rate — the fraction of evicted
// candidates that had crossed k into protected status — drives k up when
// protection is paying off and down when it isn't; (2) a gradient on the
// window hit rate self-tunes the rate thresholds, so the policy adapts to the
// workload rather than using fixed cutoffs.
func (c *Critbit[T]) maybeAdaptK() {
	total := c.evictedUnprot.Load() + c.evictedProt.Load()
	if total-c.lastAdaptCheck.Load() < adaptiveCheckInterval {
		return
	}
	c.lastAdaptCheck.Store(total)

	graduated := c.reachedProt.Load()
	if total == 0 {
		if graduated > 100 {
			c.reachedProt.Store(graduated / 2)
		}
		return
	}

	// Self-tune the rate thresholds from the hit-rate gradient once the
	// window has enough samples.
	if ops := c.windowOps.Load(); ops >= hitRateWindowSize {
		hitRate := uint64(float64(c.windowHits.Load()) / float64(ops) * 10000)
		prev := c.prevHitRate.Load()
		dir := c.lastKDir.Load()
		if prev > 0 && dir != 0 {
			improved := hitRate > prev
			rateLow := c.rateLow.Load()
			rateHigh := c.rateHigh.Load()
			if dir > 0 { // last change raised k
				if improved {
					if rateHigh > minRateHigh+thresholdLearningRate {
						c.rateHigh.Store(rateHigh - thresholdLearningRate)
					}
				} else if rateHigh < maxRateHigh-thresholdLearningRate {
					c.rateHigh.Store(rateHigh + thresholdLearningRate)
				}
			} else { // last change lowered k
				if improved {
					if rateLow < maxRateLow-thresholdLearningRate {
						c.rateLow.Store(rateLow + thresholdLearningRate)
					}
				} else if rateLow > minRateLow+thresholdLearningRate {
					c.rateLow.Store(rateLow - thresholdLearningRate)
				}
			}
		}
		c.prevHitRate.Store(hitRate)
		c.windowHits.Store(0)
		c.windowOps.Store(0)
	}

	rate := float64(graduated) / float64(total)
	rateLow := float64(c.rateLow.Load()) / 10000.0
	rateHigh := float64(c.rateHigh.Load()) / 10000.0
	k := c.evictK.Load()
	var dir int32
	if rate < rateLow && k > 1 {
		c.evictK.Store(k - 1)
		dir = -1
	} else if rate > rateHigh && k < maxLeafFreq-1 {
		c.evictK.Store(k + 1)
		dir = 1
	}
	c.lastKDir.Store(dir)

	// Decay counters so recent behaviour dominates.
	if graduated > 100 {
		c.reachedProt.Store(graduated / 2)
	}
	if total > 100 {
		c.evictedUnprot.Store(c.evictedUnprot.Load() / 2)
		c.evictedProt.Store(c.evictedProt.Load() / 2)
		c.lastAdaptCheck.Store(0)
	}
}

// sampleLeaves calls fn on up to n live leaves drawn by independent random
// root-to-leaf descents (a random child at each internal node). It mirrors
// CloxCache's hash-distributed slot sampling: each descent lands somewhere
// uncorrelated with key order, so the caller's LRU-of-sample approximates the
// global LRU instead of the LRU of a contiguous key range. A leaf's draw
// probability is 2^-depth, so shallow leaves are mildly oversampled — the same
// trade-off as Redis's random-sample LRU.
//
// Ghost/deleted leaves are skipped and re-drawn so n counts live candidates;
// total descents are capped at 4n so a ghost-heavy trie can't spin. Lock-free
// read traversal. Runs under evictMu, the only mutator of evictRand.
func (c *Critbit[T]) sampleLeaves(n int, fn func(*critNode[T])) {
	root := c.root.Load()
	if root == nil {
		return
	}

	seen := 0
	for attempts := 0; seen < n && attempts < n*4; attempts++ {
		cur := root
		for cur != nil && !cur.isLeaf {
			dir := int(c.nextRand() & 1)
			next := cur.child[dir].Load()
			if next == nil { // mid-reap transient; take the sibling
				next = cur.child[1-dir].Load()
			}
			cur = next
		}
		if cur == nil || !cur.isLeaf {
			continue
		}
		if cur.isDeleted() { // ghost/deleted: doesn't count toward the budget
			continue
		}
		fn(cur)
		seen++
	}
}

// nextRand advances the sweep's xorshift64 PRNG. Seeded lazily from the trie
// clock so an un-accessed trie still yields a usable stream. Caller holds
// evictMu, so no synchronization is required.
func (c *Critbit[T]) nextRand() uint64 {
	x := c.evictRand
	if x == 0 {
		x = c.clock.Load() | 1 // xorshift64 must not start at zero
	}
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	c.evictRand = x
	return x
}
