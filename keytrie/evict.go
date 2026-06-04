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
}

// EvictBounded runs one bounded eviction sweep. It scans at most maxScan
// leaves in key order from the resumable cursor, selects the
// least-recently-used unprotected (freq <= k) leaf — falling back to the
// overall LRU leaf when every candidate is protected — soft-deletes it, and
// notifies the consumer. Returns true if a leaf was evicted.
//
// Latency is bounded by maxScan regardless of keyspace size: the property
// CloxCache gets from scanning a fraction of a fixed shard, achieved here with
// an absolute scan budget advanced over an in-order cursor.
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
		lastKey   string
	)

	reachedEnd := c.forwardLeaves(c.evictHand, maxScan, func(leaf *critNode[T]) {
		lastKey = leaf.key
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

	if reachedEnd {
		c.evictHand = "" // wrap
	} else {
		c.evictHand = lastKey
	}

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
		nf := -f + 1
		if nf > maxLeafFreq {
			nf = maxLeafFreq
		}
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

// forwardLeaves visits leaves in key order strictly greater than `after`,
// calling fn for at most max of them, and returns whether the keyspace end was
// reached within the budget (so the caller wraps its cursor). It descends to
// `after` in O(depth) and walks forward, so cost is O(depth + visited) — it
// does not re-scan the skipped prefix. Lock-free read traversal; deleted/ghost
// leaves are yielded too so the caller can account for them.
func (c *Critbit[T]) forwardLeaves(after string, max int, fn func(*critNode[T])) (reachedEnd bool) {
	root := c.root.Load()
	if root == nil {
		return true
	}

	// Descend toward `after`. Wherever we take the left child, the right
	// subtree lies after us in key order — stash it to visit afterward.
	// Stashed deepest-last, so popping from the end yields in-order.
	var pending []*critNode[T]
	cur := root
	for cur != nil && !cur.isLeaf {
		dir := getDirection(after, cur.bytePos, cur.otherbits)
		if dir == 0 {
			if right := cur.child[1].Load(); right != nil {
				pending = append(pending, right)
			}
		}
		cur = cur.child[dir].Load()
	}

	count := 0
	visit := func(leaf *critNode[T]) bool { // returns keepGoing
		if leaf.key <= after {
			return true
		}
		fn(leaf)
		count++
		return count < max
	}

	if cur != nil && cur.isLeaf {
		if !visit(cur) {
			return false
		}
	}
	for len(pending) > 0 {
		n := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if !c.dfsLeaves(n, visit) {
			return false
		}
	}
	return true
}

// dfsLeaves walks the subtree rooted at n left-first, calling visit on each
// leaf until visit returns false. Returns visit's last keepGoing value.
func (c *Critbit[T]) dfsLeaves(n *critNode[T], visit func(*critNode[T]) bool) bool {
	stack := []*critNode[T]{n}
	for len(stack) > 0 {
		x := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if x.isLeaf {
			if !visit(x) {
				return false
			}
			continue
		}
		// Push right then left so the left subtree is popped (visited) first.
		if right := x.child[1].Load(); right != nil {
			stack = append(stack, right)
		}
		if left := x.child[0].Load(); left != nil {
			stack = append(stack, left)
		}
	}
	return true
}
