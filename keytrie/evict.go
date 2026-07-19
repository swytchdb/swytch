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
	"math"
	"sort"
	"unsafe"
)

// Eviction policy constants, lifted from CloxCache. The trie runs the policy
// as a single bounded domain (see Critbit.EvictBounded) rather than over
// fixed-size shards, so a sweep's cost is bounded by the absolute scan budget
// instead of a fraction of a shard.
//
// Boundary conditions on the policy's edge over plain LRU (measured against
// offline Belady replay): the win lives at capacity/working-set ratios of
// roughly 0.3–0.8 — below ~0.25 every policy produces near-identical contents,
// above ~1 plain recency already retains the working set — and it shrinks
// toward LRU-parity under any hot tier above the trie (subdag cache, client
// caches), which skims the frequency skew protection feeds on. State these
// conditions with any performance claim.
const (
	// defaultProtectedFreqThreshold: leaves with freq above this are
	// protected from eviction; below-or-equal are eviction candidates. k = 0
	// means protection is off — every live leaf is a candidate and the sweep
	// is pure LRU. That is the starting state: protection must earn its way
	// in via the graduation probe (see bumpAccess) rather than being
	// presumed, so a cache serving a low-reuse workload never starts out
	// worse than plain LRU. On a reuse-≈1.3 real trace, starting at k=2
	// meant every key that delivered its single re-read became a protected
	// squatter with zero future value.
	defaultProtectedFreqThreshold = 0

	// adaptiveCheckInterval: re-evaluate k every N evictions. Kept at CloxCache's
	// 1000: once the eviction counter was split from reclaim churn, real cold-key
	// evictions proved to flow fast enough that 1000 is reached readily — the
	// earlier worry that EvictBounded was too low-volume to ever hit it was an
	// artifact of the conflated counter. A 1000-sample window gives a far steadier
	// graduation-rate estimate than a smaller one, so k moves deliberately instead
	// of overshooting to the ceiling on noise.
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

	// minSweep: floor for the per-call sweep window so a small trie still scans a
	// useful sample rather than a sub-1% sliver. The window is now bounded to a
	// small multiple of want (see EvictBatch) rather than a fraction of the whole
	// keyspace, so eviction is O(want) and can run inline on the write path; the
	// persistent clock hand covers the rest of the arena across successive calls.
	minSweep = 128

	// sweepSegments: the sweep window is taken as this many contiguous runs
	// whose origins are spread evenly across the arena, not one contiguous
	// run. Arena position correlates with insertion cohort (allocation order
	// plus whole-chunk recycling), so a single contiguous window is
	// age-biased sampled LRU: when the hand transits a young cohort, the
	// "oldest in window" is itself young, and on low-reuse traces a young
	// victim takes its only future hit with it. Measured on a reuse-≈1.3
	// replay at the same 128-leaf budget: contiguous window −4.6pp hit rate
	// vs true LRU (60% vs 81% of achievable re-reads); 8 spread segments
	// recover ~97% of that gap while keeping cache locality within each run.
	// Full 128-way striding recovers ~100% but pays a random memory access
	// per leaf under evictMu + the reap read-lock, for ~0.1pp more.
	sweepSegments = 8

	// defaultDecayInterval: reclaims between eviction-driven frequency decay
	// steps. Each step walks a live leaf's freq down by 1 (floor 1) as the
	// sweep visits it, so a leaf that saturated freq while hot forgets over
	// (maxLeafFreq-1)*interval ≈ 112 reclaims instead of squatting protected
	// forever after the workload drifts — squatters are invisible to eviction
	// metrics because they are never evicted at all. Linear −1 rather than
	// halving: halving's first step (15→7) erases the hot-vs-warm ranking
	// exactly when a starved cache needs it. The clock is reclaims, never
	// wall time, so an idle trie forgets nothing. Too-fast decay degrades
	// into plain LRU (never below); too-slow re-admits squatters — the knob
	// is forgiving. Tunable via SetDecayInterval.
	defaultDecayInterval = 8
)

// SetEvictHooks installs the consumer callbacks the eviction sweep uses.
// decider vetoes a key as an eviction victim (return false to pin it, e.g.
// system keys). notify fires after a victim leaf is soft-deleted, handing the
// consumer the dropped key's tips and leaf payload so it can release refs and
// tear down (e.g. unsubscribe). pressure reports whether the consumer is
// currently over its eviction target — the instantaneous condition that gates
// graduation counting in bumpAccess (see underPressure). All three run without
// any trie lock the consumer could re-enter; keep them prompt.
//
// decider must be installed before any Insert: the sweep reads each leaf's pin
// verdict (critNode.pinned) cached at creation, so a leaf written before the
// decider exists would carry the default (unpinned) verdict. The memory
// governor installs the hooks inside NewEngine, before any write, so this
// holds.
func (c *Critbit[T]) SetEvictHooks(decider func(key string) bool, notify func(key string, tips []EffectRef, data *T), pressure func() bool) {
	c.evictDecider = decider
	c.evictNotify = notify
	c.underPressure = pressure
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

// EvictStats is a point-in-time snapshot of the adaptive eviction policy's
// internal state, for telemetry. The counters (EvictedUnprotected/Protected,
// ReachedProtected) are windowed — maybeAdaptK halves them periodically — so
// they reflect recent behaviour, not lifetime totals. GraduationRate is the
// quantity that actually drives k: when it exceeds RateHigh, k rises; below
// RateLow, k falls.
type EvictStats struct {
	K                  int32   // current protected-freq threshold
	GraduationRate     float64 // ReachedProtected / (EvictedUnprotected+EvictedProtected)
	RateLow            float64 // learned threshold below which k decreases
	RateHigh           float64 // learned threshold above which k increases
	EvictedUnprotected uint64  // windowed: victims with freq <= k (cold, expected)
	EvictedProtected   uint64  // windowed: victims with freq > k (forced — pressure too high)
	ReachedProtected   uint64  // windowed: leaves that graduated past k
	WindowHitRate      float64 // current adapt-window hit rate (self-tuning gradient input)
	GhostCount         int64   // ghosts retained for warm restart
}

// EvictStats snapshots the adaptive policy's internals. Lock-free reads of the
// same atomics maybeAdaptK uses; the snapshot is not transactionally consistent
// across fields, which is fine for telemetry.
func (c *Critbit[T]) EvictStats() EvictStats {
	eu := c.evictedUnprot.Load()
	ep := c.evictedProt.Load()
	grad := c.reachedProt.Load()
	var rate float64
	if total := eu + ep; total > 0 {
		rate = float64(grad) / float64(total)
	}
	var hitRate float64
	if ops := c.windowOps.load(); ops > 0 {
		// windowHits/windowOps are bumped as two separate atomics, so a reader can
		// momentarily observe more hits than ops; clamp so the rate never exceeds 1.
		hits := min(c.windowHits.load(), ops)
		hitRate = float64(hits) / float64(ops)
	}
	return EvictStats{
		K:                  c.evictK.Load(),
		GraduationRate:     rate,
		RateLow:            float64(c.rateLow.Load()) / 10000.0,
		RateHigh:           float64(c.rateHigh.Load()) / 10000.0,
		EvictedUnprotected: eu,
		EvictedProtected:   ep,
		ReachedProtected:   grad,
		WindowHitRate:      hitRate,
		GhostCount:         c.ghostCount.Load(),
	}
}

// EvictBounded evicts a single leaf (or reclaims one ghost). It is a thin
// wrapper over EvictBatch for one-at-a-time callers and tests; maxScan is
// ignored (EvictBatch sizes its own window). Returns true if it reclaimed
// anything.
func (c *Critbit[T]) EvictBounded(maxScan int) bool {
	return c.EvictBatch(1) > 0
}

// EvictBatch sweeps the leaf arena once and reclaims up to want leaves, so the
// scan cost is amortized across many evictions — this is what lets the memory
// governor drain under heavy write load instead of paying a full sweep per key.
//
// It evicts the LRU `want` of the cold (freq <= k) leaves in the window: keeping
// the *oldest* cold leaves means a just-written key (high lastAccess) is spared,
// preserving read-your-writes. If the window holds no cold leaf, it falls back —
// once — to reclaiming the oldest ghost (already-evicted dead weight pinning a
// chunk), then to evicting the LRU protected leaf. Returns the count reclaimed.
func (c *Critbit[T]) EvictBatch(want int) int {
	if c.closed.Load() || want <= 0 {
		return 0
	}

	c.evictMu.Lock()
	defer c.evictMu.Unlock()

	k := c.evictK.Load()
	epoch := c.decayEpoch()
	// Window: wide enough to choose the want oldest cold leaves with some
	// selectivity (so recent writes are spared), but bounded to a small multiple
	// of want rather than a fraction of the whole keyspace. The persistent clock
	// hand (evictHand in sweepLeaves) advances across calls, so successive sweeps
	// still cover the entire arena — capping the per-call window just keeps an
	// eviction O(want) so it can run inline on the write path as back-pressure
	// rather than paying an O(keyspace) sweep per batch.
	window := max(want*2, minSweep)

	type cand struct {
		leaf   *critNode[T]
		access uint64
	}
	cands := make([]cand, 0, window)
	var (
		ghosts   []cand // ghosts seen this sweep, sorted oldest-first below
		fbVictim *critNode[T]
		fbAccess = uint64(math.MaxUint64)
	)

	// Hold the reap read-lock across the sweep and the claims. It excludes the
	// reap write lock — the only thing that frees/reclaims a leaf slot — so a
	// published leaf's fields (including the non-atomic key) cannot be mutated
	// mid-read.
	rt := c.reapMu.RLock()

	c.sweepLeaves(window, func(leaf *critNode[T]) {
		access := leaf.lastAccess.Load()
		if leaf.deleted.Load() {
			// The sweep only forwards deleted leaves that are ghosts (freq < 0).
			ghosts = append(ghosts, cand{leaf, access})
			return
		}
		// Absorb pending eviction-driven decay before judging protection: −1
		// per elapsed epoch, floor 1, so a leaf that saturated freq while hot
		// steps back below k after the workload drifts instead of squatting on
		// its slot forever. Ghosts are exempt (their forgetting is turnover);
		// pinned leaves still decay so freq stays honest if they unpin.
		if epoch > 0 {
			decayLeaf(leaf, epoch)
		}
		if leaf.pinned {
			return // pinned (e.g. a system key) — verdict cached at leaf creation
		}
		if leaf.pins.Load() != 0 {
			return // dynamically pinned (e.g. cloud outbox holds un-acked uploads)
		}
		if access < fbAccess {
			fbVictim, fbAccess = leaf, access
		}
		// k == 0 is explicit "protection off": every live leaf is a candidate
		// and the batch is pure LRU. Without the special case, freq <= 0
		// matches nobody and eviction would fall through to the single-victim
		// fallback — LRU selection, but one reclaim per sweep instead of
		// `want`, and no ghosting (which is the recurrence memory k needs to
		// climb back out of 0).
		if k == 0 || leaf.freq.Load() <= k {
			cands = append(cands, cand{leaf, access})
		}
	})

	// Keep the want oldest cold candidates (LRU); recent writes have higher
	// lastAccess and are left alone.
	if len(cands) > want {
		sort.Slice(cands, func(i, j int) bool { return cands[i].access < cands[j].access })
		cands = cands[:want]
	}
	// Oldest-first, so the trim below and the reclaim fallback always take the
	// least-recent ghost.
	sort.Slice(ghosts, func(i, j int) bool { return ghosts[i].access < ghosts[j].access })

	// tick advances the decay clock one reclaim: forgetting is
	// eviction-clocked, never wall-clocked, so an idle trie forgets nothing.
	tick := func() {
		c.evictClock++
		if every := c.decayIntervalNow(); every > 0 && c.evictClock >= every {
			c.evictClock = 0
			c.decayEpochN.Add(1)
		}
	}

	// trimOldestGhost reclaims the least-recent ghost seen this sweep, skipping
	// any that were concurrently promoted back to live.
	trimOldestGhost := func() bool {
		for len(ghosts) > 0 {
			g := ghosts[0].leaf
			ghosts = ghosts[1:]
			if c.reclaimGhost(g) {
				return true
			}
		}
		return false
	}

	type evicted struct {
		key  string
		tips *TipSet
		data *T
	}
	victims := make([]evicted, 0, len(cands))
	for _, cd := range cands {
		leaf := cd.leaf
		if !c.claimForEviction(leaf) {
			continue
		}
		tips := leaf.tips.Load()
		data := leaf.data.Load()
		leaf.tips.Store(nil)
		leaf.data.Store(nil)
		c.size.Add(-1)
		// At ghost capacity, trim the oldest existing ghost to make room BEFORE
		// ghosting the fresh victim. The order is load-bearing: the victim was
		// chosen as LRU, so its recency is near-oldest — ghost-first followed by
		// "trim the LRU ghost" would almost always trim the ghost just created.
		// Ghost memory then freezes on a stale early cohort and every returning
		// key re-admits at freq 1 like a stranger; nothing crashes, the policy
		// just quietly becomes LRU-with-overhead (the colibri C port inverted
		// this exact order). Fresh ghosts from earlier victims in this batch are
		// not in the ghosts slice, so they can never be the trimmed one.
		if c.ghostCount.Load() >= c.ghostCap() {
			trimOldestGhost()
		}
		if c.ghostCount.Load() < c.ghostCap() {
			// Ghost: retain the frequency (negated) for a warm restart.
			for {
				f := leaf.freq.Load()
				nf := -f
				if nf >= 0 {
					nf = -1
				}
				if leaf.freq.CompareAndSwap(f, nf) {
					break
				}
			}
			c.ghostCount.Add(1)
		} else {
			c.deletedCount.Add(1)
			c.enqueueReap(leaf)
		}
		tick()
		victims = append(victims, evicted{leaf.key, tips, data})
	}

	// Fallback when the window held no cold leaf: reclaim the oldest ghost ahead
	// of evicting a hot key; only with neither do we take the LRU protected leaf.
	reclaimedGhost := false
	if len(victims) == 0 {
		if trimOldestGhost() {
			reclaimedGhost = true
			// Count the reclaim as an unprotected eviction (a ghost is a
			// previously-cold key) so the adaptive-k clock keeps advancing — without
			// this, k freezes exactly when ghost reclaims dominate (cold-scarce).
			c.evictedUnprot.Add(1)
			tick()
		} else if fbVictim != nil && c.claimForEviction(fbVictim) {
			tips := fbVictim.tips.Load()
			data := fbVictim.data.Load()
			fbVictim.tips.Store(nil)
			fbVictim.data.Store(nil)
			c.size.Add(-1)
			c.deletedCount.Add(1)
			c.enqueueReap(fbVictim)
			victims = append(victims, evicted{fbVictim.key, tips, data})
			c.evictedProt.Add(1)
			tick()
		}
	} else {
		c.evictedUnprot.Add(uint64(len(victims)))
	}
	c.reapMu.RUnlock(rt)

	// Release tip/payload references and notify, outside the lock (the consumer
	// callbacks may re-enter the trie).
	for _, v := range victims {
		c.fireTipDelta(v.tips, nil, v.data)
		if c.evictNotify != nil {
			var tipRefs []EffectRef
			if v.tips != nil {
				tipRefs = v.tips.Tips()
			}
			c.evictNotify(v.key, tipRefs, v.data)
		}
	}

	c.maybeAdaptK()
	c.maybeReap()

	if reclaimedGhost {
		return 1
	}
	return len(victims)
}

// SetDecayInterval overrides the eviction-driven frequency decay cadence:
// n > 0 sets reclaims-per-step, n <= 0 disables decay entirely. Call at setup;
// the default auto-scales with the live-leaf count (see decayIntervalNow).
func (c *Critbit[T]) SetDecayInterval(n int) {
	if n > 0 {
		c.decayEvery.Store(uint64(n))
		return
	}
	// MaxUint64 keeps the epoch counter frozen — decay never applies — while
	// staying distinct from 0, which means "not configured, auto-scale".
	c.decayEvery.Store(math.MaxUint64)
}

// decayIntervalNow returns the reclaims-per-decay-step in effect: the
// configured value when set, otherwise auto-scaled to the current live-leaf
// count so forget-from-saturation stays ≈7 turnovers of the resident set
// regardless of domain size — with a fixed small interval, a large domain's
// epochs elapse so fast that every leaf hits the floor by its next sweep
// visit and protection degrades to plain LRU. Returns 0 when disabled.
func (c *Critbit[T]) decayIntervalNow() uint64 {
	switch every := c.decayEvery.Load(); every {
	case 0:
		return max(defaultDecayInterval, uint64(c.size.Load())/2)
		//return defaultDecayInterval
	case math.MaxUint64:
		return 0
	default:
		return every
	}
}

// decayEpoch returns the current eviction-decay epoch. Leaves store the epoch
// they last absorbed and step down by the difference when the sweep visits
// them. The explicit counter (rather than clock/interval division) keeps
// epochs monotonic while the auto-scaled interval moves with the live count.
func (c *Critbit[T]) decayEpoch() uint64 {
	return c.decayEpochN.Load()
}

// decayLeaf absorbs a leaf's pending eviction-driven frequency decay: one −1
// step per decay epoch elapsed since its last sweep visit, floor 1. The CAS on
// lastDecayEpoch elects a single decayer per epoch — the sweep runs under
// evictMu, but ghost promotion also writes the field from the lock-free read
// path; the freq CAS loop rides out concurrent bumpAccess bumps.
func decayLeaf[T any](leaf *critNode[T], epoch uint64) {
	last := leaf.lastDecayEpoch.Load()
	if last >= epoch || !leaf.lastDecayEpoch.CompareAndSwap(last, epoch) {
		return
	}
	steps := epoch - last
	for {
		f := leaf.freq.Load()
		if f <= 1 {
			return // floor 1; ghosts (freq < 0) never reach here
		}
		nf := f - int32(min(steps, uint64(f-1)))
		if leaf.freq.CompareAndSwap(f, nf) {
			return
		}
	}
}

// claimForEviction atomically claims a live leaf as an eviction victim and
// revalidates its dynamic pins. Claim: deleted=false means a live, linked
// leaf, so a successful CAS false→true is an atomic claim (a lost CAS means a
// concurrent delete won). The pin/claim race is resolved Dekker-style: Pin
// increments then re-reads deleted (undoing if set); the claimer claims then
// re-reads pins. With seq-cst atomics exactly one side observes the other, so
// a pin can never be stranded on an evicted leaf and a pinned leaf can never
// be evicted — whichever write lands second detects the first.
func (c *Critbit[T]) claimForEviction(leaf *critNode[T]) bool {
	if !leaf.deleted.CompareAndSwap(false, true) {
		return false
	}
	if leaf.pins.Load() != 0 {
		leaf.deleted.Store(false)
		return false
	}
	return true
}

// ghostCap bounds how many ghosts (freq-only memory of evicted keys) the trie
// retains. A ghost is a still-linked dead leaf, so it pins its arena chunk from
// reclamation — the cost cloxcache (fixed slots) never had. So this is kept to a
// fraction of the live set rather than ~100% of it: enough to remember a recent
// evicted working set for warm restart, without holding a large share of chunks
// hostage. The eviction fallback (reclaimGhost) also draws ghosts back down when
// cold candidates run short. Tunable.
func (c *Critbit[T]) ghostCap() int64 {
	return max(c.size.Load()/8, 16)
}

// reclaimGhost converts a ghost (deleted, freq < 0) into a full delete so it is
// unlinked and its chunk can be reclaimed. It is the eviction fallback used
// ahead of evicting a live protected key: a ghost carries no tips/data (cleared
// when it was first evicted), so there is nothing to release — it is pure dead
// weight. Returns false if the ghost was concurrently promoted back to live.
func (c *Critbit[T]) reclaimGhost(g *critNode[T]) bool {
	for {
		f := g.freq.Load()
		if f >= 0 {
			return false // promoted back to a live leaf under us
		}
		if g.freq.CompareAndSwap(f, 0) {
			c.ghostCount.Add(-1)
			c.deletedCount.Add(1)
			c.enqueueReap(g)
			return true
		}
	}
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
			// The ghost period is not decay debt: without this reset the next
			// sweep visit would charge every epoch spent as a ghost against the
			// remembered freq and collapse it to the floor, erasing the warm
			// restart the ghost exists to provide.
			leaf.lastDecayEpoch.Store(c.decayEpoch())
			// A returning ghost is a window miss (op, not hit). Shard by the
			// reactivated leaf's address, same as the live-hit path.
			c.windowOps.add(unsafe.Pointer(leaf), 1)
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
	// Intentional unsigned underflow: the decay below halves the counters but
	// leaves lastAdaptCheck at this pre-decay total, so the next eviction
	// computes (smaller total) - (larger lastAdaptCheck), which wraps to a huge
	// value >= the interval and re-triggers adaptation. That gives a short burst
	// of back-to-back adapts as the counters decay toward the floor — letting k
	// move several steps to track a workload shift instead of crawling one step
	// per interval — then a quiet window until the counters climb again. This is
	// a faithful port of CloxCache's adaptThreshold; it reads like a bug and was
	// "fixed" once already (a lastAdaptCheck.Store(0) that killed the burst).
	// Don't reintroduce that store. The underflow is also simply cheaper than
	// tracking the burst with an explicit loop.
	if total-c.lastAdaptCheck.Load() < adaptiveCheckInterval {
		return
	}
	// Window gate (ported from the colibri C fix): the eviction interval arms
	// the check, but the k step waits for a completed hit-rate window, making
	// adaptation strictly change → measure → learn — every k step is evaluated
	// over exactly one full window, and lastKDir is never overwritten by an
	// unmeasured call. Without the gate, several k steps land inside one
	// window whenever evictions outpace probes (exactly when the cache is
	// under pressure and adaptation matters) and the gradient learns a
	// hit-rate delta attributed to only the most recent direction — noise.
	// lastAdaptCheck is not advanced while gated, so the check stays armed
	// until the window completes; this also paces the post-halving underflow
	// burst above to one k step per completed window rather than one per
	// eviction, which is the burst's intended shape.
	if c.windowOps.load() < hitRateWindowSize {
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
	if ops := c.windowOps.load(); ops >= hitRateWindowSize {
		// Clamp: the two counters are bumped independently, so hits can briefly
		// read above ops — never let the gradient see a rate over 1.
		hits := min(c.windowHits.load(), ops)
		hitRate := uint64(float64(hits) / float64(ops) * 10000)
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
		// bumpAccess bumps hits before ops. Resetting in the same order (ops
		// first, hits last) means a concurrent access straddling the reset has its
		// hits increment wiped while its ops increment survives, so the residual
		// is always ops >= hits — never a persisted windowHits > windowOps skew
		// bleeding into the next window.
		c.windowOps.reset()
		c.windowHits.reset()
	}

	rate := float64(graduated) / float64(total)
	rateLow := float64(c.rateLow.Load()) / 10000.0
	rateHigh := float64(c.rateHigh.Load()) / 10000.0
	k := c.evictK.Load()
	var dir int32
	// k may walk all the way to 0: protection off, pure LRU. When graduation
	// collapses, LRU IS the correct policy and the adapter must be able to
	// reach it — a floor of 1 still shields every key that was ever read
	// once, which on low-reuse workloads is exactly the spent-key set.
	// The graduation probe keeps counting at k=0 (see bumpAccess), so a
	// workload that develops skew climbs back out.
	if rate < rateLow && k > 0 {
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
		// lastAdaptCheck is deliberately NOT reset here — see the underflow note
		// at the top of this function. Halving the counters below the retained
		// lastAdaptCheck is what arms the next-eviction re-trigger.
	}
}

// sweepLeaves forwards leaves to fn by advancing a persistent CLOCK hand over the
// leaf arena's slots — the trie analogue of CloxCache's slot scan, with
// sequential array reads instead of per-sample tree descent. It forwards live
// leaves (counted toward the base budget) and ghosts (freq < 0, not counted, so
// the caller can pick the oldest as an eviction fallback). It skips uninitialized
// slots, reclaimed-chunk holes, and unlink-pending leaves. It stops after base
// live leaves or one full pass — there is no escalation: when the window has no
// cold leaf the caller reclaims a ghost rather than hunting further. The hand
// persists across calls. Runs under evictMu (sole mutator of evictHand) and the
// reap read-lock.
func (c *Critbit[T]) sweepLeaves(base int, fn func(*critNode[T])) {
	chunks, chunkSize := c.leafArena.chunkSnapshot()
	total := uint64(len(chunks)) * chunkSize
	if total == 0 {
		return
	}
	if base < 1 {
		base = 1
	}

	// Segment the window across the arena (see sweepSegments). When the whole
	// live set fits in the window there is nothing to de-bias — a single
	// contiguous pass keeps full deterministic coverage for small tries.
	nSeg := sweepSegments
	if int64(base) >= c.size.Load() || base < nSeg {
		nSeg = 1
	}
	segLen := base / nSeg
	segGap := total / uint64(nSeg) // origin spacing; also each segment's max span
	if segGap == 0 {
		segGap = total
	}

	hand := c.evictHand
	var advance uint64
	for s := 0; s < nSeg; s++ {
		want := segLen
		if s == nSeg-1 {
			want = base - segLen*(nSeg-1) // remainder rides the last segment
		}
		origin := hand + uint64(s)*segGap
		live := 0
		var j uint64
		for ; live < want && j < segGap; j++ {
			idx := (origin + j) % total
			chunk := chunks[idx/chunkSize]
			if chunk == nil {
				// Chunk was reclaimed and not recycled. Jump to the next
				// chunk boundary in one step — paying one iteration per dead
				// slot made the sweep O(high-water mark) on a churned arena,
				// and the sweep runs under the reap read-lock with evictMu
				// held, so every wasted iteration is serialized against
				// writers.
				j += chunkSize - 1 - idx%chunkSize
				continue
			}
			n := &chunk[idx%chunkSize]
			if n.deleted.Load() {
				if n.freq.Load() < 0 {
					fn(n) // ghost — forwarded for the reclaim fallback, not counted
				}
				continue // ghosts/uninitialized/unlink-pending don't count as live
			}
			fn(n)
			live++
		}
		if s == 0 {
			// The hand advances by segment 0's span only; every origin rotates
			// with it, so successive sweeps still cover the entire arena.
			advance = max(j, 1)
		}
	}
	c.evictHand = hand + advance
}
