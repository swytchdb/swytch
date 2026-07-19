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

// Trace-replay harness: replays a request trace through plain LRU, the real
// CloxCache, and clairvoyant Belady OPT (with bypass), and scores individual
// eviction decisions against hindsight. Hit rate alone is a proxy — two
// policies can hit equally while sacrificing different entries — so the
// discriminating metrics are: excess misses over OPT; suboptimal-eviction
// rate (victim re-requested before some other then-resident key — a choice
// Belady would not make); victim return rate; and time-to-return. Wall-clock
// benchmarks are noise on I/O-bound systems (a 3.1× decode spread was
// observed between identical binaries twenty minutes apart); these
// trace-derived signals are stable and found every real bug the colibri port
// hit. The three synthetics below are adversarial regression fixtures:
// scan-burst (protection should approach OPT while LRU thrashes), zipf
// (protection should win modestly), and sliding-window drift (the squatter
// detector — undecayed protection collapses; decayed must track LRU).
package cache

import (
	"container/list"
	"math"
	"math/rand"
	"sort"
	"testing"
)

// occIndex maps each key to its sorted occurrence positions in the trace.
type occIndex map[uint64][]int

func buildOcc(trace []uint64) occIndex {
	occ := make(occIndex)
	for i, k := range trace {
		occ[k] = append(occ[k], i)
	}
	return occ
}

// nextAfter returns the first position strictly after pos at which key is
// requested again, or math.MaxInt if it never returns.
func (o occIndex) nextAfter(key uint64, pos int) int {
	lst := o[key]
	if i := sort.SearchInts(lst, pos+1); i < len(lst) {
		return lst[i]
	}
	return math.MaxInt
}

// decisionMetrics scores individual eviction decisions against hindsight.
type decisionMetrics struct {
	evictions     int
	victimReturns int   // victims re-requested at any later point
	suboptEvicts  int   // victim returned before some then-resident key would have
	ttrSum        int64 // total positions until a returning victim came back
}

func (m decisionMetrics) victimReturnRate() float64 {
	if m.evictions == 0 {
		return 0
	}
	return float64(m.victimReturns) / float64(m.evictions)
}

func (m decisionMetrics) suboptRate() float64 {
	if m.evictions == 0 {
		return 0
	}
	return float64(m.suboptEvicts) / float64(m.evictions)
}

func (m decisionMetrics) meanTimeToReturn() float64 {
	if m.victimReturns == 0 {
		return 0
	}
	return float64(m.ttrSum) / float64(m.victimReturns)
}

// scorer shadows a policy's live set and grades each eviction as it happens.
type scorer struct {
	occ      occIndex
	resident map[uint64]struct{}
	m        decisionMetrics
}

func newScorer(occ occIndex, capacity int) *scorer {
	return &scorer{occ: occ, resident: make(map[uint64]struct{}, capacity)}
}

func (s *scorer) admit(key uint64) { s.resident[key] = struct{}{} }

func (s *scorer) evict(key uint64, pos int) {
	delete(s.resident, key)
	s.m.evictions++
	vNext := s.occ.nextAfter(key, pos)
	if vNext == math.MaxInt {
		return
	}
	s.m.victimReturns++
	s.m.ttrSum += int64(vNext - pos)
	// Suboptimal iff some still-resident key's next use is farther than the
	// victim's: Belady would have sacrificed that key instead.
	for rk := range s.resident {
		if s.occ.nextAfter(rk, pos) > vNext {
			s.m.suboptEvicts++
			return
		}
	}
}

// replayLRU replays the trace through a plain LRU of the given capacity.
func replayLRU(trace []uint64, capacity int, occ occIndex) (int, decisionMetrics) {
	s := newScorer(occ, capacity)
	l := list.New()
	elems := make(map[uint64]*list.Element, capacity)
	hits := 0
	for i, k := range trace {
		if el, ok := elems[k]; ok {
			hits++
			l.MoveToFront(el)
			continue
		}
		if l.Len() >= capacity {
			back := l.Back()
			victim := back.Value.(uint64)
			delete(elems, victim)
			l.Remove(back)
			s.evict(victim, i)
		}
		elems[k] = l.PushFront(k)
		s.admit(k)
	}
	return hits, s.m
}

// replayOPT replays the trace through clairvoyant Belady with bypass: on a
// miss at capacity it evicts the resident with the farthest next use, unless
// the incoming key's own next use is farther still — then it bypasses
// admission entirely. This is the offline optimum any online policy is scored
// against.
func replayOPT(trace []uint64, capacity int) int {
	// next[i] = position of the next request for trace[i], or MaxInt.
	next := make([]int, len(trace))
	lastSeen := make(map[uint64]int, capacity)
	for i := len(trace) - 1; i >= 0; i-- {
		if p, ok := lastSeen[trace[i]]; ok {
			next[i] = p
		} else {
			next[i] = math.MaxInt
		}
		lastSeen[trace[i]] = i
	}

	resident := make(map[uint64]int, capacity) // key → next use
	hits := 0
	for i, k := range trace {
		if _, ok := resident[k]; ok {
			hits++
			resident[k] = next[i]
			continue
		}
		if len(resident) < capacity {
			resident[k] = next[i]
			continue
		}
		var worstKey uint64
		worst := -1
		for rk, rn := range resident {
			if rn > worst {
				worst, worstKey = rn, rk
			}
		}
		if next[i] >= worst {
			continue // bypass: the incoming key is the farthest-future of them all
		}
		delete(resident, worstKey)
		resident[k] = next[i]
	}
	return hits
}

// replayClox replays the trace through a real CloxCache, logging every
// eviction decision (ghost conversion or full unlink both leave the live set)
// through the scorer via the evict-notify hook.
func replayClox(trace []uint64, cfg Config, occ occIndex) (int, decisionMetrics) {
	c := NewCloxCache[uint64, int](cfg)
	defer c.Close()

	s := newScorer(occ, cfg.Capacity)
	pos := 0
	c.SetEvictNotify(func(k uint64, _ int) { s.evict(k, pos) })

	hits := 0
	for i, k := range trace {
		pos = i
		if _, ok := c.Get(k, 0); ok {
			hits++
			continue
		}
		c.Put(k, 0)
		s.admit(k)
	}
	return hits, s.m
}

// replayCfg is the fixture cache: 256 entries across 2 shards, full-shard
// sweeps for victim selectivity, default decay. Trace working sets are sized
// against this to land in the 0.3–0.8 capacity/working-set band where policy
// differences are visible (see the package comment's boundary conditions).
func replayCfg() Config {
	return Config{NumShards: 2, SlotsPerShard: 256, Capacity: 256, SweepPercent: 100}
}

func logReplay(t *testing.T, name string, ops, lruHits, cloxHits, optHits int, lruM, cloxM decisionMetrics) {
	t.Helper()
	pct := func(h int) float64 { return 100 * float64(h) / float64(ops) }
	t.Logf("%s: hit rate LRU %.1f%% / clox %.1f%% / OPT %.1f%%", name, pct(lruHits), pct(cloxHits), pct(optHits))
	t.Logf("%s: excess misses over OPT: LRU %d, clox %d", name, optHits-lruHits, optHits-cloxHits)
	t.Logf("%s: subopt-evict rate LRU %.1f%% clox %.1f%% | victim-return LRU %.1f%% clox %.1f%% | mean-ttr LRU %.0f clox %.0f",
		name, 100*lruM.suboptRate(), 100*cloxM.suboptRate(),
		100*lruM.victimReturnRate(), 100*cloxM.victimReturnRate(),
		lruM.meanTimeToReturn(), cloxM.meanTimeToReturn())
	if cloxHits > optHits {
		t.Fatalf("%s: clox hits %d exceed OPT's %d — the harness itself is broken", name, cloxHits, optHits)
	}
}

// TestReplayScanBurst: a stable hot set with periodic one-shot floods.
// Protection should shield the hot set and approach OPT while LRU flushes it
// on every flood.
func TestReplayScanBurst(t *testing.T) {
	const (
		ops       = 60000
		hotKeys   = 128
		burstLen  = 512
		burstGap  = 1000
		burstBase = uint64(1) << 32 // one-shot ids, disjoint from the hot set
	)
	rng := rand.New(rand.NewSource(1))
	var trace []uint64
	oneShot := burstBase
	for len(trace) < ops {
		for range burstGap {
			trace = append(trace, uint64(rng.Intn(hotKeys)))
		}
		for range burstLen {
			trace = append(trace, oneShot)
			oneShot++
		}
	}
	trace = trace[:ops]

	occ := buildOcc(trace)
	lruHits, lruM := replayLRU(trace, replayCfg().Capacity, occ)
	cloxHits, cloxM := replayClox(trace, replayCfg(), occ)
	optHits := replayOPT(trace, replayCfg().Capacity)
	logReplay(t, "scan-burst", ops, lruHits, cloxHits, optHits, lruM, cloxM)

	if cloxHits <= lruHits {
		t.Fatalf("scan-burst: clox hits %d ≤ LRU's %d — protection is not shielding the hot set", cloxHits, lruHits)
	}
}

// TestReplayZipf: skewed stationary popularity. Protection should win
// modestly over LRU by holding the head of the distribution.
func TestReplayZipf(t *testing.T) {
	const ops = 60000
	rng := rand.New(rand.NewSource(2))
	zipf := rand.NewZipf(rng, 1.2, 1, 2047)
	trace := make([]uint64, ops)
	for i := range trace {
		trace[i] = zipf.Uint64()
	}

	occ := buildOcc(trace)
	lruHits, lruM := replayLRU(trace, replayCfg().Capacity, occ)
	cloxHits, cloxM := replayClox(trace, replayCfg(), occ)
	optHits := replayOPT(trace, replayCfg().Capacity)
	logReplay(t, "zipf", ops, lruHits, cloxHits, optHits, lruM, cloxM)

	if cloxHits < lruHits {
		t.Fatalf("zipf: clox hits %d < LRU's %d — protection should win on stationary skew", cloxHits, lruHits)
	}
}

// TestReplaySlidingWindowDrift: the squatter detector. The working set
// drifts across the keyspace; undecayed protection collapses here (measured
// 50.8% hit vs LRU's 89.9% on the colibri traces) because saturated entries
// keep their slots after the workload moves on — and those retention
// mistakes are invisible to eviction metrics, since squatters are never
// evicted at all. With eviction-driven decay the policy must track LRU.
func TestReplaySlidingWindowDrift(t *testing.T) {
	const (
		ops        = 60000
		windowKeys = 320 // capacity/working-set ≈ 0.8
		slideEvery = 32  // window advances one key every N ops
	)
	rng := rand.New(rand.NewSource(3))
	trace := make([]uint64, ops)
	for i := range trace {
		trace[i] = uint64(i/slideEvery + rng.Intn(windowKeys))
	}

	occ := buildOcc(trace)
	lruHits, lruM := replayLRU(trace, replayCfg().Capacity, occ)
	cloxHits, cloxM := replayClox(trace, replayCfg(), occ)
	optHits := replayOPT(trace, replayCfg().Capacity)
	logReplay(t, "drift", ops, lruHits, cloxHits, optHits, lruM, cloxM)

	// For the record: the same trace with decay disabled shows what the
	// assertion below is guarding against.
	undecayedCfg := replayCfg()
	undecayedCfg.DecayInterval = -1
	undecayedHits, _ := replayClox(trace, undecayedCfg, occ)
	t.Logf("drift: undecayed clox hit rate %.1f%% (squatter mode)", 100*float64(undecayedHits)/float64(ops))

	if float64(cloxHits) < 0.9*float64(lruHits) {
		t.Fatalf("drift parity broken: clox hits %d < 90%% of LRU's %d — protection is squatting (decay regression?)",
			cloxHits, lruHits)
	}
}
