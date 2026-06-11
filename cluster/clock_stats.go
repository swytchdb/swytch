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

package cluster

import (
	"sort"
	"sync"
	"time"
)

// minAdaptiveSamples is the number of inter-arrival samples required before
// the adaptive liveness timeout replaces the warm-up fallback.
const minAdaptiveSamples = 8

// clockWindow is a fixed-size ring of int64 samples. Pushes are O(1);
// min/quantile queries scan (and for quantiles, sort) the live samples.
// Windows are small (default 64) and queried at heartbeat cadence, so the
// O(n log n) query cost is irrelevant. Network delay is heavy-tailed:
// minimums and quantiles are the right estimators, never means or stddevs.
type clockWindow struct {
	samples []int64
	next    int
	count   int
}

func newClockWindow(size int) clockWindow {
	return clockWindow{samples: make([]int64, size)}
}

func (w *clockWindow) push(v int64) {
	w.samples[w.next] = v
	w.next = (w.next + 1) % len(w.samples)
	if w.count < len(w.samples) {
		w.count++
	}
}

func (w *clockWindow) len() int { return w.count }

func (w *clockWindow) reset() {
	w.next = 0
	w.count = 0
}

// min returns the smallest live sample. Caller must ensure len() > 0.
func (w *clockWindow) min() int64 {
	m := w.samples[0]
	for i := 1; i < w.count; i++ {
		if w.samples[i] < m {
			m = w.samples[i]
		}
	}
	return m
}

// quantile returns the q-quantile (0 ≤ q ≤ 1) of the live samples.
// Caller must ensure len() > 0.
func (w *clockWindow) quantile(q float64) int64 {
	sorted := make([]int64, w.count)
	copy(sorted, w.samples[:w.count])
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(q * float64(w.count-1))
	return sorted[idx]
}

// ClockStats is a point-in-time snapshot of the clock measurements for one
// peer, derived from the raw observation windows. All bounds follow
// docs/design/clock.md: RTTMin and Epsilon are skew-free; SkewEst carries an
// error of at most half the path asymmetry (≤ RTTMin/2).
type ClockStats struct {
	// Primed is true once both offset directions have samples and the
	// derived RTT is positive. RTTMin, SkewEst, and Epsilon are only
	// meaningful when Primed.
	Primed bool

	// RTTMin is the min-filtered round-trip time to the peer. Skew cancels
	// exactly in the sum of the two directional offsets.
	RTTMin time.Duration

	// SkewEst estimates peer_wall_clock − local_wall_clock (positive: the
	// peer's clock is ahead of ours), by the midpoint convention on
	// min-filtered offsets.
	SkewEst time.Duration

	// TickInterval is the median inter-arrival gap of the peer's ticks
	// (δ_B), measured purely on the local monotonic clock.
	TickInterval time.Duration

	// JitterQ99 is the q99 inter-arrival gap's excess over the median —
	// the margin term for failure detection.
	JitterQ99 time.Duration

	// Epsilon bounds how stale our view of the peer can be right now:
	// elapsed time since the peer's last tick arrived plus RTTMin.
	Epsilon time.Duration
}

// peerClockStats holds the raw per-peer observation windows. All methods are
// safe for concurrent use; pushes arrive from transport goroutines while
// liveness checks and leader selection read concurrently.
type peerClockStats struct {
	mu sync.Mutex

	// offsetIn: raw offset of inbound ticks, arrival_wall − tick_wall (ns).
	// Equals d_peer→us − skew.
	offsetIn clockWindow

	// offsetOut: raw offset of our ticks as observed by the peer,
	// echo_observed_at − own_tick_wall (ns). Equals d_us→peer + skew.
	offsetOut clockWindow

	// interArrival: monotonic gaps between consecutive tick arrivals (ns).
	interArrival clockWindow

	// lastArrival carries a monotonic reading; zero until the first tick.
	lastArrival time.Time

	// lastSeq is the highest tick sequence seen from this peer.
	lastSeq uint64
}

func newPeerClockStats(window int) *peerClockStats {
	return &peerClockStats{
		offsetIn:     newClockWindow(window),
		offsetOut:    newClockWindow(window),
		interArrival: newClockWindow(window),
	}
}

// observeTick ingests an inbound tick from the peer. A tick flagged with a
// wall-clock jump resets both offset windows first: pre- and post-jump
// samples carry different skews, and a min-filter over the mixture corrupts
// both SkewEst and RTTMin (the skew terms only cancel in RTTMin when both
// directional minimums share the same skew). The inter-arrival window is
// monotonic-based and unaffected.
func (s *peerClockStats) observeTick(tick *ClockTick, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if tick.JumpNs != 0 {
		s.offsetIn.reset()
		s.offsetOut.reset()
	}

	if !s.lastArrival.IsZero() {
		s.interArrival.push(int64(now.Sub(s.lastArrival)))
	}
	s.lastArrival = now

	s.offsetIn.push(now.UnixNano() - tick.WallClock)
	if tick.Seq > s.lastSeq {
		s.lastSeq = tick.Seq
	}
}

// observeEcho ingests an outbound-direction offset sample: the peer's
// observation timestamp of one of our ticks minus that tick's send time.
func (s *peerClockStats) observeEcho(rawOffsetOut int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offsetOut.push(rawOffsetOut)
}

// snapshot derives the current ClockStats.
func (s *peerClockStats) snapshot(now time.Time) ClockStats {
	s.mu.Lock()
	defer s.mu.Unlock()

	var st ClockStats
	if s.offsetIn.len() > 0 && s.offsetOut.len() > 0 {
		minIn := s.offsetIn.min()
		minOut := s.offsetOut.min()
		if rtt := minIn + minOut; rtt > 0 {
			st.Primed = true
			st.RTTMin = time.Duration(rtt)
			st.SkewEst = time.Duration((minOut - minIn) / 2)
		}
	}
	if s.interArrival.len() > 0 {
		median := s.interArrival.quantile(0.5)
		st.TickInterval = time.Duration(median)
		if margin := s.interArrival.quantile(0.99) - median; margin > 0 {
			st.JitterQ99 = time.Duration(margin)
		}
	}
	if st.Primed && !s.lastArrival.IsZero() {
		st.Epsilon = now.Sub(s.lastArrival) + st.RTTMin
	}
	return st
}

// sinceLastArrival returns the monotonic elapsed time since the peer's last
// tick, and false if no tick has ever arrived.
func (s *peerClockStats) sinceLastArrival(now time.Time) (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastArrival.IsZero() {
		return 0, false
	}
	return now.Sub(s.lastArrival), true
}

// livenessTimeout returns the adaptive failure-detection timeout
// M·δ_B + q99 jitter margin, clamped to [MinTimeout, MaxTimeout]. Returns
// false until enough inter-arrival samples exist; callers then use the
// warm-up fallback instead.
func (s *peerClockStats) livenessTimeout(cfg ClockConfig) (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.interArrival.len() < minAdaptiveSamples {
		return 0, false
	}
	median := s.interArrival.quantile(0.5)
	margin := s.interArrival.quantile(0.99) - median
	if margin < 0 {
		margin = 0
	}
	timeout := time.Duration(int64(cfg.MissedIntervals)*median + margin)
	return clampDuration(timeout, cfg.MinTimeout, cfg.MaxTimeout), true
}

func clampDuration(d, lo, hi time.Duration) time.Duration {
	if d < lo {
		return lo
	}
	if d > hi {
		return hi
	}
	return d
}
