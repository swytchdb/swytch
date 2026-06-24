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
	"testing"
	"time"
)

func TestClockWindow(t *testing.T) {
	w := newClockWindow(4)

	if w.len() != 0 {
		t.Fatalf("empty window len %d", w.len())
	}

	w.push(30)
	w.push(10)
	w.push(20)
	if w.len() != 3 || w.min() != 10 {
		t.Fatalf("len=%d min=%d, expected 3, 10", w.len(), w.min())
	}
	if got := w.quantile(0.5); got != 20 {
		t.Fatalf("median %d, expected 20", got)
	}
	if got := w.quantile(1.0); got != 30 {
		t.Fatalf("max quantile %d, expected 30", got)
	}

	// Wrap: pushes 40, 50 overwrite 30, 10 → live samples {20, 40, 50, 60}.
	w.push(40)
	w.push(50)
	w.push(60)
	if w.len() != 4 {
		t.Fatalf("wrapped len %d, expected 4", w.len())
	}
	if w.min() != 20 {
		t.Fatalf("wrapped min %d, expected 20", w.min())
	}

	w.reset()
	if w.len() != 0 {
		t.Fatalf("reset len %d, expected 0", w.len())
	}
}

// TestSkewAndRTTDerivation drives the windows with a synthetic geometry —
// peer clock ahead by S=100ms, one-way delays 2ms inbound / 8ms outbound
// (6ms asymmetry) — and checks the derived values against the design doc's
// bounds: RTT_min exact and skew-free, skew error exactly half the asymmetry.
func TestSkewAndRTTDerivation(t *testing.T) {
	const (
		skew    = 100 * time.Millisecond
		dIn     = 2 * time.Millisecond
		dOut    = 8 * time.Millisecond
		samples = 10
	)

	s := newPeerClockStats(64)
	base := time.Now().Add(-time.Duration(samples) * 50 * time.Millisecond)

	for i := range samples {
		arrival := base.Add(time.Duration(i) * 50 * time.Millisecond)
		// Queueing jitter on every sample except one per direction, so the
		// min-filter must dig the floor out of noisy measurements.
		var jitter time.Duration
		if i != 3 {
			jitter = time.Duration(i) * 700 * time.Microsecond
		}

		// Inbound: raw_offset = d_in − S (+ jitter).
		tickWall := arrival.UnixNano() - int64(dIn-skew+jitter)
		s.observeTick(&ClockTick{NodeID: 2, Seq: uint64(i + 1), WallClock: tickWall}, arrival)

		// Outbound echo: raw_offset = d_out + S (+ jitter).
		s.observeEcho(int64(dOut + skew + jitter))
	}

	st := s.snapshot(base.Add(time.Duration(samples-1)*50*time.Millisecond + 30*time.Millisecond))
	if !st.Primed {
		t.Fatalf("stats should be primed, got %+v", st)
	}

	// RTT_min = (d_in − S) + (d_out + S) = 10ms, skew cancels exactly.
	if st.RTTMin != dIn+dOut {
		t.Fatalf("RTTMin = %v, expected %v", st.RTTMin, dIn+dOut)
	}

	// skew_est = S + (d_out − d_in)/2 = 103ms: error is half the asymmetry.
	wantSkew := skew + (dOut-dIn)/2
	if st.SkewEst != wantSkew {
		t.Fatalf("SkewEst = %v, expected %v", st.SkewEst, wantSkew)
	}
	if errBound := st.RTTMin / 2; st.SkewEst-skew > errBound {
		t.Fatalf("skew error %v exceeds RTT_min/2 bound %v", st.SkewEst-skew, errBound)
	}

	// δ_B: ticks arrived every 50ms.
	if st.TickInterval != 50*time.Millisecond {
		t.Fatalf("TickInterval = %v, expected 50ms", st.TickInterval)
	}

	// ε = elapsed since last arrival + RTT_min.
	if want := 30*time.Millisecond + st.RTTMin; st.Epsilon != want {
		t.Fatalf("Epsilon = %v, expected %v", st.Epsilon, want)
	}
}

func TestLivenessTimeoutFormula(t *testing.T) {
	cfg := ClockConfig{MinTimeout: 10 * time.Millisecond}.withDefaults()
	s := newPeerClockStats(64)

	// N arrivals yield N−1 gaps: below minAdaptiveSamples, not adaptive yet.
	now := time.Now()
	for i := range minAdaptiveSamples {
		at := now.Add(time.Duration(i) * 100 * time.Millisecond)
		s.observeTick(&ClockTick{NodeID: 2, Seq: uint64(i + 1), WallClock: at.UnixNano()}, at)
	}
	if _, ok := s.livenessTimeout(cfg); ok {
		t.Fatal("timeout should not be adaptive before the window is primed")
	}

	// One more arrival primes it: timeout = M·median + margin.
	at := now.Add(time.Duration(minAdaptiveSamples) * 100 * time.Millisecond)
	s.observeTick(&ClockTick{NodeID: 2, Seq: minAdaptiveSamples + 1, WallClock: at.UnixNano()}, at)

	timeout, ok := s.livenessTimeout(cfg)
	if !ok {
		t.Fatal("timeout should be adaptive once primed")
	}
	if want := 300 * time.Millisecond; timeout != want {
		t.Fatalf("adaptive timeout = %v, expected %v", timeout, want)
	}

	// Clamping to MaxTimeout.
	cfgTight := cfg
	cfgTight.MaxTimeout = 150 * time.Millisecond
	if timeout, _ := s.livenessTimeout(cfgTight); timeout != 150*time.Millisecond {
		t.Fatalf("clamped timeout = %v, expected 150ms", timeout)
	}
}

// TestAdjustInterval checks the adaptive Nyquist tuning: target is the
// closest peer's RTT_min, clamped to the floor, moving at most 2× per
// adjustment window.
func TestAdjustInterval(t *testing.T) {
	health := NewPeerHealthTable(ClockConfig{})
	hm := NewHeartbeatManager(1, health, ClockConfig{})
	hm.AddPeer(2, "local")

	// No primed stats: no adjustment even at the window boundary.
	for range intervalAdjustEvery {
		if _, changed := hm.adjustInterval(); changed {
			t.Fatal("interval must not change without primed peer stats")
		}
	}

	// Prime peer 2 at RTT_min = 12ms.
	ph := health.Get(2)
	now := time.Now()
	ph.clock.observeTick(&ClockTick{NodeID: 2, Seq: 1, WallClock: now.UnixNano() - int64(6*time.Millisecond)}, now)
	ph.clock.observeEcho(int64(6 * time.Millisecond))

	// 1s → 500ms → ... → 12ms: geometric descent, one halving per window,
	// landing on the RTT_min target, then stable.
	expected := []time.Duration{
		500 * time.Millisecond,
		250 * time.Millisecond,
		125 * time.Millisecond,
		62500 * time.Microsecond,
		31250 * time.Microsecond,
		15625 * time.Microsecond,
		12 * time.Millisecond,
		0, // converged: no further change
	}
	for stepIdx, want := range expected {
		var next time.Duration
		var changed bool
		for range intervalAdjustEvery {
			next, changed = hm.adjustInterval()
		}
		if want == 0 {
			if changed {
				t.Fatalf("step %d: interval changed past convergence to %v", stepIdx, next)
			}
			continue
		}
		if !changed || next != want {
			t.Fatalf("step %d: interval (%v, %v), expected (%v, true)", stepIdx, next, changed, want)
		}
	}
}
