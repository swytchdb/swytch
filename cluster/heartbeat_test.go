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

func TestMarshalParseClockTick(t *testing.T) {
	tick := &ClockTick{
		NodeID:    42,
		Seq:       7,
		WallClock: 123456789,
		JumpNs:    -5000,
		Observed: []ObservedTick{
			{NodeID: 9, Seq: 3, ObservedAt: 111},
			{NodeID: 11, Seq: 5, ObservedAt: 222},
		},
	}

	packet := MarshalClockTick(tick)
	if len(packet) != tickHeaderSize+2*observedTickSize {
		t.Fatalf("packet size %d, expected %d", len(packet), tickHeaderSize+2*observedTickSize)
	}
	if packet[0] != PacketTypeClockTick {
		t.Fatalf("packet type %d, expected %d", packet[0], PacketTypeClockTick)
	}

	parsed, err := ParseClockTick(packet)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if parsed.NodeID != 42 || parsed.Seq != 7 || parsed.WallClock != 123456789 || parsed.JumpNs != -5000 {
		t.Fatalf("header mismatch: %+v", parsed)
	}
	if len(parsed.Observed) != 2 {
		t.Fatalf("observed count %d, expected 2", len(parsed.Observed))
	}
	if parsed.Observed[0] != tick.Observed[0] || parsed.Observed[1] != tick.Observed[1] {
		t.Fatalf("observed mismatch: %+v", parsed.Observed)
	}

	// Short input
	if _, err := ParseClockTick([]byte{PacketTypeClockTick, clockTickVersion}); err == nil {
		t.Fatal("expected error for short input")
	}

	// Unknown version
	bad := MarshalClockTick(tick)
	bad[1] = 99
	if _, err := ParseClockTick(bad); err == nil {
		t.Fatal("expected error for unknown version")
	}

	// Truncated observations
	if _, err := ParseClockTick(packet[:len(packet)-1]); err == nil {
		t.Fatal("expected error for truncated observations")
	}
}

func TestTickMarksSymmetric(t *testing.T) {
	healthA := NewPeerHealthTable(ClockConfig{})
	healthB := NewPeerHealthTable(ClockConfig{})

	hmA := NewHeartbeatManager(1, healthA, ClockConfig{})
	hmB := NewHeartbeatManager(2, healthB, ClockConfig{})

	hmA.AddPeer(2, "local")
	hmB.AddPeer(1, "local")

	// A's tick arrives at B (through the wire format)
	tickA, err := ParseClockTick(MarshalClockTick(&ClockTick{
		NodeID: 1, Seq: 1, WallClock: time.Now().UnixNano(),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hmB.ProcessInboundTick(tickA.NodeID, tickA)

	phB := healthB.Get(1)
	if phB == nil {
		t.Fatal("B should have health entry for A")
	}
	if !phB.alive.Load() {
		t.Fatal("B should see A as alive")
	}
	if !phB.symmetric.Load() {
		t.Fatal("B should see A as symmetric after receiving a tick")
	}

	// B's tick arrives at A
	tickB, err := ParseClockTick(MarshalClockTick(&ClockTick{
		NodeID: 2, Seq: 1, WallClock: time.Now().UnixNano(),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	hmA.ProcessInboundTick(tickB.NodeID, tickB)

	phA := healthA.Get(2)
	if phA == nil {
		t.Fatal("A should have health entry for B")
	}
	if !phA.alive.Load() {
		t.Fatal("A should see B as alive")
	}
	if !phA.symmetric.Load() {
		t.Fatal("A should see B as symmetric after receiving a tick")
	}
}

// TestTickExchangeMeasuresRTT wires two managers back to back and verifies
// that one full tick round (A→B, B→A with the echo) primes A's RTT and skew
// measurements for B.
func TestTickExchangeMeasuresRTT(t *testing.T) {
	healthA := NewPeerHealthTable(ClockConfig{})
	healthB := NewPeerHealthTable(ClockConfig{})

	hmA := NewHeartbeatManager(1, healthA, ClockConfig{})
	hmB := NewHeartbeatManager(2, healthB, ClockConfig{})

	hmA.AddPeer(2, "local")
	hmB.AddPeer(1, "local")

	deliver := func(to *HeartbeatManager) func(NodeId, []byte) error {
		return func(_ NodeId, data []byte) error {
			// Simulated path delay keeps both directional offsets positive.
			time.Sleep(time.Millisecond)
			tick, err := ParseClockTick(data)
			if err != nil {
				return err
			}
			to.ProcessInboundTick(tick.NodeID, tick)
			return nil
		}
	}
	hmA.sendFunc = deliver(hmB)
	hmB.sendFunc = deliver(hmA)

	hmA.sendTick() // B observes A's tick, queues the observation
	hmB.sendTick() // B's tick carries the echo; A measures both directions

	if rtt := healthA.GetRTT(2); rtt <= 0 {
		t.Fatalf("A should have a positive RTT for B, got %v", rtt)
	}
	st, ok := healthA.ClockStats(2)
	if !ok || !st.Primed {
		t.Fatalf("A's clock stats for B should be primed, got %+v", st)
	}
	if st.Epsilon < st.RTTMin {
		t.Fatalf("staleness bound %v should include RTTMin %v", st.Epsilon, st.RTTMin)
	}

	// B has only seen A's tick (one inbound offset) and no echo yet.
	if rtt := healthB.GetRTT(1); rtt != 0 {
		t.Fatalf("B should not have RTT for A before an echo, got %v", rtt)
	}

	// One more round closes B's loop too.
	hmA.sendTick()
	if rtt := healthB.GetRTT(1); rtt <= 0 {
		t.Fatalf("B should have a positive RTT for A after the echo, got %v", rtt)
	}
}

func TestLivenessWarmupTimeout(t *testing.T) {
	health := NewPeerHealthTable(ClockConfig{})
	ph := health.GetOrCreate(1)

	// Last tick arrived 4s ago — beyond the 3-tick warm-up fallback (3×1s).
	past := time.Now().Add(-4 * time.Second)
	ph.clock.observeTick(&ClockTick{NodeID: 1, Seq: 1, WallClock: past.UnixNano()}, past)
	ph.alive.Store(true)

	health.CheckLiveness()

	if ph.alive.Load() {
		t.Fatal("peer should be marked dead after warm-up timeout")
	}

	// A fresh arrival keeps the peer alive.
	ph2 := health.GetOrCreate(2)
	now := time.Now()
	ph2.clock.observeTick(&ClockTick{NodeID: 2, Seq: 1, WallClock: now.UnixNano()}, now)
	ph2.alive.Store(true)

	health.CheckLiveness()

	if !ph2.alive.Load() {
		t.Fatal("recently heard peer should stay alive")
	}
}

// TestAdaptiveLivenessTimeout verifies that once the inter-arrival window is
// primed, the timeout follows the peer's observed tick rate instead of the
// warm-up fallback.
func TestAdaptiveLivenessTimeout(t *testing.T) {
	// MinTimeout below the adaptive value so clamping doesn't mask it.
	health := NewPeerHealthTable(ClockConfig{MinTimeout: 50 * time.Millisecond})
	ph := health.GetOrCreate(1)

	// Ticks every 100ms, the last one 400ms ago. Adaptive timeout =
	// 3×100ms + 0 margin = 300ms < 400ms elapsed → dead. The warm-up
	// fallback (3s) would have kept it alive.
	base := time.Now().Add(-400 * time.Millisecond)
	for i := range 10 {
		at := base.Add(-time.Duration(9-i) * 100 * time.Millisecond)
		ph.clock.observeTick(&ClockTick{NodeID: 1, Seq: uint64(i + 1), WallClock: at.UnixNano()}, at)
	}
	ph.alive.Store(true)

	health.CheckLiveness()

	if ph.alive.Load() {
		t.Fatal("peer should be dead under the adaptive timeout")
	}

	// Same arrival pattern, but with the default MinTimeout (1s) the
	// adaptive value clamps up to 1s and 400ms of silence is tolerated.
	healthClamped := NewPeerHealthTable(ClockConfig{})
	phC := healthClamped.GetOrCreate(1)
	for i := range 10 {
		at := base.Add(-time.Duration(9-i) * 100 * time.Millisecond)
		phC.clock.observeTick(&ClockTick{NodeID: 1, Seq: uint64(i + 1), WallClock: at.UnixNano()}, at)
	}
	phC.alive.Store(true)

	healthClamped.CheckLiveness()

	if !phC.alive.Load() {
		t.Fatal("peer should stay alive when the timeout clamps to MinTimeout")
	}
}

func TestJumpFlaggedTickResetsOffsets(t *testing.T) {
	health := NewPeerHealthTable(ClockConfig{})
	hm := NewHeartbeatManager(1, health, ClockConfig{})
	hm.AddPeer(2, "local")

	now := time.Now()
	hm.ProcessInboundTick(2, &ClockTick{NodeID: 2, Seq: 1, WallClock: now.UnixNano()})
	ph := health.Get(2)
	ph.clock.observeEcho(int64(5 * time.Millisecond))

	if st := ph.clock.snapshot(time.Now()); !st.Primed {
		t.Fatalf("stats should be primed before the jump, got %+v", st)
	}

	// A jump-flagged tick must clear both offset windows: the origin's wall
	// clock stepped, so accumulated skew samples are stale.
	hm.ProcessInboundTick(2, &ClockTick{
		NodeID: 2, Seq: 2, WallClock: time.Now().UnixNano(), JumpNs: int64(time.Minute),
	})

	if st := ph.clock.snapshot(time.Now()); st.Primed {
		t.Fatalf("stats should be unprimed after a jump reset, got %+v", st)
	}
	ph.clock.mu.Lock()
	inLen, outLen := ph.clock.offsetIn.len(), ph.clock.offsetOut.len()
	ph.clock.mu.Unlock()
	if inLen != 1 || outLen != 0 {
		t.Fatalf("expected offset windows (1, 0) after reset+repush, got (%d, %d)", inLen, outLen)
	}

	// Peer stays alive throughout — jumps affect skew, not liveness.
	if !ph.alive.Load() {
		t.Fatal("peer should remain alive across a clock jump")
	}
}

func TestComputeWallJump(t *testing.T) {
	// Wall advanced 5s while only 1s really elapsed: +4s forward jump.
	if got := computeWallJump(0, int64(5*time.Second), time.Second); got != 4*time.Second {
		t.Fatalf("forward jump = %v, expected 4s", got)
	}
	// Wall stood still while 2s elapsed: −2s backward adjustment.
	if got := computeWallJump(0, 0, 2*time.Second); got != -2*time.Second {
		t.Fatalf("backward jump = %v, expected -2s", got)
	}
	// Healthy clock: wall and monotonic agree.
	if got := computeWallJump(0, int64(time.Second), time.Second); got != 0 {
		t.Fatalf("healthy clock jump = %v, expected 0", got)
	}
}

func TestTickRing(t *testing.T) {
	ring := newTickRing(4)

	for seq := uint64(1); seq <= 6; seq++ {
		ring.put(seq, int64(seq*100))
	}

	// Sequences 3..6 are live (window 4); 1 and 2 were overwritten.
	if _, ok := ring.get(1); ok {
		t.Fatal("seq 1 should have been overwritten")
	}
	if _, ok := ring.get(2); ok {
		t.Fatal("seq 2 should have been overwritten")
	}
	for seq := uint64(3); seq <= 6; seq++ {
		wall, ok := ring.get(seq)
		if !ok || wall != int64(seq*100) {
			t.Fatalf("seq %d: got (%d, %v), expected (%d, true)", seq, wall, ok, seq*100)
		}
	}

	// Sequence 0 never matches (zero-valued slots).
	if _, ok := ring.get(0); ok {
		t.Fatal("seq 0 should never match")
	}
}

func TestPeerHealthTable(t *testing.T) {
	table := NewPeerHealthTable(ClockConfig{})

	ph := table.GetOrCreate(1)
	if ph == nil {
		t.Fatal("should create health entry")
	}

	ph2 := table.Get(1)
	if ph2 != ph {
		t.Fatal("should return same entry")
	}

	if table.Get(99) != nil {
		t.Fatal("should return nil for unknown peer")
	}

	// AliveSymmetricPeers
	ph.alive.Store(true)
	ph.symmetric.Store(true)

	ph3 := table.GetOrCreate(2)
	ph3.alive.Store(true)
	ph3.symmetric.Store(false) // alive but not symmetric

	regions := map[NodeId]string{1: "us-east", 2: "us-east"}
	peers := table.AliveSymmetricPeers("us-east", regions)
	if len(peers) != 1 || peers[0] != 1 {
		t.Fatalf("expected [1], got %v", peers)
	}

	count := table.AliveSymmetricCount("us-east", regions)
	if count != 1 {
		t.Fatalf("expected count 1, got %d", count)
	}

	// Remove
	table.Remove(1)
	if table.Get(1) != nil {
		t.Fatal("should be removed")
	}
}
