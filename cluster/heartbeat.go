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
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
)

// Packet type prefixes for the UDP fast-path protocol.
// 0x01 (heartbeat) and 0x02 (heartbeat ACK) were the legacy fixed-size
// liveness packets, replaced by PacketTypeClockTick; reserved to avoid reuse.
const (
	PacketTypeNotify    byte = 0x03
	PacketTypeNotifyACK byte = 0x04
	PacketTypeNack      byte = 0x05
	PacketTypeClockTick byte = 0x06
)

// Clock tick protocol constants.
const (
	// Liveness check cadence tracks the current tick interval within these
	// bounds, so detection latency follows adaptive tuning instead of being
	// floored by a fixed sweep rate.
	livenessCheckFloor = 10 * time.Millisecond
	livenessCheckCeil  = 500 * time.Millisecond

	clockTickVersion byte = 1

	// tickHeaderSize = type(1) + version(1) + nodeID(8) + seq(8) +
	// wallClock(8) + jumpNs(8) + count(2) = 36 bytes.
	// (Noise AEAD tag is added externally by the transport layer.)
	tickHeaderSize = 36

	// observedTickSize = node(8) + seq(8) + observedAt(8) = 24 bytes.
	observedTickSize = 24

	// jumpDetectFactor: a wall-vs-monotonic discrepancy larger than this
	// many tick intervals marks the tick as a wall-clock jump.
	jumpDetectFactor = 10

	// Adaptive interval bounds (see ClockConfig.DisableAdaptiveInterval).
	// The floor prevents the measure-RTT→tune-interval feedback loop from
	// running away; the ceiling bounds failure-detection latency.
	intervalFloor       = 10 * time.Millisecond
	intervalCeil        = 5 * time.Second
	intervalAdjustEvery = 16 // ticks between interval adjustments
)

type NodeId = pb.NodeID

// ObservedTick records that this node observed a peer's tick: the peer, the
// tick's sequence number, and our wall-clock time at observation. Echoed in
// our next outgoing tick, it is both a clock-DAG edge and the raw material
// for the peer's outbound-direction offset measurement.
type ObservedTick struct {
	NodeID     NodeId
	Seq        uint64
	ObservedAt int64 // observer's wall clock, ns since epoch
}

// ClockTick is one clock-DAG entry: a node's heartbeat promoted to a causal
// event. The implicit edge to the node's previous tick is Seq−1; the
// cross-node edges are the Observed entries. See docs/design/clock.md.
type ClockTick struct {
	NodeID    NodeId
	Seq       uint64
	WallClock int64 // sender's wall clock, ns since epoch

	// JumpNs is the wall-vs-monotonic discrepancy the sender detected at
	// emission (0 = none). Receivers reset their offset windows for this
	// origin: its wall clock stepped, so accumulated skew samples are stale.
	JumpNs int64

	Observed []ObservedTick
}

// HeartbeatManager emits and ingests clock ticks for all registered peers,
// driving liveness, path symmetry, and the per-peer clock measurements
// (RTT_min, skew, staleness bounds) in PeerHealthTable.
type HeartbeatManager struct {
	nodeID      NodeId
	healthTable *PeerHealthTable
	cfg         ClockConfig

	mu          sync.Mutex
	peerRegions map[NodeId]string

	// Tick emission state, guarded by mu.
	seq          uint64
	ownTicks     tickRing                // our recent ticks, for matching peers' echoes
	pendingObs   map[NodeId]ObservedTick // latest observation per peer, drained each tick
	lastTickWall int64
	lastTickTime time.Time // carries a monotonic reading
	interval     time.Duration
	ticksToAdjust int

	// sendFunc transmits a tick packet to a peer via the QUIC transport.
	sendFunc func(peerID NodeId, data []byte) error

	cancel chan struct{}
	wg     sync.WaitGroup
}

// NewHeartbeatManager creates a new HeartbeatManager.
func NewHeartbeatManager(nodeID NodeId, healthTable *PeerHealthTable, cfg ClockConfig) *HeartbeatManager {
	cfg = cfg.withDefaults()
	return &HeartbeatManager{
		nodeID:      nodeID,
		healthTable: healthTable,
		cfg:         cfg,
		peerRegions: make(map[NodeId]string),
		ownTicks:    newTickRing(cfg.WindowSize),
		pendingObs:  make(map[NodeId]ObservedTick),
		interval:    cfg.Interval,
		cancel:      make(chan struct{}),
	}
}

// AddPeer registers a peer for clock tick exchange and liveness monitoring.
func (hm *HeartbeatManager) AddPeer(nodeID NodeId, region string) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.peerRegions[nodeID] = region
	hm.healthTable.GetOrCreate(nodeID)
}

// RemovePeer unregisters a peer.
func (hm *HeartbeatManager) RemovePeer(nodeID NodeId) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	delete(hm.peerRegions, nodeID)
	delete(hm.pendingObs, nodeID)
	hm.healthTable.Remove(nodeID)
}

// Start begins the tick send loop and liveness checker.
func (hm *HeartbeatManager) Start(sendFunc func(peerID NodeId, data []byte) error) {
	hm.sendFunc = sendFunc

	hm.wg.Add(2)
	go hm.sendLoop()
	go hm.livenessLoop()
}

// SendTickTo sends a single tick to a specific peer immediately, bypassing
// the ticker. Used to accelerate symmetry establishment on new connections.
// Carries no observations; the regular tick fan-out handles those.
func (hm *HeartbeatManager) SendTickTo(peerID NodeId) {
	if hm.sendFunc == nil {
		return
	}
	hm.mu.Lock()
	hm.seq++
	tick := &ClockTick{NodeID: hm.nodeID, Seq: hm.seq, WallClock: time.Now().UnixNano()}
	hm.ownTicks.put(tick.Seq, tick.WallClock)
	hm.mu.Unlock()

	if err := hm.sendFunc(peerID, MarshalClockTick(tick)); err != nil {
		slog.Debug("immediate clock tick send failed", "peer", peerID, "error", err)
	}
}

// Stop stops the heartbeat manager.
func (hm *HeartbeatManager) Stop() {
	close(hm.cancel)
	hm.wg.Wait()
}

// ProcessInboundTick handles a received clock tick (already decrypted by
// Noise and validated against the sender's connection identity).
func (hm *HeartbeatManager) ProcessInboundTick(peerID NodeId, tick *ClockTick) {
	now := time.Now()
	ph := hm.healthTable.GetOrCreate(peerID)
	RecordClockTickReceived(peerID)

	if tick.JumpNs != 0 {
		slog.Warn("peer reported wall-clock jump, resetting offset windows",
			"peer", peerID, "jump", time.Duration(tick.JumpNs))
		RecordPeerClockJump(peerID)
	}

	// Detect dead→alive transition before updating state
	wasAlive := ph.alive.Load()

	ph.clock.observeTick(tick, now)
	ph.alive.Store(true)
	RecordPeerAlive(peerID, true)

	// If we receive ticks, the path is working — mark as symmetric.
	// Hash chain based detection proved unreliable over lossy UDP with Noise.
	ph.symmetric.Store(true)
	RecordPeerSymmetric(peerID, true)

	// Fire recovery callback on dead→alive transition
	if !wasAlive && hm.healthTable.OnPeerRecovered != nil {
		slog.Info("peer recovered, triggering reconvergence", "peer", peerID)
		go hm.healthTable.OnPeerRecovered(peerID)
	}

	// Match echoes of our own ticks: the peer's observation time minus our
	// send time is the outbound-direction offset sample.
	for _, obs := range tick.Observed {
		if obs.NodeID != hm.nodeID {
			continue
		}
		hm.mu.Lock()
		wall, ok := hm.ownTicks.get(obs.Seq)
		hm.mu.Unlock()
		if ok {
			ph.clock.observeEcho(obs.ObservedAt - wall)
		}
	}

	// Queue our observation of this tick for the next outgoing tick. Only
	// the latest observation per peer is kept: superseded ticks add nothing
	// to the peer's min-filter at steady state, and this bounds the packet
	// at O(peers) without cap/drop bookkeeping.
	hm.mu.Lock()
	hm.pendingObs[peerID] = ObservedTick{NodeID: peerID, Seq: tick.Seq, ObservedAt: now.UnixNano()}
	hm.mu.Unlock()

	st := ph.clock.snapshot(now)
	if st.Primed {
		RecordPeerRTT(peerID, int64(st.RTTMin))
		RecordPeerClockSkew(peerID, int64(st.SkewEst))
		RecordPeerStalenessBound(peerID, int64(st.TickInterval+st.RTTMin))
	}
	if st.TickInterval > 0 {
		RecordPeerTickInterval(peerID, int64(st.TickInterval))
	}

	slog.Debug("clock tick received", "from_peer", peerID, "seq", tick.Seq)
}

// sendLoop sends ticks to all peers every interval. Unless disabled, the
// interval retunes toward the closest peer's RTT_min — ticks faster than
// the path round-trip arrive superseded and carry no extra information.
func (hm *HeartbeatManager) sendLoop() {
	defer hm.wg.Done()
	hm.mu.Lock()
	interval := hm.interval
	hm.mu.Unlock()
	RecordClockTickInterval(int64(interval))
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-hm.cancel:
			return
		case <-ticker.C:
			hm.sendTick()
			if !hm.cfg.DisableAdaptiveInterval {
				if next, changed := hm.adjustInterval(); changed {
					ticker.Reset(next)
				}
			}
		}
	}
}

// livenessLoop periodically checks peer liveness, re-deriving its cadence
// from the current tick interval so adaptive tuning shortens detection
// latency rather than stalling at a fixed sweep rate.
func (hm *HeartbeatManager) livenessLoop() {
	defer hm.wg.Done()
	cadence := hm.livenessCadence()
	ticker := time.NewTicker(cadence)
	defer ticker.Stop()

	for {
		select {
		case <-hm.cancel:
			return
		case <-ticker.C:
			hm.healthTable.CheckLiveness()
			if next := hm.livenessCadence(); next != cadence {
				cadence = next
				ticker.Reset(next)
			}
		}
	}
}

// livenessCadence clamps the current tick interval into the liveness sweep
// bounds: checking roughly once per tick keeps the cadence from dominating
// the adaptive timeout (M·δ + margin) while bounding the sweep rate.
func (hm *HeartbeatManager) livenessCadence() time.Duration {
	hm.mu.Lock()
	interval := hm.interval
	hm.mu.Unlock()
	return clampDuration(interval, livenessCheckFloor, livenessCheckCeil)
}

// sendTick emits the next clock tick to every registered peer: one sequence
// number, one wall-clock reading, the drained observation set, marshaled
// once and fanned out — every peer sees the same clock-DAG entry.
func (hm *HeartbeatManager) sendTick() {
	now := time.Now()
	wall := now.UnixNano()

	hm.mu.Lock()
	peerIDs := make([]NodeId, 0, len(hm.peerRegions))
	for peerID := range hm.peerRegions {
		peerIDs = append(peerIDs, peerID)
	}

	// Wall-vs-monotonic divergence since the previous tick reveals clock
	// steps (NTP step, VM resume, manual set). Monotonic is ground truth
	// for elapsed time; the wall clock only reports what it believes.
	var jump time.Duration
	if !hm.lastTickTime.IsZero() {
		jump = computeWallJump(hm.lastTickWall, wall, now.Sub(hm.lastTickTime))
		if jump.Abs() <= jumpDetectFactor*hm.interval {
			jump = 0
		}
	}
	hm.lastTickWall = wall
	hm.lastTickTime = now

	hm.seq++
	tick := &ClockTick{
		NodeID:    hm.nodeID,
		Seq:       hm.seq,
		WallClock: wall,
		JumpNs:    int64(jump),
		Observed:  make([]ObservedTick, 0, len(hm.pendingObs)),
	}
	for _, obs := range hm.pendingObs {
		tick.Observed = append(tick.Observed, obs)
	}
	clear(hm.pendingObs)
	hm.ownTicks.put(tick.Seq, wall)
	hm.mu.Unlock()

	if jump != 0 {
		slog.Warn("local wall-clock jump detected, flagging tick", "jump", jump)
	}

	packet := MarshalClockTick(tick)
	for _, peerID := range peerIDs {
		if err := hm.sendFunc(peerID, packet); err != nil {
			slog.Debug("clock tick send failed", "peer", peerID, "error", err)
			continue
		}
	}
}

// computeWallJump returns how far wall-clock progress diverged from
// monotonic progress over the same span. Zero for a healthy clock; positive
// when the wall clock stepped forward, negative when it stepped back (or
// was slewed backward).
func computeWallJump(prevWall, wall int64, monoElapsed time.Duration) time.Duration {
	return time.Duration(wall-prevWall) - monoElapsed
}

// adjustInterval retunes the tick interval toward the closest peer's
// RTT_min once per adjustment window, moving at most 2× per step and
// clamped to [intervalFloor, intervalCeil] for feedback stability.
func (hm *HeartbeatManager) adjustInterval() (time.Duration, bool) {
	hm.mu.Lock()
	hm.ticksToAdjust++
	if hm.ticksToAdjust < intervalAdjustEvery {
		hm.mu.Unlock()
		return 0, false
	}
	hm.ticksToAdjust = 0
	peerIDs := make([]NodeId, 0, len(hm.peerRegions))
	for peerID := range hm.peerRegions {
		peerIDs = append(peerIDs, peerID)
	}
	current := hm.interval
	hm.mu.Unlock()

	var minRTT time.Duration
	for _, peerID := range peerIDs {
		st, ok := hm.healthTable.ClockStats(peerID)
		if !ok || !st.Primed {
			continue
		}
		if minRTT == 0 || st.RTTMin < minRTT {
			minRTT = st.RTTMin
		}
	}
	if minRTT == 0 {
		return 0, false
	}

	target := clampDuration(minRTT, intervalFloor, intervalCeil)
	next := clampDuration(target, current/2, current*2)
	if next == current {
		return 0, false
	}

	hm.mu.Lock()
	hm.interval = next
	hm.mu.Unlock()
	RecordClockTickInterval(int64(next))
	slog.Info("clock tick interval adjusted", "from", current, "to", next, "target", target)
	return next, true
}

// tickRing remembers the wall-clock send time of our recent ticks, indexed
// by sequence number, so peers' echoed observations can be matched to the
// tick they observed. Slots are reused modulo the window; an echo older
// than the window simply misses.
type tickRing struct {
	entries []tickRecord
}

type tickRecord struct {
	seq  uint64
	wall int64
}

func newTickRing(size int) tickRing {
	return tickRing{entries: make([]tickRecord, size)}
}

func (r *tickRing) put(seq uint64, wall int64) {
	r.entries[seq%uint64(len(r.entries))] = tickRecord{seq: seq, wall: wall}
}

// get returns the wall-clock send time for seq. Sequences start at 1, so
// zero-valued slots never match.
func (r *tickRing) get(seq uint64) (int64, bool) {
	if seq == 0 {
		return 0, false
	}
	rec := r.entries[seq%uint64(len(r.entries))]
	if rec.seq != seq {
		return 0, false
	}
	return rec.wall, true
}

// MarshalClockTick builds a clock tick plaintext body:
// [1:type][1:version][8:nodeID LE][8:seq LE][8:wallClock LE][8:jumpNs LE]
// [2:count LE] + count × [8:node LE][8:seq LE][8:observedAt LE]
// Encryption (Noise AEAD) is applied by the transport layer.
func MarshalClockTick(tick *ClockTick) []byte {
	buf := make([]byte, tickHeaderSize+len(tick.Observed)*observedTickSize)
	buf[0] = PacketTypeClockTick
	buf[1] = clockTickVersion
	binary.LittleEndian.PutUint64(buf[2:10], uint64(tick.NodeID))
	binary.LittleEndian.PutUint64(buf[10:18], tick.Seq)
	binary.LittleEndian.PutUint64(buf[18:26], uint64(tick.WallClock))
	binary.LittleEndian.PutUint64(buf[26:34], uint64(tick.JumpNs))
	binary.LittleEndian.PutUint16(buf[34:36], uint16(len(tick.Observed)))
	off := tickHeaderSize
	for _, obs := range tick.Observed {
		binary.LittleEndian.PutUint64(buf[off:off+8], uint64(obs.NodeID))
		binary.LittleEndian.PutUint64(buf[off+8:off+16], obs.Seq)
		binary.LittleEndian.PutUint64(buf[off+16:off+24], uint64(obs.ObservedAt))
		off += observedTickSize
	}
	return buf
}

// ParseClockTick parses a clock tick plaintext body (already Noise-decrypted).
func ParseClockTick(body []byte) (*ClockTick, error) {
	if len(body) < tickHeaderSize {
		return nil, fmt.Errorf("clock tick too short: %d < %d", len(body), tickHeaderSize)
	}
	if body[1] != clockTickVersion {
		return nil, fmt.Errorf("unsupported clock tick version: %d", body[1])
	}
	count := int(binary.LittleEndian.Uint16(body[34:36]))
	if want := tickHeaderSize + count*observedTickSize; len(body) != want {
		return nil, fmt.Errorf("clock tick length %d does not match %d observations (want %d)",
			len(body), count, want)
	}

	tick := &ClockTick{
		NodeID:    NodeId(binary.LittleEndian.Uint64(body[2:10])),
		Seq:       binary.LittleEndian.Uint64(body[10:18]),
		WallClock: int64(binary.LittleEndian.Uint64(body[18:26])),
		JumpNs:    int64(binary.LittleEndian.Uint64(body[26:34])),
	}
	if count > 0 {
		tick.Observed = make([]ObservedTick, count)
		off := tickHeaderSize
		for i := range tick.Observed {
			tick.Observed[i] = ObservedTick{
				NodeID:     NodeId(binary.LittleEndian.Uint64(body[off : off+8])),
				Seq:        binary.LittleEndian.Uint64(body[off+8 : off+16]),
				ObservedAt: int64(binary.LittleEndian.Uint64(body[off+16 : off+24])),
			}
			off += observedTickSize
		}
	}
	return tick, nil
}
