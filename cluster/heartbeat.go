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
const (
	PacketTypeHeartbeat    byte = 0x01
	PacketTypeHeartbeatACK byte = 0x02
	PacketTypeNotify       byte = 0x03
	PacketTypeNotifyACK    byte = 0x04
	PacketTypeNack         byte = 0x05
)

// Heartbeat protocol constants.
const (
	heartbeatInterval = 1 * time.Second
	heartbeatTimeout  = 3 * time.Second
	livenessCheckFreq = 500 * time.Millisecond

	// heartbeatPacketSize = type(1) + nodeID(8) + timestamp(8) = 17 bytes
	// (Noise AEAD tag is added externally by the transport layer)
	heartbeatPacketSize = 17
)

type NodeId = pb.NodeID

// HeartbeatManager manages the heartbeat protocol for all same-region peers.
type HeartbeatManager struct {
	nodeID      NodeId
	healthTable *PeerHealthTable

	mu          sync.Mutex
	peerRegions map[NodeId]string

	// sendFunc transmits a heartbeat packet to a peer via the QUIC transport.
	sendFunc func(peerID NodeId, data []byte) error

	// rttCallback feeds heartbeat RTT measurements. May be nil.
	rttCallback func(peerID NodeId, rtt time.Duration)

	cancel chan struct{}
	wg     sync.WaitGroup
}

// NewHeartbeatManager creates a new HeartbeatManager.
func NewHeartbeatManager(nodeID NodeId, healthTable *PeerHealthTable) *HeartbeatManager {
	return &HeartbeatManager{
		nodeID:      nodeID,
		healthTable: healthTable,
		peerRegions: make(map[NodeId]string),
		cancel:      make(chan struct{}),
	}
}

// AddPeer registers a peer for heartbeat monitoring.
func (hm *HeartbeatManager) AddPeer(nodeID NodeId, region string) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	hm.peerRegions[nodeID] = region
	hm.healthTable.GetOrCreate(nodeID)
}

// RemovePeer unregisters a peer from heartbeat monitoring.
func (hm *HeartbeatManager) RemovePeer(nodeID NodeId) {
	hm.mu.Lock()
	defer hm.mu.Unlock()
	delete(hm.peerRegions, nodeID)
	hm.healthTable.Remove(nodeID)
}

// Start begins the heartbeat send loop and liveness checker.
func (hm *HeartbeatManager) Start(sendFunc func(peerID NodeId, data []byte) error) {
	hm.sendFunc = sendFunc

	hm.wg.Add(2)
	go hm.sendLoop()
	go hm.livenessLoop()
}

// SendHeartbeatTo sends a single heartbeat to a specific peer immediately,
// bypassing the ticker. Used to accelerate symmetry establishment on new
// connections.
func (hm *HeartbeatManager) SendHeartbeatTo(peerID NodeId) {
	if hm.sendFunc == nil {
		return
	}
	timestamp := uint64(time.Now().UnixNano())
	packet := MarshalHeartbeat(hm.nodeID, timestamp)
	if err := hm.sendFunc(peerID, packet); err != nil {
		slog.Debug("immediate heartbeat send failed", "peer", peerID, "error", err)
	}
}

// Stop stops the heartbeat manager.
func (hm *HeartbeatManager) Stop() {
	close(hm.cancel)
	hm.wg.Wait()
}

// ProcessInbound handles a received heartbeat packet (already decrypted by Noise).
// peerID and timestamp are parsed from the packet body.
// Sends a HeartbeatACK back to the sender echoing the received timestamp for true RTT measurement.
func (hm *HeartbeatManager) ProcessInbound(peerID NodeId, timestamp uint64) {
	now := time.Now()
	ph := hm.healthTable.GetOrCreate(peerID)
	RecordHeartbeatReceived()

	// Detect dead→alive transition before updating state
	wasAlive := ph.alive.Load()

	// Update liveness
	ph.lastHeartbeat.Store(now.UnixNano())
	ph.alive.Store(true)
	RecordPeerAlive(peerID, true)

	// If we receive heartbeats, the path is working — mark as symmetric.
	// Hash chain based detection proved unreliable over lossy UDP with Noise.
	ph.symmetric.Store(true)
	RecordPeerSymmetric(peerID, true)

	// Fire recovery callback on dead→alive transition
	if !wasAlive && hm.healthTable.OnPeerRecovered != nil {
		slog.Info("peer recovered, triggering reconvergence", "peer", peerID)
		go hm.healthTable.OnPeerRecovered(peerID)
	}

	// Send ACK back echoing the sender's timestamp for true RTT measurement
	if hm.sendFunc != nil {
		ack := MarshalHeartbeatACK(hm.nodeID, timestamp)
		if err := hm.sendFunc(peerID, ack); err != nil {
			slog.Debug("heartbeat ACK send failed", "peer", peerID, "error", err)
		}
	}

	slog.Debug("heartbeat received", "from_peer", peerID)
}

// ProcessInboundACK handles a received HeartbeatACK packet.
// The echoedTimestamp is the original sender's timestamp echoed back, so
// RTT = now - echoedTimestamp uses the same clock for true round-trip measurement.
func (hm *HeartbeatManager) ProcessInboundACK(peerID NodeId, echoedTimestamp uint64) {
	rttNs := time.Now().UnixNano() - int64(echoedTimestamp)
	if rttNs <= 0 {
		return
	}
	ph := hm.healthTable.GetOrCreate(peerID)
	ph.rtt.Store(rttNs)
	RecordPeerRTT(peerID, rttNs)
	if hm.rttCallback != nil {
		hm.rttCallback(peerID, time.Duration(rttNs))
	}
	slog.Debug("heartbeat ACK received", "from_peer", peerID, "rtt_ms", float64(rttNs)/1e6)
}

// sendLoop sends heartbeats to all peers every heartbeatInterval.
func (hm *HeartbeatManager) sendLoop() {
	defer hm.wg.Done()
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-hm.cancel:
			return
		case <-ticker.C:
			hm.sendHeartbeats()
		}
	}
}

// livenessLoop periodically checks peer liveness.
func (hm *HeartbeatManager) livenessLoop() {
	defer hm.wg.Done()
	ticker := time.NewTicker(livenessCheckFreq)
	defer ticker.Stop()

	for {
		select {
		case <-hm.cancel:
			return
		case <-ticker.C:
			hm.healthTable.CheckLiveness(heartbeatTimeout)
		}
	}
}

// sendHeartbeats sends a heartbeat to each registered peer.
func (hm *HeartbeatManager) sendHeartbeats() {
	hm.mu.Lock()
	// Collect peer IDs under lock
	peerIDs := make([]NodeId, 0, len(hm.peerRegions))
	for peerID := range hm.peerRegions {
		peerIDs = append(peerIDs, peerID)
	}
	hm.mu.Unlock()

	timestamp := uint64(time.Now().UnixNano())

	for _, peerID := range peerIDs {
		packet := MarshalHeartbeat(hm.nodeID, timestamp)

		if err := hm.sendFunc(peerID, packet); err != nil {
			slog.Debug("heartbeat send failed", "peer", peerID, "error", err)
			continue
		}

		slog.Debug("heartbeat sent", "peer", peerID)
	}

	RecordHeartbeatsSent()
}

// MarshalHeartbeat builds a heartbeat plaintext body:
// [1:type][8:nodeID LE][8:timestamp LE]
// Encryption (Noise AEAD) is applied by the transport layer.
func MarshalHeartbeat(nodeID NodeId, timestamp uint64) []byte {
	buf := make([]byte, heartbeatPacketSize)
	buf[0] = PacketTypeHeartbeat
	binary.LittleEndian.PutUint64(buf[1:9], uint64(nodeID))
	binary.LittleEndian.PutUint64(buf[9:17], timestamp)
	return buf
}

// MarshalHeartbeatACK builds a heartbeat ACK plaintext body:
// [1:type 0x02][8:nodeID LE][8:timestamp LE]
// The timestamp is the original sender's timestamp echoed back unchanged.
// Encryption (Noise AEAD) is applied by the transport layer.
func MarshalHeartbeatACK(nodeID NodeId, echoedTimestamp uint64) []byte {
	buf := make([]byte, heartbeatPacketSize)
	buf[0] = PacketTypeHeartbeatACK
	binary.LittleEndian.PutUint64(buf[1:9], uint64(nodeID))
	binary.LittleEndian.PutUint64(buf[9:17], echoedTimestamp)
	return buf
}

// ParseHeartbeat parses a heartbeat plaintext body (already Noise-decrypted).
// Returns nodeID, timestamp, and an error if the body is too short.
func ParseHeartbeat(body []byte) (nodeID NodeId, timestamp uint64, err error) {
	if len(body) < heartbeatPacketSize {
		return 0, 0, fmt.Errorf("heartbeat too short: %d < %d", len(body), heartbeatPacketSize)
	}
	nodeID = NodeId(binary.LittleEndian.Uint64(body[1:9]))
	timestamp = binary.LittleEndian.Uint64(body[9:17])
	return nodeID, timestamp, nil
}
