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
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
)

// PeerHealth tracks the liveness, path symmetry, and clock measurements of a
// single peer.
type PeerHealth struct {
	// alive indicates the peer has sent a tick within its liveness timeout.
	alive atomic.Bool

	// symmetric indicates bidirectional path verification.
	symmetric atomic.Bool

	// clock holds the per-peer measurement windows (offsets, inter-arrival)
	// and derives RTT_min / skew / staleness bounds from them.
	clock *peerClockStats
}

// IsReplicationTarget returns true if this peer is both alive and has a
// verified symmetric path — suitable for same-region replication with ACK.
func (ph *PeerHealth) IsReplicationTarget() bool {
	return ph.alive.Load() && ph.symmetric.Load()
}

// PeerHealthTable maps peer node IDs to their health state.
type PeerHealthTable struct {
	peers           *xsync.Map[NodeId, *PeerHealth]
	cfg             ClockConfig
	OnPeerRecovered func(peerID NodeId) // called when a peer transitions from dead to alive
}

// NewPeerHealthTable creates an empty health table. Zero-valued cfg fields
// select defaults.
func NewPeerHealthTable(cfg ClockConfig) *PeerHealthTable {
	return &PeerHealthTable{
		peers: xsync.NewMap[NodeId, *PeerHealth](),
		cfg:   cfg.withDefaults(),
	}
}

// Get returns the PeerHealth for the given node, or nil if not tracked.
func (t *PeerHealthTable) Get(nodeID NodeId) *PeerHealth {
	v, ok := t.peers.Load(nodeID)
	if !ok {
		return nil
	}
	return v
}

// GetOrCreate returns the PeerHealth for the given node, creating it if needed.
func (t *PeerHealthTable) GetOrCreate(nodeID NodeId) *PeerHealth {
	if v, ok := t.peers.Load(nodeID); ok {
		return v
	}
	v, _ := t.peers.LoadOrStore(nodeID, &PeerHealth{clock: newPeerClockStats(t.cfg.WindowSize)})
	return v
}

// Remove removes a peer from the health table and drops its metric series.
func (t *PeerHealthTable) Remove(nodeID NodeId) {
	t.peers.Delete(nodeID)
	removePeerMetrics(nodeID)
}

// AliveSymmetricPeers returns the node IDs of all peers that are in the given
// localRegion and are both alive and symmetric (valid replication targets).
func (t *PeerHealthTable) AliveSymmetricPeers(localRegion string, peerRegions map[NodeId]string) []NodeId {
	var result []NodeId
	t.peers.Range(func(nodeID NodeId, ph *PeerHealth) bool {
		if peerRegions[nodeID] == localRegion && ph.IsReplicationTarget() {
			result = append(result, nodeID)
		}
		return true
	})
	return result
}

// AllSameRegionPeers returns the node IDs of all peers in the given local region,
// regardless of liveness. Used for fire-and-forget replication to dead peers
// so they receive effects when the partition heals.
func (t *PeerHealthTable) AllSameRegionPeers(localRegion string, peerRegions map[NodeId]string) []NodeId {
	var result []NodeId
	t.peers.Range(func(nodeID NodeId, _ *PeerHealth) bool {
		if peerRegions[nodeID] == localRegion {
			result = append(result, nodeID)
		}
		return true
	})
	return result
}

// AliveSymmetricCount returns the count of alive+symmetric peers in the local region.
func (t *PeerHealthTable) AliveSymmetricCount(localRegion string, peerRegions map[NodeId]string) int {
	count := 0
	t.peers.Range(func(nodeID NodeId, ph *PeerHealth) bool {
		if peerRegions[nodeID] == localRegion && ph.IsReplicationTarget() {
			count++
		}
		return true
	})
	return count
}

// AllRegionPeersReachable returns true if every same-region peer is alive
// and has a verified symmetric path. Returns true when there are no
// same-region peers (standalone or cross-region-only deployment).
func (t *PeerHealthTable) AllRegionPeersReachable(localRegion string, peerRegions map[NodeId]string) bool {
	reachable := true
	t.peers.Range(func(nodeID NodeId, ph *PeerHealth) bool {
		if peerRegions[nodeID] == localRegion && !ph.IsReplicationTarget() {
			reachable = false
			return false // short-circuit
		}
		return true
	})
	return reachable
}

// InMajorityPartition returns true if this node (counting itself) can reach
// a strict majority of same-region nodes. With N total region nodes
// (including self), majority requires > N/2 reachable. Returns true when
// there are no same-region peers (standalone).
func (t *PeerHealthTable) InMajorityPartition(localRegion string, peerRegions map[NodeId]string) bool {
	total := 1     // count self
	reachable := 1 // self is always reachable
	t.peers.Range(func(nodeID NodeId, ph *PeerHealth) bool {
		if peerRegions[nodeID] == localRegion {
			total++
			if ph.IsReplicationTarget() {
				reachable++
			}
		}
		return true
	})
	return reachable > total/2
}

// GetRTT returns the min-filtered round-trip time to the given peer.
// Returns 0 if the peer is unknown or both offset directions haven't been
// sampled yet. Implements effects.PeerRTTProvider.
func (t *PeerHealthTable) GetRTT(nodeID NodeId) time.Duration {
	ph := t.Get(nodeID)
	if ph == nil {
		return 0
	}
	return ph.clock.snapshot(time.Now()).RTTMin
}

// ClockStats returns the derived clock measurements for the given peer, and
// false if the peer is not tracked.
func (t *PeerHealthTable) ClockStats(nodeID NodeId) (ClockStats, bool) {
	ph := t.Get(nodeID)
	if ph == nil {
		return ClockStats{}, false
	}
	return ph.clock.snapshot(time.Now()), true
}

// AlivePeerIDs returns the IDs of all peers that are currently alive.
// Implements effects.PeerRTTProvider.
func (t *PeerHealthTable) AlivePeerIDs() []NodeId {
	var result []NodeId
	t.peers.Range(func(nodeID NodeId, ph *PeerHealth) bool {
		if ph.alive.Load() {
			result = append(result, nodeID)
		}
		return true
	})
	return result
}

// PruneUnknownPeers removes health table entries for peers not present in the
// given set of known peer IDs. This prevents unbounded growth from transient
// inbound connections (e.g. crash-looping joiners).
func (t *PeerHealthTable) PruneUnknownPeers(known map[NodeId]struct{}) {
	t.peers.Range(func(nodeID NodeId, _ *PeerHealth) bool {
		if _, ok := known[nodeID]; !ok {
			t.peers.Delete(nodeID)
			removePeerMetrics(nodeID)
		}
		return true
	})
}

// CheckLiveness marks peers as dead when the monotonic elapsed time since
// their last tick exceeds their timeout. Once a peer's inter-arrival window
// is primed, the timeout adapts to its observed tick rate
// (MissedIntervals·δ_B + q99 jitter margin, clamped); until then a warm-up
// fallback derived from the configured interval applies. A single missed
// tick never trips detection — delay is heavy-tailed and the margin term
// absorbs GC pauses and packet drops.
func (t *PeerHealthTable) CheckLiveness() {
	now := time.Now()
	fallback := clampDuration(
		time.Duration(t.cfg.MissedIntervals)*t.cfg.Interval,
		t.cfg.MinTimeout, t.cfg.MaxTimeout)
	t.peers.Range(func(nodeID NodeId, ph *PeerHealth) bool {
		if !ph.alive.Load() {
			return true
		}
		elapsed, heard := ph.clock.sinceLastArrival(now)
		if !heard {
			return true
		}
		timeout, adaptive := ph.clock.livenessTimeout(t.cfg)
		if !adaptive {
			timeout = fallback
		}
		RecordPeerLivenessTimeout(nodeID, int64(timeout))
		if elapsed > timeout {
			ph.alive.Store(false)
			RecordPeerAlive(nodeID, false)
		}
		return true
	})
}
