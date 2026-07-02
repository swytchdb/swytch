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

package effects

import (
	"context"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
)

// Broadcaster sends effect notifications to cluster peers.
// Nil means standalone mode.
type Broadcaster interface {
	Broadcast(notify *pb.OffsetNotify)
	BroadcastWithData(notify *pb.OffsetNotify, effectData []byte)
	Replicate(notify *pb.OffsetNotify, wireData []byte) error
	// ReplicateTo sends to a specific peer and waits for ACK/NACK.
	// Returns NackNotify slice on conflict, nil on ACK.
	ReplicateTo(notify *pb.OffsetNotify, wireData []byte, targetNodeID pb.NodeID) ([]*pb.NackNotify, error)
	// ReplicateMarshalled is ReplicateTo with a pre-marshalled notify body, so
	// a fan-out (the bind to every subscriber) marshals the body once instead
	// of once per peer. notify is passed for inspection; the body is the wire
	// payload.
	ReplicateMarshalled(notify *pb.OffsetNotify, notifyBody []byte, targetNodeID pb.NodeID) ([]*pb.NackNotify, error)
	// SendNack sends an enriched NACK to the originator.
	SendNack(nack *pb.NackNotify, targetNodeID pb.NodeID)
	FetchFromAny(ref *pb.EffectRef) ([]byte, error)
	Fetch(ref *pb.EffectRef) ([]byte, error)
	PeerIDs() []pb.NodeID
	// AllRegionPeersReachable returns true if every same-region peer is
	// alive and has a verified symmetric path. Used by SafeMode to gate
	// writes: a key must not be written unless all region peers are reachable.
	AllRegionPeersReachable() bool
	// InMajorityPartition returns true if this node can reach a strict
	// majority of same-region nodes (including itself). Used by SafeMode
	// to block transactions when in a minority partition.
	InMajorityPartition() bool
	// ForwardTransaction sends a transaction to a specific peer for execution
	// (adaptive serialization §5). Returns the leader's response.
	ForwardTransaction(ctx context.Context, targetNodeID pb.NodeID, tx *pb.ForwardedTransaction) (*pb.ForwardedResponse, error)
}

// CloudReader is the tiered-storage backstop: it reports the tip frontier that
// durable Cloud storage holds for a key. A key evicted from every live peer
// (e.g. its writer departed) may still live on Cloud, so a read that finds
// nothing cluster-wide asks Cloud for the frontier, installs it, and lets the
// cluster take over. Nil (unset) means no Cloud is configured and the read
// free-misses as before.
type CloudReader interface {
	// CloudTips returns the tip frontier Cloud holds for key, or nil (with nil
	// error) if Cloud holds nothing for it. The engine installs these tips via
	// walkAndInstall — the effect blobs themselves are pulled on demand by
	// FetchFromAny, which already races the CDN — so this only needs to supply
	// the frontier, not the sub-DAG. Content-blind on the wire: the
	// implementation maps key to its Cloud PRF image and calls GetTips.
	CloudTips(ctx context.Context, key string) ([]Tip, error)
}

// PeerRTTProvider provides RTT measurements to peers for optimal leader selection.
type PeerRTTProvider interface {
	// GetRTT returns the estimated round-trip time to the given peer.
	// Returns 0 if the peer is unknown or RTT has not been measured.
	GetRTT(nodeID pb.NodeID) time.Duration
	// AlivePeerIDs returns the IDs of all peers that are currently alive.
	AlivePeerIDs() []pb.NodeID
}
