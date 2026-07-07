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
	// FetchFromAny fetches effect bytes from peers or the cloud CDN. hint
	// orders the two sources; the second is a fallback tried only after the
	// first actually failed — never raced.
	FetchFromAny(ref *pb.EffectRef, hint FetchHint) ([]byte, error)
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

// FetchHint orders the two effect-byte sources for FetchFromAny. The engine
// derives it from knowledge it already holds: the per-peer key filters claim
// the key → PreferPeers; otherwise the cluster provably lacks it →
// PreferCDN. The unpreferred source is a failure fallback, never raced.
type FetchHint uint8

const (
	// PreferPeers tries connected peers first, cloud CDN on total peer
	// failure. The default for peer-origin fetches (NACK ingest, backfill)
	// and any fetch without key context.
	PreferPeers FetchHint = iota
	// PreferCDN tries the cloud CDN first, peers on failure. Chosen when no
	// peer's key filter claims the key — a cloud rehydrate of state the
	// cluster no longer holds.
	PreferCDN
)

// CloudEffect is one closure effect delivered inline by a GetTips response:
// the decrypted inner effect plus its marshaled proto length for cache
// accounting.
type CloudEffect struct {
	Tip      Tip
	Eff      *pb.Effect
	ProtoLen int
}

// CloudReader is the tiered-storage backstop: it reports the tip frontier that
// durable Cloud storage holds for a key. A key evicted from every live peer
// (e.g. its writer departed) may still live on Cloud, so a read that finds
// nothing cluster-wide asks Cloud for the frontier, installs it, and lets the
// cluster take over. Nil (unset) means no Cloud is configured and the read
// free-misses as before.
type CloudReader interface {
	// MayHold reports whether Cloud may hold key, answered from the pushed
	// key-name filter — free, no RPC. False is the filter's definite no and
	// lets a read free-miss without subscribing or consulting; true routes
	// the read through the subscribe + consult path, whose per-leaf
	// cloudConsulted marker caps the WAN round-trips.
	MayHold(key string) bool
	// CloudTips returns the tip frontier Cloud holds for key, or nil (with nil
	// error) if Cloud holds nothing for it. sidecar is the closure — every
	// effect reachable from those tips down to the LCA snapshot — delivered
	// inline by GetTips and installed into the effect cache before the tip
	// walk, so the walk runs locally instead of one WAN fetch per dep. The
	// sidecar may be partial (capped, or missing a blob the cloud is still
	// fetching back) — the walk stays the authority and pulls anything
	// missing on demand via FetchFromAny. Content-blind on the wire: the
	// implementation maps key to its Cloud PRF image and calls GetTips.
	CloudTips(ctx context.Context, key string) (tips []Tip, sidecar []CloudEffect, err error)
}

// PeerRTTProvider provides RTT measurements to peers for optimal leader selection.
type PeerRTTProvider interface {
	// GetRTT returns the estimated round-trip time to the given peer.
	// Returns 0 if the peer is unknown or RTT has not been measured.
	GetRTT(nodeID pb.NodeID) time.Duration
	// AlivePeerIDs returns the IDs of all peers that are currently alive.
	AlivePeerIDs() []pb.NodeID
}
