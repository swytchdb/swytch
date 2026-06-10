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
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/quic-go/quic-go"
	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// CDNFetcher can fetch effect data from a CDN / object storage endpoint.
type CDNFetcher interface {
	FetchFromCDN(ctx context.Context, offset *pb.EffectRef) ([]byte, error)
}

// PeerManager manages all peer connections and the local QUIC listener.
type PeerManager struct {
	config *ClusterConfig
	// selfID is pm.config.NodeID captured at construction time. UpdateTopology
	// swaps pm.config under pm.mu; callers that only need THIS node's ID
	// (which never changes for a given PeerManager) read selfID instead
	// to avoid the lock.
	selfID    NodeId
	peers     map[NodeId]*PeerConn
	listener  *quic.Listener
	handler   EffectHandler
	logReader LogReader
	lisAddr   net.Addr // actual listening address after binding
	serverTLS *tls.Config
	clientTLS *tls.Config

	// QUIC notification transport (replaces UDP + Noise + chunking + pacing)
	healthTable   *PeerHealthTable
	heartbeat     *HeartbeatManager
	quicTransport *QUICNotifyTransport
	replicator    *Replicator

	// CDN fetch for racing against peer fetch
	cdnFetcher CDNFetcher

	// Forward handler for adaptive serialization (§5)
	forwardHandler ForwardHandler

	onPeerAdded   func(NodeId)
	onPeerRemoved func(NodeId)

	// inboundConns tracks QUIC connections that peers dialed TO us. The
	// key is the peer's authoritative NodeID (learned from the 8-byte
	// sender prefix on the first uni-stream they open). We keep these
	// around because outbound entries in `peers` are keyed by the ID we
	// DIALED under — synthetic IDs during DNS bootstrap, or the real ID
	// once membership converges. When a peer sends us a subscription
	// (tagged with OUR real NodeID) and we need to ACK/NACK back to
	// THEIR real NodeID, the outbound `peers` map often has no entry
	// for that real ID; the dialer registered under a synthetic one.
	// Falling back to `inboundConns` uses the connection the peer
	// already established with us.
	inboundMu    sync.RWMutex
	inboundConns map[NodeId]*quic.Conn

	mu     xsync.RBMutex
	ctx    context.Context
	cancel context.CancelFunc
}

// NewPeerManager creates a new PeerManager from the cluster configuration.
func NewPeerManager(config *ClusterConfig, handler EffectHandler, logReader LogReader) (*PeerManager, error) {
	serverTLS, err := config.BuildTLSConfig()
	if err != nil {
		return nil, err
	}
	clientTLS, err := config.BuildClientTLSConfig()
	if err != nil {
		return nil, err
	}

	return &PeerManager{
		config:       config,
		selfID:       config.NodeID,
		peers:        make(map[NodeId]*PeerConn),
		handler:      handler,
		logReader:    logReader,
		serverTLS:    serverTLS,
		clientTLS:    clientTLS,
		inboundConns: make(map[NodeId]*quic.Conn),
	}, nil
}

// registerInboundConn records that a peer has dialed us, so outbound
// packets addressed to that peer's real NodeID can use the same
// connection rather than failing with ErrPeerUnavailable. Called from
// the notify transport's first-stream handler, which extracts the
// sender NodeID from the 8-byte wire header.
//
// Registrations for this node's own NodeID are ignored — a node never
// legitimately dials itself.
func (pm *PeerManager) registerInboundConn(peerID NodeId, conn *quic.Conn) (newPeer bool) {
	if peerID == pm.selfID {
		return false
	}
	pm.inboundMu.Lock()
	existing := pm.inboundConns[peerID]
	if existing == conn {
		pm.inboundMu.Unlock()
		return false
	}
	pm.inboundConns[peerID] = conn
	pm.inboundMu.Unlock()
	slog.Debug("inbound connection registered",
		"peer", peerID, "local_node_id", pm.selfID)
	return true
}

// forgetInboundConn removes inbound-map entries that reference the
// given connection. Called when the connection closes so stale
// entries don't shadow a fresh dial.
func (pm *PeerManager) forgetInboundConn(conn *quic.Conn) {
	pm.inboundMu.Lock()
	defer pm.inboundMu.Unlock()
	for id, c := range pm.inboundConns {
		if c == conn {
			delete(pm.inboundConns, id)
		}
	}
}

// inboundConnFor returns the inbound QUIC connection a peer has open
// to us, or nil if none is registered.
func (pm *PeerManager) inboundConnFor(peerID NodeId) *quic.Conn {
	pm.inboundMu.RLock()
	defer pm.inboundMu.RUnlock()
	return pm.inboundConns[peerID]
}

// Start launches the QUIC transport, heartbeat manager, QUIC listener, and dials all peers.
func (pm *PeerManager) Start(ctx context.Context) error {
	pm.ctx, pm.cancel = context.WithCancel(ctx)

	self := pm.config.Self()
	selfRegion := ""
	if self != nil {
		selfRegion = self.Region
	}

	if self != nil {
		pm.healthTable = NewPeerHealthTable()

		// Create QUIC notification transport. Connection lookup tries
		// the outbound map first (the one we dialed) and falls back to
		// the inbound map (one the peer dialed to us). The fallback is
		// what lets a node ACK/NACK back to a peer whose real NodeID
		// isn't (yet) in our outbound topology — see registerInboundConn
		// for the rationale.
		pm.quicTransport = NewQUICNotifyTransport(
			pm.config.NodeID,
			pm,
			func(peerID NodeId) *quic.Conn {
				token := pm.mu.RLock()
				pc := pm.peers[peerID]
				pm.mu.RUnlock(token)
				if pc != nil {
					if c := pc.GetQuicConn(); c != nil {
						return c
					}
				}
				return pm.inboundConnFor(peerID)
			},
			func(peerID NodeId, conn *quic.Conn) {
				if !pm.registerInboundConn(peerID, conn) {
					return
				}
				if pm.heartbeat != nil {
					pm.heartbeat.SendHeartbeatTo(peerID)
				}
			},
		)

		// Start heartbeat manager
		pm.heartbeat = NewHeartbeatManager(pm.config.NodeID, pm.healthTable)
		pm.heartbeat.Start(func(peerID NodeId, data []byte) error {
			return pm.quicTransport.SendDirect(peerID, data)
		})

		// Start replicator
		pm.replicator = NewReplicator(pm.config.NodeID, selfRegion, pm.healthTable, pm.quicTransport, pm.handler, pm.config.ReplicationTimeout)
		pm.replicator.Start()
	}

	// Start QUIC listener
	if err := pm.startServer(); err != nil {
		return err
	}

	// Connect to all peers. registerPeer fires lifecycle hooks that
	// may re-enter PeerManager via the QUIC connFunc (which RLocks
	// pm.mu), so it runs outside the critical section.
	peers := pm.config.Peers()
	pm.mu.Lock()
	for _, peer := range peers {
		peerID := peer.ID
		pc := newPeerConn(peer.ID, peer.Address, peer.Region, selfRegion, pm.handler, pm.clientTLS, func(conn *quic.Conn) {
			pm.acceptOutboundStreams(conn)
			if pm.heartbeat != nil {
				pm.heartbeat.SendHeartbeatTo(peerID)
			}
		})
		pm.peers[peer.ID] = pc
		pc.Start(pm.ctx)
	}
	pm.mu.Unlock()

	for _, peer := range peers {
		pm.registerPeer(peer)
	}

	slog.Info("peer manager started", "node_id", pm.config.NodeID, "peers", len(pm.config.Peers()))
	return nil
}

// WaitForAnyPeer blocks until at least one peer connection is ready for
// streaming, or the context is cancelled. Must be called after Start.
func (pm *PeerManager) WaitForAnyPeer(ctx context.Context) error {
	if len(pm.config.Peers()) == 0 {
		return nil
	}
	for {
		token := pm.mu.RLock()
		for _, pc := range pm.peers {
			if pc.streamReady.Load() {
				pm.mu.RUnlock(token)
				return nil
			}
		}
		pm.mu.RUnlock(token)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// registerPeer adds a peer to the heartbeat manager and replicator.
func (pm *PeerManager) registerPeer(peer NodeConfig) {
	if pm.heartbeat != nil {
		pm.heartbeat.AddPeer(peer.ID, peer.Region)
	}
	if pm.replicator != nil {
		pm.replicator.AddPeer(peer.ID, peer.Region)
	}
	if pm.onPeerAdded != nil {
		pm.onPeerAdded(peer.ID)
	}
}

// unregisterPeer removes a peer from the heartbeat manager and replicator.
func (pm *PeerManager) unregisterPeer(peerID NodeId) {
	if pm.heartbeat != nil {
		pm.heartbeat.RemovePeer(peerID)
	}
	if pm.replicator != nil {
		pm.replicator.RemovePeer(peerID)
	}
	if pm.onPeerRemoved != nil {
		pm.onPeerRemoved(peerID)
	}
}

// SetPeerLifecycleHooks registers callbacks fired when a peer is
// added or removed from the live peer set. Either may be nil. The
// pub/sub router uses these to re-announce subscriptions to new
// peers and to purge entries when peers leave.
func (pm *PeerManager) SetPeerLifecycleHooks(onAdded, onRemoved func(NodeId)) {
	pm.onPeerAdded = onAdded
	pm.onPeerRemoved = onRemoved
}

// Stop shuts down all components.
func (pm *PeerManager) Stop() {
	if pm.cancel != nil {
		pm.cancel()
	}

	if pm.replicator != nil {
		pm.replicator.Stop()
	}
	if pm.heartbeat != nil {
		pm.heartbeat.Stop()
	}
	if pm.quicTransport != nil {
		pm.quicTransport.Stop()
	}

	// Detach under the lock; run pc.Stop() (QUIC close) and fire
	// onPeerRemoved hooks outside it — both can block / re-enter
	// PeerManager via pm.mu.RLock.
	pm.mu.Lock()
	conns := make([]*PeerConn, 0, len(pm.peers))
	departing := make([]NodeId, 0, len(pm.peers))
	for id, pc := range pm.peers {
		conns = append(conns, pc)
		departing = append(departing, id)
		delete(pm.peers, id)
	}
	pm.mu.Unlock()

	for _, pc := range conns {
		pc.Stop()
	}
	if pm.onPeerRemoved != nil {
		for _, id := range departing {
			pm.onPeerRemoved(id)
		}
	}

	if pm.listener != nil {
		if err := pm.listener.Close(); err != nil {
			slog.Error("failed to close QUIC listener", "error", err)
		}
	}

	slog.Info("peer manager stopped", "node_id", pm.config.NodeID)
}

// Broadcast sends an OffsetNotify to all connected peers. Fire-and-forget.
// HealthTable returns the peer health table, or nil if UDP/Noise is not configured.
func (pm *PeerManager) HealthTable() *PeerHealthTable {
	return pm.healthTable
}

// SendOneWayTo dispatches a single OffsetNotify to one peer without
// retransmits or ACK tracking. Used by the pub/sub router for fire-
// and-forget ephemeral effects.
func (pm *PeerManager) SendOneWayTo(notify *pb.OffsetNotify, wireData []byte, targetNodeID NodeId) error {
	if pm.replicator == nil {
		return fmt.Errorf("replicator not initialized")
	}
	return pm.replicator.SendOneWay(notify, wireData, targetNodeID)
}

func (pm *PeerManager) Broadcast(notify *pb.OffsetNotify) {
	pm.BroadcastWithData(notify, nil)
}

// BroadcastWithData sends an OffsetNotify to all connected peers.
// Routes through the replicator (UDP) when available.
func (pm *PeerManager) BroadcastWithData(notify *pb.OffsetNotify, effectData []byte) {
	if pm.replicator != nil {
		pm.replicator.Replicate(notify, effectData)
		return
	}
	slog.Warn("BroadcastWithData called without replicator — notifications not sent")
}

// Replicate sends the notification using first-ACK semantics for same-region peers.
// Blocks until at least one same-region peer ACKs or the replication timeout expires.
// Returns nil on success, or an error if replication failed (no peers, timeout, etc.).
// If the replicator is not set up (no UDP transport), falls back to BroadcastWithData.
func (pm *PeerManager) Replicate(notify *pb.OffsetNotify, wireData []byte) error {
	remoteCtx := tracing.ExtractFromBytes(notify.GetTraceContext())
	_, waitSpan := tracing.Tracer().Start(remoteCtx, "replication.wait_ack",
		trace.WithAttributes(attribute.String("effect.key", string(notify.GetKey()))))
	defer waitSpan.End()

	future := pm.replicator.Replicate(notify, wireData)
	return future.Wait()
}

// AllRegionPeersReachable returns true if every same-region peer is alive
// and has a verified symmetric path. Returns true in standalone mode.
func (pm *PeerManager) AllRegionPeersReachable() bool {
	if pm.replicator != nil {
		return pm.replicator.AllRegionPeersReachable()
	}
	return true
}

// InMajorityPartition returns true if this node can reach a strict majority
// of same-region nodes. Returns true in standalone mode.
func (pm *PeerManager) InMajorityPartition() bool {
	if pm.replicator != nil {
		return pm.replicator.InMajorityPartition()
	}
	return true
}

// PeerIDs returns the IDs of all configured peers.
func (pm *PeerManager) PeerIDs() []NodeId {
	token := pm.mu.RLock()
	defer pm.mu.RUnlock(token)

	ids := make([]NodeId, 0, len(pm.peers))
	for id := range pm.peers {
		ids = append(ids, id)
	}
	return ids
}

// SetForwardHandler sets the handler for forwarded transactions (adaptive serialization).
// Must be called before any forwarded transactions are received.
func (pm *PeerManager) SetForwardHandler(h ForwardHandler) {
	pm.forwardHandler = h
}

// ForwardTransaction sends a transaction to a specific peer for execution.
// Used by adaptive serialization to route operations through the leader.
func (pm *PeerManager) ForwardTransaction(ctx context.Context, targetNodeID NodeId, tx *pb.ForwardedTransaction) (*pb.ForwardedResponse, error) {
	token := pm.mu.RLock()
	pc := pm.peers[targetNodeID]
	pm.mu.RUnlock(token)

	if pc == nil {
		return nil, ErrPeerUnavailable
	}

	return pc.ForwardTransaction(ctx, tx)
}

// SetCDNFetcher sets the CDN fetcher for racing CDN against peer fetch.
func (pm *PeerManager) SetCDNFetcher(f CDNFetcher) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.cdnFetcher = f
}

// FetchFromAny tries to fetch effect bytes from any connected peer,
// racing against CDN fetch if available. First successful result wins.
func (pm *PeerManager) FetchFromAny(offset *pb.EffectRef) ([]byte, error) {
	token := pm.mu.RLock()
	cdnFetcher := pm.cdnFetcher
	pm.mu.RUnlock(token)

	conns := pm.fetchConns()

	// No CDN fetcher: peer fetch only (original behavior). No outer race to
	// honor, so root the fetch in pm.ctx (lives for the manager's lifetime).
	if cdnFetcher == nil {
		return pm.fetchFromConns(pm.ctx, offset, conns)
	}

	// Race CDN against peer fetch
	ctx, cancel := context.WithTimeout(pm.ctx, 10*time.Second)
	defer cancel()

	type result struct {
		data []byte
		err  error
	}
	ch := make(chan result, 2)

	// Path 1: CDN fetch
	go func() {
		data, err := cdnFetcher.FetchFromCDN(ctx, offset)
		ch <- result{data, err}
	}()

	// Path 2: Peer fetch — share the race ctx so peer fetches cancel the
	// moment CDN wins (or the 10s timeout fires).
	go func() {
		data, err := pm.fetchFromConns(ctx, offset, conns)
		ch <- result{data, err}
	}()

	// First success wins
	var lastErr error
	for range 2 {
		select {
		case r := <-ch:
			if r.err == nil && len(r.data) > 0 {
				return r.data, nil
			}
			lastErr = r.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, ErrPeerUnavailable
}

// fetchConns returns every distinct QUIC connection we can fetch over: the
// outbound connections we dialed (pm.peers) plus the inbound connections peers
// dialed to us (pm.inboundConns). Deduped by connection pointer so a peer we
// hold both ways is tried once. Including inbound connections is what lets us
// pull an effect from a node we never dialed — e.g. a freshly-arrived transient
// holding a dep no one else has yet. Without it, fetch is blind to exactly the
// peers the send path already reaches via the transport's inbound fallback.
func (pm *PeerManager) fetchConns() []*quic.Conn {
	token := pm.mu.RLock()
	pcs := make([]*PeerConn, 0, len(pm.peers))
	for _, pc := range pm.peers {
		pcs = append(pcs, pc)
	}
	pm.mu.RUnlock(token)

	seen := make(map[*quic.Conn]struct{})
	conns := make([]*quic.Conn, 0, len(pcs))
	add := func(c *quic.Conn) {
		if c == nil {
			return
		}
		if _, ok := seen[c]; ok {
			return
		}
		seen[c] = struct{}{}
		conns = append(conns, c)
	}
	for _, pc := range pcs {
		add(pc.GetQuicConn())
	}

	pm.inboundMu.RLock()
	for _, c := range pm.inboundConns {
		add(c)
	}
	pm.inboundMu.RUnlock()

	return conns
}

// fetchFromConns fetches the offset over every connection in parallel,
// returning the first successful result and cancelling the rest.
func (pm *PeerManager) fetchFromConns(ctx context.Context, offset *pb.EffectRef, conns []*quic.Conn) ([]byte, error) {
	if len(conns) == 0 {
		return nil, ErrPeerUnavailable
	}

	type result struct {
		data []byte
		err  error
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch := make(chan result, len(conns))
	for _, c := range conns {
		go func(c *quic.Conn) {
			data, err := fetchOverConn(ctx, c, offset)
			if err != nil {
				ch <- result{nil, err}
				return
			}
			if len(data) == 0 {
				ch <- result{nil, errFetchNotFound}
				return
			}
			ch <- result{data, nil}
		}(c)
	}

	var lastErr error
	for range conns {
		r := <-ch
		if r.err != nil {
			lastErr = r.err
			continue
		}
		return r.data, nil
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, ErrPeerUnavailable
}

// Fetch retrieves effect bytes from a specific peer by slot and offset.
func (pm *PeerManager) Fetch(ref *pb.EffectRef) ([]byte, error) {
	nodeID := NodeId(ref.NodeId)
	token := pm.mu.RLock()
	pc := pm.peers[nodeID]
	pm.mu.RUnlock(token)

	if pc == nil {
		return nil, ErrPeerUnavailable
	}

	data, err := pc.Fetch(pm.ctx, ref)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, errFetchNotFound
	}
	return data, nil
}

// UpdateTopology applies a new cluster configuration, connecting to new peers
// and disconnecting removed ones.
func (pm *PeerManager) UpdateTopology(newCfg *ClusterConfig) {
	if newCfg == nil {
		return
	}

	diff := DiffTopology(pm.config, newCfg)

	selfRegion := ""
	if self := newCfg.Self(); self != nil {
		selfRegion = self.Region
	}

	connect := func(n NodeConfig) *PeerConn {
		peerID := n.ID
		return newPeerConn(n.ID, n.Address, n.Region, selfRegion, pm.handler, pm.clientTLS, func(conn *quic.Conn) {
			pm.acceptOutboundStreams(conn)
			if pm.heartbeat != nil {
				pm.heartbeat.SendHeartbeatTo(peerID)
			}
		})
	}

	// pm.mu only protects pm.peers and pm.config. Detach departing
	// PeerConns under the lock; Stop() them (which closes QUIC
	// connections and can block) after Unlock. register/unregister
	// also run after Unlock — they fire lifecycle hooks that re-enter
	// PeerManager via the QUIC connFunc (which RLocks pm.mu).
	pm.mu.Lock()

	stops := make([]*PeerConn, 0, len(diff.Removed)+len(diff.Changed))

	for _, removed := range diff.Removed {
		if pc, ok := pm.peers[removed.ID]; ok {
			slog.Info("disconnecting removed peer", "peer", removed.ID, "address", removed.Address)
			stops = append(stops, pc)
			delete(pm.peers, removed.ID)
		}
	}

	for _, added := range diff.Added {
		slog.Info("connecting to new peer", "peer", added.ID, "address", added.Address, "region", added.Region)
		pc := connect(added)
		pm.peers[added.ID] = pc
		pc.Start(pm.ctx)
	}

	for _, changed := range diff.Changed {
		if pc, ok := pm.peers[changed.ID]; ok {
			slog.Info("reconnecting peer with new config", "peer", changed.ID, "old_address", pc.addr, "new_address", changed.Address)
			stops = append(stops, pc)
			delete(pm.peers, changed.ID)
		}
		pc := connect(changed)
		pm.peers[changed.ID] = pc
		pc.Start(pm.ctx)
	}

	pm.config = newCfg

	if pm.healthTable != nil {
		known := make(map[NodeId]struct{}, len(newCfg.Nodes))
		for _, n := range newCfg.Nodes {
			known[n.ID] = struct{}{}
		}
		pm.healthTable.PruneUnknownPeers(known)
	}

	pm.mu.Unlock()

	for _, pc := range stops {
		pc.Stop()
	}

	for _, removed := range diff.Removed {
		pm.unregisterPeer(removed.ID)
	}
	for _, added := range diff.Added {
		pm.registerPeer(added)
	}
	for _, changed := range diff.Changed {
		pm.unregisterPeer(changed.ID)
		pm.registerPeer(changed)
	}
}

// WaitForPeer blocks until the given peer's stream is ready or context is cancelled.
func (pm *PeerManager) WaitForPeer(ctx context.Context, nodeID NodeId) bool {
	for {
		token := pm.mu.RLock()
		pc := pm.peers[nodeID]
		pm.mu.RUnlock(token)

		if pc != nil && pc.IsStreamReady() {
			return true
		}

		select {
		case <-ctx.Done():
			return false
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// ListenAddr returns the actual address the QUIC listener is bound to.
// Useful when binding to port 0 in tests.
func (pm *PeerManager) ListenAddr() string {
	if pm.lisAddr != nil {
		return pm.lisAddr.String()
	}
	return ""
}

// --- PacketHandler implementation (bridges QUIC events to heartbeat + replicator) ---

// HandleHeartbeat routes inbound heartbeats to the heartbeat manager.
func (pm *PeerManager) HandleHeartbeat(peerID NodeId, timestamp uint64) {
	if pm.heartbeat != nil {
		pm.heartbeat.ProcessInbound(peerID, timestamp)
	}
}

// HandleHeartbeatACK routes inbound heartbeat ACKs to the heartbeat manager.
func (pm *PeerManager) HandleHeartbeatACK(peerID NodeId, timestamp uint64) {
	if pm.heartbeat != nil {
		pm.heartbeat.ProcessInboundACK(peerID, timestamp)
	}
}

// HandleNotify routes inbound notifications to the replicator.
func (pm *PeerManager) HandleNotify(peerID NodeId, requestID uint64, notify *pb.OffsetNotify) {
	if pm.replicator != nil {
		pm.replicator.HandleNotify(peerID, requestID, notify)
	}
}

// HandleNotifyACK routes inbound ACKs to the replicator.
func (pm *PeerManager) HandleNotifyACK(peerID NodeId, requestID uint64, status byte, credits uint32) {
	if pm.replicator != nil {
		pm.replicator.HandleNotifyACK(peerID, requestID, status, credits)
	}
}

func (pm *PeerManager) HandleNotifyACKWithData(peerID NodeId, requestID uint64, status byte, credits uint32, fullPacket []byte) {
	if pm.replicator != nil {
		pm.replicator.HandleNotifyACKWithData(peerID, requestID, status, credits, fullPacket)
	}
}

// HandleNackNotify routes inbound NACKs to the effect handler.
func (pm *PeerManager) HandleNackNotify(peerID NodeId, nack *pb.NackNotify) {
	if pm.handler != nil {
		if err := pm.handler.HandleNack(nack); err != nil {
			slog.Debug("failed to handle NACK", "peer", peerID, "error", err)
		}
	}
}

// ReplicateTo sends a notification to a specific peer and waits for ACK or NACK.
// Returns NackNotify slice on conflict, nil on ACK.
func (pm *PeerManager) ReplicateTo(notify *pb.OffsetNotify, wireData []byte, targetNodeID NodeId) ([]*pb.NackNotify, error) {
	if pm.replicator != nil {
		return pm.replicator.ReplicateTo(notify, wireData, targetNodeID)
	}
	return nil, nil
}

// ReplicateMarshalled sends a pre-marshalled notify body to a peer (see
// Replicator.ReplicateMarshalled). Used by the bind fan-out so the body is
// marshalled once for all subscribers.
func (pm *PeerManager) ReplicateMarshalled(notify *pb.OffsetNotify, notifyBody []byte, targetNodeID NodeId) ([]*pb.NackNotify, error) {
	if pm.replicator != nil {
		return pm.replicator.ReplicateMarshalled(notify, notifyBody, targetNodeID)
	}
	return nil, nil
}

// SendNack sends an enriched NACK to a specific peer.
func (pm *PeerManager) SendNack(nack *pb.NackNotify, targetNodeID NodeId) {
	if pm.replicator != nil {
		pm.replicator.SendNack(nack, targetNodeID)
	}
}

// startServer creates and starts the QUIC listener on the local node's cluster address.
func (pm *PeerManager) startServer() error {
	self := pm.config.Self()
	if self == nil {
		slog.Warn("no self entry in cluster config, skipping QUIC listener")
		return nil
	}

	tlsCfg := pm.serverTLS
	if tlsCfg == nil {
		return fmt.Errorf("TLS configuration is required for cluster mode")
	}
	// QUIC requires ALPN
	tlsCfg = tlsCfg.Clone()
	tlsCfg.NextProtos = []string{"swytch-cluster"}

	listener, err := quic.ListenAddr(self.Address, tlsCfg, clusterQUICConfig())
	if err != nil {
		return err
	}
	pm.listener = listener
	pm.lisAddr = listener.Addr()

	go pm.acceptLoop()

	slog.Info("QUIC listener started", "address", pm.lisAddr.String())
	return nil
}

// acceptLoop accepts incoming QUIC connections and dispatches their streams.
func (pm *PeerManager) acceptLoop() {
	for {
		conn, err := pm.listener.Accept(pm.ctx)
		if err != nil {
			if pm.ctx.Err() != nil {
				return // shutting down
			}
			slog.Error("failed to accept QUIC connection", "error", err)
			continue
		}
		pm.dispatchConnStreams(conn)
		// Clean up the inbound-connection map when this conn closes so a
		// later reconnect from the same peer isn't shadowed by a stale
		// entry pointing at a defunct *quic.Conn.
		go func(c *quic.Conn) {
			<-c.Context().Done()
			pm.forgetInboundConn(c)
		}(conn)
	}
}

// dispatchConnStreams spawns the two stream-acceptor goroutines on a
// QUIC connection — bidirectional streams (fetch, forward) and
// unidirectional streams (notify, heartbeat, ACK/NACK). Called for
// every connection we participate in, regardless of who dialed: QUIC
// is symmetric so return traffic can arrive on either side's conn,
// and silently dropping streams on our outbound conns is what
// originally hung the session-init path (peer B's NACK to node A
// addressed to A's real NodeID routed onto B's inbound-from-A conn,
// which is A's outbound-to-B from A's POV — and nobody on A was
// calling AcceptUniStream on it).
func (pm *PeerManager) dispatchConnStreams(conn *quic.Conn) {
	go acceptStreams(conn, pm.logReader, pm.forwardHandler)
	if pm.quicTransport != nil {
		go pm.quicTransport.AcceptUniStreams(conn)
	}
}

// acceptOutboundStreams is the callback PeerConn fires after a
// successful dial. It wires the accept loops onto our dialed
// connection so the peer can open return streams (e.g. NACKs
// routed via their own inboundConns fallback to our outbound
// conn).
func (pm *PeerManager) acceptOutboundStreams(conn *quic.Conn) {
	pm.dispatchConnStreams(conn)
}
