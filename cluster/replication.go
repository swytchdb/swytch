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
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	pb "github.com/swytchdb/cache/cluster/proto"
	"github.com/swytchdb/cache/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

// Sentinel errors for replication failures.
var (
	ErrNoPeers            = errors.New("no alive symmetric peers available for replication")
	ErrReplicationTimeout = errors.New("replication timed out waiting for ACK")
)

// Retransmission constants.
const (
	sweepInterval = 250 * time.Millisecond // sweep tick rate — no point checking faster than rtoFloor/2
)

// ReplicationFuture represents a pending replication operation.
// It resolves when the first same-region peer ACKs or when the deadline expires.
// Callers check Wait()'s return value to distinguish success from failure.
type ReplicationFuture struct {
	done  chan struct{}
	once  sync.Once
	err   atomic.Pointer[error]
	nacks atomic.Pointer[[]*pb.NackNotify] // NACKs from NACK response (status=0x01)
}

// Wait blocks until the replication future is resolved and returns any error.
func (f *ReplicationFuture) Wait() error {
	<-f.done
	return f.Err()
}

// Done returns a channel that is closed when the future resolves.
func (f *ReplicationFuture) Done() <-chan struct{} {
	return f.done
}

// Err returns the error stored in the future, or nil on success.
func (f *ReplicationFuture) Err() error {
	if p := f.err.Load(); p != nil {
		return *p
	}
	return nil
}

// Nacks returns the NACKs attached to this future, or nil.
func (f *ReplicationFuture) Nacks() []*pb.NackNotify {
	if p := f.nacks.Load(); p != nil {
		return *p
	}
	return nil
}

// resolve closes the future successfully. First call wins; subsequent calls are no-ops.
func (f *ReplicationFuture) resolve() {
	f.once.Do(func() {
		close(f.done)
	})
}

// reject stores an error then closes the future. First call wins.
func (f *ReplicationFuture) reject(err error) {
	f.err.CompareAndSwap(nil, &err)
	f.resolve()
}

// rejectedFuture returns a pre-rejected future with the given error.
func rejectedFuture(err error) *ReplicationFuture {
	f := &ReplicationFuture{done: make(chan struct{})}
	f.reject(err)
	return f
}

// requestTracker maps pending request IDs to their futures.
type requestTracker struct {
	pending *xsync.Map[uint64, *trackedRequest]
	nextID  atomic.Uint64
}

type trackedRequest struct {
	future       *ReplicationFuture
	createdAt    int64 // UnixNano
	lastSentAt   int64 // UnixNano of most recent send (original or retransmit)
	deadline     int64 // UnixNano — absolute deadline for this request
	packetSize   int   // bytes sent (for metrics)
	peerID       NodeId
	packetData   []byte           // serialized plaintext for retransmission
	nacks        []*pb.NackNotify // NACKs received in a NACK response (status=0x01)
	traceContext []byte           // OTel trace context for correlating ACK spans
}

// Register creates a new tracked request and returns its ID and future.
func (rt *requestTracker) Register(peerID NodeId, packetSize int) (uint64, *ReplicationFuture) {
	id := rt.nextID.Add(1)
	f := &ReplicationFuture{done: make(chan struct{})}
	rt.pending.Store(id, &trackedRequest{
		future:     f,
		createdAt:  time.Now().UnixNano(),
		packetSize: packetSize,
		peerID:     peerID,
	})
	return id, f
}

// Resolve resolves the future for the given request ID. First call wins.
// Returns the tracked request if found, nil otherwise.
func (rt *requestTracker) Resolve(requestID uint64) *trackedRequest {
	if tr, ok := rt.pending.LoadAndDelete(requestID); ok {
		tr.future.resolve()
		return tr
	}
	return nil
}

// SweepDeadlines checks pending requests and rejects any that have exceeded
// their deadline. QUIC handles all retransmission at the transport layer —
// no application-level retransmit needed.
func (rt *requestTracker) SweepDeadlines() {
	nowNano := time.Now().UnixNano()

	rt.pending.Range(func(key uint64, tr *trackedRequest) bool {
		if nowNano > tr.deadline {
			rt.pending.Delete(key)
			tr.future.reject(ErrReplicationTimeout)
			RecordRetransmissionGiveUp(tr.peerID)
		}
		return true
	})
}

// Replicator manages replication with first-ACK semantics for same-region
// and fire-and-forget for cross-region peers. Uses QUIC stream-per-message transport.
type Replicator struct {
	nodeID             NodeId
	localRegion        string
	replicationTimeout time.Duration

	healthTable *PeerHealthTable
	transport   *QUICNotifyTransport
	tracker     requestTracker

	mu          xsync.RBMutex
	peerRegions map[NodeId]string

	handler EffectHandler // for handling inbound notifications

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewReplicator creates a new Replicator.
func NewReplicator(
	nodeID NodeId,
	localRegion string,
	healthTable *PeerHealthTable,
	transport *QUICNotifyTransport,
	handler EffectHandler,
	replicationTimeout time.Duration,
) *Replicator {
	if replicationTimeout <= 0 {
		replicationTimeout = 30 * time.Second
	}
	return &Replicator{
		nodeID:             nodeID,
		localRegion:        localRegion,
		replicationTimeout: replicationTimeout,
		healthTable:        healthTable,
		transport:          transport,
		handler:            handler,
		tracker:            requestTracker{pending: xsync.NewMap[uint64, *trackedRequest]()},
		peerRegions:        make(map[NodeId]string),
		stopCh:             make(chan struct{}),
	}
}

// Start begins the timeout sweep loop.
func (r *Replicator) Start() {
	r.wg.Add(1)
	go r.sweepLoop()
}

// Stop shuts down the replicator.
func (r *Replicator) Stop() {
	close(r.stopCh)
	r.wg.Wait()
}

// AddPeer registers a peer for replication.
func (r *Replicator) AddPeer(nodeID NodeId, region string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peerRegions[nodeID] = region
}

// RemovePeer unregisters a peer.
func (r *Replicator) RemovePeer(nodeID NodeId) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.peerRegions, nodeID)
}

// AllRegionPeersReachable returns true if every same-region peer is alive
// and has a verified symmetric path.
func (r *Replicator) AllRegionPeersReachable() bool {
	token := r.mu.RLock()
	regions := make(map[NodeId]string, len(r.peerRegions))
	maps.Copy(regions, r.peerRegions)
	r.mu.RUnlock(token)
	return r.healthTable.AllRegionPeersReachable(r.localRegion, regions)
}

// InMajorityPartition returns true if this node can reach a strict majority
// of same-region nodes (including itself).
func (r *Replicator) InMajorityPartition() bool {
	token := r.mu.RLock()
	regions := make(map[NodeId]string, len(r.peerRegions))
	maps.Copy(regions, r.peerRegions)
	r.mu.RUnlock(token)
	return r.healthTable.InMajorityPartition(r.localRegion, regions)
}

// Replicate sends a notification to all peers with first-ACK semantics for same-region.
// Returns a future that resolves when the first same-region ACK arrives, or rejects
// with ErrNoPeers if there are no alive+symmetric same-region peers.
// Cross-region peers receive the notification fire-and-forget.
func (r *Replicator) Replicate(notify *pb.OffsetNotify, wireData []byte) *ReplicationFuture {
	// Extract trace context from the notification for a child span
	remoteCtx := tracing.ExtractFromBytes(notify.GetTraceContext())
	_, sendSpan := tracing.Tracer().Start(remoteCtx, "replication.send",
		trace.WithAttributes(attribute.String("effect.key", string(notify.GetKey()))))
	defer sendSpan.End()

	token := r.mu.RLock()
	regions := make(map[NodeId]string, len(r.peerRegions))
	maps.Copy(regions, r.peerRegions)
	r.mu.RUnlock(token)

	// Find alive+symmetric same-region peers for tracked replication
	targets := r.healthTable.AliveSymmetricPeers(r.localRegion, regions)

	// Send to cross-region peers (fire-and-forget, off critical path)
	go r.sendCrossRegion(notify, wireData, regions)

	// Also fire-and-forget to dead same-region peers so they receive
	// effects when the partition heals — don't wait for ACKs from these.
	go r.sendToDeadPeers(notify, wireData, regions, targets)

	// No alive same-region targets — fail immediately
	if len(targets) == 0 {
		return rejectedFuture(ErrNoPeers)
	}

	// Same-region peers get the full OffsetNotify with effect_data inline
	_, cloneSpan := tracing.Tracer().Start(remoteCtx, "replication.marshal")
	fullNotify := proto.Clone(notify).(*pb.OffsetNotify)
	fullNotify.EffectData = wireData
	cloneSpan.End()

	// One tracked request per peer; future resolves on FIRST ACK (first-ACK semantics).
	future := &ReplicationFuture{done: make(chan struct{})}

	trackedCount := 0
	deadlineNano := time.Now().Add(r.replicationTimeout).UnixNano()

	for _, targetID := range targets {
		requestID := r.tracker.nextID.Add(1)

		notifyPkt, err := MarshalNotifyPacket(requestID, fullNotify)
		if err != nil {
			slog.Error("failed to marshal notify packet", "error", err)
			continue
		}

		_, transportSpan := tracing.Tracer().Start(remoteCtx, "replication.transport_send",
			trace.WithAttributes(attribute.Int("peer.id", int(targetID))))
		wireSize, err := r.transport.Send(targetID, notifyPkt)
		transportSpan.End()
		if err != nil {
			slog.Error("same-region send failed, replication degraded",
				"peer", targetID, "error", err)
			continue
		}

		// Track for ACK or retransmission
		now := time.Now().UnixNano()
		tr := &trackedRequest{
			future:       future,
			createdAt:    now,
			lastSentAt:   now,
			deadline:     deadlineNano,
			peerID:       targetID,
			packetSize:   wireSize,
			packetData:   notifyPkt,
			traceContext: notify.GetTraceContext(),
		}
		r.tracker.pending.Store(requestID, tr)
		RecordNotificationSent()
		trackedCount++
	}

	if trackedCount == 0 {
		slog.Error("all same-region sends failed", "targets", len(targets))
		future.reject(ErrNoPeers)
	}

	return future
}

// sendToDeadPeers sends fire-and-forget notifications to same-region peers
// that are currently dead. This ensures they receive effects when the
// partition heals, without blocking the caller or tracking ACKs.
func (r *Replicator) sendToDeadPeers(notify *pb.OffsetNotify, wireData []byte, regions map[NodeId]string, aliveTargets []NodeId) {
	allPeers := r.healthTable.AllSameRegionPeers(r.localRegion, regions)

	aliveSet := make(map[NodeId]bool, len(aliveTargets))
	for _, id := range aliveTargets {
		aliveSet[id] = true
	}

	var fullNotify *pb.OffsetNotify

	for _, peerID := range allPeers {
		if aliveSet[peerID] {
			continue
		}

		if fullNotify == nil {
			fullNotify = proto.Clone(notify).(*pb.OffsetNotify)
			fullNotify.EffectData = wireData
		}

		requestID := r.tracker.nextID.Add(1)
		notifyPkt, err := MarshalNotifyPacket(requestID, fullNotify)
		if err != nil {
			continue
		}

		// Fire and forget
		_, _ = r.transport.Send(peerID, notifyPkt)
	}
}

// sendCrossRegion sends the notification to all cross-region peers.
// Fire-and-forget — callers don't block on cross-region ACKs.
func (r *Replicator) sendCrossRegion(notify *pb.OffsetNotify, wireData []byte, regions map[NodeId]string) {
	var fullNotify *pb.OffsetNotify
	var nullFuture *ReplicationFuture
	deadlineNano := time.Now().Add(r.replicationTimeout).UnixNano()

	for peerID, region := range regions {
		if region == r.localRegion {
			continue
		}

		if fullNotify == nil {
			fullNotify = proto.Clone(notify).(*pb.OffsetNotify)
			fullNotify.EffectData = wireData
			nullFuture = &ReplicationFuture{done: make(chan struct{})}
			nullFuture.resolve()
		}

		requestID := r.tracker.nextID.Add(1)

		notifyPkt, err := MarshalNotifyPacket(requestID, fullNotify)
		if err != nil {
			slog.Error("failed to marshal cross-region notify packet", "error", err)
			continue
		}

		wireSize, err := r.transport.Send(peerID, notifyPkt)
		if err != nil {
			slog.Error("cross-region send failed, notification dropped",
				"peer", peerID, "error", err)
			continue
		}

		now := time.Now().UnixNano()
		tr := &trackedRequest{
			future:     nullFuture,
			createdAt:  now,
			lastSentAt: now,
			deadline:   deadlineNano,
			peerID:     peerID,
			packetSize: wireSize,
			packetData: notifyPkt,
		}
		r.tracker.pending.Store(requestID, tr)
		RecordNotificationSent()
	}
}

// HandleNotify processes an inbound notification from a peer.
// It applies the effect and sends an ACK or NACK response back via QUIC.
func (r *Replicator) HandleNotify(peerID NodeId, requestID uint64, notify *pb.OffsetNotify) {
	// Create a child span from the remote trace context so it appears
	// under flush.broadcast in the originator's trace tree.
	remoteCtx := tracing.ExtractFromBytes(notify.GetTraceContext())
	_, recvSpan := tracing.Tracer().Start(remoteCtx, "replication.receive",
		trace.WithAttributes(
			attribute.Int("peer.id", int(peerID)),
			attribute.Int64("request.id", int64(requestID)),
		))
	defer recvSpan.End()

	slog.Debug("HandleNotify: received",
		"peer", peerID, "requestID", requestID,
		"key", string(notify.GetKey()),
		"offset", notify.GetOrigin().GetOffset())

	RecordNotificationReceived()

	var nacks []*pb.NackNotify
	if r.handler != nil && notify != nil {
		var err error
		nacks, err = r.handler.HandleOffsetNotify(notify)
		if err != nil {
			slog.Debug("HandleNotify: handler error",
				"peer", peerID, "requestID", requestID,
				"error", err,
			)
		}
	}

	// Credits field kept for wire compatibility but QUIC handles flow control
	credits := uint32(0)

	if len(nacks) > 0 {
		nackPkt := MarshalNotifyNACKPacket(r.nodeID, requestID, nacks, credits)
		if err := r.transport.SendDirect(peerID, nackPkt); err != nil {
			slog.Debug("HandleNotify: failed to send NACK",
				"peer", peerID, "requestID", requestID, "error", err)
		} else {
			slog.Debug("HandleNotify: sent NACK",
				"peer", peerID, "requestID", requestID, "nacks", len(nacks))
		}
	} else {
		ackPkt := MarshalNotifyACKPacket(r.nodeID, requestID, 0x00, credits)
		if err := r.transport.SendDirect(peerID, ackPkt); err != nil {
			slog.Debug("HandleNotify: failed to send ACK",
				"peer", peerID, "requestID", requestID, "error", err)
		} else {
			slog.Debug("HandleNotify: sent ACK",
				"peer", peerID, "requestID", requestID)
		}
	}
}

// HandleNotifyACK processes an inbound ACK from a peer.
func (r *Replicator) HandleNotifyACK(peerID NodeId, requestID uint64, status byte, credits uint32) {
	tr := r.tracker.Resolve(requestID)
	if tr == nil {
		slog.Debug("HandleNotifyACK: late ACK (already timed out or resolved)",
			"peer", peerID, "requestID", requestID)
		return
	}

	// Create span as child of the original replication trace
	traceCtx := tracing.ExtractFromBytes(tr.traceContext)
	_, ackSpan := tracing.Tracer().Start(traceCtx, "replication.ack_received",
		trace.WithAttributes(
			attribute.Int("peer.id", int(peerID)),
			attribute.Int64("request.id", int64(requestID)),
		))

	now := time.Now()
	latencyMs := float64(now.UnixNano()-tr.createdAt) / 1e6
	ackSpan.SetAttributes(attribute.Float64("rtt_ms", latencyMs))
	ackSpan.End()

	RecordUDPNotifyACKLatency(latencyMs)

	slog.Debug("HandleNotifyACK: received ACK",
		"peer", peerID, "requestID", requestID,
		"rtt_ms", latencyMs)
}

// HandleNotifyACKWithData processes the full ACK/NACK packet including payload.
// Called from dispatch when status=0x01 to extract embedded NACKs.
func (r *Replicator) HandleNotifyACKWithData(peerID NodeId, requestID uint64, status byte, credits uint32, fullPacket []byte) {
	tr, ok := r.tracker.pending.LoadAndDelete(requestID)
	if !ok {
		slog.Debug("HandleNotifyACKWithData: late NACK (already timed out or resolved)",
			"peer", peerID, "requestID", requestID)
		return
	}

	now := time.Now()
	latencyMs := float64(now.UnixNano()-tr.createdAt) / 1e6
	RecordUDPNotifyACKLatency(latencyMs)

	slog.Debug("HandleNotifyACKWithData: received NACK",
		"peer", peerID, "requestID", requestID,
		"rtt_ms", latencyMs)

	// Parse and store NACKs BEFORE resolving the future so callers
	// of Wait()/Nacks() see them immediately.
	if status == 0x01 && len(fullPacket) > 22 {
		nacks, err := parseNotifyNACKPayload(fullPacket[22:])
		if err != nil {
			slog.Debug("failed to parse NACK payload", "peer", peerID, "error", err)
		}
		if len(nacks) > 0 {
			tr.future.nacks.Store(&nacks)
		}
	}

	tr.future.resolve()
}

// ReplicateTo sends a notification to a specific peer and waits for ACK or NACK.
// Used by transactional bind to get deliberate per-subscriber responses.
func (r *Replicator) ReplicateTo(notify *pb.OffsetNotify, wireData []byte, targetPeerID NodeId) ([]*pb.NackNotify, error) {
	fullNotify := proto.Clone(notify).(*pb.OffsetNotify)
	fullNotify.EffectData = wireData

	requestID, future := r.tracker.Register(targetPeerID, 0)

	notifyPkt, err := MarshalNotifyPacket(requestID, fullNotify)
	if err != nil {
		return nil, fmt.Errorf("marshal notify packet: %w", err)
	}

	wireSize, err := r.transport.Send(targetPeerID, notifyPkt)
	if err != nil {
		return nil, fmt.Errorf("send to peer %d: %w", targetPeerID, err)
	}

	// Set tracked request details for retransmission
	now := time.Now().UnixNano()
	deadlineNano := time.Now().Add(r.replicationTimeout).UnixNano()
	if tr, ok := r.tracker.pending.Load(requestID); ok {
		tr.lastSentAt = now
		tr.deadline = deadlineNano
		tr.packetSize = wireSize
		tr.packetData = notifyPkt
	}

	RecordNotificationSent()

	// Wait for ACK or NACK response
	if err := future.Wait(); err != nil {
		if errors.Is(err, ErrReplicationTimeout) {
			if ph := r.healthTable.Get(targetPeerID); ph != nil {
				slog.Warn("ReplicateTo: peer timed out, marking dead",
					"peer", targetPeerID)
				ph.alive.Store(false)
			}
		}
		return nil, err
	}

	return future.Nacks(), nil
}

// SendNack sends an enriched NACK to a specific peer.
func (r *Replicator) SendNack(nack *pb.NackNotify, targetPeerID NodeId) {
	pkt, err := MarshalNackPacket(nack)
	if err != nil {
		slog.Error("failed to marshal NACK packet", "error", err)
		return
	}

	if err := r.transport.SendDirect(targetPeerID, pkt); err != nil {
		slog.Debug("failed to send NACK", "peer", targetPeerID, "error", err)
	}
}

// sweepLoop periodically checks pending requests and retransmits or gives up.
func (r *Replicator) sweepLoop() {
	defer r.wg.Done()
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			r.tracker.pending.Range(func(key uint64, tr *trackedRequest) bool {
				r.tracker.pending.Delete(key)
				tr.future.reject(ErrReplicationTimeout)
				return true
			})
			return
		case <-ticker.C:
			r.tracker.SweepDeadlines()
		}
	}
}
