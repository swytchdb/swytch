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
	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

// Sentinel errors for replication failures.
var (
	ErrNoPeers               = errors.New("no alive symmetric peers available for replication")
	ErrReplicationTimeout    = errors.New("replication timed out waiting for ACK")
	ErrAllPeersNotSubscribed = errors.New("every tracked peer reported they are not subscribed to the key")
)

// Retransmission constants.
const (
	sweepInterval = 250 * time.Millisecond // sweep tick rate — no point checking faster than rtoFloor/2
)

// ReplicationFuture represents a pending replication operation.
// It resolves when the first same-region peer ACKs or when the deadline expires.
// Callers check Wait()'s return value to distinguish success from failure.
//
// expected/dropped/setupDone implement fast-fail on "every peer says
// they're not subscribed": a NotSubscribed NACK does not resolve the
// future (it would falsely satisfy first-ACK), but when every tracked
// peer's response is NotSubscribed the future rejects immediately
// instead of waiting for the timeout.
type ReplicationFuture struct {
	done      chan struct{}
	once      sync.Once
	err       atomic.Pointer[error]
	nacks     atomic.Pointer[[]*pb.NackNotify] // NACKs from NACK response (status=0x01)
	expected  atomic.Int32                     // total tracker entries attached to this future
	dropped   atomic.Int32                     // tracked peers that can no longer ACK: NotSubscribed, or swept after leaving the alive+symmetric set
	setupDone atomic.Bool                      // true once expected has been finalized
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
	future     *ReplicationFuture
	createdAt  int64        // UnixNano
	lastSentAt atomic.Int64 // UnixNano of most recent send attempt (original or retransmit)
	deadline   int64        // UnixNano — absolute deadline (deadline-mode requests only)
	packetSize int          // bytes sent (for metrics)
	peerID     NodeId
	packetData []byte // serialized plaintext for retransmission

	// retransmit selects the sweep's give-up policy. When true (same-region
	// first-ACK replication) a send that fails only because the peer's
	// connection isn't up yet must be HELD and retried, not dropped: dropping
	// it orphaned compaction snapshots whose dependents later became
	// unreconstructable cluster-wide. The sweep retransmits the packet every
	// tick until the peer ACKs or leaves the alive+symmetric set — no
	// wall-clock deadline. When false (cross-region fire-and-forget, bind
	// ReplicateTo) the sweep uses the deadline-based give-up below.
	retransmit bool

	nacks        []*pb.NackNotify // NACKs received in a NACK response (status=0x01)
	traceContext []byte           // OTel trace context for correlating ACK spans
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

// rejectDeadlineExpired rejects deadline-mode requests (cross-region
// fire-and-forget, bind ReplicateTo) that have exceeded their deadline.
// Same-region first-ACK requests (retransmit=true) are swept by
// Replicator.sweep, which retransmits instead of expiring on a clock.
func (rt *requestTracker) rejectDeadlineExpired() {
	nowNano := time.Now().UnixNano()

	rt.pending.Range(func(key uint64, tr *trackedRequest) bool {
		if tr.retransmit || nowNano <= tr.deadline {
			return true
		}
		rt.pending.Delete(key)
		tr.future.reject(ErrReplicationTimeout)
		RecordRetransmissionGiveUp(tr.peerID)
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
	var sendSpan trace.Span
	if tracing.Enabled() {
		remoteCtx := tracing.ExtractFromBytes(notify.GetTraceContext())
		_, sendSpan = tracing.Tracer().Start(remoteCtx, "replication.send",
			trace.WithAttributes(attribute.String("effect.key", string(notify.GetKey()))))
		defer sendSpan.End()
	}

	token := r.mu.RLock()
	regions := make(map[NodeId]string, len(r.peerRegions))
	maps.Copy(regions, r.peerRegions)
	r.mu.RUnlock(token)

	// Attach effect data inline. Safe to mutate: callers pass a
	// per-effect notify that isn't read after this call.
	notify.EffectData = wireData

	// Find alive+symmetric same-region peers for tracked replication
	targets := r.healthTable.AliveSymmetricPeers(r.localRegion, regions)

	// Send to cross-region peers (fire-and-forget, off critical path)
	go r.sendCrossRegion(notify, regions)

	// No alive same-region targets — fail immediately
	if len(targets) == 0 {
		return rejectedFuture(ErrNoPeers)
	}

	// Marshal the notify body once: it is byte-identical for every peer
	// (only the per-request header varies), so AssembleNotifyPacket frames
	// a packet per peer without re-serializing the effect. Re-marshalling
	// per peer was O(peers × effectSize) of redundant work on every PUT.
	body, err := proto.Marshal(notify)
	if err != nil {
		return rejectedFuture(fmt.Errorf("marshal notify packet: %w", err))
	}

	// One tracked request per peer; future resolves on FIRST ACK (first-ACK semantics).
	future := &ReplicationFuture{done: make(chan struct{})}

	// Fan the per-peer sends out concurrently: each transport.Send opens a
	// QUIC uni-stream that can block on flow control, so a serial loop made
	// PUT latency grow linearly with same-region peer count. wg.Wait below
	// keeps the expected/setupDone finalization correct (every expected.Add
	// completes before we set setupDone).
	var wg sync.WaitGroup
	wg.Add(len(targets))
	for _, targetID := range targets {
		go func(targetID NodeId) {
			defer wg.Done()

			requestID := r.tracker.nextID.Add(1)
			notifyPkt := AssembleNotifyPacket(requestID, body)

			// Store tracked request BEFORE sending so the NACK handler can
			// find it. On fast networks (localhost) the peer's response can
			// arrive before Send() returns.
			now := time.Now().UnixNano()
			tr := &trackedRequest{
				future:       future,
				createdAt:    now,
				peerID:       targetID,
				packetData:   notifyPkt,
				retransmit:   true,
				traceContext: notify.GetTraceContext(),
			}
			tr.lastSentAt.Store(now)
			r.tracker.pending.Store(requestID, tr)
			future.expected.Add(1)

			// A send failure here is almost always "the peer's connection
			// isn't up yet" (startup race, mid-reconnect). The request stays
			// tracked; Replicator.sweep retransmits it once a connection is
			// available and only gives up if the peer leaves the
			// alive+symmetric set. We must NOT drop it: an un-replicated tip
			// that a later effect depends on becomes an unrecoverable orphan.
			wireSize, err := r.transport.Send(targetID, notifyPkt)
			if err != nil {
				slog.Debug("same-region send failed, holding for retransmit",
					"peer", targetID, "error", err)
				return
			}
			tr.packetSize = wireSize
			RecordNotificationSent()
		}(targetID)
	}
	wg.Wait()

	// Finalize expected so NotSubscribed responses and sweep give-ups can
	// resolve the future. Re-check here covers the race where every tracked
	// peer responded before we set setupDone.
	future.setupDone.Store(true)
	if future.dropped.Load() == future.expected.Load() {
		future.reject(ErrAllPeersNotSubscribed)
	}

	return future
}

// sendCrossRegion sends the notification to all cross-region peers.
// Fire-and-forget — callers don't block on cross-region ACKs.
func (r *Replicator) sendCrossRegion(notify *pb.OffsetNotify, regions map[NodeId]string) {
	deadlineNano := time.Now().Add(r.replicationTimeout).UnixNano()

	// Marshal the notify body once and reuse it across peers (see Replicate).
	var (
		body       []byte
		nullFuture *ReplicationFuture
	)

	for peerID, region := range regions {
		if region == r.localRegion {
			continue
		}

		if nullFuture == nil {
			marshalled, err := proto.Marshal(notify)
			if err != nil {
				slog.Error("failed to marshal cross-region notify packet", "error", err)
				return
			}
			body = marshalled
			nullFuture = &ReplicationFuture{done: make(chan struct{})}
			nullFuture.resolve()
		}

		requestID := r.tracker.nextID.Add(1)
		notifyPkt := AssembleNotifyPacket(requestID, body)

		now := time.Now().UnixNano()
		tr := &trackedRequest{
			future:     nullFuture,
			createdAt:  now,
			deadline:   deadlineNano,
			peerID:     peerID,
			packetData: notifyPkt,
		}
		tr.lastSentAt.Store(now)
		r.tracker.pending.Store(requestID, tr)

		wireSize, err := r.transport.Send(peerID, notifyPkt)
		if err != nil {
			r.tracker.pending.Delete(requestID)
			slog.Error("cross-region send failed, notification dropped",
				"peer", peerID, "error", err)
			continue
		}
		tr.packetSize = wireSize
		RecordNotificationSent()
	}
}

// HandleNotify processes an inbound notification from a peer.
// It applies the effect and sends an ACK or NACK response back via QUIC.
func (r *Replicator) HandleNotify(peerID NodeId, requestID uint64, notify *pb.OffsetNotify) {
	var recvSpan trace.Span
	if tracing.Enabled() {
		remoteCtx := tracing.ExtractFromBytes(notify.GetTraceContext())
		_, recvSpan = tracing.Tracer().Start(remoteCtx, "replication.receive",
			trace.WithAttributes(
				attribute.Int("peer.id", int(peerID)),
				attribute.Int64("request.id", int64(requestID)),
			))
		defer recvSpan.End()
	}

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

	now := time.Now()
	latencyMs := float64(now.UnixNano()-tr.createdAt) / 1e6

	if tracing.Enabled() {
		traceCtx := tracing.ExtractFromBytes(tr.traceContext)
		_, ackSpan := tracing.Tracer().Start(traceCtx, "replication.ack_received",
			trace.WithAttributes(
				attribute.Int("peer.id", int(peerID)),
				attribute.Int64("request.id", int64(requestID)),
				attribute.Float64("rtt_ms", latencyMs),
			))
		ackSpan.End()
	}

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
	notSubscribed := false
	if status == 0x01 && len(fullPacket) > 22 {
		nacks, err := parseNotifyNACKPayload(fullPacket[22:])
		if err != nil {
			slog.Debug("failed to parse NACK payload", "peer", peerID, "error", err)
		}
		if len(nacks) > 0 {
			tr.future.nacks.Store(&nacks)
			notSubscribed = allNotSubscribed(nacks)
		}
	}

	// A NotSubscribed NACK means "this peer discarded the write."
	// It must not satisfy first-ACK semantics — wait for a real
	// accepting peer. When every tracked peer reports NotSubscribed
	// the future rejects fast instead of waiting for the deadline.
	if notSubscribed {
		dropped := tr.future.dropped.Add(1)
		if tr.future.setupDone.Load() && dropped == tr.future.expected.Load() {
			tr.future.reject(ErrAllPeersNotSubscribed)
		}
		return
	}

	tr.future.resolve()
}

// allNotSubscribed reports whether every NACK in the response carries
// the NotSubscribed flag. Mixed responses (any real conflict NACK)
// fall through to normal resolve so the sender can still process
// missing-tip NACKs.
func allNotSubscribed(nacks []*pb.NackNotify) bool {
	for _, n := range nacks {
		if !n.GetNotSubscribed() {
			return false
		}
	}
	return len(nacks) > 0
}

// ReplicateTo sends a notification to a specific peer and waits for ACK or NACK.
// Used by transactional bind to get deliberate per-subscriber responses.
func (r *Replicator) ReplicateTo(notify *pb.OffsetNotify, wireData []byte, targetPeerID NodeId) ([]*pb.NackNotify, error) {
	notify.EffectData = wireData

	body, err := proto.Marshal(notify)
	if err != nil {
		return nil, fmt.Errorf("marshal notify packet: %w", err)
	}
	return r.replicateBody(body, targetPeerID)
}

// ReplicateMarshalled sends an already-marshalled OffsetNotify body to a
// specific peer and waits for ACK/NACK. The body is the proto-marshalled
// notify, shared across a fan-out so it is marshalled once; only the per-peer
// request header is assembled here. notify is unused by the wire path (the
// body carries everything) and is present only so test doubles can inspect it.
func (r *Replicator) ReplicateMarshalled(_ *pb.OffsetNotify, notifyBody []byte, targetPeerID NodeId) ([]*pb.NackNotify, error) {
	return r.replicateBody(notifyBody, targetPeerID)
}

// replicateBody registers a tracked request, frames the notify body into a
// packet, sends it, and blocks for the ACK/NACK. Shared by ReplicateTo (which
// marshals per call) and ReplicateMarshalled (which reuses a shared body).
func (r *Replicator) replicateBody(notifyBody []byte, targetPeerID NodeId) ([]*pb.NackNotify, error) {
	requestID := r.tracker.nextID.Add(1)
	notifyPkt := AssembleNotifyPacket(requestID, notifyBody)

	// Populate every field — deadline and retransmit included — BEFORE storing,
	// so the sweep can never observe a deadline=0 entry and expire it in the
	// window before initialization. Stored before Send because a fast peer's
	// ACK/NACK can land before Send() returns. retransmit stays false: this is
	// the deadline-mode bind ReplicateTo path.
	now := time.Now().UnixNano()
	future := &ReplicationFuture{done: make(chan struct{})}
	future.expected.Store(1)
	future.setupDone.Store(true)
	tr := &trackedRequest{
		future:     future,
		createdAt:  now,
		deadline:   now + int64(r.replicationTimeout),
		peerID:     targetPeerID,
		packetData: notifyPkt,
	}
	tr.lastSentAt.Store(now)
	r.tracker.pending.Store(requestID, tr)

	wireSize, err := r.transport.Send(targetPeerID, notifyPkt)
	if err != nil {
		r.tracker.pending.Delete(requestID)
		return nil, fmt.Errorf("send to peer %d: %w", targetPeerID, err)
	}
	tr.packetSize = wireSize

	RecordNotificationSent()

	if err := future.Wait(); err != nil {
		return nil, err
	}

	return future.Nacks(), nil
}

// SendOneWay sends a notify packet to a single peer without registering
// a tracked request. Used for ephemeral pub/sub effects that are fire-
// and-forget: no retransmits (which would violate at-most-once), no
// future to wait on, the ACK that comes back is silently discarded by
// HandleNotifyACK's missing-entry path.
func (r *Replicator) SendOneWay(notify *pb.OffsetNotify, wireData []byte, targetPeerID NodeId) error {
	notify.EffectData = wireData

	notifyPkt, err := MarshalNotifyPacket(0, notify)
	if err != nil {
		return fmt.Errorf("marshal notify packet: %w", err)
	}

	if _, err := r.transport.Send(targetPeerID, notifyPkt); err != nil {
		return fmt.Errorf("send to peer %d: %w", targetPeerID, err)
	}

	RecordNotificationSent()
	return nil
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

// sweepLoop periodically retransmits held same-region requests and expires
// deadline-mode ones.
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
			r.sweep()
		}
	}
}

// sweep advances every pending request once per tick. Deadline-mode requests
// (cross-region, bind ReplicateTo) expire on their clock. Same-region
// first-ACK requests (retransmit=true) are driven to durability: a request
// whose target has left the alive+symmetric set is given up — the peer is
// declared dead, so we stop holding the write open and let the originator
// fall back to local commit (it is now the last node standing on this key);
// otherwise an unacked request is retransmitted. A request that ACKs is
// removed from pending by the ACK handler, so only sends that never landed —
// the peer's connection wasn't up at first attempt — keep reaching here.
func (r *Replicator) sweep() {
	r.tracker.rejectDeadlineExpired()

	now := time.Now().UnixNano()
	r.tracker.pending.Range(func(key uint64, tr *trackedRequest) bool {
		if !tr.retransmit {
			return true
		}

		// Give up only when the peer is no longer a replication target: no
		// heartbeat within the liveness timeout. While it IS alive, connFunc
		// always resolves a live connection (outbound, or inbound fallback) we
		// can send over, so retransmission will land — no wall-clock needed.
		if ph := r.healthTable.Get(tr.peerID); ph == nil || !ph.IsReplicationTarget() {
			r.tracker.pending.Delete(key)
			RecordRetransmissionGiveUp(tr.peerID)
			// Mirror the NotSubscribed accounting: this peer can no longer
			// positively resolve the future. When the last live target drops,
			// reject so the caller (a blocking SafeMode flush) unblocks and
			// commits locally.
			dropped := tr.future.dropped.Add(1)
			if tr.future.setupDone.Load() && dropped == tr.future.expected.Load() {
				tr.future.reject(ErrNoPeers)
			}
			return true
		}

		if now-tr.lastSentAt.Load() < int64(sweepInterval) {
			return true
		}
		tr.lastSentAt.Store(now)
		if wireSize, err := r.transport.Send(tr.peerID, tr.packetData); err != nil {
			slog.Debug("retransmit failed (peer connection not up yet)",
				"peer", tr.peerID, "error", err)
		} else {
			tr.packetSize = wireSize
			RecordNotificationSent()
		}
		return true
	})
}
