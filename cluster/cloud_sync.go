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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	gproto "google.golang.org/protobuf/proto"

	pb "github.com/swytchdb/swytch/cluster/proto"
	dp "github.com/swytchdb/swytch/cluster/proto/dataplane"
	"github.com/swytchdb/swytch/effects"
)

// Swytch Cloud endpoints. Deliberately constants: first-party service URLs are
// not configurable (host entries are the local-testing path).
const (
	CloudEndpoint    = "ingest.next.swytch.earth:443"
	CloudCDNEndpoint = "https://next.swytch.earth"
)

// cloudEffectInfo is the domain-separation info for sealed effect payloads.
var cloudEffectInfo = []byte("effect")

const (
	cloudRetryMin = time.Second
	cloudRetryMax = 30 * time.Second
	// cloudNackRetry delays re-sending an effect the cloud nacked (its storage
	// error is not going to clear instantly).
	cloudNackRetry = 5 * time.Second
	// cloudDrainTimeout bounds how long a graceful Stop waits for the outbox
	// to be acked before abandoning it.
	cloudDrainTimeout = 5 * time.Second
	// cloudReconcileRetry paces the retry loop that re-consults GetTips for
	// keys whose read was served from the outbox during a cloud outage.
	cloudReconcileRetry = 15 * time.Second
	// cloudReconcileBatch bounds keys per reconcile GetTips call. One RPC per
	// key melted down for real: a proxy request-per-connection budget was
	// spent on reconcile churn, GOAWAYing everything else on the connection.
	cloudReconcileBatch = 256
	// cloudReconcileTipsTimeout bounds one reconcile GetTips round-trip. Per
	// batch, so it's generous: the server walks a closure per key.
	cloudReconcileTipsTimeout = 30 * time.Second
	// backlogWarnEvery paces the "cloud unreachable, backlog growing" warning.
	backlogWarnEvery = 1024
	// cloudProgressInterval paces the outbox drain log line: pending/queued/
	// in-flight and the interval's send/ack deltas, so a backlog is always
	// attributable to a stage (nothing queued vs sends stalled vs acks
	// stalled) instead of just a growing pending gauge.
	cloudProgressInterval = 30 * time.Second
)

// CloudSync uploads this node's locally-authored effects to Swytch Cloud over
// the dataplane WriteEffects stream.
//
// The engine side is fire-and-forget: handleLocalEffect only records the mint
// and never blocks a write. The sync side is durable: an effect stays in the
// outbox — holding a pool refcount so eviction can't drop it — until the cloud
// acks it as stored. Nacks and stream failures re-send; only process exit loses
// the un-acked tail (the documented durability lag, recoverable via cluster
// replication).
//
// The response stream also carries FetchRequests: the cloud asking the cluster
// for an effect it's missing. Those are answered from the vertex pool — any
// resident effect, not just our own, since the cloud broadcasts the ask to
// whichever streams are open — and silence is the correct reply for an effect
// we no longer hold.
type CloudSync struct {
	engine     *effects.Engine
	enc        *effects.Encryptor
	keyNameKey []byte
	authKey    []byte

	target string
	conn   *grpc.ClientConn
	client dp.DataPlaneClient
	// streamConn carries ONLY the WriteEffects stream. Unary traffic (GetTips)
	// stays on cs.conn: a proxy that budgets requests per connection (nginx
	// answers the ~1000th HTTP/2 request with GOAWAY) must never count unary
	// churn against the connection holding the long-lived upload stream —
	// that coupling killed the stream every ~30s and the outbox never drained.
	streamConn   *grpc.ClientConn
	streamClient dp.DataPlaneClient
	folder       string // opaque CDN path segment, CloudFolder(authKey)
	httpClient   *http.Client

	mu      sync.Mutex
	pending map[effects.Tip]*pb.Effect // un-acked mints; the held pointer keeps the effect reachable even if its pool ref is consumed
	// pendingKeys indexes pending by plaintext key, so CloudTips can answer a
	// read-miss from the outbox: an evicted-before-ack key has no index tips
	// and no cloud tips, but its effects sit refcount-pinned in the pool — the
	// outbox tips are the frontier the cloud will eventually hold, and the
	// walk fetches their bytes locally.
	pendingKeys map[string]map[effects.Tip]struct{}
	sendQ       []effects.Tip
	relayQ      []*dp.Effect // fetch-request replies, already enveloped
	// reconcile holds keys whose read was answered from the outbox while
	// GetTips was failing. The cloud may hold tips for them we've never seen
	// (another node's uploads); reconcileLoop re-consults until the cloud
	// answers and the missed frontier is merged into the DAG.
	reconcile map[string]struct{}

	// Cloud-pushed key-name filter gating the read-miss consult (CloudTips).
	// filterBulk is replaced wholesale per KeyFilter frame; filterOwn is
	// append-only with our own uploads' key names, covering the window until
	// the cloud's next push reflects them. Both hold PRF images (CloudKeyName).
	// A nil filterBulk (no frame received yet) leaves the gate open — every
	// miss consults the cloud, the pre-filter behavior. Guarded by filterMu,
	// not mu: the gate runs on the read path and must not contend with the
	// outbox.
	filterMu   sync.RWMutex
	filterBulk *effects.CuckooChain
	filterOwn  effects.CuckooChain

	// sentCount/ackedCount mirror the prometheus counters so the progress
	// logger can read interval deltas (prometheus counters aren't readable).
	sentCount  atomic.Uint64
	ackedCount atomic.Uint64

	wake   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewCloudSync derives the cloud key material from the connection secret and
// drops the secret: only the auth key (wire identity), the key-name PRF key,
// and the payload encryption key are retained.
func NewCloudSync(engine *effects.Engine, connectionSecret string) (*CloudSync, error) {
	enc, err := effects.NewEncryptorFromIKM(DeriveEncryptionKey(connectionSecret))
	if err != nil {
		return nil, fmt.Errorf("cloud sync: derive encryption keypair: %w", err)
	}
	authKey := DeriveAuthKeyBytes(DeriveCloudSecret(connectionSecret))
	cs := &CloudSync{
		engine:      engine,
		enc:         enc,
		keyNameKey:  DeriveKeyNameKey(connectionSecret),
		authKey:     authKey,
		target:      CloudEndpoint,
		folder:      CloudFolder(authKey),
		httpClient:  &http.Client{Timeout: 30 * time.Second},
		pending:     make(map[effects.Tip]*pb.Effect),
		pendingKeys: make(map[string]map[effects.Tip]struct{}),
		reconcile:   make(map[string]struct{}),
		wake:        make(chan struct{}, 1),
	}
	creds := credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13})
	// GetTips responses carry the closure effects inline; the closure is
	// bounded by the dataplane's walk (closureCap), not by gRPC's default
	// 4MiB receive cap, which would artificially truncate a legit response.
	recvCap := grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32))
	// Keepalive is load-bearing for the outbox: a dataplane restart behind the
	// edge leaves a half-open conn with no RST, and without pings the sender
	// parks in Send flow control forever — the outbox freezes silently while
	// pending grows (a 2026-07-06 bench wedged ~1M effects/node this way, with
	// zero reconnect attempts). With pings the dead path errors out of
	// Send/Recv and run()'s reconnect loop takes over.
	kal := grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                20 * time.Second,
		Timeout:             10 * time.Second,
		PermitWithoutStream: true,
	})
	conn, err := grpc.NewClient(cs.target, grpc.WithTransportCredentials(creds), recvCap, kal)
	if err != nil {
		return nil, fmt.Errorf("cloud sync: dial %s: %w", cs.target, err)
	}
	streamConn, err := grpc.NewClient(cs.target, grpc.WithTransportCredentials(creds), recvCap, kal)
	if err != nil {
		err = errors.Join(err, conn.Close())
		return nil, fmt.Errorf("cloud sync: dial %s (stream): %w", cs.target, err)
	}
	cs.conn = conn
	cs.client = dp.NewDataPlaneClient(conn)
	cs.streamConn = streamConn
	cs.streamClient = dp.NewDataPlaneClient(streamConn)
	return cs, nil
}

// Start installs the engine's OnLocalEffect hook and launches the upload loop.
func (cs *CloudSync) Start() error {
	cs.ctx, cs.cancel = context.WithCancel(context.Background())
	cs.engine.OnLocalEffect = cs.handleLocalEffect
	cs.wg.Add(3)
	go cs.run()
	go cs.reconcileLoop()
	go cs.progressLoop()
	return nil
}

// progressLoop runs the outbox watchdog on its own goroutine — the sender can
// park in stream.Send flow control for the entire duration of a stall, so the
// log that reports the stall must not share its loop.
func (cs *CloudSync) progressLoop() {
	defer cs.wg.Done()
	t := time.NewTicker(cloudProgressInterval)
	defer t.Stop()
	lastSent, lastAcked := cs.sentCount.Load(), cs.ackedCount.Load()
	for {
		select {
		case <-cs.ctx.Done():
			return
		case <-t.C:
			lastSent, lastAcked = cs.logOutboxProgress(lastSent, lastAcked)
		}
	}
}

// Stop drains the outbox, then halts the upload loop. Graceful shutdown must
// not look like a crash to the cloud — the beacon's departure REMOVE is
// typically the last thing enqueued and peers may already be gone, so cloud is
// its only way out. The drain is bounded on STALL, not total time: a deep
// backlog on a live cloud flushes fully; only a cloud that stops acking gets
// abandoned, exactly as a crash would (a solo node has no peers to re-replicate
// from, so an abandoned tail is lost — run 2841213 abandoned 217 effects this
// way and every key in them missed forever).
func (cs *CloudSync) Stop() {
	cs.waitDrained(cloudDrainTimeout)
	if cs.cancel != nil {
		cs.cancel()
	}
	cs.wg.Wait()
	for _, conn := range []*grpc.ClientConn{cs.conn, cs.streamConn} {
		if conn != nil {
			if err := conn.Close(); err != nil {
				slog.Warn("cloud sync: close connection", "error", err)
			}
		}
	}
}

// waitDrained polls until the outbox is empty, abandoning only when the count
// stops changing for a full timeout window — acks in flight keep resetting the
// stall clock.
func (cs *CloudSync) waitDrained(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	last := -1
	for {
		cs.mu.Lock()
		n := len(cs.pending)
		cs.mu.Unlock()
		if n == 0 {
			return
		}
		if n != last {
			deadline = time.Now().Add(timeout)
			last = n
		}
		if time.Now().After(deadline) {
			slog.Warn("cloud sync: shutdown drain stalled, abandoning outbox", "pending", n)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// handleLocalEffect is the engine's OnLocalEffect hook: enqueue the mint and
// return. Runs inside the emit path, so it must not block. The pool Incref is
// the outbox's DAG reference — the effect stays resident across evictions until
// the cloud's ack releases it.
func (cs *CloudSync) handleLocalEffect(offset effects.Tip, eff *pb.Effect) {
	// Wire-only kinds never persist and never reach this hook today; the guard
	// keeps a future mint-path change from silently uploading them.
	if sub := eff.GetSubscription(); sub != nil && (sub.Discovery || sub.Ephemeral) {
		return
	}
	if eff.GetPubsubMessage() != nil {
		return
	}

	// Anything cloud-bound is filter-positive from the moment it's enqueued,
	// even if it's later evicted locally before the cloud's push reflects it.
	// Except subscriptions: ensureSubscribed mints one on every read of an
	// absent key, moments before that same read reaches the cloudMayHold gate
	// — letting it into the filter would open the gate for the very key being
	// missed, turning every first-touch read into a wasted consult of a
	// stateless chain.
	if eff.GetSubscription() == nil {
		name := CloudKeyName(cs.keyNameKey, eff.Key)
		cs.filterMu.Lock()
		cs.filterOwn.Add(string(name))
		cs.filterMu.Unlock()
	}

	// OnLocalEffect fires synchronously in emit, after PutSized installed the
	// vertex with its creation ref — the incref cannot find it missing or
	// claimed. If it ever does, the ref protocol is broken and retire's Decref
	// would steal another holder's reference (premature reclaim of a vertex
	// someone still reads); fail at the violation, same policy as decref's
	// underflow panic.
	if !cs.engine.EffectCache().Incref(offset) {
		panic(fmt.Sprintf("cloud sync: outbox incref failed for just-emitted %v — vertex missing or claimed at emit time", offset))
	}
	cs.mu.Lock()
	cs.pending[offset] = eff
	k := string(eff.Key)
	set := cs.pendingKeys[k]
	if set == nil {
		set = make(map[effects.Tip]struct{})
		cs.pendingKeys[k] = set
	}
	set[offset] = struct{}{}
	cs.sendQ = append(cs.sendQ, offset)
	backlog := len(cs.pending)
	cs.mu.Unlock()
	cloudOutboxEnqueuedTotal.Inc()
	cloudOutboxPending.Set(float64(backlog))
	cs.wakeSender()

	if backlog%backlogWarnEvery == 0 {
		slog.Warn("cloud sync: upload backlog growing", "pending", backlog)
	}
}

func (cs *CloudSync) wakeSender() {
	select {
	case cs.wake <- struct{}{}:
	default:
	}
}

// retire removes an outbox entry and releases its pool reference. Returns
// whether the tip was actually held — false for acks of fetch-relays and
// duplicate acks, which were never outbox entries.
func (cs *CloudSync) retire(tip effects.Tip) bool {
	cs.mu.Lock()
	eff, held := cs.pending[tip]
	delete(cs.pending, tip)
	if held {
		k := string(eff.Key)
		if set := cs.pendingKeys[k]; set != nil {
			delete(set, tip)
			if len(set) == 0 {
				delete(cs.pendingKeys, k)
			}
		}
	}
	backlog := len(cs.pending)
	cs.mu.Unlock()
	if held {
		cloudOutboxPending.Set(float64(backlog))
		cs.engine.EffectCache().Decref(tip)
	}
	return held
}

// pendingTipsFor returns the outbox's un-acked tips on key — the frontier the
// cloud will eventually hold. Their bytes are refcount-pinned in the pool, so
// a walk over them resolves locally.
func (cs *CloudSync) pendingTipsFor(key string) []effects.Tip {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	set := cs.pendingKeys[key]
	if len(set) == 0 {
		return nil
	}
	tips := make([]effects.Tip, 0, len(set))
	for tip := range set {
		tips = append(tips, tip)
	}
	return tips
}

func (cs *CloudSync) run() {
	defer cs.wg.Done()
	backoff := cloudRetryMin
	for cs.ctx.Err() == nil {
		started := time.Now()
		err := cs.runStream()
		if cs.ctx.Err() != nil {
			return
		}
		// A stream that lived a while earned a fresh backoff; rapid failures
		// climb toward the cap.
		if time.Since(started) > time.Minute {
			backoff = cloudRetryMin
		}
		cloudStreamReconnectsTotal.Inc()
		slog.Warn("cloud sync: stream ended, reconnecting", "error", err, "retry_in", backoff)
		select {
		case <-cs.ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, cloudRetryMax)
	}
}

// runStream drives one WriteEffects stream to completion: authenticate,
// re-queue everything still pending (acks from a previous stream may have
// retired some), then interleave sends with the reader's acks and
// fetch-requests until the stream breaks or we shut down.
func (cs *CloudSync) runStream() error {
	stream, err := cs.streamClient.WriteEffects(cs.ctx)
	if err != nil {
		return err
	}
	if err := stream.Send(&dp.WriteEffectsRequest{
		Payload: &dp.WriteEffectsRequest_AuthKey{AuthKey: cs.authKey},
	}); err != nil {
		return err
	}

	cs.mu.Lock()
	cs.sendQ = cs.sendQ[:0]
	for tip := range cs.pending {
		cs.sendQ = append(cs.sendQ, tip)
	}
	cs.mu.Unlock()
	cs.wakeSender()

	readerErr := make(chan error, 1)
	go func() { readerErr <- cs.readLoop(stream) }()

	for {
		select {
		case <-cs.ctx.Done():
			if err := stream.CloseSend(); err != nil {
				slog.Debug("cloud sync: close send", "error", err)
			}
			<-readerErr
			return cs.ctx.Err()
		case err := <-readerErr:
			return err
		case <-cs.wake:
		}
		for {
			env, ok := cs.nextEnvelope()
			if !ok {
				break
			}
			if err := stream.Send(&dp.WriteEffectsRequest{
				Payload: &dp.WriteEffectsRequest_Effect{Effect: env},
			}); err != nil {
				return err
			}
		}
	}
}

// logOutboxProgress emits the per-interval drain line and returns the new
// send/ack watermarks. Warn when a non-empty outbox moved nothing this
// interval — with keepalive that means the stream is dying (about to error
// out), without it that was the silent-wedge signature.
func (cs *CloudSync) logOutboxProgress(lastSent, lastAcked uint64) (uint64, uint64) {
	sent, acked := cs.sentCount.Load(), cs.ackedCount.Load()
	cs.mu.Lock()
	pending := len(cs.pending)
	queued := len(cs.sendQ)
	relays := len(cs.relayQ)
	cs.mu.Unlock()
	if pending == 0 && queued == 0 && relays == 0 {
		return sent, acked
	}
	if sent == lastSent && acked == lastAcked {
		slog.Warn("cloud sync: outbox stalled",
			"pending", pending, "queued", queued, "relays", relays,
			"inflight", sent-acked, "interval", cloudProgressInterval)
	} else {
		slog.Info("cloud sync: outbox progress",
			"pending", pending, "queued", queued, "relays", relays,
			"inflight", sent-acked,
			"sent_delta", sent-lastSent, "acked_delta", acked-lastAcked,
			"interval", cloudProgressInterval)
	}
	return sent, acked
}

// nextEnvelope pops the next upload: fetch-relays first (the cloud is blocked
// on them mid-scan), then outbox mints. Sealing happens here, on the sync
// goroutine, never on the emit path.
func (cs *CloudSync) nextEnvelope() (*dp.Effect, bool) {
	for {
		cs.mu.Lock()
		if len(cs.relayQ) > 0 {
			env := cs.relayQ[0]
			cs.relayQ = cs.relayQ[1:]
			cs.mu.Unlock()
			return env, true
		}
		if len(cs.sendQ) == 0 {
			cs.mu.Unlock()
			return nil, false
		}
		tip := cs.sendQ[0]
		cs.sendQ = cs.sendQ[1:]
		eff := cs.pending[tip]
		cs.mu.Unlock()

		if eff == nil {
			continue // acked while queued
		}
		env, err := cs.buildEnvelope(tip, eff)
		if err != nil {
			slog.Error("cloud sync: envelope build failed, dropping effect",
				"tip", tip, "error", err)
			cs.retire(tip)
			continue
		}
		cloudEffectsSentTotal.Inc()
		cs.sentCount.Add(1)
		return env, true
	}
}

func (cs *CloudSync) readLoop(stream dp.DataPlane_WriteEffectsClient) error {
	for {
		resp, err := stream.Recv()
		if err != nil {
			return err
		}
		switch m := resp.GetMsg().(type) {
		case *dp.WriteResponse_Ack:
			cs.handleAck(m.Ack)
		case *dp.WriteResponse_Fetch:
			cs.handleFetch(m.Fetch)
		case *dp.WriteResponse_Filter:
			cs.handleFilter(m.Filter)
		}
	}
}

// handleAck retires a durably-stored effect, or schedules a nacked one for
// re-send. Acks for fetch-relays land here too — they were never in the
// outbox, so they fall through both paths untouched.
func (cs *CloudSync) handleAck(ack *dp.WriteAck) {
	tip := effects.Tip{ack.GetId().GetNodeId(), ack.GetId().GetOffset()}
	if ack.GetOk() {
		if cs.retire(tip) {
			cloudEffectsAckedTotal.Inc()
			cs.ackedCount.Add(1)
		}
		return
	}

	cs.mu.Lock()
	_, held := cs.pending[tip]
	cs.mu.Unlock()
	if !held {
		return
	}
	cloudEffectsNackedTotal.Inc()
	slog.Warn("cloud sync: cloud failed to store effect, will retry",
		"tip", tip, "error", ack.GetError())
	time.AfterFunc(cloudNackRetry, func() {
		cs.mu.Lock()
		if _, ok := cs.pending[tip]; ok {
			cs.sendQ = append(cs.sendQ, tip)
		}
		cs.mu.Unlock()
		cs.wakeSender()
	})
}

// handleFetch answers the cloud's ask for an effect it is missing. Served from
// the vertex pool regardless of author; a non-resident effect gets silence and
// the cloud re-asks elsewhere or later.
func (cs *CloudSync) handleFetch(req *dp.FetchRequest) {
	ref := req.GetRef()
	tip := effects.Tip{ref.GetNodeId(), ref.GetOffset()}
	eff, ok := cs.engine.EffectCache().Get(tip)
	if !ok {
		return
	}
	env, err := cs.buildEnvelope(tip, eff)
	if err != nil {
		slog.Error("cloud sync: fetch-relay envelope build failed", "tip", tip, "error", err)
		return
	}
	cs.mu.Lock()
	cs.relayQ = append(cs.relayQ, env)
	cs.mu.Unlock()
	cs.wakeSender()
}

// cloudMayHold is the read-miss filter gate: once the cloud has pushed its
// key-name filter, a PRF name absent from both it and our own-uploads chain
// cannot be in the cloud — free-miss without the round-trip. No filter yet
// (filterBulk nil) leaves the gate open. False positives fall through to a
// wasted consult; false negatives (stale filter) cost one premature miss and
// never hide a key a live peer holds — the cluster subscription path doesn't
// run through here.
func (cs *CloudSync) cloudMayHold(name []byte) bool {
	cs.filterMu.RLock()
	defer cs.filterMu.RUnlock()
	if cs.filterBulk == nil {
		return true
	}
	return cs.filterBulk.MaybeContains(string(name)) || cs.filterOwn.MaybeContains(string(name))
}

// handleFilter installs the cloud's key-name filter, replacing the previous
// one wholesale (the stream is ordered and each reconnect re-delivers the
// current filter, so latest always wins). An undecodable frame is dropped and
// the prior filter kept — the filter is advisory, never worth failing over.
func (cs *CloudSync) handleFilter(kf *dp.KeyFilter) {
	decoded := &effects.CuckooChain{}
	if err := decoded.UnmarshalBinary(kf.GetFilter()); err != nil {
		slog.Warn("cloud sync: undecodable key filter frame, keeping previous", "error", err)
		return
	}
	cs.filterMu.Lock()
	cs.filterBulk = decoded
	cs.filterMu.Unlock()
	slog.Debug("cloud sync: key filter installed", "bytes", len(kf.GetFilter()))
}

// buildEnvelope maps a swytch effect to the cloud's structural envelope: the
// (NodeID, Offset) identity, deps, kind, and time stay readable (the cloud's
// retention scan walks them); the key name becomes its PRF image and the whole
// serialized effect is sealed into raw_effect.
func (cs *CloudSync) buildEnvelope(tip effects.Tip, eff *pb.Effect) (*dp.Effect, error) {
	raw, err := effects.MarshalEffect(eff)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}
	sealed, err := cs.enc.SealAndCompress(raw, cloudEffectInfo)
	if err != nil {
		return nil, fmt.Errorf("seal: %w", err)
	}

	deps := make([]*dp.EffectRef, len(eff.Deps))
	for i, d := range eff.Deps {
		deps[i] = &dp.EffectRef{NodeId: d.NodeId, Offset: d.Offset}
	}
	var timeLocal int64
	if eff.Hlc != nil {
		timeLocal = eff.Hlc.AsTime().UnixNano()
	}
	snap := eff.GetSnapshot()
	return &dp.Effect{
		Id:                    &dp.EffectRef{NodeId: tip[0], Offset: tip[1]},
		Deps:                  deps,
		Key:                   CloudKeyName(cs.keyNameKey, eff.Key),
		RawEffect:             sealed,
		TimeLocal:             timeLocal,
		EffectType:            cloudEffectType(eff),
		SnapshotStateCarrying: snap != nil && snap.State != nil,
	}, nil
}

// FetchFromCDN implements CDNFetcher: FetchFromAny orders this against peer
// fetch by the caller's FetchHint, so every recursive dep fetch (NACK ingest,
// backfill, subscription bootstrap) transparently falls back to the cloud for
// effects no live peer holds. Returns wire-format bytes, same as a peer fetch.
func (cs *CloudSync) FetchFromCDN(ctx context.Context, ref *pb.EffectRef) ([]byte, error) {
	eff, err := cs.fetchEffect(ctx, effects.Tip{ref.GetNodeId(), ref.GetOffset()})
	if err != nil {
		return nil, err
	}
	return MarshalEffectWire(eff)
}

// DiscoverMembers reads the membership roster from the cloud for peer
// discovery: GetTips names the frontier and delivers the closure effects
// inline, and ReduceBranch — the real reducer — produces the roster.
// Deliberately zero engine interaction: pre-loading membership into the
// engine makes this node non-divergent at subscription bootstrap, so peers
// ACK instead of NACK and their key filters never arrive — free-read-misses
// then return false negatives for keys written before we joined. Discovery
// only yields candidate addresses; the authoritative state arrives through
// the normal join path.
func (cs *CloudSync) DiscoverMembers(ctx context.Context, membershipKey string) (*pb.ReducedEffect, error) {
	resp, err := cs.client.GetTips(ctx, &dp.GetTipsRequest{
		AuthKey: cs.authKey,
		Keys:    [][]byte{CloudKeyName(cs.keyNameKey, []byte(membershipKey))},
	})
	if err != nil {
		return nil, fmt.Errorf("cloud get tips: %w", err)
	}
	// Pull the sub-DAG tips-down, seeded from the inline closure; the per-tip
	// CDN fetch below only covers stragglers the sidecar didn't carry.
	// Snapshot compaction keeps membership chains short; the bound only stops
	// a runaway walk, and hitting it means the roster may be partial — say
	// so, since discovery still works with any one live address.
	const walkLimit = 1024
	fetched := map[effects.Tip]*pb.Effect{}
	var queue []effects.Tip
	for _, kt := range resp.GetKeys() {
		for _, ce := range cs.peelSidecar(kt) {
			fetched[ce.Tip] = ce.Eff
			for _, dep := range ce.Eff.Deps {
				queue = append(queue, effects.Tip{dep.NodeId, dep.Offset})
			}
		}
		for _, ref := range kt.GetTips() {
			queue = append(queue, effects.Tip{ref.GetNodeId(), ref.GetOffset()})
		}
	}
	for len(queue) > 0 {
		if len(fetched) >= walkLimit {
			slog.Warn("cloud discover: membership walk truncated, roster may be partial",
				"limit", walkLimit)
			break
		}
		tip := queue[0]
		queue = queue[1:]
		if _, ok := fetched[tip]; ok {
			continue
		}
		eff, err := cs.fetchEffect(ctx, tip)
		if err != nil {
			slog.Warn("cloud discover: blob fetch failed", "tip", tip, "error", err)
			continue
		}
		fetched[tip] = eff
		for _, dep := range eff.Deps {
			queue = append(queue, effects.Tip{dep.NodeId, dep.Offset})
		}
	}
	if len(fetched) == 0 {
		return nil, nil // fresh cluster: nothing in the cloud yet
	}
	return effects.ReduceBranch(topoOrder(fetched)), nil
}

// CloudTips implements effects.CloudReader: it returns the tip frontier Cloud
// holds for key, mapping key to its Cloud PRF image (CloudKeyName) and calling
// GetTips — merged with the outbox's un-acked tips on the key, which are the
// frontier the cloud hasn't seen yet. Without the merge, a key evicted before
// its upload acked would free-miss even though its bytes sit refcount-pinned
// in our own pool. A nil slice means neither holds anything. This is the
// read-miss backstop — the engine installs these tips (walkAndInstall) and
// the closure effects arrive inline (the sidecar), so the install walk runs
// locally instead of one fetch per dep, rehydrating an evicted key so the
// cluster owns it again. Unlike DiscoverMembers this does no reduction: it
// hands the frontier to the engine, whose own reconstruct path does the rest.
// Outbox tips need no sidecar — their bytes are already local.
// MayHold implements effects.CloudReader: the free, no-RPC filter gate the
// engine consults before deciding a read-miss can skip the subscribe +
// CloudTips path entirely. Own un-acked uploads are covered: filterOwn is fed
// at outbox enqueue.
func (cs *CloudSync) MayHold(key string) bool {
	return cs.cloudMayHold(CloudKeyName(cs.keyNameKey, []byte(key)))
}

func (cs *CloudSync) CloudTips(ctx context.Context, key string) ([]effects.Tip, []effects.CloudEffect, error) {
	name := CloudKeyName(cs.keyNameKey, []byte(key))
	if !cs.cloudMayHold(name) {
		return nil, nil, nil
	}
	tips := cs.pendingTipsFor(key)

	resp, err := cs.client.GetTips(ctx, &dp.GetTipsRequest{
		AuthKey: cs.authKey,
		Keys:    [][]byte{name},
	})
	if err != nil {
		if len(tips) > 0 {
			// Cloud unreachable, but the outbox holds the key's un-acked
			// frontier locally — serve our own writes rather than failing
			// the read. The answer is incomplete (the cloud may hold tips
			// we've never seen), so the key is marked for reconcile:
			// reconcileLoop re-consults until the cloud answers and merges
			// the missed frontier into the DAG.
			slog.Warn("cloud sync: get tips failed, serving outbox frontier", "key", key, "error", err)
			cs.markReconcile(key)
			return tips, nil, nil
		}
		return nil, nil, fmt.Errorf("cloud get tips: %w", err)
	}
	seen := make(map[effects.Tip]struct{}, len(tips))
	for _, t := range tips {
		seen[t] = struct{}{}
	}
	var sidecar []effects.CloudEffect
	for _, kt := range resp.GetKeys() {
		for _, ref := range kt.GetTips() {
			t := effects.Tip{ref.GetNodeId(), ref.GetOffset()}
			if _, ok := seen[t]; !ok {
				seen[t] = struct{}{}
				tips = append(tips, t)
			}
		}
		sidecar = append(sidecar, cs.peelSidecar(kt)...)
	}
	return tips, sidecar, nil
}

// markReconcile records a key whose read was served outbox-only during a
// cloud outage, pending a re-consult once the cloud answers again. The
// caller logs the failure; this just tracks it.
func (cs *CloudSync) markReconcile(key string) {
	cs.mu.Lock()
	cs.reconcile[key] = struct{}{}
	cs.mu.Unlock()
}

// reconcileKeys snapshots the pending reconcile set.
func (cs *CloudSync) reconcileKeys() []string {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if len(cs.reconcile) == 0 {
		return nil
	}
	keys := make([]string, 0, len(cs.reconcile))
	for k := range cs.reconcile {
		keys = append(keys, k)
	}
	return keys
}

func (cs *CloudSync) clearReconcile(key string) {
	cs.mu.Lock()
	delete(cs.reconcile, key)
	cs.mu.Unlock()
}

// reconcileLoop drives eventual convergence for keys whose reads were served
// from the outbox while GetTips was failing: once you've answered a read from
// your own un-acked writes, you owe the DAG whatever the cloud held that you
// couldn't see. Each tick re-consults every pending key in GetTips batches; a
// key stays pending until the cloud gives a complete answer and its frontier
// installs cleanly.
func (cs *CloudSync) reconcileLoop() {
	defer cs.wg.Done()
	ticker := time.NewTicker(cloudReconcileRetry)
	defer ticker.Stop()
	for {
		select {
		case <-cs.ctx.Done():
			return
		case <-ticker.C:
		}
		keys := cs.reconcileKeys()
		for start := 0; start < len(keys); start += cloudReconcileBatch {
			if cs.ctx.Err() != nil {
				return
			}
			end := min(start+cloudReconcileBatch, len(keys))
			cs.reconcileBatch(keys[start:end])
		}
	}
}

// reconcileBatch re-consults GetTips for a batch of keys in one RPC and merges
// each returned frontier into the DAG. A key is cleared from the pending set
// only when the cloud gave a complete answer for it and any frontier installed
// cleanly; anything else leaves it pending for the next tick. The whole RPC
// failing (cloud still unreachable) clears nothing.
func (cs *CloudSync) reconcileBatch(keys []string) {
	names := make([][]byte, len(keys))
	byName := make(map[string]string, len(keys))
	for i, key := range keys {
		name := CloudKeyName(cs.keyNameKey, []byte(key))
		names[i] = name
		byName[string(name)] = key
	}

	ctx, cancel := context.WithTimeout(cs.ctx, cloudReconcileTipsTimeout)
	defer cancel()
	resp, err := cs.client.GetTips(ctx, &dp.GetTipsRequest{AuthKey: cs.authKey, Keys: names})
	if err != nil {
		return // cloud still down; keep every key pending
	}

	for _, kt := range resp.GetKeys() {
		key, ok := byName[string(kt.GetKey())]
		if !ok {
			continue
		}
		var tips []effects.Tip
		for _, ref := range kt.GetTips() {
			tips = append(tips, effects.Tip{ref.GetNodeId(), ref.GetOffset()})
		}
		if len(tips) == 0 {
			cs.clearReconcile(key) // the cloud holds nothing we haven't already served
			continue
		}
		if err := cs.engine.InstallCloudTips(key, tips, cs.peelSidecar(kt)); err != nil {
			slog.Warn("cloud reconcile: install failed; will retry", "key", key, "error", err)
			continue
		}
		slog.Info("cloud reconcile: merged cloud frontier", "key", key, "tips", len(tips))
		cs.clearReconcile(key)
	}
}

// topoOrder sorts a fetched sub-DAG deps-before-dependents (Kahn). The walk's
// discovery order is NOT that: the cloud's tip set can contain orphans — an
// effect whose superseding tip-delete raced ahead of it in the cloud's
// concurrent write pipeline — and an orphan tip that is also an ancestor would
// otherwise reduce after its own descendants (a REMOVE'd member resurrecting
// off its stale INSERT was the observed failure). Concurrent siblings order by
// ForkChoiceHash ascending, mirroring dag.walk.
func topoOrder(fetched map[effects.Tip]*pb.Effect) []*pb.Effect {
	indeg := make(map[effects.Tip]int, len(fetched))
	dependents := make(map[effects.Tip][]effects.Tip, len(fetched))
	for tip, eff := range fetched {
		for _, d := range eff.Deps {
			dt := effects.Tip{d.NodeId, d.Offset}
			if _, ok := fetched[dt]; ok {
				indeg[tip]++
				dependents[dt] = append(dependents[dt], tip)
			}
		}
	}

	var ready []effects.Tip
	for tip := range fetched {
		if indeg[tip] == 0 {
			ready = append(ready, tip)
		}
	}
	chain := make([]*pb.Effect, 0, len(fetched))
	for len(ready) > 0 {
		next := 0
		for i := 1; i < len(ready); i++ {
			if effects.ForkChoiceLess(fetched[ready[i]].ForkChoiceHash, fetched[ready[next]].ForkChoiceHash) {
				next = i
			}
		}
		tip := ready[next]
		ready[next] = ready[len(ready)-1]
		ready = ready[:len(ready)-1]

		chain = append(chain, fetched[tip])
		for _, dep := range dependents[tip] {
			indeg[dep]--
			if indeg[dep] == 0 {
				ready = append(ready, dep)
			}
		}
	}
	return chain
}

// fetchEffect pulls one effect blob from the CDN and peels it: envelope →
// open → inner effect.
func (cs *CloudSync) fetchEffect(ctx context.Context, tip effects.Tip) (*pb.Effect, error) {
	// Our own un-acked mints are served from the outbox: the CDN cannot have
	// an effect that hasn't been uploaded yet, and asking it anyway both fails
	// the read (404) and risks the CDN negative-caching the miss. The read
	// path lands here when a key's effects were evicted from the pool while
	// still waiting in the upload queue — CloudTips names the outbox tip as
	// frontier, and this is where its bytes come from.
	cs.mu.Lock()
	eff := cs.pending[tip]
	cs.mu.Unlock()
	if eff != nil {
		return eff, nil
	}

	url := fmt.Sprintf("%s/%s/%016x-%016x", CloudCDNEndpoint, cs.folder, tip[0], tip[1])
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	cloudCDNFetchesTotal.Inc()
	resp, err := cs.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cdn fetch %s: status %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var env dp.Effect
	if err := gproto.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("cdn blob decode: %w", err)
	}
	fetched, _, err := cs.peelEnvelope(&env)
	if err != nil {
		return nil, err
	}
	return fetched, nil
}

// peelEnvelope opens one cloud effect envelope — seal off, decompress,
// unmarshal the inner effect. Returns the inner effect and its marshaled
// proto length (the pool's cache-accounting size).
func (cs *CloudSync) peelEnvelope(env *dp.Effect) (*pb.Effect, int, error) {
	raw, err := cs.enc.OpenAndDecompress(env.GetRawEffect(), cloudEffectInfo)
	if err != nil {
		return nil, 0, fmt.Errorf("cloud blob open: %w", err)
	}
	eff := &pb.Effect{}
	if err := effects.UnmarshalEffect(raw, eff); err != nil {
		return nil, 0, err
	}
	return eff, len(raw), nil
}

// peelSidecar decodes a KeyTips' inline closure into engine-installable
// effects. An envelope that fails to peel is skipped with a warning — the
// install walk fetches it like any other straggler.
func (cs *CloudSync) peelSidecar(kt *dp.KeyTips) []effects.CloudEffect {
	closure := kt.GetClosure()
	if len(closure) == 0 {
		return nil
	}
	sidecar := make([]effects.CloudEffect, 0, len(closure))
	for _, env := range closure {
		eff, protoLen, err := cs.peelEnvelope(env)
		if err != nil {
			slog.Warn("cloud sync: sidecar envelope unusable, walk will fetch it",
				"tip", effects.Tip{env.GetId().GetNodeId(), env.GetId().GetOffset()}, "error", err)
			continue
		}
		sidecar = append(sidecar, effects.CloudEffect{
			Tip:      effects.Tip{env.GetId().GetNodeId(), env.GetId().GetOffset()},
			Eff:      eff,
			ProtoLen: protoLen,
		})
	}
	cloudSidecarEffectsTotal.Add(float64(len(sidecar)))
	return sidecar
}

// cloudEffectType maps the Effect.kind oneof to the cloud's structural enum.
func cloudEffectType(eff *pb.Effect) dp.EffectType {
	switch {
	case eff.GetData() != nil:
		return dp.EffectType_EFFECT_TYPE_DATA
	case eff.GetMeta() != nil:
		return dp.EffectType_EFFECT_TYPE_META
	case eff.GetTxnBind() != nil:
		return dp.EffectType_EFFECT_TYPE_TXN_BIND
	case eff.GetSnapshot() != nil:
		return dp.EffectType_EFFECT_TYPE_SNAPSHOT
	case eff.GetSubscription() != nil:
		return dp.EffectType_EFFECT_TYPE_SUBSCRIPTION
	case eff.GetSerialization() != nil:
		return dp.EffectType_EFFECT_TYPE_SERIALIZATION
	case eff.GetNoop() != nil:
		return dp.EffectType_EFFECT_TYPE_NOOP
	case eff.GetObservation() != nil:
		return dp.EffectType_EFFECT_TYPE_OBSERVATION
	case eff.GetRowWrite() != nil:
		return dp.EffectType_EFFECT_TYPE_ROW_WRITE
	case eff.GetPubsubMessage() != nil:
		return dp.EffectType_EFFECT_TYPE_PUBSUB_MESSAGE
	default:
		return dp.EffectType_EFFECT_TYPE_UNSPECIFIED
	}
}
