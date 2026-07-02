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
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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

// cloudEffectInfo is the HPKE domain-separation info for sealed effect payloads.
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
	// backlogWarnEvery paces the "cloud unreachable, backlog growing" warning.
	backlogWarnEvery = 1024
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

	target     string
	conn       *grpc.ClientConn
	client     dp.DataPlaneClient
	folder     string // opaque CDN path segment, CloudFolder(authKey)
	httpClient *http.Client

	mu      sync.Mutex
	pending map[effects.Tip]*pb.Effect // un-acked mints; the held pointer keeps the effect reachable even if its pool ref is consumed
	sendQ   []effects.Tip
	relayQ  []*dp.Effect // fetch-request replies, already enveloped

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

	wake   chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewCloudSync derives the cloud key material from the connection secret and
// drops the secret: only the auth key (wire identity), the key-name PRF key,
// and the HPKE keypair (payload encryption) are retained.
func NewCloudSync(engine *effects.Engine, connectionSecret string) (*CloudSync, error) {
	enc, err := effects.NewEncryptorFromIKM(DeriveEncryptionKey(connectionSecret))
	if err != nil {
		return nil, fmt.Errorf("cloud sync: derive encryption keypair: %w", err)
	}
	authKey := DeriveAuthKeyBytes(DeriveCloudSecret(connectionSecret))
	cs := &CloudSync{
		engine:     engine,
		enc:        enc,
		keyNameKey: DeriveKeyNameKey(connectionSecret),
		authKey:    authKey,
		target:     CloudEndpoint,
		folder:     CloudFolder(authKey),
		httpClient: &http.Client{Timeout: 30 * time.Second},
		pending:    make(map[effects.Tip]*pb.Effect),
		wake:       make(chan struct{}, 1),
	}
	conn, err := grpc.NewClient(cs.target,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13})))
	if err != nil {
		return nil, fmt.Errorf("cloud sync: dial %s: %w", cs.target, err)
	}
	cs.conn = conn
	cs.client = dp.NewDataPlaneClient(conn)
	return cs, nil
}

// Start installs the engine's OnLocalEffect hook and launches the upload loop.
func (cs *CloudSync) Start() error {
	cs.ctx, cs.cancel = context.WithCancel(context.Background())
	cs.engine.OnLocalEffect = cs.handleLocalEffect
	cs.wg.Add(1)
	go cs.run()
	return nil
}

// Stop drains the outbox, then halts the upload loop. Graceful shutdown must
// not look like a crash to the cloud — the beacon's departure REMOVE is
// typically the last thing enqueued and peers may already be gone, so cloud is
// its only way out. The drain is bounded; whatever the cloud hasn't acked by
// the deadline (cloud down, backlog too deep) is abandoned exactly as a crash
// would, recoverable via cluster replication.
func (cs *CloudSync) Stop() {
	cs.waitDrained(cloudDrainTimeout)
	if cs.cancel != nil {
		cs.cancel()
	}
	cs.wg.Wait()
	if cs.conn != nil {
		if err := cs.conn.Close(); err != nil {
			slog.Warn("cloud sync: close connection", "error", err)
		}
	}
}

// waitDrained polls until the outbox is empty or the deadline passes.
func (cs *CloudSync) waitDrained(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for {
		cs.mu.Lock()
		n := len(cs.pending)
		cs.mu.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(deadline) {
			slog.Warn("cloud sync: shutdown drain timed out, abandoning outbox", "pending", n)
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
	name := CloudKeyName(cs.keyNameKey, eff.Key)
	cs.filterMu.Lock()
	cs.filterOwn.Add(string(name))
	cs.filterMu.Unlock()

	cs.engine.EffectCache().Incref(offset)
	cs.mu.Lock()
	cs.pending[offset] = eff
	cs.sendQ = append(cs.sendQ, offset)
	backlog := len(cs.pending)
	cs.mu.Unlock()
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

// retire removes an outbox entry and releases its pool reference.
func (cs *CloudSync) retire(tip effects.Tip) {
	cs.mu.Lock()
	_, held := cs.pending[tip]
	delete(cs.pending, tip)
	cs.mu.Unlock()
	if held {
		cs.engine.EffectCache().Decref(tip)
	}
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
	stream, err := cs.client.WriteEffects(cs.ctx)
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
		cs.retire(tip)
		return
	}

	cs.mu.Lock()
	_, held := cs.pending[tip]
	cs.mu.Unlock()
	if !held {
		return
	}
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
// serialized effect is HPKE-sealed into raw_effect.
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

// FetchFromCDN implements CDNFetcher: the engine's FetchFromAny races this
// against peer fetch, so every recursive dep fetch (NACK ingest, backfill,
// subscription bootstrap) transparently falls back to the cloud for effects
// no live peer holds. Returns wire-format bytes, same as a peer fetch.
func (cs *CloudSync) FetchFromCDN(ctx context.Context, ref *pb.EffectRef) ([]byte, error) {
	eff, err := cs.fetchEffect(ctx, effects.Tip{ref.GetNodeId(), ref.GetOffset()})
	if err != nil {
		return nil, err
	}
	return MarshalEffectWire(eff)
}

// DiscoverMembers reads the membership roster from the cloud for peer
// discovery: GetTips names the frontier, the CDN serves the blobs, and
// ReduceBranch — the real reducer — produces the roster. Deliberately zero
// engine interaction: pre-loading membership into the engine makes this node
// non-divergent at subscription bootstrap, so peers ACK instead of NACK and
// their key filters never arrive — free-read-misses then return false
// negatives for keys written before we joined. Discovery only yields candidate
// addresses; the authoritative state arrives through the normal join path.
func (cs *CloudSync) DiscoverMembers(ctx context.Context, membershipKey string) (*pb.ReducedEffect, error) {
	resp, err := cs.client.GetTips(ctx, &dp.GetTipsRequest{
		AuthKey: cs.authKey,
		Keys:    [][]byte{CloudKeyName(cs.keyNameKey, []byte(membershipKey))},
	})
	if err != nil {
		return nil, fmt.Errorf("cloud get tips: %w", err)
	}
	var queue []effects.Tip
	for _, kt := range resp.GetKeys() {
		for _, ref := range kt.GetTips() {
			queue = append(queue, effects.Tip{ref.GetNodeId(), ref.GetOffset()})
		}
	}

	// Pull the sub-DAG tips-down. Snapshot compaction keeps membership chains
	// short; the bound only stops a runaway walk, and hitting it means the
	// roster may be partial — say so, since discovery still works with any
	// one live address.
	const walkLimit = 1024
	fetched := map[effects.Tip]*pb.Effect{}
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
// GetTips. A nil slice means Cloud holds nothing for the key. This is the
// read-miss backstop — the engine installs these tips (walkAndInstall) and pulls
// the effect blobs on demand via the CDN, rehydrating an evicted key so the
// cluster owns it again. Unlike DiscoverMembers this does no reduction: it hands
// the frontier to the engine, whose own reconstruct path does the rest.
func (cs *CloudSync) CloudTips(ctx context.Context, key string) ([]effects.Tip, error) {
	name := CloudKeyName(cs.keyNameKey, []byte(key))
	if !cs.cloudMayHold(name) {
		return nil, nil
	}

	resp, err := cs.client.GetTips(ctx, &dp.GetTipsRequest{
		AuthKey: cs.authKey,
		Keys:    [][]byte{name},
	})
	if err != nil {
		return nil, fmt.Errorf("cloud get tips: %w", err)
	}
	var tips []effects.Tip
	for _, kt := range resp.GetKeys() {
		for _, ref := range kt.GetTips() {
			tips = append(tips, effects.Tip{ref.GetNodeId(), ref.GetOffset()})
		}
	}
	return tips, nil
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
// HPKE-open → inner effect.
func (cs *CloudSync) fetchEffect(ctx context.Context, tip effects.Tip) (*pb.Effect, error) {
	url := fmt.Sprintf("%s/%s/%016x-%016x", CloudCDNEndpoint, cs.folder, tip[0], tip[1])
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
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
	raw, err := cs.enc.OpenAndDecompress(env.GetRawEffect(), cloudEffectInfo)
	if err != nil {
		return nil, fmt.Errorf("cdn blob open: %w", err)
	}
	eff := &pb.Effect{}
	if err := effects.UnmarshalEffect(raw, eff); err != nil {
		return nil, err
	}
	return eff, nil
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
