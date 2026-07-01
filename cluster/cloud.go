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
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	clpb "github.com/swytchdb/swytch/cluster/proto/cloud"
	"github.com/swytchdb/swytch/effects"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

// These endpoints are first-party Swytch Cloud services and are compiled in, not
// configurable — a cluster either talks to Swytch Cloud or it does not phone home
// at all. Local testing points them at a loopback via /etc/hosts entries.
const (
	// cloudDataPlaneEndpoint is the gRPC write/query endpoint (DataPlane service).
	cloudDataPlaneEndpoint = "swytch.earth:443"
	// cloudCDNBase is the read origin: effect blobs are fetched by opaque address.
	cloudCDNBase = "https://data.swytch.earth"

	// folderSaltV1 domain-separates the object-store folder derivation. It MUST
	// match the cloud's storage.folderSaltV1 (github.com/swytchdb/cloud) — the node
	// derives the same folder the cloud writes into, so a mismatch would read from
	// the wrong (empty) prefix. Bump on both sides together.
	folderSaltV1 = "swytch-cloud-folder-v1"

	// cloudSubmitQueue bounds effects awaiting upload. Uploads are fire-and-forget
	// (write-path.md), so when the queue is full we drop — the cloud re-requests
	// anything it later finds missing, and anti-entropy repairs the rest.
	cloudSubmitQueue = 4096

	// cloudDedupWindow bounds the recently-queued offset set that collapses the
	// per-peer fan-out (a bind is replicated once per subscriber, but we only want
	// to upload it once). Sized well above any realistic in-flight fan-out burst.
	cloudDedupWindow = 8192
)

// cloudJob is one effect queued for upload: its ref, and the wire effect bytes
// ([keyLen][key][proto]) captured at broadcast time. wire may be empty, in which
// case the sender resolves it from the local log by ref (a re-requested effect).
type cloudJob struct {
	ref  effects.Tip
	wire []byte
}

// CloudClient ships this node's own effects to Swytch Cloud for durability and
// serves the cloud's fetch-requests back. It is the cluster's durability channel:
// a persistent gRPC stream to the data plane, plus blob-by-address reads from the
// CDN. Everything it sends is customer-encrypted; the cloud stores opaque bytes.
//
// It is fed by the PeerManager's broadcast methods — every effect this node
// broadcasts to peers is also teed here — so by construction it only ever uploads
// this node's own writes (leaderless durability, cluster-connect.md).
type CloudClient struct {
	authKey []byte // 32-byte wire auth key, presented first on every stream/RPC
	prefix  string // opaque object-store folder = hex(sha256(folderSalt || authKey))
	crypto  *cloudCrypto

	logReader LogReader // resolves a re-requested effect's bytes from the local log

	cdnHTTP *http.Client

	queue chan cloudJob

	dedupMu   sync.Mutex
	dedup     map[effects.Tip]struct{}
	dedupRing []effects.Tip
	dedupIdx  int

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	dropped atomic.Uint64
}

// NewCloudClient builds the durability channel from the cluster's connection
// secret. It derives the wire auth key, the folder prefix, and the customer
// encryption layer; logReader answers the cloud's fetch-requests from the local
// log. Call Start to open the stream.
func NewCloudClient(connectionSecret string, logReader LogReader) (*CloudClient, error) {
	cloudSecret := DeriveCloudSecret(connectionSecret)
	authKey, err := base64.RawURLEncoding.DecodeString(DeriveAuthKey(cloudSecret))
	if err != nil {
		return nil, fmt.Errorf("decode auth key: %w", err)
	}
	crypto, err := newCloudCrypto(DeriveEncryptionKey(connectionSecret))
	if err != nil {
		return nil, fmt.Errorf("cloud crypto: %w", err)
	}
	return &CloudClient{
		authKey:   authKey,
		prefix:    cloudPrefix(authKey),
		crypto:    crypto,
		logReader: logReader,
		cdnHTTP:   &http.Client{Timeout: 30 * time.Second},
		queue:     make(chan cloudJob, cloudSubmitQueue),
		dedup:     make(map[effects.Tip]struct{}, cloudDedupWindow),
		dedupRing: make([]effects.Tip, cloudDedupWindow),
	}, nil
}

// cloudPrefix names this cluster's object-store folder from its auth key,
// mirroring the cloud's storage.Prefix byte-for-byte.
func cloudPrefix(authKey []byte) string {
	h := sha256.New()
	h.Write([]byte(folderSaltV1))
	h.Write(authKey)
	return hex.EncodeToString(h.Sum(nil))
}

// Start opens the durability channel and begins draining the upload queue. The
// stream reconnects on its own; Start returns immediately.
func (c *CloudClient) Start(ctx context.Context) {
	c.ctx, c.cancel = context.WithCancel(ctx)
	c.wg.Add(1)
	go c.run()
	slog.Info("cloud durability channel started", "endpoint", cloudDataPlaneEndpoint)
}

// Stop tears down the channel and waits for the run loop to exit.
func (c *CloudClient) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
}

// tee enqueues one of this node's broadcast effects for upload. It is called on
// the hot broadcast path, so it does only the cheap work: skip synthetic probe
// effects (node 0), collapse fan-out duplicates, and hand the wire bytes off.
// All parsing/encryption happens on the sender goroutine. Non-blocking: a full
// queue drops the effect (fire-and-forget).
func (c *CloudClient) tee(notify *pb.OffsetNotify) {
	if c == nil || notify == nil || notify.Origin == nil {
		return
	}
	if notify.Origin.NodeId == 0 {
		return // synthetic discovery probe, never durable
	}
	ref := effects.Tip{notify.Origin.NodeId, notify.Origin.Offset}
	if !c.markNew(ref) {
		return // already queued/uploaded (fan-out duplicate)
	}
	select {
	case c.queue <- cloudJob{ref: ref, wire: notify.EffectData}:
	default:
		c.forget(ref)
		c.dropped.Add(1)
	}
}

// markNew records ref as queued and reports whether it was new. A bounded ring
// evicts the oldest entry so the set never grows without bound.
func (c *CloudClient) markNew(ref effects.Tip) bool {
	c.dedupMu.Lock()
	defer c.dedupMu.Unlock()
	if _, ok := c.dedup[ref]; ok {
		return false
	}
	if old := c.dedupRing[c.dedupIdx]; old != (effects.Tip{}) {
		delete(c.dedup, old)
	}
	c.dedupRing[c.dedupIdx] = ref
	c.dedupIdx = (c.dedupIdx + 1) % len(c.dedupRing)
	c.dedup[ref] = struct{}{}
	return true
}

// forget drops ref from the dedup set so a later re-broadcast can retry it (used
// when the queue was full and we dropped the effect).
func (c *CloudClient) forget(ref effects.Tip) {
	c.dedupMu.Lock()
	delete(c.dedup, ref)
	c.dedupMu.Unlock()
}

// run keeps a write stream open, reconnecting with backoff. Each connection runs
// until an error, then we redial; the queue persists across reconnects.
func (c *CloudClient) run() {
	defer c.wg.Done()
	backoff := 250 * time.Millisecond
	const maxBackoff = 30 * time.Second
	for {
		if c.ctx.Err() != nil {
			return
		}
		if err := c.stream(); err != nil && c.ctx.Err() == nil {
			slog.Warn("cloud stream ended, reconnecting", "error", err, "backoff", backoff)
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}
		backoff = 250 * time.Millisecond
	}
}

// dial opens a gRPC client connection to the data plane over TLS.
func (c *CloudClient) dial() (*grpc.ClientConn, error) {
	creds := credentials.NewTLS(&tls.Config{})
	return grpc.NewClient(cloudDataPlaneEndpoint, grpc.WithTransportCredentials(creds))
}

// stream opens one WriteEffects stream, sends the auth handshake, then pumps the
// upload queue (sender) while draining acks + fetch-requests (receiver). It
// returns when either direction fails or the client is stopped.
func (c *CloudClient) stream() error {
	conn, err := c.dial()
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	client := clpb.NewDataPlaneClient(conn)
	streamCtx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	ws, err := client.WriteEffects(streamCtx)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}
	if err := ws.Send(&clpb.WriteEffectsRequest{
		Payload: &clpb.WriteEffectsRequest_AuthKey{AuthKey: c.authKey},
	}); err != nil {
		return fmt.Errorf("send auth: %w", err)
	}

	// Receiver: acks are informational; fetch-requests are re-served from the log.
	recvErr := make(chan error, 1)
	go func() {
		for {
			resp, err := ws.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			if fetch := resp.GetFetch(); fetch != nil {
				c.handleFetch(fetch.GetRef())
			}
		}
	}()

	// Sender: drain the upload queue until an error or shutdown.
	for {
		select {
		case <-streamCtx.Done():
			return streamCtx.Err()
		case err := <-recvErr:
			return fmt.Errorf("recv: %w", err)
		case job := <-c.queue:
			eff, err := c.buildCloudEffect(job)
			if err != nil {
				slog.Debug("cloud: skip effect", "ref", job.ref, "error", err)
				continue
			}
			if eff == nil {
				continue // filtered (pubsub / ephemeral)
			}
			if err := ws.Send(&clpb.WriteEffectsRequest{
				Payload: &clpb.WriteEffectsRequest_Effect{Effect: eff},
			}); err != nil {
				// Re-queue so the effect isn't lost across the reconnect, then bail.
				c.requeue(job)
				return fmt.Errorf("send effect: %w", err)
			}
		}
	}
}

// requeue puts a job back after a failed send, best-effort (drop if full).
func (c *CloudClient) requeue(job cloudJob) {
	select {
	case c.queue <- job:
	default:
		c.forget(job.ref)
		c.dropped.Add(1)
	}
}

// handleFetch re-serves an effect the cloud asked for. If the effect is still in
// the local log we enqueue it for upload; if we have evicted it we stay silent
// and the cloud re-asks after a later scan.
func (c *CloudClient) handleFetch(ref *clpb.EffectRef) {
	if ref == nil {
		return
	}
	pref := &pb.EffectRef{NodeId: ref.GetNodeId(), Offset: ref.GetOffset()}
	wire, err := c.logReader.ReadEffect(pref)
	if err != nil {
		slog.Debug("cloud: fetch miss, staying silent", "ref", pref)
		return
	}
	// A re-request bypasses dedup: the cloud is explicitly asking again.
	c.forget(effects.Tip{ref.GetNodeId(), ref.GetOffset()})
	c.tee(&pb.OffsetNotify{Origin: pref, EffectData: wire})
}

// buildCloudEffect maps a queued job to an encrypted cloud effect. It resolves
// the wire bytes (from the job or the log), parses the envelope for its
// structural fields, and seals the key name and payload. Returns (nil, nil) for
// effects that must not be durabilized (pubsub messages, ephemeral subscriptions).
func (c *CloudClient) buildCloudEffect(job cloudJob) (*clpb.Effect, error) {
	wire := job.wire
	if len(wire) == 0 {
		ref := &pb.EffectRef{NodeId: job.ref[0], Offset: job.ref[1]}
		var err error
		if wire, err = c.logReader.ReadEffect(ref); err != nil {
			return nil, fmt.Errorf("resolve effect: %w", err)
		}
	}
	protoData := stripWireFrame(wire)

	eff := &pb.Effect{}
	if err := effects.UnmarshalEffect(protoData, eff); err != nil {
		return nil, fmt.Errorf("unmarshal effect: %w", err)
	}
	if eff.GetPubsubMessage() != nil {
		return nil, nil // wire-only, never persisted
	}
	if sub := eff.GetSubscription(); sub != nil && sub.Ephemeral {
		return nil, nil // discovery-only subscription
	}

	sealed, err := c.crypto.sealPayload(protoData)
	if err != nil {
		return nil, fmt.Errorf("seal payload: %w", err)
	}

	out := &clpb.Effect{
		Id:                    &clpb.EffectRef{NodeId: job.ref[0], Offset: job.ref[1]},
		Deps:                  toCloudRefs(eff.Deps),
		Key:                   c.crypto.sealKeyName(eff.Key),
		RawEffect:             sealed,
		EffectType:            cloudEffectType(eff),
		SnapshotStateCarrying: isStateCarryingSnapshot(eff),
	}
	if eff.Hlc != nil {
		out.TimeLocal = eff.Hlc.AsTime().UnixNano()
	}
	return out, nil
}

// GetTips returns the current cloud tip frontier for each named key, sealing the
// names to their opaque cloud form first. Used to bootstrap a key (e.g. cluster
// membership) from the cloud when there is no peer to gossip with.
func (c *CloudClient) GetTips(ctx context.Context, keyNames ...string) (map[string][]effects.Tip, error) {
	conn, err := c.dial()
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	sealedToName := make(map[string]string, len(keyNames))
	req := &clpb.GetTipsRequest{AuthKey: c.authKey}
	for _, name := range keyNames {
		sealed := c.crypto.sealKeyName([]byte(name))
		sealedToName[string(sealed)] = name
		req.Keys = append(req.Keys, sealed)
	}

	resp, err := clpb.NewDataPlaneClient(conn).GetTips(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("get tips: %w", err)
	}

	out := make(map[string][]effects.Tip, len(keyNames))
	for _, kt := range resp.GetKeys() {
		name, ok := sealedToName[string(kt.GetKey())]
		if !ok {
			continue
		}
		tips := make([]effects.Tip, 0, len(kt.GetTips()))
		for _, t := range kt.GetTips() {
			tips = append(tips, effects.Tip{t.GetNodeId(), t.GetOffset()})
		}
		out[name] = tips
	}
	return out, nil
}

// FetchEffect downloads one effect blob from the CDN by its address, peels the
// customer layer, and returns the decrypted effect. Used by the bootstrap DAG
// walk (reconstruct membership) and by CDN-raced fetches.
func (c *CloudClient) FetchEffect(ctx context.Context, ref *pb.EffectRef) (*pb.Effect, error) {
	url := fmt.Sprintf("%s/%s/%s", cloudCDNBase, c.prefix, cloudRef(ref.GetNodeId(), ref.GetOffset()))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.cdnHTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cdn get: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cdn get %s: status %d", url, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("cdn read: %w", err)
	}

	var blob clpb.Effect
	if err := proto.Unmarshal(body, &blob); err != nil {
		return nil, fmt.Errorf("unmarshal cloud effect: %w", err)
	}
	protoData, err := c.crypto.openPayload(blob.GetRawEffect())
	if err != nil {
		return nil, fmt.Errorf("open payload: %w", err)
	}
	eff := &pb.Effect{}
	if err := effects.UnmarshalEffect(protoData, eff); err != nil {
		return nil, fmt.Errorf("unmarshal effect: %w", err)
	}
	return eff, nil
}

// FetchFromCDN implements CDNFetcher so the engine can race a CDN read against a
// peer fetch for a missing effect. It returns the effect in engine wire format
// ([keyLen][key][proto]).
func (c *CloudClient) FetchFromCDN(ctx context.Context, offset *pb.EffectRef) ([]byte, error) {
	eff, err := c.FetchEffect(ctx, offset)
	if err != nil {
		return nil, err
	}
	protoData, err := effects.MarshalEffect(eff)
	if err != nil {
		return nil, err
	}
	return buildWireFrame(eff.Key, protoData), nil
}

// cloudRef encodes an (nodeID, offset) as the CDN blob name, mirroring the
// cloud's storage.Ref.
func cloudRef(nodeID, offset uint64) string {
	return fmt.Sprintf("%016x-%016x", nodeID, offset)
}

// toCloudRefs maps engine effect refs to the cloud proto's refs.
func toCloudRefs(deps []*pb.EffectRef) []*clpb.EffectRef {
	if len(deps) == 0 {
		return nil
	}
	out := make([]*clpb.EffectRef, 0, len(deps))
	for _, d := range deps {
		out = append(out, &clpb.EffectRef{NodeId: d.GetNodeId(), Offset: d.GetOffset()})
	}
	return out
}

// cloudEffectType maps an effect's kind to the cloud's structural type enum. The
// cloud uses this for snapshot metering and retention without decrypting anything.
func cloudEffectType(eff *pb.Effect) clpb.EffectType {
	switch {
	case eff.GetData() != nil:
		return clpb.EffectType_EFFECT_TYPE_DATA
	case eff.GetMeta() != nil:
		return clpb.EffectType_EFFECT_TYPE_META
	case eff.GetTxnBind() != nil:
		return clpb.EffectType_EFFECT_TYPE_TXN_BIND
	case eff.GetSnapshot() != nil:
		return clpb.EffectType_EFFECT_TYPE_SNAPSHOT
	case eff.GetSubscription() != nil:
		return clpb.EffectType_EFFECT_TYPE_SUBSCRIPTION
	case eff.GetSerialization() != nil:
		return clpb.EffectType_EFFECT_TYPE_SERIALIZATION
	case eff.GetNoop() != nil:
		return clpb.EffectType_EFFECT_TYPE_NOOP
	case eff.GetObservation() != nil:
		return clpb.EffectType_EFFECT_TYPE_OBSERVATION
	case eff.GetRowWrite() != nil:
		return clpb.EffectType_EFFECT_TYPE_ROW_WRITE
	case eff.GetPubsubMessage() != nil:
		return clpb.EffectType_EFFECT_TYPE_PUBSUB_MESSAGE
	default:
		return clpb.EffectType_EFFECT_TYPE_UNSPECIFIED
	}
}

// isStateCarryingSnapshot reports whether eff is a snapshot that carries
// materialized state (vs a verdict-only marker) — the cloud's retention walk
// terminates only at these.
func isStateCarryingSnapshot(eff *pb.Effect) bool {
	snap := eff.GetSnapshot()
	return snap != nil && snap.State != nil
}

// stripWireFrame drops the [4-byte LE keyLen][key] prefix from engine wire bytes,
// returning the marshalled effect proto. A frame too short to hold a length is
// returned verbatim (already bare proto).
func stripWireFrame(wire []byte) []byte {
	if len(wire) <= 4 {
		return wire
	}
	keyLen := binary.LittleEndian.Uint32(wire[:4])
	if uint32(len(wire)) >= 4+keyLen {
		return wire[4+keyLen:]
	}
	return wire
}

// buildWireFrame prepends the [4-byte LE keyLen][key] frame onto a marshalled
// effect proto, producing engine wire format.
func buildWireFrame(key, protoData []byte) []byte {
	out := make([]byte, 4+len(key)+len(protoData))
	binary.LittleEndian.PutUint32(out[:4], uint32(len(key)))
	copy(out[4:], key)
	copy(out[4+len(key):], protoData)
	return out
}
