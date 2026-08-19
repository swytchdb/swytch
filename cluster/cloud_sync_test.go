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
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	dp "github.com/swytchdb/swytch/cluster/proto/dataplane"
	"github.com/swytchdb/swytch/effects"
	"google.golang.org/grpc"
)

type staticTipsClient struct {
	dp.DataPlaneClient
	response *dp.GetTipsResponse
}

func (c *staticTipsClient) GetTips(context.Context, *dp.GetTipsRequest, ...grpc.CallOption) (*dp.GetTipsResponse, error) {
	return c.response, nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// TestStopWaitsForInflightEnqueue pins the shutdown boundary: Stop must not
// observe an empty outbox and cancel the sender while a synchronous local-mint
// callback is already in flight but has not enqueued its effect yet.
func TestStopWaitsForInflightEnqueue(t *testing.T) {
	engine := effects.NewEngine(effects.EngineConfig{NodeID: 1})
	defer func() {
		if err := engine.Close(); err != nil {
			t.Fatalf("close engine: %v", err)
		}
	}()
	cs := &CloudSync{
		engine:      engine,
		keyNameKey:  DeriveKeyNameKey("shutdown-test-secret"),
		pending:     make(map[effects.Tip]*pb.Effect),
		pendingKeys: make(map[string]*pendingKeyEntry),
		wake:        make(chan struct{}, 1),
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	enqueued := make(chan effects.Tip, 1)
	engine.SetOnLocalEffect(func(tip effects.Tip, eff *pb.Effect) {
		close(entered)
		<-release
		cs.handleLocalEffect(tip, eff)
		enqueued <- tip
	})

	emitDone := make(chan error, 1)
	go func() {
		ctx := engine.NewContext()
		if err := ctx.Emit(&pb.Effect{
			Key: []byte("shutdown-key"),
			Kind: &pb.Effect_Data{Data: &pb.DataEffect{
				Op:    pb.EffectOp_INSERT_OP,
				Value: &pb.DataEffect_Raw{Raw: []byte("value")},
			}},
		}); err != nil {
			emitDone <- err
			return
		}
		emitDone <- ctx.Flush()
	}()
	<-entered

	stopped := make(chan struct{})
	go func() {
		cs.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("Stop returned while a local-effect callback was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	tip := <-enqueued
	cs.retire(tip) // stand in for the cloud ack so the drain can complete
	if err := <-emitDone; err != nil {
		t.Fatalf("emit: %v", err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish after the in-flight enqueue drained")
	}
}

// filterFrame builds the bloom wire bytes a KeyFilter frame carries over the
// given PRF names.
func filterFrame(t *testing.T, names ...[]byte) []byte {
	t.Helper()
	b := effects.NewBloom(effects.BloomMinBytes)
	for _, n := range names {
		b.Set(n)
	}
	return b.Frame()
}

// TestCloudMayHoldGate covers the read-miss filter gate: open with no filter,
// closed for names absent from both chains, open for cloud-held names, and
// open for our own uploads even when the cloud's push doesn't reflect them yet.
func TestCloudMayHoldGate(t *testing.T) {
	keyNameKey := DeriveKeyNameKey("gate-test-secret")
	held := CloudKeyName(keyNameKey, []byte("held"))
	absent := CloudKeyName(keyNameKey, []byte("absent"))
	ours := CloudKeyName(keyNameKey, []byte("ours"))

	cs := &CloudSync{keyNameKey: keyNameKey}

	// No filter received yet: every name consults.
	if !cs.cloudMayHold(absent) {
		t.Fatal("gate must stay open before any filter frame arrives")
	}

	cs.handleFilter(&dp.KeyFilter{Filter: filterFrame(t, held)})
	if !cs.cloudMayHold(held) {
		t.Fatal("cloud-held name must pass the gate")
	}
	if cs.cloudMayHold(absent) {
		t.Fatal("name absent from the filter must free-miss")
	}

	// Our own upload stays consultable before the cloud's push reflects it.
	cs.filterMu.Lock()
	cs.filterOwn.add(ours)
	cs.filterMu.Unlock()
	if !cs.cloudMayHold(ours) {
		t.Fatal("own-uploaded name must pass the gate")
	}

	// A replacement frame wins wholesale, but own uploads remain visible.
	cs.handleFilter(&dp.KeyFilter{Filter: filterFrame(t, absent)})
	if !cs.cloudMayHold(absent) || cs.cloudMayHold(held) {
		t.Fatal("a new frame must replace the previous filter wholesale")
	}
	if !cs.cloudMayHold(ours) {
		t.Fatal("own uploads must survive a bulk replacement")
	}
}

// TestHandleFilterUndecodableKeepsPrevious: a garbage frame is dropped and the
// prior filter's verdicts stand.
func TestHandleFilterUndecodableKeepsPrevious(t *testing.T) {
	keyNameKey := DeriveKeyNameKey("gate-test-secret")
	held := CloudKeyName(keyNameKey, []byte("held"))

	cs := &CloudSync{keyNameKey: keyNameKey}
	cs.handleFilter(&dp.KeyFilter{Filter: filterFrame(t, held)})
	cs.handleFilter(&dp.KeyFilter{Filter: []byte{0xff, 0x00, 0xba, 0xad}})

	if !cs.cloudMayHold(held) {
		t.Fatal("undecodable frame must not replace the previous filter")
	}
	if cs.cloudMayHold(CloudKeyName(keyNameKey, []byte("absent"))) {
		t.Fatal("undecodable frame must not open the gate")
	}
}

// TestHandleMemberRemoveRoutesToHandler confirms a cloud-pushed MemberRemove
// response is applied through the wired handler rather than silently dropped.
func TestHandleMemberRemoveRoutesToHandler(t *testing.T) {
	cs := &CloudSync{}

	// No handler wired yet: the command is a no-op, not a panic.
	cs.handleMemberRemove(&dp.MemberRemove{NodeId: 7})

	// A nil callback stores a nil pointer and stays a no-op rather than
	// dereferencing a pointer to a nil func.
	cs.SetMemberRemoveHandler(nil)
	cs.handleMemberRemove(&dp.MemberRemove{NodeId: 8})

	var got uint64
	cs.SetMemberRemoveHandler(func(nodeID uint64) { got = nodeID })
	cs.handleMemberRemove(&dp.MemberRemove{NodeId: 42})
	if got != 42 {
		t.Fatalf("member remove not routed to handler: got node_id %d, want 42", got)
	}
}

// TestOwnFilterGrowth: the own-uploads filter doubles as it fills and never
// loses a name across doublings — a false negative would free-miss a key
// whose upload the cloud frame doesn't cover yet.
func TestOwnFilterGrowth(t *testing.T) {
	var f ownFilter
	const n = 10_000 // enough to force several doublings from the 4KB floor
	names := make([][]byte, n)
	for i := range n {
		names[i] = []byte{byte(i), byte(i >> 8), 'o', 'w', 'n'}
		f.add(names[i])
	}
	if len(f.blooms) < 2 {
		t.Fatalf("expected the filter to grow, still %d bloom(s)", len(f.blooms))
	}
	for i, name := range names {
		if !f.has(effects.BloomHash(name)) {
			t.Fatalf("false negative for name %d after growth", i)
		}
	}
}

// TestPendingTipsIndex covers the outbox's per-key tip index: enqueue makes a
// key's un-acked tips visible to the read-miss path, retire removes them, and
// the last retire clears the key entirely.
func TestPendingTipsIndex(t *testing.T) {
	engine := effects.NewEngine(effects.EngineConfig{NodeID: 1})
	defer func() {
		if err := engine.Close(); err != nil {
			t.Fatalf("close engine: %v", err)
		}
	}()
	cs := &CloudSync{
		engine:      engine,
		keyNameKey:  DeriveKeyNameKey("outbox-test-secret"),
		pending:     make(map[effects.Tip]*pb.Effect),
		pendingKeys: make(map[string]*pendingKeyEntry),
		wake:        make(chan struct{}, 1),
	}

	effA := &pb.Effect{Key: []byte("k")}
	effB := &pb.Effect{Key: []byte("k")}
	tipA := effects.Tip{1, 10}
	tipB := effects.Tip{1, 11}
	engine.EffectCache().PutSized(tipA, effA, 64)
	engine.EffectCache().PutSized(tipB, effB, 64)

	cs.handleLocalEffect(tipA, effA)
	cs.handleLocalEffect(tipB, effB)
	if got := cs.pendingTipsFor("k"); len(got) != 2 {
		t.Fatalf("expected both un-acked tips on k, got %v", got)
	}

	cs.retire(tipA)
	if got := cs.pendingTipsFor("k"); len(got) != 1 || got[0] != tipB {
		t.Fatalf("expected only %v after retiring %v, got %v", tipB, tipA, got)
	}
	cs.retire(tipB)
	if got := cs.pendingTipsFor("k"); got != nil {
		t.Fatalf("expected no pending tips after full retire, got %v", got)
	}
	if len(cs.pendingKeys) != 0 {
		t.Fatal("pendingKeys entry should be removed with its last tip")
	}
}

// TestOutboxPinsKeyUntilDrained: the outbox holds a do-not-evict pin on a key
// from its first un-acked upload to its last ack — evicting mid-flight would
// unsubscribe and drop the tips while this node holds the only copy, hiding
// committed data from every reader in the cluster.
func TestOutboxPinsKeyUntilDrained(t *testing.T) {
	engine := effects.NewEngine(effects.EngineConfig{NodeID: 1})
	defer func() {
		if err := engine.Close(); err != nil {
			t.Fatalf("close engine: %v", err)
		}
	}()
	cs := &CloudSync{
		engine:      engine,
		keyNameKey:  DeriveKeyNameKey("pin-test-secret"),
		pending:     make(map[effects.Tip]*pb.Effect),
		pendingKeys: make(map[string]*pendingKeyEntry),
		wake:        make(chan struct{}, 1),
	}

	// A real emit creates the key's index leaf, as it has always done by the
	// time handleLocalEffect fires in production.
	const key = "pinned-key"
	ectx := engine.NewContext()
	if err := ectx.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:    pb.EffectOp_INSERT_OP,
			Value: &pb.DataEffect_Raw{Raw: []byte("v")},
		}},
	}); err != nil {
		t.Fatalf("emit: %v", err)
	}
	if err := ectx.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	effA := &pb.Effect{Key: []byte(key)}
	effB := &pb.Effect{Key: []byte(key)}
	tipA := effects.Tip{1, 1000}
	tipB := effects.Tip{1, 1001}
	engine.EffectCache().PutSized(tipA, effA, 64)
	engine.EffectCache().PutSized(tipB, effB, 64)

	cs.handleLocalEffect(tipA, effA)
	if !cs.pendingKeys[key].pinned {
		t.Fatal("first enqueue on a live key must take the eviction pin")
	}
	cs.handleLocalEffect(tipB, effB)

	cs.retire(tipA)
	if _, held := cs.pendingKeys[key]; !held {
		t.Fatal("key entry must survive while a tip is still un-acked")
	}
	// The last retire must UnpinKey exactly once — a second release would
	// underflow the leaf's pin count and panic.
	cs.retire(tipB)

	// Balanced count check: a fresh pin/unpin cycle on the drained key panics
	// if the drain under- or over-released.
	if !engine.PinKey(key) {
		t.Fatal("key leaf disappeared after drain")
	}
	engine.UnpinKey(key)

	// A key with no live leaf (the eviction path's own unsubscribe mint):
	// enqueue takes no pin, drain releases none.
	ghost := &pb.Effect{Key: []byte("never-indexed")}
	ghostTip := effects.Tip{1, 1002}
	engine.EffectCache().PutSized(ghostTip, ghost, 64)
	cs.handleLocalEffect(ghostTip, ghost)
	if cs.pendingKeys["never-indexed"].pinned {
		t.Fatal("enqueue on a leafless key must not claim a pin")
	}
	cs.retire(ghostTip)
}

// TestSubscriptionMintDoesNotOpenGate: ensureSubscribed mints a
// SubscriptionEffect on every read of an absent key, moments before that read
// reaches the cloudMayHold gate. The mint must upload (the DAG needs the blob
// for dep-references) but must NOT enter filterOwn — otherwise every
// first-touch read opens its own gate and consults the cloud for a stateless
// chain.
func TestSubscriptionMintDoesNotOpenGate(t *testing.T) {
	engine := effects.NewEngine(effects.EngineConfig{NodeID: 1})
	defer func() {
		if err := engine.Close(); err != nil {
			t.Fatalf("close engine: %v", err)
		}
	}()
	keyNameKey := DeriveKeyNameKey("sub-gate-test-secret")
	cs := &CloudSync{
		engine:      engine,
		keyNameKey:  keyNameKey,
		pending:     make(map[effects.Tip]*pb.Effect),
		pendingKeys: make(map[string]*pendingKeyEntry),
		wake:        make(chan struct{}, 1),
	}
	// The cloud has pushed an (empty) filter — the gate is armed.
	cs.handleFilter(&dp.KeyFilter{Filter: filterFrame(t)})

	sub := &pb.Effect{
		Key:  []byte("k"),
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{SubscriberNodeId: 1}},
	}
	tip := effects.Tip{1, 10}
	engine.EffectCache().PutSized(tip, sub, 64)
	cs.handleLocalEffect(tip, sub)

	if cs.cloudMayHold(CloudKeyName(keyNameKey, []byte("k"))) {
		t.Fatal("a subscription mint must not open the read-miss gate for its key")
	}
	if got := cs.pendingTipsFor("k"); len(got) != 1 || got[0] != tip {
		t.Fatalf("the subscription must still upload, got outbox tips %v", got)
	}

	data := &pb.Effect{
		Key: []byte("k"),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:    pb.EffectOp_INSERT_OP,
			Value: &pb.DataEffect_Raw{Raw: []byte("v")},
		}},
	}
	dataTip := effects.Tip{1, 11}
	engine.EffectCache().PutSized(dataTip, data, 64)
	cs.handleLocalEffect(dataTip, data)

	if !cs.cloudMayHold(CloudKeyName(keyNameKey, []byte("k"))) {
		t.Fatal("a data mint must open the gate for its key")
	}
}

func TestDiscoverySkipsOnlyDependencyLessSubscriptions(t *testing.T) {
	plainSub := &pb.Effect{
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{}},
	}
	unsub := &pb.Effect{
		Deps: []*pb.EffectRef{{NodeId: 7, Offset: 3}},
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
			Unsubscribe: true,
		}},
	}
	data := &pb.Effect{Kind: &pb.Effect_Data{Data: &pb.DataEffect{}}}

	if !discoverySkipsTip(plainSub) {
		t.Fatal("dependency-less subscription must not keep the LCA queue open")
	}
	if discoverySkipsTip(unsub) {
		t.Fatal("unsubscribe dependencies must remain reachable from the cloud frontier")
	}
	if discoverySkipsTip(data) {
		t.Fatal("data tip was mistaken for subscription metadata")
	}
}

// TestDiscoverMembersNormalizesCandidateMarkersBeforeMissingVerdict pins the
// Cloud tips contract: returned refs are marker candidates, not authoritative
// logical tips. A stale marker whose blob is gone must not poison discovery
// when a readable state snapshot candidate dep-references and supersedes it.
func TestDiscoverMembersNormalizesCandidateMarkersBeforeMissingVerdict(t *testing.T) {
	enc, err := effects.NewEncryptorFromIKM([]byte("candidate-frontier-test-ikm"))
	if err != nil {
		t.Fatalf("encryptor: %v", err)
	}
	stale := effects.Tip{77, 1}
	real := effects.Tip{88, 2}
	wantState := &pb.ReducedEffect{
		Collection: pb.CollectionKind_SCALAR,
		Scalar: &pb.DataEffect{
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: []byte("membership-state")},
		},
	}
	realEff := &pb.Effect{
		Key:  []byte("__swytch:members"),
		Deps: []*pb.EffectRef{{NodeId: stale[0], Offset: stale[1]}},
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{State: wantState}},
	}
	cs := &CloudSync{
		enc:        enc,
		keyNameKey: DeriveKeyNameKey("candidate-frontier-test-secret"),
		folder:     "test-folder",
	}
	realEnv, err := cs.buildEnvelope(real, realEff)
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	cs.client = &staticTipsClient{response: &dp.GetTipsResponse{Keys: []*dp.KeyTips{{
		Tips: []*dp.EffectRef{
			{NodeId: stale[0], Offset: stale[1]},
			{NodeId: real[0], Offset: real[1]},
		},
		Closure: []*dp.Effect{realEnv},
	}}}}
	missingFetches := 0
	cs.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		missingFetches++
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     make(http.Header),
		}, nil
	})}

	got, err := cs.DiscoverMembers(context.Background(), "__swytch:members")
	if err != nil {
		t.Fatalf("discover members: %v", err)
	}
	if got == nil || got.GetScalar() == nil || string(got.GetScalar().GetRaw()) != "membership-state" {
		t.Fatalf("discovered state = %v, want snapshot scalar %q", got, "membership-state")
	}
	if missingFetches != 1 {
		t.Fatalf("missing stale marker fetched %d times, want one normalization probe", missingFetches)
	}
}

// TestCloudTipsGatedSkipsRPC: a filter-negative CloudTips returns nil without
// touching the network — the nil client proves it, since any RPC attempt
// would panic.
func TestCloudTipsGatedSkipsRPC(t *testing.T) {
	keyNameKey := DeriveKeyNameKey("gate-test-secret")
	cs := &CloudSync{keyNameKey: keyNameKey}
	cs.handleFilter(&dp.KeyFilter{Filter: filterFrame(t, CloudKeyName(keyNameKey, []byte("held")))})

	tips, closure, err := cs.CloudTips(context.Background(), "absent")
	if err != nil {
		t.Fatalf("gated CloudTips errored: %v", err)
	}
	if tips != nil || closure != nil {
		t.Fatalf("gated CloudTips returned tips: %v (closure %v)", tips, closure)
	}
}

// TestFetchEffectServesOutbox pins the outbox-before-CDN order in fetchEffect:
// an un-acked mint's bytes come from cs.pending, with no HTTP attempt — the
// CDN cannot hold an effect that hasn't been uploaded, and a read that lands
// here (evicted key whose frontier is an outbox tip) must not fail on a 404
// the process can answer itself.
func TestFetchEffectServesOutbox(t *testing.T) {
	tip := effects.Tip{7, 42}
	want := &pb.Effect{Key: []byte("k")}
	cs := &CloudSync{
		pending: map[effects.Tip]*pb.Effect{tip: want},
		// No httpClient: reaching for the CDN would nil-panic, which is the
		// test's proof that the outbox short-circuits the fetch.
	}

	got, err := cs.fetchEffect(context.Background(), tip)
	if err != nil {
		t.Fatalf("fetchEffect: %v", err)
	}
	if got != want {
		t.Fatalf("fetchEffect returned %v, want the outbox effect", got)
	}
}

// TestBuildEnvelopeDeclaresRawSize pins raw_size to the pre-seal marshal
// length. The cloud bills this field, and it must not move with compression
// or seal overhead — the payload here is highly compressible precisely so the
// sealed length diverges from the raw one.
func TestBuildEnvelopeDeclaresRawSize(t *testing.T) {
	enc, err := effects.NewEncryptorFromIKM([]byte("raw-size-test-ikm"))
	if err != nil {
		t.Fatalf("encryptor: %v", err)
	}
	cs := &CloudSync{keyNameKey: DeriveKeyNameKey("raw-size-secret"), enc: enc}

	eff := &pb.Effect{
		Key: []byte("k"),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:    pb.EffectOp_INSERT_OP,
			Value: &pb.DataEffect_Raw{Raw: make([]byte, 4096)},
		}},
	}
	raw, err := effects.MarshalEffect(eff)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	env, err := cs.buildEnvelope(effects.Tip{1, 2}, eff)
	if err != nil {
		t.Fatalf("buildEnvelope: %v", err)
	}
	if env.GetRawSize() != uint64(len(raw)) {
		t.Fatalf("raw_size = %d, want pre-seal length %d", env.GetRawSize(), len(raw))
	}
	if uint64(len(env.GetRawEffect())) == env.GetRawSize() {
		t.Fatal("sealed length equals raw_size; the test payload no longer exercises the divergence")
	}
}

// ancestrySync builds a CloudSync over a locally-authored chain root <- mid <-
// ours, where only the newest mint reaches OnLocalEffect.
func ancestrySync(t *testing.T, engine *effects.Engine) (*CloudSync, effects.Tip, effects.Tip, effects.Tip, *pb.Effect) {
	t.Helper()
	enc, err := effects.NewEncryptorFromIKM([]byte("ancestry-test-ikm"))
	if err != nil {
		t.Fatalf("encryptor: %v", err)
	}
	cs := &CloudSync{
		engine:      engine,
		enc:         enc,
		keyNameKey:  DeriveKeyNameKey("ancestry-secret"),
		pending:     make(map[effects.Tip]*pb.Effect),
		pendingKeys: make(map[string]*pendingKeyEntry),
		wake:        make(chan struct{}, 1),
	}

	localNode := uint64(engine.NodeID())
	root, mid, ours := effects.Tip{localNode, 1}, effects.Tip{localNode, 2}, effects.Tip{localNode, 3}
	rootEff := &pb.Effect{Key: []byte("k")}
	midEff := &pb.Effect{Key: []byte("k"), Deps: []*pb.EffectRef{{NodeId: root[0], Offset: root[1]}}}
	oursEff := &pb.Effect{Key: []byte("k"), Deps: []*pb.EffectRef{{NodeId: mid[0], Offset: mid[1]}}}
	engine.EffectCache().PutSized(root, rootEff, 64)
	engine.EffectCache().PutSized(mid, midEff, 64)
	engine.EffectCache().PutSized(ours, oursEff, 64)
	return cs, root, mid, ours, oursEff
}

// TestUnsentAncestryUploads verifies that a local mint carries the locally-
// authored history Cloud may not have seen yet.
func TestUnsentAncestryUploads(t *testing.T) {
	engine := effects.NewEngine(effects.EngineConfig{NodeID: 1})
	defer func() {
		if err := engine.Close(); err != nil {
			t.Fatalf("close engine: %v", err)
		}
	}()
	cs, root, mid, ours, oursEff := ancestrySync(t, engine)

	cs.handleLocalEffect(ours, oursEff)

	// The emit path walks one dep level; chasing the rest of the closure there
	// would put an unbounded walk inside every write.
	if _, queued := cs.pending[mid]; !queued {
		t.Fatal("the mint's own dep must be enqueued on the emit path")
	}
	if _, queued := cs.pending[root]; queued {
		t.Fatal("the deeper ancestor must wait for the send-path walk")
	}

	for range 8 {
		if _, ok := cs.nextEnvelope(); !ok {
			break
		}
	}
	if _, queued := cs.pending[root]; !queued {
		t.Fatal("draining the outbox must complete the ancestry closure")
	}

	// Ancestors are interior nodes of someone else's chain, not this node's
	// frontier: answering a read-miss with them would name an effect and its
	// own ancestor as the key's tips.
	got := cs.pendingTipsFor("k")
	if len(got) != 1 || got[0] != ours {
		t.Fatalf("outbox frontier on k = %v, want only the local mint %v", got, ours)
	}
}

func TestForeignDependenciesRequireExplicitCloudFetch(t *testing.T) {
	engine := effects.NewEngine(effects.EngineConfig{NodeID: 1})
	defer func() {
		if err := engine.Close(); err != nil {
			t.Fatalf("close engine: %v", err)
		}
	}()
	cs, _, _, ours, oursEff := ancestrySync(t, engine)
	foreignNode := uint64(engine.NodeID()) ^ 1
	foreignRoot := effects.Tip{foreignNode, 1}
	foreignDep := effects.Tip{foreignNode, 2}
	rootEff := &pb.Effect{Key: []byte("k")}
	depEff := &pb.Effect{Key: []byte("k"), Deps: []*pb.EffectRef{{NodeId: foreignRoot[0], Offset: foreignRoot[1]}}}
	engine.EffectCache().PutSized(foreignRoot, rootEff, 64)
	engine.EffectCache().PutSized(foreignDep, depEff, 64)
	oursEff.Deps = []*pb.EffectRef{{NodeId: foreignDep[0], Offset: foreignDep[1]}}

	cs.handleLocalEffect(ours, oursEff)
	if _, queued := cs.pending[foreignDep]; queued {
		t.Fatal("foreign dependency entered the outbox without a Cloud fetch")
	}
	if _, queued := cs.pending[foreignRoot]; queued {
		t.Fatal("foreign ancestry entered the outbox without a Cloud fetch")
	}
	if env, ok := cs.nextEnvelope(); !ok {
		t.Fatal("local mint was not queued")
	} else if got := (effects.Tip{env.GetId().GetNodeId(), env.GetId().GetOffset()}); got != ours {
		t.Fatalf("first upload = %v, want local mint %v", got, ours)
	}

	cs.handleFetch(&dp.FetchRequest{Ref: &dp.EffectRef{NodeId: foreignDep[0], Offset: foreignDep[1]}})
	if _, queued := cs.pending[foreignDep]; !queued {
		t.Fatal("explicitly requested foreign dependency did not enter the outbox")
	}
	env, ok := cs.nextEnvelope()
	if !ok {
		t.Fatal("explicitly requested foreign dependency was not sent")
	}
	if got := (effects.Tip{env.GetId().GetNodeId(), env.GetId().GetOffset()}); got != foreignDep {
		t.Fatalf("fetch reply = %v, want exactly %v", got, foreignDep)
	}
	if _, queued := cs.pending[foreignRoot]; queued {
		t.Fatal("fetch reply recursively queued a dependency Cloud did not request")
	}
}

// TestUnsentAncestryUploadsOldestFirst covers the shutdown-sensitive ordering:
// the cloud must see root and mid before it sees the new head that names them.
// Sending head-first lets the cloud advertise a dangling frontier if this node
// exits as the last holder before the ancestry backfill completes.
func TestUnsentAncestryUploadsOldestFirst(t *testing.T) {
	engine := effects.NewEngine(effects.EngineConfig{NodeID: 1})
	defer func() {
		if err := engine.Close(); err != nil {
			t.Fatalf("close engine: %v", err)
		}
	}()
	cs, root, mid, ours, oursEff := ancestrySync(t, engine)

	cs.handleLocalEffect(ours, oursEff)
	var got []effects.Tip
	for range 3 {
		env, ok := cs.nextEnvelope()
		if !ok {
			t.Fatalf("outbox ended after %d envelopes", len(got))
		}
		got = append(got, effects.Tip{env.GetId().GetNodeId(), env.GetId().GetOffset()})
		cs.handleAck(&dp.WriteAck{Id: env.GetId(), Ok: true})
	}
	want := []effects.Tip{root, mid, ours}
	if !slices.Equal(got, want) {
		t.Fatalf("upload order = %v, want oldest-first %v", got, want)
	}
}

// TestUnsentAncestryWaitsForParentAck covers the distinction between send
// order and Cloud commit order. Cloud pipelines one stream through a shared
// worker pool, so merely putting root before child on the wire still lets the
// child's dependency check win the race. The child must remain queued until
// the parent is durably ACKed.
func TestUnsentAncestryWaitsForParentAck(t *testing.T) {
	engine := effects.NewEngine(effects.EngineConfig{NodeID: 1})
	defer func() {
		if err := engine.Close(); err != nil {
			t.Fatalf("close engine: %v", err)
		}
	}()
	cs, root, mid, ours, oursEff := ancestrySync(t, engine)

	cs.handleLocalEffect(ours, oursEff)
	assertNext := func(want effects.Tip) *dp.Effect {
		t.Helper()
		env, ok := cs.nextEnvelope()
		if !ok {
			t.Fatalf("outbox ended before %v", want)
		}
		got := effects.Tip{env.GetId().GetNodeId(), env.GetId().GetOffset()}
		if got != want {
			t.Fatalf("next upload = %v, want %v", got, want)
		}
		return env
	}

	rootEnv := assertNext(root)
	if env, ok := cs.nextEnvelope(); ok {
		t.Fatalf("uploaded %v before parent %v was ACKed", env.GetId(), root)
	}
	cs.handleAck(&dp.WriteAck{Id: rootEnv.GetId(), Ok: true})

	midEnv := assertNext(mid)
	if env, ok := cs.nextEnvelope(); ok {
		t.Fatalf("uploaded %v before parent %v was ACKed", env.GetId(), mid)
	}
	cs.handleAck(&dp.WriteAck{Id: midEnv.GetId(), Ok: true})
	assertNext(ours)
}

// TestUnsentAncestryReconnectOrderDoesNotStrand covers runStream rebuilding
// sendQ from a map in arbitrary order. If the last item inspected discovers a
// deeper ancestor, that newly queued work must remain in the current scan;
// there is no ACK yet to wake another one.
func TestUnsentAncestryReconnectOrderDoesNotStrand(t *testing.T) {
	engine := effects.NewEngine(effects.EngineConfig{NodeID: 1})
	defer func() {
		if err := engine.Close(); err != nil {
			t.Fatalf("close engine: %v", err)
		}
	}()
	cs, root, mid, ours, oursEff := ancestrySync(t, engine)

	cs.handleLocalEffect(ours, oursEff)
	cs.mu.Lock()
	cs.sendQ = []effects.Tip{ours, mid}
	cs.mu.Unlock()

	env, ok := cs.nextEnvelope()
	if !ok {
		t.Fatal("adverse reconnect order stranded newly discovered ancestry")
	}
	if got := (effects.Tip{env.GetId().GetNodeId(), env.GetId().GetOffset()}); got != root {
		t.Fatalf("first upload after reconnect = %v, want root %v", got, root)
	}
}

// TestFetchReplyUsesAckGatedOutbox ensures a Cloud-requested effect takes the
// pinned, retryable path but does not recursively queue refs Cloud did not ask
// for. The dataplane lands the requested repair and asks for deeper holes itself.
func TestFetchReplyUsesAckGatedOutbox(t *testing.T) {
	engine := effects.NewEngine(effects.EngineConfig{NodeID: 1})
	defer func() {
		if err := engine.Close(); err != nil {
			t.Fatalf("close engine: %v", err)
		}
	}()
	cs, root, mid, ours, _ := ancestrySync(t, engine)

	cs.handleFetch(&dp.FetchRequest{Ref: &dp.EffectRef{NodeId: ours[0], Offset: ours[1]}})
	assertNext := func(want effects.Tip) *dp.Effect {
		t.Helper()
		env, ok := cs.nextEnvelope()
		if !ok {
			t.Fatalf("fetch outbox ended before %v", want)
		}
		got := effects.Tip{env.GetId().GetNodeId(), env.GetId().GetOffset()}
		if got != want {
			t.Fatalf("next fetch upload = %v, want %v", got, want)
		}
		return env
	}

	oursEnv := assertNext(ours)
	if _, queued := cs.pending[mid]; queued {
		t.Fatal("fetch reply recursively queued its direct dependency")
	}
	if _, queued := cs.pending[root]; queued {
		t.Fatal("fetch reply recursively queued deeper ancestry")
	}
	cs.handleAck(&dp.WriteAck{Id: oursEnv.GetId(), Ok: true})
	cs.mu.Lock()
	pending := len(cs.pending)
	cs.mu.Unlock()
	if pending != 0 {
		t.Fatalf("fetch outbox retained %d effects after ACKs", pending)
	}
}

// TestCloudHeldAncestryStops: the cloud served the rehydrated part of this
// chain in the first place, so re-uploading it on every local write to the key
// is pure waste. Acks and cloud-served effects are the walk's stop rule.
func TestCloudHeldAncestryStops(t *testing.T) {
	engine := effects.NewEngine(effects.EngineConfig{NodeID: 1})
	defer func() {
		if err := engine.Close(); err != nil {
			t.Fatalf("close engine: %v", err)
		}
	}()
	cs, root, mid, ours, oursEff := ancestrySync(t, engine)

	cs.markCloudHolds(mid)
	cs.handleLocalEffect(ours, oursEff)
	for range 8 {
		if _, ok := cs.nextEnvelope(); !ok {
			break
		}
	}

	if _, queued := cs.pending[mid]; queued {
		t.Fatal("an effect the cloud already holds must not be re-uploaded")
	}
	if _, queued := cs.pending[root]; queued {
		t.Fatal("the walk must stop at the first cloud-held ancestor, not walk past it")
	}
}

// TestTipGenSetRotation: the known-in-cloud set is bounded, and overflowing it
// may only cost a re-upload the cloud re-acks — never a skipped ancestor,
// which would leave exactly the hole the walk exists to close.
func TestTipGenSetRotation(t *testing.T) {
	var s tipGenSet
	for i := range cloudHasGenSize*2 + 1 {
		s.add(effects.Tip{1, uint64(i)})
	}
	if newest := (effects.Tip{1, cloudHasGenSize * 2}); !s.has(newest) {
		t.Fatalf("most recent add %v missing after rotation", newest)
	}
	if s.has(effects.Tip{1, 0}) {
		t.Fatal("the oldest generation should have been dropped")
	}
	if total := len(s.cur) + len(s.prev); total > 2*cloudHasGenSize {
		t.Fatalf("set holds %d tips, above the %d bound", total, 2*cloudHasGenSize)
	}
}
