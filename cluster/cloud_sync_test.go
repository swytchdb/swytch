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
	"testing"

	pb "github.com/swytchdb/swytch/cluster/proto"
	dp "github.com/swytchdb/swytch/cluster/proto/dataplane"
	"github.com/swytchdb/swytch/effects"
)

// filterFrame marshals a CuckooChain over the given PRF names into the wire
// bytes a KeyFilter frame carries.
func filterFrame(t *testing.T, names ...[]byte) []byte {
	t.Helper()
	var cc effects.CuckooChain
	for _, n := range names {
		cc.Add(string(n))
	}
	b, err := cc.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal filter: %v", err)
	}
	return b
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
	cs.filterOwn.Add(string(ours))
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
