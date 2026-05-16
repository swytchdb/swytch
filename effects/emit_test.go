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
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	clox "github.com/swytchdb/swytch/cache"
	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/crdt"
	"github.com/swytchdb/swytch/keytrie"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// --- mock broadcaster ---

type mockBroadcaster struct {
	mu               sync.Mutex
	broadcasts       []*pb.OffsetNotify
	replicates       []*pb.OffsetNotify
	replicateErr     error
	replicateToPeers []pb.NodeID // tracked ReplicateTo calls
	peerIDs          []pb.NodeID
	nackResponses    map[pb.NodeID]*pb.NackNotify // per-peer ReplicateTo NACK responses
	sentNacks        []*pb.NackNotify
	// nackTarget: if set, ReplicateTo sends an empty NACK to this engine
	// for the key in the notify, simulating the full bootstrap protocol.
	nackTarget *Engine

	allRegionPeersReachable bool
}

func (m *mockBroadcaster) BroadcastWithData(notify *pb.OffsetNotify, effectData []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.broadcasts = append(m.broadcasts, notify)
}
func (m *mockBroadcaster) Broadcast(notify *pb.OffsetNotify) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.broadcasts = append(m.broadcasts, notify)
}
func (m *mockBroadcaster) Replicate(notify *pb.OffsetNotify, wireData []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replicates = append(m.replicates, notify)
	return m.replicateErr
}
func (m *mockBroadcaster) ReplicateTo(notify *pb.OffsetNotify, wireData []byte, targetNodeID pb.NodeID) ([]*pb.NackNotify, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replicateToPeers = append(m.replicateToPeers, targetNodeID)
	m.replicates = append(m.replicates, notify)
	if m.nackResponses != nil {
		if nack, ok := m.nackResponses[targetNodeID]; ok {
			if nack != nil {
				return []*pb.NackNotify{nack}, m.replicateErr
			}
			return nil, m.replicateErr
		}
	}
	// Simulate the bootstrap protocol: send an empty NACK back through
	// the engine's HandleNack so subscription bootstrapping completes.
	if m.nackTarget != nil && len(notify.Key) != 0 {
		_ = m.nackTarget.HandleNack(&pb.NackNotify{Key: notify.Key})
	}
	return nil, m.replicateErr
}
func (m *mockBroadcaster) SendNack(nack *pb.NackNotify, targetNodeID pb.NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sentNacks = append(m.sentNacks, nack)
}
func (m *mockBroadcaster) FetchFromAny(ref *pb.EffectRef) ([]byte, error) { return nil, nil }
func (m *mockBroadcaster) Fetch(ref *pb.EffectRef) ([]byte, error) {
	return nil, nil
}
func (m *mockBroadcaster) PeerIDs() []pb.NodeID          { return m.peerIDs }
func (m *mockBroadcaster) AllRegionPeersReachable() bool { return m.allRegionPeersReachable }
func (m *mockBroadcaster) InMajorityPartition() bool     { return m.allRegionPeersReachable }
func (m *mockBroadcaster) ForwardTransaction(_ context.Context, _ pb.NodeID, _ *pb.ForwardedTransaction) (*pb.ForwardedResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

// --- helpers ---

func newTestEngine(bc Broadcaster) *Engine {
	e := &Engine{
		index:             keytrie.New(),
		broadcaster:       bc,
		nodeID:            42,
		clock:             crdt.NewHLC(),
		subscriptions:     xsync.NewMap[string, *subscriptionState](),
		pendingTxns:       xsync.NewMap[Tip, *pendingTxn](),
		pendingTxTips:     xsync.NewMap[Tip, []Tip](),
		txAbortCounts:     xsync.NewMap[string, *atomic.Int32](),
		pendingBootstraps: xsync.NewMap[string, *bootstrapCollector](),
		unsubInFlight:     xsync.NewMap[string, struct{}](),
		voidedBinds:       clox.NewCloxCache[string, struct{}](clox.ConfigFromCapacity(256)),
		effectCache:       clox.NewCloxCache[Tip, *pb.Effect](clox.ConfigFromMemorySize(1024 * 1024)),
	}
	e.safety.Store(&safetyMap{defaultMode: UnsafeMode})
	return e
}

func dataEffect(key string) *pb.Effect {
	return &pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:    pb.EffectOp_INSERT_OP,
			Merge: pb.MergeRule_LAST_WRITE_WINS,
		}},
	}
}

func metaEffect(key string) *pb.Effect {
	return &pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{
			ExpiresAt: timestamppb.New(time.Unix(60, 0)),
		}},
	}
}

// --- tests ---

func TestEmitSingle(t *testing.T) {
	e := newTestEngine(nil)
	ctx := e.NewContext()

	eff := dataEffect("foo")
	if err := ctx.Emit(eff); err != nil {
		t.Fatal(err)
	}

	// index NOT updated yet
	if tips := e.index.Contains("foo"); tips != nil {
		t.Fatal("index should not be updated before Flush")
	}

	// Flush updates index + no error (nil broadcaster)
	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	tips := e.index.Contains("foo")
	if tips == nil || tips.Len() != 1 {
		t.Fatal("expected 1 tip after Flush")
	}
}

func TestEmitTwoSameKey(t *testing.T) {
	e := newTestEngine(nil)
	ctx := e.NewContext()

	// First effect: data
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatal(err)
	}
	// Second effect: meta (deps should chain from first)
	if err := ctx.Emit(metaEffect("k")); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	// After flush: single tip (the meta effect)
	tips := e.index.Contains("k")
	if tips == nil || tips.Len() != 1 {
		t.Fatalf("expected 1 tip, got %v", tips)
	}

	// Verify dep chaining: the tip (meta effect) should depend on the first effect
	tipOff := tips.Tips()[0]
	cached, ok := e.effectCache.Get(tipOff, 0)
	if !ok {
		t.Fatal("expected effect in cache")
	}
	if len(cached.Deps) != 1 {
		t.Fatalf("expected 1 dep, got %v", cached.Deps)
	}
}

func TestEmitForkResolution(t *testing.T) {
	e := newTestEngine(nil)

	// Pre-populate index with 2 tips (simulating fork)
	e.index.Insert("k", nil, keytrie.NewTipSet(Tip{0, 10}, Tip{0, 20}))

	ctx := e.NewContext()
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	// Flush collapses to 1 tip
	tips := e.index.Contains("k")
	if tips == nil || tips.Len() != 1 {
		t.Fatal("expected 1 tip after fork resolution flush")
	}

	// Deps should include both original tips (fork resolution)
	tipOff := tips.Tips()[0]
	cached, ok := e.effectCache.Get(tipOff, 0)
	if !ok {
		t.Fatal("expected effect in cache")
	}
	if len(cached.Deps) != 2 {
		t.Fatalf("expected 2 deps (fork resolution), got %v", cached.Deps)
	}
}

func TestFlushSafeMode(t *testing.T) {
	bc := &mockBroadcaster{allRegionPeersReachable: true}
	e := newTestEngine(bc)
	e.safety.Store(&safetyMap{defaultMode: SafeMode})

	ctx := e.NewContext()
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	if len(bc.replicates) != 1 {
		t.Fatalf("expected 1 Replicate call, got %d", len(bc.replicates))
	}
	if len(bc.broadcasts) != 0 {
		t.Fatal("should not call BroadcastWithData in safe mode")
	}
}

func TestFlushSafeModeError_ReplicateFailAfterPrecheck(t *testing.T) {
	// When pre-check passes but Replicate fails, the write is already committed
	// (index updated). Flush should succeed — the pre-check is the safety gate.
	bc := &mockBroadcaster{replicateErr: errors.New("quorum unreachable"), allRegionPeersReachable: true}
	e := newTestEngine(bc)
	e.safety.Store(&safetyMap{defaultMode: SafeMode})

	ctx := e.NewContext()
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Flush(); err != nil {
		t.Fatalf("pre-check passed, write is committed — should not fail: %v", err)
	}
}

func TestFlushSafeMode_AllowsWhenAllPeersReachable(t *testing.T) {
	bc := &mockBroadcaster{allRegionPeersReachable: true}
	e := newTestEngine(bc)
	e.safety.Store(&safetyMap{defaultMode: SafeMode})

	ctx := e.NewContext()
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	if len(bc.replicates) != 1 {
		t.Fatalf("expected 1 Replicate call, got %d", len(bc.replicates))
	}
}

func TestFlushUnsafeMode_AllowsWhenPeersUnreachable(t *testing.T) {
	bc := &mockBroadcaster{allRegionPeersReachable: false}
	e := newTestEngine(bc)
	// UnsafeMode is the default from newTestEngine

	ctx := e.NewContext()
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatal(err)
	}

	// UnsafeMode should allow writes regardless of peer reachability
	if err := ctx.Flush(); err != nil {
		t.Fatal("UnsafeMode should allow writes even when peers are unreachable")
	}
}

func TestFlushTx_SafeMode_RejectsWhenPeersUnreachable(t *testing.T) {
	bc := &mockBroadcaster{allRegionPeersReachable: false}
	e := newTestEngine(bc)
	e.safety.Store(&safetyMap{defaultMode: SafeMode})

	ctx := e.NewContext()
	ctx.BeginTx()
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatal(err)
	}

	err := ctx.Flush()
	if err == nil {
		t.Fatal("expected error when not all region peers are reachable in SafeMode tx")
	}
	if !errors.Is(err, ErrRegionPartitioned) {
		t.Fatalf("expected ErrRegionPartitioned, got: %v", err)
	}
}

func TestFlushUnsafeMode(t *testing.T) {
	bc := &mockBroadcaster{}
	e := newTestEngine(bc)

	ctx := e.NewContext()
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	if len(bc.broadcasts) != 1 {
		t.Fatalf("expected 1 BroadcastWithData call, got %d", len(bc.broadcasts))
	}
	if len(bc.replicates) != 0 {
		t.Fatal("should not call Replicate in unsafe mode")
	}
}

func TestFlushStandalone(t *testing.T) {
	e := newTestEngine(nil)

	ctx := e.NewContext()
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestBeginTxSetsFlag(t *testing.T) {
	e := newTestEngine(nil)

	ctx := e.NewContext()
	ctx.BeginTx()
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	// Look up the emitted effect via the index and effectCache
	tips := e.index.Contains("k")
	if tips == nil {
		t.Fatal("expected index entry for key k")
	}
	// In a tx, the tip is the BIND; walk tips to find a data effect with TxnId
	found := false
	for _, off := range tips.Tips() {
		cached, ok := e.effectCache.Get(off, 0)
		if !ok {
			continue
		}
		if cached.TxnId != "" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected TxnId to be set on at least one effect")
	}
}

func TestAbort(t *testing.T) {
	e := newTestEngine(nil)

	ctx := e.NewContext()
	ctx.BeginTx()
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatal(err)
	}

	ctx.Abort()

	// Flush should be a no-op
	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	// Index unchanged
	if tips := e.index.Contains("k"); tips != nil {
		t.Fatal("index should not be updated after Abort")
	}
}

func TestAbortClearsWatchedKeys(t *testing.T) {
	e := newTestEngine(nil)

	ctx := e.NewContext()
	ctx.Watch("k1")
	ctx.Watch("k2")

	if len(ctx.watchedKeys) != 2 {
		t.Fatalf("expected 2 watchedKeys before Abort, got %d", len(ctx.watchedKeys))
	}

	ctx.BeginTx()
	ctx.Abort()

	if len(ctx.watchedKeys) != 0 {
		t.Fatalf("expected 0 watchedKeys after Abort, got %d", len(ctx.watchedKeys))
	}
}

func TestAbortClearsWatchedKeysWithoutTx(t *testing.T) {
	e := newTestEngine(nil)

	ctx := e.NewContext()
	ctx.Watch("k")

	if len(ctx.watchedKeys) != 1 {
		t.Fatalf("expected 1 watchedKey before Abort, got %d", len(ctx.watchedKeys))
	}

	// Abort without BeginTx — still must clear watchedKeys
	ctx.Abort()

	if len(ctx.watchedKeys) != 0 {
		t.Fatalf("expected 0 watchedKeys after Abort, got %d", len(ctx.watchedKeys))
	}
}

func TestAbortAllowsCleanReuse(t *testing.T) {
	e := newTestEngine(nil)

	ctx := e.NewContext()
	ctx.Watch("k")
	ctx.BeginTx()
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatal(err)
	}

	ctx.Abort()

	// All state should be reset: keys, watchedKeys, inTx, txnID, txSnapshot
	if len(ctx.keys) != 0 {
		t.Fatal("keys should be empty after Abort")
	}
	if len(ctx.watchedKeys) != 0 {
		t.Fatal("watchedKeys should be empty after Abort")
	}
	if ctx.inTx {
		t.Fatal("inTx should be false after Abort")
	}
	if ctx.txnID != "" {
		t.Fatal("txnID should be empty after Abort")
	}
	if ctx.txSnapshot != nil {
		t.Fatal("txSnapshot should be nil after Abort")
	}
}

func TestCASRetry(t *testing.T) {
	e := newTestEngine(nil)

	ctx := e.NewContext()
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatal(err)
	}

	// Simulate concurrent writer: inject a new tip before Flush
	e.index.Insert("k", nil, keytrie.NewTipSet(Tip{0, 999}))

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	// Should have merged: both 999 (concurrent) and our emitted offset
	tips := e.index.Contains("k")
	if tips == nil {
		t.Fatal("expected tips after CAS retry")
	}
	if !tips.Contains(Tip{0, 999}) {
		t.Fatalf("expected tips to contain 999, got %v", tips.Tips())
	}
	if len(tips.Tips()) != 2 {
		t.Fatalf("expected 2 tips (concurrent + ours), got %v", tips.Tips())
	}
}

func TestNotifyEffectData(t *testing.T) {
	bc := &mockBroadcaster{}
	e := newTestEngine(bc)

	ctx := e.NewContext()
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatal(err)
	}
	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	if len(bc.broadcasts) != 1 {
		t.Fatal("expected 1 broadcast")
	}
	notify := bc.broadcasts[0]
	if len(notify.EffectData) == 0 {
		t.Fatal("EffectData should carry serialized effect bytes")
	}
	// EffectData is wire format: [4-byte LE keyLen][key][protoData]
	wireData := notify.EffectData
	if len(wireData) < 4 {
		t.Fatal("wire data too short")
	}
	keyLen := binary.LittleEndian.Uint32(wireData[:4])
	if keyLen == 0 {
		t.Fatal("expected non-zero keyLen in wire format")
	}
	protoData := wireData[4+keyLen:]
	var eff pb.Effect
	if err := proto.Unmarshal(protoData, &eff); err != nil {
		t.Fatal(err)
	}
	if string(eff.Key) != "k" {
		t.Fatalf("expected key=k, got %s", eff.Key)
	}
}

func TestMultipleKeys(t *testing.T) {
	bc := &mockBroadcaster{}
	e := newTestEngine(bc)

	ctx := e.NewContext()
	if err := ctx.Emit(dataEffect("a")); err != nil {
		t.Fatal(err)
	}
	if err := ctx.Emit(dataEffect("b")); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	// Both keys in index
	if e.index.Contains("a") == nil {
		t.Fatal("expected index entry for key a")
	}
	if e.index.Contains("b") == nil {
		t.Fatal("expected index entry for key b")
	}

	// Both broadcast
	if len(bc.broadcasts) != 2 {
		t.Fatalf("expected 2 broadcasts, got %d", len(bc.broadcasts))
	}
}

func TestIndexInvisibleBeforeFlush(t *testing.T) {
	e := newTestEngine(nil)

	ctx := e.NewContext()
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatal(err)
	}

	// Before flush: index still empty
	if tips := e.index.Contains("k"); tips != nil {
		t.Fatal("index should not be updated before Flush")
	}

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	// After flush: visible
	if tips := e.index.Contains("k"); tips == nil {
		t.Fatal("index should be updated after Flush")
	}
}

// --- txn mock broadcaster ---

type txnMockBroadcaster struct {
	mockBroadcaster
	replicateToResults map[pb.NodeID]*pb.NackNotify // nodeID → NackNotify (nil = ACK)
	replicateToErr     error
	replicateToCalls   []pb.NodeID // node IDs called
}

func (m *txnMockBroadcaster) ReplicateTo(notify *pb.OffsetNotify, wireData []byte, targetNodeID pb.NodeID) ([]*pb.NackNotify, error) {
	m.replicateToCalls = append(m.replicateToCalls, targetNodeID)
	if m.replicateToErr != nil {
		return nil, m.replicateToErr
	}
	if m.replicateToResults != nil {
		if nack := m.replicateToResults[targetNodeID]; nack != nil {
			return []*pb.NackNotify{nack}, nil
		}
		return nil, nil
	}
	return nil, nil
}

// --- txn integration tests ---

func newTxnTestEngine(bc Broadcaster) *Engine {
	e := &Engine{
		index:             keytrie.New(),
		broadcaster:       bc,
		nodeID:            42,
		clock:             crdt.NewHLC(),
		subscriptions:     xsync.NewMap[string, *subscriptionState](),
		pendingTxns:       xsync.NewMap[Tip, *pendingTxn](),
		pendingTxTips:     xsync.NewMap[Tip, []Tip](),
		txAbortCounts:     xsync.NewMap[string, *atomic.Int32](),
		pendingBootstraps: xsync.NewMap[string, *bootstrapCollector](),
		unsubInFlight:     xsync.NewMap[string, struct{}](),
		voidedBinds:       clox.NewCloxCache[string, struct{}](clox.ConfigFromCapacity(256)),
		effectCache:       clox.NewCloxCache[Tip, *pb.Effect](clox.ConfigFromMemorySize(1024 * 1024)),
	}
	e.safety.Store(&safetyMap{defaultMode: UnsafeMode})
	return e
}

func TestFlushTx_NoSubscribers_CommitsImmediately(t *testing.T) {
	bc := &txnMockBroadcaster{}
	e := newTxnTestEngine(bc)

	ctx := e.NewContext()
	ctx.BeginTx()
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	// Index should be updated
	if tips := e.index.Contains("k"); tips == nil {
		t.Fatal("expected index entry for key k after tx commit")
	}

	// No ReplicateTo calls (no subscribers)
	if len(bc.replicateToCalls) != 0 {
		t.Fatalf("expected 0 ReplicateTo calls, got %d", len(bc.replicateToCalls))
	}
}

func TestFlushTx_NoNACKs_Commits(t *testing.T) {
	bc := &txnMockBroadcaster{}
	e := newTxnTestEngine(bc)

	// Pre-populate subscriber info: write a subscription effect to the key
	// so that GetSnapshot sees a subscriber
	subCtx := e.NewContext()
	subEff := &pb.Effect{
		Key: []byte("k"),
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
			SubscriberNodeId: 99,
		}},
	}
	if err := subCtx.Emit(subEff); err != nil {
		t.Fatal(err)
	}
	if err := subCtx.Flush(); err != nil {
		t.Fatal(err)
	}

	// Now do the transactional write
	ctx := e.NewContext()
	ctx.BeginTx()
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestFlushTx_RealConflict_Aborts(t *testing.T) {
	bc := &txnMockBroadcaster{
		replicateToResults: map[pb.NodeID]*pb.NackNotify{
			99: {
				Key: []byte("k"),
				TipDetails: []*pb.NackTipDetail{
					{
						Ref:        &pb.EffectRef{NodeId: 0, Offset: 700},
						Hlc:        timestamppb.New(time.Unix(0, 150)),
						IsData:     true,
						Collection: pb.CollectionKind_SCALAR,
					},
				},
			},
		},
	}
	e := newTxnTestEngine(bc)

	// Add subscriber
	subCtx := e.NewContext()
	subEff := &pb.Effect{
		Key: []byte("k"),
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
			SubscriberNodeId: 99,
		}},
	}
	if err := subCtx.Emit(subEff); err != nil {
		t.Fatal(err)
	}
	if err := subCtx.Flush(); err != nil {
		t.Fatal(err)
	}

	// Transactional write
	ctx := e.NewContext()
	ctx.BeginTx()
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatal(err)
	}

	err := ctx.Flush()
	if !errors.Is(err, ErrTxnAborted) {
		t.Fatalf("expected ErrTxnAborted, got %v", err)
	}
}

func TestFlushTx_FakeConflict_Commits(t *testing.T) {
	bc := &txnMockBroadcaster{
		replicateToResults: map[pb.NodeID]*pb.NackNotify{
			99: {
				Key: []byte("k"),
				TipDetails: []*pb.NackTipDetail{
					{
						Ref:    &pb.EffectRef{NodeId: 0, Offset: 700},
						Hlc:    timestamppb.New(time.Unix(0, 150)),
						IsData: false, // MetaEffect — commutative, not a real conflict
						IsBind: false,
					},
				},
			},
		},
	}
	e := newTxnTestEngine(bc)

	// Add subscriber
	subCtx := e.NewContext()
	subEff := &pb.Effect{
		Key: []byte("k"),
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
			SubscriberNodeId: 99,
		}},
	}
	if err := subCtx.Emit(subEff); err != nil {
		t.Fatal(err)
	}
	if err := subCtx.Flush(); err != nil {
		t.Fatal(err)
	}

	// Transactional write
	ctx := e.NewContext()
	ctx.BeginTx()
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestEmit_ExcludesInProgressTxTips(t *testing.T) {
	e := newTxnTestEngine(nil)

	// Simulate an in-progress tx tip at offset 500 with pre-tx deps [200]
	e.pendingTxTips.Store(Tip{42, 500}, []Tip{{42, 200}})

	// Set up index with tip 500
	e.index.Insert("k", nil, keytrie.NewTipSet(Tip{42, 500}))

	// Non-tx emit should resolve tip 500 → pre-tx dep 200
	ctx := e.NewContext()
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	// Check the emitted effect's deps via effectCache
	tips := e.index.Contains("k")
	if tips == nil {
		t.Fatal("expected index entry for key k")
	}
	for _, off := range tips.Tips() {
		cached, ok := e.effectCache.Get(off, 0)
		if !ok {
			continue
		}
		if cached.GetData() != nil {
			if len(cached.Deps) != 1 || cached.Deps[0].Offset != 200 {
				t.Fatalf("expected deps with offset 200 (pre-tx dep), got %v", cached.Deps)
			}
			return
		}
	}
	t.Fatal("did not find data effect in cache")
}

func TestFlushNonTx_Unchanged(t *testing.T) {
	// Regression: non-tx flush should work identically to before
	bc := &mockBroadcaster{}
	e := newTxnTestEngine(bc)

	ctx := e.NewContext()
	if err := ctx.Emit(dataEffect("a")); err != nil {
		t.Fatal(err)
	}
	if err := ctx.Emit(dataEffect("b")); err != nil {
		t.Fatal(err)
	}
	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	if e.index.Contains("a") == nil {
		t.Fatal("expected index entry for key a")
	}
	if e.index.Contains("b") == nil {
		t.Fatal("expected index entry for key b")
	}
	if len(bc.broadcasts) != 2 {
		t.Fatalf("expected 2 broadcasts, got %d", len(bc.broadcasts))
	}
}

func TestResolveTipDeps_NoPendingTips(t *testing.T) {
	e := newTxnTestEngine(nil)

	tips := []Tip{{42, 100}, {42, 200}, {42, 300}}
	resolved := e.resolveTipDeps(tips)

	if len(resolved) != 3 {
		t.Fatalf("expected 3 tips, got %d", len(resolved))
	}
	for i, tip := range tips {
		if resolved[i] != tip {
			t.Fatalf("expected tip %v, got %v", tip, resolved[i])
		}
	}
}

func TestResolveTipDeps_WithPendingTips(t *testing.T) {
	e := newTxnTestEngine(nil)

	// {42,200} is an in-progress tx tip, with pre-tx deps [{42,50}, {42,60}]
	e.pendingTxTips.Store(Tip{42, 200}, []Tip{{42, 50}, {42, 60}})

	tips := []Tip{{42, 100}, {42, 200}, {42, 300}}
	resolved := e.resolveTipDeps(tips)

	// Should be [{42,100}, {42,50}, {42,60}, {42,300}] (200 replaced with 50, 60)
	expected := map[Tip]bool{{42, 100}: true, {42, 50}: true, {42, 60}: true, {42, 300}: true}
	if len(resolved) != 4 {
		t.Fatalf("expected 4 resolved tips, got %d: %v", len(resolved), resolved)
	}
	for _, r := range resolved {
		if !expected[r] {
			t.Fatalf("unexpected tip %v in resolved %v", r, resolved)
		}
	}
}

func TestFlushTx_SerializationEscalation_AfterNAborts(t *testing.T) {
	if !adaptiveSerializationEnabled {
		t.Skip("adaptive serialization disabled")
	}
	bc := &txnMockBroadcaster{
		replicateToResults: map[pb.NodeID]*pb.NackNotify{
			99: {
				Key: []byte("k"),
				TipDetails: []*pb.NackTipDetail{
					{
						Ref:        &pb.EffectRef{NodeId: 0, Offset: 700},
						Hlc:        timestamppb.New(time.Unix(0, 150)),
						IsData:     true,
						Collection: pb.CollectionKind_SCALAR,
					},
				},
			},
		},
	}
	e := newTxnTestEngine(bc)

	// Add subscriber
	subCtx := e.NewContext()
	subEff := &pb.Effect{
		Key: []byte("k"),
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
			SubscriberNodeId: 99,
		}},
	}
	if err := subCtx.Emit(subEff); err != nil {
		t.Fatal(err)
	}
	if err := subCtx.Flush(); err != nil {
		t.Fatal(err)
	}

	// Abort DefaultSerializationThreshold times
	for i := range DefaultSerializationThreshold {
		ctx := e.NewContext()
		ctx.BeginTx()
		if err := ctx.Emit(dataEffect("k")); err != nil {
			t.Fatal(err)
		}
		err := ctx.Flush()
		if !errors.Is(err, ErrTxnAborted) {
			t.Fatalf("iteration %d: expected ErrTxnAborted, got %v", i, err)
		}
	}

	// After N aborts, a SerializationEffect should have been emitted.
	// Check via the index: the serialization effect should be a tip for key "k".
	tips := e.index.Contains("k")
	if tips == nil {
		t.Fatal("expected index entry for key k after serialization escalation")
	}
	found := false
	for _, off := range tips.Tips() {
		cached, ok := e.effectCache.Get(off, 0)
		if !ok {
			continue
		}
		if cached.GetSerialization() != nil {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected SerializationEffect after consecutive aborts")
	}
}

func TestHandleRemoteBind_AbortsLocalPendingTx(t *testing.T) {
	e := newTxnTestEngine(nil)

	// Set up a local pending txn
	ptxn := &pendingTxn{
		txnHLC:     time.Unix(0, 100),
		originNode: 42,
		bindOffset: Tip{42, 8888},
		keys: []pendingTxnKey{
			{key: "k", newTip: Tip{42, 500}},
		},
		done: make(chan struct{}),
	}
	e.pendingTxns.Store(Tip{42, 8888}, ptxn)
	e.pendingTxTips.Store(Tip{42, 500}, []Tip{{42, 200}})

	// Remote bind arrives that aborts our offset 500
	bind := &pb.TransactionalBindEffect{
		TxnHlc:           timestamppb.New(time.Unix(0, 50)),
		OriginatorNodeId: 99,
		AbortDeps:        []*pb.EffectRef{{NodeId: 42, Offset: 500}},
		Keys: []*pb.TransactionalBindEffect_KeyBind{
			{Key: []byte("k"), NewTip: &pb.EffectRef{NodeId: 99, Offset: 600}},
		},
	}

	e.handleRemoteBind(bind, Tip{99, 7777}, "99:50:1")

	// Our pending txn should be aborted
	if ptxn.state.Load() != txnStateAborted {
		t.Fatal("expected pending txn to be aborted")
	}

	// done channel should be closed
	select {
	case <-ptxn.done:
	default:
		t.Fatal("done channel should be closed")
	}

	// pendingTxTips should be cleaned up
	if _, ok := e.pendingTxTips.Load(Tip{42, 500}); ok {
		t.Fatal("pendingTxTips should be cleaned for aborted offset")
	}
}

func TestHandleRemoteBind_CleansNormalKeyTips(t *testing.T) {
	e := newTxnTestEngine(nil)

	// Set up a pending tx tip for a key that will be committed (not aborted)
	e.pendingTxTips.Store(Tip{99, 600}, []Tip{{99, 300}})

	bind := &pb.TransactionalBindEffect{
		TxnHlc:           timestamppb.New(time.Unix(0, 50)),
		OriginatorNodeId: 99,
		Keys: []*pb.TransactionalBindEffect_KeyBind{
			{Key: []byte("k"), NewTip: &pb.EffectRef{NodeId: 99, Offset: 600}},
		},
	}

	e.handleRemoteBind(bind, Tip{99, 7777}, "99:50:2")

	// pendingTxTips for the committed key should be cleaned
	if _, ok := e.pendingTxTips.Load(Tip{99, 600}); ok {
		t.Fatal("pendingTxTips should be cleaned for committed key tip")
	}
}

func TestResolveTipDeps_DeduplicatesOverlapping(t *testing.T) {
	e := newTxnTestEngine(nil)

	// Two in-progress tx tips both resolve to the same pre-tx dep
	e.pendingTxTips.Store(Tip{42, 200}, []Tip{{42, 50}})
	e.pendingTxTips.Store(Tip{42, 300}, []Tip{{42, 50}})

	tips := []Tip{{42, 200}, {42, 300}}
	resolved := e.resolveTipDeps(tips)

	// Should deduplicate: just [{42,50}]
	if len(resolved) != 1 || resolved[0] != (Tip{42, 50}) {
		t.Fatalf("expected [{42,50}], got %v", resolved)
	}
}

func TestResolveTipDeps_NilDepsKeepsTip(t *testing.T) {
	e := newTxnTestEngine(nil)

	// {42,200} is an in-progress tx tip that created a new key (nil pre-tx deps)
	e.pendingTxTips.Store(Tip{42, 200}, []Tip(nil))

	tips := []Tip{{42, 100}, {42, 200}, {42, 300}}
	resolved := e.resolveTipDeps(tips)

	// {42,200} should be kept since it has no pre-tx deps (key creation)
	if len(resolved) != 3 {
		t.Fatalf("expected 3 resolved tips, got %d: %v", len(resolved), resolved)
	}
	expected := []Tip{{42, 100}, {42, 200}, {42, 300}}
	for i, r := range resolved {
		if r != expected[i] {
			t.Fatalf("expected tip %v at index %d, got %v; full: %v", expected[i], i, r, resolved)
		}
	}
}

func TestHandleNack_NonTransactional_NoOp(t *testing.T) {
	e := newTxnTestEngine(nil)

	// No pending txns — HandleNack should be a no-op
	nack := &pb.NackNotify{
		Key: []byte("k"),
		TipDetails: []*pb.NackTipDetail{
			{Ref: &pb.EffectRef{NodeId: 0, Offset: 100}, IsData: true, Collection: pb.CollectionKind_SCALAR},
		},
	}

	if err := e.HandleNack(nack); err != nil {
		t.Fatal(err)
	}
}

func TestHandleNack_TransactionalKey_AbortsOnConflict(t *testing.T) {
	e := newTxnTestEngine(nil)

	ptxn := &pendingTxn{
		txnHLC:     time.Unix(0, 100),
		originNode: 42,
		bindOffset: Tip{42, 9999},
		keys: []pendingTxnKey{
			{key: "k", newTip: Tip{42, 500}, collection: pb.CollectionKind_SCALAR},
		},
		done: make(chan struct{}),
	}
	e.pendingTxns.Store(Tip{42, 9999}, ptxn)

	nack := &pb.NackNotify{
		Key: []byte("k"),
		TipDetails: []*pb.NackTipDetail{
			{
				Ref:        &pb.EffectRef{NodeId: 0, Offset: 700},
				Hlc:        timestamppb.New(time.Unix(0, 150)),
				IsData:     true,
				Collection: pb.CollectionKind_SCALAR,
			},
		},
	}

	if err := e.HandleNack(nack); err != nil {
		t.Fatal(err)
	}

	if ptxn.state.Load() != txnStateAborted {
		t.Fatal("expected pending txn to be aborted by HandleNack")
	}
}

func TestGetSnapshot_ExcludesPendingTxTips(t *testing.T) {
	e := newTxnTestEngine(nil)

	// Write a committed effect via normal emit
	ctxCommit := e.NewContext()
	if err := ctxCommit.Emit(scalarSet("k", "committed")); err != nil {
		t.Fatal(err)
	}
	if err := ctxCommit.Flush(); err != nil {
		t.Fatal(err)
	}

	committedTips := e.index.Contains("k")
	if committedTips == nil || committedTips.Len() != 1 {
		t.Fatal("expected 1 tip after committed write")
	}
	committedOff := committedTips.Tips()[0]

	// Write an in-progress tx effect
	ctxTx := e.NewContext()
	ctxTx.BeginTx()
	if err := ctxTx.Emit(scalarSet("k", "uncommitted")); err != nil {
		t.Fatal(err)
	}
	// Flush the tx (this updates the index), then simulate it being pending
	if err := ctxTx.Flush(); err != nil {
		t.Fatal(err)
	}

	// Get the new tip (the tx BIND)
	txTips := e.index.Contains("k")
	if txTips == nil {
		t.Fatal("expected tips after tx flush")
	}
	// Find the tx bind tip (differs from committed offset, and skip the
	// originator's own subscription tip that ensureSubscribed installed
	// during the tx flush — that one is commutative metadata, not the
	// bind we want to simulate as pending).
	var txOff Tip
	for _, off := range txTips.Tips() {
		if off == committedOff {
			continue
		}
		eff, ok := e.effectCache.Get(off, 0)
		if !ok {
			continue
		}
		if eff.GetSubscription() != nil {
			continue
		}
		txOff = off
		break
	}
	if txOff == (Tip{}) {
		t.Fatal("did not find tx bind tip in index after tx flush")
	}

	// Register as in-progress tx tip (simulating the tx being pending)
	e.pendingTxTips.Store(txOff, []Tip{committedOff})

	// GetSnapshot should see the committed state, not the uncommitted tx
	result, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if string(result.Scalar.GetRaw()) != "committed" {
		t.Fatalf("expected committed value, got %q", string(result.Scalar.GetRaw()))
	}
}

// --- SSI snapshot tests ---

func scalarSet(key, value string) *pb.Effect {
	return &pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: []byte(value)},
		}},
	}
}

func TestSSI_SnapshotIsolation_ReadsConsistentState(t *testing.T) {
	e := newTxnTestEngine(nil)

	// Write initial value
	ctxSetup := e.NewContext()
	if err := ctxSetup.Emit(scalarSet("x", "original")); err != nil {
		t.Fatal(err)
	}
	if err := ctxSetup.Flush(); err != nil {
		t.Fatal(err)
	}

	// Context A: start a MULTI/EXEC tx (captures snapshot)
	ctxA := e.NewContext()
	ctxA.BeginTx()
	ctxA.CheckWatches() // captures snapshot

	// Context B: write a new value AFTER A's snapshot
	ctxB := e.NewContext()
	if err := ctxB.Emit(scalarSet("x", "modified")); err != nil {
		t.Fatal(err)
	}
	if err := ctxB.Flush(); err != nil {
		t.Fatal(err)
	}

	// A reads x — should see "original" (snapshot value), not "modified"
	result, _, err := ctxA.GetSnapshot("x")
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected non-nil result from snapshot read")
	}
	if string(result.Scalar.GetRaw()) != "original" {
		t.Fatalf("expected 'original' from snapshot, got %q", string(result.Scalar.GetRaw()))
	}
}

func TestSSI_ReadWriteConflict_Aborts(t *testing.T) {
	e := newTxnTestEngine(nil)

	// Write initial value
	ctxSetup := e.NewContext()
	if err := ctxSetup.Emit(scalarSet("x", "v1")); err != nil {
		t.Fatal(err)
	}
	if err := ctxSetup.Flush(); err != nil {
		t.Fatal(err)
	}

	// Context A: start tx, read x (emits NOOP via snapshot)
	ctxA := e.NewContext()
	ctxA.BeginTx()
	ctxA.CheckWatches()
	_, _, err := ctxA.GetSnapshot("x")
	if err != nil {
		t.Fatal(err)
	}

	// Context B: write to x (advances tips past A's NOOP)
	ctxB := e.NewContext()
	if err := ctxB.Emit(scalarSet("x", "v2")); err != nil {
		t.Fatal(err)
	}
	if err := ctxB.Flush(); err != nil {
		t.Fatal(err)
	}

	// A's flush should detect the fork and abort
	err = ctxA.Flush()
	if !errors.Is(err, ErrTxnAborted) {
		t.Fatalf("expected ErrTxnAborted, got %v", err)
	}
}

func TestSSI_SubscriberNACK_AbortsLoser(t *testing.T) {
	// Use the smallest possible hash (all zeros) to guarantee FWW victory
	// against any real SHA-256 hash the engine will compute.
	theirNode := pb.NodeID(0)
	theirHLC := timestamppb.New(time.Unix(0, 11))
	theirHash := make([]byte, 32) // all zeros — always lower than any real hash
	bc := &txnMockBroadcaster{}
	e := newTxnTestEngine(bc)

	// Add subscriber
	subCtx := e.NewContext()
	if err := subCtx.Emit(&pb.Effect{
		Key: []byte("x"),
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
			SubscriberNodeId: 99,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := subCtx.Flush(); err != nil {
		t.Fatal(err)
	}

	// Write initial value — capture the offset for consumed tips
	setupCtx := e.NewContext()
	if err := setupCtx.Emit(scalarSet("x", "v1")); err != nil {
		t.Fatal(err)
	}
	if err := setupCtx.Flush(); err != nil {
		t.Fatal(err)
	}
	// The setup write's offset is the shared causal base
	setupTips := e.index.Contains("x")
	var sharedBase []*pb.EffectRef
	if setupTips != nil {
		for _, t := range setupTips.Tips() {
			sharedBase = append(sharedBase, toPbRef(t))
		}
	}

	// Now configure the NACK response with shared consumed tips
	bc.replicateToResults = map[pb.NodeID]*pb.NackNotify{
		99: {
			Key: []byte("x"),
			TipDetails: []*pb.NackTipDetail{
				{
					Ref:                &pb.EffectRef{NodeId: 0, Offset: 700},
					Hlc:                timestamppb.New(time.Unix(0, 50)),
					IsData:             true,
					IsBind:             true,
					BindHlc:            theirHLC,
					BindNodeId:         uint64(theirNode),
					BindForkChoiceHash: theirHash,
					Collection:         pb.CollectionKind_SCALAR,
					BindConsumedTips: []*pb.KeyConsumedTips{
						{Key: []byte("x"), ConsumedTips: sharedBase},
					},
				},
			},
		},
	}

	// Start tx, read x (creates NOOP in read set)
	ctx := e.NewContext()
	ctx.BeginTx()
	ctx.CheckWatches()
	_, _, err := ctx.GetSnapshot("x")
	if err != nil {
		t.Fatal(err)
	}

	// Flush — subscriber NACKs with a competing bind that wins FWW
	err = ctx.Flush()
	if !errors.Is(err, ErrTxnAborted) {
		t.Fatalf("expected ErrTxnAborted from subscriber NACK, got %v", err)
	}
}

func TestSSI_WinnerResultsReturned_LoserAborted(t *testing.T) {
	e := newTxnTestEngine(nil)

	// Write initial value
	setupCtx := e.NewContext()
	if err := setupCtx.Emit(scalarSet("x", "v1")); err != nil {
		t.Fatal(err)
	}
	if err := setupCtx.Flush(); err != nil {
		t.Fatal(err)
	}

	// Context A: start tx, read x, write x
	ctxA := e.NewContext()
	ctxA.BeginTx()
	ctxA.CheckWatches()
	_, _, err := ctxA.GetSnapshot("x")
	if err != nil {
		t.Fatal(err)
	}
	if err := ctxA.Emit(scalarSet("x", "winner")); err != nil {
		t.Fatal(err)
	}

	// Context B: concurrent non-tx write (advances tips)
	ctxB := e.NewContext()
	if err := ctxB.Emit(scalarSet("x", "concurrent")); err != nil {
		t.Fatal(err)
	}
	if err := ctxB.Flush(); err != nil {
		t.Fatal(err)
	}

	// A's flush should abort (fork on x: A's write + B's write)
	err = ctxA.Flush()
	if !errors.Is(err, ErrTxnAborted) {
		t.Fatalf("expected ErrTxnAborted, got %v", err)
	}

	// After A aborts, both A's and B's effects are tips in the index.
	// The surviving value depends on fork_choice_hash winner selection.
	// The important assertion is that A's flush aborted (checked above).
	result, _, _, err2 := e.GetSnapshot("x")
	if err2 != nil {
		t.Fatal(err2)
	}
	if result == nil {
		t.Fatal("expected non-nil snapshot after abort")
	}
	val := string(result.Scalar.GetRaw())
	if val != "concurrent" && val != "winner" {
		t.Fatalf("expected 'concurrent' or 'winner' as surviving value, got %q", val)
	}
}

func TestSSI_SnapshotDoesNotSeeKeysCreatedAfterTxStart(t *testing.T) {
	e := newTxnTestEngine(nil)

	// Context A: start tx (captures snapshot — "newkey" doesn't exist yet)
	ctxA := e.NewContext()
	ctxA.BeginTx()
	ctxA.CheckWatches()

	// Context B: create "newkey" after A's snapshot
	ctxB := e.NewContext()
	if err := ctxB.Emit(scalarSet("newkey", "hello")); err != nil {
		t.Fatal(err)
	}
	if err := ctxB.Flush(); err != nil {
		t.Fatal(err)
	}

	// A reads "newkey" — should get nil (not in snapshot)
	result, _, err := ctxA.GetSnapshot("newkey")
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Fatal("expected nil for key created after snapshot, got non-nil")
	}
}

func TestSSI_WriteWithinTxUsesSnapshotDeps(t *testing.T) {
	e := newTxnTestEngine(nil)

	// Write initial value
	setupCtx := e.NewContext()
	if err := setupCtx.Emit(scalarSet("x", "v1")); err != nil {
		t.Fatal(err)
	}
	if err := setupCtx.Flush(); err != nil {
		t.Fatal(err)
	}

	// Context A: start tx (captures snapshot with x=v1 tips)
	ctxA := e.NewContext()
	ctxA.BeginTx()
	ctxA.CheckWatches()

	// Context B: write to x (advances live index past snapshot)
	ctxB := e.NewContext()
	if err := ctxB.Emit(scalarSet("x", "v2")); err != nil {
		t.Fatal(err)
	}
	if err := ctxB.Flush(); err != nil {
		t.Fatal(err)
	}

	// A writes to x within the tx — should use snapshot deps, not live index
	if err := ctxA.Emit(scalarSet("x", "v3")); err != nil {
		t.Fatal(err)
	}

	// A's flush should detect the fork (A's write deps on snapshot tips,
	// B's write deps on same base → fork) and abort
	err := ctxA.Flush()
	if !errors.Is(err, ErrTxnAborted) {
		t.Fatalf("expected ErrTxnAborted from SSI fork detection, got %v", err)
	}
}

func TestFlushTx_ConsumesTips_LinearChain(t *testing.T) {
	// Verify that repeated transactional read-modify-writes produce a linear
	// chain: each BIND replaces the consumed tips in the index, so subsequent
	// NOOPs depend only on the previous BIND (not on accumulated DATA+BIND offsets).
	e := newTxnTestEngine(nil)

	// Setup: write initial value (non-tx)
	setup := e.NewContext()
	if err := setup.Emit(scalarSet("k", "v0")); err != nil {
		t.Fatal(err)
	}
	if err := setup.Flush(); err != nil {
		t.Fatal(err)
	}

	initialTips := e.index.Contains("k")
	if initialTips == nil || len(initialTips.Tips()) != 1 {
		t.Fatal("expected exactly 1 tip after initial write")
	}

	const iterations = 5

	for i := range iterations {
		// Simulate GETSET pattern: read → BeginTx → NOOP → DATA → Flush
		ctx := e.NewContext()

		// Read current state (before BeginTx, like GETSET does)
		_, tips, err := ctx.GetSnapshot("k")
		if err != nil {
			t.Fatalf("iteration %d: GetSnapshot failed: %v", i, err)
		}

		ctx.BeginTx()

		// Emit NOOP to record the read
		if err := ctx.Emit(&pb.Effect{
			Key:  []byte("k"),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			t.Fatalf("iteration %d: Emit NOOP failed: %v", i, err)
		}

		// Emit DATA (the write)
		if err := ctx.Emit(scalarSet("k", fmt.Sprintf("v%d", i+1))); err != nil {
			t.Fatalf("iteration %d: Emit DATA failed: %v", i, err)
		}

		if err := ctx.Flush(); err != nil {
			t.Fatalf("iteration %d: Flush failed: %v", i, err)
		}

		// After each transaction, the index should have exactly 1 tip (the BIND)
		postTips := e.index.Contains("k")
		if postTips == nil {
			t.Fatalf("iteration %d: expected tips after flush, got nil", i)
		}
		if len(postTips.Tips()) != 1 {
			t.Fatalf("iteration %d: expected 1 tip after tx, got %d: %v",
				i, len(postTips.Tips()), postTips.Tips())
		}

		// Verify the tip is a BIND effect via effectCache
		bindOffset := postTips.Tips()[0]
		cached, ok := e.effectCache.Get(bindOffset, 0)
		if !ok {
			t.Fatalf("iteration %d: could not find BIND at offset %d in effectCache", i, bindOffset)
		}
		if cached.GetTxnBind() == nil {
			t.Fatalf("iteration %d: tip at offset %d is not a BIND", i, bindOffset)
		}
	}
}

// TestVoidedBinds_BoundedMemory verifies that the voidedBinds cache does not
// grow without bound. It stores far more entries than the cache capacity and
// asserts that the entry count stays bounded. This is the regression test
// for the xsync.Map memory leak where voided txnIDs accumulated forever.
func TestVoidedBinds_BoundedMemory(t *testing.T) {
	const capacity = 8192
	vb := clox.NewCloxCache[string, struct{}](clox.ConfigFromCapacity(capacity))
	defer vb.Close()

	// Insert 4x the capacity — with the old xsync.Map this would have
	// grown to 4*capacity entries; with CloxCache it stays bounded.
	const insertCount = capacity * 4
	for i := range insertCount {
		vb.Put(fmt.Sprintf("txn:%d", i), struct{}{})
	}

	count := vb.EntryCount()
	if count > capacity {
		t.Fatalf("voidedBinds should be bounded at ~%d entries, got %d", capacity, count)
	}
	t.Logf("after %d inserts: %d entries (capacity %d)", insertCount, count, capacity)
}

// TestVoidedBinds_ReadYourWrites verifies that a txnID stored in the
// voidedBinds cache is immediately visible to a subsequent lookup on
// the same goroutine (read-your-writes guarantee within capacity).
// Uses a large capacity with few inserts to stay well within bounds.
func TestVoidedBinds_ReadYourWrites(t *testing.T) {
	const capacity = 8192
	vb := clox.NewCloxCache[string, struct{}](clox.ConfigFromCapacity(capacity))
	defer vb.Close()

	// Insert only 100 entries — well within capacity, so no evictions.
	for i := range 100 {
		txnID := fmt.Sprintf("txn:%d", i)
		vb.Put(txnID, struct{}{})
		if _, ok := vb.Get(txnID, 0); !ok {
			t.Fatalf("read-your-writes failed: txnID %q not found immediately after Put", txnID)
		}
	}
}

// TestVoidedBinds_EngineIntegration verifies that the Engine's voidedBinds
// field is properly bounded by inserting entries and checking that the
// cache does not grow unbounded.
func TestVoidedBinds_EngineIntegration(t *testing.T) {
	// Use production-sized capacity for realistic behavior
	e := &Engine{
		index:             keytrie.New(),
		nodeID:            42,
		clock:             crdt.NewHLC(),
		subscriptions:     xsync.NewMap[string, *subscriptionState](),
		pendingTxns:       xsync.NewMap[Tip, *pendingTxn](),
		pendingTxTips:     xsync.NewMap[Tip, []Tip](),
		txAbortCounts:     xsync.NewMap[string, *atomic.Int32](),
		pendingBootstraps: xsync.NewMap[string, *bootstrapCollector](),
		voidedBinds:       clox.NewCloxCache[string, struct{}](clox.ConfigFromCapacity(8192)),
		effectCache:       clox.NewCloxCache[Tip, *pb.Effect](clox.ConfigFromMemorySize(1024 * 1024)),
	}
	e.safety.Store(&safetyMap{defaultMode: UnsafeMode})
	defer e.voidedBinds.Close()
	defer e.effectCache.Close()

	cap := e.voidedBinds.Capacity()

	// Insert 3x the capacity to force evictions — with the old
	// xsync.Map this would have grown to 3*cap; now it's bounded.
	insertCount := cap * 3
	for i := range insertCount {
		e.voidedBinds.Put(fmt.Sprintf("txn:%d:%d", e.nodeID, i), struct{}{})
	}

	count := e.voidedBinds.EntryCount()
	if count > cap {
		t.Fatalf("engine voidedBinds exceeded capacity: %d entries, capacity %d", count, cap)
	}
	t.Logf("after %d inserts: %d entries (capacity %d)", insertCount, count, cap)

	// Verify read-your-writes within capacity: insert a fresh entry and
	// immediately read it back. This is the path flushTx depends on —
	// the Put and Get happen on the same goroutine within microseconds.
	freshKey := "txn:fresh:ryw"
	e.voidedBinds.Put(freshKey, struct{}{})
	if _, ok := e.voidedBinds.Get(freshKey, 0); !ok {
		t.Fatal("read-your-writes failed for fresh entry after overflow")
	}
}
