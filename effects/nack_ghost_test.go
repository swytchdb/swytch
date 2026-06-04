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
	"encoding/binary"
	"errors"
	"testing"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/keytrie"
	"google.golang.org/protobuf/proto"
)

// selectiveBroadcaster returns a fixed set of NACK tips from
// ReplicateTo and selectively serves FetchFromAny based on the
// fetchable map. Used to drive ensureSubscribed through ghost-tip
// scenarios.
type selectiveBroadcaster struct {
	mockBroadcaster
	nackTips  []*pb.EffectRef
	fetchable map[Tip][]byte // offset → wire bytes; absence = NACK only, not fetchable
}

func (b *selectiveBroadcaster) ReplicateTo(notify *pb.OffsetNotify, _ []byte, target pb.NodeID) ([]*pb.NackNotify, error) {
	b.mockBroadcaster.replicateToPeers = append(b.mockBroadcaster.replicateToPeers, target)
	return []*pb.NackNotify{{
		Key:  notify.Key,
		Tips: b.nackTips,
	}}, nil
}

func (b *selectiveBroadcaster) FetchFromAny(ref *pb.EffectRef) ([]byte, error) {
	if data, ok := b.fetchable[Tip{ref.NodeId, ref.Offset}]; ok && data != nil {
		return data, nil
	}
	return nil, errors.New("effect not found on peer")
}

// wireEffect produces the wire-format bytes for an effect, matching
// what storeWireData expects: [4-byte LE keyLen][key][protoData].
func wireEffect(eff *pb.Effect) []byte {
	protoData, _ := proto.Marshal(eff)
	keyLen := uint32(len(eff.Key))
	wire := make([]byte, 4+int(keyLen)+len(protoData))
	binary.LittleEndian.PutUint32(wire[:4], keyLen)
	copy(wire[4:4+keyLen], eff.Key)
	copy(wire[4+keyLen:], protoData)
	return wire
}

// makeReachableEffect builds a leaf effect (no deps) at offset off
// originating from peerNode for the given key. Returned wire bytes
// can be installed in selectiveBroadcaster.fetchable so the walk
// resolves successfully.
func makeReachableEffect(t *testing.T, key string, peerNode pb.NodeID, off uint64) (Tip, []byte) {
	t.Helper()
	hlc := sTs(int64(off))
	eff := &pb.Effect{
		Key:            []byte(key),
		Hlc:            hlc,
		NodeId:         uint64(peerNode),
		ForkChoiceHash: ComputeForkChoiceHash(peerNode, hlc),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: []byte("v")},
		}},
	}
	return Tip{uint64(peerNode), off}, wireEffect(eff)
}

// TestBuildEnrichedNack_SkipsTipsWithoutLocalBytes verifies the
// cluster-scale invariant: a NACK only advertises tips this node
// can actually serve from its local effectCache. Tips present in
// the index but absent from the cache are silently dropped from the
// NACK — preventing the joiner-pollution chain where a single
// failed bootstrap leaves an index claiming authority over bytes
// no one in the cluster holds.
func TestBuildEnrichedNack_SkipsTipsWithoutLocalBytes(t *testing.T) {
	bc := &mockBroadcaster{}
	e := newTestEngine(bc)

	const key = "shared"

	// Effect A: in both index AND cache (legitimate authority).
	effA := &pb.Effect{
		Key:            []byte(key),
		Hlc:            sTs(10),
		NodeId:         42,
		ForkChoiceHash: ComputeForkChoiceHash(42, sTs(10)),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: []byte("alive")},
		}},
	}
	tipA := Tip{42, 100}
	e.effectCache.Put(tipA, effA)
	e.index.Insert(key, nil, keytrie.NewTipSet(tipA))

	// Ghost tip B: in the index (we adopted it from a NACK at some
	// point) but not in the cache (we never actually fetched the
	// bytes, or they were evicted).
	tipB := Tip{99, 200}
	current := e.index.Contains(key)
	currentTips := []keytrie.EffectRef{tipA}
	if current != nil {
		currentTips = current.Tips()
	}
	e.index.Insert(key, nil, keytrie.NewTipSet(append(currentTips, tipB)...))

	// Trigger NACK construction by simulating a remote subscribe
	// arriving via HandleRemote. The peer's subscribe will route
	// through the line-724 NACK-back path.
	subEff := &pb.Effect{
		Key:            []byte(key),
		Hlc:            sTs(20),
		NodeId:         2,
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(20)),
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
			SubscriberNodeId: 2,
		}},
	}
	subData, _ := proto.Marshal(subEff)
	notify := BuildOffsetNotify(2, Tip{2, 5000}, subEff, subData, nil)

	nacks, err := e.HandleRemote(notify)
	if err != nil {
		t.Fatal(err)
	}
	if len(nacks) != 1 {
		t.Fatalf("expected 1 NACK, got %d", len(nacks))
	}
	nack := nacks[0]

	// NACK should mention tipA (we have its bytes) but NOT tipB
	// (ghost — we'd be lying about authority).
	if len(nack.Tips) != 1 {
		t.Fatalf("expected NACK with exactly 1 tip (the cache-resident one), got %d tips: %v",
			len(nack.Tips), nack.Tips)
	}
	if r(nack.Tips[0]) != tipA {
		t.Fatalf("expected NACK to advertise %v, got %v", tipA, r(nack.Tips[0]))
	}
	if len(nack.TipDetails) != 1 {
		t.Fatalf("expected 1 TipDetail, got %d", len(nack.TipDetails))
	}
}

// TestEnsureSubscribed_InstallsOnlyWalkableTips drives the bootstrap
// path with a mix of reachable and unreachable NACK tips. The
// reachable subset must end up in the local index; the ghost tip
// must NOT be installed. Bootstrap returns success (state ready),
// not ErrBootstrapIncomplete — partial reachability is the new
// happy path.
func TestEnsureSubscribed_InstallsOnlyWalkableTips(t *testing.T) {
	const key = "membership"
	const peerNode pb.NodeID = 7

	// Two NACK tips: one fully fetchable, one a ghost (no bytes
	// anywhere).
	realTip, realBytes := makeReachableEffect(t, key, peerNode, 100)
	ghostTip := Tip{99999, 4}

	bc := &selectiveBroadcaster{
		mockBroadcaster: mockBroadcaster{
			peerIDs:                 []pb.NodeID{peerNode},
			allRegionPeersReachable: true,
		},
		nackTips: []*pb.EffectRef{toPbRef(realTip), toPbRef(ghostTip)},
		fetchable: map[Tip][]byte{
			realTip: realBytes,
			// ghostTip intentionally absent — FetchFromAny returns error.
		},
	}

	e := newTestEngine(bc)

	// GetSnapshot drives ensureSubscribed; we just need bootstrap to run.
	if err := e.ensureSubscribed(key); err != nil {
		t.Fatalf("ensureSubscribed returned error on partial reachability: %v", err)
	}

	// Reachable tip must be installed.
	idx := e.index.Contains(key)
	if idx == nil {
		t.Fatal("index has no entry for key after bootstrap")
	}
	tips := idx.Tips()
	foundReal := false
	for _, tip := range tips {
		if tip == realTip {
			foundReal = true
		}
		if tip == ghostTip {
			t.Fatalf("ghost tip %v was installed; expected to be filtered out", ghostTip)
		}
	}
	if !foundReal {
		t.Fatalf("reachable tip %v missing from index; tips: %v", realTip, tips)
	}

	// Subscription state must be ready (no retryBootstrap goroutine).
	state, ok := e.subscriptions.Load(key)
	if !ok {
		t.Fatal("no subscription state for key after successful bootstrap")
	}
	if state.incomplete.Load() {
		t.Fatal("subscription marked incomplete despite partial bootstrap success")
	}
	select {
	case <-state.ready:
		// expected
	default:
		t.Fatal("subscription state.ready channel is not closed")
	}
}

// TestEnsureSubscribed_AllTipsUnreachable_RetriesBootstrap asserts
// the "no tips reachable but peers did NACK" path: we must report
// ErrBootstrapIncomplete and arm retryBootstrap. The index stays
// empty so we never advertise ghost authority to others while we
// retry.
func TestEnsureSubscribed_AllTipsUnreachable_RetriesBootstrap(t *testing.T) {
	const key = "membership"
	const peerNode pb.NodeID = 7

	// Every NACK tip is a ghost.
	ghostA := Tip{500, 1}
	ghostB := Tip{500, 2}

	bc := &selectiveBroadcaster{
		mockBroadcaster: mockBroadcaster{
			peerIDs:                 []pb.NodeID{peerNode},
			allRegionPeersReachable: true,
		},
		nackTips:  []*pb.EffectRef{toPbRef(ghostA), toPbRef(ghostB)},
		fetchable: map[Tip][]byte{}, // nothing fetchable
	}

	e := newTestEngine(bc)
	defer e.closed.Store(true) // stop retryBootstrap on test exit

	err := e.ensureSubscribed(key)
	if !errors.Is(err, ErrBootstrapIncomplete) {
		t.Fatalf("expected ErrBootstrapIncomplete, got %v", err)
	}

	// ensureSubscribed now installs the originator's own SubscriptionEffect
	// into the local index before broadcasting, so a fully-failed bootstrap
	// still leaves that one tip in place. What MUST NOT happen is installing
	// any of the unreachable ghost tips — that would advertise ghost
	// authority to others.
	tips := e.index.Contains(key)
	if tips == nil {
		t.Fatal("expected our own subscription tip to be installed locally")
	}
	for _, tp := range tips.Tips() {
		if tp == ghostA || tp == ghostB {
			t.Fatalf("ghost tip %v installed despite unreachable bootstrap", tp)
		}
		eff, ok := e.effectCache.Get(tp)
		if !ok {
			t.Fatalf("tip %v not in effectCache", tp)
		}
		if eff.GetSubscription() == nil || pb.NodeID(eff.NodeId) != e.nodeID {
			t.Fatalf("unexpected installed tip %v (effect %+v) — only the local "+
				"SubscriptionEffect should be present after a fully-failed bootstrap",
				tp, eff)
		}
	}

	// Subscription state is incomplete with retry in flight.
	state, ok := e.subscriptions.Load(key)
	if !ok {
		t.Fatal("no subscription state after failed bootstrap")
	}
	if !state.incomplete.Load() {
		t.Fatal("expected state.incomplete = true after full bootstrap failure")
	}
	select {
	case <-state.ready:
		t.Fatal("state.ready was closed despite full failure")
	default:
		// expected
	}
}

// TestEnsureSubscribed_Partition_FailsCleanly verifies the partition
// error path: an ErrRegionPartitioned return must delete the
// subscription state (so a later call re-bootstraps when the
// partition resolves) and mark the state failed (so any concurrent
// waiter that already passed the incomplete-check now re-checks
// after <-ready and surfaces ErrBootstrapIncomplete instead of
// interpreting the closed channel as success).
func TestEnsureSubscribed_Partition_FailsCleanly(t *testing.T) {
	const key = "membership"
	bc := &selectiveBroadcaster{
		mockBroadcaster: mockBroadcaster{
			peerIDs:                 []pb.NodeID{7},
			allRegionPeersReachable: false, // simulate minority partition
		},
		nackTips:  nil,
		fetchable: map[Tip][]byte{},
	}

	e := newTestEngine(bc)

	err := e.ensureSubscribed(key)
	if !errors.Is(err, ErrRegionPartitioned) {
		t.Fatalf("expected ErrRegionPartitioned, got %v", err)
	}
	if _, ok := e.subscriptions.Load(key); ok {
		t.Fatal("subscription state not deleted after partition failure; future calls won't re-bootstrap")
	}
}

// TestRetryBootstrap_ShutdownUnblocksWaiters asserts that an engine
// closed mid-retry transitions any parked subscription state to
// markFailed: waiters unblock and re-check incomplete rather than
// blocking until process exit.
func TestRetryBootstrap_ShutdownUnblocksWaiters(t *testing.T) {
	const key = "membership"
	const peerNode pb.NodeID = 7

	bc := &selectiveBroadcaster{
		mockBroadcaster: mockBroadcaster{
			peerIDs:                 []pb.NodeID{peerNode},
			allRegionPeersReachable: true,
		},
		nackTips:  []*pb.EffectRef{toPbRef(Tip{500, 1})},
		fetchable: map[Tip][]byte{},
	}

	e := newTestEngine(bc)

	if err := e.ensureSubscribed(key); !errors.Is(err, ErrBootstrapIncomplete) {
		t.Fatalf("expected ErrBootstrapIncomplete, got %v", err)
	}

	state, ok := e.subscriptions.Load(key)
	if !ok {
		t.Fatal("no subscription state after incomplete bootstrap")
	}

	// Simulate a waiter that has already passed the incomplete check
	// and is now parked on <-state.ready. Engine.Close should release
	// it via markFailed (closes ready + sets incomplete=true).
	released := make(chan struct{})
	go func() {
		<-state.ready
		close(released)
	}()

	e.closed.Store(true)

	select {
	case <-released:
		// expected
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not unblock after engine close; retryBootstrap leak")
	}

	if !state.incomplete.Load() {
		t.Fatal("state.incomplete should be true after shutdown so re-check returns ErrBootstrapIncomplete")
	}
}
