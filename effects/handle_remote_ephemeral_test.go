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
	"sync/atomic"
	"testing"

	"github.com/puzpuzpuz/xsync/v4"
	clox "github.com/swytchdb/swytch/cache"
	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/crdt"
	"github.com/swytchdb/swytch/keytrie"
	"google.golang.org/protobuf/proto"
)

// newTestEngineForEphemeral builds a minimally-wired Engine suitable for
// driving HandleRemote tests. Mirrors the boilerplate in snapshot_test.go.
func newTestEngineForEphemeral(t *testing.T) *Engine {
	t.Helper()
	log := newSnapshotLog()
	e := &Engine{
		effectCache:       log.effectCache,
		index:             keytrie.New(),
		broadcaster:       &mockBroadcaster{},
		nodeID:            1,
		clock:             crdt.NewHLC(),
		subscriptions:     xsync.NewMap[string, *subscriptionState](),
		pendingTxns:       xsync.NewMap[Tip, *pendingTxn](),
		pendingTxTips:     xsync.NewMap[Tip, []Tip](),
		txAbortCounts:     xsync.NewMap[string, *atomic.Int32](),
		pendingBootstraps: xsync.NewMap[string, *bootstrapCollector](),
		peerSubscribers:   xsync.NewMap[string, *xsync.Map[pb.NodeID, struct{}]](),
		unsubInFlight:     xsync.NewMap[string, struct{}](),
		spokenBinds:       clox.NewCloxCache[Tip, struct{}](clox.ConfigFromCapacity(256)),
	}
	e.safety.Store(&safetyMap{defaultMode: UnsafeMode})
	return e
}

// TestHandleRemote_EphemeralSubscribe_FiresCallback verifies the
// short-circuit fires the OnEphemeralSubscribe callback with the raw
// routing key and subscriber id, and that no NACK/error is produced.
func TestHandleRemote_EphemeralSubscribe_FiresCallback(t *testing.T) {
	e := newTestEngineForEphemeral(t)

	var gotPeer uint64
	var gotKey []byte
	var gotUnsub bool
	var calls int
	e.OnEphemeralSubscribe = func(peer uint64, key []byte, unsub bool) {
		gotPeer = peer
		gotKey = append([]byte(nil), key...)
		gotUnsub = unsub
		calls++
	}

	subEff := &pb.Effect{
		Key:            []byte("__swytch:ch\x00news"),
		Hlc:            sTs(20),
		NodeId:         2,
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(20)),
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
			SubscriberNodeId: 2,
			Ephemeral:        true,
		}},
	}
	data, err := proto.Marshal(subEff)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	notify := BuildOffsetNotify(2, Tip{2, 5000}, subEff, data, nil)

	nacks, err := e.HandleRemote(notify)
	if err != nil {
		t.Fatalf("HandleRemote: %v", err)
	}
	if len(nacks) != 0 {
		t.Fatalf("ephemeral path should not produce NACKs, got %d", len(nacks))
	}
	if calls != 1 {
		t.Fatalf("expected exactly 1 callback, got %d", calls)
	}
	if gotPeer != 2 {
		t.Errorf("subscriber id: got %d want 2", gotPeer)
	}
	if string(gotKey) != "__swytch:ch\x00news" {
		t.Errorf("routing key: got %q", string(gotKey))
	}
	if gotUnsub {
		t.Errorf("expected unsub=false")
	}

	// The ephemeral effect must NOT be indexed.
	if tips := e.index.Contains("__swytch:ch\x00news"); tips != nil {
		t.Errorf("ephemeral effect should not be indexed, but found tips: %v", tips.Tips())
	}
}

// TestHandleRemote_EphemeralUnsubscribe_FiresCallbackWithUnsub verifies
// the unsubscribe flag is propagated.
func TestHandleRemote_EphemeralUnsubscribe_FiresCallbackWithUnsub(t *testing.T) {
	e := newTestEngineForEphemeral(t)

	var gotUnsub bool
	e.OnEphemeralSubscribe = func(_ uint64, _ []byte, unsub bool) {
		gotUnsub = unsub
	}

	subEff := &pb.Effect{
		Key:            []byte("__swytch:pat\x00news.*"),
		Hlc:            sTs(20),
		NodeId:         2,
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(20)),
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
			SubscriberNodeId: 2,
			Ephemeral:        true,
			Unsubscribe:      true,
		}},
	}
	data, _ := proto.Marshal(subEff)
	notify := BuildOffsetNotify(2, Tip{2, 5000}, subEff, data, nil)

	if _, err := e.HandleRemote(notify); err != nil {
		t.Fatalf("HandleRemote: %v", err)
	}
	if !gotUnsub {
		t.Errorf("expected unsub=true to propagate")
	}
}

// TestHandleRemote_PubSubMessage_DeliversToCallback verifies the
// message short-circuit fires OnPubSubMessage with channel + payload
// and produces no NACKs.
func TestHandleRemote_PubSubMessage_DeliversToCallback(t *testing.T) {
	e := newTestEngineForEphemeral(t)

	var gotChannel []byte
	var gotPayload []byte
	var calls int
	e.OnPubSubMessage = func(channel, payload []byte) {
		gotChannel = append([]byte(nil), channel...)
		gotPayload = append([]byte(nil), payload...)
		calls++
	}

	msgEff := &pb.Effect{
		Key:            []byte("__swytch:ch\x00news"),
		Hlc:            sTs(20),
		NodeId:         2,
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(20)),
		Kind: &pb.Effect_PubsubMessage{PubsubMessage: &pb.PubSubMessage{
			Channel: []byte("news"),
			Payload: []byte("breaking"),
		}},
	}
	data, _ := proto.Marshal(msgEff)
	notify := BuildOffsetNotify(2, Tip{2, 5000}, msgEff, data, nil)

	nacks, err := e.HandleRemote(notify)
	if err != nil {
		t.Fatalf("HandleRemote: %v", err)
	}
	if len(nacks) != 0 {
		t.Fatalf("pubsub message path should not produce NACKs, got %d", len(nacks))
	}
	if calls != 1 {
		t.Fatalf("expected exactly 1 callback, got %d", calls)
	}
	if string(gotChannel) != "news" {
		t.Errorf("channel: got %q", string(gotChannel))
	}
	if string(gotPayload) != "breaking" {
		t.Errorf("payload: got %q", string(gotPayload))
	}

	// The pubsub message must NOT be indexed.
	if tips := e.index.Contains("__swytch:ch\x00news"); tips != nil {
		t.Errorf("pubsub message should not be indexed, but found tips: %v", tips.Tips())
	}
}

// TestHandleRemote_EphemeralCallbacks_NilSafe ensures the engine
// tolerates callbacks not being set.
func TestHandleRemote_EphemeralCallbacks_NilSafe(t *testing.T) {
	e := newTestEngineForEphemeral(t)
	// Leave callbacks nil.

	subEff := &pb.Effect{
		Key:            []byte("__swytch:ch\x00x"),
		Hlc:            sTs(20),
		NodeId:         2,
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(20)),
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
			SubscriberNodeId: 2, Ephemeral: true,
		}},
	}
	data, _ := proto.Marshal(subEff)
	notify := BuildOffsetNotify(2, Tip{2, 5000}, subEff, data, nil)
	if _, err := e.HandleRemote(notify); err != nil {
		t.Errorf("ephemeral sub with nil callback: %v", err)
	}

	msgEff := &pb.Effect{
		Key:            []byte("__swytch:ch\x00x"),
		Hlc:            sTs(21),
		NodeId:         2,
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(21)),
		Kind: &pb.Effect_PubsubMessage{PubsubMessage: &pb.PubSubMessage{
			Channel: []byte("x"), Payload: []byte("y"),
		}},
	}
	data, _ = proto.Marshal(msgEff)
	notify = BuildOffsetNotify(2, Tip{2, 5001}, msgEff, data, nil)
	if _, err := e.HandleRemote(notify); err != nil {
		t.Errorf("pubsub message with nil callback: %v", err)
	}
}
