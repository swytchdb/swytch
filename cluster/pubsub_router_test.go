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
	"slices"
	"sync"
	"testing"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"google.golang.org/protobuf/proto"
)

// --- Mocks --------------------------------------------------------------

type fakeBroker struct {
	mu            sync.Mutex
	deliveries    []deliveredMessage
	localChannels []string
	localPatterns []string
}

type deliveredMessage struct {
	channel string
	payload string
}

func (b *fakeBroker) DeliverLocal(channel string, payload []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deliveries = append(b.deliveries, deliveredMessage{channel, string(payload)})
}

func (b *fakeBroker) LocalSubsSnapshot() (channels []string, patterns []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.localChannels...), append([]string(nil), b.localPatterns...)
}

func (b *fakeBroker) deliveredChannels() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, len(b.deliveries))
	for i, d := range b.deliveries {
		out[i] = d.channel
	}
	return out
}

type fakeTransport struct {
	mu      sync.Mutex
	peers   []NodeId
	sends   []sentNotify
	sendErr error
}

type sentNotify struct {
	peerID NodeId
	notify *pb.OffsetNotify
}

func (t *fakeTransport) PeerIDs() []NodeId {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]NodeId(nil), t.peers...)
}

func (t *fakeTransport) SendOneWayTo(notify *pb.OffsetNotify, _ []byte, peerID NodeId) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.sendErr != nil {
		return t.sendErr
	}
	t.sends = append(t.sends, sentNotify{peerID: peerID, notify: notify})
	return nil
}

func (t *fakeTransport) sendsToPeer(peerID NodeId) []*pb.OffsetNotify {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []*pb.OffsetNotify
	for _, s := range t.sends {
		if s.peerID == peerID {
			out = append(out, s.notify)
		}
	}
	return out
}

// --- Encoding -----------------------------------------------------------

func TestRoutingKeyRoundtrip(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		isPattern bool
	}{
		{"plain channel", "news.weather", false},
		{"channel with colon", "pat:x", false},
		{"empty channel", "", false},
		{"pattern", "news.*", true},
		{"pattern with brackets", "news.[ab]?", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var enc []byte
			if tc.isPattern {
				enc = encodeRoutingKey(patKeyPrefix, tc.input)
			} else {
				enc = encodeRoutingKey(chKeyPrefix, tc.input)
			}
			name, isPattern, ok := decodeRoutingKey(enc)
			if !ok {
				t.Fatalf("decode failed for %q", tc.input)
			}
			if name != tc.input {
				t.Errorf("name: got %q want %q", name, tc.input)
			}
			if isPattern != tc.isPattern {
				t.Errorf("isPattern: got %v want %v", isPattern, tc.isPattern)
			}
		})
	}
}

func TestRoutingKeyDecode_RejectsUnrelated(t *testing.T) {
	if _, _, ok := decodeRoutingKey([]byte("regular-key")); ok {
		t.Errorf("decodeRoutingKey accepted non-pubsub key")
	}
	if _, _, ok := decodeRoutingKey([]byte("__swytch:members")); ok {
		t.Errorf("decodeRoutingKey accepted membership key")
	}
}

// --- HandleEphemeralSubscribe ------------------------------------------

func TestHandleEphemeralSubscribe_AddRemoveChannel(t *testing.T) {
	r := NewPubSubRouter(NodeId(1), &fakeTransport{}, &fakeBroker{})
	r.HandleEphemeralSubscribe(uint64(2), encodeRoutingKey(chKeyPrefix, "news"), false)

	if got := r.ClusterNumSub([]string{"news"})["news"]; got != 1 {
		t.Errorf("expected 1 peer subscribed to news, got %d", got)
	}

	r.HandleEphemeralSubscribe(uint64(2), encodeRoutingKey(chKeyPrefix, "news"), true)
	if got := r.ClusterNumSub([]string{"news"})["news"]; got != 0 {
		t.Errorf("expected 0 peers after unsubscribe, got %d", got)
	}
}

func TestHandleEphemeralSubscribe_IgnoresSelf(t *testing.T) {
	r := NewPubSubRouter(NodeId(1), &fakeTransport{}, &fakeBroker{})
	r.HandleEphemeralSubscribe(uint64(1), encodeRoutingKey(chKeyPrefix, "news"), false)
	if got := r.ClusterNumSub([]string{"news"})["news"]; got != 0 {
		t.Errorf("self-origin announce should be ignored, got %d", got)
	}
}

func TestHandleEphemeralSubscribe_UnknownPrefixIgnored(t *testing.T) {
	r := NewPubSubRouter(NodeId(1), &fakeTransport{}, &fakeBroker{})
	r.HandleEphemeralSubscribe(uint64(2), []byte("not-a-pubsub-key"), false)
	if got := r.ClusterPatterns(); len(got) != 0 {
		t.Errorf("unknown prefix should not register: ClusterPatterns=%v", got)
	}
}

// --- Pattern routing ----------------------------------------------------

func TestTargetsFor_LiteralChannel(t *testing.T) {
	r := NewPubSubRouter(NodeId(1), &fakeTransport{}, &fakeBroker{})
	r.HandleEphemeralSubscribe(uint64(2), encodeRoutingKey(chKeyPrefix, "news"), false)
	r.HandleEphemeralSubscribe(uint64(3), encodeRoutingKey(chKeyPrefix, "sports"), false)

	got := r.targetsFor("news")
	if len(got) != 1 || got[0] != NodeId(2) {
		t.Errorf("targetsFor news: got %v want [2]", got)
	}
	got = r.targetsFor("unknown")
	if len(got) != 0 {
		t.Errorf("targetsFor unknown: got %v want []", got)
	}
}

func TestTargetsFor_PatternGlob(t *testing.T) {
	r := NewPubSubRouter(NodeId(1), &fakeTransport{}, &fakeBroker{})
	r.HandleEphemeralSubscribe(uint64(2), encodeRoutingKey(patKeyPrefix, "news.*"), false)
	r.HandleEphemeralSubscribe(uint64(3), encodeRoutingKey(patKeyPrefix, "*.urgent"), false)
	r.HandleEphemeralSubscribe(uint64(4), encodeRoutingKey(chKeyPrefix, "alerts.urgent"), false)

	gotIDs := sortNodeIDs(r.targetsFor("news.weather"))
	if !sliceEqual(gotIDs, []NodeId{2}) {
		t.Errorf("news.weather: got %v want [2]", gotIDs)
	}

	gotIDs = sortNodeIDs(r.targetsFor("alerts.urgent"))
	if !sliceEqual(gotIDs, []NodeId{3, 4}) {
		t.Errorf("alerts.urgent: got %v want [3 4]", gotIDs)
	}

	// A channel matching one peer's pattern AND another peer's literal
	// should count both, deduped.
	r.HandleEphemeralSubscribe(uint64(5), encodeRoutingKey(patKeyPrefix, "alerts.*"), false)
	gotIDs = sortNodeIDs(r.targetsFor("alerts.urgent"))
	if !sliceEqual(gotIDs, []NodeId{3, 4, 5}) {
		t.Errorf("alerts.urgent w/ pattern: got %v want [3 4 5]", gotIDs)
	}
}

// --- Peer lifecycle -----------------------------------------------------

func TestOnPeerRemoved_DropsEntries(t *testing.T) {
	r := NewPubSubRouter(NodeId(1), &fakeTransport{}, &fakeBroker{})
	r.HandleEphemeralSubscribe(uint64(2), encodeRoutingKey(chKeyPrefix, "news"), false)
	r.HandleEphemeralSubscribe(uint64(2), encodeRoutingKey(patKeyPrefix, "sports.*"), false)
	r.HandleEphemeralSubscribe(uint64(3), encodeRoutingKey(chKeyPrefix, "news"), false)

	r.OnPeerRemoved(NodeId(2))

	if got := r.ClusterNumSub([]string{"news"})["news"]; got != 1 {
		t.Errorf("after dropping peer 2: news count = %d, want 1", got)
	}
	if got := r.ClusterPatterns(); len(got) != 0 {
		t.Errorf("after dropping peer 2: ClusterPatterns = %v, want []", got)
	}
}

func TestOnPeerAdded_ReannouncesLocalSubs(t *testing.T) {
	transport := &fakeTransport{peers: []NodeId{NodeId(2)}}
	broker := &fakeBroker{
		localChannels: []string{"news", "sports"},
		localPatterns: []string{"alerts.*"},
	}
	r := NewPubSubRouter(NodeId(1), transport, broker)

	r.OnPeerAdded(NodeId(2))

	sent := transport.sendsToPeer(NodeId(2))
	if len(sent) != 3 {
		t.Fatalf("expected 3 announces to peer 2, got %d", len(sent))
	}
	// Each announce carries an ephemeral SubscriptionEffect with our self ID.
	for _, n := range sent {
		eff := mustDecodeEffect(t, n)
		sub := eff.GetSubscription()
		if sub == nil {
			t.Fatalf("announce missing SubscriptionEffect: %v", eff)
		}
		if !sub.Ephemeral {
			t.Errorf("expected Ephemeral=true on announce")
		}
		if sub.Unsubscribe {
			t.Errorf("expected Unsubscribe=false on join re-announce")
		}
		if sub.SubscriberNodeId != uint64(1) {
			t.Errorf("subscriber id: got %d want 1", sub.SubscriberNodeId)
		}
	}
}

// --- RouteMessage -------------------------------------------------------

func TestRouteMessage_SendsOnlyToMatchedPeers(t *testing.T) {
	transport := &fakeTransport{peers: []NodeId{NodeId(2), NodeId(3), NodeId(4)}}
	r := NewPubSubRouter(NodeId(1), transport, &fakeBroker{})

	r.HandleEphemeralSubscribe(uint64(2), encodeRoutingKey(chKeyPrefix, "news"), false)
	r.HandleEphemeralSubscribe(uint64(3), encodeRoutingKey(patKeyPrefix, "news.*"), false)
	// peer 4 announces a completely different channel — shouldn't match.
	r.HandleEphemeralSubscribe(uint64(4), encodeRoutingKey(chKeyPrefix, "sports"), false)

	count := r.RouteMessage("news", []byte("hello"))
	if count != 1 {
		t.Errorf("RouteMessage news: count = %d, want 1 (only literal peer 2)", count)
	}

	count = r.RouteMessage("news.weather", []byte("hi"))
	if count != 1 {
		t.Errorf("RouteMessage news.weather: count = %d, want 1 (only pattern peer 3)", count)
	}

	count = r.RouteMessage("entirely.unrelated", []byte("x"))
	if count != 0 {
		t.Errorf("RouteMessage entirely.unrelated: count = %d, want 0", count)
	}

	// Verify the payload reached peer 2 (and only peer 2) for the
	// literal channel.
	sentTo2 := transport.sendsToPeer(NodeId(2))
	if len(sentTo2) != 1 {
		t.Fatalf("peer 2 received %d notifies, want 1", len(sentTo2))
	}
	eff := mustDecodeEffect(t, sentTo2[0])
	msg := eff.GetPubsubMessage()
	if msg == nil {
		t.Fatalf("expected PubSubMessage, got %v", eff)
	}
	if string(msg.Channel) != "news" {
		t.Errorf("channel: got %q want news", string(msg.Channel))
	}
	if string(msg.Payload) != "hello" {
		t.Errorf("payload: got %q want hello", string(msg.Payload))
	}
}

// --- Introspection ------------------------------------------------------

func TestClusterChannels_FiltersByPattern(t *testing.T) {
	r := NewPubSubRouter(NodeId(1), &fakeTransport{}, &fakeBroker{})
	r.HandleEphemeralSubscribe(uint64(2), encodeRoutingKey(chKeyPrefix, "news.weather"), false)
	r.HandleEphemeralSubscribe(uint64(2), encodeRoutingKey(chKeyPrefix, "sports"), false)
	r.HandleEphemeralSubscribe(uint64(3), encodeRoutingKey(chKeyPrefix, "news.tech"), false)

	got := r.ClusterChannels("")
	if !sliceEqual(got, []string{"news.tech", "news.weather", "sports"}) {
		t.Errorf("all channels: got %v", got)
	}
	got = r.ClusterChannels("news.*")
	if !sliceEqual(got, []string{"news.tech", "news.weather"}) {
		t.Errorf("filtered news.*: got %v", got)
	}
}

func TestClusterPatterns_DedupesAcrossPeers(t *testing.T) {
	r := NewPubSubRouter(NodeId(1), &fakeTransport{}, &fakeBroker{})
	r.HandleEphemeralSubscribe(uint64(2), encodeRoutingKey(patKeyPrefix, "news.*"), false)
	r.HandleEphemeralSubscribe(uint64(3), encodeRoutingKey(patKeyPrefix, "news.*"), false) // duplicate across peers
	r.HandleEphemeralSubscribe(uint64(3), encodeRoutingKey(patKeyPrefix, "sports.*"), false)
	got := r.ClusterPatterns()
	slices.Sort(got)
	want := []string{"news.*", "sports.*"}
	if !sliceEqual(got, want) {
		t.Errorf("ClusterPatterns: got %v want %v", got, want)
	}
}

// --- Inbound delivery ---------------------------------------------------

func TestHandleInboundMessage_DeliversToLocalBroker(t *testing.T) {
	broker := &fakeBroker{}
	r := NewPubSubRouter(NodeId(1), &fakeTransport{}, broker)
	r.HandleInboundMessage([]byte("news"), []byte("body"))

	got := broker.deliveredChannels()
	if !sliceEqual(got, []string{"news"}) {
		t.Errorf("delivered channels: got %v", got)
	}
}

// --- helpers ------------------------------------------------------------

func mustDecodeEffect(t *testing.T, notify *pb.OffsetNotify) *pb.Effect {
	t.Helper()
	// Wire format: [4-byte LE keyLen][key][protoData]
	data := notify.EffectData
	if len(data) < 4 {
		t.Fatalf("wire data too short")
	}
	keyLen := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
	if uint32(len(data)) < 4+keyLen {
		t.Fatalf("wire data truncated")
	}
	proto := data[4+keyLen:]
	eff := &pb.Effect{}
	if err := unmarshalProto(proto, eff); err != nil {
		t.Fatalf("unmarshal effect: %v", err)
	}
	return eff
}

func unmarshalProto(b []byte, m *pb.Effect) error {
	return proto.Unmarshal(b, m)
}

func sortNodeIDs(ids []NodeId) []NodeId {
	out := append([]NodeId(nil), ids...)
	slices.Sort(out)
	return out
}

func sliceEqual[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
