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
	"bytes"
	"log/slog"
	"sort"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/keytrie"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Reserved routing prefixes for pub/sub channel and pattern keys.
// The trailing 0x00 separates the prefix tag from the user-supplied
// channel/pattern name so a channel literally named "pat:x" cannot
// collide with the pattern namespace. KEYS may surface these — that's
// acceptable per the design.
var (
	chKeyPrefix  = []byte("__swytch:ch\x00")
	patKeyPrefix = []byte("__swytch:pat\x00")
)

// encodeRoutingKey returns the routing key carried in the Effect
// envelope. Pass chKeyPrefix for a literal channel, patKeyPrefix for
// a pattern.
func encodeRoutingKey(prefix []byte, name string) []byte {
	out := make([]byte, 0, len(prefix)+len(name))
	out = append(out, prefix...)
	out = append(out, name...)
	return out
}

// decodeRoutingKey identifies a routing key as a channel or pattern
// subscription and extracts the user-supplied name. Returns ok=false
// for keys that aren't pub/sub routing keys.
func decodeRoutingKey(routingKey []byte) (name string, isPattern bool, ok bool) {
	if bytes.HasPrefix(routingKey, chKeyPrefix) {
		return string(routingKey[len(chKeyPrefix):]), false, true
	}
	if bytes.HasPrefix(routingKey, patKeyPrefix) {
		return string(routingKey[len(patKeyPrefix):]), true, true
	}
	return "", false, false
}

// LocalPubSubBroker is the slice of the local pub/sub broker the
// cluster router needs: inbound delivery and a snapshot of local
// subscriptions for re-announce on peer join. Implemented by the
// in-process pub/sub Manager.
type LocalPubSubBroker interface {
	// DeliverLocal hands a received cross-node PUBLISH off to local
	// subscribers. The broker is responsible for matching against
	// both literal channels and locally-subscribed patterns.
	DeliverLocal(channel string, payload []byte)

	// LocalSubsSnapshot returns every channel and pattern that has at
	// least one local subscriber. Used to re-announce when a peer
	// joins.
	LocalSubsSnapshot() (channels []string, patterns []string)
}

// RouterTransport is the slice of PeerManager the router relies on.
// Exported so tests can supply a mock without spinning up QUIC.
type RouterTransport interface {
	PeerIDs() []NodeId
	SendOneWayTo(notify *pb.OffsetNotify, wireData []byte, peerID NodeId) error
}

// PubSubRouter owns the per-peer subscription table and bridges the
// local pub/sub broker with cluster gossip.
//
// It is registered into redis/shared via SetPubSubClusterRouter so
// the local broker can call into it, and into the effects engine via
// callbacks so inbound ephemeral effects land here.
type PubSubRouter struct {
	self      NodeId
	transport RouterTransport
	broker    LocalPubSubBroker

	mu       xsync.RBMutex
	channels map[NodeId]map[string]struct{} // peerID -> set of exact channels
	patterns map[NodeId]map[string]struct{} // peerID -> set of patterns
}

// NewPubSubRouter constructs a router. The broker is the in-process
// pub/sub manager; the transport supplies broadcast/addressed-send.
// Caller is responsible for wiring the router into the engine's
// OnPubSubMessage / OnEphemeralSubscribe callbacks and into
// PeerManager.SetPeerLifecycleHooks.
func NewPubSubRouter(self NodeId, transport RouterTransport, broker LocalPubSubBroker) *PubSubRouter {
	return &PubSubRouter{
		self:      self,
		transport: transport,
		broker:    broker,
		channels:  make(map[NodeId]map[string]struct{}),
		patterns:  make(map[NodeId]map[string]struct{}),
	}
}

// --- Engine callbacks (inbound) -----------------------------------------

// HandleEphemeralSubscribe is wired to effects.Engine.OnEphemeralSubscribe.
// It mutates the per-peer table based on the routing key encoding.
func (r *PubSubRouter) HandleEphemeralSubscribe(subscriberNodeID uint64, routingKey []byte, unsubscribe bool) {
	name, isPattern, ok := decodeRoutingKey(routingKey)
	if !ok {
		slog.Debug("pubsub router: ignoring unknown routing key prefix",
			"key", string(routingKey))
		return
	}
	peerID := NodeId(subscriberNodeID)
	if peerID == r.self {
		// Local-origin echo: we never broadcast to ourselves, but
		// guard regardless.
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	table := r.channels
	if isPattern {
		table = r.patterns
	}

	if unsubscribe {
		if set := table[peerID]; set != nil {
			delete(set, name)
			if len(set) == 0 {
				delete(table, peerID)
			}
		}
		return
	}

	set := table[peerID]
	if set == nil {
		set = make(map[string]struct{})
		table[peerID] = set
	}
	set[name] = struct{}{}
}

// HandleInboundMessage is wired to effects.Engine.OnPubSubMessage. It
// hands the message off to the local broker for delivery.
func (r *PubSubRouter) HandleInboundMessage(channel, payload []byte) {
	r.broker.DeliverLocal(string(channel), payload)
}

// --- Peer lifecycle (from PeerManager) ----------------------------------

// OnPeerAdded re-announces every local subscription to the new peer
// so it learns about them. Called from PeerManager's registerPeer.
func (r *PubSubRouter) OnPeerAdded(peerID NodeId) {
	if peerID == r.self {
		return
	}
	channels, patterns := r.broker.LocalSubsSnapshot()
	for _, ch := range channels {
		r.sendAnnounce(encodeRoutingKey(chKeyPrefix, ch), false, peerID)
	}
	for _, pat := range patterns {
		r.sendAnnounce(encodeRoutingKey(patKeyPrefix, pat), false, peerID)
	}
}

// OnPeerRemoved drops every entry the departing peer owned in the
// per-peer table. Called from PeerManager's unregisterPeer.
func (r *PubSubRouter) OnPeerRemoved(peerID NodeId) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.channels, peerID)
	delete(r.patterns, peerID)
}

// --- shared.PubSubClusterRouter implementation --------------------------

// AnnounceSub broadcasts an ephemeral SubscriptionEffect to every
// connected peer. Fire-and-forget; receivers register us in their
// per-peer tables and return immediately without log storage.
func (r *PubSubRouter) AnnounceSub(channel string, isPattern bool) {
	r.broadcastAnnounce(routingKeyFor(channel, isPattern), false)
}

// AnnounceUnsub broadcasts an unsubscribe ephemeral SubscriptionEffect.
func (r *PubSubRouter) AnnounceUnsub(channel string, isPattern bool) {
	r.broadcastAnnounce(routingKeyFor(channel, isPattern), true)
}

func routingKeyFor(name string, isPattern bool) []byte {
	if isPattern {
		return encodeRoutingKey(patKeyPrefix, name)
	}
	return encodeRoutingKey(chKeyPrefix, name)
}

// RouteMessage delivers a PUBLISH to every remote peer with a matching
// literal channel or matching pattern, and returns the count of peers
// the message was dispatched to.
func (r *PubSubRouter) RouteMessage(channel string, payload []byte) int {
	targets := r.targetsFor(channel)
	if len(targets) == 0 {
		return 0
	}

	notify, wireData := r.buildPubSubMessage(channel, payload)
	sent := 0
	for _, peerID := range targets {
		if err := r.transport.SendOneWayTo(notify, wireData, peerID); err != nil {
			slog.Debug("pubsub router: one-way send failed",
				"peer", peerID, "channel", channel, "error", err)
			continue
		}
		sent++
	}
	return sent
}

// ClusterChannels returns every channel a remote peer is interested
// in (deduped), filtered by pattern if non-empty.
func (r *PubSubRouter) ClusterChannels(pattern string) []string {
	token := r.mu.RLock()
	defer r.mu.RUnlock(token)

	seen := make(map[string]struct{})
	for _, set := range r.channels {
		for ch := range set {
			if pattern == "" || keytrie.MatchGlob(ch, pattern) {
				seen[ch] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for ch := range seen {
		out = append(out, ch)
	}
	sort.Strings(out)
	return out
}

// ClusterNumSub counts, per channel, the number of peers announcing
// that exact channel. Patterns are not counted (Redis NUMSUB ignores
// pattern subscriptions).
func (r *PubSubRouter) ClusterNumSub(channels []string) map[string]int {
	token := r.mu.RLock()
	defer r.mu.RUnlock(token)

	out := make(map[string]int, len(channels))
	for _, ch := range channels {
		count := 0
		for _, set := range r.channels {
			if _, ok := set[ch]; ok {
				count++
			}
		}
		out[ch] = count
	}
	return out
}

// ClusterPatterns returns every distinct pattern announced by any
// remote peer. The local broker unions this with its own pattern set
// to compute PUBSUB NUMPAT correctly across the cluster.
func (r *PubSubRouter) ClusterPatterns() []string {
	token := r.mu.RLock()
	defer r.mu.RUnlock(token)

	seen := make(map[string]struct{})
	for _, set := range r.patterns {
		for p := range set {
			seen[p] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	return out
}

// --- internals ----------------------------------------------------------

// targetsFor returns the peer IDs that should receive a PUBLISH for
// the given channel, based on the per-peer table.
func (r *PubSubRouter) targetsFor(channel string) []NodeId {
	token := r.mu.RLock()
	defer r.mu.RUnlock(token)

	matched := make(map[NodeId]struct{})
	for peerID, set := range r.channels {
		if _, ok := set[channel]; ok {
			matched[peerID] = struct{}{}
		}
	}
	for peerID, set := range r.patterns {
		if _, ok := matched[peerID]; ok {
			continue
		}
		for pat := range set {
			if keytrie.MatchGlob(channel, pat) {
				matched[peerID] = struct{}{}
				break
			}
		}
	}
	if len(matched) == 0 {
		return nil
	}
	out := make([]NodeId, 0, len(matched))
	for id := range matched {
		out = append(out, id)
	}
	return out
}

// broadcastAnnounce sends an ephemeral SubscriptionEffect to every
// connected peer via the replicator's broadcast path.
func (r *PubSubRouter) broadcastAnnounce(routingKey []byte, unsubscribe bool) {
	for _, peerID := range r.transport.PeerIDs() {
		r.sendAnnounce(routingKey, unsubscribe, peerID)
	}
}

func (r *PubSubRouter) sendAnnounce(routingKey []byte, unsubscribe bool, peerID NodeId) {
	hlc := timestamppb.Now()
	eff := &pb.Effect{
		Key:            routingKey,
		Hlc:            hlc,
		NodeId:         uint64(r.self),
		ForkChoiceHash: effects.ComputeForkChoiceHash(pb.NodeID(r.self), hlc),
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
			SubscriberNodeId: uint64(r.self),
			Unsubscribe:      unsubscribe,
			Ephemeral:        true,
		}},
	}
	data, err := effects.MarshalEffect(eff)
	if err != nil {
		slog.Debug("pubsub router: marshal announce failed", "error", err)
		return
	}
	notify := buildEphemeralNotify(r.self, eff, data)
	if err := r.transport.SendOneWayTo(notify, notify.EffectData, peerID); err != nil {
		slog.Debug("pubsub router: announce send failed",
			"peer", peerID, "unsubscribe", unsubscribe, "error", err)
	}
}

func (r *PubSubRouter) buildPubSubMessage(channel string, payload []byte) (*pb.OffsetNotify, []byte) {
	routingKey := encodeRoutingKey(chKeyPrefix, channel)
	hlc := timestamppb.Now()
	eff := &pb.Effect{
		Key:            routingKey,
		Hlc:            hlc,
		NodeId:         uint64(r.self),
		ForkChoiceHash: effects.ComputeForkChoiceHash(pb.NodeID(r.self), hlc),
		Kind: &pb.Effect_PubsubMessage{PubsubMessage: &pb.PubSubMessage{
			Channel: []byte(channel),
			Payload: payload,
		}},
	}
	data, err := effects.MarshalEffect(eff)
	if err != nil {
		slog.Debug("pubsub router: marshal message failed", "error", err)
		return nil, nil
	}
	notify := buildEphemeralNotify(r.self, eff, data)
	return notify, notify.EffectData
}

// buildEphemeralNotify delegates to effects.BuildOffsetNotify so wire
// framing lives in one place. The Origin is a non-zero sentinel — the
// receiver short-circuits on the SubscriptionEffect.Ephemeral /
// PubSubMessage branches before any real offset/log lookup, but
// HandleRemote rejects nil origins so we set a plausible one.
func buildEphemeralNotify(self NodeId, eff *pb.Effect, data []byte) *pb.OffsetNotify {
	origin := keytrie.EffectRef{uint64(self), uint64(time.Now().UnixNano())}
	return effects.BuildOffsetNotify(pb.NodeID(self), origin, eff, data, nil)
}
