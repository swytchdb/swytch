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

package pubsub

import (
	"bytes"
	"sort"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/swytchdb/swytch/keytrie"
	"github.com/swytchdb/swytch/redis/shared"
)

// Manager manages pub/sub subscriptions.
//
// When a cluster router is registered via shared.SetPubSubClusterRouter,
// subscribe/unsubscribe transitions (first local subscriber for a
// channel/pattern; last local subscriber leaving) are announced to
// peers, and PUBLISH fans out to peers whose recorded subscriptions
// match. Without a router (standalone), all cluster paths are no-ops
// and the manager behaves exactly as a single-node broker.
type Manager struct {
	mu xsync.RBMutex

	// channel -> set of subscribers
	channels map[string]map[*shared.PubSubClient]struct{}

	// pattern -> set of subscribers
	patterns map[string]map[*shared.PubSubClient]struct{}

	// reverse index: client -> set of channels
	clientChannels map[*shared.PubSubClient]map[string]struct{}

	// reverse index: client -> set of patterns
	clientPatterns map[*shared.PubSubClient]map[string]struct{}
}

// NewManager creates a new pub/sub manager
func NewManager() *Manager {
	return &Manager{
		channels:       make(map[string]map[*shared.PubSubClient]struct{}),
		patterns:       make(map[string]map[*shared.PubSubClient]struct{}),
		clientChannels: make(map[*shared.PubSubClient]map[string]struct{}),
		clientPatterns: make(map[*shared.PubSubClient]map[string]struct{}),
	}
}

// Subscribe subscribes a client to one or more channels
// Returns the subscription count after each subscription
func (m *Manager) Subscribe(client *shared.PubSubClient, channels ...string) []int {
	m.mu.Lock()
	counts := make([]int, len(channels))

	// Collect channels we need to announce to the cluster — only the
	// 0→1 transition for a given channel triggers a cluster announce.
	var firstTime []string

	for i, channel := range channels {
		// Add client to channel subscribers
		if m.channels[channel] == nil {
			m.channels[channel] = make(map[*shared.PubSubClient]struct{})
			firstTime = append(firstTime, channel)
		}
		m.channels[channel][client] = struct{}{}

		// Add channel to client's subscriptions
		if m.clientChannels[client] == nil {
			m.clientChannels[client] = make(map[string]struct{})
		}
		m.clientChannels[client][channel] = struct{}{}

		// Count is total subscriptions (channels + patterns)
		counts[i] = len(m.clientChannels[client]) + len(m.clientPatterns[client])
	}
	m.mu.Unlock()

	announceTransitions(firstTime, nil, false)
	return counts
}

// Unsubscribe unsubscribes a client from channels
// If no channels specified, unsubscribes from all channels
// Returns channel names and subscription counts
func (m *Manager) Unsubscribe(client *shared.PubSubClient, channels ...string) ([]string, []int) {
	m.mu.Lock()

	// If no channels specified, unsubscribe from all
	if len(channels) == 0 {
		clientChans := m.clientChannels[client]
		if len(clientChans) == 0 {
			m.mu.Unlock()
			return []string{}, []int{0}
		}
		channels = make([]string, 0, len(clientChans))
		for ch := range clientChans {
			channels = append(channels, ch)
		}
	}

	names := make([]string, len(channels))
	counts := make([]int, len(channels))
	var lastGone []string

	for i, channel := range channels {
		names[i] = channel

		// Remove client from channel subscribers
		if subs, ok := m.channels[channel]; ok {
			delete(subs, client)
			if len(subs) == 0 {
				delete(m.channels, channel)
				lastGone = append(lastGone, channel)
			}
		}

		// Remove channel from client's subscriptions
		if clientChans := m.clientChannels[client]; clientChans != nil {
			delete(clientChans, channel)
		}

		// Count is total subscriptions remaining
		counts[i] = len(m.clientChannels[client]) + len(m.clientPatterns[client])
	}
	m.mu.Unlock()

	announceTransitions(lastGone, nil, true)
	return names, counts
}

// PSubscribe subscribes a client to one or more patterns
// Returns the subscription count after each subscription
func (m *Manager) PSubscribe(client *shared.PubSubClient, patterns ...string) []int {
	m.mu.Lock()

	counts := make([]int, len(patterns))
	var firstTime []string

	for i, pattern := range patterns {
		// Add client to pattern subscribers
		if m.patterns[pattern] == nil {
			m.patterns[pattern] = make(map[*shared.PubSubClient]struct{})
			firstTime = append(firstTime, pattern)
		}
		m.patterns[pattern][client] = struct{}{}

		// Add pattern to client's subscriptions
		if m.clientPatterns[client] == nil {
			m.clientPatterns[client] = make(map[string]struct{})
		}
		m.clientPatterns[client][pattern] = struct{}{}

		// Count is total subscriptions (channels + patterns)
		counts[i] = len(m.clientChannels[client]) + len(m.clientPatterns[client])
	}
	m.mu.Unlock()

	announceTransitions(nil, firstTime, false)
	return counts
}

// PUnsubscribe unsubscribes a client from patterns
// If no patterns specified, unsubscribes from all patterns
// Returns pattern names and subscription counts
func (m *Manager) PUnsubscribe(client *shared.PubSubClient, patterns ...string) ([]string, []int) {
	m.mu.Lock()

	if len(patterns) == 0 {
		clientPats := m.clientPatterns[client]
		if len(clientPats) == 0 {
			m.mu.Unlock()
			return []string{}, []int{0}
		}
		patterns = make([]string, 0, len(clientPats))
		for pat := range clientPats {
			patterns = append(patterns, pat)
		}
	}

	names := make([]string, len(patterns))
	counts := make([]int, len(patterns))
	var lastGone []string

	for i, pattern := range patterns {
		names[i] = pattern

		// Remove client from pattern subscribers
		if subs, ok := m.patterns[pattern]; ok {
			delete(subs, client)
			if len(subs) == 0 {
				delete(m.patterns, pattern)
				lastGone = append(lastGone, pattern)
			}
		}

		// Remove pattern from client's subscriptions
		if clientPats := m.clientPatterns[client]; clientPats != nil {
			delete(clientPats, pattern)
		}

		// Count is total subscriptions remaining
		counts[i] = len(m.clientChannels[client]) + len(m.clientPatterns[client])
	}
	m.mu.Unlock()

	announceTransitions(nil, lastGone, true)
	return names, counts
}

// Publish publishes a message to a channel.
// Returns the number of local clients that received the message plus
// the number of remote peers the message was routed to (one count
// per peer regardless of remote subscriber population — Swytch acts
// like a single-node Redis instance to clients but cannot observe
// remote client counts without unacceptable staleness).
func (m *Manager) Publish(channel string, message []byte) int {
	count := m.deliverLocal(channel, message)
	if router := shared.GetPubSubClusterRouter(); router != nil {
		count += router.RouteMessage(channel, message)
	}
	return count
}

// DeliverLocal hands an inbound cross-cluster PUBLISH off to local
// subscribers without re-broadcasting. Implements
// cluster.LocalPubSubBroker.
func (m *Manager) DeliverLocal(channel string, payload []byte) {
	m.deliverLocal(channel, payload)
}

// deliverLocal is the shared local-only delivery path used both by
// Publish (after which the cluster fanout happens) and DeliverLocal
// (which receives a cross-node message and must not re-broadcast).
// Returns local delivered count.
func (m *Manager) deliverLocal(channel string, message []byte) int {
	token := m.mu.RLock()
	defer m.mu.RUnlock(token)

	count := 0

	// Exact channel subscribers
	if subs, ok := m.channels[channel]; ok {
		for client := range subs {
			if client.Send(&shared.PubSubMessage{
				Type:    "message",
				Channel: channel,
				Payload: message,
			}) {
				count++
			}
		}
	}

	// Pattern subscribers
	for pattern, subs := range m.patterns {
		if keytrie.MatchGlob(channel, pattern) {
			for client := range subs {
				if client.Send(&shared.PubSubMessage{
					Type:    "pmessage",
					Pattern: pattern,
					Channel: channel,
					Payload: message,
				}) {
					count++
				}
			}
		}
	}

	return count
}

// LocalSubsSnapshot returns a snapshot of every channel and pattern
// with at least one local subscriber. Used by the cluster router to
// re-announce to a newly joined peer. Implements
// cluster.LocalPubSubBroker.
func (m *Manager) LocalSubsSnapshot() (channels []string, patterns []string) {
	token := m.mu.RLock()
	defer m.mu.RUnlock(token)

	channels = make([]string, 0, len(m.channels))
	for ch := range m.channels {
		channels = append(channels, ch)
	}
	patterns = make([]string, 0, len(m.patterns))
	for p := range m.patterns {
		patterns = append(patterns, p)
	}
	return channels, patterns
}

// Cleanup removes a client from all subscriptions
func (m *Manager) Cleanup(client *shared.PubSubClient) {
	m.mu.Lock()

	var lastGoneChans []string
	var lastGonePats []string

	// Remove from all channels
	if channels := m.clientChannels[client]; channels != nil {
		for channel := range channels {
			if subs, ok := m.channels[channel]; ok {
				delete(subs, client)
				if len(subs) == 0 {
					delete(m.channels, channel)
					lastGoneChans = append(lastGoneChans, channel)
				}
			}
		}
		delete(m.clientChannels, client)
	}

	// Remove from all patterns
	if patterns := m.clientPatterns[client]; patterns != nil {
		for pattern := range patterns {
			if subs, ok := m.patterns[pattern]; ok {
				delete(subs, client)
				if len(subs) == 0 {
					delete(m.patterns, pattern)
					lastGonePats = append(lastGonePats, pattern)
				}
			}
		}
		delete(m.clientPatterns, client)
	}
	m.mu.Unlock()

	announceTransitions(lastGoneChans, lastGonePats, true)
	client.Close()
}

// announceTransitions notifies the cluster router about local subscribe
// or unsubscribe transitions. unsubscribe=false signals 0→1 (first
// local subscriber appeared); true signals last→0. Channels and
// patterns are announced separately. No-op when there is no router
// (standalone mode).
func announceTransitions(channels, patterns []string, unsubscribe bool) {
	if len(channels) == 0 && len(patterns) == 0 {
		return
	}
	router := shared.GetPubSubClusterRouter()
	if router == nil {
		return
	}
	announce := router.AnnounceSub
	if unsubscribe {
		announce = router.AnnounceUnsub
	}
	for _, ch := range channels {
		announce(ch, false)
	}
	for _, p := range patterns {
		announce(p, true)
	}
}

// Channels returns active channels (cluster-wide), filtered by an
// optional glob pattern.
func (m *Manager) Channels(pattern string) []string {
	token := m.mu.RLock()
	seen := make(map[string]struct{}, len(m.channels))
	for channel := range m.channels {
		if pattern == "" || keytrie.MatchGlob(channel, pattern) {
			seen[channel] = struct{}{}
		}
	}
	m.mu.RUnlock(token)

	if router := shared.GetPubSubClusterRouter(); router != nil {
		for _, ch := range router.ClusterChannels(pattern) {
			seen[ch] = struct{}{}
		}
	}

	result := make([]string, 0, len(seen))
	for ch := range seen {
		result = append(result, ch)
	}
	sort.Strings(result)
	return result
}

// NumSub returns the cluster-wide number of subscribers for the
// given channels (sum of local clients on this node and remote
// peers with a matching announce).
func (m *Manager) NumSub(channels ...string) map[string]int {
	result := make(map[string]int, len(channels))

	token := m.mu.RLock()
	for _, channel := range channels {
		if subs, ok := m.channels[channel]; ok {
			result[channel] = len(subs)
		} else {
			result[channel] = 0
		}
	}
	m.mu.RUnlock(token)

	if router := shared.GetPubSubClusterRouter(); router != nil {
		remote := router.ClusterNumSub(channels)
		for ch, n := range remote {
			result[ch] += n
		}
	}
	return result
}

// NumPat returns the number of distinct patterns subscribed to
// across the cluster, deduped across local and remote peers.
func (m *Manager) NumPat() int {
	token := m.mu.RLock()
	seen := make(map[string]struct{}, len(m.patterns))
	for p := range m.patterns {
		seen[p] = struct{}{}
	}
	m.mu.RUnlock(token)

	if router := shared.GetPubSubClusterRouter(); router != nil {
		for _, p := range router.ClusterPatterns() {
			seen[p] = struct{}{}
		}
	}
	return len(seen)
}

// SubscriptionCount returns the total subscription count for a client
func (m *Manager) SubscriptionCount(client *shared.PubSubClient) int {
	token := m.mu.RLock()
	defer m.mu.RUnlock(token)

	return len(m.clientChannels[client]) + len(m.clientPatterns[client])
}

// FormatPubSubMessage formats a pub/sub message for wire transmission
func FormatPubSubMessage(msg *shared.PubSubMessage, protocol shared.ProtocolVersion) []byte {
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	w.SetProtocol(protocol)

	switch msg.Type {
	case "message":
		if protocol == shared.RESP3 {
			w.WritePush(3)
		} else {
			w.WriteArray(3)
		}
		w.WriteBulkStringStr("message")
		w.WriteBulkStringStr(msg.Channel)
		w.WriteBulkString(msg.Payload)

	case "pmessage":
		if protocol == shared.RESP3 {
			w.WritePush(4)
		} else {
			w.WriteArray(4)
		}
		w.WriteBulkStringStr("pmessage")
		w.WriteBulkStringStr(msg.Pattern)
		w.WriteBulkStringStr(msg.Channel)
		w.WriteBulkString(msg.Payload)

	case "subscribe", "unsubscribe":
		if protocol == shared.RESP3 {
			w.WritePush(3)
		} else {
			w.WriteArray(3)
		}
		w.WriteBulkStringStr(msg.Type)
		if msg.Channel != "" {
			w.WriteBulkStringStr(msg.Channel)
		} else {
			w.WriteNullBulkString()
		}
		w.WriteInteger(int64(msg.Count))

	case "psubscribe", "punsubscribe":
		if protocol == shared.RESP3 {
			w.WritePush(3)
		} else {
			w.WriteArray(3)
		}
		w.WriteBulkStringStr(msg.Type)
		if msg.Pattern != "" {
			w.WriteBulkStringStr(msg.Pattern)
		} else {
			w.WriteNullBulkString()
		}
		w.WriteInteger(int64(msg.Count))
	}

	return buf.Bytes()
}
