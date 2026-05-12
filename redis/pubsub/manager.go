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
	"path/filepath"
	"sort"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/swytchdb/swytch/redis/shared"
)

// Manager manages pub/sub subscriptions
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
	defer m.mu.Unlock()

	counts := make([]int, len(channels))

	for i, channel := range channels {
		// Add client to channel subscribers
		if m.channels[channel] == nil {
			m.channels[channel] = make(map[*shared.PubSubClient]struct{})
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

	return counts
}

// Unsubscribe unsubscribes a client from channels
// If no channels specified, unsubscribes from all channels
// Returns channel names and subscription counts
func (m *Manager) Unsubscribe(client *shared.PubSubClient, channels ...string) ([]string, []int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// If no channels specified, unsubscribe from all
	if len(channels) == 0 {
		clientChans := m.clientChannels[client]
		if len(clientChans) == 0 {
			// No subscriptions, return empty result
			return []string{}, []int{0}
		}
		channels = make([]string, 0, len(clientChans))
		for ch := range clientChans {
			channels = append(channels, ch)
		}
	}

	names := make([]string, len(channels))
	counts := make([]int, len(channels))

	for i, channel := range channels {
		names[i] = channel

		// Remove client from channel subscribers
		if subs, ok := m.channels[channel]; ok {
			delete(subs, client)
			if len(subs) == 0 {
				delete(m.channels, channel)
			}
		}

		// Remove channel from client's subscriptions
		if clientChans := m.clientChannels[client]; clientChans != nil {
			delete(clientChans, channel)
		}

		// Count is total subscriptions remaining
		counts[i] = len(m.clientChannels[client]) + len(m.clientPatterns[client])
	}

	return names, counts
}

// PSubscribe subscribes a client to one or more patterns
// Returns the subscription count after each subscription
func (m *Manager) PSubscribe(client *shared.PubSubClient, patterns ...string) []int {
	m.mu.Lock()
	defer m.mu.Unlock()

	counts := make([]int, len(patterns))

	for i, pattern := range patterns {
		// Add client to pattern subscribers
		if m.patterns[pattern] == nil {
			m.patterns[pattern] = make(map[*shared.PubSubClient]struct{})
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

	return counts
}

// PUnsubscribe unsubscribes a client from patterns
// If no patterns specified, unsubscribes from all patterns
// Returns pattern names and subscription counts
func (m *Manager) PUnsubscribe(client *shared.PubSubClient, patterns ...string) ([]string, []int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// If no patterns specified, unsubscribe from all
	if len(patterns) == 0 {
		clientPats := m.clientPatterns[client]
		if len(clientPats) == 0 {
			// No subscriptions, return empty result
			return []string{}, []int{0}
		}
		patterns = make([]string, 0, len(clientPats))
		for pat := range clientPats {
			patterns = append(patterns, pat)
		}
	}

	names := make([]string, len(patterns))
	counts := make([]int, len(patterns))

	for i, pattern := range patterns {
		names[i] = pattern

		// Remove client from pattern subscribers
		if subs, ok := m.patterns[pattern]; ok {
			delete(subs, client)
			if len(subs) == 0 {
				delete(m.patterns, pattern)
			}
		}

		// Remove pattern from client's subscriptions
		if clientPats := m.clientPatterns[client]; clientPats != nil {
			delete(clientPats, pattern)
		}

		// Count is total subscriptions remaining
		counts[i] = len(m.clientChannels[client]) + len(m.clientPatterns[client])
	}

	return names, counts
}

// Publish publishes a message to a channel
// Returns the number of clients that received the message
func (m *Manager) Publish(channel string, message []byte) int {
	token := m.mu.RLock()
	defer m.mu.RUnlock(token)

	count := 0

	// Send to exact channel subscribers
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

	// Send to pattern subscribers
	for pattern, subs := range m.patterns {
		if matchGlob(pattern, channel) {
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

// Cleanup removes a client from all subscriptions
func (m *Manager) Cleanup(client *shared.PubSubClient) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Remove from all channels
	if channels := m.clientChannels[client]; channels != nil {
		for channel := range channels {
			if subs, ok := m.channels[channel]; ok {
				delete(subs, client)
				if len(subs) == 0 {
					delete(m.channels, channel)
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
				}
			}
		}
		delete(m.clientPatterns, client)
	}

	// Close the client
	client.Close()
}

// Channels returns active channels matching the optional pattern
func (m *Manager) Channels(pattern string) []string {
	token := m.mu.RLock()
	defer m.mu.RUnlock(token)

	result := make([]string, 0, len(m.channels))
	for channel := range m.channels {
		if pattern == "" || matchGlob(pattern, channel) {
			result = append(result, channel)
		}
	}

	sort.Strings(result)
	return result
}

// NumSub returns the number of subscribers for the given channels
func (m *Manager) NumSub(channels ...string) map[string]int {
	token := m.mu.RLock()
	defer m.mu.RUnlock(token)

	result := make(map[string]int, len(channels))
	for _, channel := range channels {
		if subs, ok := m.channels[channel]; ok {
			result[channel] = len(subs)
		} else {
			result[channel] = 0
		}
	}
	return result
}

// NumPat returns the number of unique patterns subscribed to
func (m *Manager) NumPat() int {
	token := m.mu.RLock()
	defer m.mu.RUnlock(token)

	return len(m.patterns)
}

// SubscriptionCount returns the total subscription count for a client
func (m *Manager) SubscriptionCount(client *shared.PubSubClient) int {
	token := m.mu.RLock()
	defer m.mu.RUnlock(token)

	return len(m.clientChannels[client]) + len(m.clientPatterns[client])
}

// matchGlob matches a pattern against a string using Redis glob semantics
func matchGlob(pattern, s string) bool {
	matched, err := filepath.Match(pattern, s)
	if err != nil {
		return false
	}
	return matched
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
