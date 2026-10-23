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

package shared

import "sync/atomic"

// PubSubBroker is the interface for the pub/sub subsystem.
// This allows the pubsub module to register commands via init() without circular imports.
type PubSubBroker interface {
	Subscribe(client *PubSubClient, channels ...string) []int
	Unsubscribe(client *PubSubClient, channels ...string) ([]string, []int)
	PSubscribe(client *PubSubClient, patterns ...string) []int
	PUnsubscribe(client *PubSubClient, patterns ...string) ([]string, []int)
	Publish(channel string, message []byte) int
	Cleanup(client *PubSubClient)
	SubscriptionCount(client *PubSubClient) int
	Channels(pattern string) []string
	NumSub(channels ...string) map[string]int
	NumPat() int
}

type pubSubBrokerHolder struct {
	broker PubSubBroker
}

var pubSubBroker atomic.Pointer[pubSubBrokerHolder]

// SetPubSubBroker sets the pub/sub broker instance.
func SetPubSubBroker(b PubSubBroker) {
	pubSubBroker.Store(&pubSubBrokerHolder{broker: b})
}

// GetPubSubBroker returns the current pub/sub broker instance (may be nil).
func GetPubSubBroker() PubSubBroker {
	h := pubSubBroker.Load()
	if h == nil || h.broker == nil {
		return nil
	}

	return h.broker
}

// ClearPubSubBroker fully clears the global pub/sub broker pointer.
func ClearPubSubBroker() {
	pubSubBroker.Store(nil)
}
