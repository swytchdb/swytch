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

// PubSubClusterRouter is the cluster-side surface the local pub/sub
// broker uses to fan out subscriptions and messages to peer nodes.
// A nil router (standalone mode) means every call is a no-op.
type PubSubClusterRouter interface {
	// AnnounceSub broadcasts an ephemeral SubscriptionEffect to every
	// peer so they record this node's interest in the channel / pattern.
	// Called on the 0→1 transition for a local subscription.
	AnnounceSub(channel string, isPattern bool)

	// AnnounceUnsub is the unsubscribe counterpart of AnnounceSub.
	// Called on the last→0 transition for a local subscription.
	AnnounceUnsub(channel string, isPattern bool)

	// RouteMessage delivers a PUBLISH to remote peers whose recorded
	// subscriptions match the channel (literal channel match or glob
	// match against a recorded pattern). Returns the number of peers
	// the message was sent to (one count per peer regardless of how
	// many of their local clients are subscribed).
	RouteMessage(channel string, payload []byte) int

	// ClusterChannels returns the union of channels remote peers have
	// announced interest in, optionally filtered by pattern.
	ClusterChannels(pattern string) []string

	// ClusterNumSub returns, per channel, the count of remote peers
	// announcing interest in that exact channel name. Patterns are
	// not counted (matching Redis NUMSUB semantics).
	ClusterNumSub(channels []string) map[string]int

	// ClusterPatterns returns every distinct pattern announced by a
	// remote peer. Returned to the caller (not as a count) so the
	// local broker can union with its own pattern set and dedupe.
	ClusterPatterns() []string
}

var pubsubClusterRouter atomic.Pointer[pubsubClusterRouterHolder]

type pubsubClusterRouterHolder struct {
	router PubSubClusterRouter
}

// SetPubSubClusterRouter installs the cluster router. Called by the
// cluster wiring at startup; pass nil to clear.
func SetPubSubClusterRouter(r PubSubClusterRouter) {
	if r == nil {
		pubsubClusterRouter.Store(nil)
		return
	}
	pubsubClusterRouter.Store(&pubsubClusterRouterHolder{router: r})
}

// GetPubSubClusterRouter returns the installed router, or nil in
// standalone mode.
func GetPubSubClusterRouter() PubSubClusterRouter {
	h := pubsubClusterRouter.Load()
	if h == nil {
		return nil
	}
	return h.router
}
