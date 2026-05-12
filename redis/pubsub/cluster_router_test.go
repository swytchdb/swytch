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
	"slices"
	"sync"
	"testing"

	"github.com/swytchdb/swytch/redis/shared"
)

// fakeClusterRouter records calls so manager_test can verify that
// cluster announces fire on the right transitions and that PUBLISH
// fan-out goes through the router.
type fakeClusterRouter struct {
	mu              sync.Mutex
	subAnnounces    []announceCall
	unsubAnnounces  []announceCall
	routed          []routedCall
	routeCount      int
	clusterChans    []string
	clusterNumSub   map[string]int
	clusterPatterns []string
}

type announceCall struct {
	channel   string
	isPattern bool
}

type routedCall struct {
	channel string
	payload string
}

func (r *fakeClusterRouter) AnnounceSub(channel string, isPattern bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.subAnnounces = append(r.subAnnounces, announceCall{channel, isPattern})
}

func (r *fakeClusterRouter) AnnounceUnsub(channel string, isPattern bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unsubAnnounces = append(r.unsubAnnounces, announceCall{channel, isPattern})
}

func (r *fakeClusterRouter) RouteMessage(channel string, payload []byte) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routed = append(r.routed, routedCall{channel, string(payload)})
	return r.routeCount
}

func (r *fakeClusterRouter) ClusterChannels(_ string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.clusterChans...)
}

func (r *fakeClusterRouter) ClusterNumSub(channels []string) map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]int, len(channels))
	for _, ch := range channels {
		out[ch] = r.clusterNumSub[ch]
	}
	return out
}

func (r *fakeClusterRouter) ClusterPatterns() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.clusterPatterns...)
}

func withRouter(t *testing.T, r shared.PubSubClusterRouter, fn func()) {
	t.Helper()
	shared.SetPubSubClusterRouter(r)
	defer shared.SetPubSubClusterRouter(nil)
	fn()
}

// --- Tests --------------------------------------------------------------

func TestManager_Subscribe_AnnouncesOnFirstSubscriberOnly(t *testing.T) {
	r := &fakeClusterRouter{}
	withRouter(t, r, func() {
		m := NewManager()
		c1 := shared.NewPubSubClient(nil, shared.RESP2)
		c2 := shared.NewPubSubClient(nil, shared.RESP2)

		m.Subscribe(c1, "news")
		m.Subscribe(c2, "news") // second subscriber: no new announce

		if len(r.subAnnounces) != 1 {
			t.Fatalf("expected 1 sub announce, got %d (%v)", len(r.subAnnounces), r.subAnnounces)
		}
		if r.subAnnounces[0] != (announceCall{channel: "news", isPattern: false}) {
			t.Errorf("unexpected announce: %v", r.subAnnounces[0])
		}
	})
}

func TestManager_Unsubscribe_AnnouncesWhenLastLeaves(t *testing.T) {
	r := &fakeClusterRouter{}
	withRouter(t, r, func() {
		m := NewManager()
		c1 := shared.NewPubSubClient(nil, shared.RESP2)
		c2 := shared.NewPubSubClient(nil, shared.RESP2)
		m.Subscribe(c1, "news")
		m.Subscribe(c2, "news")

		m.Unsubscribe(c1, "news") // one local subscriber remains: no unsub
		if len(r.unsubAnnounces) != 0 {
			t.Errorf("expected 0 unsub announces, got %d", len(r.unsubAnnounces))
		}

		m.Unsubscribe(c2, "news") // last local subscriber: announce unsub
		if len(r.unsubAnnounces) != 1 {
			t.Fatalf("expected 1 unsub announce, got %d", len(r.unsubAnnounces))
		}
		if r.unsubAnnounces[0] != (announceCall{channel: "news", isPattern: false}) {
			t.Errorf("unexpected unsub: %v", r.unsubAnnounces[0])
		}
	})
}

func TestManager_PSubscribe_AnnouncesPattern(t *testing.T) {
	r := &fakeClusterRouter{}
	withRouter(t, r, func() {
		m := NewManager()
		c := shared.NewPubSubClient(nil, shared.RESP2)
		m.PSubscribe(c, "news.*")

		if len(r.subAnnounces) != 1 || !r.subAnnounces[0].isPattern || r.subAnnounces[0].channel != "news.*" {
			t.Fatalf("expected pattern announce for news.*, got %v", r.subAnnounces)
		}
	})
}

func TestManager_Publish_AddsRouterCountToLocal(t *testing.T) {
	r := &fakeClusterRouter{routeCount: 2}
	withRouter(t, r, func() {
		m := NewManager()
		c := shared.NewPubSubClient(nil, shared.RESP2)
		m.Subscribe(c, "news")

		got := m.Publish("news", []byte("hi"))
		// 1 local subscriber + 2 routed peers = 3
		if got != 3 {
			t.Errorf("publish count: got %d want 3", got)
		}
		if len(r.routed) != 1 {
			t.Fatalf("expected 1 routed call, got %d", len(r.routed))
		}
		if r.routed[0] != (routedCall{"news", "hi"}) {
			t.Errorf("routed call: %v", r.routed[0])
		}
	})
}

func TestManager_DeliverLocal_NoReBroadcast(t *testing.T) {
	r := &fakeClusterRouter{routeCount: 99}
	withRouter(t, r, func() {
		m := NewManager()
		c := shared.NewPubSubClient(nil, shared.RESP2)
		m.Subscribe(c, "news")

		m.DeliverLocal("news", []byte("from-peer"))

		select {
		case msg := <-c.MsgChan():
			if string(msg.Payload) != "from-peer" {
				t.Errorf("payload: got %q", string(msg.Payload))
			}
		default:
			t.Fatalf("local subscriber did not receive inbound message")
		}
		if len(r.routed) != 0 {
			t.Errorf("DeliverLocal must not re-broadcast, routed=%v", r.routed)
		}
	})
}

func TestManager_Channels_MergesClusterView(t *testing.T) {
	r := &fakeClusterRouter{clusterChans: []string{"remote-only", "shared"}}
	withRouter(t, r, func() {
		m := NewManager()
		c := shared.NewPubSubClient(nil, shared.RESP2)
		m.Subscribe(c, "local-only")
		m.Subscribe(c, "shared")

		got := m.Channels("")
		want := []string{"local-only", "remote-only", "shared"}
		if !slices.Equal(got, want) {
			t.Errorf("Channels: got %v want %v", got, want)
		}
	})
}

func TestManager_NumSub_SumsLocalAndRemote(t *testing.T) {
	r := &fakeClusterRouter{
		clusterNumSub: map[string]int{"news": 4, "sports": 1, "absent": 0},
	}
	withRouter(t, r, func() {
		m := NewManager()
		c1 := shared.NewPubSubClient(nil, shared.RESP2)
		c2 := shared.NewPubSubClient(nil, shared.RESP2)
		m.Subscribe(c1, "news")
		m.Subscribe(c2, "news")

		got := m.NumSub("news", "sports", "absent")
		if got["news"] != 6 {
			t.Errorf("news: got %d want 6", got["news"])
		}
		if got["sports"] != 1 {
			t.Errorf("sports: got %d want 1", got["sports"])
		}
		if got["absent"] != 0 {
			t.Errorf("absent: got %d want 0", got["absent"])
		}
	})
}

func TestManager_NumPat_DedupesLocalAndRemote(t *testing.T) {
	// Local subscribes to news.* and sports.*; remote also announces
	// news.* (overlap) plus alerts.*. Cluster total should be 3, not 4.
	r := &fakeClusterRouter{clusterPatterns: []string{"news.*", "alerts.*"}}
	withRouter(t, r, func() {
		m := NewManager()
		c := shared.NewPubSubClient(nil, shared.RESP2)
		m.PSubscribe(c, "news.*")
		m.PSubscribe(c, "sports.*")

		got := m.NumPat()
		if got != 3 {
			t.Errorf("NumPat: got %d want 3", got)
		}
	})
}

func TestManager_Cleanup_AnnouncesUnsubForLastSubscriber(t *testing.T) {
	r := &fakeClusterRouter{}
	withRouter(t, r, func() {
		m := NewManager()
		c1 := shared.NewPubSubClient(nil, shared.RESP2)
		c2 := shared.NewPubSubClient(nil, shared.RESP2)
		m.Subscribe(c1, "news", "stale")
		m.Subscribe(c2, "stale")
		m.PSubscribe(c1, "alerts.*")

		// Cleaning c1 leaves c2 holding "stale" — only "news" and the
		// pattern should be announced as unsubscribed.
		r.mu.Lock()
		r.unsubAnnounces = nil
		r.mu.Unlock()

		m.Cleanup(c1)

		seen := map[string]bool{}
		r.mu.Lock()
		for _, a := range r.unsubAnnounces {
			key := a.channel
			if a.isPattern {
				key = "p:" + key
			}
			seen[key] = true
		}
		r.mu.Unlock()

		if !seen["news"] {
			t.Errorf("expected unsub for 'news'")
		}
		if !seen["p:alerts.*"] {
			t.Errorf("expected unsub for pattern 'alerts.*'")
		}
		if seen["stale"] {
			t.Errorf("'stale' has other subscriber, must not announce unsub")
		}
	})
}
