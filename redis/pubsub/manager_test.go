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
	"context"
	"testing"
	"time"

	"github.com/swytchdb/engine/keytrie"
	"github.com/swytchdb/swytch/redis/shared"
)

func TestManager_SubscribeUnsubscribe(t *testing.T) {
	m := NewManager()
	conn := &shared.Connection{Protocol: shared.RESP2, Ctx: context.Background()}
	client := shared.NewPubSubClient(conn, shared.RESP2)

	// Subscribe to channels
	counts := m.Subscribe(client, "channel1", "channel2", "channel3")
	if len(counts) != 3 {
		t.Errorf("expected 3 counts, got %d", len(counts))
	}
	if counts[0] != 1 || counts[1] != 2 || counts[2] != 3 {
		t.Errorf("unexpected counts: %v", counts)
	}

	// Verify subscription count
	if count := m.SubscriptionCount(client); count != 3 {
		t.Errorf("expected 3 subscriptions, got %d", count)
	}

	// Unsubscribe from one channel
	names, unsubCounts := m.Unsubscribe(client, "channel2")
	if len(names) != 1 || names[0] != "channel2" {
		t.Errorf("unexpected unsubscribed channels: %v", names)
	}
	if unsubCounts[0] != 2 {
		t.Errorf("expected count 2 after unsubscribe, got %d", unsubCounts[0])
	}

	// Verify subscription count
	if count := m.SubscriptionCount(client); count != 2 {
		t.Errorf("expected 2 subscriptions, got %d", count)
	}

	// Unsubscribe from all
	names, unsubCounts = m.Unsubscribe(client)
	if len(names) != 2 {
		t.Errorf("expected 2 channels unsubscribed, got %d", len(names))
	}

	// Verify subscription count
	if count := m.SubscriptionCount(client); count != 0 {
		t.Errorf("expected 0 subscriptions, got %d", count)
	}
}

func TestManager_PSubscribePUnsubscribe(t *testing.T) {
	m := NewManager()
	conn := &shared.Connection{Protocol: shared.RESP2, Ctx: context.Background()}
	client := shared.NewPubSubClient(conn, shared.RESP2)

	// Subscribe to patterns
	counts := m.PSubscribe(client, "channel.*", "test.*")
	if len(counts) != 2 {
		t.Errorf("expected 2 counts, got %d", len(counts))
	}
	if counts[0] != 1 || counts[1] != 2 {
		t.Errorf("unexpected counts: %v", counts)
	}

	// Unsubscribe from one pattern
	names, unsubCounts := m.PUnsubscribe(client, "channel.*")
	if len(names) != 1 || names[0] != "channel.*" {
		t.Errorf("unexpected unsubscribed patterns: %v", names)
	}
	if unsubCounts[0] != 1 {
		t.Errorf("expected count 1 after punsubscribe, got %d", unsubCounts[0])
	}

	// Unsubscribe from all patterns
	names, _ = m.PUnsubscribe(client)
	if len(names) != 1 {
		t.Errorf("expected 1 pattern unsubscribed, got %d", len(names))
	}
}

func TestManager_Publish(t *testing.T) {
	m := NewManager()
	conn1 := &shared.Connection{Protocol: shared.RESP2, Ctx: context.Background()}
	client1 := shared.NewPubSubClient(conn1, shared.RESP2)
	conn2 := &shared.Connection{Protocol: shared.RESP2, Ctx: context.Background()}
	client2 := shared.NewPubSubClient(conn2, shared.RESP2)

	// Subscribe clients to channels
	m.Subscribe(client1, "news", "sports")
	m.Subscribe(client2, "news")

	// Publish to a channel
	count := m.Publish("news", []byte("hello world"))
	if count != 2 {
		t.Errorf("expected 2 receivers, got %d", count)
	}

	// Check that messages were delivered
	select {
	case msg := <-client1.MsgChan():
		if msg.Type != "message" || msg.Channel != "news" || string(msg.Payload) != "hello world" {
			t.Errorf("unexpected message: %+v", msg)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for message to client1")
	}

	select {
	case msg := <-client2.MsgChan():
		if msg.Type != "message" || msg.Channel != "news" || string(msg.Payload) != "hello world" {
			t.Errorf("unexpected message: %+v", msg)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for message to client2")
	}

	// Publish to a channel with no subscribers
	count = m.Publish("weather", []byte("sunny"))
	if count != 0 {
		t.Errorf("expected 0 receivers, got %d", count)
	}
}

func TestManager_PatternPublish(t *testing.T) {
	m := NewManager()
	conn := &shared.Connection{Protocol: shared.RESP2, Ctx: context.Background()}
	client := shared.NewPubSubClient(conn, shared.RESP2)

	// Subscribe to pattern
	m.PSubscribe(client, "news.*")

	// Publish to matching channel
	count := m.Publish("news.sports", []byte("goal"))
	if count != 1 {
		t.Errorf("expected 1 receiver, got %d", count)
	}

	// Check message
	select {
	case msg := <-client.MsgChan():
		if msg.Type != "pmessage" {
			t.Errorf("expected pmessage, got %s", msg.Type)
		}
		if msg.Pattern != "news.*" {
			t.Errorf("expected pattern 'news.*', got %s", msg.Pattern)
		}
		if msg.Channel != "news.sports" {
			t.Errorf("expected channel 'news.sports', got %s", msg.Channel)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for pattern message")
	}

	// Publish to non-matching channel
	count = m.Publish("weather.rain", []byte("wet"))
	if count != 0 {
		t.Errorf("expected 0 receivers for non-matching pattern, got %d", count)
	}
}

func TestManager_Cleanup(t *testing.T) {
	m := NewManager()
	conn := &shared.Connection{Protocol: shared.RESP2, Ctx: context.Background()}
	client := shared.NewPubSubClient(conn, shared.RESP2)

	// Subscribe to channels and patterns
	m.Subscribe(client, "channel1", "channel2")
	m.PSubscribe(client, "pattern.*")

	// Verify subscriptions exist
	if len(m.Channels("")) != 2 {
		t.Errorf("expected 2 active channels")
	}
	if m.NumPat() != 1 {
		t.Errorf("expected 1 pattern subscription")
	}

	// Cleanup
	m.Cleanup(client)

	// Verify all subscriptions removed
	if len(m.Channels("")) != 0 {
		t.Errorf("expected 0 active channels after cleanup")
	}
	if m.NumPat() != 0 {
		t.Errorf("expected 0 pattern subscriptions after cleanup")
	}
}

func TestManager_Channels(t *testing.T) {
	m := NewManager()
	conn := &shared.Connection{Protocol: shared.RESP2, Ctx: context.Background()}
	client := shared.NewPubSubClient(conn, shared.RESP2)

	m.Subscribe(client, "news.sports", "news.weather", "tech.go")

	// Get all channels
	channels := m.Channels("")
	if len(channels) != 3 {
		t.Errorf("expected 3 channels, got %d", len(channels))
	}

	// Get channels matching pattern
	channels = m.Channels("news.*")
	if len(channels) != 2 {
		t.Errorf("expected 2 channels matching 'news.*', got %d", len(channels))
	}
}

func TestManager_NumSub(t *testing.T) {
	m := NewManager()
	conn1 := &shared.Connection{Protocol: shared.RESP2, Ctx: context.Background()}
	client1 := shared.NewPubSubClient(conn1, shared.RESP2)
	conn2 := &shared.Connection{Protocol: shared.RESP2, Ctx: context.Background()}
	client2 := shared.NewPubSubClient(conn2, shared.RESP2)

	m.Subscribe(client1, "channel1", "channel2")
	m.Subscribe(client2, "channel1")

	counts := m.NumSub("channel1", "channel2", "channel3")
	if counts["channel1"] != 2 {
		t.Errorf("expected 2 subscribers for channel1, got %d", counts["channel1"])
	}
	if counts["channel2"] != 1 {
		t.Errorf("expected 1 subscriber for channel2, got %d", counts["channel2"])
	}
	if counts["channel3"] != 0 {
		t.Errorf("expected 0 subscribers for channel3, got %d", counts["channel3"])
	}
}

func TestManager_NumPat(t *testing.T) {
	m := NewManager()
	conn1 := &shared.Connection{Protocol: shared.RESP2, Ctx: context.Background()}
	client1 := shared.NewPubSubClient(conn1, shared.RESP2)
	conn2 := &shared.Connection{Protocol: shared.RESP2, Ctx: context.Background()}
	client2 := shared.NewPubSubClient(conn2, shared.RESP2)

	m.PSubscribe(client1, "pattern.*")
	m.PSubscribe(client2, "pattern.*", "other.*")

	// NumPat returns unique patterns, not total subscriptions
	count := m.NumPat()
	if count != 2 {
		t.Errorf("expected 2 unique patterns, got %d", count)
	}
}

func TestPubSubClient_Close(t *testing.T) {
	conn := &shared.Connection{Protocol: shared.RESP2, Ctx: context.Background()}
	client := shared.NewPubSubClient(conn, shared.RESP2)

	// Send a message before closing
	client.Send(&shared.PubSubMessage{Type: "message", Channel: "test", Payload: []byte("hello")})

	// Close the client
	client.Close()

	// Verify done channel is closed
	select {
	case <-client.Done():
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Error("done channel should be closed")
	}

	// Verify sending fails after close
	if client.Send(&shared.PubSubMessage{Type: "message", Channel: "test", Payload: []byte("hello")}) {
		t.Error("send should fail after close")
	}
}

func TestFormatPubSubMessage_RESP2(t *testing.T) {
	msg := &shared.PubSubMessage{
		Type:    "message",
		Channel: "test",
		Payload: []byte("hello"),
	}

	data := FormatPubSubMessage(msg, shared.RESP2)
	expected := "*3\r\n$7\r\nmessage\r\n$4\r\ntest\r\n$5\r\nhello\r\n"
	if !bytes.Equal(data, []byte(expected)) {
		t.Errorf("unexpected RESP2 format:\ngot: %q\nwant: %q", string(data), expected)
	}
}

func TestFormatPubSubMessage_RESP3(t *testing.T) {
	msg := &shared.PubSubMessage{
		Type:    "message",
		Channel: "test",
		Payload: []byte("hello"),
	}

	data := FormatPubSubMessage(msg, shared.RESP3)
	expected := ">3\r\n$7\r\nmessage\r\n$4\r\ntest\r\n$5\r\nhello\r\n"
	if !bytes.Equal(data, []byte(expected)) {
		t.Errorf("unexpected RESP3 format:\ngot: %q\nwant: %q", string(data), expected)
	}
}

func TestFormatPubSubMessage_PMessage(t *testing.T) {
	msg := &shared.PubSubMessage{
		Type:    "pmessage",
		Pattern: "news.*",
		Channel: "news.sports",
		Payload: []byte("goal"),
	}

	data := FormatPubSubMessage(msg, shared.RESP2)
	expected := "*4\r\n$8\r\npmessage\r\n$6\r\nnews.*\r\n$11\r\nnews.sports\r\n$4\r\ngoal\r\n"
	if !bytes.Equal(data, []byte(expected)) {
		t.Errorf("unexpected pmessage format:\ngot: %q\nwant: %q", string(data), expected)
	}
}

func TestFormatPubSubMessage_Subscribe(t *testing.T) {
	msg := &shared.PubSubMessage{
		Type:    "subscribe",
		Channel: "test",
		Count:   1,
	}

	data := FormatPubSubMessage(msg, shared.RESP2)
	expected := "*3\r\n$9\r\nsubscribe\r\n$4\r\ntest\r\n:1\r\n"
	if !bytes.Equal(data, []byte(expected)) {
		t.Errorf("unexpected subscribe format:\ngot: %q\nwant: %q", string(data), expected)
	}
}

func TestGlobPattern(t *testing.T) {
	tests := []struct {
		pattern string
		str     string
		match   bool
	}{
		{"*", "anything", true},
		{"news.*", "news.sports", true},
		{"news.*", "weather.sports", false},
		{"news.?", "news.a", true},
		{"news.?", "news.ab", false},
		{"[abc]", "a", true},
		{"[abc]", "d", false},
		{"news.[0-9]", "news.5", true},
		{"news.[0-9]", "news.a", false},
	}

	for _, tt := range tests {
		result := keytrie.MatchGlob(tt.str, tt.pattern)
		if result != tt.match {
			t.Errorf("MatchGlob(%q, %q) = %v, want %v", tt.str, tt.pattern, result, tt.match)
		}
	}
}
