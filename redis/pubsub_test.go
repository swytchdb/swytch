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

package redis

import (
	"bytes"
	"context"
	"testing"

	"github.com/swytchdb/swytch/redis/shared"
)

func TestHandler_PubSubCommands(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := &shared.Connection{
		SelectedDB: 0,
		User:       testUser,
		Protocol:   shared.RESP2,
		Ctx:        context.Background(),
	}

	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)

	// Test PUBLISH to non-existent channel (no subscribers)
	cmd := &shared.Command{Type: shared.CmdPublish, Args: [][]byte{[]byte("test"), []byte("hello")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte(":0\r\n")) {
		t.Errorf("expected 0 subscribers, got: %s", buf.String())
	}
	buf.Reset()

	// Test SUBSCRIBE
	cmd = &shared.Command{Type: shared.CmdSubscribe, Args: [][]byte{[]byte("channel1"), []byte("channel2")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte("subscribe")) {
		t.Errorf("expected subscribe confirmation, got: %s", buf.String())
	}
	if conn.PubSubClient == nil {
		t.Error("expected PubSubClient to be set")
	}
	buf.Reset()

	// Test that we're in pubsub mode (other commands should be rejected)
	cmd = &shared.Command{Type: shared.CmdGet, Args: [][]byte{[]byte("key")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte("ERR only")) {
		t.Errorf("expected pubsub mode error, got: %s", buf.String())
	}
	buf.Reset()

	// Test UNSUBSCRIBE
	cmd = &shared.Command{Type: shared.CmdUnsubscribe, Args: [][]byte{[]byte("channel1")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte("unsubscribe")) {
		t.Errorf("expected unsubscribe confirmation, got: %s", buf.String())
	}
	buf.Reset()

	// Unsubscribe from all to exit pubsub mode
	cmd = &shared.Command{Type: shared.CmdUnsubscribe}
	h.ExecuteInto(cmd, w, conn)
	if conn.PubSubClient != nil {
		t.Error("expected PubSubClient to be nil after unsubscribing from all")
	}
	buf.Reset()

	// Test PUBSUB CHANNELS
	// First subscribe again
	conn2 := &shared.Connection{SelectedDB: 0, User: testUser, Protocol: shared.RESP2, Ctx: context.Background()}
	cmd = &shared.Command{Type: shared.CmdSubscribe, Args: [][]byte{[]byte("news.sports"), []byte("news.weather")}}
	h.ExecuteInto(cmd, w, conn2)
	buf.Reset()

	// Now test PUBSUB CHANNELS
	cmd = &shared.Command{Type: shared.CmdPubSub, Args: [][]byte{[]byte("CHANNELS")}}
	h.ExecuteInto(cmd, w, conn)
	response := buf.String()
	if !bytes.Contains(buf.Bytes(), []byte("news.sports")) || !bytes.Contains(buf.Bytes(), []byte("news.weather")) {
		t.Errorf("expected channels in response, got: %s", response)
	}
	buf.Reset()

	// Test PUBSUB CHANNELS with pattern
	cmd = &shared.Command{Type: shared.CmdPubSub, Args: [][]byte{[]byte("CHANNELS"), []byte("news.*")}}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// Test PUBSUB NUMSUB
	cmd = &shared.Command{Type: shared.CmdPubSub, Args: [][]byte{[]byte("NUMSUB"), []byte("news.sports"), []byte("nonexistent")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte("news.sports")) {
		t.Errorf("expected channel in numsub response, got: %s", buf.String())
	}
	buf.Reset()

	// Test PUBSUB NUMPAT
	cmd = &shared.Command{Type: shared.CmdPubSub, Args: [][]byte{[]byte("NUMPAT")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte(":0\r\n")) {
		t.Errorf("expected 0 pattern subscriptions, got: %s", buf.String())
	}
	buf.Reset()

	// Cleanup
	shared.GetPubSubBroker().Cleanup(conn2.PubSubClient)
}

func TestHandler_PSubscribe(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := &shared.Connection{
		SelectedDB: 0,
		User:       testUser,
		Protocol:   shared.RESP2,
		Ctx:        context.Background(),
	}

	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)

	// Test PSUBSCRIBE
	cmd := &shared.Command{Type: shared.CmdPSubscribe, Args: [][]byte{[]byte("news.*"), []byte("sports.*")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte("psubscribe")) {
		t.Errorf("expected psubscribe confirmation, got: %s", buf.String())
	}
	buf.Reset()

	// Test PUBSUB NUMPAT
	cmd = &shared.Command{Type: shared.CmdPubSub, Args: [][]byte{[]byte("NUMPAT")}}
	connNonPubSub := &shared.Connection{SelectedDB: 0, User: testUser, Protocol: shared.RESP2, Ctx: context.Background()}
	h.ExecuteInto(cmd, w, connNonPubSub)
	if !bytes.Contains(buf.Bytes(), []byte(":2\r\n")) {
		t.Errorf("expected 2 pattern subscriptions, got: %s", buf.String())
	}
	buf.Reset()

	// Test PUNSUBSCRIBE
	cmd = &shared.Command{Type: shared.CmdPUnsubscribe, Args: [][]byte{[]byte("news.*")}}
	h.ExecuteInto(cmd, w, conn)
	if !bytes.Contains(buf.Bytes(), []byte("punsubscribe")) {
		t.Errorf("expected punsubscribe confirmation, got: %s", buf.String())
	}
	buf.Reset()

	// Cleanup
	shared.GetPubSubBroker().Cleanup(conn.PubSubClient)
}
