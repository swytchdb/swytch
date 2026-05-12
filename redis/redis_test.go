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
	"strings"
	"testing"

	"github.com/swytchdb/swytch/redis/shared"
)

func TestProtocolParser(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantType shared.CommandType
		wantArgs int
	}{
		{
			name:     "PING",
			input:    "*1\r\n$4\r\nPING\r\n",
			wantType: shared.CmdPing,
			wantArgs: 0,
		},
		{
			name:     "SET",
			input:    "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
			wantType: shared.CmdSet,
			wantArgs: 2,
		},
		{
			name:     "GET",
			input:    "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
			wantType: shared.CmdGet,
			wantArgs: 1,
		},
		{
			name:     "inline PING",
			input:    "PING\r\n",
			wantType: shared.CmdPing,
			wantArgs: 0,
		},
		{
			name:     "inline SET",
			input:    "SET foo bar\r\n",
			wantType: shared.CmdSet,
			wantArgs: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := shared.NewParser(strings.NewReader(tt.input))
			cmd, err := parser.ReadCommand()
			if err != nil {
				t.Fatalf("ReadCommand() error = %v", err)
			}

			if cmd.Type != tt.wantType {
				t.Errorf("Type = %v, want %v", cmd.Type, tt.wantType)
			}

			if len(cmd.Args) != tt.wantArgs {
				t.Errorf("Args count = %v, want %v", len(cmd.Args), tt.wantArgs)
			}
		})
	}
}

func TestWriter(t *testing.T) {
	tests := []struct {
		name   string
		fn     func(w *shared.Writer)
		expect string
	}{
		{
			name:   "OK",
			fn:     func(w *shared.Writer) { w.WriteOK() },
			expect: "+OK\r\n",
		},
		{
			name:   "PONG",
			fn:     func(w *shared.Writer) { w.WritePong() },
			expect: "+PONG\r\n",
		},
		{
			name:   "Integer",
			fn:     func(w *shared.Writer) { w.WriteInteger(42) },
			expect: ":42\r\n",
		},
		{
			name:   "BulkString",
			fn:     func(w *shared.Writer) { w.WriteBulkString([]byte("hello")) },
			expect: "$5\r\nhello\r\n",
		},
		{
			name:   "NullBulkString",
			fn:     func(w *shared.Writer) { w.WriteNullBulkString() },
			expect: "$-1\r\n",
		},
		{
			name:   "Error",
			fn:     func(w *shared.Writer) { w.WriteError("ERR test error") },
			expect: "-ERR test error\r\n",
		},
		{
			name: "Array",
			fn: func(w *shared.Writer) {
				w.WriteArray(2)
				w.WriteBulkString([]byte("foo"))
				w.WriteBulkString([]byte("bar"))
			},
			expect: "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			w := shared.NewWriter(buf)
			tt.fn(w)

			if got := buf.String(); got != tt.expect {
				t.Errorf("got %q, want %q", got, tt.expect)
			}
		})
	}
}

func TestHandler(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	tests := []struct {
		name       string
		cmd        *shared.Command
		wantPrefix string
	}{
		{
			name:       "PING",
			cmd:        &shared.Command{Type: shared.CmdPing},
			wantPrefix: "+PONG\r\n",
		},
		{
			name:       "SET",
			cmd:        &shared.Command{Type: shared.CmdSet, Args: [][]byte{[]byte("mykey"), []byte("myvalue")}},
			wantPrefix: "+OK\r\n",
		},
		{
			name:       "GET existing",
			cmd:        &shared.Command{Type: shared.CmdGet, Args: [][]byte{[]byte("mykey")}},
			wantPrefix: "$7\r\nmyvalue\r\n",
		},
		{
			name:       "GET non-existing",
			cmd:        &shared.Command{Type: shared.CmdGet, Args: [][]byte{[]byte("nonexistent")}},
			wantPrefix: "$-1\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			w := shared.NewWriter(buf)
			h.ExecuteInto(tt.cmd, w, conn)

			if got := buf.String(); got != tt.wantPrefix {
				t.Errorf("got %q, want %q", got, tt.wantPrefix)
			}
		})
	}
}

func TestListCommands(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// RPUSH
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("mylist"), []byte("a"), []byte("b"), []byte("c")}}
	h.ExecuteInto(cmd, w, conn)

	if got := buf.String(); got != ":3\r\n" {
		t.Errorf("RPUSH got %q, want :3\\r\\n", got)
	}

	// LLEN
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdLLen, Args: [][]byte{[]byte("mylist")}}
	h.ExecuteInto(cmd, w, conn)

	if got := buf.String(); got != ":3\r\n" {
		t.Errorf("LLEN got %q, want :3\\r\\n", got)
	}

	// LRANGE
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdLRange, Args: [][]byte{[]byte("mylist"), []byte("0"), []byte("-1")}}
	h.ExecuteInto(cmd, w, conn)

	expected := "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("LRANGE got %q, want %q", got, expected)
	}
}

func TestHashCommands(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// HSET
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdHSet, Args: [][]byte{[]byte("myhash"), []byte("field1"), []byte("value1")}}
	h.ExecuteInto(cmd, w, conn)

	if got := buf.String(); got != ":1\r\n" {
		t.Errorf("HSET got %q, want :1\\r\\n", got)
	}

	// HGET
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdHGet, Args: [][]byte{[]byte("myhash"), []byte("field1")}}
	h.ExecuteInto(cmd, w, conn)

	expected := "$6\r\nvalue1\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("HGET got %q, want %q", got, expected)
	}

	// HLEN
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdHLen, Args: [][]byte{[]byte("myhash")}}
	h.ExecuteInto(cmd, w, conn)

	if got := buf.String(); got != ":1\r\n" {
		t.Errorf("HLEN got %q, want :1\\r\\n", got)
	}
}

func TestExpiration(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// SET with EX
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdSet, Args: [][]byte{[]byte("expkey"), []byte("expvalue"), []byte("EX"), []byte("3600")}}
	h.ExecuteInto(cmd, w, conn)

	if got := buf.String(); got != "+OK\r\n" {
		t.Errorf("SET EX got %q, want +OK\\r\\n", got)
	}

	// GET should return the value
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdGet, Args: [][]byte{[]byte("expkey")}}
	h.ExecuteInto(cmd, w, conn)
	if got := buf.String(); got != "$8\r\nexpvalue\r\n" {
		t.Errorf("GET expkey got %q, want expvalue", got)
	}

	// TTL should be positive
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdTTL, Args: [][]byte{[]byte("expkey")}}
	h.ExecuteInto(cmd, w, conn)
	if got := buf.String(); got == ":-1\r\n" || got == ":-2\r\n" {
		t.Errorf("expected positive TTL, got %q", got)
	}
}

func TestTransactionMultiExec(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)

	// MULTI
	cmd := &shared.Command{Type: shared.CmdMulti}
	h.ExecuteInto(cmd, w, conn)

	if got := buf.String(); got != "+OK\r\n" {
		t.Errorf("MULTI got %q, want +OK\\r\\n", got)
	}
	if !conn.InTransaction {
		t.Error("expected InTransaction to be true after MULTI")
	}

	// SET (should be queued)
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdSet, Args: [][]byte{[]byte("txnkey"), []byte("txnvalue")}}
	h.ExecuteInto(cmd, w, conn)

	if got := buf.String(); got != "+QUEUED\r\n" {
		t.Errorf("SET during MULTI got %q, want +QUEUED\\r\\n", got)
	}
	if len(conn.QueuedCmds) != 1 {
		t.Errorf("expected 1 queued command, got %d", len(conn.QueuedCmds))
	}

	// GET (should be queued)
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdGet, Args: [][]byte{[]byte("txnkey")}}
	h.ExecuteInto(cmd, w, conn)

	if got := buf.String(); got != "+QUEUED\r\n" {
		t.Errorf("GET during MULTI got %q, want +QUEUED\\r\\n", got)
	}
	if len(conn.QueuedCmds) != 2 {
		t.Errorf("expected 2 queued commands, got %d", len(conn.QueuedCmds))
	}

	// EXEC
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdExec}
	h.ExecuteInto(cmd, w, conn)

	// Should return array with 2 results: +OK and the value
	expected := "*2\r\n+OK\r\n$8\r\ntxnvalue\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("EXEC got %q, want %q", got, expected)
	}
	if conn.InTransaction {
		t.Error("expected InTransaction to be false after EXEC")
	}
	if len(conn.QueuedCmds) != 0 {
		t.Errorf("expected 0 queued commands after EXEC, got %d", len(conn.QueuedCmds))
	}
}

func TestTransactionDiscard(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)

	// MULTI
	cmd := &shared.Command{Type: shared.CmdMulti}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// SET (queued)
	cmd = &shared.Command{Type: shared.CmdSet, Args: [][]byte{[]byte("discardkey"), []byte("value")}}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// DISCARD
	cmd = &shared.Command{Type: shared.CmdDiscard}
	h.ExecuteInto(cmd, w, conn)

	if got := buf.String(); got != "+OK\r\n" {
		t.Errorf("DISCARD got %q, want +OK\\r\\n", got)
	}
	if conn.InTransaction {
		t.Error("expected InTransaction to be false after DISCARD")
	}
	if len(conn.QueuedCmds) != 0 {
		t.Errorf("expected 0 queued commands after DISCARD, got %d", len(conn.QueuedCmds))
	}

	// Verify key was not set (GET should return nil)
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdGet, Args: [][]byte{[]byte("discardkey")}}
	h.ExecuteInto(cmd, w, conn)
	if got := buf.String(); got != "$-1\r\n" {
		t.Errorf("discardkey should not exist after DISCARD, GET returned %q", got)
	}
}

func TestTransactionNestedMultiError(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)

	// First MULTI
	cmd := &shared.Command{Type: shared.CmdMulti}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// Second MULTI (should error)
	h.ExecuteInto(cmd, w, conn)

	if !strings.Contains(buf.String(), "ERR MULTI calls can not be nested") {
		t.Errorf("nested MULTI got %q, expected error about nesting", buf.String())
	}
}

func TestTransactionExecWithoutMulti(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)

	// EXEC without MULTI
	cmd := &shared.Command{Type: shared.CmdExec}
	h.ExecuteInto(cmd, w, conn)

	if !strings.Contains(buf.String(), "ERR EXEC without MULTI") {
		t.Errorf("EXEC without MULTI got %q, expected error", buf.String())
	}
}

func TestTransactionDiscardWithoutMulti(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)

	// DISCARD without MULTI
	cmd := &shared.Command{Type: shared.CmdDiscard}
	h.ExecuteInto(cmd, w, conn)

	if !strings.Contains(buf.String(), "ERR DISCARD without MULTI") {
		t.Errorf("DISCARD without MULTI got %q, expected error", buf.String())
	}
}

func TestTransactionWatch(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)

	// Set initial value
	cmd := &shared.Command{Type: shared.CmdSet, Args: [][]byte{[]byte("watchkey"), []byte("initial")}}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// WATCH
	cmd = &shared.Command{Type: shared.CmdWatch, Args: [][]byte{[]byte("watchkey")}}
	h.ExecuteInto(cmd, w, conn)

	if got := buf.String(); got != "+OK\r\n" {
		t.Errorf("WATCH got %q, want +OK\\r\\n", got)
	}

	// MULTI
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdMulti}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// Queue a SET
	cmd = &shared.Command{Type: shared.CmdSet, Args: [][]byte{[]byte("watchkey"), []byte("newvalue")}}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// Simulate another client modifying the key through a second connection
	conn2 := newEffectsConn(h)
	buf2 := &bytes.Buffer{}
	w2 := shared.NewWriter(buf2)
	cmd2 := &shared.Command{Type: shared.CmdSet, Args: [][]byte{[]byte("watchkey"), []byte("modified")}}
	h.ExecuteInto(cmd2, w2, conn2)

	// EXEC should fail (return nil array)
	cmd = &shared.Command{Type: shared.CmdExec}
	h.ExecuteInto(cmd, w, conn)

	if got := buf.String(); got != "*-1\r\n" {
		t.Errorf("EXEC with modified watch got %q, want *-1\\r\\n (null array)", got)
	}

	// Transaction state should be cleared
	if conn.InTransaction {
		t.Error("expected InTransaction to be false after failed EXEC")
	}
	if len(conn.WatchedKeys) != 0 {
		t.Errorf("expected 0 watched keys after EXEC, got %d", len(conn.WatchedKeys))
	}
}

func TestTransactionWatchKeyCreated(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)

	// WATCH non-existent key
	cmd := &shared.Command{Type: shared.CmdWatch, Args: [][]byte{[]byte("newkey")}}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// MULTI
	cmd = &shared.Command{Type: shared.CmdMulti}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// Queue a SET
	cmd = &shared.Command{Type: shared.CmdSet, Args: [][]byte{[]byte("newkey"), []byte("value")}}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// Another client creates the key through effects
	conn2 := newEffectsConn(h)
	buf2 := &bytes.Buffer{}
	w2 := shared.NewWriter(buf2)
	h.ExecuteInto(&shared.Command{Type: shared.CmdSet, Args: [][]byte{[]byte("newkey"), []byte("created")}}, w2, conn2)

	// EXEC should fail
	cmd = &shared.Command{Type: shared.CmdExec}
	h.ExecuteInto(cmd, w, conn)

	if got := buf.String(); got != "*-1\r\n" {
		t.Errorf("EXEC with created watch key got %q, want *-1\\r\\n", got)
	}
}

func TestTransactionWatchKeyDeleted(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)

	// Create initial key through effects
	cmd := &shared.Command{Type: shared.CmdSet, Args: [][]byte{[]byte("delkey"), []byte("value")}}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// WATCH
	cmd = &shared.Command{Type: shared.CmdWatch, Args: [][]byte{[]byte("delkey")}}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// MULTI
	cmd = &shared.Command{Type: shared.CmdMulti}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// Queue a GET
	cmd = &shared.Command{Type: shared.CmdGet, Args: [][]byte{[]byte("delkey")}}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// Another client deletes the key through effects
	conn2 := newEffectsConn(h)
	buf2 := &bytes.Buffer{}
	w2 := shared.NewWriter(buf2)
	h.ExecuteInto(&shared.Command{Type: shared.CmdDel, Args: [][]byte{[]byte("delkey")}}, w2, conn2)

	// EXEC should fail
	cmd = &shared.Command{Type: shared.CmdExec}
	h.ExecuteInto(cmd, w, conn)

	if got := buf.String(); got != "*-1\r\n" {
		t.Errorf("EXEC with deleted watch key got %q, want *-1\\r\\n", got)
	}
}

func TestTransactionUnwatch(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)

	// Create initial key through effects
	cmd := &shared.Command{Type: shared.CmdSet, Args: [][]byte{[]byte("unwatchkey"), []byte("value")}}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// WATCH
	cmd = &shared.Command{Type: shared.CmdWatch, Args: [][]byte{[]byte("unwatchkey")}}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// UNWATCH
	cmd = &shared.Command{Type: shared.CmdUnwatch}
	h.ExecuteInto(cmd, w, conn)

	if got := buf.String(); got != "+OK\r\n" {
		t.Errorf("UNWATCH got %q, want +OK\\r\\n", got)
	}
	buf.Reset()

	// MULTI
	cmd = &shared.Command{Type: shared.CmdMulti}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// Queue a SET
	cmd = &shared.Command{Type: shared.CmdSet, Args: [][]byte{[]byte("unwatchkey"), []byte("newvalue")}}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// Modify key through another connection (but we unwatched, so it shouldn't matter)
	conn2 := newEffectsConn(h)
	buf2 := &bytes.Buffer{}
	w2 := shared.NewWriter(buf2)
	h.ExecuteInto(&shared.Command{Type: shared.CmdSet, Args: [][]byte{[]byte("unwatchkey"), []byte("modified")}}, w2, conn2)

	// EXEC should succeed
	cmd = &shared.Command{Type: shared.CmdExec}
	h.ExecuteInto(cmd, w, conn)

	if got := buf.String(); got != "*1\r\n+OK\r\n" {
		t.Errorf("EXEC after UNWATCH got %q, want *1\\r\\n+OK\\r\\n", got)
	}
}

func TestTransactionWatchInsideMulti(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)

	// MULTI
	cmd := &shared.Command{Type: shared.CmdMulti}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// WATCH inside MULTI (should error)
	cmd = &shared.Command{Type: shared.CmdWatch, Args: [][]byte{[]byte("key")}}
	h.ExecuteInto(cmd, w, conn)

	if !strings.Contains(buf.String(), "ERR WATCH inside MULTI is not allowed") {
		t.Errorf("WATCH inside MULTI got %q, expected error", buf.String())
	}
}

func TestTransactionSuccessfulExec(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)

	// Set initial value through effects
	cmd := &shared.Command{Type: shared.CmdSet, Args: [][]byte{[]byte("watchok"), []byte("initial")}}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// WATCH
	cmd = &shared.Command{Type: shared.CmdWatch, Args: [][]byte{[]byte("watchok")}}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// MULTI
	cmd = &shared.Command{Type: shared.CmdMulti}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// Queue multiple commands
	cmd = &shared.Command{Type: shared.CmdSet, Args: [][]byte{[]byte("watchok"), []byte("updated")}}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	cmd = &shared.Command{Type: shared.CmdGet, Args: [][]byte{[]byte("watchok")}}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	cmd = &shared.Command{Type: shared.CmdIncr, Args: [][]byte{[]byte("counter")}}
	h.ExecuteInto(cmd, w, conn)
	buf.Reset()

	// EXEC (no modification to watched key, should succeed)
	cmd = &shared.Command{Type: shared.CmdExec}
	h.ExecuteInto(cmd, w, conn)

	// Check results: OK, "updated", :1
	expected := "*3\r\n+OK\r\n$7\r\nupdated\r\n:1\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("EXEC got %q, want %q", got, expected)
	}

	// Verify final value through GET
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdGet, Args: [][]byte{[]byte("watchok")}}
	h.ExecuteInto(cmd, w, conn)
	if got := buf.String(); got != "$7\r\nupdated\r\n" {
		t.Errorf("expected watchok to be 'updated', got %q", got)
	}
}

func TestParseTLSVersion(t *testing.T) {
	tests := []struct {
		version   string
		expected  uint16
		expectErr bool
	}{
		{"1.0", 0, true},       // tls.VersionTLS10 removed
		{"1.1", 0, true},       // tls.VersionTLS11 removed
		{"1.2", 0x0303, false}, // tls.VersionTLS12
		{"1.3", 0x0304, false}, // tls.VersionTLS13
		{"", 0x0303, false},    // default to 1.2
		{"invalid", 0, true},
		{"2.0", 0, true},
	}

	for _, tt := range tests {
		t.Run("version_"+tt.version, func(t *testing.T) {
			result, err := parseTLSVersion(tt.version)
			if tt.expectErr {
				if err == nil {
					t.Errorf("expected error for version %q, got nil", tt.version)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for version %q: %v", tt.version, err)
				}
				if result != tt.expected {
					t.Errorf("parseTLSVersion(%q) = %v, want %v", tt.version, result, tt.expected)
				}
			}
		})
	}
}

func TestTLSEnabled(t *testing.T) {
	tests := []struct {
		name     string
		config   ServerConfig
		expected bool
	}{
		{
			name:     "disabled when empty",
			config:   ServerConfig{},
			expected: false,
		},
		{
			name:     "enabled when cert file set",
			config:   ServerConfig{TLSCertFile: "/path/to/cert.pem"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.config.TLSEnabled(); got != tt.expected {
				t.Errorf("TLSEnabled() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestBuildTLSConfig_NoCert(t *testing.T) {
	config := ServerConfig{}
	tlsConfig, err := config.buildTLSConfig()
	if err != nil {
		t.Errorf("buildTLSConfig() error = %v, want nil", err)
	}
	if tlsConfig != nil {
		t.Errorf("buildTLSConfig() = %v, want nil when TLS disabled", tlsConfig)
	}
}

func TestBuildTLSConfig_MissingFiles(t *testing.T) {
	config := ServerConfig{
		TLSCertFile: "/nonexistent/cert.pem",
		TLSKeyFile:  "/nonexistent/key.pem",
	}
	_, err := config.buildTLSConfig()
	if err == nil {
		t.Error("buildTLSConfig() expected error for missing files, got nil")
	}
}

func TestBuildTLSConfig_InvalidVersion(t *testing.T) {
	// Even with invalid version, this should fail (file doesn't exist first)
	config := ServerConfig{
		TLSCertFile:   "/nonexistent/cert.pem",
		TLSKeyFile:    "/nonexistent/key.pem",
		TLSMinVersion: "invalid",
	}
	_, err := config.buildTLSConfig()
	if err == nil {
		t.Error("buildTLSConfig() expected error, got nil")
	}
}
