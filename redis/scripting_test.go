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

// execCmd is a helper that executes a command through h.ExecuteInto and returns the response string.
func execCmd(h *Handler, conn *shared.Connection, cmdType shared.CommandType, args ...string) string {
	var buf bytes.Buffer
	w := shared.NewWriter(&buf)
	w.SetProtocol(conn.Protocol)

	byteArgs := make([][]byte, len(args))
	for i, a := range args {
		byteArgs[i] = []byte(a)
	}

	cmd := &shared.Command{Type: cmdType, Args: byteArgs}
	h.ExecuteInto(cmd, w, conn)
	return buf.String()
}

func TestEvalBasic(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := func() *shared.Connection { c := newEffectsConn(h); c.Protocol = shared.RESP2; return c }()

	tests := []struct {
		name     string
		args     []string
		contains string
	}{
		{
			name:     "return integer",
			args:     []string{"return 42", "0"},
			contains: ":42\r\n",
		},
		{
			name:     "return string",
			args:     []string{"return 'hello'", "0"},
			contains: "$5\r\nhello\r\n",
		},
		{
			name:     "return array",
			args:     []string{"return {1, 2, 3}", "0"},
			contains: "*3\r\n",
		},
		{
			name:     "access KEYS",
			args:     []string{"return KEYS[1]", "1", "mykey"},
			contains: "$5\r\nmykey\r\n",
		},
		{
			name:     "access ARGV",
			args:     []string{"return ARGV[1]", "0", "myarg"},
			contains: "$5\r\nmyarg\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := execCmd(h, conn, shared.CmdEval, tt.args...)
			if !strings.Contains(result, tt.contains) {
				t.Errorf("expected response to contain %q, got %q", tt.contains, result)
			}
		})
	}
}

func TestEvalRedisCall(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := func() *shared.Connection { c := newEffectsConn(h); c.Protocol = shared.RESP2; return c }()

	t.Run("SET and GET via redis.call", func(t *testing.T) {
		script := `
			redis.call('SET', KEYS[1], ARGV[1])
			return redis.call('GET', KEYS[1])
		`
		result := execCmd(h, conn, shared.CmdEval, script, "1", "testkey", "testvalue")
		if !strings.Contains(result, "testvalue") {
			t.Errorf("expected response to contain 'testvalue', got %q", result)
		}
	})

	t.Run("BLPOP non-blocking in script", func(t *testing.T) {
		script := `return redis.call('BLPOP', 'key', 0)`
		result := execCmd(h, conn, shared.CmdEval, script, "0")
		if !strings.Contains(result, "$-1\r\n") {
			t.Errorf("expected nil for BLPOP with no data, got %q", result)
		}
	})
}

func TestEvalSha(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := func() *shared.Connection { c := newEffectsConn(h); c.Protocol = shared.RESP2; return c }()

	// First, load a script using SCRIPT LOAD
	result := execCmd(h, conn, shared.CmdScript, "LOAD", "return 123")
	// Extract SHA from bulk string response: $40\r\n<sha>\r\n
	parts := strings.Split(result, "\r\n")
	if len(parts) < 2 {
		t.Fatalf("unexpected SCRIPT LOAD response: %q", result)
	}
	sha := parts[1]

	// Now execute with EVALSHA
	result = execCmd(h, conn, shared.CmdEvalSha, sha, "0")
	if !strings.Contains(result, ":123\r\n") {
		t.Errorf("expected :123, got %q", result)
	}
}

func TestEvalShaNoscript(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := func() *shared.Connection { c := newEffectsConn(h); c.Protocol = shared.RESP2; return c }()

	result := execCmd(h, conn, shared.CmdEvalSha, "0000000000000000000000000000000000000000", "0")
	if !strings.Contains(result, "NOSCRIPT") {
		t.Errorf("expected NOSCRIPT error, got %q", result)
	}
}

func TestScriptLoad(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := func() *shared.Connection { c := newEffectsConn(h); c.Protocol = shared.RESP2; return c }()

	result := execCmd(h, conn, shared.CmdScript, "LOAD", "return 'loaded'")
	if !strings.HasPrefix(result, "$40\r\n") {
		t.Errorf("expected bulk string with SHA, got %q", result)
	}
}

func TestScriptExists(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := func() *shared.Connection { c := newEffectsConn(h); c.Protocol = shared.RESP2; return c }()

	// Load a script first
	loadResult := execCmd(h, conn, shared.CmdScript, "LOAD", "return 1")
	parts := strings.Split(loadResult, "\r\n")
	sha := parts[1]

	result := execCmd(h, conn, shared.CmdScript, "EXISTS", sha, "0000000000000000000000000000000000000000")
	if !strings.Contains(result, "*2\r\n") {
		t.Errorf("expected array of 2, got %q", result)
	}
	if !strings.Contains(result, ":1\r\n") {
		t.Errorf("expected :1 for existing script, got %q", result)
	}
	if !strings.Contains(result, ":0\r\n") {
		t.Errorf("expected :0 for non-existing script, got %q", result)
	}
}

func TestScriptFlush(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := func() *shared.Connection { c := newEffectsConn(h); c.Protocol = shared.RESP2; return c }()

	// Load a script first
	execCmd(h, conn, shared.CmdScript, "LOAD", "return 1")

	// Flush
	result := execCmd(h, conn, shared.CmdScript, "FLUSH")
	if !strings.Contains(result, "OK") {
		t.Errorf("expected OK, got %q", result)
	}
}

func TestScriptKillNoScript(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := func() *shared.Connection { c := newEffectsConn(h); c.Protocol = shared.RESP2; return c }()

	result := execCmd(h, conn, shared.CmdScript, "KILL")
	if !strings.Contains(result, "NOTBUSY") {
		t.Errorf("expected NOTBUSY error, got %q", result)
	}
}

func TestEvalErrorReply(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := func() *shared.Connection { c := newEffectsConn(h); c.Protocol = shared.RESP2; return c }()

	script := `return redis.error_reply("custom error")`
	result := execCmd(h, conn, shared.CmdEval, script, "0")
	if !strings.Contains(result, "-custom error\r\n") {
		t.Errorf("expected error reply, got %q", result)
	}
}

func TestEvalStatusReply(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := func() *shared.Connection { c := newEffectsConn(h); c.Protocol = shared.RESP2; return c }()

	script := `return redis.status_reply("custom status")`
	result := execCmd(h, conn, shared.CmdEval, script, "0")
	if !strings.Contains(result, "+custom status\r\n") {
		t.Errorf("expected status reply, got %q", result)
	}
}

func TestEvalPcall(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := func() *shared.Connection { c := newEffectsConn(h); c.Protocol = shared.RESP2; return c }()

	script := `
		local result = redis.pcall('GET')
		if type(result) == 'table' and result.err then
			return 'caught error: ' .. result.err
		end
		return 'no error'
	`
	result := execCmd(h, conn, shared.CmdEval, script, "0")
	if !strings.Contains(result, "caught error") {
		t.Errorf("expected pcall to catch error, got %q", result)
	}
}

func TestLuaToRESP(t *testing.T) {
	tests := []struct {
		name     string
		script   string
		contains string
	}{
		{"nil returns null", "return nil", "$-1\r\n"},
		{"false returns null", "return false", "$-1\r\n"},
		{"true returns 1", "return true", ":1\r\n"},
		{"integer", "return 42", ":42\r\n"},
		{"negative integer", "return -5", ":-5\r\n"},
		{"float truncated to int", "return 3.14", ":3\r\n"},
		{"empty string", "return ''", "$0\r\n\r\n"},
		{"empty table", "return {}", "*0\r\n"},
		{"nested table", "return {{1}}", "*1\r\n*1\r\n:1\r\n"},
	}

	h := newHandlerWithEffects()
	defer h.Close()

	conn := func() *shared.Connection { c := newEffectsConn(h); c.Protocol = shared.RESP2; return c }()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := execCmd(h, conn, shared.CmdEval, tt.script, "0")
			if !strings.Contains(result, tt.contains) {
				t.Errorf("expected %q, got %q", tt.contains, result)
			}
		})
	}
}

func TestEvalSyntaxError(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := func() *shared.Connection { c := newEffectsConn(h); c.Protocol = shared.RESP2; return c }()

	result := execCmd(h, conn, shared.CmdEval, "this is not valid lua", "0")
	if !strings.HasPrefix(result, "-ERR") {
		t.Errorf("expected error for syntax error, got %q", result)
	}
}
