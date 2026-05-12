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
	"testing"

	"github.com/swytchdb/swytch/redis/shared"
)

// TestInMemoryHandler_DeleteAfterIncr tests that DEL works correctly on keys
// created by INCR operations in in-memory mode.
func TestInMemoryHandler_DeleteAfterIncr(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// INCR creates a new counter
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	h.ExecuteInto(&shared.Command{Type: shared.CmdIncr, Args: [][]byte{[]byte("counter")}}, w, conn)
	if resp := buf.String(); resp != ":1\r\n" {
		t.Errorf("INCR: expected :1, got %q", resp)
	}

	// INCR again
	buf.Reset()
	h.ExecuteInto(&shared.Command{Type: shared.CmdIncr, Args: [][]byte{[]byte("counter")}}, w, conn)
	if resp := buf.String(); resp != ":2\r\n" {
		t.Errorf("INCR: expected :2, got %q", resp)
	}

	// DEL the counter
	buf.Reset()
	h.ExecuteInto(&shared.Command{Type: shared.CmdDel, Args: [][]byte{[]byte("counter")}}, w, conn)
	if resp := buf.String(); resp != ":1\r\n" {
		t.Errorf("DEL after INCR: expected :1, got %q", resp)
	}

	// EXISTS should return 0
	buf.Reset()
	h.ExecuteInto(&shared.Command{Type: shared.CmdExists, Args: [][]byte{[]byte("counter")}}, w, conn)
	if resp := buf.String(); resp != ":0\r\n" {
		t.Errorf("EXISTS after DEL: expected :0, got %q", resp)
	}

	// INCR should start from 0 again
	buf.Reset()
	h.ExecuteInto(&shared.Command{Type: shared.CmdIncr, Args: [][]byte{[]byte("counter")}}, w, conn)
	if resp := buf.String(); resp != ":1\r\n" {
		t.Errorf("INCR after DEL: expected :1 (fresh start), got %q", resp)
	}
}
