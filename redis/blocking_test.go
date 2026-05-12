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
	"sync"
	"testing"
	"time"

	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

// newHandlerWithEffects creates a handler with a minimal in-memory effects engine.
// Use this for tests that exercise effects-dependent modules (list, etc.).
func newHandlerWithEffects() *Handler {
	cfg := DefaultHandlerConfig()
	cfg.Engine = effects.NewTestEngine()
	return NewHandler(cfg)
}

// newEffectsConn creates a Connection with EffectsCtx set, matching what the server does.
func newEffectsConn(h *Handler) *shared.Connection {
	conn := &shared.Connection{SelectedDB: 0, User: testUser, Ctx: context.Background()}
	if h.engine != nil {
		conn.EffectsCtx = h.engine.NewContext()
	}
	return conn
}

func TestBLPopImmediateReturn(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// First, push some data to the list
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("mylist"), []byte("value1")}}
	h.ExecuteInto(cmd, w, conn)

	if got := buf.String(); got != ":1\r\n" {
		t.Fatalf("RPUSH got %q, want :1\\r\\n", got)
	}

	// Now BLPOP should return immediately
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdBLPop, Args: [][]byte{[]byte("mylist"), []byte("1")}}
	h.ExecuteInto(cmd, w, conn)

	// Should return array with [key, value]
	expected := "*2\r\n$6\r\nmylist\r\n$6\r\nvalue1\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BLPOP got %q, want %q", got, expected)
	}
}

func TestBLPopTimeout(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// BLPOP on non-existent key with short timeout
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	start := time.Now()
	cmd := &shared.Command{Type: shared.CmdBLPop, Args: [][]byte{[]byte("nonexistent"), []byte("0.1")}}
	h.ExecuteInto(cmd, w, conn)
	elapsed := time.Since(start)

	// Should return null array after timeout
	expected := "*-1\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BLPOP got %q, want %q", got, expected)
	}

	// Should have waited approximately 100ms
	if elapsed < 90*time.Millisecond || elapsed > 200*time.Millisecond {
		t.Errorf("BLPOP took %v, expected ~100ms", elapsed)
	}
}

func TestBLPopBlockingWithPush(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	var wg sync.WaitGroup
	var result string
	var blpopDone time.Time

	// Start BLPOP in goroutine
	wg.Go(func() {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdBLPop, Args: [][]byte{[]byte("blocklist"), []byte("5")}}
		h.ExecuteInto(cmd, w, conn)
		result = buf.String()
		blpopDone = time.Now()
	})

	// Wait a bit then push data (use separate conn to avoid race on shared context)
	time.Sleep(50 * time.Millisecond)
	pushTime := time.Now()

	conn2 := newEffectsConn(h)
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("blocklist"), []byte("pushed")}}
	h.ExecuteInto(cmd, w, conn2)

	wg.Wait()

	// Should have returned the pushed value
	expected := "*2\r\n$9\r\nblocklist\r\n$6\r\npushed\r\n"
	if result != expected {
		t.Errorf("BLPOP got %q, want %q", result, expected)
	}

	// Should have unblocked shortly after the push
	if blpopDone.Sub(pushTime) > 100*time.Millisecond {
		t.Errorf("BLPOP unblocked too slowly after push: %v", blpopDone.Sub(pushTime))
	}
}

func TestBLPopMultipleKeys(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// Push to second key only
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("list2"), []byte("val2")}}
	h.ExecuteInto(cmd, w, conn)

	// BLPOP on multiple keys - should get from list2 since list1 is empty
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdBLPop, Args: [][]byte{[]byte("list1"), []byte("list2"), []byte("1")}}
	h.ExecuteInto(cmd, w, conn)

	expected := "*2\r\n$5\r\nlist2\r\n$4\r\nval2\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BLPOP got %q, want %q", got, expected)
	}
}

func TestBLPopKeyOrder(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// Push to both keys
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("first"), []byte("val1")}}
	h.ExecuteInto(cmd, w, conn)
	cmd = &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("second"), []byte("val2")}}
	h.ExecuteInto(cmd, w, conn)

	// BLPOP should return from first key in order
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdBLPop, Args: [][]byte{[]byte("first"), []byte("second"), []byte("1")}}
	h.ExecuteInto(cmd, w, conn)

	expected := "*2\r\n$5\r\nfirst\r\n$4\r\nval1\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BLPOP got %q, want %q", got, expected)
	}
}

func TestBRPopImmediateReturn(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// Push multiple values
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("mylist"), []byte("first"), []byte("last")}}
	h.ExecuteInto(cmd, w, conn)

	// BRPOP should return the last element (right pop)
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdBRPop, Args: [][]byte{[]byte("mylist"), []byte("1")}}
	h.ExecuteInto(cmd, w, conn)

	expected := "*2\r\n$6\r\nmylist\r\n$4\r\nlast\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BRPOP got %q, want %q", got, expected)
	}
}

func TestBLPopContextCancellation(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)
	ctx, cancel := context.WithCancel(context.Background())
	conn.Ctx = ctx

	var wg sync.WaitGroup
	var result string

	// Start BLPOP in goroutine
	wg.Go(func() {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdBLPop, Args: [][]byte{[]byte("neverexists"), []byte("10")}}
		h.ExecuteInto(cmd, w, conn)
		result = buf.String()
	})

	// Cancel context after short delay
	time.Sleep(50 * time.Millisecond)
	cancel()

	wg.Wait()

	// Should return null array due to cancellation
	expected := "*-1\r\n"
	if result != expected {
		t.Errorf("BLPOP after cancel got %q, want %q", result, expected)
	}
}

func TestBLPopWrongNumArgs(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// BLPOP with no args
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdBLPop, Args: [][]byte{}}
	h.ExecuteInto(cmd, w, conn)

	if got := buf.String(); !bytes.HasPrefix([]byte(got), []byte("-ERR wrong number")) {
		t.Errorf("BLPOP with no args got %q, want error", got)
	}

	// BLPOP with only one arg (just timeout, no key)
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdBLPop, Args: [][]byte{[]byte("1")}}
	h.ExecuteInto(cmd, w, conn)

	if got := buf.String(); !bytes.HasPrefix([]byte(got), []byte("-ERR wrong number")) {
		t.Errorf("BLPOP with one arg got %q, want error", got)
	}
}

func TestBLPopInvalidTimeout(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// BLPOP with non-numeric timeout
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdBLPop, Args: [][]byte{[]byte("key"), []byte("notanumber")}}
	h.ExecuteInto(cmd, w, conn)

	if got := buf.String(); !bytes.HasPrefix([]byte(got), []byte("-ERR timeout")) {
		t.Errorf("BLPOP with invalid timeout got %q, want timeout error", got)
	}

	// BLPOP with negative timeout
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdBLPop, Args: [][]byte{[]byte("key"), []byte("-1")}}
	h.ExecuteInto(cmd, w, conn)

	if got := buf.String(); !bytes.HasPrefix([]byte(got), []byte("-ERR timeout")) {
		t.Errorf("BLPOP with negative timeout got %q, want timeout error", got)
	}
}

// LMPOP Tests

func TestLMPopBasic(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// Push some data
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("mylist"), []byte("a"), []byte("b"), []byte("c")}}
	h.ExecuteInto(cmd, w, conn)

	// LMPOP from left
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdLMPop, Args: [][]byte{[]byte("1"), []byte("mylist"), []byte("LEFT")}}
	h.ExecuteInto(cmd, w, conn)

	// Should return [key, [element]]
	expected := "*2\r\n$6\r\nmylist\r\n*1\r\n$1\r\na\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("LMPOP LEFT got %q, want %q", got, expected)
	}
}

func TestLMPopRight(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// Push some data
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("mylist"), []byte("a"), []byte("b"), []byte("c")}}
	h.ExecuteInto(cmd, w, conn)

	// LMPOP from right
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdLMPop, Args: [][]byte{[]byte("1"), []byte("mylist"), []byte("RIGHT")}}
	h.ExecuteInto(cmd, w, conn)

	// Should return [key, [element]]
	expected := "*2\r\n$6\r\nmylist\r\n*1\r\n$1\r\nc\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("LMPOP RIGHT got %q, want %q", got, expected)
	}
}

func TestLMPopWithCount(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// Push some data
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("mylist"), []byte("a"), []byte("b"), []byte("c"), []byte("d")}}
	h.ExecuteInto(cmd, w, conn)

	// LMPOP with COUNT 2
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdLMPop, Args: [][]byte{[]byte("1"), []byte("mylist"), []byte("LEFT"), []byte("COUNT"), []byte("2")}}
	h.ExecuteInto(cmd, w, conn)

	// Should return [key, [a, b]]
	expected := "*2\r\n$6\r\nmylist\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("LMPOP COUNT 2 got %q, want %q", got, expected)
	}
}

func TestLMPopMultipleKeys(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// Only push to second key
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("list2"), []byte("value")}}
	h.ExecuteInto(cmd, w, conn)

	// LMPOP from multiple keys - should get from list2
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdLMPop, Args: [][]byte{[]byte("2"), []byte("list1"), []byte("list2"), []byte("LEFT")}}
	h.ExecuteInto(cmd, w, conn)

	expected := "*2\r\n$5\r\nlist2\r\n*1\r\n$5\r\nvalue\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("LMPOP multiple keys got %q, want %q", got, expected)
	}
}

func TestLMPopEmpty(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// LMPOP on non-existent key
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdLMPop, Args: [][]byte{[]byte("1"), []byte("nonexistent"), []byte("LEFT")}}
	h.ExecuteInto(cmd, w, conn)

	expected := "*-1\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("LMPOP empty got %q, want %q", got, expected)
	}
}

func TestLMPopCountExceedsLength(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// Push 2 elements
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("mylist"), []byte("a"), []byte("b")}}
	h.ExecuteInto(cmd, w, conn)

	// LMPOP with COUNT 10 (more than list length)
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdLMPop, Args: [][]byte{[]byte("1"), []byte("mylist"), []byte("LEFT"), []byte("COUNT"), []byte("10")}}
	h.ExecuteInto(cmd, w, conn)

	// Should return all 2 elements
	expected := "*2\r\n$6\r\nmylist\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("LMPOP COUNT exceeds length got %q, want %q", got, expected)
	}
}

// BLMPOP Tests

func TestBLMPopImmediateReturn(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// Push some data
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("mylist"), []byte("a"), []byte("b")}}
	h.ExecuteInto(cmd, w, conn)

	// BLMPOP should return immediately
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdBLMPop, Args: [][]byte{[]byte("1"), []byte("1"), []byte("mylist"), []byte("LEFT")}}
	h.ExecuteInto(cmd, w, conn)

	expected := "*2\r\n$6\r\nmylist\r\n*1\r\n$1\r\na\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BLMPOP immediate got %q, want %q", got, expected)
	}
}

func TestBLMPopWithCount(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// Push some data
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("mylist"), []byte("a"), []byte("b"), []byte("c")}}
	h.ExecuteInto(cmd, w, conn)

	// BLMPOP with COUNT 2
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdBLMPop, Args: [][]byte{[]byte("1"), []byte("1"), []byte("mylist"), []byte("RIGHT"), []byte("COUNT"), []byte("2")}}
	h.ExecuteInto(cmd, w, conn)

	expected := "*2\r\n$6\r\nmylist\r\n*2\r\n$1\r\nc\r\n$1\r\nb\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BLMPOP COUNT 2 got %q, want %q", got, expected)
	}
}

func TestBLMPopTimeout(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// BLMPOP on non-existent key with short timeout
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	start := time.Now()
	cmd := &shared.Command{Type: shared.CmdBLMPop, Args: [][]byte{[]byte("0.1"), []byte("1"), []byte("nonexistent"), []byte("LEFT")}}
	h.ExecuteInto(cmd, w, conn)
	elapsed := time.Since(start)

	expected := "*-1\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BLMPOP timeout got %q, want %q", got, expected)
	}

	if elapsed < 90*time.Millisecond || elapsed > 200*time.Millisecond {
		t.Errorf("BLMPOP took %v, expected ~100ms", elapsed)
	}
}

func TestBLMPopVerySmallTimeout(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// BLMPOP with 0.001 (1ms) timeout should not block indefinitely
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	start := time.Now()
	cmd := &shared.Command{Type: shared.CmdBLMPop, Args: [][]byte{[]byte("0.001"), []byte("1"), []byte("nonexistent"), []byte("LEFT")}}
	h.ExecuteInto(cmd, w, conn)
	elapsed := time.Since(start)

	expected := "*-1\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BLMPOP 0.001 timeout got %q, want %q", got, expected)
	}

	// Should complete within 50ms (generous margin for 1ms timeout)
	if elapsed > 50*time.Millisecond {
		t.Errorf("BLMPOP 0.001 took %v, should complete quickly", elapsed)
	}
}

func TestBLPopVerySmallTimeout(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// BLPOP with 0.001 (1ms) timeout should not block indefinitely
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	start := time.Now()
	cmd := &shared.Command{Type: shared.CmdBLPop, Args: [][]byte{[]byte("nonexistent"), []byte("0.001")}}
	h.ExecuteInto(cmd, w, conn)
	elapsed := time.Since(start)

	expected := "*-1\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BLPOP 0.001 timeout got %q, want %q", got, expected)
	}

	// Should complete within 50ms (generous margin for 1ms timeout)
	if elapsed > 50*time.Millisecond {
		t.Errorf("BLPOP 0.001 took %v, should complete quickly", elapsed)
	}
}

func TestBRPopVerySmallTimeout(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// BRPOP with 0.001 (1ms) timeout should not block indefinitely
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	start := time.Now()
	cmd := &shared.Command{Type: shared.CmdBRPop, Args: [][]byte{[]byte("nonexistent"), []byte("0.001")}}
	h.ExecuteInto(cmd, w, conn)
	elapsed := time.Since(start)

	expected := "*-1\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BRPOP 0.001 timeout got %q, want %q", got, expected)
	}

	// Should complete within 50ms (generous margin for 1ms timeout)
	if elapsed > 50*time.Millisecond {
		t.Errorf("BRPOP 0.001 took %v, should complete quickly", elapsed)
	}
}

func TestConcurrentSmallTimeouts(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	// Run 10 concurrent BLPOP commands with 0.001 timeout
	// All should complete within a reasonable time
	var wg sync.WaitGroup
	errors := make(chan string, 10)

	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := context.Background()
			conn := &shared.Connection{SelectedDB: 0, User: testUser, Ctx: ctx}

			buf := &bytes.Buffer{}
			w := shared.NewWriter(buf)
			start := time.Now()
			cmd := &shared.Command{Type: shared.CmdBLPop, Args: [][]byte{[]byte("nonexistent"), []byte("0.001")}}
			h.ExecuteInto(cmd, w, conn)
			elapsed := time.Since(start)

			if elapsed > 100*time.Millisecond {
				errors <- "goroutine took too long"
			}
			if buf.String() != "*-1\r\n" {
				errors <- "unexpected response: " + buf.String()
			}
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All completed
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for concurrent blocking commands")
	}

	close(errors)
	for err := range errors {
		t.Error(err)
	}
}

func TestBLMPopBlockingWithPush(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	var wg sync.WaitGroup
	var result string

	// Start BLMPOP in goroutine
	wg.Go(func() {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdBLMPop, Args: [][]byte{[]byte("5"), []byte("1"), []byte("blocklist"), []byte("LEFT"), []byte("COUNT"), []byte("2")}}
		h.ExecuteInto(cmd, w, conn)
		result = buf.String()
	})

	// Wait then push data (use separate conn to avoid race on shared context)
	time.Sleep(50 * time.Millisecond)

	conn2 := newEffectsConn(h)
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("blocklist"), []byte("x"), []byte("y"), []byte("z")}}
	h.ExecuteInto(cmd, w, conn2)

	wg.Wait()

	// Should get 2 elements
	expected := "*2\r\n$9\r\nblocklist\r\n*2\r\n$1\r\nx\r\n$1\r\ny\r\n"
	if result != expected {
		t.Errorf("BLMPOP blocking got %q, want %q", result, expected)
	}
}

func TestLMPopSyntaxErrors(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	tests := []struct {
		name string
		args [][]byte
	}{
		{"no args", [][]byte{}},
		{"only numkeys", [][]byte{[]byte("1")}},
		{"missing direction", [][]byte{[]byte("1"), []byte("key")}},
		{"invalid direction", [][]byte{[]byte("1"), []byte("key"), []byte("UP")}},
		{"invalid numkeys", [][]byte{[]byte("0"), []byte("key"), []byte("LEFT")}},
		{"numkeys mismatch", [][]byte{[]byte("2"), []byte("key"), []byte("LEFT")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			w := shared.NewWriter(buf)
			cmd := &shared.Command{Type: shared.CmdLMPop, Args: tt.args}
			h.ExecuteInto(cmd, w, conn)

			if got := buf.String(); !bytes.HasPrefix([]byte(got), []byte("-ERR")) {
				t.Errorf("LMPOP %s got %q, want error", tt.name, got)
			}
		})
	}
}

// LMOVE Tests

func TestLMoveBasic(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// Push data to source
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("src"), []byte("a"), []byte("b"), []byte("c")}}
	h.ExecuteInto(cmd, w, conn)

	// LMOVE from right of src to left of dst
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdLMove, Args: [][]byte{[]byte("src"), []byte("dst"), []byte("RIGHT"), []byte("LEFT")}}
	h.ExecuteInto(cmd, w, conn)

	// Should return the moved element
	expected := "$1\r\nc\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("LMOVE got %q, want %q", got, expected)
	}

	// Verify dst has the element
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdLRange, Args: [][]byte{[]byte("dst"), []byte("0"), []byte("-1")}}
	h.ExecuteInto(cmd, w, conn)

	expected = "*1\r\n$1\r\nc\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("dst LRANGE got %q, want %q", got, expected)
	}

	// Verify src has remaining elements
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdLRange, Args: [][]byte{[]byte("src"), []byte("0"), []byte("-1")}}
	h.ExecuteInto(cmd, w, conn)

	expected = "*2\r\n$1\r\na\r\n$1\r\nb\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("src LRANGE got %q, want %q", got, expected)
	}
}

func TestLMoveLeftLeft(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// Push data
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("src"), []byte("a"), []byte("b"), []byte("c")}}
	h.ExecuteInto(cmd, w, conn)

	// LMOVE LEFT LEFT
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdLMove, Args: [][]byte{[]byte("src"), []byte("dst"), []byte("LEFT"), []byte("LEFT")}}
	h.ExecuteInto(cmd, w, conn)

	expected := "$1\r\na\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("LMOVE LEFT LEFT got %q, want %q", got, expected)
	}
}

func TestLMoveSameKey(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// Push data
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("mylist"), []byte("a"), []byte("b"), []byte("c")}}
	h.ExecuteInto(cmd, w, conn)

	// LMOVE same key - rotate: pop from right, push to left
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdLMove, Args: [][]byte{[]byte("mylist"), []byte("mylist"), []byte("RIGHT"), []byte("LEFT")}}
	h.ExecuteInto(cmd, w, conn)

	expected := "$1\r\nc\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("LMOVE same key got %q, want %q", got, expected)
	}

	// Verify order: should be [c, a, b]
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdLRange, Args: [][]byte{[]byte("mylist"), []byte("0"), []byte("-1")}}
	h.ExecuteInto(cmd, w, conn)

	expected = "*3\r\n$1\r\nc\r\n$1\r\na\r\n$1\r\nb\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("LRANGE after rotate got %q, want %q", got, expected)
	}
}

func TestLMoveEmptySource(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// LMOVE from non-existent key
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdLMove, Args: [][]byte{[]byte("nonexistent"), []byte("dst"), []byte("LEFT"), []byte("LEFT")}}
	h.ExecuteInto(cmd, w, conn)

	expected := "$-1\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("LMOVE empty source got %q, want %q", got, expected)
	}
}

func TestLMoveSyntaxErrors(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	tests := []struct {
		name string
		args [][]byte
	}{
		{"no args", [][]byte{}},
		{"one arg", [][]byte{[]byte("src")}},
		{"two args", [][]byte{[]byte("src"), []byte("dst")}},
		{"three args", [][]byte{[]byte("src"), []byte("dst"), []byte("LEFT")}},
		{"invalid wherefrom", [][]byte{[]byte("src"), []byte("dst"), []byte("UP"), []byte("LEFT")}},
		{"invalid whereto", [][]byte{[]byte("src"), []byte("dst"), []byte("LEFT"), []byte("DOWN")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			w := shared.NewWriter(buf)
			cmd := &shared.Command{Type: shared.CmdLMove, Args: tt.args}
			h.ExecuteInto(cmd, w, conn)

			if got := buf.String(); !bytes.HasPrefix([]byte(got), []byte("-ERR")) {
				t.Errorf("LMOVE %s got %q, want error", tt.name, got)
			}
		})
	}
}

// BLMOVE Tests

func TestBLMoveImmediateReturn(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// Push data
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("src"), []byte("a"), []byte("b")}}
	h.ExecuteInto(cmd, w, conn)

	// BLMOVE should return immediately
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdBLMove, Args: [][]byte{[]byte("src"), []byte("dst"), []byte("RIGHT"), []byte("LEFT"), []byte("1")}}
	h.ExecuteInto(cmd, w, conn)

	expected := "$1\r\nb\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BLMOVE immediate got %q, want %q", got, expected)
	}
}

func TestBLMoveTimeout(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// BLMOVE on non-existent key with short timeout
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	start := time.Now()
	cmd := &shared.Command{Type: shared.CmdBLMove, Args: [][]byte{[]byte("nonexistent"), []byte("dst"), []byte("LEFT"), []byte("LEFT"), []byte("0.1")}}
	h.ExecuteInto(cmd, w, conn)
	elapsed := time.Since(start)

	expected := "$-1\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BLMOVE timeout got %q, want %q", got, expected)
	}

	if elapsed < 90*time.Millisecond || elapsed > 200*time.Millisecond {
		t.Errorf("BLMOVE took %v, expected ~100ms", elapsed)
	}
}

func TestBLMoveBlockingWithPush(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	var wg sync.WaitGroup
	var result string

	// Start BLMOVE in goroutine
	wg.Go(func() {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdBLMove, Args: [][]byte{[]byte("blocksrc"), []byte("blockdst"), []byte("LEFT"), []byte("RIGHT"), []byte("5")}}
		h.ExecuteInto(cmd, w, conn)
		result = buf.String()
	})

	// Wait then push data (use separate conn to avoid race on shared context)
	time.Sleep(50 * time.Millisecond)

	conn2 := newEffectsConn(h)
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("blocksrc"), []byte("pushed")}}
	h.ExecuteInto(cmd, w, conn2)

	wg.Wait()

	expected := "$6\r\npushed\r\n"
	if result != expected {
		t.Errorf("BLMOVE blocking got %q, want %q", result, expected)
	}

	// Verify dst has the element
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdLRange, Args: [][]byte{[]byte("blockdst"), []byte("0"), []byte("-1")}}
	h.ExecuteInto(cmd, w, conn)

	expected = "*1\r\n$6\r\npushed\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("blockdst LRANGE got %q, want %q", got, expected)
	}
}

// RPOPLPUSH Tests (deprecated alias)

func TestRPopLPush(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// Push data to source
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("src"), []byte("a"), []byte("b"), []byte("c")}}
	h.ExecuteInto(cmd, w, conn)

	// RPOPLPUSH = LMOVE src dst RIGHT LEFT
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdRPopLPush, Args: [][]byte{[]byte("src"), []byte("dst")}}
	h.ExecuteInto(cmd, w, conn)

	expected := "$1\r\nc\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("RPOPLPUSH got %q, want %q", got, expected)
	}

	// Verify dst
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdLRange, Args: [][]byte{[]byte("dst"), []byte("0"), []byte("-1")}}
	h.ExecuteInto(cmd, w, conn)

	expected = "*1\r\n$1\r\nc\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("dst LRANGE got %q, want %q", got, expected)
	}
}

func TestRPopLPushSameKey(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// Push data
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("mylist"), []byte("a"), []byte("b"), []byte("c")}}
	h.ExecuteInto(cmd, w, conn)

	// RPOPLPUSH same key - rotate
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdRPopLPush, Args: [][]byte{[]byte("mylist"), []byte("mylist")}}
	h.ExecuteInto(cmd, w, conn)

	expected := "$1\r\nc\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("RPOPLPUSH same key got %q, want %q", got, expected)
	}

	// Verify order: [c, a, b]
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdLRange, Args: [][]byte{[]byte("mylist"), []byte("0"), []byte("-1")}}
	h.ExecuteInto(cmd, w, conn)

	expected = "*3\r\n$1\r\nc\r\n$1\r\na\r\n$1\r\nb\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("LRANGE after RPOPLPUSH got %q, want %q", got, expected)
	}
}

func TestRPopLPushEmpty(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// RPOPLPUSH from non-existent key
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPopLPush, Args: [][]byte{[]byte("nonexistent"), []byte("dst")}}
	h.ExecuteInto(cmd, w, conn)

	expected := "$-1\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("RPOPLPUSH empty got %q, want %q", got, expected)
	}
}

// BRPOPLPUSH Tests (deprecated alias)

func TestBRPopLPushImmediateReturn(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// Push data
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("src"), []byte("a"), []byte("b")}}
	h.ExecuteInto(cmd, w, conn)

	// BRPOPLPUSH should return immediately
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdBRPopLPush, Args: [][]byte{[]byte("src"), []byte("dst"), []byte("1")}}
	h.ExecuteInto(cmd, w, conn)

	expected := "$1\r\nb\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BRPOPLPUSH immediate got %q, want %q", got, expected)
	}
}

func TestBRPopLPushTimeout(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	// BRPOPLPUSH on non-existent key with short timeout
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	start := time.Now()
	cmd := &shared.Command{Type: shared.CmdBRPopLPush, Args: [][]byte{[]byte("nonexistent"), []byte("dst"), []byte("0.1")}}
	h.ExecuteInto(cmd, w, conn)
	elapsed := time.Since(start)

	expected := "$-1\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BRPOPLPUSH timeout got %q, want %q", got, expected)
	}

	if elapsed < 90*time.Millisecond || elapsed > 200*time.Millisecond {
		t.Errorf("BRPOPLPUSH took %v, expected ~100ms", elapsed)
	}
}

func TestBRPopLPushBlockingWithPush(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	conn := newEffectsConn(h)

	var wg sync.WaitGroup
	var result string

	// Start BRPOPLPUSH in goroutine
	wg.Go(func() {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdBRPopLPush, Args: [][]byte{[]byte("blocksrc2"), []byte("blockdst2"), []byte("5")}}
		h.ExecuteInto(cmd, w, conn)
		result = buf.String()
	})

	// Wait then push data (use separate conn to avoid race on shared context)
	time.Sleep(50 * time.Millisecond)

	conn2 := newEffectsConn(h)
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdRPush, Args: [][]byte{[]byte("blocksrc2"), []byte("value")}}
	h.ExecuteInto(cmd, w, conn2)

	wg.Wait()

	expected := "$5\r\nvalue\r\n"
	if result != expected {
		t.Errorf("BRPOPLPUSH blocking got %q, want %q", result, expected)
	}
}
