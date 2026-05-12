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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/swytchdb/swytch/redis/shared"
)

// Helper to execute a command string and return the result
func execStreamCmd(h *Handler, conn *shared.Connection, cmdStr string) string {
	parts := strings.Fields(cmdStr)
	if len(parts) == 0 {
		return ""
	}

	cmdType := shared.ParseCommandType([]byte(parts[0]))
	args := make([][]byte, len(parts)-1)
	for i := 1; i < len(parts); i++ {
		args[i-1] = []byte(parts[i])
	}

	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: cmdType, Args: args}
	h.ExecuteInto(cmd, w, conn)
	return buf.String()
}

// Blocking stream tests that require full Handler infrastructure

func TestXReadBlock(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	ctx := context.Background()
	conn := func() *shared.Connection { c := newEffectsConn(h); c.Ctx = ctx; return c }()

	var wg sync.WaitGroup
	var result string

	wg.Go(func() {
		result = execStreamCmd(h, conn, "XREAD BLOCK 5000 STREAMS blockstream $")
	})

	// Give the reader time to start blocking
	time.Sleep(50 * time.Millisecond)

	// Add an entry from another goroutine
	conn2 := func() *shared.Connection { c := newEffectsConn(h); c.Ctx = ctx; return c }()
	execStreamCmd(h, conn2, "XADD blockstream * field value")

	wg.Wait()

	if !strings.Contains(result, "blockstream") || !strings.Contains(result, "field") {
		t.Errorf("XREAD BLOCK should return the added entry, got %q", result)
	}
}

func TestXReadBlockWithPlus(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	ctx := context.Background()
	conn := func() *shared.Connection { c := newEffectsConn(h); c.Ctx = ctx; return c }()

	// Test 1: Blocking with + on non-empty stream should return last entry immediately
	execStreamCmd(h, conn, "XADD lestream 1-0 k1 v1")
	execStreamCmd(h, conn, "XADD lestream 2-0 k2 v2")
	execStreamCmd(h, conn, "XADD lestream 3-0 k3 v3")

	result := execStreamCmd(h, conn, "XREAD BLOCK 1000 STREAMS lestream +")
	if !strings.Contains(result, "3-0") {
		t.Errorf("XREAD BLOCK with + on non-empty stream should return last entry 3-0, got %q", result)
	}

	// Test 2: Blocking with + on empty stream should block until entry is added
	var wg sync.WaitGroup
	var blockResult string

	wg.Go(func() {
		blockResult = execStreamCmd(h, conn, "XREAD BLOCK 5000 STREAMS emptystream +")
	})

	// Give the reader time to start blocking
	time.Sleep(50 * time.Millisecond)

	// Add an entry from another connection
	conn2 := func() *shared.Connection { c := newEffectsConn(h); c.Ctx = ctx; return c }()
	execStreamCmd(h, conn2, "XADD emptystream 1-0 k1 v1")

	wg.Wait()

	if !strings.Contains(blockResult, "emptystream") || !strings.Contains(blockResult, "1-0") {
		t.Errorf("XREAD BLOCK with + on empty stream should return added entry, got %q", blockResult)
	}
}

func TestXReadGroupBlockingUnblockOnGroupDestroy(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()

	ctx := context.Background()
	conn1 := func() *shared.Connection { c := newEffectsConn(h); c.Ctx = ctx; return c }()
	conn2 := func() *shared.Connection { c := newEffectsConn(h); c.Ctx = ctx; return c }()

	// Setup: create stream with group
	execStreamCmd(h, conn1, "DEL mystream")
	execStreamCmd(h, conn1, "XGROUP CREATE mystream mygroup $ MKSTREAM")

	// Channel to receive result from blocking goroutine
	resultCh := make(chan string, 1)
	readyToDestroy := make(chan struct{})

	// Start blocking XREADGROUP in goroutine
	go func() {
		// Signal that we're about to block
		close(readyToDestroy)

		// Execute blocking XREADGROUP
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{
			Type: shared.CmdXReadGroup,
			Args: [][]byte{
				[]byte("GROUP"), []byte("mygroup"), []byte("Alice"),
				[]byte("BLOCK"), []byte("10000"),
				[]byte("STREAMS"), []byte("mystream"), []byte(">"),
			},
		}
		h.ExecuteInto(cmd, w, conn2)
		resultCh <- buf.String()
	}()

	// Wait for goroutine to be ready, then give it time to register
	<-readyToDestroy
	time.Sleep(50 * time.Millisecond)

	// Destroy the group - should unblock the waiting client
	result := execStreamCmd(h, conn1, "XGROUP DESTROY mystream mygroup")
	t.Logf("XGROUP DESTROY result: %q", result)

	// Wait for result with timeout
	select {
	case result := <-resultCh:
		t.Logf("XREADGROUP result: %q", result)
		if !strings.Contains(result, "NOGROUP") {
			t.Errorf("Expected NOGROUP error, got %q", result)
		}
	case <-time.After(2 * time.Second):
		t.Errorf("XREADGROUP did not unblock after XGROUP DESTROY")
	}
}
