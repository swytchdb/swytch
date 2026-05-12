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

package stream

import (
	"strconv"
	"strings"
	"testing"

	"github.com/swytchdb/swytch/redis/shared"
)

// xgroupCreate is a convenience to create a consumer group via the dispatcher and reset buf.
func xgroupCreate(t *testing.T, env *testEnv, args ...string) {
	t.Helper()
	allArgs := append([]string{"CREATE"}, args...)
	execHandler(t, handleXGroup, env, allArgs...)
	env.buf.Reset()
}

// xreadgroup is a convenience to deliver entries via XREADGROUP non-blocking and reset buf.
func xreadgroup(t *testing.T, env *testEnv, args ...string) string {
	t.Helper()
	result := execHandler(t, handleXReadGroupNonBlocking, env, args...)
	env.buf.Reset()
	return result
}

// --- XGROUP ---

func TestHandleXGroupCreate(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "1-0", "field", "value")

	// Create group via dispatcher
	result := execHandler(t, handleXGroup, env, "CREATE", "mystream", "mygroup", "0")
	if result != "+OK\r\n" {
		t.Errorf("XGROUP CREATE should return OK, got %q", result)
	}
	env.buf.Reset()

	// Duplicate group
	result = execHandler(t, handleXGroup, env, "CREATE", "mystream", "mygroup", "0")
	if !strings.Contains(result, "BUSYGROUP") {
		t.Errorf("XGROUP CREATE duplicate should return BUSYGROUP, got %q", result)
	}
	env.buf.Reset()

	// Create group with MKSTREAM on non-existent stream
	result = execHandler(t, handleXGroup, env, "CREATE", "newstream", "newgroup", "$", "MKSTREAM")
	if result != "+OK\r\n" {
		t.Errorf("XGROUP CREATE MKSTREAM should return OK, got %q", result)
	}
}

func TestHandleXGroupCreateConsumer(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "1-0", "field", "value")
	xgroupCreate(t, env, "mystream", "mygroup", "0")

	// Create consumer
	result := execHandler(t, handleXGroup, env, "CREATECONSUMER", "mystream", "mygroup", "consumer1")
	if result != ":1\r\n" {
		t.Errorf("XGROUP CREATECONSUMER should return 1, got %q", result)
	}
	env.buf.Reset()

	// Create existing consumer
	result = execHandler(t, handleXGroup, env, "CREATECONSUMER", "mystream", "mygroup", "consumer1")
	if result != ":0\r\n" {
		t.Errorf("XGROUP CREATECONSUMER existing should return 0, got %q", result)
	}
}

func TestHandleXGroupSetID(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "1-0", "field", "value")
	xgroupCreate(t, env, "mystream", "mygroup", "0")

	result := execHandler(t, handleXGroup, env, "SETID", "mystream", "mygroup", "$")
	if result != "+OK\r\n" {
		t.Errorf("XGROUP SETID should return OK, got %q", result)
	}
}

func TestHandleXGroupDelConsumer(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "1-0", "field", "value")
	xgroupCreate(t, env, "mystream", "mygroup", "0")
	execHandler(t, handleXGroup, env, "CREATECONSUMER", "mystream", "mygroup", "consumer1")
	env.buf.Reset()

	result := execHandler(t, handleXGroup, env, "DELCONSUMER", "mystream", "mygroup", "consumer1")
	if result != ":0\r\n" {
		t.Errorf("XGROUP DELCONSUMER should return pending count, got %q", result)
	}
}

func TestHandleXGroupDestroy(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "1-0", "field", "value")
	xgroupCreate(t, env, "mystream", "mygroup", "0")

	result := execHandler(t, handleXGroup, env, "DESTROY", "mystream", "mygroup")
	if result != ":1\r\n" {
		t.Errorf("XGROUP DESTROY should return 1, got %q", result)
	}
}

func TestHandleXGroupHelp(t *testing.T) {
	env := newTestEnv()

	result := execHandler(t, handleXGroup, env, "HELP")
	if !strings.Contains(result, "CREATE") {
		t.Errorf("XGROUP HELP should list CREATE subcommand, got %q", result)
	}
}

// --- XREADGROUP (non-blocking) ---

func TestHandleXReadGroupNonBlocking(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "1-0", "f1", "v1")
	xadd(t, env, "mystream", "2-0", "f2", "v2")
	xgroupCreate(t, env, "mystream", "mygroup", "0")

	// Read new entries with >
	result := execHandler(t, handleXReadGroupNonBlocking, env, "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", ">")
	if !strings.Contains(result, "1-0") || !strings.Contains(result, "2-0") {
		t.Errorf("XREADGROUP with > should return all entries, got %q", result)
	}
	env.buf.Reset()

	// Read again with > should return nothing
	result = execHandler(t, handleXReadGroupNonBlocking, env, "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", ">")
	if result != "*-1\r\n" {
		t.Errorf("XREADGROUP with > after delivery should return null, got %q", result)
	}
	env.buf.Reset()

	// Read pending entries with 0
	result = execHandler(t, handleXReadGroupNonBlocking, env, "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", "0")
	if !strings.Contains(result, "1-0") || !strings.Contains(result, "2-0") {
		t.Errorf("XREADGROUP with 0 should return pending entries, got %q", result)
	}
}

// --- XACK ---

func TestHandleXAck(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "1-0", "f1", "v1")
	xadd(t, env, "mystream", "2-0", "f2", "v2")
	xgroupCreate(t, env, "mystream", "mygroup", "0")

	// Deliver entries
	xreadgroup(t, env, "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", ">")

	// Acknowledge one entry
	result := execHandler(t, handleXAck, env, "mystream", "mygroup", "1-0")
	if result != ":1\r\n" {
		t.Errorf("XACK should return 1, got %q", result)
	}
	env.buf.Reset()

	// Acknowledge same entry again
	result = execHandler(t, handleXAck, env, "mystream", "mygroup", "1-0")
	if result != ":0\r\n" {
		t.Errorf("XACK already acked should return 0, got %q", result)
	}
	env.buf.Reset()

	// Read pending entries - should only have 2-0
	result = execHandler(t, handleXReadGroupNonBlocking, env, "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", "0")
	if !strings.Contains(result, "2-0") {
		t.Errorf("After XACK, pending should still have 2-0, got %q", result)
	}
}

// --- XACKDEL ---

func TestHandleXAckDel(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "1-0", "f1", "v1")
	xadd(t, env, "mystream", "2-0", "f2", "v2")
	xadd(t, env, "mystream", "3-0", "f3", "v3")
	xgroupCreate(t, env, "mystream", "mygroup", "0")

	// Deliver entries
	xreadgroup(t, env, "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", ">")

	// XACKDEL with KEEPREF - returns an array of results per ID
	result := execHandler(t, handleXAckDel, env, "mystream", "mygroup", "KEEPREF", "IDS", "2", "1-0", "2-0")
	// Each entry returns 1 if acked, so result contains :1
	if !strings.Contains(result, ":1") {
		t.Errorf("XACKDEL should acknowledge entries, got %q", result)
	}
	env.buf.Reset()

	// Wrong num args
	cmd := &shared.Command{Args: [][]byte{[]byte("mystream")}, Runtime: env.eng, Context: env.ctx}
	valid, _, _ := handleXAckDel(cmd, env.w, nil)
	if valid {
		t.Fatal("expected invalid for wrong args")
	}
	if !strings.Contains(env.buf.String(), "ERR") {
		t.Errorf("expected error, got %q", env.buf.String())
	}
}

// --- XPENDING ---

func TestHandleXPending(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "1-0", "f1", "v1")
	xadd(t, env, "mystream", "2-0", "f2", "v2")
	xadd(t, env, "mystream", "3-0", "f3", "v3")
	xgroupCreate(t, env, "mystream", "mygroup", "0")

	// Deliver to consumer
	xreadgroup(t, env, "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", ">")

	// Summary form: XPENDING key group
	result := execHandler(t, handleXPending, env, "mystream", "mygroup")
	// Should show 3 pending entries
	if !strings.Contains(result, "3") {
		t.Errorf("XPENDING summary should show 3 pending entries, got %q", result)
	}
	if !strings.Contains(result, "consumer1") {
		t.Errorf("XPENDING summary should list consumer1, got %q", result)
	}
	env.buf.Reset()

	// Extended form: XPENDING key group start end count
	result = execHandler(t, handleXPending, env, "mystream", "mygroup", "-", "+", "10")
	if !strings.Contains(result, "1-0") {
		t.Errorf("XPENDING extended should list entry 1-0, got %q", result)
	}
	if !strings.Contains(result, "consumer1") {
		t.Errorf("XPENDING extended should list consumer1, got %q", result)
	}
	env.buf.Reset()

	// Extended form with consumer filter
	result = execHandler(t, handleXPending, env, "mystream", "mygroup", "-", "+", "10", "consumer1")
	if !strings.Contains(result, "1-0") {
		t.Errorf("XPENDING with consumer filter should list entries, got %q", result)
	}
	env.buf.Reset()

	// Non-existent group
	result = execHandler(t, handleXPending, env, "mystream", "nogroup")
	if !strings.Contains(result, "NOGROUP") {
		t.Errorf("XPENDING on non-existent group should return NOGROUP, got %q", result)
	}
}

// --- XCLAIM ---

func TestHandleXClaim(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "1-0", "f1", "v1")
	xadd(t, env, "mystream", "2-0", "f2", "v2")
	xadd(t, env, "mystream", "3-0", "f3", "v3")
	xgroupCreate(t, env, "mystream", "mygroup", "0")

	// Deliver entries to consumer1
	xreadgroup(t, env, "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", ">")

	// Claim entries 1-0 and 2-0 for consumer2 with min-idle-time 0
	result := execHandler(t, handleXClaim, env, "mystream", "mygroup", "consumer2", "0", "1-0", "2-0")
	if !strings.Contains(result, "1-0") || !strings.Contains(result, "2-0") {
		t.Errorf("XCLAIM should return claimed entries, got %q", result)
	}
	env.buf.Reset()

	// Verify consumer2 now owns those entries via XPENDING
	result = execHandler(t, handleXPending, env, "mystream", "mygroup", "-", "+", "10", "consumer2")
	if !strings.Contains(result, "1-0") || !strings.Contains(result, "2-0") {
		t.Errorf("XPENDING should show entries owned by consumer2, got %q", result)
	}
	env.buf.Reset()

	// XCLAIM with JUSTID
	result = execHandler(t, handleXClaim, env, "mystream", "mygroup", "consumer1", "0", "1-0", "JUSTID")
	if strings.Contains(result, "f1") {
		t.Errorf("XCLAIM JUSTID should not include field data, got %q", result)
	}
	env.buf.Reset()

	// XCLAIM with FORCE on non-existent entry
	result = execHandler(t, handleXClaim, env, "mystream", "mygroup", "consumer1", "0", "99-0", "FORCE")
	// Should not error
	if strings.Contains(result, "ERR") {
		t.Errorf("XCLAIM FORCE should not error, got %q", result)
	}
}

// --- XAUTOCLAIM ---

func TestHandleXAutoClaim(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "1-0", "f1", "v1")
	xadd(t, env, "mystream", "2-0", "f2", "v2")
	xadd(t, env, "mystream", "3-0", "f3", "v3")
	xgroupCreate(t, env, "mystream", "mygroup", "0")

	// Deliver entries to consumer1
	xreadgroup(t, env, "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", ">")

	// Autoclaim for consumer2 with min-idle-time 0, starting from 0-0
	result := execHandler(t, handleXAutoClaim, env, "mystream", "mygroup", "consumer2", "0", "0-0")
	if !strings.Contains(result, "1-0") {
		t.Errorf("XAUTOCLAIM should return claimed entries, got %q", result)
	}
	env.buf.Reset()

	// Autoclaim with COUNT
	result = execHandler(t, handleXAutoClaim, env, "mystream", "mygroup", "consumer2", "0", "0-0", "COUNT", "1")
	if !strings.Contains(result, "1-0") && !strings.Contains(result, "2-0") && !strings.Contains(result, "3-0") {
		t.Errorf("XAUTOCLAIM COUNT 1 should return at least one entry, got %q", result)
	}
	env.buf.Reset()

	// Autoclaim with JUSTID
	result = execHandler(t, handleXAutoClaim, env, "mystream", "mygroup", "consumer2", "0", "0-0", "JUSTID")
	if strings.Contains(result, "f1") {
		t.Errorf("XAUTOCLAIM JUSTID should not include field data, got %q", result)
	}
}

// --- XINFO GROUPS ---

func TestHandleXInfoGroups(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "1-0", "field1", "value1")
	xgroupCreate(t, env, "mystream", "mygroup", "0")

	result := execHandler(t, handleXInfo, env, "GROUPS", "mystream")
	if !strings.Contains(result, "mygroup") {
		t.Errorf("XINFO GROUPS should contain 'mygroup', got %q", result)
	}
	if !strings.Contains(result, "consumers") {
		t.Errorf("XINFO GROUPS should contain 'consumers', got %q", result)
	}
	if !strings.Contains(result, "pending") {
		t.Errorf("XINFO GROUPS should contain 'pending', got %q", result)
	}
}

// --- XINFO CONSUMERS ---

func TestHandleXInfoConsumers(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "1-0", "field1", "value1")
	xgroupCreate(t, env, "mystream", "mygroup", "0")

	// Deliver to consumer
	xreadgroup(t, env, "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", ">")

	result := execHandler(t, handleXInfo, env, "CONSUMERS", "mystream", "mygroup")
	if !strings.Contains(result, "consumer1") {
		t.Errorf("XINFO CONSUMERS should contain 'consumer1', got %q", result)
	}
	if !strings.Contains(result, "idle") {
		t.Errorf("XINFO CONSUMERS should contain 'idle', got %q", result)
	}
	env.buf.Reset()

	// Non-existent group
	result = execHandler(t, handleXInfo, env, "CONSUMERS", "mystream", "nogroup")
	if !strings.Contains(result, "NOGROUP") {
		t.Errorf("XINFO CONSUMERS on non-existent group should return NOGROUP error, got %q", result)
	}
}

// --- XADD with ACKED trimming ---

func TestHandleXAddAckedTrimming(t *testing.T) {
	env := newTestEnv()

	// Add 5 entries with explicit IDs
	for i := 1; i <= 5; i++ {
		xadd(t, env, "mystream", strconv.Itoa(i)+"-0", "field", "value")
	}

	// Create a consumer group starting from 0
	xgroupCreate(t, env, "mystream", "mygroup", "0")

	// Read entries 1-3 (put them in pending)
	xreadgroup(t, env, "GROUP", "mygroup", "consumer1", "COUNT", "3", "STREAMS", "mystream", ">")

	// Add entry 6 with MAXLEN 2 and ACKED
	xadd(t, env, "mystream", "ACKED", "MAXLEN", "2", "6-0", "field", "value")

	result := execHandler(t, handleXLen, env, "mystream")
	if result != ":6\r\n" {
		t.Errorf("XLEN after ACKED MAXLEN should be 6 (pending entries not trimmed), got %q", result)
	}
	env.buf.Reset()

	// Acknowledge entries 1-3
	execHandler(t, handleXAck, env, "mystream", "mygroup", "1-0", "2-0", "3-0")
	env.buf.Reset()

	// Read remaining entries 4-6 (put them in pending)
	xreadgroup(t, env, "GROUP", "mygroup", "consumer1", "STREAMS", "mystream", ">")

	// Acknowledge 4-5, leave 6 pending
	execHandler(t, handleXAck, env, "mystream", "mygroup", "4-0", "5-0")
	env.buf.Reset()

	// Add entry 7 with MAXLEN 2 and ACKED
	xadd(t, env, "mystream", "ACKED", "MAXLEN", "2", "7-0", "field", "value")

	result = execHandler(t, handleXLen, env, "mystream")
	if result != ":2\r\n" {
		t.Errorf("XLEN after ACKED MAXLEN 2 should be 2, got %q", result)
	}
	env.buf.Reset()

	// Add entry 8 with MAXLEN 2 and ACKED
	xadd(t, env, "mystream", "ACKED", "MAXLEN", "2", "8-0", "field", "value")

	result = execHandler(t, handleXLen, env, "mystream")
	if result != ":3\r\n" {
		t.Errorf("XLEN after ACKED MAXLEN 2 with pending entry should be 3, got %q", result)
	}
}
