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
	"bytes"
	"strconv"
	"strings"
	"testing"

	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

// testEnv holds the test environment with effects engine.
type testEnv struct {
	eng *effects.Engine
	ctx *effects.Context
	w   *shared.Writer
	buf *bytes.Buffer
}

func newTestEnv() *testEnv {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	return &testEnv{eng: eng, ctx: ctx, w: w, buf: buf}
}

// execHandler runs a standard handler with the effects engine.
func execHandler(t *testing.T, handler func(*shared.Command, *shared.Writer, *shared.Database) (bool, []string, shared.CommandRunner), env *testEnv, args ...string) string {
	t.Helper()
	byteArgs := make([][]byte, len(args))
	for i, a := range args {
		byteArgs[i] = []byte(a)
	}
	cmd := &shared.Command{Args: byteArgs, Runtime: env.eng, Context: env.ctx}
	valid, _, runner := handler(cmd, env.w, nil)
	if !valid {
		return env.buf.String()
	}
	if runner != nil {
		runner()
		_ = env.ctx.Flush()
	}
	return env.buf.String()
}

// xadd is a convenience to add a stream entry and reset the buffer.
func xadd(t *testing.T, env *testEnv, args ...string) string {
	t.Helper()
	result := execHandler(t, handleXAdd, env, args...)
	env.buf.Reset()
	return result
}

// --- XADD / XLEN ---

func TestHandleXAddXLen(t *testing.T) {
	env := newTestEnv()

	// Basic XADD
	result := execHandler(t, handleXAdd, env, "mystream", "*", "field1", "value1")
	if !strings.HasPrefix(result, "$") {
		t.Errorf("XADD should return bulk string, got %q", result)
	}
	env.buf.Reset()

	// XLEN
	result = execHandler(t, handleXLen, env, "mystream")
	if result != ":1\r\n" {
		t.Errorf("XLEN should return 1, got %q", result)
	}
	env.buf.Reset()

	xadd(t, env, "mystream", "*", "field2", "value2")
	result = execHandler(t, handleXLen, env, "mystream")
	if result != ":2\r\n" {
		t.Errorf("XLEN should return 2, got %q", result)
	}
}

func TestHandleXAddNOMKSTREAM(t *testing.T) {
	env := newTestEnv()

	result := execHandler(t, handleXAdd, env, "nostream", "NOMKSTREAM", "*", "field", "value")
	if result != "$-1\r\n" {
		t.Errorf("XADD NOMKSTREAM on non-existent key should return null, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXLen, env, "nostream")
	if result != ":0\r\n" {
		t.Errorf("XLEN on non-existent key should return 0, got %q", result)
	}
}

func TestHandleXAddMAXLEN(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "trimstream", "*", "f1", "v1")
	xadd(t, env, "trimstream", "*", "f2", "v2")
	xadd(t, env, "trimstream", "*", "f3", "v3")
	xadd(t, env, "trimstream", "MAXLEN", "2", "*", "f4", "v4")

	result := execHandler(t, handleXLen, env, "trimstream")
	if result != ":2\r\n" {
		t.Errorf("XLEN after MAXLEN trim should return 2, got %q", result)
	}
}

func TestHandleXAddApproxTrimming(t *testing.T) {
	env := newTestEnv()

	prev := shared.GetStreamNodeMaxEntries()
	shared.SetStreamNodeMaxEntries(100)
	defer shared.SetStreamNodeMaxEntries(prev)

	for i := range 1000 {
		xadd(t, env, "mystream", "MAXLEN", "~", "555", "*", "xitem", strconv.Itoa(i))
	}

	result := execHandler(t, handleXLen, env, "mystream")
	if result != ":600\r\n" {
		t.Errorf("XLEN after MAXLEN ~ 555 with node size 100 should return 600, got %q", result)
	}
}

func TestHandleXAddHybridID(t *testing.T) {
	env := newTestEnv()

	result := execHandler(t, handleXAdd, env, "mystream", "123-456", "item", "1", "value", "a")
	if result != "$7\r\n123-456\r\n" {
		t.Errorf("XADD with explicit ID should return the ID, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXAdd, env, "mystream", "123-*", "item", "2", "value", "b")
	if result != "$7\r\n123-457\r\n" {
		t.Errorf("XADD with hybrid ID should return 123-457, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXAdd, env, "mystream", "42-*", "item", "3", "value", "c")
	if !strings.Contains(result, "ERR") {
		t.Errorf("XADD with hybrid ID where ms < lastID.ms should return error, got %q", result)
	}
}

func TestHandleXAddWithMINID(t *testing.T) {
	env := newTestEnv()

	for j := 1; j < 1001; j++ {
		minid := 1000
		if j >= 5 {
			minid = j - 5
		}
		xadd(t, env, "mystream", "MINID", strconv.Itoa(minid), strconv.Itoa(j), "xitem", strconv.Itoa(j))
	}

	result := execHandler(t, handleXLen, env, "mystream")
	if !strings.Contains(result, ":6\r\n") {
		t.Errorf("Expected XLEN == 6, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXRange, env, "mystream", "-", "+")
	for expected := 995; expected <= 1000; expected++ {
		expectedID := strconv.Itoa(expected) + "-0"
		if !strings.Contains(result, expectedID) {
			t.Errorf("Expected entry %s in result, got %q", expectedID, result)
		}
	}
}

func TestHandleXAddWithMINIDSimple(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "MINID", "1000", "1", "field", "value")

	result := execHandler(t, handleXLen, env, "mystream")
	if result != ":0\r\n" {
		t.Errorf("Entry 1 should have been trimmed (ID 1 < MINID 1000), got XLEN=%q", result)
	}
	env.buf.Reset()

	xadd(t, env, "mystream", "MINID", "1000", "2", "field", "value")
	result = execHandler(t, handleXLen, env, "mystream")
	if result != ":0\r\n" {
		t.Errorf("Entry 2 should have been trimmed, got XLEN=%q", result)
	}
	env.buf.Reset()

	xadd(t, env, "mystream", "MINID", "1000", "1001", "field", "value")
	result = execHandler(t, handleXLen, env, "mystream")
	if result != ":1\r\n" {
		t.Errorf("Entry 1001 should remain (ID 1001 >= MINID 1000), got XLEN=%q", result)
	}
}

func TestHandleXAddWithMINIDProgressive(t *testing.T) {
	env := newTestEnv()

	for j := 5; j <= 10; j++ {
		minid := j - 5
		xadd(t, env, "mystream", "MINID", strconv.Itoa(minid), strconv.Itoa(j), "xitem", strconv.Itoa(j))
	}

	result := execHandler(t, handleXLen, env, "mystream")
	if !strings.Contains(result, ":6\r\n") {
		t.Errorf("Expected XLEN == 6, got %q", result)
	}
	env.buf.Reset()

	// Trim manually
	result = execHandler(t, handleXTrim, env, "mystream", "MINID", "8")
	env.buf.Reset()
	result = execHandler(t, handleXLen, env, "mystream")
	if !strings.Contains(result, ":3\r\n") {
		t.Errorf("Expected XLEN == 3 after XTRIM MINID 8, got %q", result)
	}
}

func TestHandleXAddWrongNumArgs(t *testing.T) {
	env := newTestEnv()

	cmd := &shared.Command{Args: [][]byte{}, Runtime: env.eng, Context: env.ctx}
	valid, _, _ := handleXAdd(cmd, env.w, nil)
	if valid {
		t.Fatal("expected invalid for no args")
	}
	if !strings.Contains(env.buf.String(), "ERR") {
		t.Errorf("expected error, got %q", env.buf.String())
	}
}

// --- XRANGE ---

func TestHandleXRange(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "1-0", "field1", "value1")
	xadd(t, env, "mystream", "2-0", "field2", "value2")
	xadd(t, env, "mystream", "3-0", "field3", "value3")

	// Range all
	result := execHandler(t, handleXRange, env, "mystream", "-", "+")
	if !strings.Contains(result, "1-0") || !strings.Contains(result, "3-0") {
		t.Errorf("XRANGE - + should contain all entries, got %q", result)
	}
	env.buf.Reset()

	// Range with COUNT
	result = execHandler(t, handleXRange, env, "mystream", "-", "+", "COUNT", "2")
	if !strings.Contains(result, "1-0") || !strings.Contains(result, "2-0") {
		t.Errorf("XRANGE with COUNT 2 should return first 2 entries, got %q", result)
	}
	env.buf.Reset()

	// Range specific
	result = execHandler(t, handleXRange, env, "mystream", "2-0", "3-0")
	if !strings.Contains(result, "2-0") || !strings.Contains(result, "3-0") {
		t.Errorf("XRANGE 2-0 3-0 should return entries 2 and 3, got %q", result)
	}
}

func TestHandleXRangeExclusiveErrors(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "1-0", "field", "value")
	xadd(t, env, "mystream", "2-0", "field", "value")
	xadd(t, env, "mystream", "3-0", "field", "value")

	result := execHandler(t, handleXRange, env, "mystream", "(-", "+")
	if !strings.Contains(result, "ERR") {
		t.Errorf("XRANGE (- + should return error, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXRange, env, "mystream", "-", "(+")
	if !strings.Contains(result, "ERR") {
		t.Errorf("XRANGE - (+ should return error, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXRange, env, "mystream", "-", "(0-0")
	if !strings.Contains(result, "ERR") {
		t.Errorf("XRANGE - (0-0 should return error, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXRange, env, "mystream", "(0-0", "+")
	if !strings.Contains(result, "ERR") {
		t.Errorf("XRANGE (0-0 + should return error, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXRange, env, "mystream", "(18446744073709551615-18446744073709551615", "+")
	if !strings.Contains(result, "ERR") {
		t.Errorf("XRANGE (max-id + should return error, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXRange, env, "mystream", "-", "(18446744073709551615-18446744073709551615")
	if strings.Contains(result, "ERR") {
		t.Errorf("XRANGE - (max-id should work, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXRange, env, "mystream", "(1-0", "+")
	if strings.Contains(result, "ERR") {
		t.Errorf("XRANGE (1-0 + should work, got %q", result)
	}
	if !strings.Contains(result, "2-0") {
		t.Errorf("XRANGE (1-0 + should return 2-0, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXRange, env, "mystream", "-", "(3-0")
	if strings.Contains(result, "ERR") {
		t.Errorf("XRANGE - (3-0 should work, got %q", result)
	}
	if !strings.Contains(result, "1-0") || !strings.Contains(result, "2-0") {
		t.Errorf("XRANGE - (3-0 should return 1-0 and 2-0, got %q", result)
	}
	if strings.Contains(result, "3-0") {
		t.Errorf("XRANGE - (3-0 should NOT return 3-0, got %q", result)
	}
}

func TestHandleXRangeWrongNumArgs(t *testing.T) {
	env := newTestEnv()

	cmd := &shared.Command{Args: [][]byte{[]byte("mystream")}, Runtime: env.eng, Context: env.ctx}
	valid, _, _ := handleXRange(cmd, env.w, nil)
	if valid {
		t.Fatal("expected invalid")
	}
	if !strings.Contains(env.buf.String(), "ERR") {
		t.Errorf("expected error, got %q", env.buf.String())
	}
}

// --- XREVRANGE ---

func TestHandleXRevRange(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "1-0", "field1", "value1")
	xadd(t, env, "mystream", "2-0", "field2", "value2")
	xadd(t, env, "mystream", "3-0", "field3", "value3")

	result := execHandler(t, handleXRevRange, env, "mystream", "+", "-")
	if !strings.Contains(result, "3-0") || !strings.Contains(result, "1-0") {
		t.Errorf("XREVRANGE + - should return all entries, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXRevRange, env, "mystream", "+", "-", "COUNT", "1")
	if !strings.Contains(result, "3-0") {
		t.Errorf("XREVRANGE + - COUNT 1 should return last entry, got %q", result)
	}
}

// --- XREAD (non-blocking) ---

func TestHandleXReadNonBlocking(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "stream1", "1-0", "f1", "v1")
	xadd(t, env, "stream1", "2-0", "f2", "v2")
	xadd(t, env, "stream2", "1-0", "fa", "va")

	// Read from beginning
	result := execHandler(t, handleXReadNonBlocking, env, "STREAMS", "stream1", "0")
	if !strings.Contains(result, "1-0") || !strings.Contains(result, "2-0") {
		t.Errorf("XREAD STREAMS stream1 0 should return all entries, got %q", result)
	}
	env.buf.Reset()

	// Read from specific ID
	result = execHandler(t, handleXReadNonBlocking, env, "STREAMS", "stream1", "1-0")
	if !strings.Contains(result, "2-0") {
		t.Errorf("XREAD after 1-0 should return entry 2-0, got %q", result)
	}
	env.buf.Reset()

	// Read with $
	result = execHandler(t, handleXReadNonBlocking, env, "STREAMS", "stream1", "$")
	if result != "*-1\r\n" {
		t.Errorf("XREAD with $ should return null (no new entries), got %q", result)
	}
	env.buf.Reset()

	// Read with COUNT
	result = execHandler(t, handleXReadNonBlocking, env, "COUNT", "1", "STREAMS", "stream1", "0")
	if !strings.Contains(result, "1-0") || strings.Contains(result, "2-0") {
		t.Errorf("XREAD COUNT 1 should return only first entry, got %q", result)
	}
	env.buf.Reset()

	// Read from non-existent stream
	result = execHandler(t, handleXReadNonBlocking, env, "STREAMS", "nostream", "0")
	if result != "*-1\r\n" {
		t.Errorf("XREAD on non-existent stream should return null, got %q", result)
	}
}

// --- XTRIM ---

func TestHandleXTrim(t *testing.T) {
	env := newTestEnv()

	for i := 1; i <= 5; i++ {
		xadd(t, env, "mystream", strconv.Itoa(i)+"-0", "field", "value")
	}

	result := execHandler(t, handleXTrim, env, "mystream", "MAXLEN", "3")
	if result != ":2\r\n" {
		t.Errorf("XTRIM MAXLEN 3 should trim 2 entries, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXLen, env, "mystream")
	if result != ":3\r\n" {
		t.Errorf("XLEN after XTRIM MAXLEN 3 should be 3, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXTrim, env, "mystream", "MINID", "5-0")
	if result != ":2\r\n" {
		t.Errorf("XTRIM MINID 5-0 should trim 2 entries, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXLen, env, "mystream")
	if result != ":1\r\n" {
		t.Errorf("XLEN after XTRIM MINID 5-0 should be 1, got %q", result)
	}
}

func TestHandleXTrimWithEquals(t *testing.T) {
	env := newTestEnv()

	for i := 1; i <= 5; i++ {
		xadd(t, env, "mystream", strconv.Itoa(i)+"-0", "field", "value")
	}

	result := execHandler(t, handleXTrim, env, "mystream", "MAXLEN", "=", "3")
	if result != ":2\r\n" {
		t.Errorf("XTRIM MAXLEN = 3 should trim 2 entries, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXLen, env, "mystream")
	if result != ":3\r\n" {
		t.Errorf("XLEN after XTRIM MAXLEN = 3 should be 3, got %q", result)
	}
}

func TestHandleXTrimMINIDWithEquals(t *testing.T) {
	env := newTestEnv()

	for i := 1; i <= 5; i++ {
		xadd(t, env, "mystream", strconv.Itoa(i)+"-0", "field", "value")
	}

	result := execHandler(t, handleXTrim, env, "mystream", "MINID", "=", "3-0")
	if result != ":2\r\n" {
		t.Errorf("XTRIM MINID = 3-0 should trim 2 entries, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXLen, env, "mystream")
	if result != ":3\r\n" {
		t.Errorf("XLEN after XTRIM MINID = 3-0 should be 3, got %q", result)
	}
}

func TestHandleXTrimNonExistentKey(t *testing.T) {
	env := newTestEnv()

	result := execHandler(t, handleXTrim, env, "nostream", "MAXLEN", "10")
	if result != ":0\r\n" {
		t.Errorf("XTRIM on non-existent key should return 0, got %q", result)
	}
}

func TestHandleXTrimApprox(t *testing.T) {
	env := newTestEnv()

	prev := shared.GetStreamNodeMaxEntries()
	shared.SetStreamNodeMaxEntries(10)
	defer shared.SetStreamNodeMaxEntries(prev)

	for i := 1; i <= 50; i++ {
		xadd(t, env, "mystream", strconv.Itoa(i)+"-0", "field", "value")
	}

	result := execHandler(t, handleXTrim, env, "mystream", "MAXLEN", "~", "25")
	if result != ":20\r\n" {
		t.Errorf("XTRIM MAXLEN ~ 25 with node size 10 should trim 20 entries, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXLen, env, "mystream")
	if result != ":30\r\n" {
		t.Errorf("XLEN after XTRIM MAXLEN ~ 25 should be 30, got %q", result)
	}
}

func TestHandleXTrimWithLimit(t *testing.T) {
	env := newTestEnv()

	for i := 1; i <= 10; i++ {
		xadd(t, env, "mystream", strconv.Itoa(i)+"-0", "field", "value")
	}

	result := execHandler(t, handleXTrim, env, "mystream", "MAXLEN", "3", "LIMIT", "2")
	if !strings.HasPrefix(result, "-ERR") {
		t.Errorf("XTRIM MAXLEN 3 LIMIT 2 (without ~) should return error, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXTrim, env, "mystream", "MAXLEN", "~", "3", "LIMIT", "2")
	if result != ":0\r\n" {
		t.Errorf("XTRIM MAXLEN ~ 3 LIMIT 2 with NodeSize=100 should trim 0 entries, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXLen, env, "mystream")
	if result != ":10\r\n" {
		t.Errorf("XLEN after XTRIM should still be 10, got %q", result)
	}
}

// --- XDEL ---

func TestHandleXDel(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "1-0", "field", "value")
	xadd(t, env, "mystream", "2-0", "field", "value")
	xadd(t, env, "mystream", "3-0", "field", "value")
	xadd(t, env, "mystream", "4-0", "field", "value")
	xadd(t, env, "mystream", "5-0", "field", "value")

	result := execHandler(t, handleXDel, env, "mystream", "3-0")
	if result != ":1\r\n" {
		t.Errorf("XDEL 3-0 should return 1, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXLen, env, "mystream")
	if result != ":4\r\n" {
		t.Errorf("XLEN after XDEL should be 4, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXDel, env, "mystream", "1-0", "5-0")
	if result != ":2\r\n" {
		t.Errorf("XDEL 1-0 5-0 should return 2, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXLen, env, "mystream")
	if result != ":2\r\n" {
		t.Errorf("XLEN after XDEL should be 2, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXDel, env, "mystream", "99-0")
	if result != ":0\r\n" {
		t.Errorf("XDEL non-existent should return 0, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXDel, env, "nostream", "1-0")
	if result != ":0\r\n" {
		t.Errorf("XDEL from non-existent key should return 0, got %q", result)
	}
}

// --- XINFO ---

func TestHandleXInfoStream(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "1-0", "field1", "value1")
	xadd(t, env, "mystream", "2-0", "field2", "value2")
	xadd(t, env, "mystream", "3-0", "field3", "value3")

	result := execHandler(t, handleXInfo, env, "STREAM", "mystream")
	if !strings.Contains(result, "length") {
		t.Errorf("XINFO STREAM should contain 'length', got %q", result)
	}
	if !strings.Contains(result, "last-generated-id") {
		t.Errorf("XINFO STREAM should contain 'last-generated-id', got %q", result)
	}
	if !strings.Contains(result, "first-entry") {
		t.Errorf("XINFO STREAM should contain 'first-entry', got %q", result)
	}
	if !strings.Contains(result, "last-entry") {
		t.Errorf("XINFO STREAM should contain 'last-entry', got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXInfo, env, "STREAM", "mystream", "FULL")
	if !strings.Contains(result, "entries") {
		t.Errorf("XINFO STREAM FULL should contain 'entries', got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXInfo, env, "STREAM", "nostream")
	if !strings.Contains(result, "ERR") {
		t.Errorf("XINFO STREAM on non-existent key should return error, got %q", result)
	}
}

func TestHandleXInfoHelp(t *testing.T) {
	env := newTestEnv()

	result := execHandler(t, handleXInfo, env, "HELP")
	if !strings.Contains(result, "STREAM") {
		t.Errorf("XINFO HELP should list STREAM subcommand, got %q", result)
	}
}

// --- XSETID ---

func TestHandleXSetId(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "1-0", "field1", "value1")
	xadd(t, env, "mystream", "2-0", "field2", "value2")
	xadd(t, env, "mystream", "3-0", "field3", "value3")

	result := execHandler(t, handleXSetId, env, "mystream", "5-0")
	if result != "+OK\r\n" {
		t.Errorf("XSETID should return OK, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXInfo, env, "STREAM", "mystream")
	if !strings.Contains(result, "5-0") {
		t.Errorf("XINFO STREAM should show last-generated-id as 5-0, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXSetId, env, "mystream", "10-0", "ENTRIESADDED", "100")
	if result != "+OK\r\n" {
		t.Errorf("XSETID with ENTRIESADDED should return OK, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXSetId, env, "mystream", "15-0", "MAXDELETEDID", "5-0")
	if result != "+OK\r\n" {
		t.Errorf("XSETID with MAXDELETEDID should return OK, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXSetId, env, "mystream", "20-0", "ENTRIESADDED", "200", "MAXDELETEDID", "10-0")
	if result != "+OK\r\n" {
		t.Errorf("XSETID with both options should return OK, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXSetId, env, "mystream", "1-0")
	if !strings.Contains(result, "ERR") || !strings.Contains(result, "smaller") {
		t.Errorf("XSETID with ID smaller than top entry should return error, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXSetId, env, "nostream", "1-0")
	if !strings.Contains(result, "ERR") {
		t.Errorf("XSETID on non-existent key should return error, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXSetId, env, "mystream", "25-0", "MAXDELETEDID", "30-0")
	if !strings.Contains(result, "ERR") {
		t.Errorf("XSETID with MAXDELETEDID > lastID should return error, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXSetId, env, "mystream", "invalid")
	if !strings.Contains(result, "ERR") {
		t.Errorf("XSETID with invalid ID should return error, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXSetId, env, "mystream")
	if !strings.Contains(result, "ERR") {
		t.Errorf("XSETID with wrong args should return error, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXSetId, env, "mystream", "30-0", "ENTRIESADDED", "1", "MAXDELETEDID", "0-0")
	if !strings.Contains(result, "ERR") || !strings.Contains(result, "smaller") {
		t.Errorf("XSETID with ENTRIESADDED < length should return error, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXSetId, env, "mystream", "30-0", "ENTRIESADDED", "-1", "MAXDELETEDID", "0-0")
	if !strings.Contains(result, "ERR") || !strings.Contains(result, "positive") {
		t.Errorf("XSETID with negative ENTRIESADDED should return error, got %q", result)
	}
	env.buf.Reset()

	// XSETID on empty stream
	xadd(t, env, "emptystream", "MAXLEN", "0", "*", "field", "value")
	result = execHandler(t, handleXSetId, env, "emptystream", "200-0")
	if result != "+OK\r\n" {
		t.Errorf("XSETID on empty stream should return OK, got %q", result)
	}
	env.buf.Reset()

	// XSETID cannot set ID smaller than MaxDeletedID
	xadd(t, env, "xdelstream", "1-0", "a", "1")
	xadd(t, env, "xdelstream", "2-0", "b", "2")
	xadd(t, env, "xdelstream", "3-0", "c", "3")
	execHandler(t, handleXDel, env, "xdelstream", "3-0")
	env.buf.Reset()

	result = execHandler(t, handleXSetId, env, "xdelstream", "2-0")
	if !strings.Contains(result, "ERR") || !strings.Contains(result, "smaller") {
		t.Errorf("XSETID with ID < MaxDeletedID should return error, got %q", result)
	}
}

// --- StreamID Parsing ---

func TestStreamIDParsing(t *testing.T) {
	tests := []struct {
		input     string
		expectMs  uint64
		expectSeq uint64
		expectErr bool
		special   bool
	}{
		{"1234567890123-0", 1234567890123, 0, false, false},
		{"0-0", 0, 0, false, false},
		{"1-1", 1, 1, false, false},
		{"*", 0, 0, false, true},
		{"$", 0, 0, false, true},
		{">", 0, 0, false, true},
		{"-", 0, 0, false, true},
		{"+", ^uint64(0), ^uint64(0), false, true},
		{"invalid", 0, 0, true, false},
		{"", 0, 0, true, false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			id, _, isSpecial, err := shared.ParseStreamID(tt.input)
			if tt.expectErr {
				if err == nil {
					t.Errorf("Expected error for %q", tt.input)
				}
				return
			}
			if err != nil {
				t.Errorf("Unexpected error for %q: %v", tt.input, err)
				return
			}
			if isSpecial != tt.special {
				t.Errorf("Input %q: expected special=%v, got %v", tt.input, tt.special, isSpecial)
			}
			if !tt.special {
				if id.Ms != tt.expectMs || id.Seq != tt.expectSeq {
					t.Errorf("Input %q: expected %d-%d, got %d-%d", tt.input, tt.expectMs, tt.expectSeq, id.Ms, id.Seq)
				}
			}
		})
	}
}

// --- XDELEX ---

func TestHandleXDelEx(t *testing.T) {
	env := newTestEnv()

	xadd(t, env, "mystream", "1-0", "f1", "v1")
	xadd(t, env, "mystream", "2-0", "f2", "v2")
	xadd(t, env, "mystream", "3-0", "f3", "v3")
	xadd(t, env, "mystream", "4-0", "f4", "v4")
	xadd(t, env, "mystream", "5-0", "f5", "v5")

	result := execHandler(t, handleXDelEx, env, "mystream", "KEEPREF", "IDS", "2", "2-0", "4-0")
	if !strings.Contains(result, ":1") {
		t.Errorf("XDELEX should delete entries, got %q", result)
	}
	env.buf.Reset()

	result = execHandler(t, handleXLen, env, "mystream")
	if result != ":3\r\n" {
		t.Errorf("XLEN after XDELEX should be 3, got %q", result)
	}
	env.buf.Reset()

	cmd := &shared.Command{Args: [][]byte{[]byte("mystream")}, Runtime: env.eng, Context: env.ctx}
	valid, _, _ := handleXDelEx(cmd, env.w, nil)
	if valid {
		t.Fatal("expected invalid for wrong args")
	}
	if !strings.Contains(env.buf.String(), "ERR") {
		t.Errorf("expected error, got %q", env.buf.String())
	}
}

// --- XLEN wrong args ---

func TestHandleXLenWrongNumArgs(t *testing.T) {
	env := newTestEnv()

	cmd := &shared.Command{Args: [][]byte{}, Runtime: env.eng, Context: env.ctx}
	valid, _, _ := handleXLen(cmd, env.w, nil)
	if valid {
		t.Fatal("expected invalid")
	}
	if !strings.Contains(env.buf.String(), "ERR") {
		t.Errorf("expected error, got %q", env.buf.String())
	}
}
