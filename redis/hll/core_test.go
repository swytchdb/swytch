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

package hll

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

// runHandler calls a handler with a fresh effects engine, runs validation,
// then executes the runner if valid (and flushes the context).
func runHandler(handler shared.HandlerFunc, cmd *shared.Command) string {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	cmd.Runtime = eng
	cmd.Context = ctx

	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	valid, _, runner := handler(cmd, w, nil)
	if valid && runner != nil {
		runner()
		_ = ctx.Flush()
	}
	return buf.String()
}

// runHandlerWith runs a handler using the provided engine and context so
// state persists across multiple commands.
func runHandlerWith(eng *effects.Engine, ctx *effects.Context, handler shared.HandlerFunc, cmd *shared.Command) string {
	cmd.Runtime = eng
	cmd.Context = ctx

	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	valid, _, runner := handler(cmd, w, nil)
	if valid && runner != nil {
		runner()
		_ = ctx.Flush()
	}
	return buf.String()
}

func TestHLLAdd(t *testing.T) {
	hll := &shared.HLLValue{}

	if !hll.Add([]byte("hello")) {
		t.Error("First add should return true")
	}

	hll.Add([]byte("hello"))

	if !hll.Add([]byte("world")) {
		t.Error("Adding new element should return true")
	}
}

func TestHLLCount(t *testing.T) {
	hll := &shared.HLLValue{}

	if count := hll.Count(); count != 0 {
		t.Errorf("Empty HLL count should be 0, got %d", count)
	}

	for i := range 1000 {
		hll.Add(fmt.Appendf(nil, "element-%d", i))
	}

	count := hll.Count()
	if count < 900 || count > 1100 {
		t.Errorf("HLL count for 1000 elements should be ~1000, got %d", count)
	}
}

func TestHLLMerge(t *testing.T) {
	hll1 := &shared.HLLValue{}
	hll2 := &shared.HLLValue{}

	for i := range 500 {
		hll1.Add(fmt.Appendf(nil, "element-a-%d", i))
	}
	for i := range 500 {
		hll2.Add(fmt.Appendf(nil, "element-b-%d", i))
	}

	hll1.Merge(hll2)

	count := hll1.Count()
	if count < 900 || count > 1100 {
		t.Errorf("Merged HLL count should be ~1000, got %d", count)
	}
}

func TestHLLMergeWithOverlap(t *testing.T) {
	hll1 := &shared.HLLValue{}
	hll2 := &shared.HLLValue{}

	for i := range 1000 {
		hll1.Add(fmt.Appendf(nil, "element-%d", i))
		hll2.Add(fmt.Appendf(nil, "element-%d", i))
	}

	count1 := hll1.Count()
	count2 := hll2.Count()

	hll1.Merge(hll2)

	countMerged := hll1.Count()
	if countMerged < 900 || countMerged > 1100 {
		t.Errorf("Merged HLL with overlapping elements should be ~1000, got %d (before: %d, %d)", countMerged, count1, count2)
	}
}

func TestHLLLargeCardinality(t *testing.T) {
	hll := &shared.HLLValue{}

	numElements := 100000
	for i := range numElements {
		hll.Add(fmt.Appendf(nil, "element-%d", i))
	}

	count := hll.Count()
	minExpected := int64(float64(numElements) * 0.95)
	maxExpected := int64(float64(numElements) * 1.05)

	if count < minExpected || count > maxExpected {
		t.Errorf("HLL count for %d elements should be within 5%%, got %d (expected %d-%d)",
			numElements, count, minExpected, maxExpected)
	}
}

func TestPFAddCommand(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	cmd := &shared.Command{
		Type: shared.CmdPFAdd,
		Args: [][]byte{[]byte("hll_key"), []byte("a"), []byte("b"), []byte("c")},
	}
	resp := runHandlerWith(eng, ctx, handlePFAdd, cmd)
	if resp != ":1\r\n" {
		t.Errorf("Expected :1\\r\\n, got %q", resp)
	}

	// Adding same elements again should return 0
	cmd = &shared.Command{
		Type: shared.CmdPFAdd,
		Args: [][]byte{[]byte("hll_key"), []byte("a"), []byte("b"), []byte("c")},
	}
	resp = runHandlerWith(eng, ctx, handlePFAdd, cmd)
	if resp != ":0\r\n" {
		t.Errorf("Expected :0\\r\\n for duplicate add, got %q", resp)
	}
}

func TestPFCountCommand(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Add some elements
	cmd := &shared.Command{
		Type: shared.CmdPFAdd,
		Args: [][]byte{[]byte("hll_key"), []byte("a"), []byte("b"), []byte("c")},
	}
	runHandlerWith(eng, ctx, handlePFAdd, cmd)

	// Test PFCOUNT
	cmd = &shared.Command{
		Type: shared.CmdPFCount,
		Args: [][]byte{[]byte("hll_key")},
	}
	resp := runHandlerWith(eng, ctx, handlePFCount, cmd)
	if resp != ":3\r\n" {
		t.Errorf("Expected :3\\r\\n, got %q", resp)
	}
}

func TestPFCountNonExistent(t *testing.T) {
	resp := runHandler(handlePFCount, &shared.Command{
		Type: shared.CmdPFCount,
		Args: [][]byte{[]byte("nonexistent")},
	})
	if resp != ":0\r\n" {
		t.Errorf("Expected :0\\r\\n, got %q", resp)
	}
}

func TestPFMergeCommand(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Add elements to first HLL
	runHandlerWith(eng, ctx, handlePFAdd, &shared.Command{
		Type: shared.CmdPFAdd,
		Args: [][]byte{[]byte("hll1"), []byte("a"), []byte("b")},
	})

	// Add elements to second HLL
	runHandlerWith(eng, ctx, handlePFAdd, &shared.Command{
		Type: shared.CmdPFAdd,
		Args: [][]byte{[]byte("hll2"), []byte("c"), []byte("d")},
	})

	// Merge into destination
	resp := runHandlerWith(eng, ctx, handlePFMerge, &shared.Command{
		Type: shared.CmdPFMerge,
		Args: [][]byte{[]byte("dest"), []byte("hll1"), []byte("hll2")},
	})
	if resp != "+OK\r\n" {
		t.Errorf("Expected +OK\\r\\n, got %q", resp)
	}

	// Check count of merged HLL
	resp = runHandlerWith(eng, ctx, handlePFCount, &shared.Command{
		Type: shared.CmdPFCount,
		Args: [][]byte{[]byte("dest")},
	})
	if resp != ":4\r\n" {
		t.Errorf("Expected :4\\r\\n, got %q", resp)
	}
}

func TestPFCountMultipleKeys(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Add different elements to two HLLs
	runHandlerWith(eng, ctx, handlePFAdd, &shared.Command{
		Type: shared.CmdPFAdd,
		Args: [][]byte{[]byte("hll1"), []byte("a"), []byte("b")},
	})
	runHandlerWith(eng, ctx, handlePFAdd, &shared.Command{
		Type: shared.CmdPFAdd,
		Args: [][]byte{[]byte("hll2"), []byte("c"), []byte("d")},
	})

	// PFCOUNT with multiple keys should merge and count
	resp := runHandlerWith(eng, ctx, handlePFCount, &shared.Command{
		Type: shared.CmdPFCount,
		Args: [][]byte{[]byte("hll1"), []byte("hll2")},
	})
	if resp != ":4\r\n" {
		t.Errorf("Expected :4\\r\\n, got %q", resp)
	}
}

func TestPFAddEmptyCreatesKey(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// PFADD with no elements should create the key and return 1
	resp := runHandlerWith(eng, ctx, handlePFAdd, &shared.Command{
		Type: shared.CmdPFAdd,
		Args: [][]byte{[]byte("empty_hll")},
	})
	if resp != ":1\r\n" {
		t.Errorf("Expected :1\\r\\n for empty PFADD on new key, got %q", resp)
	}

	// PFCOUNT should return 0 for an empty HLL
	resp = runHandlerWith(eng, ctx, handlePFCount, &shared.Command{
		Type: shared.CmdPFCount,
		Args: [][]byte{[]byte("empty_hll")},
	})
	if resp != ":0\r\n" {
		t.Errorf("Expected :0\\r\\n for empty HLL, got %q", resp)
	}
}

func TestPFSelfTest(t *testing.T) {
	resp := runHandler(handlePFSelfTest, &shared.Command{
		Type: shared.CmdPFSelfTest,
		Args: [][]byte{},
	})
	if resp != "+OK\r\n" {
		t.Errorf("Expected +OK\\r\\n, got %q", resp)
	}
}

func TestPFDebugGetReg(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Add an element
	runHandlerWith(eng, ctx, handlePFAdd, &shared.Command{
		Type: shared.CmdPFAdd,
		Args: [][]byte{[]byte("dbg"), []byte("hello")},
	})

	// GETREG should return 16384 integers
	cmd := &shared.Command{
		Type: shared.CmdPFDebug,
		Args: [][]byte{[]byte("GETREG"), []byte("dbg")},
	}
	resp := runHandlerWith(eng, ctx, handlePFDebug, cmd)
	// Should start with *16384 (array of 16384 elements)
	if len(resp) < 6 || resp[:7] != "*16384\r" {
		t.Errorf("Expected array of 16384, got prefix %q", resp[:min(20, len(resp))])
	}
}

func TestPFDebugNonExistent(t *testing.T) {
	resp := runHandler(handlePFDebug, &shared.Command{
		Type: shared.CmdPFDebug,
		Args: [][]byte{[]byte("GETREG"), []byte("nosuchkey")},
	})
	if resp != "$-1\r\n" {
		t.Errorf("Expected null bulk string, got %q", resp)
	}
}

func TestHLLHashElement(t *testing.T) {
	// Verify hllHashElement produces the same results as HLLValue.Add
	hll := &shared.HLLValue{}
	elements := [][]byte{[]byte("hello"), []byte("world"), []byte("foo")}
	for _, elem := range elements {
		hll.Add(elem)
	}

	// Reconstruct from hllHashElement
	hll2 := &shared.HLLValue{}
	for _, elem := range elements {
		regIdx, rho := hllHashElement(elem)
		if rho > hll2.Registers[regIdx] {
			hll2.Registers[regIdx] = rho
		}
	}

	// Both should produce identical registers
	if hll.Registers != hll2.Registers {
		t.Error("hllHashElement should produce same registers as HLLValue.Add")
	}
}
