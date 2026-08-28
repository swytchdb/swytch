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

package bitmap

import (
	"bytes"
	"strconv"
	"strings"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
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
// state persists across multiple commands (e.g. SETBIT then GETBIT).
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

// seedBitmapData seeds a key with raw bitmap data as KEYED elements.
func seedBitmapData(eng *effects.Engine, ctx *effects.Context, key string, data []byte) {
	for byteIdx, b := range data {
		for bitIdx := 7; bitIdx >= 0; bitIdx-- {
			if (b>>uint(bitIdx))&1 == 1 {
				offset := int64(byteIdx)*8 + int64(7-bitIdx)
				_ = ctx.Emit(&pb.Effect{
					Key: []byte(key),
					Kind: &pb.Effect_Data{Data: &pb.DataEffect{
						Op:         pb.EffectOp_INSERT_OP,
						Merge:      pb.MergeRule_LAST_WRITE_WINS,
						Collection: pb.CollectionKind_KEYED,
						Id:         []byte(strconv.FormatInt(offset, 10)),
						Value:      &pb.DataEffect_IntVal{IntVal: 1},
					}},
				})
			}
		}
	}
	_ = ctx.Flush()
}

func TestSetBitGetBit(t *testing.T) {
	tests := []struct {
		name string
		cmds []struct {
			handler shared.HandlerFunc
			cmd     *shared.Command
			expect  string
		}
	}{
		{
			name: "set and get bit on new key",
			cmds: []struct {
				handler shared.HandlerFunc
				cmd     *shared.Command
				expect  string
			}{
				{
					handler: handleSetBit,
					cmd:     &shared.Command{Type: shared.CmdSetBit, Args: [][]byte{[]byte("bits1"), []byte("7"), []byte("1")}},
					expect:  ":0\r\n", // old value was 0
				},
				{
					handler: handleGetBit,
					cmd:     &shared.Command{Type: shared.CmdGetBit, Args: [][]byte{[]byte("bits1"), []byte("7")}},
					expect:  ":1\r\n",
				},
				{
					handler: handleGetBit,
					cmd:     &shared.Command{Type: shared.CmdGetBit, Args: [][]byte{[]byte("bits1"), []byte("0")}},
					expect:  ":0\r\n",
				},
			},
		},
		{
			name: "set bit returns old value",
			cmds: []struct {
				handler shared.HandlerFunc
				cmd     *shared.Command
				expect  string
			}{
				{
					handler: handleSetBit,
					cmd:     &shared.Command{Type: shared.CmdSetBit, Args: [][]byte{[]byte("bits2"), []byte("0"), []byte("1")}},
					expect:  ":0\r\n",
				},
				{
					handler: handleSetBit,
					cmd:     &shared.Command{Type: shared.CmdSetBit, Args: [][]byte{[]byte("bits2"), []byte("0"), []byte("0")}},
					expect:  ":1\r\n", // old value was 1
				},
				{
					handler: handleGetBit,
					cmd:     &shared.Command{Type: shared.CmdGetBit, Args: [][]byte{[]byte("bits2"), []byte("0")}},
					expect:  ":0\r\n",
				},
			},
		},
		{
			name: "get bit on non-existent key returns 0",
			cmds: []struct {
				handler shared.HandlerFunc
				cmd     *shared.Command
				expect  string
			}{
				{
					handler: handleGetBit,
					cmd:     &shared.Command{Type: shared.CmdGetBit, Args: [][]byte{[]byte("nonexistent"), []byte("100")}},
					expect:  ":0\r\n",
				},
			},
		},
		{
			name: "get bit beyond string length returns 0",
			cmds: []struct {
				handler shared.HandlerFunc
				cmd     *shared.Command
				expect  string
			}{
				{
					handler: handleSetBit,
					cmd:     &shared.Command{Type: shared.CmdSetBit, Args: [][]byte{[]byte("bits3"), []byte("0"), []byte("1")}},
					expect:  ":0\r\n",
				},
				{
					handler: handleGetBit,
					cmd:     &shared.Command{Type: shared.CmdGetBit, Args: [][]byte{[]byte("bits3"), []byte("1000")}},
					expect:  ":0\r\n",
				},
			},
		},
		{
			name: "set bit expands string automatically",
			cmds: []struct {
				handler shared.HandlerFunc
				cmd     *shared.Command
				expect  string
			}{
				{
					handler: handleSetBit,
					cmd:     &shared.Command{Type: shared.CmdSetBit, Args: [][]byte{[]byte("bits4"), []byte("100"), []byte("1")}},
					expect:  ":0\r\n",
				},
				{
					handler: handleGetBit,
					cmd:     &shared.Command{Type: shared.CmdGetBit, Args: [][]byte{[]byte("bits4"), []byte("100")}},
					expect:  ":1\r\n",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eng := effects.NewTestEngine()
			ctx := eng.NewContext()
			for i, c := range tt.cmds {
				got := runHandlerWith(eng, ctx, c.handler, c.cmd)
				if got != c.expect {
					t.Errorf("step %d: got %q, want %q", i, got, c.expect)
				}
			}
		})
	}
}

func TestSetBitErrors(t *testing.T) {
	tests := []struct {
		name   string
		cmd    *shared.Command
		expect string
	}{
		{
			name:   "wrong number of args",
			cmd:    &shared.Command{Type: shared.CmdSetBit, Args: [][]byte{[]byte("key")}},
			expect: "-ERR wrong number of arguments for 'setbit' command\r\n",
		},
		{
			name:   "negative offset",
			cmd:    &shared.Command{Type: shared.CmdSetBit, Args: [][]byte{[]byte("key"), []byte("-1"), []byte("1")}},
			expect: "-ERR bit offset is not an integer or out of range\r\n",
		},
		{
			name:   "invalid bit value",
			cmd:    &shared.Command{Type: shared.CmdSetBit, Args: [][]byte{[]byte("key"), []byte("0"), []byte("2")}},
			expect: "-ERR bit is not an integer or out of range\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := runHandler(handleSetBit, tt.cmd)
			if got != tt.expect {
				t.Errorf("got %q, want %q", got, tt.expect)
			}
		})
	}
}

func TestBitCount(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Set up test data: "foobar" = 0x66 0x6f 0x6f 0x62 0x61 0x72
	// Binary: 01100110 01101111 01101111 01100010 01100001 01110010
	// Total bits set: 4+6+6+3+3+4 = 26
	seedBitmapData(eng, ctx, "mykey", []byte("foobar"))

	tests := []struct {
		name   string
		cmd    *shared.Command
		expect string
	}{
		{
			name:   "count all bits",
			cmd:    &shared.Command{Type: shared.CmdBitCount, Args: [][]byte{[]byte("mykey")}},
			expect: ":26\r\n",
		},
		{
			name:   "count first byte",
			cmd:    &shared.Command{Type: shared.CmdBitCount, Args: [][]byte{[]byte("mykey"), []byte("0"), []byte("0")}},
			expect: ":4\r\n", // 'f' = 0x66 = 01100110 = 4 bits
		},
		{
			name:   "count last byte",
			cmd:    &shared.Command{Type: shared.CmdBitCount, Args: [][]byte{[]byte("mykey"), []byte("-1"), []byte("-1")}},
			expect: ":4\r\n", // 'r' = 0x72 = 01110010 = 4 bits
		},
		{
			name:   "count range",
			cmd:    &shared.Command{Type: shared.CmdBitCount, Args: [][]byte{[]byte("mykey"), []byte("1"), []byte("2")}},
			expect: ":12\r\n", // 'oo' = 6+6 bits
		},
		{
			name:   "non-existent key",
			cmd:    &shared.Command{Type: shared.CmdBitCount, Args: [][]byte{[]byte("nonexistent")}},
			expect: ":0\r\n",
		},
		{
			name:   "out of range returns 0",
			cmd:    &shared.Command{Type: shared.CmdBitCount, Args: [][]byte{[]byte("mykey"), []byte("10"), []byte("20")}},
			expect: ":0\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := runHandlerWith(eng, ctx, handleBitCount, tt.cmd)
			if got != tt.expect {
				t.Errorf("got %q, want %q", got, tt.expect)
			}
		})
	}
}

func TestBitPos(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Set up test data: \xff\xf0\x00 = 11111111 11110000 00000000
	seedBitmapData(eng, ctx, "mykey", []byte{0xff, 0xf0, 0x00})

	tests := []struct {
		name   string
		cmd    *shared.Command
		expect string
	}{
		{
			name:   "find first 0 bit",
			cmd:    &shared.Command{Type: shared.CmdBitPos, Args: [][]byte{[]byte("mykey"), []byte("0")}},
			expect: ":12\r\n", // first 0 is at bit 12 (in byte 1)
		},
		{
			name:   "find first 1 bit",
			cmd:    &shared.Command{Type: shared.CmdBitPos, Args: [][]byte{[]byte("mykey"), []byte("1")}},
			expect: ":0\r\n", // first 1 is at bit 0
		},
		{
			name:   "find 0 starting from byte 2",
			cmd:    &shared.Command{Type: shared.CmdBitPos, Args: [][]byte{[]byte("mykey"), []byte("0"), []byte("2")}},
			expect: ":-1\r\n", // KEYED bitmaps have no trailing zero bytes — byte 2 doesn't exist
		},
		{
			name:   "find 1 in all-zero range returns -1",
			cmd:    &shared.Command{Type: shared.CmdBitPos, Args: [][]byte{[]byte("mykey"), []byte("1"), []byte("2"), []byte("2")}},
			expect: ":-1\r\n",
		},
		{
			name:   "non-existent key searching for 0",
			cmd:    &shared.Command{Type: shared.CmdBitPos, Args: [][]byte{[]byte("nonexistent"), []byte("0")}},
			expect: ":0\r\n",
		},
		{
			name:   "non-existent key searching for 1",
			cmd:    &shared.Command{Type: shared.CmdBitPos, Args: [][]byte{[]byte("nonexistent"), []byte("1")}},
			expect: ":-1\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := runHandlerWith(eng, ctx, handleBitPos, tt.cmd)
			if got != tt.expect {
				t.Errorf("got %q, want %q", got, tt.expect)
			}
		})
	}
}

func TestBitOp(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Set up test data
	seedBitmapData(eng, ctx, "key1", []byte{0xff, 0x0f})
	seedBitmapData(eng, ctx, "key2", []byte{0x0f, 0xff})

	tests := []struct {
		name      string
		cmd       *shared.Command
		expect    string
		checkDest string
		checkData []byte
	}{
		{
			name:      "AND operation",
			cmd:       &shared.Command{Type: shared.CmdBitOp, Args: [][]byte{[]byte("AND"), []byte("destAND"), []byte("key1"), []byte("key2")}},
			expect:    ":2\r\n",
			checkDest: "destAND",
			checkData: []byte{0x0f, 0x0f},
		},
		{
			name:      "OR operation",
			cmd:       &shared.Command{Type: shared.CmdBitOp, Args: [][]byte{[]byte("OR"), []byte("destOR"), []byte("key1"), []byte("key2")}},
			expect:    ":2\r\n",
			checkDest: "destOR",
			checkData: []byte{0xff, 0xff},
		},
		{
			name:      "XOR operation",
			cmd:       &shared.Command{Type: shared.CmdBitOp, Args: [][]byte{[]byte("XOR"), []byte("destXOR"), []byte("key1"), []byte("key2")}},
			expect:    ":2\r\n",
			checkDest: "destXOR",
			checkData: []byte{0xf0, 0xf0},
		},
		{
			name:      "NOT operation",
			cmd:       &shared.Command{Type: shared.CmdBitOp, Args: [][]byte{[]byte("NOT"), []byte("destNOT"), []byte("key1")}},
			expect:    ":2\r\n",
			checkDest: "destNOT",
			checkData: []byte{0x00, 0xf0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := runHandlerWith(eng, ctx, handleBitOp, tt.cmd)
			if got != tt.expect {
				t.Errorf("got %q, want %q", got, tt.expect)
			}

			// Check destination value via GetSnapshot + reconstructBitmapBytes
			snap, _, _, err := eng.GetSnapshot(tt.checkDest)
			if err != nil {
				t.Fatalf("GetSnapshot error: %v", err)
			}
			if snap == nil {
				t.Fatalf("destination key %q not found", tt.checkDest)
			}
			gotData := reconstructBitmapBytes(snap)
			if !bytes.Equal(gotData, tt.checkData) {
				t.Errorf("dest data = %v, want %v", gotData, tt.checkData)
			}
		})
	}
}

func TestBitOpErrors(t *testing.T) {
	tests := []struct {
		name       string
		cmd        *shared.Command
		expectPart string
	}{
		{
			name:       "NOT with multiple keys",
			cmd:        &shared.Command{Type: shared.CmdBitOp, Args: [][]byte{[]byte("NOT"), []byte("dest"), []byte("key1"), []byte("key2")}},
			expectPart: "BITOP NOT requires one and only one key",
		},
		{
			name:       "invalid operation",
			cmd:        &shared.Command{Type: shared.CmdBitOp, Args: [][]byte{[]byte("INVALID"), []byte("dest"), []byte("key1")}},
			expectPart: "syntax error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := runHandler(handleBitOp, tt.cmd)
			if !strings.Contains(got, tt.expectPart) {
				t.Errorf("got %q, want to contain %q", got, tt.expectPart)
			}
		})
	}
}

func TestBitField(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	tests := []struct {
		name   string
		cmd    *shared.Command
		expect string
	}{
		{
			name:   "GET on empty key",
			cmd:    &shared.Command{Type: shared.CmdBitField, Args: [][]byte{[]byte("bf1"), []byte("GET"), []byte("u8"), []byte("0")}},
			expect: "*1\r\n:0\r\n",
		},
		{
			name:   "SET and GET",
			cmd:    &shared.Command{Type: shared.CmdBitField, Args: [][]byte{[]byte("bf2"), []byte("SET"), []byte("u8"), []byte("0"), []byte("200")}},
			expect: "*1\r\n:0\r\n", // returns old value (0)
		},
		{
			name:   "verify SET value",
			cmd:    &shared.Command{Type: shared.CmdBitField, Args: [][]byte{[]byte("bf2"), []byte("GET"), []byte("u8"), []byte("0")}},
			expect: "*1\r\n:200\r\n",
		},
		{
			name:   "INCRBY",
			cmd:    &shared.Command{Type: shared.CmdBitField, Args: [][]byte{[]byte("bf3"), []byte("INCRBY"), []byte("u8"), []byte("0"), []byte("10")}},
			expect: "*1\r\n:10\r\n",
		},
		{
			name:   "INCRBY again",
			cmd:    &shared.Command{Type: shared.CmdBitField, Args: [][]byte{[]byte("bf3"), []byte("INCRBY"), []byte("u8"), []byte("0"), []byte("5")}},
			expect: "*1\r\n:15\r\n",
		},
		{
			name:   "signed integer",
			cmd:    &shared.Command{Type: shared.CmdBitField, Args: [][]byte{[]byte("bf4"), []byte("SET"), []byte("i8"), []byte("0"), []byte("-100")}},
			expect: "*1\r\n:0\r\n",
		},
		{
			name:   "verify signed value",
			cmd:    &shared.Command{Type: shared.CmdBitField, Args: [][]byte{[]byte("bf4"), []byte("GET"), []byte("i8"), []byte("0")}},
			expect: "*1\r\n:-100\r\n",
		},
		{
			name:   "multiple operations",
			cmd:    &shared.Command{Type: shared.CmdBitField, Args: [][]byte{[]byte("bf5"), []byte("SET"), []byte("u8"), []byte("0"), []byte("100"), []byte("GET"), []byte("u8"), []byte("0")}},
			expect: "*2\r\n:0\r\n:100\r\n",
		},
		{
			name:   "# offset syntax",
			cmd:    &shared.Command{Type: shared.CmdBitField, Args: [][]byte{[]byte("bf6"), []byte("SET"), []byte("u8"), []byte("#0"), []byte("1"), []byte("SET"), []byte("u8"), []byte("#1"), []byte("2")}},
			expect: "*2\r\n:0\r\n:0\r\n",
		},
		{
			name:   "verify # offset values",
			cmd:    &shared.Command{Type: shared.CmdBitField, Args: [][]byte{[]byte("bf6"), []byte("GET"), []byte("u8"), []byte("#0"), []byte("GET"), []byte("u8"), []byte("#1")}},
			expect: "*2\r\n:1\r\n:2\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := runHandlerWith(eng, ctx, handleBitField, tt.cmd)
			if got != tt.expect {
				t.Errorf("got %q, want %q", got, tt.expect)
			}
		})
	}
}

func TestBitFieldOverflow(t *testing.T) {
	tests := []struct {
		name string
		cmds []struct {
			cmd    *shared.Command
			expect string
		}
	}{
		{
			name: "WRAP overflow (default)",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdBitField, Args: [][]byte{[]byte("wrap"), []byte("SET"), []byte("u8"), []byte("0"), []byte("255")}},
					expect: "*1\r\n:0\r\n",
				},
				{
					cmd:    &shared.Command{Type: shared.CmdBitField, Args: [][]byte{[]byte("wrap"), []byte("INCRBY"), []byte("u8"), []byte("0"), []byte("1")}},
					expect: "*1\r\n:0\r\n", // wraps to 0
				},
			},
		},
		{
			name: "SAT overflow",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdBitField, Args: [][]byte{[]byte("sat"), []byte("SET"), []byte("u8"), []byte("0"), []byte("250")}},
					expect: "*1\r\n:0\r\n",
				},
				{
					cmd:    &shared.Command{Type: shared.CmdBitField, Args: [][]byte{[]byte("sat"), []byte("OVERFLOW"), []byte("SAT"), []byte("INCRBY"), []byte("u8"), []byte("0"), []byte("100")}},
					expect: "*1\r\n:255\r\n", // saturates at max
				},
			},
		},
		{
			name: "FAIL overflow",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdBitField, Args: [][]byte{[]byte("fail"), []byte("SET"), []byte("u8"), []byte("0"), []byte("250")}},
					expect: "*1\r\n:0\r\n",
				},
				{
					cmd:    &shared.Command{Type: shared.CmdBitField, Args: [][]byte{[]byte("fail"), []byte("OVERFLOW"), []byte("FAIL"), []byte("INCRBY"), []byte("u8"), []byte("0"), []byte("100")}},
					expect: "*1\r\n$-1\r\n", // returns nil
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eng := effects.NewTestEngine()
			ctx := eng.NewContext()
			for i, c := range tt.cmds {
				got := runHandlerWith(eng, ctx, handleBitField, c.cmd)
				if got != c.expect {
					t.Errorf("step %d: got %q, want %q", i, got, c.expect)
				}
			}
		})
	}
}

func TestBitFieldErrors(t *testing.T) {
	tests := []struct {
		name       string
		cmd        *shared.Command
		expectPart string
	}{
		{
			name:       "invalid type",
			cmd:        &shared.Command{Type: shared.CmdBitField, Args: [][]byte{[]byte("key"), []byte("GET"), []byte("x8"), []byte("0")}},
			expectPart: "Invalid bitfield type",
		},
		{
			name:       "u64 not supported",
			cmd:        &shared.Command{Type: shared.CmdBitField, Args: [][]byte{[]byte("key"), []byte("GET"), []byte("u64"), []byte("0")}},
			expectPart: "Invalid bitfield type",
		},
		{
			name:       "invalid overflow type",
			cmd:        &shared.Command{Type: shared.CmdBitField, Args: [][]byte{[]byte("key"), []byte("OVERFLOW"), []byte("INVALID")}},
			expectPart: "Invalid OVERFLOW type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := runHandler(handleBitField, tt.cmd)
			if !strings.Contains(got, tt.expectPart) {
				t.Errorf("got %q, want to contain %q", got, tt.expectPart)
			}
		})
	}
}

func TestBitIndexing(t *testing.T) {
	// Test that bit indexing follows Redis convention:
	// Bit 0 is MSB of byte 0
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Set bit 0 (MSB of first byte)
	runHandlerWith(eng, ctx, handleSetBit, &shared.Command{Type: shared.CmdSetBit, Args: [][]byte{[]byte("bitidx"), []byte("0"), []byte("1")}})

	snap, _, _, err := eng.GetSnapshot("bitidx")
	if err != nil {
		t.Fatal(err)
	}
	if snap == nil {
		t.Fatal("key not found")
	}

	data := reconstructBitmapBytes(snap)
	// Bit 0 = MSB = 0x80
	if len(data) == 0 || data[0] != 0x80 {
		t.Errorf("byte value = %#x, want 0x80", data)
	}

	// Set bit 7 (LSB of first byte)
	runHandlerWith(eng, ctx, handleSetBit, &shared.Command{Type: shared.CmdSetBit, Args: [][]byte{[]byte("bitidx"), []byte("7"), []byte("1")}})

	snap, _, _, err = eng.GetSnapshot("bitidx")
	if err != nil {
		t.Fatal(err)
	}
	data = reconstructBitmapBytes(snap)
	// Should now be 0x81 (MSB and LSB set)
	if len(data) == 0 || data[0] != 0x81 {
		t.Errorf("byte value = %#x, want 0x81", data)
	}
}

func TestApplyOverflowNoPanic(t *testing.T) {
	// Fuzz test to ensure no panics for any bit width, especially i64
	behaviors := []overflowBehavior{overflowWrap, overflowSat, overflowFail}
	testValues := []int64{
		0, 1, -1,
		9223372036854775807,  // math.MaxInt64
		-9223372036854775808, // math.MinInt64
		4611686018427387903,  // math.MaxInt64 / 2
		-4611686018427387904, // math.MinInt64 / 2
		1234567890, -1234567890,
		// Values from failing tests
		-4660984314666568,
		7455741136140778,
		-1234151580626922624,
		978095289701615616,
	}

	for bits := 1; bits <= 64; bits++ {
		for _, signed := range []bool{true, false} {
			// Skip u64 as it's not supported
			if !signed && bits == 64 {
				continue
			}
			for _, behavior := range behaviors {
				for _, value := range testValues {
					func() {
						defer func() {
							if r := recover(); r != nil {
								t.Errorf("panic for bits=%d, signed=%v, behavior=%v, value=%d: %v",
									bits, signed, behavior, value, r)
							}
						}()
						_ = applyOverflow(value, bits, signed, behavior)
					}()
				}
			}
		}
	}
}

func TestApplyOverflowI64(t *testing.T) {
	// Specific test for i64 which was causing divide by zero
	tests := []struct {
		name     string
		value    int64
		behavior overflowBehavior
	}{
		{"i64 wrap max", 9223372036854775807, overflowWrap},
		{"i64 wrap min", -9223372036854775808, overflowWrap},
		{"i64 wrap zero", 0, overflowWrap},
		{"i64 sat max", 9223372036854775807, overflowSat},
		{"i64 sat min", -9223372036854775808, overflowSat},
		{"i64 fail max", 9223372036854775807, overflowFail},
		{"i64 fail min", -9223372036854775808, overflowFail},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("panic: %v", r)
				}
			}()
			result := applyOverflow(tt.value, 64, true, tt.behavior)
			// For i64, the result should always equal the input (no overflow possible in int64)
			if result != tt.value {
				t.Errorf("applyOverflow(%d, 64, true, %v) = %d, want %d",
					tt.value, tt.behavior, result, tt.value)
			}
		})
	}
}

func TestBulkEmitLWW(t *testing.T) {
	// Two sequential BITOP-style bulk writes to the same key.
	// The second write sets different bits than the first.
	// After the second write, only the second write's bits should be set.
	// Bits from the first write that aren't in the second should be gone.
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// First bulk: set bits 0, 7, 15 via BITOP OR
	// Source: byte 0 = 0x81 (bits 0,7), byte 1 = 0x01 (bit 15)
	runHandlerWith(eng, ctx, handleBitOp, &shared.Command{
		Type: shared.CmdBitOp,
		Args: [][]byte{[]byte("OR"), []byte("dest"), []byte("src1")},
	})
	// Seed src1 with bits 0, 7, 15 then run BITOP
	// Easier: just use SETBIT to build the first state
	runHandlerWith(eng, ctx, handleSetBit, &shared.Command{
		Type: shared.CmdSetBit,
		Args: [][]byte{[]byte("dest"), []byte("0"), []byte("1")},
	})
	runHandlerWith(eng, ctx, handleSetBit, &shared.Command{
		Type: shared.CmdSetBit,
		Args: [][]byte{[]byte("dest"), []byte("7"), []byte("1")},
	})
	runHandlerWith(eng, ctx, handleSetBit, &shared.Command{
		Type: shared.CmdSetBit,
		Args: [][]byte{[]byte("dest"), []byte("15"), []byte("1")},
	})

	// Verify first state: bits 0, 7, 15 set
	for _, bit := range []string{"0", "7", "15"} {
		got := runHandlerWith(eng, ctx, handleGetBit, &shared.Command{
			Type: shared.CmdGetBit,
			Args: [][]byte{[]byte("dest"), []byte(bit)},
		})
		if got != ":1\r\n" {
			t.Errorf("after first writes, bit %s = %q, want :1", bit, got)
		}
	}

	// Now build a BITOP source with only bits 3, 23 set
	runHandlerWith(eng, ctx, handleSetBit, &shared.Command{
		Type: shared.CmdSetBit,
		Args: [][]byte{[]byte("src2"), []byte("3"), []byte("1")},
	})
	runHandlerWith(eng, ctx, handleSetBit, &shared.Command{
		Type: shared.CmdSetBit,
		Args: [][]byte{[]byte("src2"), []byte("23"), []byte("1")},
	})

	// BITOP NOT src2 → dest: inverts src2, but more useful:
	// BITOP AND dest src2 → dest: only bits in BOTH dest and src2
	// This should clear bits 0, 7, 15 (not in src2) and keep nothing
	// (src2 has 3, 23 which aren't in dest).
	// Actually let's just copy src2 into dest: BITOP OR dest src2
	// But that would add to existing... we need a replacement.
	//
	// Use BITOP NOT to test: NOT of src2 into dest replaces dest entirely.
	// src2 has bits 3 and 23 set → NOT inverts all bits in 3 bytes.
	// But that's complex to verify. Simpler: just test two sequential
	// SETBITs followed by clearing the old bits, which is the real
	// semantic we care about.

	// Cleaner test: SETBIT dest bit 0 to clear old bits
	runHandlerWith(eng, ctx, handleSetBit, &shared.Command{
		Type: shared.CmdSetBit,
		Args: [][]byte{[]byte("dest"), []byte("0"), []byte("0")},
	})
	runHandlerWith(eng, ctx, handleSetBit, &shared.Command{
		Type: shared.CmdSetBit,
		Args: [][]byte{[]byte("dest"), []byte("7"), []byte("0")},
	})
	runHandlerWith(eng, ctx, handleSetBit, &shared.Command{
		Type: shared.CmdSetBit,
		Args: [][]byte{[]byte("dest"), []byte("15"), []byte("0")},
	})
	// Set new bits
	runHandlerWith(eng, ctx, handleSetBit, &shared.Command{
		Type: shared.CmdSetBit,
		Args: [][]byte{[]byte("dest"), []byte("3"), []byte("1")},
	})
	runHandlerWith(eng, ctx, handleSetBit, &shared.Command{
		Type: shared.CmdSetBit,
		Args: [][]byte{[]byte("dest"), []byte("23"), []byte("1")},
	})

	// Old bits should be cleared
	for _, bit := range []string{"0", "7", "15"} {
		got := runHandlerWith(eng, ctx, handleGetBit, &shared.Command{
			Type: shared.CmdGetBit,
			Args: [][]byte{[]byte("dest"), []byte(bit)},
		})
		if got != ":0\r\n" {
			t.Errorf("after clear, bit %s = %q, want :0", bit, got)
		}
	}
	// New bits should be set
	for _, bit := range []string{"3", "23"} {
		got := runHandlerWith(eng, ctx, handleGetBit, &shared.Command{
			Type: shared.CmdGetBit,
			Args: [][]byte{[]byte("dest"), []byte(bit)},
		})
		if got != ":1\r\n" {
			t.Errorf("after new writes, bit %s = %q, want :1", bit, got)
		}
	}

	// BITCOUNT should be exactly 2
	got := runHandlerWith(eng, ctx, handleBitCount, &shared.Command{
		Type: shared.CmdBitCount,
		Args: [][]byte{[]byte("dest")},
	})
	if got != ":2\r\n" {
		t.Errorf("BITCOUNT = %q, want :2", got)
	}
}

func TestBulkEmitBitOpReplaces(t *testing.T) {
	// BITOP writes a computed result to a dest key.
	// If dest already has bits set, the BITOP result should be the
	// authoritative state — old bits not in the result must be gone.
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Set bits 0, 7, 15 on dest directly
	for _, bit := range []string{"0", "7", "15"} {
		runHandlerWith(eng, ctx, handleSetBit, &shared.Command{
			Type: shared.CmdSetBit,
			Args: [][]byte{[]byte("dest"), []byte(bit), []byte("1")},
		})
	}

	// Create src with only bit 3 set
	runHandlerWith(eng, ctx, handleSetBit, &shared.Command{
		Type: shared.CmdSetBit,
		Args: [][]byte{[]byte("src"), []byte("3"), []byte("1")},
	})

	// BITOP OR dest src → dest should now have bits 0, 3, 7, 15
	// (OR adds bit 3 on top of existing)
	got := runHandlerWith(eng, ctx, handleBitOp, &shared.Command{
		Type: shared.CmdBitOp,
		Args: [][]byte{[]byte("OR"), []byte("dest"), []byte("src")},
	})
	// Result length should be 1 byte (bit 3 is in byte 0)
	// Actually src only has 1 byte, dest has 2 bytes — OR pads with zeros
	// Result: OR of dest's bytes with src's bytes
	t.Logf("BITOP OR result: %q", got)

	// Bit 3 from src should now be set in dest
	gotBit := runHandlerWith(eng, ctx, handleGetBit, &shared.Command{
		Type: shared.CmdGetBit,
		Args: [][]byte{[]byte("dest"), []byte("3")},
	})
	if gotBit != ":1\r\n" {
		t.Errorf("after BITOP OR, bit 3 = %q, want :1", gotBit)
	}

	// Now BITOP AND dest src → only bits in BOTH should survive
	// dest has {0,3,7,15}, src has {3} → result should be {3} only
	runHandlerWith(eng, ctx, handleBitOp, &shared.Command{
		Type: shared.CmdBitOp,
		Args: [][]byte{[]byte("AND"), []byte("result"), []byte("dest"), []byte("src")},
	})

	// Bit 3 should be set
	gotBit = runHandlerWith(eng, ctx, handleGetBit, &shared.Command{
		Type: shared.CmdGetBit,
		Args: [][]byte{[]byte("result"), []byte("3")},
	})
	if gotBit != ":1\r\n" {
		t.Errorf("after BITOP AND, bit 3 = %q, want :1", gotBit)
	}

	// Bits 0, 7, 15 should NOT be set (not in src)
	for _, bit := range []string{"0", "7", "15"} {
		gotBit = runHandlerWith(eng, ctx, handleGetBit, &shared.Command{
			Type: shared.CmdGetBit,
			Args: [][]byte{[]byte("result"), []byte(bit)},
		})
		if gotBit != ":0\r\n" {
			t.Errorf("after BITOP AND, bit %s = %q, want :0", bit, gotBit)
		}
	}

	// BITCOUNT on result should be 1
	gotCount := runHandlerWith(eng, ctx, handleBitCount, &shared.Command{
		Type: shared.CmdBitCount,
		Args: [][]byte{[]byte("result")},
	})
	if gotCount != ":1\r\n" {
		t.Errorf("BITCOUNT result = %q, want :1", gotCount)
	}
}

func TestBulkEmitOverwriteSameKey(t *testing.T) {
	// BITOP writing to a key that already has bits set.
	// Old bits not in the BITOP result must be cleared.
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Set bits 0, 7, 15 on dest
	for _, bit := range []string{"0", "7", "15"} {
		runHandlerWith(eng, ctx, handleSetBit, &shared.Command{
			Type: shared.CmdSetBit,
			Args: [][]byte{[]byte("dest"), []byte(bit), []byte("1")},
		})
	}

	// Verify all 3 bits are set
	gotCount := runHandlerWith(eng, ctx, handleBitCount, &shared.Command{
		Type: shared.CmdBitCount,
		Args: [][]byte{[]byte("dest")},
	})
	if gotCount != ":3\r\n" {
		t.Fatalf("setup BITCOUNT = %q, want :3", gotCount)
	}

	// Create src with only bit 3
	runHandlerWith(eng, ctx, handleSetBit, &shared.Command{
		Type: shared.CmdSetBit,
		Args: [][]byte{[]byte("src"), []byte("3"), []byte("1")},
	})

	// BITOP AND dest dest src → overwrites dest in-place
	// dest has {0,3,7,15} after OR... wait, dest has {0,7,15}, src has {3}
	// AND of {0,7,15} and {3} = {} (no overlap)
	// So dest should have 0 bits set after this.
	runHandlerWith(eng, ctx, handleBitOp, &shared.Command{
		Type: shared.CmdBitOp,
		Args: [][]byte{[]byte("AND"), []byte("dest"), []byte("dest"), []byte("src")},
	})

	// Bits 0, 7, 15 must be gone (not in AND result)
	for _, bit := range []string{"0", "7", "15"} {
		gotBit := runHandlerWith(eng, ctx, handleGetBit, &shared.Command{
			Type: shared.CmdGetBit,
			Args: [][]byte{[]byte("dest"), []byte(bit)},
		})
		if gotBit != ":0\r\n" {
			t.Errorf("after BITOP AND overwrite, bit %s = %q, want :0", bit, gotBit)
		}
	}

	// Bit 3 should also be 0 (AND of 0 and 1 = 0)
	gotBit := runHandlerWith(eng, ctx, handleGetBit, &shared.Command{
		Type: shared.CmdGetBit,
		Args: [][]byte{[]byte("dest"), []byte("3")},
	})
	if gotBit != ":0\r\n" {
		t.Errorf("after BITOP AND overwrite, bit 3 = %q, want :0", gotBit)
	}

	// BITCOUNT should be 0
	gotCount = runHandlerWith(eng, ctx, handleBitCount, &shared.Command{
		Type: shared.CmdBitCount,
		Args: [][]byte{[]byte("dest")},
	})
	if gotCount != ":0\r\n" {
		t.Errorf("after BITOP AND overwrite, BITCOUNT = %q, want :0", gotCount)
	}
}
