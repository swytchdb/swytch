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

package zset

import (
	"bytes"
	"strings"
	"testing"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

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

// --- ZADD tests ---

func TestHandleZAdd(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	cmd := &shared.Command{Args: [][]byte{[]byte("myzset"), []byte("1.0"), []byte("a"), []byte("2.0"), []byte("b"), []byte("3.0"), []byte("c")}}
	resp := runHandlerWith(eng, ctx, handleZAdd, cmd)
	if resp != ":3\r\n" {
		t.Fatalf("expected :3\\r\\n, got %q", resp)
	}

	// Add duplicate + new
	cmd = &shared.Command{Args: [][]byte{[]byte("myzset"), []byte("1.5"), []byte("a"), []byte("4.0"), []byte("d")}}
	resp = runHandlerWith(eng, ctx, handleZAdd, cmd)
	if resp != ":1\r\n" {
		t.Fatalf("expected :1\\r\\n (only d is new), got %q", resp)
	}
}

func TestHandleZAdd_NX(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Add initial member
	runHandlerWith(eng, ctx, handleZAdd, &shared.Command{
		Args: [][]byte{[]byte("myzset"), []byte("1.0"), []byte("a")},
	})

	// NX: should not update existing, should add new
	cmd := &shared.Command{Args: [][]byte{[]byte("myzset"), []byte("NX"), []byte("2.0"), []byte("a"), []byte("3.0"), []byte("b")}}
	resp := runHandlerWith(eng, ctx, handleZAdd, cmd)
	if resp != ":1\r\n" {
		t.Fatalf("expected :1\\r\\n (only b added), got %q", resp)
	}

	// Verify a still has score 1.0
	cmd = &shared.Command{Args: [][]byte{[]byte("myzset"), []byte("a")}}
	resp = runHandlerWith(eng, ctx, handleZScore, cmd)
	if !strings.Contains(resp, "1") {
		t.Fatalf("expected score 1 for member a, got %q", resp)
	}
}

func TestHandleZAdd_XX(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// XX on non-existing key returns 0
	cmd := &shared.Command{Args: [][]byte{[]byte("myzset"), []byte("XX"), []byte("1.0"), []byte("a")}}
	resp := runHandlerWith(eng, ctx, handleZAdd, cmd)
	if resp != ":0\r\n" {
		t.Fatalf("expected :0\\r\\n, got %q", resp)
	}

	// Add member
	runHandlerWith(eng, ctx, handleZAdd, &shared.Command{
		Args: [][]byte{[]byte("myzset"), []byte("1.0"), []byte("a")},
	})

	// XX: should update existing but not add new
	cmd = &shared.Command{Args: [][]byte{[]byte("myzset"), []byte("XX"), []byte("2.0"), []byte("a"), []byte("3.0"), []byte("b")}}
	resp = runHandlerWith(eng, ctx, handleZAdd, cmd)
	if resp != ":0\r\n" {
		t.Fatalf("expected :0\\r\\n (no new members), got %q", resp)
	}
}

func TestHandleZAdd_GT(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleZAdd, &shared.Command{
		Args: [][]byte{[]byte("myzset"), []byte("5.0"), []byte("a")},
	})

	// GT: score 3 < 5, should not update
	cmd := &shared.Command{Args: [][]byte{[]byte("myzset"), []byte("GT"), []byte("3.0"), []byte("a")}}
	resp := runHandlerWith(eng, ctx, handleZAdd, cmd)
	if resp != ":0\r\n" {
		t.Fatalf("expected :0\\r\\n, got %q", resp)
	}

	// GT: score 10 > 5, should update
	cmd = &shared.Command{Args: [][]byte{[]byte("myzset"), []byte("GT"), []byte("10.0"), []byte("a")}}
	resp = runHandlerWith(eng, ctx, handleZAdd, cmd)
	if resp != ":0\r\n" {
		t.Fatalf("expected :0\\r\\n (updated not added), got %q", resp)
	}
}

func TestHandleZAdd_LT(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleZAdd, &shared.Command{
		Args: [][]byte{[]byte("myzset"), []byte("5.0"), []byte("a")},
	})

	// LT: score 10 > 5, should not update
	cmd := &shared.Command{Args: [][]byte{[]byte("myzset"), []byte("LT"), []byte("10.0"), []byte("a")}}
	resp := runHandlerWith(eng, ctx, handleZAdd, cmd)
	if resp != ":0\r\n" {
		t.Fatalf("expected :0\\r\\n, got %q", resp)
	}

	// LT: score 1 < 5, should update
	cmd = &shared.Command{Args: [][]byte{[]byte("myzset"), []byte("LT"), []byte("1.0"), []byte("a")}}
	resp = runHandlerWith(eng, ctx, handleZAdd, cmd)
	if resp != ":0\r\n" {
		t.Fatalf("expected :0\\r\\n (updated not added), got %q", resp)
	}
}

func TestHandleZAdd_CH(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleZAdd, &shared.Command{
		Args: [][]byte{[]byte("myzset"), []byte("1.0"), []byte("a")},
	})

	// CH: count changed + added
	cmd := &shared.Command{Args: [][]byte{[]byte("myzset"), []byte("CH"), []byte("2.0"), []byte("a"), []byte("3.0"), []byte("b")}}
	resp := runHandlerWith(eng, ctx, handleZAdd, cmd)
	if resp != ":2\r\n" {
		t.Fatalf("expected :2\\r\\n (1 changed + 1 added), got %q", resp)
	}
}

func TestHandleZAdd_INCR(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// INCR on new member
	cmd := &shared.Command{Args: [][]byte{[]byte("myzset"), []byte("INCR"), []byte("5.0"), []byte("a")}}
	resp := runHandlerWith(eng, ctx, handleZAdd, cmd)
	if !strings.Contains(resp, "5") {
		t.Fatalf("expected score 5 for new member, got %q", resp)
	}

	// INCR on existing member
	cmd = &shared.Command{Args: [][]byte{[]byte("myzset"), []byte("INCR"), []byte("3.0"), []byte("a")}}
	resp = runHandlerWith(eng, ctx, handleZAdd, cmd)
	if !strings.Contains(resp, "8") {
		t.Fatalf("expected score 8 after increment, got %q", resp)
	}
}

func TestHandleZAdd_WrongNumArgs(t *testing.T) {
	resp := runHandler(handleZAdd, &shared.Command{Args: [][]byte{[]byte("myzset"), []byte("1.0")}})
	if !strings.Contains(resp, "ERR") {
		t.Fatalf("expected error, got %q", resp)
	}
}

func TestHandleZAdd_WrongType(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Create a scalar (string) value
	_ = ctx.Emit(&pb.Effect{
		Key: []byte("mykey"),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: []byte("hello")},
		}},
	})
	_ = ctx.Flush()

	cmd := &shared.Command{Args: [][]byte{[]byte("mykey"), []byte("1.0"), []byte("a")}}
	resp := runHandlerWith(eng, ctx, handleZAdd, cmd)
	if !strings.Contains(resp, "WRONGTYPE") {
		t.Fatalf("expected WRONGTYPE error, got %q", resp)
	}
}

// --- ZINCRBY tests ---

func TestHandleZIncrBy(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Increment on new key/member
	cmd := &shared.Command{Args: [][]byte{[]byte("myzset"), []byte("5"), []byte("a")}}
	resp := runHandlerWith(eng, ctx, handleZIncrBy, cmd)
	if !strings.Contains(resp, "5") {
		t.Fatalf("expected score 5, got %q", resp)
	}

	// Increment existing
	cmd = &shared.Command{Args: [][]byte{[]byte("myzset"), []byte("3"), []byte("a")}}
	resp = runHandlerWith(eng, ctx, handleZIncrBy, cmd)
	if !strings.Contains(resp, "8") {
		t.Fatalf("expected score 8, got %q", resp)
	}
}

// --- ZSCORE tests ---

func TestHandleZScore(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Non-existent key
	cmd := &shared.Command{Args: [][]byte{[]byte("myzset"), []byte("a")}}
	resp := runHandlerWith(eng, ctx, handleZScore, cmd)
	if resp != "$-1\r\n" {
		t.Fatalf("expected null bulk string, got %q", resp)
	}

	// Add and check score
	runHandlerWith(eng, ctx, handleZAdd, &shared.Command{
		Args: [][]byte{[]byte("myzset"), []byte("3.14"), []byte("a")},
	})

	cmd = &shared.Command{Args: [][]byte{[]byte("myzset"), []byte("a")}}
	resp = runHandlerWith(eng, ctx, handleZScore, cmd)
	if !strings.Contains(resp, "3.14") {
		t.Fatalf("expected 3.14, got %q", resp)
	}

	// Non-existent member
	cmd = &shared.Command{Args: [][]byte{[]byte("myzset"), []byte("b")}}
	resp = runHandlerWith(eng, ctx, handleZScore, cmd)
	if resp != "$-1\r\n" {
		t.Fatalf("expected null bulk string for missing member, got %q", resp)
	}
}

// --- ZCARD tests ---

func TestHandleZCard(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Non-existent key
	resp := runHandlerWith(eng, ctx, handleZCard, &shared.Command{
		Args: [][]byte{[]byte("myzset")},
	})
	if resp != ":0\r\n" {
		t.Fatalf("expected :0\\r\\n, got %q", resp)
	}

	// Add members
	runHandlerWith(eng, ctx, handleZAdd, &shared.Command{
		Args: [][]byte{[]byte("myzset"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")},
	})

	resp = runHandlerWith(eng, ctx, handleZCard, &shared.Command{
		Args: [][]byte{[]byte("myzset")},
	})
	if resp != ":3\r\n" {
		t.Fatalf("expected :3\\r\\n, got %q", resp)
	}
}

// --- ZRANK tests ---

func TestHandleZRank(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleZAdd, &shared.Command{
		Args: [][]byte{[]byte("myzset"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")},
	})

	// Rank of "a" (lowest score) should be 0
	resp := runHandlerWith(eng, ctx, handleZRank, &shared.Command{
		Args: [][]byte{[]byte("myzset"), []byte("a")},
	})
	if resp != ":0\r\n" {
		t.Fatalf("expected :0\\r\\n, got %q", resp)
	}

	// Rank of "c" (highest score) should be 2
	resp = runHandlerWith(eng, ctx, handleZRank, &shared.Command{
		Args: [][]byte{[]byte("myzset"), []byte("c")},
	})
	if resp != ":2\r\n" {
		t.Fatalf("expected :2\\r\\n, got %q", resp)
	}

	// Non-existent member
	resp = runHandlerWith(eng, ctx, handleZRank, &shared.Command{
		Args: [][]byte{[]byte("myzset"), []byte("d")},
	})
	if resp != "$-1\r\n" {
		t.Fatalf("expected null bulk string, got %q", resp)
	}
}

// --- ZREM tests ---

func TestHandleZRem(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleZAdd, &shared.Command{
		Args: [][]byte{[]byte("myzset"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")},
	})

	// Remove existing and non-existing
	resp := runHandlerWith(eng, ctx, handleZRem, &shared.Command{
		Args: [][]byte{[]byte("myzset"), []byte("a"), []byte("d")},
	})
	if resp != ":1\r\n" {
		t.Fatalf("expected :1\\r\\n, got %q", resp)
	}

	// Verify card is now 2
	resp = runHandlerWith(eng, ctx, handleZCard, &shared.Command{
		Args: [][]byte{[]byte("myzset")},
	})
	if resp != ":2\r\n" {
		t.Fatalf("expected :2\\r\\n, got %q", resp)
	}
}

func TestHandleZRem_NonExistentKey(t *testing.T) {
	resp := runHandler(handleZRem, &shared.Command{
		Args: [][]byte{[]byte("nokey"), []byte("a")},
	})
	if resp != ":0\r\n" {
		t.Fatalf("expected :0\\r\\n, got %q", resp)
	}
}

// --- ZPOPMIN/ZPOPMAX tests ---

func TestHandleZPopMin(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleZAdd, &shared.Command{
		Args: [][]byte{[]byte("myzset"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")},
	})

	// Pop min
	resp := runHandlerWith(eng, ctx, handleZPopMin, &shared.Command{
		Args: [][]byte{[]byte("myzset")},
	})
	// Should return member "a" with score 1
	if !strings.Contains(resp, "a") || !strings.Contains(resp, "1") {
		t.Fatalf("expected member a with score 1, got %q", resp)
	}
}

func TestHandleZPopMax(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleZAdd, &shared.Command{
		Args: [][]byte{[]byte("myzset"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")},
	})

	// Pop max
	resp := runHandlerWith(eng, ctx, handleZPopMax, &shared.Command{
		Args: [][]byte{[]byte("myzset")},
	})
	// Should return member "c" with score 3
	if !strings.Contains(resp, "c") || !strings.Contains(resp, "3") {
		t.Fatalf("expected member c with score 3, got %q", resp)
	}
}

func TestHandleZPopMin_Empty(t *testing.T) {
	resp := runHandler(handleZPopMin, &shared.Command{
		Args: [][]byte{[]byte("nokey")},
	})
	if resp != "*0\r\n" {
		t.Fatalf("expected empty array, got %q", resp)
	}
}

// --- ZUNIONSTORE tests ---

func TestHandleZUnionStore(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Create two zsets
	runHandlerWith(eng, ctx, handleZAdd, &shared.Command{
		Args: [][]byte{[]byte("zs1"), []byte("1"), []byte("a"), []byte("2"), []byte("b")},
	})
	runHandlerWith(eng, ctx, handleZAdd, &shared.Command{
		Args: [][]byte{[]byte("zs2"), []byte("3"), []byte("b"), []byte("4"), []byte("c")},
	})

	// ZUNIONSTORE dest 2 zs1 zs2
	resp := runHandlerWith(eng, ctx, handleZUnionStore, &shared.Command{
		Args: [][]byte{[]byte("dest"), []byte("2"), []byte("zs1"), []byte("zs2")},
	})
	if resp != ":3\r\n" {
		t.Fatalf("expected :3\\r\\n, got %q", resp)
	}

	// Verify dest has 3 members
	resp = runHandlerWith(eng, ctx, handleZCard, &shared.Command{
		Args: [][]byte{[]byte("dest")},
	})
	if resp != ":3\r\n" {
		t.Fatalf("expected :3\\r\\n for dest card, got %q", resp)
	}
}

// --- Wrong type tests ---

func TestHandleZScore_WrongType(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Create a scalar value
	_ = ctx.Emit(&pb.Effect{
		Key: []byte("mykey"),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: []byte("hello")},
		}},
	})
	_ = ctx.Flush()

	cmd := &shared.Command{Args: [][]byte{[]byte("mykey"), []byte("a")}}
	resp := runHandlerWith(eng, ctx, handleZScore, cmd)
	if !strings.Contains(resp, "WRONGTYPE") {
		t.Fatalf("expected WRONGTYPE error, got %q", resp)
	}
}

func TestHandleZCard_WrongType(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	_ = ctx.Emit(&pb.Effect{
		Key: []byte("mykey"),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: []byte("hello")},
		}},
	})
	_ = ctx.Flush()

	resp := runHandlerWith(eng, ctx, handleZCard, &shared.Command{
		Args: [][]byte{[]byte("mykey")},
	})
	if !strings.Contains(resp, "WRONGTYPE") {
		t.Fatalf("expected WRONGTYPE error, got %q", resp)
	}
}
