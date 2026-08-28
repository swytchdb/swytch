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

package list

import (
	"bytes"
	"fmt"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
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

// seedList inserts ordered elements into a list key via effects.
func seedList(eng *effects.Engine, ctx *effects.Context, key string, values ...string) {
	for i, v := range values {
		_ = ctx.Emit(&pb.Effect{
			Key: []byte(key),
			Kind: &pb.Effect_Data{Data: &pb.DataEffect{
				Op:         pb.EffectOp_INSERT_OP,
				Merge:      pb.MergeRule_LAST_WRITE_WINS,
				Collection: pb.CollectionKind_ORDERED,
				Placement:  pb.Placement_PLACE_TAIL,
				Id:         fmt.Appendf(nil, "id-%d", i),
				Value:      &pb.DataEffect_Raw{Raw: []byte(v)},
			}},
		})
	}
	_ = ctx.Flush()
}

// seedHash creates a hash key (for wrong-type testing).
func seedHash(eng *effects.Engine, ctx *effects.Context, key string) {
	_ = ctx.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte("field1"),
			Value:      &pb.DataEffect_Raw{Raw: []byte("value1")},
		}},
	})
	_ = ctx.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_HASH}},
	})
	_ = ctx.Flush()
}

func TestHandleLLen(t *testing.T) {
	t.Run("missing key", func(t *testing.T) {
		resp := runHandler(handleLLen, &shared.Command{Args: [][]byte{[]byte("nokey")}})
		if resp != ":0\r\n" {
			t.Fatalf("expected :0\\r\\n, got %q", resp)
		}
	})

	t.Run("3 elements", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLLen, &shared.Command{Args: [][]byte{[]byte("mylist")}})
		if resp != ":3\r\n" {
			t.Fatalf("expected :3\\r\\n, got %q", resp)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedHash(eng, ctx, "myhash")
		resp := runHandlerWith(eng, ctx, handleLLen, &shared.Command{Args: [][]byte{[]byte("myhash")}})
		if resp != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
			t.Fatalf("expected WRONGTYPE, got %q", resp)
		}
	})
}

func TestHandleLRange(t *testing.T) {
	t.Run("missing key", func(t *testing.T) {
		resp := runHandler(handleLRange, &shared.Command{Args: [][]byte{[]byte("nokey"), []byte("0"), []byte("-1")}})
		if resp != "*0\r\n" {
			t.Fatalf("expected *0\\r\\n, got %q", resp)
		}
	})

	t.Run("full range 0 -1", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLRange, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("0"), []byte("-1")}})
		expect := "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
		if resp != expect {
			t.Fatalf("expected %q, got %q", expect, resp)
		}
	})

	t.Run("partial range", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c", "d")
		resp := runHandlerWith(eng, ctx, handleLRange, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("1"), []byte("2")}})
		expect := "*2\r\n$1\r\nb\r\n$1\r\nc\r\n"
		if resp != expect {
			t.Fatalf("expected %q, got %q", expect, resp)
		}
	})

	t.Run("negative indices", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c", "d")
		resp := runHandlerWith(eng, ctx, handleLRange, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("-3"), []byte("-2")}})
		expect := "*2\r\n$1\r\nb\r\n$1\r\nc\r\n"
		if resp != expect {
			t.Fatalf("expected %q, got %q", expect, resp)
		}
	})

	t.Run("out-of-range stop clamped", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b")
		resp := runHandlerWith(eng, ctx, handleLRange, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("0"), []byte("100")}})
		expect := "*2\r\n$1\r\na\r\n$1\r\nb\r\n"
		if resp != expect {
			t.Fatalf("expected %q, got %q", expect, resp)
		}
	})

	t.Run("start > stop", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLRange, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("2"), []byte("0")}})
		if resp != "*0\r\n" {
			t.Fatalf("expected *0\\r\\n, got %q", resp)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedHash(eng, ctx, "myhash")
		resp := runHandlerWith(eng, ctx, handleLRange, &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("0"), []byte("-1")}})
		if resp != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
			t.Fatalf("expected WRONGTYPE, got %q", resp)
		}
	})
}

func TestHandleLIndex(t *testing.T) {
	t.Run("missing key", func(t *testing.T) {
		resp := runHandler(handleLIndex, &shared.Command{Args: [][]byte{[]byte("nokey"), []byte("0")}})
		if resp != "$-1\r\n" {
			t.Fatalf("expected null bulk string, got %q", resp)
		}
	})

	t.Run("index 0", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLIndex, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("0")}})
		if resp != "$1\r\na\r\n" {
			t.Fatalf("expected $1\\r\\na\\r\\n, got %q", resp)
		}
	})

	t.Run("index 2", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLIndex, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("2")}})
		if resp != "$1\r\nc\r\n" {
			t.Fatalf("expected $1\\r\\nc\\r\\n, got %q", resp)
		}
	})

	t.Run("negative index -1", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLIndex, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("-1")}})
		if resp != "$1\r\nc\r\n" {
			t.Fatalf("expected $1\\r\\nc\\r\\n, got %q", resp)
		}
	})

	t.Run("negative index -3", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLIndex, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("-3")}})
		if resp != "$1\r\na\r\n" {
			t.Fatalf("expected $1\\r\\na\\r\\n, got %q", resp)
		}
	})

	t.Run("out of range", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLIndex, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("10")}})
		if resp != "$-1\r\n" {
			t.Fatalf("expected null bulk string, got %q", resp)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedHash(eng, ctx, "myhash")
		resp := runHandlerWith(eng, ctx, handleLIndex, &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("0")}})
		if resp != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
			t.Fatalf("expected WRONGTYPE, got %q", resp)
		}
	})
}

func TestHandleLPos(t *testing.T) {
	t.Run("missing key no COUNT", func(t *testing.T) {
		resp := runHandler(handleLPos, &shared.Command{Args: [][]byte{[]byte("nokey"), []byte("a")}})
		if resp != "$-1\r\n" {
			t.Fatalf("expected null bulk string, got %q", resp)
		}
	})

	t.Run("missing key with COUNT", func(t *testing.T) {
		resp := runHandler(handleLPos, &shared.Command{Args: [][]byte{[]byte("nokey"), []byte("a"), []byte("COUNT"), []byte("0")}})
		if resp != "*0\r\n" {
			t.Fatalf("expected *0\\r\\n, got %q", resp)
		}
	})

	t.Run("found", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c", "b", "d")
		resp := runHandlerWith(eng, ctx, handleLPos, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("b")}})
		if resp != ":1\r\n" {
			t.Fatalf("expected :1\\r\\n, got %q", resp)
		}
	})

	t.Run("not found", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLPos, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("z")}})
		if resp != "$-1\r\n" {
			t.Fatalf("expected null bulk string, got %q", resp)
		}
	})

	t.Run("RANK skip", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c", "b", "d")
		resp := runHandlerWith(eng, ctx, handleLPos, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("b"), []byte("RANK"), []byte("2")}})
		if resp != ":3\r\n" {
			t.Fatalf("expected :3\\r\\n, got %q", resp)
		}
	})

	t.Run("negative RANK", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c", "b", "d")
		resp := runHandlerWith(eng, ctx, handleLPos, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("b"), []byte("RANK"), []byte("-1")}})
		if resp != ":3\r\n" {
			t.Fatalf("expected :3\\r\\n, got %q", resp)
		}
	})

	t.Run("COUNT 0 all matches", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c", "b", "d")
		resp := runHandlerWith(eng, ctx, handleLPos, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("b"), []byte("COUNT"), []byte("0")}})
		expect := "*2\r\n:1\r\n:3\r\n"
		if resp != expect {
			t.Fatalf("expected %q, got %q", expect, resp)
		}
	})

	t.Run("COUNT N", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c", "b", "d", "b")
		resp := runHandlerWith(eng, ctx, handleLPos, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("b"), []byte("COUNT"), []byte("2")}})
		expect := "*2\r\n:1\r\n:3\r\n"
		if resp != expect {
			t.Fatalf("expected %q, got %q", expect, resp)
		}
	})

	t.Run("MAXLEN", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c", "b", "d")
		// MAXLEN 2 means only scan first 2 elements
		resp := runHandlerWith(eng, ctx, handleLPos, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("b"), []byte("MAXLEN"), []byte("1")}})
		if resp != "$-1\r\n" {
			t.Fatalf("expected null bulk string (not found within maxlen), got %q", resp)
		}
	})

	t.Run("RANK 0 error", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLPos, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("b"), []byte("RANK"), []byte("0")}})
		if resp != "-ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list\r\n" {
			t.Fatalf("expected RANK error, got %q", resp)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedHash(eng, ctx, "myhash")
		resp := runHandlerWith(eng, ctx, handleLPos, &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("a")}})
		if resp != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
			t.Fatalf("expected WRONGTYPE, got %q", resp)
		}
	})
}

func TestHandleLPush(t *testing.T) {
	t.Run("new key", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		resp := runHandlerWith(eng, ctx, handleLPush, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("a"), []byte("b"), []byte("c")}})
		if resp != ":3\r\n" {
			t.Fatalf("expected :3\\r\\n, got %q", resp)
		}
		// Verify snapshot has 3 elements
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap == nil || len(snap.OrderedElements) != 3 {
			t.Fatalf("expected 3 elements in snapshot, got %v", snap)
		}
	})

	t.Run("existing key", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "x", "y")
		resp := runHandlerWith(eng, ctx, handleLPush, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("a")}})
		if resp != ":3\r\n" {
			t.Fatalf("expected :3\\r\\n, got %q", resp)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedHash(eng, ctx, "myhash")
		resp := runHandlerWith(eng, ctx, handleLPush, &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("a")}})
		if resp != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
			t.Fatalf("expected WRONGTYPE, got %q", resp)
		}
	})

	t.Run("multiple elements order", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		resp := runHandlerWith(eng, ctx, handleLPush, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("a"), []byte("b"), []byte("c")}})
		if resp != ":3\r\n" {
			t.Fatalf("expected :3\\r\\n, got %q", resp)
		}
		// LPUSH inserts at head, so order should be c b a
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap == nil {
			t.Fatal("expected snapshot")
		}
		elems := snap.OrderedElements
		if len(elems) != 3 {
			t.Fatalf("expected 3 elements, got %d", len(elems))
		}
		if string(elems[0].Data.GetRaw()) != "c" || string(elems[1].Data.GetRaw()) != "b" || string(elems[2].Data.GetRaw()) != "a" {
			t.Fatalf("expected [c b a], got [%s %s %s]", elems[0].Data.GetRaw(), elems[1].Data.GetRaw(), elems[2].Data.GetRaw())
		}
	})

	t.Run("type tag emitted for new key", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		runHandlerWith(eng, ctx, handleLPush, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("a")}})
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap == nil {
			t.Fatal("expected snapshot")
		}
		if snap.TypeTag != pb.ValueType_TYPE_LIST {
			t.Fatalf("expected TYPE_LIST tag, got %v", snap.TypeTag)
		}
	})
}

func TestHandleLPushX(t *testing.T) {
	t.Run("missing key", func(t *testing.T) {
		resp := runHandler(handleLPushX, &shared.Command{Args: [][]byte{[]byte("nokey"), []byte("a")}})
		if resp != ":0\r\n" {
			t.Fatalf("expected :0\\r\\n, got %q", resp)
		}
	})

	t.Run("existing key", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "x", "y")
		resp := runHandlerWith(eng, ctx, handleLPushX, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("a")}})
		if resp != ":3\r\n" {
			t.Fatalf("expected :3\\r\\n, got %q", resp)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedHash(eng, ctx, "myhash")
		resp := runHandlerWith(eng, ctx, handleLPushX, &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("a")}})
		if resp != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
			t.Fatalf("expected WRONGTYPE, got %q", resp)
		}
	})
}

func TestHandleRPush(t *testing.T) {
	t.Run("new key", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		resp := runHandlerWith(eng, ctx, handleRPush, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("a"), []byte("b"), []byte("c")}})
		if resp != ":3\r\n" {
			t.Fatalf("expected :3\\r\\n, got %q", resp)
		}
		// RPUSH inserts at tail, so order should be a b c
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap == nil {
			t.Fatal("expected snapshot")
		}
		elems := snap.OrderedElements
		if len(elems) != 3 {
			t.Fatalf("expected 3 elements, got %d", len(elems))
		}
		if string(elems[0].Data.GetRaw()) != "a" || string(elems[1].Data.GetRaw()) != "b" || string(elems[2].Data.GetRaw()) != "c" {
			t.Fatalf("expected [a b c], got [%s %s %s]", elems[0].Data.GetRaw(), elems[1].Data.GetRaw(), elems[2].Data.GetRaw())
		}
	})

	t.Run("existing key", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "x", "y")
		resp := runHandlerWith(eng, ctx, handleRPush, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("a")}})
		if resp != ":3\r\n" {
			t.Fatalf("expected :3\\r\\n, got %q", resp)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedHash(eng, ctx, "myhash")
		resp := runHandlerWith(eng, ctx, handleRPush, &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("a")}})
		if resp != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
			t.Fatalf("expected WRONGTYPE, got %q", resp)
		}
	})

	t.Run("type tag emitted for new key", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		runHandlerWith(eng, ctx, handleRPush, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("a")}})
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap == nil {
			t.Fatal("expected snapshot")
		}
		if snap.TypeTag != pb.ValueType_TYPE_LIST {
			t.Fatalf("expected TYPE_LIST tag, got %v", snap.TypeTag)
		}
	})
}

func TestHandleRPushX(t *testing.T) {
	t.Run("missing key", func(t *testing.T) {
		resp := runHandler(handleRPushX, &shared.Command{Args: [][]byte{[]byte("nokey"), []byte("a")}})
		if resp != ":0\r\n" {
			t.Fatalf("expected :0\\r\\n, got %q", resp)
		}
	})

	t.Run("existing key", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "x", "y")
		resp := runHandlerWith(eng, ctx, handleRPushX, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("a")}})
		if resp != ":3\r\n" {
			t.Fatalf("expected :3\\r\\n, got %q", resp)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedHash(eng, ctx, "myhash")
		resp := runHandlerWith(eng, ctx, handleRPushX, &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("a")}})
		if resp != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
			t.Fatalf("expected WRONGTYPE, got %q", resp)
		}
	})
}

func TestHandleLSet(t *testing.T) {
	t.Run("valid index", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLSet, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("1"), []byte("X")}})
		if resp != "+OK\r\n" {
			t.Fatalf("expected +OK\\r\\n, got %q", resp)
		}
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap == nil || len(snap.OrderedElements) != 3 {
			t.Fatal("expected 3 elements")
		}
		if string(snap.OrderedElements[1].Data.GetRaw()) != "X" {
			t.Fatalf("expected element 1 to be X, got %q", snap.OrderedElements[1].Data.GetRaw())
		}
	})

	t.Run("negative index", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLSet, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("-1"), []byte("Z")}})
		if resp != "+OK\r\n" {
			t.Fatalf("expected +OK\\r\\n, got %q", resp)
		}
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if string(snap.OrderedElements[2].Data.GetRaw()) != "Z" {
			t.Fatalf("expected last element to be Z, got %q", snap.OrderedElements[2].Data.GetRaw())
		}
	})

	t.Run("key not found", func(t *testing.T) {
		resp := runHandler(handleLSet, &shared.Command{Args: [][]byte{[]byte("nokey"), []byte("0"), []byte("v")}})
		if resp != "-ERR no such key\r\n" {
			t.Fatalf("expected ERR no such key, got %q", resp)
		}
	})

	t.Run("out of range", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b")
		resp := runHandlerWith(eng, ctx, handleLSet, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("5"), []byte("v")}})
		if resp != "-ERR index out of range\r\n" {
			t.Fatalf("expected out of range error, got %q", resp)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedHash(eng, ctx, "myhash")
		resp := runHandlerWith(eng, ctx, handleLSet, &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("0"), []byte("v")}})
		if resp != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
			t.Fatalf("expected WRONGTYPE, got %q", resp)
		}
	})
}

func TestHandleLInsert(t *testing.T) {
	t.Run("insert before", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLInsert, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("BEFORE"), []byte("b"), []byte("X")}})
		if resp != ":4\r\n" {
			t.Fatalf("expected :4\\r\\n, got %q", resp)
		}
	})

	t.Run("insert after", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLInsert, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("AFTER"), []byte("b"), []byte("X")}})
		if resp != ":4\r\n" {
			t.Fatalf("expected :4\\r\\n, got %q", resp)
		}
	})

	t.Run("pivot not found", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLInsert, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("BEFORE"), []byte("z"), []byte("X")}})
		if resp != ":-1\r\n" {
			t.Fatalf("expected :-1\\r\\n, got %q", resp)
		}
	})

	t.Run("key not found", func(t *testing.T) {
		resp := runHandler(handleLInsert, &shared.Command{Args: [][]byte{[]byte("nokey"), []byte("BEFORE"), []byte("a"), []byte("X")}})
		if resp != ":0\r\n" {
			t.Fatalf("expected :0\\r\\n, got %q", resp)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedHash(eng, ctx, "myhash")
		resp := runHandlerWith(eng, ctx, handleLInsert, &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("BEFORE"), []byte("a"), []byte("X")}})
		if resp != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
			t.Fatalf("expected WRONGTYPE, got %q", resp)
		}
	})
}

func TestHandleLRem(t *testing.T) {
	t.Run("count positive removes from head", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "a", "c", "a")
		resp := runHandlerWith(eng, ctx, handleLRem, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("2"), []byte("a")}})
		if resp != ":2\r\n" {
			t.Fatalf("expected :2\\r\\n, got %q", resp)
		}
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap == nil || len(snap.OrderedElements) != 3 {
			t.Fatalf("expected 3 remaining elements, got %v", snap)
		}
	})

	t.Run("count negative removes from tail", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "a", "c", "a")
		resp := runHandlerWith(eng, ctx, handleLRem, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("-2"), []byte("a")}})
		if resp != ":2\r\n" {
			t.Fatalf("expected :2\\r\\n, got %q", resp)
		}
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap == nil || len(snap.OrderedElements) != 3 {
			t.Fatalf("expected 3 remaining elements, got %v", snap)
		}
	})

	t.Run("count zero removes all", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "a", "c", "a")
		resp := runHandlerWith(eng, ctx, handleLRem, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("0"), []byte("a")}})
		if resp != ":3\r\n" {
			t.Fatalf("expected :3\\r\\n, got %q", resp)
		}
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap == nil || len(snap.OrderedElements) != 2 {
			t.Fatalf("expected 2 remaining elements, got %v", snap)
		}
	})

	t.Run("no matches", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLRem, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("1"), []byte("z")}})
		if resp != ":0\r\n" {
			t.Fatalf("expected :0\\r\\n, got %q", resp)
		}
	})

	t.Run("key not found", func(t *testing.T) {
		resp := runHandler(handleLRem, &shared.Command{Args: [][]byte{[]byte("nokey"), []byte("1"), []byte("a")}})
		if resp != ":0\r\n" {
			t.Fatalf("expected :0\\r\\n, got %q", resp)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedHash(eng, ctx, "myhash")
		resp := runHandlerWith(eng, ctx, handleLRem, &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("1"), []byte("a")}})
		if resp != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
			t.Fatalf("expected WRONGTYPE, got %q", resp)
		}
	})
}

func TestHandleLTrim(t *testing.T) {
	t.Run("normal range", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c", "d", "e")
		resp := runHandlerWith(eng, ctx, handleLTrim, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("1"), []byte("3")}})
		if resp != "+OK\r\n" {
			t.Fatalf("expected +OK\\r\\n, got %q", resp)
		}
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap == nil || len(snap.OrderedElements) != 3 {
			t.Fatalf("expected 3 remaining elements, got %v", snap)
		}
		if string(snap.OrderedElements[0].Data.GetRaw()) != "b" {
			t.Fatalf("expected first element b, got %q", snap.OrderedElements[0].Data.GetRaw())
		}
	})

	t.Run("out of range clamped", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLTrim, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("0"), []byte("100")}})
		if resp != "+OK\r\n" {
			t.Fatalf("expected +OK\\r\\n, got %q", resp)
		}
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap == nil || len(snap.OrderedElements) != 3 {
			t.Fatalf("expected 3 elements unchanged, got %v", snap)
		}
	})

	t.Run("start > stop removes all", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLTrim, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("5"), []byte("1")}})
		if resp != "+OK\r\n" {
			t.Fatalf("expected +OK\\r\\n, got %q", resp)
		}
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap != nil && len(snap.OrderedElements) != 0 {
			t.Fatalf("expected empty list, got %d elements", len(snap.OrderedElements))
		}
	})

	t.Run("key not found", func(t *testing.T) {
		resp := runHandler(handleLTrim, &shared.Command{Args: [][]byte{[]byte("nokey"), []byte("0"), []byte("1")}})
		if resp != "+OK\r\n" {
			t.Fatalf("expected +OK\\r\\n, got %q", resp)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedHash(eng, ctx, "myhash")
		resp := runHandlerWith(eng, ctx, handleLTrim, &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("0"), []byte("1")}})
		if resp != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
			t.Fatalf("expected WRONGTYPE, got %q", resp)
		}
	})

	t.Run("negative indices", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c", "d", "e")
		resp := runHandlerWith(eng, ctx, handleLTrim, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("-3"), []byte("-1")}})
		if resp != "+OK\r\n" {
			t.Fatalf("expected +OK\\r\\n, got %q", resp)
		}
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap == nil || len(snap.OrderedElements) != 3 {
			t.Fatalf("expected 3 remaining elements, got %v", snap)
		}
		if string(snap.OrderedElements[0].Data.GetRaw()) != "c" {
			t.Fatalf("expected first element c, got %q", snap.OrderedElements[0].Data.GetRaw())
		}
	})
}

func TestHandleLPop(t *testing.T) {
	t.Run("missing key", func(t *testing.T) {
		resp := runHandler(handleLPop, &shared.Command{Args: [][]byte{[]byte("nokey")}})
		if resp != "$-1\r\n" {
			t.Fatalf("expected null bulk string, got %q", resp)
		}
	})

	t.Run("missing key with COUNT", func(t *testing.T) {
		resp := runHandler(handleLPop, &shared.Command{Args: [][]byte{[]byte("nokey"), []byte("2")}})
		if resp != "*-1\r\n" {
			t.Fatalf("expected null array, got %q", resp)
		}
	})

	t.Run("single pop no count", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLPop, &shared.Command{Args: [][]byte{[]byte("mylist")}})
		if resp != "$1\r\na\r\n" {
			t.Fatalf("expected $1\\r\\na\\r\\n, got %q", resp)
		}
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap == nil || len(snap.OrderedElements) != 2 {
			t.Fatalf("expected 2 remaining elements, got %v", snap)
		}
	})

	t.Run("multi pop count=2", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLPop, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("2")}})
		expect := "*2\r\n$1\r\na\r\n$1\r\nb\r\n"
		if resp != expect {
			t.Fatalf("expected %q, got %q", expect, resp)
		}
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap == nil || len(snap.OrderedElements) != 1 {
			t.Fatalf("expected 1 remaining element, got %v", snap)
		}
	})

	t.Run("count > list length", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b")
		resp := runHandlerWith(eng, ctx, handleLPop, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("5")}})
		expect := "*2\r\n$1\r\na\r\n$1\r\nb\r\n"
		if resp != expect {
			t.Fatalf("expected %q, got %q", expect, resp)
		}
	})

	t.Run("pop last element", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "x")
		resp := runHandlerWith(eng, ctx, handleLPop, &shared.Command{Args: [][]byte{[]byte("mylist")}})
		if resp != "$1\r\nx\r\n" {
			t.Fatalf("expected $1\\r\\nx\\r\\n, got %q", resp)
		}
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap != nil && len(snap.OrderedElements) != 0 {
			t.Fatalf("expected empty snapshot, got %d elements", len(snap.OrderedElements))
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedHash(eng, ctx, "myhash")
		resp := runHandlerWith(eng, ctx, handleLPop, &shared.Command{Args: [][]byte{[]byte("myhash")}})
		if resp != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
			t.Fatalf("expected WRONGTYPE, got %q", resp)
		}
	})
}

func TestHandleRPop(t *testing.T) {
	t.Run("missing key", func(t *testing.T) {
		resp := runHandler(handleRPop, &shared.Command{Args: [][]byte{[]byte("nokey")}})
		if resp != "$-1\r\n" {
			t.Fatalf("expected null bulk string, got %q", resp)
		}
	})

	t.Run("missing key with COUNT", func(t *testing.T) {
		resp := runHandler(handleRPop, &shared.Command{Args: [][]byte{[]byte("nokey"), []byte("2")}})
		if resp != "*-1\r\n" {
			t.Fatalf("expected null array, got %q", resp)
		}
	})

	t.Run("single pop no count", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleRPop, &shared.Command{Args: [][]byte{[]byte("mylist")}})
		if resp != "$1\r\nc\r\n" {
			t.Fatalf("expected $1\\r\\nc\\r\\n, got %q", resp)
		}
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap == nil || len(snap.OrderedElements) != 2 {
			t.Fatalf("expected 2 remaining elements, got %v", snap)
		}
	})

	t.Run("multi pop count=2", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleRPop, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("2")}})
		// First popped = rightmost = c, then b
		expect := "*2\r\n$1\r\nc\r\n$1\r\nb\r\n"
		if resp != expect {
			t.Fatalf("expected %q, got %q", expect, resp)
		}
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap == nil || len(snap.OrderedElements) != 1 {
			t.Fatalf("expected 1 remaining element, got %v", snap)
		}
	})

	t.Run("count > list length", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b")
		resp := runHandlerWith(eng, ctx, handleRPop, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("5")}})
		expect := "*2\r\n$1\r\nb\r\n$1\r\na\r\n"
		if resp != expect {
			t.Fatalf("expected %q, got %q", expect, resp)
		}
	})

	t.Run("pop last element", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "x")
		resp := runHandlerWith(eng, ctx, handleRPop, &shared.Command{Args: [][]byte{[]byte("mylist")}})
		if resp != "$1\r\nx\r\n" {
			t.Fatalf("expected $1\\r\\nx\\r\\n, got %q", resp)
		}
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap != nil && len(snap.OrderedElements) != 0 {
			t.Fatalf("expected empty snapshot, got %d elements", len(snap.OrderedElements))
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedHash(eng, ctx, "myhash")
		resp := runHandlerWith(eng, ctx, handleRPop, &shared.Command{Args: [][]byte{[]byte("myhash")}})
		if resp != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
			t.Fatalf("expected WRONGTYPE, got %q", resp)
		}
	})
}

func TestHandleLMPop(t *testing.T) {
	t.Run("missing all keys", func(t *testing.T) {
		resp := runHandler(handleLMPop, &shared.Command{Args: [][]byte{[]byte("1"), []byte("nokey"), []byte("LEFT")}})
		if resp != "*-1\r\n" {
			t.Fatalf("expected null array, got %q", resp)
		}
	})

	t.Run("first key empty second has data", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "list2", "x", "y", "z")
		resp := runHandlerWith(eng, ctx, handleLMPop, &shared.Command{Args: [][]byte{[]byte("2"), []byte("empty"), []byte("list2"), []byte("LEFT")}})
		expect := "*2\r\n$5\r\nlist2\r\n*1\r\n$1\r\nx\r\n"
		if resp != expect {
			t.Fatalf("expected %q, got %q", expect, resp)
		}
	})

	t.Run("LEFT direction", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLMPop, &shared.Command{Args: [][]byte{[]byte("1"), []byte("mylist"), []byte("LEFT"), []byte("COUNT"), []byte("2")}})
		expect := "*2\r\n$6\r\nmylist\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n"
		if resp != expect {
			t.Fatalf("expected %q, got %q", expect, resp)
		}
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap == nil || len(snap.OrderedElements) != 1 {
			t.Fatalf("expected 1 remaining element, got %v", snap)
		}
	})

	t.Run("RIGHT direction", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLMPop, &shared.Command{Args: [][]byte{[]byte("1"), []byte("mylist"), []byte("RIGHT"), []byte("COUNT"), []byte("2")}})
		expect := "*2\r\n$6\r\nmylist\r\n*2\r\n$1\r\nc\r\n$1\r\nb\r\n"
		if resp != expect {
			t.Fatalf("expected %q, got %q", expect, resp)
		}
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap == nil || len(snap.OrderedElements) != 1 {
			t.Fatalf("expected 1 remaining element, got %v", snap)
		}
	})

	t.Run("wrong type", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedHash(eng, ctx, "myhash")
		resp := runHandlerWith(eng, ctx, handleLMPop, &shared.Command{Args: [][]byte{[]byte("1"), []byte("myhash"), []byte("LEFT")}})
		if resp != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
			t.Fatalf("expected WRONGTYPE, got %q", resp)
		}
	})

	t.Run("single key default count", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLMPop, &shared.Command{Args: [][]byte{[]byte("1"), []byte("mylist"), []byte("LEFT")}})
		expect := "*2\r\n$6\r\nmylist\r\n*1\r\n$1\r\na\r\n"
		if resp != expect {
			t.Fatalf("expected %q, got %q", expect, resp)
		}
	})
}

func TestHandleLMove(t *testing.T) {
	t.Run("LEFT LEFT", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "src", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLMove, &shared.Command{Args: [][]byte{[]byte("src"), []byte("dst"), []byte("LEFT"), []byte("LEFT")}})
		if resp != "$1\r\na\r\n" {
			t.Fatalf("expected $1\\r\\na\\r\\n, got %q", resp)
		}
		srcSnap, _, _, _ := eng.GetSnapshot("src")
		if srcSnap == nil || len(srcSnap.OrderedElements) != 2 {
			t.Fatalf("expected 2 elements in src, got %v", srcSnap)
		}
		dstSnap, _, _, _ := eng.GetSnapshot("dst")
		if dstSnap == nil || len(dstSnap.OrderedElements) != 1 {
			t.Fatalf("expected 1 element in dst, got %v", dstSnap)
		}
		if string(dstSnap.OrderedElements[0].Data.GetRaw()) != "a" {
			t.Fatalf("expected dst[0]=a, got %q", dstSnap.OrderedElements[0].Data.GetRaw())
		}
	})

	t.Run("RIGHT LEFT", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "src", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLMove, &shared.Command{Args: [][]byte{[]byte("src"), []byte("dst"), []byte("RIGHT"), []byte("LEFT")}})
		if resp != "$1\r\nc\r\n" {
			t.Fatalf("expected $1\\r\\nc\\r\\n, got %q", resp)
		}
	})

	t.Run("same key rotate", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "mylist", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleLMove, &shared.Command{Args: [][]byte{[]byte("mylist"), []byte("mylist"), []byte("LEFT"), []byte("RIGHT")}})
		if resp != "$1\r\na\r\n" {
			t.Fatalf("expected $1\\r\\na\\r\\n, got %q", resp)
		}
		snap, _, _, _ := eng.GetSnapshot("mylist")
		if snap == nil || len(snap.OrderedElements) != 3 {
			t.Fatalf("expected 3 elements, got %v", snap)
		}
	})

	t.Run("missing source", func(t *testing.T) {
		resp := runHandler(handleLMove, &shared.Command{Args: [][]byte{[]byte("nosrc"), []byte("dst"), []byte("LEFT"), []byte("LEFT")}})
		if resp != "$-1\r\n" {
			t.Fatalf("expected null bulk string, got %q", resp)
		}
	})

	t.Run("wrong type source", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedHash(eng, ctx, "myhash")
		resp := runHandlerWith(eng, ctx, handleLMove, &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("dst"), []byte("LEFT"), []byte("LEFT")}})
		if resp != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
			t.Fatalf("expected WRONGTYPE, got %q", resp)
		}
	})

	t.Run("wrong type dest", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "src", "a", "b")
		seedHash(eng, ctx, "myhash")
		resp := runHandlerWith(eng, ctx, handleLMove, &shared.Command{Args: [][]byte{[]byte("src"), []byte("myhash"), []byte("LEFT"), []byte("LEFT")}})
		if resp != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
			t.Fatalf("expected WRONGTYPE, got %q", resp)
		}
		// Source should NOT have been modified (dest type checked before pop)
		srcSnap, _, _, _ := eng.GetSnapshot("src")
		if srcSnap == nil || len(srcSnap.OrderedElements) != 2 {
			t.Fatalf("expected source unchanged with 2 elements, got %v", srcSnap)
		}
	})
}

func TestHandleRPopLPush(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "src", "a", "b", "c")
		resp := runHandlerWith(eng, ctx, handleRPopLPush, &shared.Command{Args: [][]byte{[]byte("src"), []byte("dst")}})
		if resp != "$1\r\nc\r\n" {
			t.Fatalf("expected $1\\r\\nc\\r\\n, got %q", resp)
		}
		srcSnap, _, _, _ := eng.GetSnapshot("src")
		if srcSnap == nil || len(srcSnap.OrderedElements) != 2 {
			t.Fatalf("expected 2 elements in src, got %v", srcSnap)
		}
		dstSnap, _, _, _ := eng.GetSnapshot("dst")
		if dstSnap == nil || len(dstSnap.OrderedElements) != 1 {
			t.Fatalf("expected 1 element in dst, got %v", dstSnap)
		}
	})

	t.Run("missing source", func(t *testing.T) {
		resp := runHandler(handleRPopLPush, &shared.Command{Args: [][]byte{[]byte("nosrc"), []byte("dst")}})
		if resp != "$-1\r\n" {
			t.Fatalf("expected null bulk string, got %q", resp)
		}
	})

	t.Run("wrong type source", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedHash(eng, ctx, "myhash")
		resp := runHandlerWith(eng, ctx, handleRPopLPush, &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("dst")}})
		if resp != "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" {
			t.Fatalf("expected WRONGTYPE, got %q", resp)
		}
	})
}

func TestHandleLPopNonBlocking(t *testing.T) {
	t.Run("pop from first non-empty key", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "list2", "x", "y")
		cmd := &shared.Command{Args: [][]byte{[]byte("empty"), []byte("list2"), []byte("0")}}
		cmd.Runtime = eng
		cmd.Context = ctx
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		valid, _, runner := handleLPopNonBlocking(cmd, w, nil, true)
		if valid && runner != nil {
			runner()
			_ = ctx.Flush()
		}
		resp := buf.String()
		expect := "*2\r\n$5\r\nlist2\r\n$1\r\nx\r\n"
		if resp != expect {
			t.Fatalf("expected %q, got %q", expect, resp)
		}
	})

	t.Run("all empty returns nil", func(t *testing.T) {
		cmd := &shared.Command{Args: [][]byte{[]byte("empty1"), []byte("empty2"), []byte("0")}}
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		cmd.Runtime = eng
		cmd.Context = ctx
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		valid, _, runner := handleLPopNonBlocking(cmd, w, nil, true)
		if valid && runner != nil {
			runner()
			_ = ctx.Flush()
		}
		resp := buf.String()
		if resp != "*-1\r\n" {
			t.Fatalf("expected null array, got %q", resp)
		}
	})
}

func TestHandleBLMPopNonBlocking(t *testing.T) {
	t.Run("pop from first non-empty key", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "list2", "a", "b", "c")
		cmd := &shared.Command{Args: [][]byte{[]byte("0"), []byte("2"), []byte("empty"), []byte("list2"), []byte("LEFT"), []byte("COUNT"), []byte("2")}}
		cmd.Runtime = eng
		cmd.Context = ctx
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		valid, _, runner := handleBLMPopNonBlocking(cmd, w, nil)
		if valid && runner != nil {
			runner()
			_ = ctx.Flush()
		}
		resp := buf.String()
		expect := "*2\r\n$5\r\nlist2\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n"
		if resp != expect {
			t.Fatalf("expected %q, got %q", expect, resp)
		}
	})

	t.Run("all empty returns nil", func(t *testing.T) {
		cmd := &shared.Command{Args: [][]byte{[]byte("0"), []byte("1"), []byte("empty"), []byte("LEFT")}}
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		cmd.Runtime = eng
		cmd.Context = ctx
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		valid, _, runner := handleBLMPopNonBlocking(cmd, w, nil)
		if valid && runner != nil {
			runner()
			_ = ctx.Flush()
		}
		resp := buf.String()
		if resp != "*-1\r\n" {
			t.Fatalf("expected null array, got %q", resp)
		}
	})
}

func TestHandleBRPopLPushNonBlocking(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "src", "a", "b", "c")
		cmd := &shared.Command{Args: [][]byte{[]byte("src"), []byte("dst"), []byte("0")}}
		cmd.Runtime = eng
		cmd.Context = ctx
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		valid, _, runner := handleBRPopLPushNonBlocking(cmd, w, nil)
		if valid && runner != nil {
			runner()
			_ = ctx.Flush()
		}
		resp := buf.String()
		if resp != "$1\r\nc\r\n" {
			t.Fatalf("expected $1\\r\\nc\\r\\n, got %q", resp)
		}
	})

	t.Run("missing source", func(t *testing.T) {
		cmd := &shared.Command{Args: [][]byte{[]byte("nosrc"), []byte("dst"), []byte("0")}}
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		cmd.Runtime = eng
		cmd.Context = ctx
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		valid, _, runner := handleBRPopLPushNonBlocking(cmd, w, nil)
		if valid && runner != nil {
			runner()
			_ = ctx.Flush()
		}
		resp := buf.String()
		if resp != "$-1\r\n" {
			t.Fatalf("expected null bulk string, got %q", resp)
		}
	})
}

func TestHandleBLMoveNonBlocking(t *testing.T) {
	t.Run("basic LEFT RIGHT", func(t *testing.T) {
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		seedList(eng, ctx, "src", "a", "b", "c")
		cmd := &shared.Command{Args: [][]byte{[]byte("src"), []byte("dst"), []byte("LEFT"), []byte("RIGHT"), []byte("0")}}
		cmd.Runtime = eng
		cmd.Context = ctx
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		valid, _, runner := handleBLMoveNonBlocking(cmd, w, nil)
		if valid && runner != nil {
			runner()
			_ = ctx.Flush()
		}
		resp := buf.String()
		if resp != "$1\r\na\r\n" {
			t.Fatalf("expected $1\\r\\na\\r\\n, got %q", resp)
		}
	})

	t.Run("missing source", func(t *testing.T) {
		cmd := &shared.Command{Args: [][]byte{[]byte("nosrc"), []byte("dst"), []byte("LEFT"), []byte("RIGHT"), []byte("0")}}
		eng := effects.NewTestEngine()
		ctx := eng.NewContext()
		cmd.Runtime = eng
		cmd.Context = ctx
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		valid, _, runner := handleBLMoveNonBlocking(cmd, w, nil)
		if valid && runner != nil {
			runner()
			_ = ctx.Flush()
		}
		resp := buf.String()
		if resp != "$-1\r\n" {
			t.Fatalf("expected null bulk string, got %q", resp)
		}
	})
}
