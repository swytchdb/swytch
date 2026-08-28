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

package set

import (
	"bytes"
	"strings"
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

func TestHandleSAdd(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	cmd := &shared.Command{Args: [][]byte{[]byte("myset"), []byte("a"), []byte("b"), []byte("c")}}
	resp := runHandlerWith(eng, ctx, handleSAdd, cmd)
	if resp != ":3\r\n" {
		t.Fatalf("expected :3\\r\\n, got %q", resp)
	}

	// Add duplicate members
	cmd = &shared.Command{Args: [][]byte{[]byte("myset"), []byte("a"), []byte("d")}}
	resp = runHandlerWith(eng, ctx, handleSAdd, cmd)
	if resp != ":1\r\n" {
		t.Fatalf("expected :1\\r\\n, got %q", resp)
	}
}

func TestHandleSAdd_WrongNumArgs(t *testing.T) {
	resp := runHandler(handleSAdd, &shared.Command{Args: [][]byte{[]byte("myset")}})
	if !strings.Contains(resp, "ERR") {
		t.Fatalf("expected error, got %q", resp)
	}
}

func TestHandleSAdd_WrongType(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Create a scalar (string) value so SADD gets a wrong-type error
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
	resp := runHandlerWith(eng, ctx, handleSAdd, cmd)
	if !strings.Contains(resp, "WRONGTYPE") {
		t.Fatalf("expected WRONGTYPE error, got %q", resp)
	}
}

func TestHandleSRem(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Seed a set
	runHandlerWith(eng, ctx, handleSAdd, &shared.Command{
		Args: [][]byte{[]byte("myset"), []byte("a"), []byte("b"), []byte("c")},
	})

	cmd := &shared.Command{Args: [][]byte{[]byte("myset"), []byte("a"), []byte("d")}}
	resp := runHandlerWith(eng, ctx, handleSRem, cmd)
	if resp != ":1\r\n" {
		t.Fatalf("expected :1\\r\\n, got %q", resp)
	}
}

func TestHandleSRem_NonExistentKey(t *testing.T) {
	resp := runHandler(handleSRem, &shared.Command{
		Args: [][]byte{[]byte("nokey"), []byte("a")},
	})
	if resp != ":0\r\n" {
		t.Fatalf("expected :0\\r\\n, got %q", resp)
	}
}

func TestHandleSPop(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleSAdd, &shared.Command{
		Args: [][]byte{[]byte("myset"), []byte("a"), []byte("b"), []byte("c")},
	})

	// Pop single (no count)
	cmd := &shared.Command{Args: [][]byte{[]byte("myset")}}
	resp := runHandlerWith(eng, ctx, handleSPop, cmd)
	// Should return a bulk string
	if !strings.Contains(resp, "$1") {
		t.Fatalf("expected bulk string response, got %q", resp)
	}
}

func TestHandleSPop_WithCount(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleSAdd, &shared.Command{
		Args: [][]byte{[]byte("myset"), []byte("a"), []byte("b"), []byte("c")},
	})

	cmd := &shared.Command{Args: [][]byte{[]byte("myset"), []byte("2")}}
	resp := runHandlerWith(eng, ctx, handleSPop, cmd)
	if !strings.Contains(resp, "*2") {
		t.Fatalf("expected array of 2, got %q", resp)
	}
}

func TestHandleSPop_NonExistent(t *testing.T) {
	resp := runHandler(handleSPop, &shared.Command{
		Args: [][]byte{[]byte("nokey")},
	})
	if !strings.Contains(resp, "$-1") {
		t.Fatalf("expected null bulk string, got %q", resp)
	}
}

func TestHandleSMove(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleSAdd, &shared.Command{
		Args: [][]byte{[]byte("src"), []byte("a"), []byte("b")},
	})
	runHandlerWith(eng, ctx, handleSAdd, &shared.Command{
		Args: [][]byte{[]byte("dst"), []byte("c")},
	})

	cmd := &shared.Command{Args: [][]byte{[]byte("src"), []byte("dst"), []byte("a")}}
	resp := runHandlerWith(eng, ctx, handleSMove, cmd)
	if resp != ":1\r\n" {
		t.Fatalf("expected :1\\r\\n, got %q", resp)
	}

	// Verify "a" is now in dst
	resp = runHandlerWith(eng, ctx, handleSIsMember, &shared.Command{
		Args: [][]byte{[]byte("dst"), []byte("a")},
	})
	if resp != ":1\r\n" {
		t.Fatalf("expected member in dst, got %q", resp)
	}

	// Verify "a" is not in src
	resp = runHandlerWith(eng, ctx, handleSIsMember, &shared.Command{
		Args: [][]byte{[]byte("src"), []byte("a")},
	})
	if resp != ":0\r\n" {
		t.Fatalf("expected member removed from src, got %q", resp)
	}
}

func TestHandleSMove_MemberNotInSource(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleSAdd, &shared.Command{
		Args: [][]byte{[]byte("src"), []byte("a")},
	})

	cmd := &shared.Command{Args: [][]byte{[]byte("src"), []byte("dst"), []byte("x")}}
	resp := runHandlerWith(eng, ctx, handleSMove, cmd)
	if resp != ":0\r\n" {
		t.Fatalf("expected :0\\r\\n, got %q", resp)
	}
}

func TestHandleSMove_MissingSrc(t *testing.T) {
	resp := runHandler(handleSMove, &shared.Command{
		Args: [][]byte{[]byte("nosrc"), []byte("dst"), []byte("x")},
	})
	if resp != ":0\r\n" {
		t.Fatalf("expected :0\\r\\n, got %q", resp)
	}
}
