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

package hash

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

// seedHash creates a hash via HSET effects
func seedHash(eng *effects.Engine, ctx *effects.Context, key string, fields map[string]string) {
	for field, value := range fields {
		_ = ctx.Emit(&pb.Effect{
			Key: []byte(key),
			Kind: &pb.Effect_Data{Data: &pb.DataEffect{
				Op:         pb.EffectOp_INSERT_OP,
				Merge:      pb.MergeRule_LAST_WRITE_WINS,
				Collection: pb.CollectionKind_KEYED,
				Id:         []byte(field),
				Value:      &pb.DataEffect_Raw{Raw: []byte(value)},
			}},
		})
	}
	_ = ctx.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_HASH}},
	})
	_ = ctx.Flush()
}

// --- HGet ---

func TestHandleHGet(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"field1": "value1"})

	cmd := &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("field1")}}
	resp := runHandlerWith(eng, ctx, handleHGet, cmd)
	if !strings.Contains(resp, "value1") {
		t.Fatalf("expected value1, got %q", resp)
	}
}

func TestHandleHGet_NonExistent(t *testing.T) {
	resp := runHandler(handleHGet, &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("field1")}})
	if !strings.Contains(resp, "$-1") {
		t.Fatalf("expected null bulk string, got %q", resp)
	}
}

func TestHandleHGet_MissingField(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"field1": "value1"})

	cmd := &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("nofield")}}
	resp := runHandlerWith(eng, ctx, handleHGet, cmd)
	if !strings.Contains(resp, "$-1") {
		t.Fatalf("expected null bulk string, got %q", resp)
	}
}

func TestHandleHGet_WrongNumArgs(t *testing.T) {
	resp := runHandler(handleHGet, &shared.Command{Args: [][]byte{[]byte("myhash")}})
	if !strings.Contains(resp, "ERR") {
		t.Fatalf("expected error, got %q", resp)
	}
}

func TestHandleHGet_WrongType(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Create a scalar (string) value so HGET gets a wrong-type error
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

	cmd := &shared.Command{Args: [][]byte{[]byte("mykey"), []byte("field1")}}
	resp := runHandlerWith(eng, ctx, handleHGet, cmd)
	if !strings.Contains(resp, "WRONGTYPE") {
		t.Fatalf("expected WRONGTYPE error, got %q", resp)
	}
}

// --- HSet ---

func TestHandleHSet(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	cmd := &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("f1"), []byte("v1"), []byte("f2"), []byte("v2")}}
	resp := runHandlerWith(eng, ctx, handleHSet, cmd)
	if !strings.Contains(resp, ":2") {
		t.Fatalf("expected 2 added, got %q", resp)
	}

	// Verify fields via HGET
	cmd = &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("f1")}}
	resp = runHandlerWith(eng, ctx, handleHGet, cmd)
	if !strings.Contains(resp, "v1") {
		t.Fatalf("expected v1, got %q", resp)
	}
}

func TestHandleHSet_Duplicates(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// First set
	cmd := &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("f1"), []byte("v1"), []byte("f2"), []byte("v2")}}
	runHandlerWith(eng, ctx, handleHSet, cmd)

	// Set again with one existing and one new
	cmd = &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("f1"), []byte("v1new"), []byte("f3"), []byte("v3")}}
	resp := runHandlerWith(eng, ctx, handleHSet, cmd)
	if !strings.Contains(resp, ":1") {
		t.Fatalf("expected 1 new field, got %q", resp)
	}
}

func TestHandleHSet_WrongNumArgs(t *testing.T) {
	resp := runHandler(handleHSet, &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("f1")}})
	if !strings.Contains(resp, "ERR") {
		t.Fatalf("expected error, got %q", resp)
	}
}

// --- HSetNX ---

func TestHandleHSetNX(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	cmd := &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("f1"), []byte("v1")}}
	resp := runHandlerWith(eng, ctx, handleHSetNX, cmd)
	if !strings.Contains(resp, ":1") {
		t.Fatalf("expected 1, got %q", resp)
	}

	// Set again - should return 0
	cmd = &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("f1"), []byte("v2")}}
	resp = runHandlerWith(eng, ctx, handleHSetNX, cmd)
	if !strings.Contains(resp, ":0") {
		t.Fatalf("expected 0 (not set), got %q", resp)
	}

	// Verify original value preserved
	cmd = &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("f1")}}
	resp = runHandlerWith(eng, ctx, handleHGet, cmd)
	if !strings.Contains(resp, "v1") {
		t.Fatalf("expected v1 preserved, got %q", resp)
	}
}

// --- HDel ---

func TestHandleHDel(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1", "f2": "v2", "f3": "v3"})

	cmd := &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("f1"), []byte("f2"), []byte("nonexistent")}}
	resp := runHandlerWith(eng, ctx, handleHDel, cmd)
	if !strings.Contains(resp, ":2") {
		t.Fatalf("expected 2 deleted, got %q", resp)
	}

	// Verify f3 still exists
	cmd = &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("f3")}}
	resp = runHandlerWith(eng, ctx, handleHGet, cmd)
	if !strings.Contains(resp, "v3") {
		t.Fatalf("expected f3 to still exist, got %q", resp)
	}
}

func TestHandleHDel_NonExistent(t *testing.T) {
	resp := runHandler(handleHDel, &shared.Command{Args: [][]byte{[]byte("nokey"), []byte("f1")}})
	if !strings.Contains(resp, ":0") {
		t.Fatalf("expected 0, got %q", resp)
	}
}

// --- HExists ---

func TestHandleHExists(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1"})

	cmd := &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("f1")}}
	resp := runHandlerWith(eng, ctx, handleHExists, cmd)
	if !strings.Contains(resp, ":1") {
		t.Fatalf("expected 1, got %q", resp)
	}

	cmd = &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("nope")}}
	resp = runHandlerWith(eng, ctx, handleHExists, cmd)
	if !strings.Contains(resp, ":0") {
		t.Fatalf("expected 0, got %q", resp)
	}
}

// --- HGetAll ---

func TestHandleHGetAll(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1", "f2": "v2"})

	cmd := &shared.Command{Args: [][]byte{[]byte("myhash")}}
	resp := runHandlerWith(eng, ctx, handleHGetAll, cmd)
	if !strings.Contains(resp, "f1") || !strings.Contains(resp, "v1") ||
		!strings.Contains(resp, "f2") || !strings.Contains(resp, "v2") {
		t.Fatalf("expected all fields/values, got %q", resp)
	}
}

func TestHandleHGetAll_NonExistent(t *testing.T) {
	resp := runHandler(handleHGetAll, &shared.Command{Args: [][]byte{[]byte("nokey")}})
	if !strings.Contains(resp, "*0") {
		t.Fatalf("expected empty result, got %q", resp)
	}
}

// --- HKeys ---

func TestHandleHKeys(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1", "f2": "v2"})

	cmd := &shared.Command{Args: [][]byte{[]byte("myhash")}}
	resp := runHandlerWith(eng, ctx, handleHKeys, cmd)
	if !strings.Contains(resp, "f1") || !strings.Contains(resp, "f2") {
		t.Fatalf("expected f1 and f2, got %q", resp)
	}
}

// --- HVals ---

func TestHandleHVals(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1", "f2": "v2"})

	cmd := &shared.Command{Args: [][]byte{[]byte("myhash")}}
	resp := runHandlerWith(eng, ctx, handleHVals, cmd)
	if !strings.Contains(resp, "v1") || !strings.Contains(resp, "v2") {
		t.Fatalf("expected v1 and v2, got %q", resp)
	}
}

// --- HLen ---

func TestHandleHLen(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1", "f2": "v2"})

	cmd := &shared.Command{Args: [][]byte{[]byte("myhash")}}
	resp := runHandlerWith(eng, ctx, handleHLen, cmd)
	if !strings.Contains(resp, ":2") {
		t.Fatalf("expected 2, got %q", resp)
	}
}

func TestHandleHLen_NonExistent(t *testing.T) {
	resp := runHandler(handleHLen, &shared.Command{Args: [][]byte{[]byte("nokey")}})
	if !strings.Contains(resp, ":0") {
		t.Fatalf("expected 0, got %q", resp)
	}
}

// --- HMGet ---

func TestHandleHMGet(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1", "f2": "v2"})

	cmd := &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("f1"), []byte("nofield"), []byte("f2")}}
	resp := runHandlerWith(eng, ctx, handleHMGet, cmd)
	if !strings.Contains(resp, "v1") || !strings.Contains(resp, "v2") {
		t.Fatalf("expected v1 and v2, got %q", resp)
	}
	if !strings.Contains(resp, "$-1") {
		t.Fatalf("expected null for missing field, got %q", resp)
	}
}

// --- HMSet ---

func TestHandleHMSet(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	cmd := &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("f1"), []byte("v1"), []byte("f2"), []byte("v2")}}
	resp := runHandlerWith(eng, ctx, handleHMSet, cmd)
	if !strings.Contains(resp, "+OK") {
		t.Fatalf("expected OK, got %q", resp)
	}
}

// --- HIncrBy ---

func TestHandleHIncrBy(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Increment non-existent field
	cmd := &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("counter"), []byte("5")}}
	resp := runHandlerWith(eng, ctx, handleHIncrBy, cmd)
	if !strings.Contains(resp, ":5") {
		t.Fatalf("expected 5, got %q", resp)
	}

	// Increment existing field
	cmd = &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("counter"), []byte("3")}}
	resp = runHandlerWith(eng, ctx, handleHIncrBy, cmd)
	if !strings.Contains(resp, ":8") {
		t.Fatalf("expected 8, got %q", resp)
	}
}

func TestHandleHIncrBy_NotInteger(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "notanumber"})

	cmd := &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("f1"), []byte("1")}}
	resp := runHandlerWith(eng, ctx, handleHIncrBy, cmd)
	if !strings.Contains(resp, "ERR") {
		t.Fatalf("expected error for non-integer value, got %q", resp)
	}
}

// --- HIncrByFloat ---

func TestHandleHIncrByFloat(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	cmd := &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("price"), []byte("10.5")}}
	resp := runHandlerWith(eng, ctx, handleHIncrByFloat, cmd)
	if !strings.Contains(resp, "10.5") {
		t.Fatalf("expected 10.5, got %q", resp)
	}

	cmd = &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("price"), []byte("2.3")}}
	resp = runHandlerWith(eng, ctx, handleHIncrByFloat, cmd)
	if !strings.Contains(resp, "12.8") {
		t.Fatalf("expected 12.8, got %q", resp)
	}
}

// --- HStrLen ---

func TestHandleHStrLen(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "hello"})

	cmd := &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("f1")}}
	resp := runHandlerWith(eng, ctx, handleHStrLen, cmd)
	if !strings.Contains(resp, ":5") {
		t.Fatalf("expected 5, got %q", resp)
	}
}

func TestHandleHStrLen_NonExistent(t *testing.T) {
	resp := runHandler(handleHStrLen, &shared.Command{Args: [][]byte{[]byte("nokey"), []byte("f1")}})
	if !strings.Contains(resp, ":0") {
		t.Fatalf("expected 0, got %q", resp)
	}
}

// --- checkAdditionOverflow ---

func TestCheckAdditionOverflow(t *testing.T) {
	if !checkAdditionOverflow(9223372036854775807, 1) {
		t.Fatal("expected overflow for MaxInt64 + 1")
	}
	if !checkAdditionOverflow(-9223372036854775808, -1) {
		t.Fatal("expected overflow for MinInt64 - 1")
	}
	if checkAdditionOverflow(100, 200) {
		t.Fatal("unexpected overflow for 100 + 200")
	}
}
