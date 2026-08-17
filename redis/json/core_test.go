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

package json

import (
	"bytes"
	"strings"
	"testing"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

// harness drives handlers against a single shared engine so writes from one
// command are visible to the next (like a real connection).
type harness struct {
	eng *effects.Engine
	ctx *effects.Context
}

func newHarness() *harness {
	eng := effects.NewTestEngine()
	return &harness{eng: eng, ctx: eng.NewContext()}
}

func (h *harness) run(handler shared.HandlerFunc, args ...string) string {
	cmd := &shared.Command{Context: h.ctx, Runtime: h.eng}
	for _, a := range args {
		cmd.Args = append(cmd.Args, []byte(a))
	}
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	valid, _, runner := handler(cmd, w, nil)
	if valid && runner != nil {
		runner()
		_ = h.ctx.Flush()
	}
	return buf.String()
}

func TestJSON_SetGetRoundTrip(t *testing.T) {
	h := newHarness()
	if got := h.run(handleJSONSet, "k", "$", `{"a":1,"b":{"c":2},"arr":[1,2,3]}`); !strings.Contains(got, "+OK") {
		t.Fatalf("SET: %q", got)
	}
	// JSONPath root → array-wrapped document.
	if got := h.run(handleJSONGet, "k", "$"); !strings.Contains(got, `[{"a":1,"b":{"c":2},"arr":[1,2,3]}]`) {
		t.Fatalf("GET $: %q", got)
	}
	// JSONPath nested → array of matches.
	if got := h.run(handleJSONGet, "k", "$.b.c"); !strings.Contains(got, `[2]`) {
		t.Fatalf("GET $.b.c: %q", got)
	}
	// Legacy nested → bare value.
	if got := h.run(handleJSONGet, "k", ".b.c"); !strings.Contains(got, "\r\n2\r\n") {
		t.Fatalf("GET .b.c: %q", got)
	}
	// Array index.
	if got := h.run(handleJSONGet, "k", "$.arr[0]"); !strings.Contains(got, `[1]`) {
		t.Fatalf("GET $.arr[0]: %q", got)
	}
}

func TestJSON_NestedSet(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"a":{}}`)
	// Create a new nested member.
	if got := h.run(handleJSONSet, "k", "$.a.b", `9`); !strings.Contains(got, "+OK") {
		t.Fatalf("nested SET: %q", got)
	}
	if got := h.run(handleJSONGet, "k", "$.a"); !strings.Contains(got, `[{"b":9}]`) {
		t.Fatalf("GET $.a: %q", got)
	}
	// Missing intermediate → nil (RedisJSON behavior).
	if got := h.run(handleJSONSet, "k", "$.x.y", `1`); !strings.Contains(got, "$-1") {
		t.Fatalf("missing-intermediate SET should be nil: %q", got)
	}
}

func TestJSON_NXXX(t *testing.T) {
	h := newHarness()
	// NX on a fresh key → OK.
	if got := h.run(handleJSONSet, "k", "$", `{"a":1}`, "NX"); !strings.Contains(got, "+OK") {
		t.Fatalf("NX new: %q", got)
	}
	// NX on an existing path → nil.
	if got := h.run(handleJSONSet, "k", "$.a", `2`, "NX"); !strings.Contains(got, "$-1") {
		t.Fatalf("NX existing should be nil: %q", got)
	}
	// XX on a new member → nil.
	if got := h.run(handleJSONSet, "k", "$.new", `2`, "XX"); !strings.Contains(got, "$-1") {
		t.Fatalf("XX new should be nil: %q", got)
	}
	// XX on the existing member → OK.
	if got := h.run(handleJSONSet, "k", "$.a", `5`, "XX"); !strings.Contains(got, "+OK") {
		t.Fatalf("XX existing: %q", got)
	}
	if got := h.run(handleJSONGet, "k", "$.a"); !strings.Contains(got, `[5]`) {
		t.Fatalf("after XX: %q", got)
	}
}

func TestJSON_Type(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"i":1,"f":1.5,"s":"x","b":true,"n":null,"o":{},"a":[]}`)
	cases := map[string]string{
		".i": "integer", ".f": "number", ".s": "string",
		".b": "boolean", ".n": "null", ".o": "object", ".a": "array",
	}
	for path, want := range cases {
		got := h.run(handleJSONType, "k", path)
		if !strings.Contains(got, want) {
			t.Errorf("TYPE %s = %q, want %s", path, got, want)
		}
	}
}

func TestJSON_Del(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"a":1,"b":2}`)
	if got := h.run(handleJSONDel, "k", "$.a"); !strings.Contains(got, ":1\r\n") {
		t.Fatalf("DEL $.a: %q", got)
	}
	if got := h.run(handleJSONGet, "k", "$"); !strings.Contains(got, `[{"b":2}]`) {
		t.Fatalf("after DEL: %q", got)
	}
	// FORGET alias (same handler).
	if got := h.run(handleJSONDel, "k", "$.b"); !strings.Contains(got, ":1\r\n") {
		t.Fatalf("DEL $.b: %q", got)
	}
	// Root del removes the key.
	h.run(handleJSONSet, "k2", "$", `{"x":1}`)
	if got := h.run(handleJSONDel, "k2"); !strings.Contains(got, ":1\r\n") {
		t.Fatalf("root DEL: %q", got)
	}
	if got := h.run(handleJSONType, "k2"); !strings.Contains(got, "$-1") {
		t.Fatalf("TYPE after root DEL should be nil: %q", got)
	}
}

func TestJSON_Merge(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"a":1,"b":2}`)
	// RFC 7396: null deletes, new key adds, existing updates.
	if got := h.run(handleJSONMerge, "k", "$", `{"b":null,"c":3}`); !strings.Contains(got, "+OK") {
		t.Fatalf("MERGE: %q", got)
	}
	if got := h.run(handleJSONGet, "k", "$"); !strings.Contains(got, `[{"a":1,"c":3}]`) {
		t.Fatalf("after MERGE: %q", got)
	}
}

func TestJSON_Clear(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"o":{"x":1},"a":[1,2],"n":5,"s":"keep"}`)
	if got := h.run(handleJSONClear, "k", "$.o"); !strings.Contains(got, ":1\r\n") {
		t.Fatalf("CLEAR object: %q", got)
	}
	if got := h.run(handleJSONClear, "k", "$.s"); !strings.Contains(got, ":0\r\n") {
		t.Fatalf("CLEAR string should be no-op: %q", got)
	}
	if got := h.run(handleJSONGet, "k", "$.o"); !strings.Contains(got, `[{}]`) {
		t.Fatalf("object after clear: %q", got)
	}
}

func TestJSON_EmptyContainerExists(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{}`)
	// An empty object must still exist (not auto-deleted).
	if got := h.run(handleJSONType, "k", "$"); !strings.Contains(got, "object") {
		t.Fatalf("empty object TYPE: %q", got)
	}
	if got := h.run(handleJSONGet, "k", "$"); !strings.Contains(got, `[{}]`) {
		t.Fatalf("empty object GET: %q", got)
	}
}

func TestJSON_WrongType(t *testing.T) {
	h := newHarness()
	// Seed a non-JSON value by emitting a plain string scalar with a string tag.
	seedString(h, "str", "hello")
	if got := h.run(handleJSONGet, "str", "$"); !strings.Contains(got, "Existing key has wrong Redis type") {
		t.Fatalf("JSON.GET on string key should be wrong-type: %q", got)
	}
}

func TestJSON_GetMultiPath(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"a":1,"b":2}`)
	// Any JSONPath present → all values are arrays, keyed by the path string.
	if got := h.run(handleJSONGet, "k", "$.a", ".b"); !strings.Contains(got, `{"$.a":[1],".b":[2]}`) {
		t.Fatalf("multi-path GET: %q", got)
	}
}

func TestJSON_MSetMGet(t *testing.T) {
	h := newHarness()
	// Atomic multi-key set.
	if got := h.run(handleJSONMSet, "k1", "$", `{"x":1}`, "k2", "$", `{"x":2}`, "k3", "$", `{"x":3}`); !strings.Contains(got, "+OK") {
		t.Fatalf("MSET: %q", got)
	}
	// All three committed.
	for i, want := range []string{`[1]`, `[2]`, `[3]`} {
		key := []string{"k1", "k2", "k3"}[i]
		if got := h.run(handleJSONGet, key, "$.x"); !strings.Contains(got, want) {
			t.Fatalf("GET %s $.x: %q", key, got)
		}
	}
	// MGET pulls one path across keys; a missing key/path → null element.
	got := h.run(handleJSONMGet, "k1", "k2", "missing", "$.x")
	if !strings.Contains(got, "*3\r\n") || !strings.Contains(got, "1") || !strings.Contains(got, "2") {
		t.Fatalf("MGET: %q", got)
	}
}

func seedString(h *harness, key, val string) {
	_ = h.ctx.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR, Value: &pb.DataEffect_Raw{Raw: []byte(val)},
		}},
	})
	_ = h.ctx.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_STRING}},
	})
	_ = h.ctx.Flush()
}
