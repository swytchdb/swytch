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
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

// collectEffects returns an emitFn that appends to effs.
func collectEffects() (emitFn, *[]*pb.Effect) {
	var effs []*pb.Effect
	emit := func(e *pb.Effect) error {
		effs = append(effs, e)
		return nil
	}
	return emit, &effs
}

// roundTrip encodes a document to effects, reduces them through the real engine
// reducer, assembles the result back, and checks it matches the canonical input.
func roundTrip(t *testing.T, in string) {
	t.Helper()
	v, err := Parse([]byte(in))
	if err != nil {
		t.Fatalf("Parse(%s): %v", in, err)
	}
	emit, effs := collectEffects()
	if err := encodeRoot(emit, "k", v); err != nil {
		t.Fatalf("encodeRoot(%s): %v", in, err)
	}
	root := effects.ReduceBranch(*effs)
	got := assemble(root, "")
	if got == nil {
		t.Fatalf("assemble(%s) = nil", in)
	}
	if out := string(Serialize(got)); out != in {
		t.Errorf("round-trip(%s) = %s", in, out)
	}
}

func TestTree_RoundTrip(t *testing.T) {
	cases := []string{
		`5`,
		`"hi"`,
		`true`,
		`null`,
		`{}`,
		`[]`,
		`{"a":1}`,
		`{"a":1,"b":2,"c":3}`,
		`[1,2,3]`,
		`["a","b","c"]`,
		`{"a":1,"b":{"c":2}}`,
		`{"user":{"name":"bob","tags":["a","b"]},"n":42}`,
		`[{"x":1},{"y":[2,3]},4]`,
		`{"z":1,"a":2,"m":3}`, // key order preserved through encode+reduce+assemble
		`{"deep":{"deeper":{"deepest":[1,{"k":"v"}]}}}`,
	}
	for _, c := range cases {
		roundTrip(t, c)
	}
}

// Each container node must land in its own partition; the root holds only its
// direct members, nested objects/arrays live under generated virtual ids.
func TestTree_PartitionsPerContainer(t *testing.T) {
	v, _ := Parse([]byte(`{"a":1,"b":{"c":2},"arr":[10,{"d":3}]}`))
	emit, effs := collectEffects()
	if err := encodeRoot(emit, "k", v); err != nil {
		t.Fatal(err)
	}
	root := effects.ReduceBranch(*effs)

	// Root is an object with 3 direct members (a, b, arr) in order.
	if root.TypeTag != pb.ValueType_TYPE_JSON_OBJECT {
		t.Fatalf("root TypeTag = %v", root.TypeTag)
	}
	if len(root.OrderedElements) != 3 {
		t.Fatalf("root has %d direct members, want 3", len(root.OrderedElements))
	}
	// b (object) and arr (array)/its object element are separate partitions:
	// 1 for b, 1 for arr, 1 for arr's {"d":3} element = 3 non-root partitions.
	if len(root.Partitions) != 3 {
		t.Fatalf("got %d non-root partitions, want 3: keys=%v", len(root.Partitions), partitionKinds(root))
	}
	// The scalar member a is inline (no child partition): its value is the raw
	// variant holding JSON text, not the child variant.
	for _, el := range root.OrderedElements {
		if string(el.Data.Id) == "a" {
			if d := el.Data; len(d.GetChild()) != 0 || string(d.GetRaw()) != "1" {
				t.Fatalf("member a not inline scalar: child=%q raw=%q", d.GetChild(), d.GetRaw())
			}
		}
	}
}

func partitionKinds(root *pb.ReducedEffect) map[string]pb.ValueType {
	m := make(map[string]pb.ValueType)
	for k, p := range root.Partitions {
		m[k] = p.TypeTag
	}
	return m
}
