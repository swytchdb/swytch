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

import "testing"

func TestParsePath_Dialect(t *testing.T) {
	cases := []struct {
		in       string
		jsonpath bool
		nSegs    int
	}{
		{`$`, true, 0},
		{``, false, 0},
		{`.`, false, 0},
		{`$.a`, true, 1},
		{`$.a.b`, true, 2},
		{`.a.b`, false, 2},
		{`a.b`, false, 2},
		{`$['a']`, true, 1},
		{`$["a"]`, true, 1},
		{`$.a[0]`, true, 2},
		{`$.a[-1]`, true, 2},
		{`a[2].b`, false, 3},
	}
	for _, c := range cases {
		p, err := ParsePath(c.in)
		if err != nil {
			t.Fatalf("ParsePath(%q): %v", c.in, err)
		}
		if p.JSONPath != c.jsonpath {
			t.Errorf("ParsePath(%q).JSONPath = %v, want %v", c.in, p.JSONPath, c.jsonpath)
		}
		if len(p.segs) != c.nSegs {
			t.Errorf("ParsePath(%q) segs = %d, want %d", c.in, len(p.segs), c.nSegs)
		}
	}
}

// TestPath_ResolveMulti exercises the extended grammar — wildcard, slice,
// union, recursive descent, and filters — against an assembled tree, asserting
// the matched values in document order. Ordering mirrors RedisJSON (verified
// against the live ReJSON container).
func TestPath_ResolveMulti(t *testing.T) {
	root, _ := Parse([]byte(`{"a":1,"b":2,"c":{"a":10,"d":3},"arr":[1,2,3,4,5],"nested":{"x":{"a":100}}}`))
	store, _ := Parse([]byte(`{"bikes":[{"price":1000,"id":1},{"price":3000,"id":2},{"price":2500,"id":3}]}`))
	cases := []struct {
		doc  *Value
		path string
		want string // JSON array of matches in order
	}{
		{root, `$.*`, `[1,2,{"a":10,"d":3},[1,2,3,4,5],{"x":{"a":100}}]`},
		{root, `$.arr[*]`, `[1,2,3,4,5]`},
		{root, `$..a`, `[1,10,100]`},
		{root, `$.arr[1:4]`, `[2,3,4]`},
		{root, `$.arr[::2]`, `[1,3,5]`},
		{root, `$.arr[-2:]`, `[4,5]`},
		{root, `$.arr[0,2,4]`, `[1,3,5]`},
		{root, `$['a','b']`, `[1,2]`},
		{store, `$.bikes[?(@.price>2000)]`, `[{"price":3000,"id":2},{"price":2500,"id":3}]`},
		{store, `$.bikes[?(@.price>2000)].id`, `[2,3]`},
		{store, `$.bikes[?(@.price>1000 && @.price<3000)].id`, `[3]`},
	}
	for _, c := range cases {
		p, err := ParsePath(c.path)
		if err != nil {
			t.Fatalf("ParsePath(%q): %v", c.path, err)
		}
		got := Serialize(&Value{Kind: KindArray, Arr: p.resolve(c.doc)})
		if string(got) != c.want {
			t.Errorf("resolve(%q) = %s, want %s", c.path, got, c.want)
		}
	}
}

// TestPath_ResolveConcrete verifies the normalized concrete paths a multi-match
// resolution produces — these drive writes via walkToParent.
func TestPath_ResolveConcrete(t *testing.T) {
	root, _ := Parse([]byte(`{"a":1,"b":{"a":2},"arr":[10,20,30]}`))
	p, _ := ParsePath(`$..a`)
	ms := p.resolveMatches(root)
	if len(ms) != 2 {
		t.Fatalf("$..a = %d matches, want 2", len(ms))
	}
	// First match: root .a (one segKey). Second: .b.a (two segKeys).
	if len(ms[0].segs) != 1 || ms[0].segs[0].kind != segKey || ms[0].segs[0].key != "a" {
		t.Errorf("match0 segs = %+v", ms[0].segs)
	}
	if len(ms[1].segs) != 2 || ms[1].segs[1].key != "a" {
		t.Errorf("match1 segs = %+v", ms[1].segs)
	}
	// Slice yields concrete positive indices.
	ps, _ := ParsePath(`$.arr[1:3]`)
	sm := ps.resolveMatches(root)
	if len(sm) != 2 || sm[0].segs[1].kind != segIndex || sm[0].segs[1].idx != 1 || sm[1].segs[1].idx != 2 {
		t.Errorf("arr[1:3] concrete = %+v", sm)
	}
}

func TestPath_Resolve(t *testing.T) {
	root, _ := Parse([]byte(`{"a":1,"b":{"c":2},"arr":[10,20,{"d":3}]}`))
	cases := []struct {
		path string
		want string // serialized match, or "" for no match
	}{
		{`$`, `{"a":1,"b":{"c":2},"arr":[10,20,{"d":3}]}`},
		{`$.a`, `1`},
		{`.a`, `1`},
		{`$.b.c`, `2`},
		{`$.arr[0]`, `10`},
		{`$.arr[-1]`, `{"d":3}`},
		{`$.arr[2].d`, `3`},
		{`$.missing`, ``},
		{`$.a.x`, ``}, // descend into a scalar = no match
		{`$.arr[9]`, ``},
	}
	for _, c := range cases {
		p, err := ParsePath(c.path)
		if err != nil {
			t.Fatalf("ParsePath(%q): %v", c.path, err)
		}
		matches := p.resolve(root)
		if c.want == "" {
			if len(matches) != 0 {
				t.Errorf("resolve(%q) = %d matches, want none", c.path, len(matches))
			}
			continue
		}
		if len(matches) != 1 {
			t.Fatalf("resolve(%q) = %d matches, want 1", c.path, len(matches))
		}
		if got := string(Serialize(matches[0])); got != c.want {
			t.Errorf("resolve(%q) = %s, want %s", c.path, got, c.want)
		}
	}
}
