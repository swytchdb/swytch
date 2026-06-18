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

func TestParsePath_Unsupported(t *testing.T) {
	for _, in := range []string{`$.*`, `$..a`, `$[*]`} {
		if _, err := ParsePath(in); err == nil {
			t.Errorf("ParsePath(%q) should be unsupported", in)
		}
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
