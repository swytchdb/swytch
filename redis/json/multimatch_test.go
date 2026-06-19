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
	"strings"
	"testing"
)

// These cover the extended JSONPath grammar (wildcard, slice, union, recursive
// descent, filters) end-to-end through the engine. Expected replies were
// verified against the live ReJSON (redis:8) container.

func TestJSON_MultiMatch_Read(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"a":1,"b":2,"c":{"a":10,"d":3},"arr":[1,2,3,4,5]}`)
	cases := []struct {
		path string
		want string
	}{
		{"$.*", `[1,2,{"a":10,"d":3},[1,2,3,4,5]]`},
		{"$.arr[*]", `[1,2,3,4,5]`},
		{"$..a", `[1,10]`},
		{"$.arr[1:4]", `[2,3,4]`},
		{"$.arr[::2]", `[1,3,5]`},
		{"$.arr[-2:]", `[4,5]`},
		{"$.arr[0,2,4]", `[1,3,5]`},
		{"$['a','b']", `[1,2]`},
	}
	for _, c := range cases {
		if got := h.run(handleJSONGet, "k", c.path); !strings.Contains(got, c.want) {
			t.Errorf("GET %s = %q, want substring %s", c.path, got, c.want)
		}
	}
}

func TestJSON_MultiMatch_Filter(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "store", "$", `{"bikes":[{"price":1000,"id":1},{"price":3000,"id":2},{"price":2500,"id":3}]}`)
	if got := h.run(handleJSONGet, "store", `$.bikes[?(@.price>2000)].id`); !strings.Contains(got, `[2,3]`) {
		t.Errorf("filter id: %q", got)
	}
	if got := h.run(handleJSONGet, "store", `$.bikes[?(@.price>1000 && @.price<3000)].id`); !strings.Contains(got, `[3]`) {
		t.Errorf("filter &&: %q", got)
	}
	if got := h.run(handleJSONGet, "store", `$.bikes[?(@.price<2000 || @.id==2)].id`); !strings.Contains(got, `[1,2]`) {
		t.Errorf("filter ||: %q", got)
	}
	// Regex (=~), unanchored.
	h.run(handleJSONSet, "st", "$", `{"x":[{"n":"alpha"},{"n":"beta"},{"n":"alpina"}]}`)
	if got := h.run(handleJSONGet, "st", `$.x[?(@.n =~ "al.*")].n`); !strings.Contains(got, `["alpha","alpina"]`) {
		t.Errorf("filter regex: %q", got)
	}
	// Bare-@ comparison over a scalar array.
	h.run(handleJSONSet, "sc", "$", `{"x":[1,2,3,4]}`)
	if got := h.run(handleJSONGet, "sc", `$.x[?(@>2)]`); !strings.Contains(got, `[3,4]`) {
		t.Errorf("filter bare @: %q", got)
	}
}

func TestJSON_MultiMatch_NumIncrBy(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"a":1,"b":2,"c":{"a":10}}`)
	// $..a increments root.a (1→6) and c.a (10→15); reply is the JSON array text.
	if got := h.run(handleJSONNumIncrBy, "k", "$..a", "5"); !strings.Contains(got, `[6,15]`) {
		t.Fatalf("NUMINCRBY $..a: %q", got)
	}
	if got := h.run(handleJSONGet, "k", "$..a"); !strings.Contains(got, `[6,15]`) {
		t.Fatalf("after: %q", got)
	}
}

func TestJSON_MultiMatch_StrAppendToggle(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "s", "$", `{"s":"hi","n":5,"t":"yo"}`)
	// Only the two strings append; n yields a null entry.
	if got := h.run(handleJSONStrAppend, "s", "$.*", `"X"`); !strings.Contains(got, "*3\r\n:3\r\n$-1\r\n:3\r\n") {
		t.Fatalf("STRAPPEND $.*: %q", got)
	}
	h.run(handleJSONSet, "b", "$", `{"a":true,"b":false,"n":5}`)
	if got := h.run(handleJSONToggle, "b", "$.*"); !strings.Contains(got, "*3\r\n:0\r\n:1\r\n$-1\r\n") {
		t.Fatalf("TOGGLE $.*: %q", got)
	}
}

func TestJSON_MultiMatch_Array(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"x":[1],"y":[1,2],"n":5}`)
	// ARRAPPEND on every match: new lengths 2,3 and a null for the non-array.
	if got := h.run(handleJSONArrAppend, "k", "$.*", "9"); !strings.Contains(got, "*3\r\n:2\r\n:3\r\n$-1\r\n") {
		t.Fatalf("ARRAPPEND $.*: %q", got)
	}
	if got := h.run(handleJSONGet, "k", "$"); !strings.Contains(got, `{"x":[1,9],"y":[1,2,9],"n":5}`) {
		t.Fatalf("after ARRAPPEND: %q", got)
	}
	h.run(handleJSONSet, "p", "$", `{"x":[1,2,3],"y":[10,20]}`)
	if got := h.run(handleJSONArrPop, "p", "$.*"); !strings.Contains(got, "*2\r\n$1\r\n3\r\n$2\r\n20\r\n") {
		t.Fatalf("ARRPOP $.*: %q", got)
	}
}

func TestJSON_MultiMatch_SetDelClearMerge(t *testing.T) {
	h := newHarness()
	// SET $.* replaces all existing members.
	h.run(handleJSONSet, "k", "$", `{"a":1,"b":2}`)
	if got := h.run(handleJSONSet, "k", "$.*", "9"); !strings.Contains(got, "+OK") {
		t.Fatalf("SET $.*: %q", got)
	}
	if got := h.run(handleJSONGet, "k", "$"); !strings.Contains(got, `{"a":9,"b":9}`) {
		t.Fatalf("after SET $.*: %q", got)
	}
	// DEL $..a removes every 'a' at any depth; reply is the count.
	h.run(handleJSONSet, "d", "$", `{"a":1,"c":{"a":2,"e":{"a":3}}}`)
	if got := h.run(handleJSONDel, "d", "$..a"); !strings.Contains(got, ":3\r\n") {
		t.Fatalf("DEL $..a: %q", got)
	}
	if got := h.run(handleJSONGet, "d", "$"); !strings.Contains(got, `{"c":{"e":{}}}`) {
		t.Fatalf("after DEL: %q", got)
	}
	// CLEAR $.* empties containers and zeroes numbers; strings stay (count 3).
	h.run(handleJSONSet, "c", "$", `{"a":[1,2],"b":{"x":1},"n":5,"s":"hi"}`)
	if got := h.run(handleJSONClear, "c", "$.*"); !strings.Contains(got, ":3\r\n") {
		t.Fatalf("CLEAR $.*: %q", got)
	}
	if got := h.run(handleJSONGet, "c", "$"); !strings.Contains(got, `{"a":[],"b":{},"n":0,"s":"hi"}`) {
		t.Fatalf("after CLEAR: %q", got)
	}
	// MERGE $.* patches every matched object.
	h.run(handleJSONSet, "m", "$", `{"a":{"x":1},"b":{"x":1}}`)
	if got := h.run(handleJSONMerge, "m", "$.*", `{"y":2}`); !strings.Contains(got, "+OK") {
		t.Fatalf("MERGE $.*: %q", got)
	}
	if got := h.run(handleJSONGet, "m", "$"); !strings.Contains(got, `{"a":{"x":1,"y":2},"b":{"x":1,"y":2}}`) {
		t.Fatalf("after MERGE: %q", got)
	}
}

// Legacy paths accept the extended grammar but act on the first match only.
func TestJSON_MultiMatch_LegacyFirst(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"s":"hi","t":"yo"}`)
	// Legacy .* GET → first match bare.
	if got := h.run(handleJSONGet, "k", ".*"); !strings.Contains(got, `"hi"`) {
		t.Fatalf("GET .*: %q", got)
	}
	// Legacy .* STRAPPEND → bare integer (first match only).
	if got := h.run(handleJSONStrAppend, "k", ".*", `"X"`); !strings.Contains(got, ":3\r\n") {
		t.Fatalf("STRAPPEND .*: %q", got)
	}
	if got := h.run(handleJSONGet, "k", "$"); !strings.Contains(got, `{"s":"hiX","t":"yo"}`) {
		t.Fatalf("after legacy STRAPPEND: %q", got)
	}
}
