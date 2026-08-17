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

func TestJSON_ArrLen(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"a":[1,2,3],"s":"x","e":[]}`)
	// JSONPath → array of lengths.
	if got := h.run(handleJSONArrLen, "k", "$.a"); !strings.Contains(got, ":3") {
		t.Fatalf("ARRLEN $.a: %q", got)
	}
	// Legacy → bare integer.
	if got := h.run(handleJSONArrLen, "k", ".a"); !strings.Contains(got, ":3\r\n") {
		t.Fatalf("ARRLEN .a: %q", got)
	}
	// Empty array.
	if got := h.run(handleJSONArrLen, "k", ".e"); !strings.Contains(got, ":0\r\n") {
		t.Fatalf("ARRLEN .e: %q", got)
	}
	// Non-array legacy → error; JSONPath → null entry.
	if got := h.run(handleJSONArrLen, "k", ".s"); !strings.Contains(got, "-ERR") {
		t.Fatalf("ARRLEN .s should error: %q", got)
	}
	if got := h.run(handleJSONArrLen, "k", "$.s"); !strings.Contains(got, "*1\r\n$-1") {
		t.Fatalf("ARRLEN $.s should be [null]: %q", got)
	}
	// Missing key → null.
	if got := h.run(handleJSONArrLen, "missing", "$.a"); !strings.Contains(got, "$-1") {
		t.Fatalf("ARRLEN missing key: %q", got)
	}
}

func TestJSON_ArrAppend(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"a":[1,2]}`)
	// Append two values → new length 4.
	if got := h.run(handleJSONArrAppend, "k", "$.a", "3", "4"); !strings.Contains(got, ":4") {
		t.Fatalf("ARRAPPEND: %q", got)
	}
	if got := h.run(handleJSONGet, "k", "$.a"); !strings.Contains(got, "[[1,2,3,4]]") {
		t.Fatalf("after append: %q", got)
	}
	// Append a container value.
	if got := h.run(handleJSONArrAppend, "k", ".a", `{"x":1}`); !strings.Contains(got, ":5\r\n") {
		t.Fatalf("ARRAPPEND container: %q", got)
	}
	if got := h.run(handleJSONGet, "k", ".a"); !strings.Contains(got, `[1,2,3,4,{"x":1}]`) {
		t.Fatalf("after container append: %q", got)
	}
}

func TestJSON_ArrInsert(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"a":[1,4]}`)
	// Insert 2,3 before index 1.
	if got := h.run(handleJSONArrInsert, "k", "$.a", "1", "2", "3"); !strings.Contains(got, ":4") {
		t.Fatalf("ARRINSERT: %q", got)
	}
	if got := h.run(handleJSONGet, "k", ".a"); !strings.Contains(got, "[1,2,3,4]") {
		t.Fatalf("after insert: %q", got)
	}
	// Insert at the tail via index == len.
	if got := h.run(handleJSONArrInsert, "k", ".a", "4", "5"); !strings.Contains(got, ":5\r\n") {
		t.Fatalf("ARRINSERT tail: %q", got)
	}
	if got := h.run(handleJSONGet, "k", ".a"); !strings.Contains(got, "[1,2,3,4,5]") {
		t.Fatalf("after tail insert: %q", got)
	}
	// Negative index inserts from the end.
	if got := h.run(handleJSONArrInsert, "k", ".a", "-1", "9"); !strings.Contains(got, ":6\r\n") {
		t.Fatalf("ARRINSERT neg: %q", got)
	}
	if got := h.run(handleJSONGet, "k", ".a"); !strings.Contains(got, "[1,2,3,4,9,5]") {
		t.Fatalf("after neg insert: %q", got)
	}
	// Out-of-range index → error.
	if got := h.run(handleJSONArrInsert, "k", ".a", "99", "0"); !strings.Contains(got, "-ERR") {
		t.Fatalf("ARRINSERT out of range should error: %q", got)
	}
}

func TestJSON_ArrPop(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"a":[1,2,3,4]}`)
	// Default pops the last element (legacy bare value).
	if got := h.run(handleJSONArrPop, "k", ".a"); !strings.Contains(got, "\r\n4\r\n") {
		t.Fatalf("ARRPOP default: %q", got)
	}
	// Pop at index 0.
	if got := h.run(handleJSONArrPop, "k", ".a", "0"); !strings.Contains(got, "\r\n1\r\n") {
		t.Fatalf("ARRPOP 0: %q", got)
	}
	if got := h.run(handleJSONGet, "k", ".a"); !strings.Contains(got, "[2,3]") {
		t.Fatalf("after pops: %q", got)
	}
	// JSONPath pop → array-wrapped value.
	if got := h.run(handleJSONArrPop, "k", "$.a"); !strings.Contains(got, "*1\r\n$1\r\n3") {
		t.Fatalf("ARRPOP $.a: %q", got)
	}
	// Pop until empty, then null.
	h.run(handleJSONArrPop, "k", ".a")
	if got := h.run(handleJSONArrPop, "k", ".a"); !strings.Contains(got, "$-1") {
		t.Fatalf("ARRPOP empty should be null: %q", got)
	}
}

func TestJSON_ArrTrim(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"a":[0,1,2,3,4,5]}`)
	// Keep [1,3].
	if got := h.run(handleJSONArrTrim, "k", "$.a", "1", "3"); !strings.Contains(got, ":3") {
		t.Fatalf("ARRTRIM: %q", got)
	}
	if got := h.run(handleJSONGet, "k", ".a"); !strings.Contains(got, "[1,2,3]") {
		t.Fatalf("after trim: %q", got)
	}
	// start > stop empties the array.
	if got := h.run(handleJSONArrTrim, "k", ".a", "2", "1"); !strings.Contains(got, ":0\r\n") {
		t.Fatalf("ARRTRIM empty: %q", got)
	}
	if got := h.run(handleJSONGet, "k", "$.a"); !strings.Contains(got, "[[]]") {
		t.Fatalf("after empty trim: %q", got)
	}
}

func TestJSON_ArrIndex(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"a":[10,20,30,20]}`)
	if got := h.run(handleJSONArrIndex, "k", ".a", "20"); !strings.Contains(got, ":1\r\n") {
		t.Fatalf("ARRINDEX 20: %q", got)
	}
	if got := h.run(handleJSONArrIndex, "k", ".a", "99"); !strings.Contains(got, ":-1\r\n") {
		t.Fatalf("ARRINDEX missing: %q", got)
	}
	// Search starting past the first match finds the second.
	if got := h.run(handleJSONArrIndex, "k", ".a", "20", "2"); !strings.Contains(got, ":3\r\n") {
		t.Fatalf("ARRINDEX 20 from 2: %q", got)
	}
	// JSONPath → array of indices.
	if got := h.run(handleJSONArrIndex, "k", "$.a", "30"); !strings.Contains(got, "*1\r\n:2") {
		t.Fatalf("ARRINDEX $.a 30: %q", got)
	}
}
