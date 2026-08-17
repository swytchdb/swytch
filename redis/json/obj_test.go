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

func TestJSON_ObjLen(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"o":{"a":1,"b":2,"c":3},"n":5}`)
	// Legacy → bare integer.
	if got := h.run(handleJSONObjLen, "k", ".o"); !strings.Contains(got, ":3\r\n") {
		t.Fatalf("OBJLEN .o: %q", got)
	}
	// JSONPath → array of lengths.
	if got := h.run(handleJSONObjLen, "k", "$.o"); !strings.Contains(got, "*1\r\n:3") {
		t.Fatalf("OBJLEN $.o: %q", got)
	}
	// Non-object legacy → error; JSONPath → null entry.
	if got := h.run(handleJSONObjLen, "k", ".n"); !strings.Contains(got, "-ERR") {
		t.Fatalf("OBJLEN .n should error: %q", got)
	}
	if got := h.run(handleJSONObjLen, "k", "$.n"); !strings.Contains(got, "*1\r\n$-1") {
		t.Fatalf("OBJLEN $.n should be [null]: %q", got)
	}
	// Missing key → null.
	if got := h.run(handleJSONObjLen, "missing", "$.o"); !strings.Contains(got, "$-1") {
		t.Fatalf("OBJLEN missing key: %q", got)
	}
}

func TestJSON_ObjKeys(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"o":{"a":1,"b":2},"n":5}`)
	// Legacy → bare array of keys, in insertion order.
	if got := h.run(handleJSONObjKeys, "k", ".o"); !strings.Contains(got, "*2\r\n$1\r\na\r\n$1\r\nb\r\n") {
		t.Fatalf("OBJKEYS .o: %q", got)
	}
	// JSONPath → array of key-arrays.
	if got := h.run(handleJSONObjKeys, "k", "$.o"); !strings.Contains(got, "*1\r\n*2\r\n$1\r\na\r\n$1\r\nb") {
		t.Fatalf("OBJKEYS $.o: %q", got)
	}
	// Non-object legacy → error; JSONPath → null entry.
	if got := h.run(handleJSONObjKeys, "k", ".n"); !strings.Contains(got, "-ERR") {
		t.Fatalf("OBJKEYS .n should error: %q", got)
	}
	if got := h.run(handleJSONObjKeys, "k", "$.n"); !strings.Contains(got, "*1\r\n*-1") {
		t.Fatalf("OBJKEYS $.n should be [null]: %q", got)
	}
	// Missing key → null array.
	if got := h.run(handleJSONObjKeys, "missing", "$.o"); !strings.Contains(got, "*-1") {
		t.Fatalf("OBJKEYS missing key: %q", got)
	}
}
