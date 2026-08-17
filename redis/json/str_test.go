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

func TestJSON_StrLen(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"s":"hello","n":5}`)
	// Legacy → bare integer.
	if got := h.run(handleJSONStrLen, "k", ".s"); !strings.Contains(got, ":5\r\n") {
		t.Fatalf("STRLEN .s: %q", got)
	}
	// JSONPath → array of lengths.
	if got := h.run(handleJSONStrLen, "k", "$.s"); !strings.Contains(got, "*1\r\n:5") {
		t.Fatalf("STRLEN $.s: %q", got)
	}
	// Non-string legacy → error; JSONPath → null entry.
	if got := h.run(handleJSONStrLen, "k", ".n"); !strings.Contains(got, "-ERR") {
		t.Fatalf("STRLEN .n should error: %q", got)
	}
	if got := h.run(handleJSONStrLen, "k", "$.n"); !strings.Contains(got, "*1\r\n$-1") {
		t.Fatalf("STRLEN $.n should be [null]: %q", got)
	}
	// Missing key → null.
	if got := h.run(handleJSONStrLen, "missing", "$.s"); !strings.Contains(got, "$-1") {
		t.Fatalf("STRLEN missing key: %q", got)
	}
}

func TestJSON_StrAppend(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"s":"hello"}`)
	// Append → new length, legacy bare integer.
	if got := h.run(handleJSONStrAppend, "k", ".s", `" world"`); !strings.Contains(got, ":11\r\n") {
		t.Fatalf("STRAPPEND .s: %q", got)
	}
	if got := h.run(handleJSONGet, "k", ".s"); !strings.Contains(got, `"hello world"`) {
		t.Fatalf("after append: %q", got)
	}
	// JSONPath → array of lengths.
	if got := h.run(handleJSONStrAppend, "k", "$.s", `"!"`); !strings.Contains(got, "*1\r\n:12") {
		t.Fatalf("STRAPPEND $.s: %q", got)
	}
	// Root string with the path omitted.
	h.run(handleJSONSet, "r", "$", `"ab"`)
	if got := h.run(handleJSONStrAppend, "r", `"cd"`); !strings.Contains(got, "*1\r\n:4") {
		t.Fatalf("STRAPPEND root no-path: %q", got)
	}
	// A non-string append value is rejected.
	if got := h.run(handleJSONStrAppend, "k", ".s", `5`); !strings.Contains(got, "-ERR") {
		t.Fatalf("STRAPPEND non-string value should error: %q", got)
	}
	// Non-string target: legacy errors, JSONPath null.
	h.run(handleJSONSet, "k", "$.n", `7`)
	if got := h.run(handleJSONStrAppend, "k", ".n", `"x"`); !strings.Contains(got, "-ERR") {
		t.Fatalf("STRAPPEND non-string target should error: %q", got)
	}
	if got := h.run(handleJSONStrAppend, "k", "$.n", `"x"`); !strings.Contains(got, "*1\r\n$-1") {
		t.Fatalf("STRAPPEND $.n should be [null]: %q", got)
	}
}
