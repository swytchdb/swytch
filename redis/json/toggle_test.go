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

func TestJSON_Toggle(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"b":true,"n":5}`)
	// JSONPath true → 0 (the new value), and the document flips.
	if got := h.run(handleJSONToggle, "k", "$.b"); !strings.Contains(got, "*1\r\n:0") {
		t.Fatalf("TOGGLE $.b: %q", got)
	}
	if got := h.run(handleJSONGet, "k", ".b"); !strings.Contains(got, "false") {
		t.Fatalf("after toggle: %q", got)
	}
	// Legacy false → 1.
	if got := h.run(handleJSONToggle, "k", ".b"); !strings.Contains(got, ":1\r\n") {
		t.Fatalf("TOGGLE .b: %q", got)
	}
	// Non-boolean legacy → error; JSONPath → null entry.
	if got := h.run(handleJSONToggle, "k", ".n"); !strings.Contains(got, "-ERR") {
		t.Fatalf("TOGGLE .n should error: %q", got)
	}
	if got := h.run(handleJSONToggle, "k", "$.n"); !strings.Contains(got, "*1\r\n$-1") {
		t.Fatalf("TOGGLE $.n should be [null]: %q", got)
	}
	// Missing key → error.
	if got := h.run(handleJSONToggle, "missing", "$.b"); !strings.Contains(got, "-ERR") {
		t.Fatalf("TOGGLE missing key should error: %q", got)
	}
}
