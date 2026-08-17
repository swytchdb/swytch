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

func TestJSON_NumPowBy(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"i":2,"f":2.0,"a":3,"c":{"a":2},"s":"x"}`)
	// Legacy int^int → bare integer.
	if got := h.run(handleJSONNumPowBy, "k", ".i", "10"); !strings.Contains(got, "\r\n1024\r\n") {
		t.Fatalf("NUMPOWBY .i 10: %q", got)
	}
	// JSONPath multi → array of new values (3^2=9, 2^2=4).
	if got := h.run(handleJSONNumPowBy, "k", "$..a", "2"); !strings.Contains(got, "[9,4]") {
		t.Fatalf("NUMPOWBY $..a 2: %q", got)
	}
	// Integer exponentiation by a negative power is a numeric overflow.
	if got := h.run(handleJSONNumPowBy, "k", ".i", "-1"); !strings.Contains(got, "numeric overflow") {
		t.Fatalf("NUMPOWBY .i -1: %q", got)
	}
	// Non-number legacy → error.
	if got := h.run(handleJSONNumPowBy, "k", ".s", "2"); !strings.HasPrefix(got, "-ERR") {
		t.Fatalf("NUMPOWBY .s 2: %q", got)
	}
}

func TestJSON_NumIncrBy(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"a":5,"b":{"c":2},"f":1.5,"s":"x"}`)

	// JSONPath → array-wrapped new value.
	if got := h.run(handleJSONNumIncrBy, "k", "$.a", "3"); !strings.Contains(got, "[8]") {
		t.Fatalf("NUMINCRBY $.a 3: %q", got)
	}
	// Persisted.
	if got := h.run(handleJSONGet, "k", "$.a"); !strings.Contains(got, "[8]") {
		t.Fatalf("GET $.a after incr: %q", got)
	}
	// Legacy path → bare new value.
	if got := h.run(handleJSONNumIncrBy, "k", ".a", "2"); !strings.Contains(got, "\r\n10\r\n") {
		t.Fatalf("NUMINCRBY .a 2: %q", got)
	}
	// Nested.
	if got := h.run(handleJSONNumIncrBy, "k", "$.b.c", "10"); !strings.Contains(got, "[12]") {
		t.Fatalf("NUMINCRBY $.b.c 10: %q", got)
	}
	// Int + float → float result.
	if got := h.run(handleJSONNumIncrBy, "k", "$.f", "1"); !strings.Contains(got, "[2.5]") {
		t.Fatalf("NUMINCRBY $.f 1: %q", got)
	}
	// Negative delta.
	if got := h.run(handleJSONNumIncrBy, "k", "$.a", "-5"); !strings.Contains(got, "[5]") {
		t.Fatalf("NUMINCRBY $.a -5: %q", got)
	}
}

func TestJSON_NumMultBy(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"a":5,"f":2.0}`)
	if got := h.run(handleJSONNumMultBy, "k", "$.a", "4"); !strings.Contains(got, "[20]") {
		t.Fatalf("NUMMULTBY $.a 4: %q", got)
	}
	if got := h.run(handleJSONNumMultBy, "k", ".f", "1.5"); !strings.Contains(got, "\r\n3\r\n") {
		t.Fatalf("NUMMULTBY .f 1.5: %q", got)
	}
}

func TestJSON_NumByNonNumber(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "k", "$", `{"s":"x"}`)
	// Legacy non-number → error.
	if got := h.run(handleJSONNumIncrBy, "k", ".s", "1"); !strings.Contains(got, "-ERR") {
		t.Fatalf("NUMINCRBY .s 1 should error: %q", got)
	}
	// JSONPath non-number → null in the result array, no modification.
	if got := h.run(handleJSONNumIncrBy, "k", "$.s", "1"); !strings.Contains(got, "[null]") {
		t.Fatalf("NUMINCRBY $.s 1 should be [null]: %q", got)
	}
	if got := h.run(handleJSONGet, "k", "$.s"); !strings.Contains(got, `["x"]`) {
		t.Fatalf("$.s unchanged: %q", got)
	}
}

func TestJSON_NumByErrors(t *testing.T) {
	h := newHarness()
	// Missing key → error.
	if got := h.run(handleJSONNumIncrBy, "missing", "$.a", "1"); !strings.Contains(got, "-ERR") {
		t.Fatalf("NUMINCRBY on missing key should error: %q", got)
	}
	h.run(handleJSONSet, "k", "$", `{"a":1}`)
	// Non-number delta → error.
	if got := h.run(handleJSONNumIncrBy, "k", "$.a", "abc"); !strings.Contains(got, "-ERR") {
		t.Fatalf("NUMINCRBY with non-number delta should error: %q", got)
	}
	// Legacy missing path → error.
	if got := h.run(handleJSONNumIncrBy, "k", ".missing", "1"); !strings.Contains(got, "-ERR") {
		t.Fatalf("NUMINCRBY on missing legacy path should error: %q", got)
	}
	// JSONPath missing path → empty array (no error).
	if got := h.run(handleJSONNumIncrBy, "k", "$.missing", "1"); !strings.Contains(got, "[]") {
		t.Fatalf("NUMINCRBY on missing JSONPath should be []: %q", got)
	}
}
