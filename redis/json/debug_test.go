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

func TestJSON_Debug(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "d", "$", `{"a":1,"b":2,"c":{"a":10}}`)

	// MEMORY legacy → bare integer; JSONPath → array, one per match.
	if got := h.run(handleJSONDebug, "MEMORY", "d", ".a"); !strings.HasPrefix(got, ":") {
		t.Errorf("MEMORY .a = %q, want bare integer", got)
	}
	if got := h.run(handleJSONDebug, "MEMORY", "d", "$..a"); !strings.HasPrefix(got, "*2\r\n:") {
		t.Errorf("MEMORY $..a = %q, want 2-element integer array", got)
	}
	// Subcommand is case-insensitive.
	if got := h.run(handleJSONDebug, "memory", "d"); !strings.HasPrefix(got, ":") {
		t.Errorf("memory (lowercase) = %q", got)
	}
	// Missing key: legacy → 0, JSONPath → empty array.
	if got := h.run(handleJSONDebug, "MEMORY", "nokey", ".a"); got != ":0\r\n" {
		t.Errorf("MEMORY missing legacy = %q", got)
	}
	if got := h.run(handleJSONDebug, "MEMORY", "nokey", "$.a"); got != "*0\r\n" {
		t.Errorf("MEMORY missing jsonpath = %q", got)
	}
	// Missing path: legacy → error, JSONPath → empty array.
	if got := h.run(handleJSONDebug, "MEMORY", "d", ".nope"); !strings.HasPrefix(got, "-ERR") {
		t.Errorf("MEMORY .nope = %q", got)
	}
	if got := h.run(handleJSONDebug, "MEMORY", "d", "$.nope"); got != "*0\r\n" {
		t.Errorf("MEMORY $.nope = %q", got)
	}
	// HELP → 2-line array.
	if got := h.run(handleJSONDebug, "HELP"); !strings.HasPrefix(got, "*2\r\n") || !strings.Contains(got, "MEMORY") {
		t.Errorf("HELP = %q", got)
	}
	// Unknown subcommand → specific error.
	if got := h.run(handleJSONDebug, "FOO", "d"); !strings.Contains(got, "unknown subcommand") {
		t.Errorf("FOO = %q", got)
	}
}
