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

// Wire encodings verified against the live ReJSON (redis:8) container:
// bool → simple string, null → null bulk, int → integer, float/string → bulk,
// array → array led by simple "[", object → array led by simple "{".
func TestJSON_Resp(t *testing.T) {
	h := newHarness()
	h.run(handleJSONSet, "d", "$", `{"i":5,"f":3.14,"s":"hi","b":true,"z":false,"n":null,"arr":[1,2],"obj":{"k":"v"}}`)

	cases := []struct {
		path string
		want string
	}{
		{".i", ":5\r\n"},
		{".b", "+true\r\n"},
		{".z", "+false\r\n"},
		{".n", "$-1\r\n"},
		{".f", "$4\r\n3.14\r\n"},
		{".s", "$2\r\nhi\r\n"},
		{".arr", "*3\r\n+[\r\n:1\r\n:2\r\n"},
		{".obj", "*3\r\n+{\r\n$1\r\nk\r\n$1\r\nv\r\n"},
		// JSONPath wraps in an outer array (one entry per match).
		{"$.arr", "*1\r\n*3\r\n+[\r\n:1\r\n:2\r\n"},
	}
	for _, c := range cases {
		if got := h.run(handleJSONResp, "d", c.path); got != c.want {
			t.Errorf("RESP %s = %q, want %q", c.path, got, c.want)
		}
	}

	// Default path is the legacy root → bare object form.
	if got := h.run(handleJSONResp, "d", ".obj"); !strings.HasPrefix(got, "*3\r\n+{\r\n") {
		t.Errorf("RESP default: %q", got)
	}
	// Missing key → null; existing key + JSONPath no match → empty array; legacy
	// no match → error.
	if got := h.run(handleJSONResp, "nokey", "$"); got != "$-1\r\n" {
		t.Errorf("RESP missing key: %q", got)
	}
	if got := h.run(handleJSONResp, "d", "$.nope"); got != "*0\r\n" {
		t.Errorf("RESP $.nope: %q", got)
	}
	if got := h.run(handleJSONResp, "d", ".nope"); !strings.HasPrefix(got, "-ERR") {
		t.Errorf("RESP .nope: %q", got)
	}
}
