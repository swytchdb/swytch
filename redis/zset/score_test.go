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

package zset

import (
	"math"
	"testing"
)

func TestParseScore(t *testing.T) {
	tests := []struct {
		input     string
		score     float64
		exclusive bool
		ok        bool
	}{
		{"-inf", math.Inf(-1), false, true},
		{"+inf", math.Inf(1), false, true},
		{"inf", math.Inf(1), false, true},
		{"(-inf", math.Inf(-1), true, true},
		{"(+inf", math.Inf(1), true, true},
		{"5.5", 5.5, false, true},
		{"(5.5", 5.5, true, true},
		{"-3.14", -3.14, false, true},
		{"(-3.14", -3.14, true, true},
		{"0", 0, false, true},
		{"", 0, false, false},
		{"abc", 0, false, false},
	}

	for _, tt := range tests {
		score, exclusive, ok := ParseScore([]byte(tt.input))
		if ok != tt.ok {
			t.Errorf("ParseScore(%q): expected ok=%v, got %v", tt.input, tt.ok, ok)

			continue
		}

		if !ok {
			continue
		}

		if exclusive != tt.exclusive {
			t.Errorf("ParseScore(%q): expected exclusive=%v, got %v", tt.input, tt.exclusive, exclusive)
		}

		if math.IsInf(tt.score, 1) {
			if !math.IsInf(score, 1) {
				t.Errorf("ParseScore(%q): expected +Inf, got %v", tt.input, score)
			}
		} else if math.IsInf(tt.score, -1) {
			if !math.IsInf(score, -1) {
				t.Errorf("ParseScore(%q): expected -Inf, got %v", tt.input, score)
			}
		} else if score != tt.score {
			t.Errorf("ParseScore(%q): expected %v, got %v", tt.input, tt.score, score)
		}
	}
}
