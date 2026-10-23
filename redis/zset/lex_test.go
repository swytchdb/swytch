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

import "testing"

func TestParseLexBound(t *testing.T) {
	tests := []struct {
		input string
		want  LexBound
		ok    bool
	}{
		{"-", LexBound{IsMin: true}, true},
		{"+", LexBound{IsMax: true}, true},
		{"[abc", LexBound{Value: "abc", Inclusive: true}, true},
		{"(abc", LexBound{Value: "abc", Inclusive: false}, true},
		{"[", LexBound{Value: "", Inclusive: true}, true},
		{"(", LexBound{Value: "", Inclusive: false}, true},
		{"", LexBound{}, false},
		{"abc", LexBound{}, false},
		{"123", LexBound{}, false},
	}

	for _, tt := range tests {
		bound, ok := ParseLexBound([]byte(tt.input))
		if ok != tt.ok {
			t.Errorf("ParseLexBound(%q): ok = %v, want %v", tt.input, ok, tt.ok)

			continue
		}

		if !ok {
			continue
		}

		if bound != tt.want {
			t.Errorf("ParseLexBound(%q) = %+v, want %+v", tt.input, bound, tt.want)
		}
	}
}

func TestCompareLex(t *testing.T) {
	tests := []struct {
		member string
		bound  LexBound
		want   int
	}{
		// IsMin: member is always > negative infinity
		{"a", LexBound{IsMin: true}, 1},
		{"", LexBound{IsMin: true}, 1},
		// IsMax: member is always < positive infinity
		{"z", LexBound{IsMax: true}, -1},
		{"", LexBound{IsMax: true}, -1},
		// Equal
		{"abc", LexBound{Value: "abc"}, 0},
		// Less than
		{"abc", LexBound{Value: "def"}, -1},
		// Greater than
		{"def", LexBound{Value: "abc"}, 1},
		// Empty string comparisons
		{"", LexBound{Value: ""}, 0},
		{"a", LexBound{Value: ""}, 1},
		{"", LexBound{Value: "a"}, -1},
	}

	for _, tt := range tests {
		got := CompareLex(tt.member, tt.bound)
		if got != tt.want {
			t.Errorf("CompareLex(%q, %+v) = %d, want %d", tt.member, tt.bound, got, tt.want)
		}
	}
}

func TestInLexRange(t *testing.T) {
	tests := []struct {
		name   string
		member string
		min    LexBound
		max    LexBound
		want   bool
	}{
		{
			"inclusive both bounds",
			"b",
			LexBound{Value: "a", Inclusive: true},
			LexBound{Value: "c", Inclusive: true},
			true,
		},
		{
			"at inclusive min",
			"a",
			LexBound{Value: "a", Inclusive: true},
			LexBound{Value: "c", Inclusive: true},
			true,
		},
		{
			"at inclusive max",
			"c",
			LexBound{Value: "a", Inclusive: true},
			LexBound{Value: "c", Inclusive: true},
			true,
		},
		{
			"at exclusive min - excluded",
			"a",
			LexBound{Value: "a", Inclusive: false},
			LexBound{Value: "c", Inclusive: true},
			false,
		},
		{
			"at exclusive max - excluded",
			"c",
			LexBound{Value: "a", Inclusive: true},
			LexBound{Value: "c", Inclusive: false},
			false,
		},
		{
			"below min",
			"a",
			LexBound{Value: "b", Inclusive: true},
			LexBound{Value: "d", Inclusive: true},
			false,
		},
		{
			"above max",
			"e",
			LexBound{Value: "b", Inclusive: true},
			LexBound{Value: "d", Inclusive: true},
			false,
		},
		{
			"min infinity",
			"a",
			LexBound{IsMin: true},
			LexBound{Value: "z", Inclusive: true},
			true,
		},
		{
			"max infinity",
			"z",
			LexBound{Value: "a", Inclusive: true},
			LexBound{IsMax: true},
			true,
		},
		{
			"full range - and +",
			"m",
			LexBound{IsMin: true},
			LexBound{IsMax: true},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := InLexRange(tt.member, tt.min, tt.max)
			if got != tt.want {
				t.Errorf("InLexRange(%q, %+v, %+v) = %v, want %v",
					tt.member, tt.min, tt.max, got, tt.want)
			}
		})
	}
}
