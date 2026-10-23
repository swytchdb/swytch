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

type LexBound struct {
	Value     string
	Inclusive bool
	IsMin     bool // true for "-" (negative infinity)
	IsMax     bool // true for "+" (positive infinity)
}

// ParseLexBound parses a lexicographic range bound
// Valid formats: "-", "+", "[value", "(value"
func ParseLexBound(s []byte) (LexBound, bool) {
	if len(s) == 0 {
		return LexBound{}, false
	}

	str := string(s)

	switch str {
	case "-":
		return LexBound{IsMin: true}, true
	case "+":
		return LexBound{IsMax: true}, true
	}

	if str[0] == '[' {
		return LexBound{Value: str[1:], Inclusive: true}, true
	}

	if str[0] == '(' {
		return LexBound{Value: str[1:], Inclusive: false}, true
	}

	return LexBound{}, false
}

// CompareLex compares a member against a lex bound
// Returns: -1 if member < bound, 0 if equal, 1 if member > bound
func CompareLex(member string, bound LexBound) int {
	if bound.IsMin {
		return 1 // member is always > negative infinity
	}

	if bound.IsMax {
		return -1 // member is always < positive infinity
	}

	if member < bound.Value {
		return -1
	}

	if member > bound.Value {
		return 1
	}

	return 0
}

// InLexRange checks if a member is within the lex range from minBound to maxBound.
func InLexRange(member string, minBound, maxBound LexBound) bool {
	// Check min bound
	cmp := CompareLex(member, minBound)
	if cmp < 0 {
		return false
	}

	if cmp == 0 && !minBound.Inclusive {
		return false
	}

	// Check max bound
	cmp = CompareLex(member, maxBound)
	if cmp > 0 {
		return false
	}

	if cmp == 0 && !maxBound.Inclusive {
		return false
	}

	return true
}
