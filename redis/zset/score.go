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
	"strconv"
)

// ParseScore parses a score string, handling -inf, +inf, and exclusive markers
func ParseScore(s []byte) (score float64, exclusive bool, ok bool) {
	if len(s) == 0 {
		return 0, false, false
	}

	str := string(s)

	// Check for exclusive marker
	if str[0] == '(' {
		exclusive = true
		str = str[1:]
	}

	// Handle special values
	switch str {
	case "-inf":
		return math.Inf(-1), exclusive, true
	case "+inf", "inf":
		return math.Inf(1), exclusive, true
	}

	// Parse as float
	f, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return 0, false, false
	}
	// Reject NaN values
	if math.IsNaN(f) {
		return 0, false, false
	}

	return f, exclusive, true
}
