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

package shared

import (
	"math"
	"strconv"
	"time"
)

// ParseBlockingTimeout parses a timeout string for blocking commands.
// Returns the timeout in seconds and an error message if invalid.
// Handles floats, integers, and hex values (0x...).
func ParseBlockingTimeout(s string) (float64, string) {
	timeout, err := strconv.ParseFloat(s, 64)
	if err != nil {
		// Try parsing as integer (handles hex like 0x7FFF...)
		if intVal, intErr := strconv.ParseInt(s, 0, 64); intErr == nil {
			timeout = float64(intVal)
		} else if uintVal, uintErr := strconv.ParseUint(s, 0, 64); uintErr == nil {
			timeout = float64(uintVal)
		} else {
			return 0, "ERR timeout is not a float or out of range"
		}
	}

	if math.IsNaN(timeout) || math.IsInf(timeout, 0) {
		return 0, "ERR timeout is not a float or out of range"
	}

	if timeout < 0 {
		return 0, "ERR timeout is negative"
	}
	// Check for overflow when converting to duration (max ~290 years)
	if timeout > float64(1<<62)/float64(time.Second) {
		return 0, "ERR timeout is out of range"
	}

	return timeout, ""
}
