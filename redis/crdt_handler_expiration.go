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

package redis

import (
	"github.com/swytchdb/swytch/redis/shared"
)

// parseExpireOptions parses NX/XX/GT/LT options for EXPIRE commands
// Returns nx, xx, gt, lt flags and an error string (empty if successful)
func parseExpireOptions(args [][]byte, startIdx int) (nx, xx, gt, lt bool, errMsg string) {
	for i := startIdx; i < len(args); i++ {
		opt := shared.ToUpper(args[i])
		switch string(opt) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "GT":
			gt = true
		case "LT":
			lt = true
		default:
			return false, false, false, false, "ERR Unsupported option " + string(args[i])
		}
	}

	// Validate option combinations
	// GT and LT are mutually exclusive
	if gt && lt {
		return false, false, false, false, "ERR GT and LT options at the same time are not compatible"
	}
	// NX and XX/GT/LT are mutually exclusive
	if nx && (xx || gt || lt) {
		return false, false, false, false, "ERR NX and XX, GT or LT options at the same time are not compatible"
	}

	return nx, xx, gt, lt, ""
}

// shouldSetExpire checks if expiration should be set based on NX/XX/GT/LT options
// newExpireMs is the new expiration time in milliseconds (absolute timestamp)
// currentExpireMs is the current expiration (0 means no expiry)
func shouldSetExpire(newExpireMs, currentExpireMs int64, nx, xx, gt, lt bool) bool {
	hasExpiry := currentExpireMs != 0

	// NX: only set if key has no expiry
	if nx && hasExpiry {
		return false
	}

	// XX: only set if key already has expiry
	if xx && !hasExpiry {
		return false
	}

	// GT: only set if new expiry > current (no expiry treated as infinite)
	if gt {
		if !hasExpiry {
			// Current has no expiry (infinite), new can't be greater
			return false
		}
		if newExpireMs <= currentExpireMs {
			return false
		}
	}

	// LT: only set if new expiry < current (no expiry treated as infinite)
	if lt {
		if hasExpiry && newExpireMs >= currentExpireMs {
			return false
		}
		// If no expiry (infinite), any finite value is less, so allow
	}

	return true
}
