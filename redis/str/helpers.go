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

package str

import (
	"encoding/hex"
	"math"
	"strings"
	"time"

	"github.com/swytchdb/swytch/redis/shared"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// maxExpireSeconds is the maximum number of seconds that can be safely converted to milliseconds
// without overflow: (math.MaxInt64 / 1000)
const maxExpireSeconds int64 = 9223372036854775

// validateExpireSeconds checks if seconds can be safely converted to milliseconds
// and added to current time without overflow. Returns false if invalid.
func validateExpireSeconds(seconds int64) bool {
	if seconds <= 0 {
		return false
	}
	// Check if seconds * 1000 would overflow
	if seconds > maxExpireSeconds {
		return false
	}
	// Check if now + seconds*1000 would overflow
	nowMs := time.Now().UnixMilli()
	millis := seconds * 1000
	return millis <= math.MaxInt64-nowMs
}

// validateExpireMillis checks if milliseconds can be safely added to current time
// without overflow. Returns false if invalid.
func validateExpireMillis(millis int64) bool {
	if millis <= 0 {
		return false
	}
	// Check if now + millis would overflow
	nowMs := time.Now().UnixMilli()
	return millis <= math.MaxInt64-nowMs
}

// validateExpireAtSeconds checks if a Unix timestamp in seconds can be safely
// converted to milliseconds without overflow.
func validateExpireAtSeconds(timestamp int64) bool {
	if timestamp <= 0 {
		return false
	}
	// Check if timestamp * 1000 would overflow
	if timestamp > maxExpireSeconds {
		return false
	}
	return true
}

// validateExpireAtMillis checks if a Unix timestamp in milliseconds is valid.
func validateExpireAtMillis(timestamp int64) bool {
	return timestamp > 0
}

// validateExpireSecondsRelative validates seconds for EXPIRE command.
// Unlike SET EX which requires positive values, EXPIRE accepts negative values
// (which cause immediate expiration) but must reject values that would overflow.
func validateExpireSecondsRelative(seconds int64) bool {
	// Check if seconds * 1000 would overflow (either direction)
	if seconds > maxExpireSeconds || seconds < -maxExpireSeconds {
		return false
	}
	// Check if now + seconds*1000 would overflow
	nowMs := time.Now().UnixMilli()
	millis := seconds * 1000
	// Check positive overflow
	if millis > 0 && millis > math.MaxInt64-nowMs {
		return false
	}
	// Check negative overflow
	if millis < 0 && nowMs < math.MinInt64-millis {
		return false
	}
	return true
}

// validateExpireMillisRelative validates milliseconds for PEXPIRE command.
// Accepts negative values but rejects values that would overflow.
func validateExpireMillisRelative(millis int64) bool {
	nowMs := time.Now().UnixMilli()
	// Check positive overflow
	if millis > 0 && millis > math.MaxInt64-nowMs {
		return false
	}
	// Check negative overflow
	if millis < 0 && nowMs < math.MinInt64-millis {
		return false
	}
	return true
}

// validateExpireAtSecondsRelative validates Unix timestamp in seconds for EXPIREAT.
// Accepts negative values but rejects values that would overflow when converted to ms.
func validateExpireAtSecondsRelative(timestamp int64) bool {
	// Check if timestamp * 1000 would overflow (either direction)
	if timestamp > maxExpireSeconds || timestamp < -maxExpireSeconds {
		return false
	}
	return true
}

// isValidHexDigest checks if the digest is exactly 16 hex characters
func isValidHexDigest(digest []byte) bool {
	if len(digest) != 16 {
		return false
	}
	for _, c := range digest {
		//nolint:staticcheck // QF1001: De Morgan's form is less readable than "not a hex digit"
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}

// digestEquals compares a computed hash with a provided digest (case-insensitive)
func digestEquals(hash uint64, digest []byte) bool {
	var hashBytes [8]byte
	hashBytes[0] = byte(hash >> 56)
	hashBytes[1] = byte(hash >> 48)
	hashBytes[2] = byte(hash >> 40)
	hashBytes[3] = byte(hash >> 32)
	hashBytes[4] = byte(hash >> 24)
	hashBytes[5] = byte(hash >> 16)
	hashBytes[6] = byte(hash >> 8)
	hashBytes[7] = byte(hash)
	hexStr := hex.EncodeToString(hashBytes[:])
	// Case-insensitive comparison
	return strings.EqualFold(hexStr, string(digest))
}

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
func shouldSetExpire(newExpireMs time.Time, currentExpireMs *timestamppb.Timestamp, nx, xx, gt, lt bool) bool {
	hasExpiry := currentExpireMs != nil

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
		if newExpireMs.Before(currentExpireMs.AsTime()) {
			return false
		}
	}

	// LT: only set if new expiry < current (no expiry treated as infinite)
	if lt {
		if hasExpiry && newExpireMs.After(currentExpireMs.AsTime()) {
			return false
		}
		// If no expiry (infinite), any finite value is less, so allow
	}

	return true
}

// lcsMatch represents a match range for LCS IDX mode
type lcsMatch struct {
	start1, end1 int64 // positions in key1
	start2, end2 int64 // positions in key2
	length       int64 // match length
}

// computeLCS computes the longest common subsequence between two byte slices
// Returns the LCS string, total length, and match ranges for IDX mode
func computeLCS(a, b []byte) ([]byte, int, []lcsMatch) {
	m, n := len(a), len(b)
	if m == 0 || n == 0 {
		return nil, 0, nil
	}

	// Build DP table
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}

	for i := 1; i <= m; i++ {
		for j := 1; j <= n; j++ {
			if a[i-1] == b[j-1] {
				dp[i][j] = dp[i-1][j-1] + 1
			} else {
				dp[i][j] = max(dp[i-1][j], dp[i][j-1])
			}
		}
	}

	lcsLen := dp[m][n]
	if lcsLen == 0 {
		return nil, 0, nil
	}

	// Backtrack to build result and collect matches
	result := make([]byte, lcsLen)
	var matches []lcsMatch
	i, j, k := m, n, lcsLen-1

	// Track current match range
	var inMatch bool
	var matchEnd1, matchEnd2 int64

	for i > 0 && j > 0 {
		if a[i-1] == b[j-1] {
			result[k] = a[i-1]
			k--

			if !inMatch {
				// Starting a new match
				inMatch = true
				matchEnd1 = int64(i - 1)
				matchEnd2 = int64(j - 1)
			}

			i--
			j--

			// Check if match ends (either at beginning or next chars don't match)
			if i == 0 || j == 0 || a[i-1] != b[j-1] {
				// Record the match
				matches = append(matches, lcsMatch{
					start1: int64(i),
					end1:   matchEnd1,
					start2: int64(j),
					end2:   matchEnd2,
					length: matchEnd1 - int64(i) + 1,
				})
				inMatch = false
			}
		} else {
			if inMatch {
				// End current match
				matches = append(matches, lcsMatch{
					start1: int64(i),
					end1:   matchEnd1,
					start2: int64(j),
					end2:   matchEnd2,
					length: matchEnd1 - int64(i) + 1,
				})
				inMatch = false
			}

			if dp[i-1][j] > dp[i][j-1] {
				i--
			} else {
				j--
			}
		}
	}

	return result, lcsLen, matches
}
