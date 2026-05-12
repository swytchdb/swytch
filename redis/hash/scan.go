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

package hash

import (
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"

	"github.com/swytchdb/swytch/crdt"
	"github.com/swytchdb/swytch/redis/shared"
)

func handleHScan(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("hscan")
		return
	}

	key := string(cmd.Args[0])
	cursor, ok := shared.ParseUint64(cmd.Args[1])
	if !ok {
		w.WriteError("ERR invalid cursor")
		return
	}

	// Parse options
	pattern := "*"
	count := 10

	for i := 2; i < len(cmd.Args)-1; i += 2 {
		opt := shared.ToUpper(cmd.Args[i])
		switch string(opt) {
		case "MATCH":
			pattern = string(cmd.Args[i+1])
		case "COUNT":
			c, ok := shared.ParseInt64(cmd.Args[i+1])
			if !ok || c <= 0 {
				w.WriteNotInteger()
				return
			}
			count = int(c)
		case "NOVALUES":
			// Not implemented yet
		}
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			// Return empty result with cursor 0
			w.WriteArray(2)
			w.WriteBulkStringStr("0")
			w.WriteArray(0)
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		// Get all matching fields from snapshot
		var matchedFields []string
		if snap.NetAdds != nil {
			for field := range snap.NetAdds {
				if crdt.MatchGlobPattern(field, pattern) {
					matchedFields = append(matchedFields, field)
				}
			}
		}

		// Sort for deterministic iteration order
		sort.Strings(matchedFields)

		// Simple cursor-based pagination
		startIdx := int(cursor)
		if startIdx >= len(matchedFields) {
			// Return empty result with cursor 0
			w.WriteArray(2)
			w.WriteBulkStringStr("0")
			w.WriteArray(0)
			return
		}

		endIdx := min(startIdx+count, len(matchedFields))

		// Calculate next cursor
		var nextCursor string
		if endIdx >= len(matchedFields) {
			nextCursor = "0"
		} else {
			nextCursor = strconv.Itoa(endIdx)
		}

		fields := matchedFields[startIdx:endIdx]

		w.WriteArray(2)
		w.WriteBulkStringStr(nextCursor)
		w.WriteArray(len(fields) * 2) // field-value pairs
		for _, field := range fields {
			w.WriteBulkStringStr(field)
			fieldVal, _ := hashFieldValue(snap, field)
			w.WriteBulkString(fieldVal)
		}
	}
	return
}

func handleHRandField(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 || len(cmd.Args) > 3 {
		w.WriteWrongNumArguments("hrandfield")
		return
	}

	key := string(cmd.Args[0])

	count := int64(1)
	returnArray := false
	withValues := false

	if len(cmd.Args) >= 2 {
		returnArray = true
		c, ok := shared.ParseInt64(cmd.Args[1])
		if !ok {
			w.WriteError("ERR value is not an integer or out of range")
			return
		}
		// Check for overflow: reject values outside 32-bit range
		if c > 2147483647 || c < -2147483647 {
			w.WriteError("ERR value is out of range")
			return
		}
		count = c
	}

	if len(cmd.Args) == 3 {
		if strings.ToUpper(string(cmd.Args[2])) != "WITHVALUES" {
			w.WriteSyntaxError()
			return
		}
		withValues = true
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			if returnArray {
				w.WriteArray(0)
			} else {
				w.WriteNullBulkString()
			}
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		// Collect fields from snapshot
		var fields []string
		if snap.NetAdds != nil {
			for field := range snap.NetAdds {
				fields = append(fields, field)
			}
		}

		if len(fields) == 0 {
			if returnArray {
				w.WriteArray(0)
			} else {
				w.WriteNullBulkString()
			}
			return
		}

		if returnArray {
			if count > 0 {
				// Positive count: return distinct fields in random order
				n := min(int(count), len(fields))
				// Fisher-Yates shuffle to get random selection
				shuffled := make([]string, len(fields))
				copy(shuffled, fields)
				for i := len(shuffled) - 1; i > 0; i-- {
					j := rand.IntN(i + 1)
					shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
				}
				if withValues {
					if w.Protocol() == shared.RESP3 {
						w.WriteArray(n)
						for i := range n {
							w.WriteArray(2)
							w.WriteBulkStringStr(shuffled[i])
							fieldVal, _ := hashFieldValue(snap, shuffled[i])
							w.WriteBulkString(fieldVal)
						}
					} else {
						w.WriteArray(n * 2)
						for i := range n {
							w.WriteBulkStringStr(shuffled[i])
							fieldVal, _ := hashFieldValue(snap, shuffled[i])
							w.WriteBulkString(fieldVal)
						}
					}
				} else {
					w.WriteArray(n)
					for i := range n {
						w.WriteBulkStringStr(shuffled[i])
					}
				}
			} else {
				// Negative count: return |count| fields with possible repeats
				absCount := int(-count)
				if withValues {
					if w.Protocol() == shared.RESP3 {
						w.WriteArray(absCount)
						for range absCount {
							idx := rand.IntN(len(fields))
							w.WriteArray(2)
							w.WriteBulkStringStr(fields[idx])
							fieldVal, _ := hashFieldValue(snap, fields[idx])
							w.WriteBulkString(fieldVal)
						}
					} else {
						w.WriteArray(absCount * 2)
						for range absCount {
							idx := rand.IntN(len(fields))
							w.WriteBulkStringStr(fields[idx])
							fieldVal, _ := hashFieldValue(snap, fields[idx])
							w.WriteBulkString(fieldVal)
						}
					}
				} else {
					w.WriteArray(absCount)
					for range absCount {
						idx := rand.IntN(len(fields))
						w.WriteBulkStringStr(fields[idx])
					}
				}
			}
		} else {
			// No count: return single random field
			idx := rand.IntN(len(fields))
			w.WriteBulkStringStr(fields[idx])
		}
	}
	return
}
