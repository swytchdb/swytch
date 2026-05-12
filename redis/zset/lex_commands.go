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
	"strconv"

	"github.com/swytchdb/swytch/redis/shared"
)

func handleZLexCount(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("zlexcount")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	min, ok1 := ParseLexBound(cmd.Args[1])
	max, ok2 := ParseLexBound(cmd.Args[2])
	if !ok1 || !ok2 {
		w.WriteError("ERR min or max not valid string range item")
		return
	}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getZSetSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteInteger(0)
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		count := 0
		if snap.NetAdds != nil {
			for member := range snap.NetAdds {
				if InLexRange(member, min, max) {
					count++
				}
			}
		}

		w.WriteInteger(int64(count))
	}
	return
}

func handleZRangeByLex(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("zrangebylex")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	min, ok1 := ParseLexBound(cmd.Args[1])
	max, ok2 := ParseLexBound(cmd.Args[2])
	if !ok1 || !ok2 {
		w.WriteError("ERR min or max not valid string range item")
		return
	}

	offset, count := 0, -1

	for i := 3; i < len(cmd.Args); i++ {
		opt := shared.ToUpper(cmd.Args[i])
		switch string(opt) {
		case "LIMIT":
			if i+2 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			var err error
			offset, err = strconv.Atoi(string(cmd.Args[i+1]))
			if err != nil {
				w.WriteNotInteger()
				return
			}
			count, err = strconv.Atoi(string(cmd.Args[i+2]))
			if err != nil {
				w.WriteNotInteger()
				return
			}
			i += 2
		default:
			w.WriteSyntaxError()
			return
		}
	}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getZSetSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteArray(0)
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		sorted := zsetSortedMembers(snap)

		// Filter by lex range
		result := make([]string, 0)
		for _, entry := range sorted {
			if InLexRange(entry.Member, min, max) {
				result = append(result, entry.Member)
			}
		}

		// Apply offset and count
		if offset < 0 {
			w.WriteArray(0)
			return
		}
		if offset > 0 {
			if offset >= len(result) {
				w.WriteArray(0)
				return
			}
			result = result[offset:]
		}
		if count >= 0 && count < len(result) {
			result = result[:count]
		}

		w.WriteArray(len(result))
		for _, member := range result {
			w.WriteBulkStringStr(member)
		}
	}
	return
}

func handleZRevRangeByLex(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("zrevrangebylex")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	// Note: for ZREVRANGEBYLEX, max comes before min
	max, ok1 := ParseLexBound(cmd.Args[1])
	min, ok2 := ParseLexBound(cmd.Args[2])
	if !ok1 || !ok2 {
		w.WriteError("ERR min or max not valid string range item")
		return
	}

	offset, count := 0, -1

	for i := 3; i < len(cmd.Args); i++ {
		opt := shared.ToUpper(cmd.Args[i])
		switch string(opt) {
		case "LIMIT":
			if i+2 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			var err error
			offset, err = strconv.Atoi(string(cmd.Args[i+1]))
			if err != nil {
				w.WriteNotInteger()
				return
			}
			count, err = strconv.Atoi(string(cmd.Args[i+2]))
			if err != nil {
				w.WriteNotInteger()
				return
			}
			i += 2
		default:
			w.WriteSyntaxError()
			return
		}
	}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getZSetSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteArray(0)
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		sorted := zsetSortedMembers(snap)
		// Reverse the sorted order
		reversed := make([]shared.ZSetEntry, len(sorted))
		for i, j := 0, len(sorted)-1; i < len(sorted); i, j = i+1, j-1 {
			reversed[i] = sorted[j]
		}

		// Filter by lex range
		result := make([]string, 0)
		for _, entry := range reversed {
			if InLexRange(entry.Member, min, max) {
				result = append(result, entry.Member)
			}
		}

		// Apply offset and count
		if offset < 0 {
			w.WriteArray(0)
			return
		}
		if offset > 0 {
			if offset >= len(result) {
				w.WriteArray(0)
				return
			}
			result = result[offset:]
		}
		if count >= 0 && count < len(result) {
			result = result[:count]
		}

		w.WriteArray(len(result))
		for _, member := range result {
			w.WriteBulkStringStr(member)
		}
	}
	return
}
