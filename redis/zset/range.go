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
	"slices"
	"strconv"

	"github.com/swytchdb/swytch/redis/shared"
)

func handleZRange(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("zrange")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	// Parse options
	var byScore, byLex, rev, withScores bool
	offset, count := 0, -1
	var hasLimit bool

	// First two args after key are start/stop
	startArg := cmd.Args[1]
	stopArg := cmd.Args[2]

	// Parse remaining options
	for i := 3; i < len(cmd.Args); i++ {
		opt := shared.ToUpper(cmd.Args[i])
		switch string(opt) {
		case "BYSCORE":
			byScore = true
		case "BYLEX":
			byLex = true
		case "REV":
			rev = true
		case "WITHSCORES":
			withScores = true
		case "LIMIT":
			hasLimit = true
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

	// BYSCORE and BYLEX are mutually exclusive
	if byScore && byLex {
		w.WriteSyntaxError()
		return
	}

	// LIMIT requires BYSCORE or BYLEX
	if hasLimit && !byScore && !byLex {
		w.WriteSyntaxError()
		return
	}

	// BYLEX and WITHSCORES cannot be used together
	if byLex && withScores {
		w.WriteSyntaxError()
		return
	}

	// Pre-parse score/lex/index ranges during validation
	var minScore, maxScore float64
	var minScoreEx, maxScoreEx bool
	var minLex, maxLex LexBound
	var startIdx, stopIdx int

	if byScore {
		var ok1, ok2 bool
		minScore, minScoreEx, ok1 = ParseScore(startArg)
		maxScore, maxScoreEx, ok2 = ParseScore(stopArg)
		if !ok1 || !ok2 {
			w.WriteError("ERR min or max is not a float")
			return
		}
	} else if byLex {
		var ok1, ok2 bool
		minLex, ok1 = ParseLexBound(startArg)
		maxLex, ok2 = ParseLexBound(stopArg)
		if !ok1 || !ok2 {
			w.WriteError("ERR min or max not valid string range item")
			return
		}
	} else {
		var err error
		startIdx, err = strconv.Atoi(string(startArg))
		if err != nil {
			w.WriteNotInteger()
			return
		}
		stopIdx, err = strconv.Atoi(string(stopArg))
		if err != nil {
			w.WriteNotInteger()
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
		var entries []shared.ZSetEntry

		if byScore {
			mn, mx := minScore, maxScore
			mnEx, mxEx := minScoreEx, maxScoreEx
			if rev {
				mn, mx = mx, mn
				mnEx, mxEx = mxEx, mnEx
			}
			// Filter by score range
			filtered := make([]shared.ZSetEntry, 0)
			if rev {
				for _, s := range slices.Backward(sorted) {
					if scoreInRange(s.Score, mn, mx, mnEx, mxEx) {
						filtered = append(filtered, s)
					}
				}
			} else {
				for _, e := range sorted {
					if scoreInRange(e.Score, mn, mx, mnEx, mxEx) {
						filtered = append(filtered, e)
					}
				}
			}
			// Apply offset and count
			if offset < 0 {
				filtered = nil
			} else if offset > 0 {
				if offset >= len(filtered) {
					filtered = nil
				} else {
					filtered = filtered[offset:]
				}
			}
			if count >= 0 && count < len(filtered) {
				filtered = filtered[:count]
			}
			entries = filtered
		} else if byLex {
			mn, mx := minLex, maxLex
			if rev {
				mn, mx = mx, mn
			}
			var source []shared.ZSetEntry
			if rev {
				source = make([]shared.ZSetEntry, len(sorted))
				for i, j := 0, len(sorted)-1; i < len(sorted); i, j = i+1, j-1 {
					source[i] = sorted[j]
				}
			} else {
				source = sorted
			}
			filtered := make([]shared.ZSetEntry, 0)
			for _, entry := range source {
				if InLexRange(entry.Member, mn, mx) {
					filtered = append(filtered, entry)
				}
			}
			// Apply offset and count
			if offset < 0 {
				filtered = nil
			} else if offset > 0 {
				if offset >= len(filtered) {
					filtered = nil
				} else {
					filtered = filtered[offset:]
				}
			}
			if count >= 0 && count < len(filtered) {
				filtered = filtered[:count]
			}
			entries = filtered
		} else {
			// By index (rank)
			entries = zsetRangeByIndex(sorted, startIdx, stopIdx, rev)
		}

		if entries == nil {
			w.WriteArray(0)
			return
		}

		writeZSetEntries(w, entries, withScores)
	}
	return
}

func handleZRevRange(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("zrevrange")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	start, err := strconv.Atoi(string(cmd.Args[1]))
	if err != nil {
		w.WriteNotInteger()
		return
	}
	stop, err := strconv.Atoi(string(cmd.Args[2]))
	if err != nil {
		w.WriteNotInteger()
		return
	}

	var withScores bool
	for i := 3; i < len(cmd.Args); i++ {
		opt := shared.ToUpper(cmd.Args[i])
		if string(opt) == "WITHSCORES" {
			withScores = true
		} else {
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
		entries := zsetRangeByIndex(sorted, start, stop, true)
		if entries == nil {
			w.WriteArray(0)
			return
		}

		writeZSetEntries(w, entries, withScores)
	}
	return
}

func handleZRangeByScore(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("zrangebyscore")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	min, minEx, ok1 := ParseScore(cmd.Args[1])
	max, maxEx, ok2 := ParseScore(cmd.Args[2])
	if !ok1 || !ok2 {
		w.WriteError("ERR min or max is not a float")
		return
	}

	var withScores bool
	offset, count := 0, -1

	for i := 3; i < len(cmd.Args); i++ {
		opt := shared.ToUpper(cmd.Args[i])
		switch string(opt) {
		case "WITHSCORES":
			withScores = true
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
		entries := zsetFilterByScore(sorted, min, max, minEx, maxEx, false, offset, count)
		if entries == nil {
			w.WriteArray(0)
			return
		}

		writeZSetEntries(w, entries, withScores)
	}
	return
}

func handleZRevRangeByScore(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("zrevrangebyscore")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	// Note: for ZREVRANGEBYSCORE, max comes before min
	max, maxEx, ok1 := ParseScore(cmd.Args[1])
	min, minEx, ok2 := ParseScore(cmd.Args[2])
	if !ok1 || !ok2 {
		w.WriteError("ERR min or max is not a float")
		return
	}

	var withScores bool
	offset, count := 0, -1

	for i := 3; i < len(cmd.Args); i++ {
		opt := shared.ToUpper(cmd.Args[i])
		switch string(opt) {
		case "WITHSCORES":
			withScores = true
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
		entries := zsetFilterByScore(sorted, min, max, minEx, maxEx, true, offset, count)
		if entries == nil {
			w.WriteArray(0)
			return
		}

		writeZSetEntries(w, entries, withScores)
	}
	return
}

// zsetRangeByIndex extracts entries by rank index range, handling negative indices.
func zsetRangeByIndex(sorted []shared.ZSetEntry, start, stop int, rev bool) []shared.ZSetEntry {
	length := len(sorted)
	if length == 0 {
		return nil
	}

	// Handle negative indices
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	// Clamp
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}

	if start > stop || start >= length {
		return nil
	}

	result := make([]shared.ZSetEntry, 0, stop-start+1)
	if rev {
		for i := length - 1 - start; i >= length-1-stop; i-- {
			if i >= 0 && i < length {
				result = append(result, sorted[i])
			}
		}
	} else {
		for i := start; i <= stop; i++ {
			result = append(result, sorted[i])
		}
	}
	return result
}

// zsetFilterByScore filters sorted members by score range, with optional reverse and LIMIT.
func zsetFilterByScore(sorted []shared.ZSetEntry, min, max float64, minEx, maxEx bool, rev bool, offset, count int) []shared.ZSetEntry {
	filtered := make([]shared.ZSetEntry, 0)
	if rev {
		for _, s := range slices.Backward(sorted) {
			if scoreInRange(s.Score, min, max, minEx, maxEx) {
				filtered = append(filtered, s)
			}
		}
	} else {
		for _, e := range sorted {
			if scoreInRange(e.Score, min, max, minEx, maxEx) {
				filtered = append(filtered, e)
			}
		}
	}

	// Apply offset and count
	if offset < 0 {
		return nil
	}
	if offset > 0 {
		if offset >= len(filtered) {
			return nil
		}
		filtered = filtered[offset:]
	}
	if count >= 0 && count < len(filtered) {
		filtered = filtered[:count]
	}
	return filtered
}

// writeZSetEntries writes entries to the writer with optional WITHSCORES.
func writeZSetEntries(w *shared.Writer, entries []shared.ZSetEntry, withScores bool) {
	if withScores {
		if w.Protocol() == shared.RESP3 {
			w.WriteArray(len(entries))
			for _, entry := range entries {
				w.WriteArray(2)
				w.WriteBulkStringStr(entry.Member)
				w.WriteScore(entry.Score)
			}
		} else {
			w.WriteArray(len(entries) * 2)
			for _, entry := range entries {
				w.WriteBulkStringStr(entry.Member)
				w.WriteScore(entry.Score)
			}
		}
	} else {
		w.WriteArray(len(entries))
		for _, entry := range entries {
			w.WriteBulkStringStr(entry.Member)
		}
	}
}
