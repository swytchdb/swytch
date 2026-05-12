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
	"math/rand"
	"strconv"
	"strings"

	"github.com/swytchdb/swytch/redis/shared"
)

func handleZScore(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 2 {
		w.WriteWrongNumArguments("zscore")
		return
	}

	key := string(cmd.Args[0])
	member := string(cmd.Args[1])
	keys = []string{key}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getZSetSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteNullBulkString()
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		score, found := zsetMemberScore(snap, member)
		if !found {
			w.WriteNullBulkString()
			return
		}

		w.WriteScore(score)
	}
	return
}

// handleZMScore implements ZMSCORE key member [member ...]
func handleZMScore(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("zmscore")
		return
	}

	key := string(cmd.Args[0])
	members := cmd.Args[1:]
	keys = []string{key}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getZSetSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			// Return array of nils
			w.WriteArray(len(members))
			for range members {
				w.WriteNullBulkString()
			}
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		w.WriteArray(len(members))
		for _, member := range members {
			score, found := zsetMemberScore(snap, string(member))
			if !found {
				w.WriteNullBulkString()
			} else {
				w.WriteScore(score)
			}
		}
	}
	return
}

func handleZCard(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("zcard")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

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
			count = len(snap.NetAdds)
		}
		w.WriteInteger(int64(count))
	}
	return
}

func handleZRank(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 || len(cmd.Args) > 3 {
		w.WriteWrongNumArguments("zrank")
		return
	}

	key := string(cmd.Args[0])
	member := string(cmd.Args[1])
	keys = []string{key}

	// Check for WITHSCORE option
	withScore := false
	if len(cmd.Args) == 3 {
		if !strings.EqualFold(string(cmd.Args[2]), "withscore") {
			w.WriteError("ERR syntax error")
			return
		}
		withScore = true
	}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getZSetSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			if withScore {
				w.WriteNullArray()
			} else {
				w.WriteNullBulkString()
			}
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		sorted := zsetSortedMembers(snap)
		rank := -1
		for i, e := range sorted {
			if e.Member == member {
				rank = i
				break
			}
		}

		if rank < 0 {
			if withScore {
				w.WriteNullArray()
			} else {
				w.WriteNullBulkString()
			}
			return
		}

		if withScore {
			w.WriteArray(2)
			w.WriteInteger(int64(rank))
			w.WriteScore(sorted[rank].Score)
		} else {
			w.WriteInteger(int64(rank))
		}
	}
	return
}

func handleZRevRank(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 || len(cmd.Args) > 3 {
		w.WriteWrongNumArguments("zrevrank")
		return
	}

	key := string(cmd.Args[0])
	member := string(cmd.Args[1])
	keys = []string{key}

	// Check for WITHSCORE option
	withScore := false
	if len(cmd.Args) == 3 {
		if !strings.EqualFold(string(cmd.Args[2]), "withscore") {
			w.WriteError("ERR syntax error")
			return
		}
		withScore = true
	}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getZSetSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			if withScore {
				w.WriteNullArray()
			} else {
				w.WriteNullBulkString()
			}
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		sorted := zsetSortedMembers(snap)
		// Find member in sorted order, then compute reverse rank
		rank := -1
		for i, e := range sorted {
			if e.Member == member {
				rank = len(sorted) - 1 - i
				break
			}
		}

		if rank < 0 {
			if withScore {
				w.WriteNullArray()
			} else {
				w.WriteNullBulkString()
			}
			return
		}

		if withScore {
			score, _ := zsetMemberScore(snap, member)
			w.WriteArray(2)
			w.WriteInteger(int64(rank))
			w.WriteScore(score)
		} else {
			w.WriteInteger(int64(rank))
		}
	}
	return
}

func handleZCount(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("zcount")
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
		for _, entry := range zsetSortedMembers(snap) {
			if scoreInRange(entry.Score, min, max, minEx, maxEx) {
				count++
			}
		}

		w.WriteInteger(int64(count))
	}
	return
}

func handleZRandMember(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("zrandmember")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	hasCount := len(cmd.Args) > 1
	var count int
	var withScores bool

	if hasCount {
		// Parse count - use ParseInt to handle large values
		count64, err := strconv.ParseInt(string(cmd.Args[1]), 10, 64)
		if err != nil {
			// Check if it's a range error (overflow)
			if numErr, ok := err.(*strconv.NumError); ok && numErr.Err == strconv.ErrRange {
				w.WriteError("ERR value is out of range")
				return
			}
			w.WriteNotInteger()
			return
		}
		// Check if absolute value is too large (would cause allocation issues)
		if count64 < -1<<31 || count64 > 1<<31 {
			w.WriteError("ERR value is out of range")
			return
		}
		count = int(count64)

		// Parse WITHSCORES option
		if len(cmd.Args) > 2 {
			opt := shared.ToUpper(cmd.Args[2])
			if string(opt) == "WITHSCORES" {
				withScores = true
			} else {
				w.WriteSyntaxError()
				return
			}
		}
		if len(cmd.Args) > 3 {
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
			if !hasCount {
				w.WriteNullBulkString()
			} else {
				w.WriteArray(0)
			}
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		// Get all members as a slice for random access
		members := make([]shared.ZSetEntry, 0, len(snap.NetAdds))
		for member, elem := range snap.NetAdds {
			members = append(members, shared.ZSetEntry{Member: member, Score: elem.Data.GetFloatVal()})
		}

		if len(members) == 0 {
			if !hasCount {
				w.WriteNullBulkString()
			} else {
				w.WriteArray(0)
			}
			return
		}

		// No count argument - return single random member
		if !hasCount {
			idx := rand.Intn(len(members))
			w.WriteBulkStringStr(members[idx].Member)
			return
		}

		if count == 0 {
			w.WriteArray(0)
			return
		}

		var result []shared.ZSetEntry

		if count > 0 {
			// Positive count: distinct elements only
			actualCount := min(count, len(members))
			// Shuffle and take first count elements (Fisher-Yates)
			perm := rand.Perm(len(members))
			result = make([]shared.ZSetEntry, actualCount)
			for i := range actualCount {
				result[i] = members[perm[i]]
			}
		} else {
			// Negative count: repetition allowed
			absCount := -count
			result = make([]shared.ZSetEntry, absCount)
			for i := range absCount {
				idx := rand.Intn(len(members))
				result[i] = members[idx]
			}
		}

		if withScores {
			if w.Protocol() == shared.RESP3 {
				// RESP3: array of [member, score] pairs
				w.WriteArray(len(result))
				for _, entry := range result {
					w.WriteArray(2)
					w.WriteBulkStringStr(entry.Member)
					w.WriteScore(entry.Score)
				}
			} else {
				// RESP2: flat array
				w.WriteArray(len(result) * 2)
				for _, entry := range result {
					w.WriteBulkStringStr(entry.Member)
					w.WriteScore(entry.Score)
				}
			}
		} else {
			w.WriteArray(len(result))
			for _, entry := range result {
				w.WriteBulkStringStr(entry.Member)
			}
		}
	}
	return
}

// scoreInRange checks if a score is within the given range, respecting exclusive bounds.
func scoreInRange(score, min, max float64, minEx, maxEx bool) bool {
	if minEx {
		if score <= min {
			return false
		}
	} else {
		if score < min {
			return false
		}
	}
	if maxEx {
		if score >= max {
			return false
		}
	} else {
		if score > max {
			return false
		}
	}
	return true
}
