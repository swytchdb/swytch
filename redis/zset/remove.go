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

func handleZRem(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("zrem")
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

		removed := 0
		for _, arg := range cmd.Args[1:] {
			member := string(arg)
			if snap.NetAdds != nil {
				if _, has := snap.NetAdds[member]; has {
					removed++
					if err := emitZSetRemove(cmd, key, member); err != nil {
						w.WriteError(err.Error())
						return
					}
				}
			}
		}

		w.WriteInteger(int64(removed))
	}
	return
}

func handleZRemRangeByScore(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("zremrangebyscore")
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

		removed := 0
		if snap.NetAdds != nil {
			for member, elem := range snap.NetAdds {
				score := elem.Data.GetFloatVal()
				if scoreInRange(score, min, max, minEx, maxEx) {
					if err := emitZSetRemove(cmd, key, member); err != nil {
						w.WriteError(err.Error())
						return
					}
					removed++
				}
			}
		}

		w.WriteInteger(int64(removed))
	}
	return
}

func handleZRemRangeByRank(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("zremrangebyrank")
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

		sorted := zsetSortedMembers(snap)
		length := len(sorted)

		// Handle negative indices
		s, e := start, stop
		if s < 0 {
			s = length + s
		}
		if e < 0 {
			e = length + e
		}

		// Clamp to valid range
		if s < 0 {
			s = 0
		}
		if e >= length {
			e = length - 1
		}

		if s > e || s >= length {
			w.WriteInteger(0)
			return
		}

		removed := 0
		for i := s; i <= e; i++ {
			if err := emitZSetRemove(cmd, key, sorted[i].Member); err != nil {
				w.WriteError(err.Error())
				return
			}
			removed++
		}

		w.WriteInteger(int64(removed))
	}
	return
}

func handleZRemRangeByLex(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("zremrangebylex")
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

		removed := 0
		if snap.NetAdds != nil {
			for member := range snap.NetAdds {
				if InLexRange(member, min, max) {
					if err := emitZSetRemove(cmd, key, member); err != nil {
						w.WriteError(err.Error())
						return
					}
					removed++
				}
			}
		}

		w.WriteInteger(int64(removed))
	}
	return
}
