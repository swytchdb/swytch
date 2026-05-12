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

package set

import (
	"math/rand/v2"

	"github.com/swytchdb/swytch/redis/shared"
)

func handleSCard(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("scard")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getSetSnapshot(cmd, key)
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

func handleSIsMember(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 2 {
		w.WriteWrongNumArguments("sismember")
		return
	}

	key := string(cmd.Args[0])
	member := cmd.Args[1]
	keys = []string{key}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getSetSnapshot(cmd, key)
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

		if snap.NetAdds != nil {
			if _, has := snap.NetAdds[string(member)]; has {
				w.WriteInteger(1)
				return
			}
		}
		w.WriteInteger(0)
	}
	return
}

func handleSMIsMember(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("smismember")
		return
	}

	key := string(cmd.Args[0])
	members := cmd.Args[1:]
	keys = []string{key}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getSetSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteArray(len(members))
			for range members {
				w.WriteInteger(0)
			}
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		w.WriteArray(len(members))
		for _, member := range members {
			if snap.NetAdds != nil {
				if _, has := snap.NetAdds[string(member)]; has {
					w.WriteInteger(1)
					continue
				}
			}
			w.WriteInteger(0)
		}
	}
	return
}

func handleSMembers(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("smembers")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getSetSnapshot(cmd, key)
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

		if snap.NetAdds == nil {
			w.WriteArray(0)
			return
		}

		w.WriteArray(len(snap.NetAdds))
		for m := range snap.NetAdds {
			w.WriteBulkString([]byte(m))
		}
	}
	return
}

func handleSRandMember(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 || len(cmd.Args) > 2 {
		w.WriteWrongNumArguments("srandmember")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	count := int64(1)
	returnArray := false
	if len(cmd.Args) == 2 {
		returnArray = true
		c, ok := shared.ParseInt64(cmd.Args[1])
		if !ok {
			w.WriteError("ERR value is not an integer or out of range")
			return
		}
		if c == -9223372036854775808 {
			w.WriteError("ERR value is out of range")
			return
		}
		count = c
	}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getSetSnapshot(cmd, key)
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

		// Collect members from snapshot
		members := make([][]byte, 0, len(snap.NetAdds))
		for m := range snap.NetAdds {
			members = append(members, []byte(m))
		}

		if len(members) == 0 {
			if returnArray {
				w.WriteArray(0)
			} else {
				w.WriteNullBulkString()
			}
			return
		}

		if returnArray {
			if count > 0 {
				n := min(int(count), len(members))
				shuffled := make([][]byte, len(members))
				copy(shuffled, members)
				for i := len(shuffled) - 1; i > 0; i-- {
					j := rand.IntN(i + 1)
					shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
				}
				w.WriteArray(n)
				for i := range n {
					w.WriteBulkString(shuffled[i])
				}
			} else {
				absCount := int(-count)
				w.WriteArray(absCount)
				for range absCount {
					idx := rand.IntN(len(members))
					w.WriteBulkString(members[idx])
				}
			}
		} else {
			idx := rand.IntN(len(members))
			w.WriteBulkString(members[idx])
		}
	}
	return
}
