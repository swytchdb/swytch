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
	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/redis/shared"
)

// getSetMembers extracts the member set from a snapshot as a map.
func getSetMembers(snap *pb.ReducedEffect) map[string]struct{} {
	if snap == nil || snap.NetAdds == nil {
		return nil
	}
	members := make(map[string]struct{}, len(snap.NetAdds))
	for m := range snap.NetAdds {
		members[m] = struct{}{}
	}
	return members
}

func handleSInter(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("sinter")
		return
	}

	keys = make([]string, len(cmd.Args))
	for i, arg := range cmd.Args {
		keys[i] = string(arg)
	}

	valid = true
	runner = func() {
		// Type-check all keys first, collecting snapshots
		sets := make([]map[string]struct{}, len(keys))
		hasEmpty := false
		for i, key := range keys {
			snap, _, exists, wrongType, err := getSetSnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if exists && wrongType {
				w.WriteWrongType()
				return
			}
			if !exists {
				hasEmpty = true
			} else {
				sets[i] = getSetMembers(snap)
			}
		}

		// Any non-existent key means empty intersection
		if hasEmpty {
			w.WriteArray(0)
			return
		}

		// Compute intersection
		result := make(map[string]struct{})
		for m := range sets[0] {
			inAll := true
			for _, other := range sets[1:] {
				if other == nil {
					inAll = false
					break
				}
				if _, has := other[m]; !has {
					inAll = false
					break
				}
			}
			if inAll {
				result[m] = struct{}{}
			}
		}

		w.WriteArray(len(result))
		for m := range result {
			w.WriteBulkString([]byte(m))
		}
	}
	return
}

func handleSInterStore(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("sinterstore")
		return
	}

	keys = make([]string, len(cmd.Args))
	for i, arg := range cmd.Args {
		keys[i] = string(arg)
	}
	destKey := keys[0]

	valid = true
	runner = func() {
		// Type-check all source keys
		srcSets := make([]map[string]struct{}, 0, len(keys)-1)
		for _, key := range keys[1:] {
			snap, _, exists, wrongType, err := getSetSnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if exists && wrongType {
				w.WriteWrongType()
				return
			}
			if !exists {
				srcSets = append(srcSets, nil)
			} else {
				srcSets = append(srcSets, getSetMembers(snap))
			}
		}

		// Compute intersection
		var result map[string]struct{}
		if srcSets[0] == nil {
			result = map[string]struct{}{}
		} else {
			result = make(map[string]struct{})
			for m := range srcSets[0] {
				inAll := true
				for _, other := range srcSets[1:] {
					if other == nil {
						inAll = false
						break
					}
					if _, has := other[m]; !has {
						inAll = false
						break
					}
				}
				if inAll {
					result[m] = struct{}{}
				}
			}
		}

		writeStoreResult(cmd, w, destKey, result)
	}
	return
}

func handleSInterCard(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("sintercard")
		return
	}

	numKeys, ok := shared.ParseInt64(cmd.Args[0])
	if !ok || numKeys < 1 {
		w.WriteError("ERR numkeys should be greater than 0")
		return
	}

	if int(numKeys) > len(cmd.Args)-1 {
		w.WriteError("ERR Number of keys can't be greater than number of args")
		return
	}

	limit := int64(0)
	keyArgs := cmd.Args[1 : 1+numKeys]
	remaining := cmd.Args[1+numKeys:]
	for i := 0; i < len(remaining); i++ {
		arg := shared.ToUpper(remaining[i])
		if string(arg) == "LIMIT" {
			if i+1 >= len(remaining) {
				w.WriteSyntaxError()
				return
			}
			l, ok := shared.ParseInt64(remaining[i+1])
			if !ok || l < 0 {
				w.WriteError("ERR LIMIT can't be negative")
				return
			}
			limit = l
			i++
		} else {
			w.WriteSyntaxError()
			return
		}
	}

	keys = make([]string, len(keyArgs))
	for i, arg := range keyArgs {
		keys[i] = string(arg)
	}

	valid = true
	runner = func() {
		sets := make([]map[string]struct{}, len(keys))
		hasEmpty := false
		for i, key := range keys {
			snap, _, exists, wrongType, err := getSetSnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if exists && wrongType {
				w.WriteWrongType()
				return
			}
			if !exists {
				hasEmpty = true
			} else {
				sets[i] = getSetMembers(snap)
			}
		}

		if hasEmpty {
			w.WriteInteger(0)
			return
		}

		count := int64(0)
		for m := range sets[0] {
			inAll := true
			for _, other := range sets[1:] {
				if other == nil {
					inAll = false
					break
				}
				if _, has := other[m]; !has {
					inAll = false
					break
				}
			}
			if inAll {
				count++
				if limit > 0 && count >= limit {
					break
				}
			}
		}

		w.WriteInteger(count)
	}
	return
}

func handleSUnion(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("sunion")
		return
	}

	keys = make([]string, len(cmd.Args))
	for i, arg := range cmd.Args {
		keys[i] = string(arg)
	}

	valid = true
	runner = func() {
		result := make(map[string]struct{})
		for _, key := range keys {
			snap, _, exists, wrongType, err := getSetSnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if exists && wrongType {
				w.WriteWrongType()
				return
			}
			if exists && snap.NetAdds != nil {
				for m := range snap.NetAdds {
					result[m] = struct{}{}
				}
			}
		}

		w.WriteArray(len(result))
		for m := range result {
			w.WriteBulkString([]byte(m))
		}
	}
	return
}

func handleSUnionStore(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("sunionstore")
		return
	}

	keys = make([]string, len(cmd.Args))
	for i, arg := range cmd.Args {
		keys[i] = string(arg)
	}
	destKey := keys[0]

	valid = true
	runner = func() {
		result := make(map[string]struct{})
		for _, key := range keys[1:] {
			snap, _, exists, wrongType, err := getSetSnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if exists && wrongType {
				w.WriteWrongType()
				return
			}
			if exists && snap.NetAdds != nil {
				for m := range snap.NetAdds {
					result[m] = struct{}{}
				}
			}
		}

		writeStoreResult(cmd, w, destKey, result)
	}
	return
}

func handleSDiff(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("sdiff")
		return
	}

	keys = make([]string, len(cmd.Args))
	for i, arg := range cmd.Args {
		keys[i] = string(arg)
	}

	valid = true
	runner = func() {
		sets := make([]map[string]struct{}, len(keys))
		for i, key := range keys {
			snap, _, exists, wrongType, err := getSetSnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if exists && wrongType {
				w.WriteWrongType()
				return
			}
			if !exists {
				sets[i] = nil
			} else {
				sets[i] = getSetMembers(snap)
			}
		}

		if sets[0] == nil {
			w.WriteArray(0)
			return
		}

		result := make(map[string]struct{})
		for m := range sets[0] {
			inOther := false
			for _, other := range sets[1:] {
				if other != nil {
					if _, has := other[m]; has {
						inOther = true
						break
					}
				}
			}
			if !inOther {
				result[m] = struct{}{}
			}
		}

		w.WriteArray(len(result))
		for m := range result {
			w.WriteBulkString([]byte(m))
		}
	}
	return
}

func handleSDiffStore(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("sdiffstore")
		return
	}

	keys = make([]string, len(cmd.Args))
	for i, arg := range cmd.Args {
		keys[i] = string(arg)
	}
	destKey := keys[0]

	valid = true
	runner = func() {
		srcSets := make([]map[string]struct{}, 0, len(keys)-1)
		for _, key := range keys[1:] {
			snap, _, exists, wrongType, err := getSetSnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if exists && wrongType {
				w.WriteWrongType()
				return
			}
			if !exists {
				srcSets = append(srcSets, nil)
			} else {
				srcSets = append(srcSets, getSetMembers(snap))
			}
		}

		var result map[string]struct{}
		if srcSets[0] == nil {
			result = map[string]struct{}{}
		} else {
			result = make(map[string]struct{})
			for m := range srcSets[0] {
				inOther := false
				for _, other := range srcSets[1:] {
					if other != nil {
						if _, has := other[m]; has {
							inOther = true
							break
						}
					}
				}
				if !inOther {
					result[m] = struct{}{}
				}
			}
		}

		writeStoreResult(cmd, w, destKey, result)
	}
	return
}

// writeStoreResult handles the common *STORE pattern: delete dest, insert result, write count.
func writeStoreResult(cmd *shared.Command, w *shared.Writer, destKey string, result map[string]struct{}) {
	_, destTips, _, _, _ := getSetSnapshot(cmd, destKey)

	if err := emitDeleteKey(cmd, destKey, destTips); err != nil {
		w.WriteError(err.Error())
		return
	}

	for m := range result {
		if err := emitSetInsert(cmd, destKey, []byte(m)); err != nil {
			w.WriteError(err.Error())
			return
		}
	}

	if len(result) > 0 {
		if err := emitSetTypeTag(cmd, destKey); err != nil {
			w.WriteError(err.Error())
			return
		}
	}

	w.WriteInteger(int64(len(result)))
}
