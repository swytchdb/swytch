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
	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

// getSetSnapshot returns the snapshot for a set key.
// Returns (snapshot, tips, exists, wrongType, error).
func getSetSnapshot(cmd *shared.Command, key string) (snap *pb.ReducedEffect, tips []effects.Tip, exists bool, wrongType bool, err error) {
	snap, tips, err = cmd.Context.GetSnapshot(key)
	if err != nil {
		return nil, nil, false, false, err
	}
	if snap == nil || snap.Op == pb.EffectOp_REMOVE_OP {
		return nil, tips, false, false, nil
	}
	if snap.Collection == pb.CollectionKind_KEYED {
		return snap, tips, true, false, nil
	}
	if snap.Collection == pb.CollectionKind_SCALAR && snap.TypeTag == pb.ValueType_TYPE_SET {
		return snap, tips, true, false, nil
	}
	return nil, tips, true, true, nil
}

func emitSetInsert(cmd *shared.Command, key string, member []byte) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Id:         member,
			Value:      &pb.DataEffect_Raw{Raw: member},
		}},
	})
}

func emitSetRemove(cmd *shared.Command, key string, member []byte) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_KEYED,
			Id:         member,
		}},
	})
}

func emitSetTypeTag(cmd *shared.Command, key string) error {
	return cmd.Context.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_SET}},
	})
}

func emitDeleteKey(cmd *shared.Command, key string, tips ...[]effects.Tip) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_SCALAR,
		}},
	}, tips...)
}

func handleSAdd(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("sadd")
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
		if wrongType {
			w.WriteWrongType()
			return
		}

		added := 0
		for _, m := range members {
			if snap != nil && snap.NetAdds != nil {
				if _, has := snap.NetAdds[string(m)]; has {
					continue
				}
			}
			added++
			if err := emitSetInsert(cmd, key, m); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		if !exists {
			if err := emitSetTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteInteger(int64(added))
	}
	return
}

func handleSRem(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("srem")
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
			w.WriteInteger(0)
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		removed := 0
		for _, m := range members {
			if snap.NetAdds != nil {
				if _, has := snap.NetAdds[string(m)]; has {
					removed++
					if err := emitSetRemove(cmd, key, m); err != nil {
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

func handleSPop(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 || len(cmd.Args) > 2 {
		w.WriteWrongNumArguments("spop")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	count := 1
	returnArray := false
	if len(cmd.Args) == 2 {
		returnArray = true
		c, ok := shared.ParseInt64(cmd.Args[1])
		if !ok || c < 0 {
			w.WriteError("ERR value is not an integer or out of range")
			return
		}
		count = int(c)
	}

	valid = true
	runner = func() {
		snap, tips, exists, wrongType, err := getSetSnapshot(cmd, key)
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

		cmd.Context.BeginTx()

		// Emit Noop to record the read
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			w.WriteError(err.Error())
			return
		}

		// Collect members from snapshot
		members := make([]string, 0, len(snap.NetAdds))
		for m := range snap.NetAdds {
			members = append(members, m)
		}

		// Pick members to pop (Go map iteration is random via the slice)
		popped := make([][]byte, 0, count)
		for i := 0; i < count && i < len(members); i++ {
			popped = append(popped, []byte(members[i]))
		}

		// Emit removes for popped members
		for _, m := range popped {
			if err := emitSetRemove(cmd, key, m); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		if returnArray {
			w.WriteArray(len(popped))
			for _, member := range popped {
				w.WriteBulkString(member)
			}
		} else {
			if len(popped) == 0 {
				w.WriteNullBulkString()
			} else {
				w.WriteBulkString(popped[0])
			}
		}
	}
	return
}

func handleSMove(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("smove")
		return
	}

	srcKey := string(cmd.Args[0])
	dstKey := string(cmd.Args[1])
	member := cmd.Args[2]
	keys = []string{srcKey, dstKey}

	valid = true
	runner = func() {
		srcSnap, srcTips, srcExists, srcWrongType, err := getSetSnapshot(cmd, srcKey)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if srcExists && srcWrongType {
			w.WriteWrongType()
			return
		}

		_, _, dstExists, dstWrongType, err := getSetSnapshot(cmd, dstKey)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if dstExists && dstWrongType {
			w.WriteWrongType()
			return
		}

		if !srcExists {
			w.WriteInteger(0)
			return
		}

		// Check if member exists in source
		if srcSnap.NetAdds == nil {
			w.WriteInteger(0)
			return
		}
		if _, has := srcSnap.NetAdds[string(member)]; !has {
			w.WriteInteger(0)
			return
		}

		cmd.Context.BeginTx()

		// Emit Noop on source to record the read
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(srcKey),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, srcTips); err != nil {
			w.WriteError(err.Error())
			return
		}

		if err := emitSetRemove(cmd, srcKey, member); err != nil {
			w.WriteError(err.Error())
			return
		}

		if err := emitSetInsert(cmd, dstKey, member); err != nil {
			w.WriteError(err.Error())
			return
		}

		if !dstExists {
			if err := emitSetTypeTag(cmd, dstKey); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteInteger(1)
	}
	return
}
