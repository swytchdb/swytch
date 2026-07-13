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
	"math"
	"strconv"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/redis/shared"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func getHashSnapshot(cmd *shared.Command, key string) (snap *pb.ReducedEffect, tips []effects.Tip, exists bool, wrongType bool, err error) {
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
	if snap.Collection == pb.CollectionKind_SCALAR && snap.TypeTag == pb.ValueType_TYPE_HASH {
		return snap, tips, true, false, nil
	}
	return nil, tips, true, true, nil
}

func emitHashInsert(cmd *shared.Command, key string, field []byte, value []byte) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Id:         field,
			Value:      &pb.DataEffect_Raw{Raw: value},
		}},
	})
}

func emitHashIncrInt(cmd *shared.Command, key string, field []byte, delta int64) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_ADDITIVE_INT,
			Collection: pb.CollectionKind_KEYED,
			Id:         field,
			Value:      &pb.DataEffect_IntVal{IntVal: delta},
		}},
	})
}

func emitHashIncrFloat(cmd *shared.Command, key string, field []byte, delta float64) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_ADDITIVE_FLOAT,
			Collection: pb.CollectionKind_KEYED,
			Id:         field,
			Value:      &pb.DataEffect_FloatVal{FloatVal: delta},
		}},
	})
}

func emitHashRemove(cmd *shared.Command, key string, field []byte) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_KEYED,
			Id:         field,
		}},
	})
}

func emitHashTypeTag(cmd *shared.Command, key string) error {
	return cmd.Context.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_HASH}},
	})
}

func emitHashDeleteKey(cmd *shared.Command, key string, tips ...[]effects.Tip) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_SCALAR,
		}},
	}, tips...)
}

func emitHashFieldTTL(cmd *shared.Command, key string, field string, expiresAtMs time.Time) error {
	return cmd.Context.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{ElementId: []byte(field), ExpiresAt: timestamppb.New(expiresAtMs)}},
	})
}

func emitHashFieldPersist(cmd *shared.Command, key string, field string) error {
	return cmd.Context.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{ElementId: []byte(field), ExpiresAt: nil}},
	})
}

func elementValueBytes(data *pb.DataEffect) []byte {
	switch v := data.Value.(type) {
	case *pb.DataEffect_Raw:
		return v.Raw
	case *pb.DataEffect_IntVal:
		return []byte(strconv.FormatInt(v.IntVal, 10))
	case *pb.DataEffect_FloatVal:
		return []byte(strconv.FormatFloat(v.FloatVal, 'f', -1, 64))
	}
	return nil
}

func hashFieldValue(snap *pb.ReducedEffect, field string) ([]byte, bool) {
	if snap == nil || snap.NetAdds == nil {
		return nil, false
	}
	elem, ok := snap.NetAdds[field]
	if !ok {
		return nil, false
	}
	return elementValueBytes(elem.Data), true
}

func hashFieldExpiresAt(snap *pb.ReducedEffect, field string) (time.Time, bool) {
	if snap == nil || snap.NetAdds == nil {
		return time.Time{}, false
	}
	elem, ok := snap.NetAdds[field]
	if !ok {
		return time.Time{}, false
	}
	if elem.ExpiresAt == nil {
		return time.Time{}, true
	}
	return elem.ExpiresAt.AsTime(), true
}

func handleHGet(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 2 {
		w.WriteWrongNumArguments("hget")
		return
	}

	key := string(cmd.Args[0])
	field := string(cmd.Args[1])
	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
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

		fieldVal, ok := hashFieldValue(snap, field)
		if !ok {
			w.WriteNullBulkString()
			return
		}

		w.WriteBulkString(fieldVal)
	}
	return
}

func handleHSet(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 || len(cmd.Args)%2 == 0 {
		w.WriteWrongNumArguments("hset")
		return
	}

	key := string(cmd.Args[0])
	fieldValues := cmd.Args[1:]
	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		added := 0
		for i := 0; i < len(fieldValues); i += 2 {
			field := fieldValues[i]
			value := fieldValues[i+1]

			// Count new fields (not already in snap)
			if snap == nil || snap.NetAdds == nil {
				added++
			} else if _, has := snap.NetAdds[string(field)]; !has {
				added++
			}

			if err := emitHashInsert(cmd, key, field, value); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		if !exists {
			if err := emitHashTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteInteger(int64(added))
	}
	return
}

func handleHSetNX(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("hsetnx")
		return
	}

	key := string(cmd.Args[0])
	field := string(cmd.Args[1])
	value := cmd.Args[2]
	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		// If field already exists, return 0
		if snap != nil && snap.NetAdds != nil {
			if _, has := snap.NetAdds[field]; has {
				w.WriteZero()
				return
			}
		}

		if err := emitHashInsert(cmd, key, []byte(field), value); err != nil {
			w.WriteError(err.Error())
			return
		}

		if !exists {
			if err := emitHashTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteOne()
	}
	return
}

func handleHDel(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("hdel")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteZero()
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		deleted := 0
		for _, f := range cmd.Args[1:] {
			field := string(f)
			if snap.NetAdds != nil {
				if _, has := snap.NetAdds[field]; has {
					deleted++
					if err := emitHashRemove(cmd, key, f); err != nil {
						w.WriteError(err.Error())
						return
					}
				}
			}
		}

		w.WriteInteger(int64(deleted))
	}
	return
}

func handleHExists(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 2 {
		w.WriteWrongNumArguments("hexists")
		return
	}

	key := string(cmd.Args[0])
	field := string(cmd.Args[1])
	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteZero()
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		if snap.NetAdds != nil {
			if _, has := snap.NetAdds[field]; has {
				w.WriteOne()
				return
			}
		}
		w.WriteZero()
	}
	return
}

func handleHGetAll(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("hgetall")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteMap(0)
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		if snap.NetAdds == nil {
			w.WriteMap(0)
			return
		}

		w.WriteMap(len(snap.NetAdds))
		for field, elem := range snap.NetAdds {
			w.WriteBulkStringStr(field)
			w.WriteBulkString(elementValueBytes(elem.Data))
		}
	}
	return
}

func handleHKeys(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("hkeys")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
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
		for field := range snap.NetAdds {
			w.WriteBulkStringStr(field)
		}
	}
	return
}

func handleHVals(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("hvals")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
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
		for _, elem := range snap.NetAdds {
			w.WriteBulkString(elementValueBytes(elem.Data))
		}
	}
	return
}

func handleHLen(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("hlen")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteZero()
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		if snap.NetAdds == nil {
			w.WriteZero()
			return
		}

		w.WriteInteger(int64(len(snap.NetAdds)))
	}
	return
}

func handleHMGet(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("hmget")
		return
	}

	key := string(cmd.Args[0])
	fields := cmd.Args[1:]
	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		w.WriteArray(len(fields))

		if !exists {
			for range fields {
				w.WriteNullBulkString()
			}
			return
		}

		for _, fieldBytes := range fields {
			field := string(fieldBytes)
			fieldVal, ok := hashFieldValue(snap, field)
			if ok {
				w.WriteBulkString(fieldVal)
			} else {
				w.WriteNullBulkString()
			}
		}
	}
	return
}

func handleHMSet(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 || len(cmd.Args)%2 == 0 {
		w.WriteWrongNumArguments("hmset")
		return
	}

	key := string(cmd.Args[0])
	fieldValues := cmd.Args[1:]
	keys = []string{key}
	valid = true

	runner = func() {
		_, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		for i := 0; i < len(fieldValues); i += 2 {
			field := fieldValues[i]
			value := fieldValues[i+1]

			if err := emitHashInsert(cmd, key, field, value); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		if !exists {
			if err := emitHashTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteOK()
	}
	return
}

func handleHIncrBy(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("hincrby")
		return
	}

	key := string(cmd.Args[0])
	field := string(cmd.Args[1])
	increment, ok := shared.ParseInt64(cmd.Args[2])
	if !ok {
		w.WriteNotInteger()
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		// Read current value from snapshot to validate and compute return value
		var current int64
		if snap != nil && snap.NetAdds != nil {
			if elem, has := snap.NetAdds[field]; has {
				switch v := elem.Data.GetValue().(type) {
				case *pb.DataEffect_IntVal:
					// Additive (HINCRBY-produced) value reads directly.
					current = v.IntVal
				default:
					// Byte-shaped (LWW) value must parse. Decompress returns
					// nil for a corrupt compressed value — that parses as
					// not-an-integer rather than silently reading as zero.
					current, ok = shared.ParseInt64(elem.Data.Decompress())
					if !ok {
						w.WriteNotInteger()
						return
					}
				}
			}
		}

		// Check for overflow before adding
		if checkAdditionOverflow(current, increment) {
			w.WriteError("ERR increment or decrement would overflow")
			return
		}

		newValue := current + increment

		if err := emitHashIncrInt(cmd, key, []byte(field), increment); err != nil {
			w.WriteError(err.Error())
			return
		}

		if !exists {
			if err := emitHashTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteInteger(newValue)
	}
	return
}

func handleHIncrByFloat(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("hincrbyfloat")
		return
	}

	key := string(cmd.Args[0])
	field := string(cmd.Args[1])
	incrementStr := string(cmd.Args[2])
	increment, err := strconv.ParseFloat(incrementStr, 64)
	if err != nil {
		w.WriteError("ERR value is not a valid float")
		return
	}

	// Check if increment is NaN or Infinity
	if math.IsNaN(increment) || math.IsInf(increment, 0) {
		w.WriteError("ERR value is NaN or Infinity")
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		// Read current value from snapshot to validate and compute return value
		var current float64
		if snap != nil && snap.NetAdds != nil {
			if elem, has := snap.NetAdds[field]; has {
				switch v := elem.Data.GetValue().(type) {
				case *pb.DataEffect_FloatVal:
					// Additive (HINCRBYFLOAT-produced) value reads directly.
					current = v.FloatVal
				case *pb.DataEffect_IntVal:
					// An integer counter is a valid float base.
					current = float64(v.IntVal)
				default:
					// Byte-shaped (LWW) value must parse. Decompress returns
					// nil for a corrupt compressed value — that parses as
					// not-a-float rather than silently reading as zero.
					var parseErr error
					current, parseErr = strconv.ParseFloat(string(elem.Data.Decompress()), 64)
					if parseErr != nil {
						w.WriteError("ERR hash value is not a float")
						return
					}
				}
			}
		}

		newValue := current + increment

		// Check if result is NaN or Infinity
		if math.IsNaN(newValue) || math.IsInf(newValue, 0) {
			w.WriteError("ERR increment would produce NaN or Infinity")
			return
		}

		if err := emitHashIncrFloat(cmd, key, []byte(field), increment); err != nil {
			w.WriteError(err.Error())
			return
		}

		if !exists {
			if err := emitHashTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		newStr := strconv.FormatFloat(newValue, 'f', -1, 64)
		w.WriteBulkString([]byte(newStr))
	}
	return
}

func handleHStrLen(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 2 {
		w.WriteWrongNumArguments("hstrlen")
		return
	}

	key := string(cmd.Args[0])
	field := string(cmd.Args[1])
	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteZero()
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		fieldVal, ok := hashFieldValue(snap, field)
		if !ok {
			w.WriteZero()
			return
		}

		w.WriteInteger(int64(len(fieldVal)))
	}
	return
}

// checkAdditionOverflow checks if a + b would overflow int64.
func checkAdditionOverflow(a, b int64) bool {
	if b > 0 && a > math.MaxInt64-b {
		return true // positive overflow
	}
	if b < 0 && a < math.MinInt64-b {
		return true // negative overflow
	}
	return false
}
