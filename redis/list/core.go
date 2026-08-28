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

package list

import (
	"bytes"
	"math"
	"slices"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

func getListSnapshot(cmd *shared.Command, key string) (snap *pb.ReducedEffect, tips []effects.Tip, exists bool, wrongType bool, err error) {
	snap, tips, err = cmd.Context.GetSnapshot(key)
	if err != nil {
		return nil, nil, false, false, err
	}
	if snap == nil || snap.Op == pb.EffectOp_REMOVE_OP {
		return nil, tips, false, false, nil
	}
	if snap.Collection == pb.CollectionKind_ORDERED {
		// Only accept ORDERED collections that are actually lists (or untagged legacy data)
		if snap.TypeTag != pb.ValueType_TYPE_LIST && snap.TypeTag != pb.ValueType_TYPE_UNSPECIFIED {
			return nil, tips, true, true, nil
		}
		return snap, tips, true, false, nil
	}
	if snap.Collection == pb.CollectionKind_SCALAR && snap.TypeTag == pb.ValueType_TYPE_LIST {
		return snap, tips, true, false, nil
	}
	return nil, tips, true, true, nil
}

func emitListInsert(cmd *shared.Command, key string, placement pb.Placement, value []byte) error {
	hlc := shared.NextHLC()
	id := shared.EncodeElementID(hlc, shared.EffectsNodeID)

	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_ORDERED,
			Placement:  placement,
			Id:         id,
			Value:      &pb.DataEffect_Raw{Raw: value},
		}},
	})
}

func emitListInsertAt(cmd *shared.Command, key string, placement pb.Placement, value []byte, referenceID []byte) error {
	hlc := shared.NextHLC()
	id := shared.EncodeElementID(hlc, shared.EffectsNodeID)
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_ORDERED,
			Placement:  placement,
			Reference:  referenceID,
			Id:         id,
			Value:      &pb.DataEffect_Raw{Raw: value},
		}},
	})
}

func emitListUpdate(cmd *shared.Command, key string, elementID []byte, value []byte) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_ORDERED,
			Placement:  pb.Placement_PLACE_SELF,
			Id:         elementID,
			Value:      &pb.DataEffect_Raw{Raw: value},
		}},
	})
}

func emitListRemove(cmd *shared.Command, key string, elementID []byte) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_ORDERED,
			Placement:  pb.Placement_PLACE_SELF,
			Id:         elementID,
		}},
	})
}

func emitListTypeTag(cmd *shared.Command, key string) error {
	return cmd.Context.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_LIST}},
	})
}

func handleLPush(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("lpush")
		return
	}

	key := string(cmd.Args[0])
	elements := cmd.Args[1:]
	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getListSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		for _, elem := range elements {
			if err := emitListInsert(cmd, key, pb.Placement_PLACE_HEAD, elem); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		if !exists {
			if err := emitListTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		currentLen := 0
		if snap != nil {
			currentLen = len(snap.OrderedElements)
		}
		w.WriteInteger(int64(currentLen + len(elements)))
	}
	return
}

func handleLPushX(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("lpushx")
		return
	}

	key := string(cmd.Args[0])
	elements := cmd.Args[1:]
	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getListSnapshot(cmd, key)
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

		for _, elem := range elements {
			if err := emitListInsert(cmd, key, pb.Placement_PLACE_HEAD, elem); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteInteger(int64(len(snap.OrderedElements) + len(elements)))
	}
	return
}

func handleRPush(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("rpush")
		return
	}

	key := string(cmd.Args[0])
	elements := cmd.Args[1:]
	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getListSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		for _, elem := range elements {
			if err := emitListInsert(cmd, key, pb.Placement_PLACE_TAIL, elem); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		if !exists {
			if err := emitListTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		currentLen := 0
		if snap != nil {
			currentLen = len(snap.OrderedElements)
		}
		w.WriteInteger(int64(currentLen + len(elements)))
	}
	return
}

func handleRPushX(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("rpushx")
		return
	}

	key := string(cmd.Args[0])
	elements := cmd.Args[1:]
	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getListSnapshot(cmd, key)
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

		for _, elem := range elements {
			if err := emitListInsert(cmd, key, pb.Placement_PLACE_TAIL, elem); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteInteger(int64(len(snap.OrderedElements) + len(elements)))
	}
	return
}

func handleLPop(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 || len(cmd.Args) > 2 {
		w.WriteWrongNumArguments("lpop")
		return
	}

	key := string(cmd.Args[0])

	// Parse optional count argument
	count := 1
	hasCountArg := len(cmd.Args) > 1
	if hasCountArg {
		c, ok := shared.ParseInt64(cmd.Args[1])
		if !ok || c < 0 {
			w.WriteNotInteger()
			return
		}
		count = int(c)
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, tips, exists, wrongType, err := getListSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}
		if !exists || len(snap.OrderedElements) == 0 {
			if hasCountArg {
				w.WriteNullArray()
			} else {
				w.WriteNullBulkString()
			}
			return
		}

		elements := snap.OrderedElements
		n := min(count, len(elements))

		cmd.Context.BeginTx()
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			w.WriteError(err.Error())
			return
		}

		for i := range n {
			if err := emitListRemove(cmd, key, elements[i].Data.Id); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		if count == 1 && !hasCountArg {
			w.WriteBulkString(elements[0].Data.Decompress())
		} else {
			w.WriteArray(n)
			for i := range n {
				w.WriteBulkString(elements[i].Data.Decompress())
			}
		}
	}
	return
}

func handleRPop(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 || len(cmd.Args) > 2 {
		w.WriteWrongNumArguments("rpop")
		return
	}

	key := string(cmd.Args[0])

	// Parse optional count argument
	count := 1
	hasCountArg := len(cmd.Args) > 1
	if hasCountArg {
		c, ok := shared.ParseInt64(cmd.Args[1])
		if !ok || c < 0 {
			w.WriteNotInteger()
			return
		}
		count = int(c)
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, tips, exists, wrongType, err := getListSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}
		if !exists || len(snap.OrderedElements) == 0 {
			if hasCountArg {
				w.WriteNullArray()
			} else {
				w.WriteNullBulkString()
			}
			return
		}

		elements := snap.OrderedElements
		n := min(count, len(elements))

		cmd.Context.BeginTx()
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			w.WriteError(err.Error())
			return
		}

		for i := range n {
			idx := len(elements) - 1 - i
			if err := emitListRemove(cmd, key, elements[idx].Data.Id); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		if count == 1 && !hasCountArg {
			w.WriteBulkString(elements[len(elements)-1].Data.Decompress())
		} else {
			w.WriteArray(n)
			for i := range n {
				w.WriteBulkString(elements[len(elements)-1-i].Data.Decompress())
			}
		}
	}
	return
}

func handleLLen(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("llen")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getListSnapshot(cmd, key)
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
		w.WriteInteger(int64(len(snap.OrderedElements)))
	}
	return
}

func handleLRange(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("lrange")
		return
	}

	key := string(cmd.Args[0])
	start, ok1 := shared.ParseInt64(cmd.Args[1])
	stop, ok2 := shared.ParseInt64(cmd.Args[2])
	if !ok1 || !ok2 {
		w.WriteNotInteger()
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getListSnapshot(cmd, key)
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

		elements := snap.OrderedElements
		listLen := len(elements)
		if listLen == 0 {
			w.WriteArray(0)
			return
		}

		s, e := int(start), int(stop)
		if s < 0 {
			s = listLen + s
		}
		if e < 0 {
			e = listLen + e
		}
		if s < 0 {
			s = 0
		}
		if e >= listLen {
			e = listLen - 1
		}
		if s > e {
			w.WriteArray(0)
			return
		}

		result := elements[s : e+1]
		w.WriteArray(len(result))
		for _, elem := range result {
			w.WriteBulkString(elem.Data.Decompress())
		}
	}
	return
}

func handleLIndex(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 2 {
		w.WriteWrongNumArguments("lindex")
		return
	}

	key := string(cmd.Args[0])
	index, ok := shared.ParseInt64(cmd.Args[1])
	if !ok {
		w.WriteNotInteger()
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getListSnapshot(cmd, key)
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

		elements := snap.OrderedElements
		listLen := len(elements)
		idx := int(index)
		if idx < 0 {
			idx = listLen + idx
		}
		if idx < 0 || idx >= listLen {
			w.WriteNullBulkString()
			return
		}
		w.WriteBulkString(elements[idx].Data.Decompress())
	}
	return
}

func handleLSet(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("lset")
		return
	}

	key := string(cmd.Args[0])
	index, ok := shared.ParseInt64(cmd.Args[1])
	if !ok {
		w.WriteNotInteger()
		return
	}
	value := cmd.Args[2]

	keys = []string{key}
	valid = true

	runner = func() {
		snap, tips, exists, wrongType, err := getListSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteError("ERR no such key")
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		elements := snap.OrderedElements
		listLen := len(elements)
		idx := int(index)
		if idx < 0 {
			idx = listLen + idx
		}
		if idx < 0 || idx >= listLen {
			w.WriteOutOfRange()
			return
		}

		elementID := elements[idx].Data.Id
		cmd.Context.BeginTx()
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			w.WriteError(err.Error())
			return
		}
		if err := emitListUpdate(cmd, key, elementID, value); err != nil {
			w.WriteError(err.Error())
			return
		}
		w.WriteOK()
	}
	return
}

func handleLRem(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("lrem")
		return
	}

	key := string(cmd.Args[0])
	count, ok := shared.ParseInt64(cmd.Args[1])
	if !ok {
		w.WriteNotInteger()
		return
	}
	element := cmd.Args[2]

	keys = []string{key}
	valid = true

	runner = func() {
		snap, tips, exists, wrongType, err := getListSnapshot(cmd, key)
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

		elements := snap.OrderedElements

		var toRemove []*pb.ReducedElement
		if count > 0 {
			for _, elem := range elements {
				if bytes.Equal(elem.Data.Decompress(), element) {
					toRemove = append(toRemove, elem)
					if int64(len(toRemove)) >= count {
						break
					}
				}
			}
		} else if count < 0 {
			for _, element0 := range slices.Backward(elements) {
				if bytes.Equal(element0.Data.Decompress(), element) {
					toRemove = append(toRemove, element0)
					if int64(len(toRemove)) >= -count {
						break
					}
				}
			}
		} else {
			for _, elem := range elements {
				if bytes.Equal(elem.Data.Decompress(), element) {
					toRemove = append(toRemove, elem)
				}
			}
		}

		if len(toRemove) == 0 {
			w.WriteZero()
			return
		}

		cmd.Context.BeginTx()
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			w.WriteError(err.Error())
			return
		}
		for _, elem := range toRemove {
			if err := emitListRemove(cmd, key, elem.Data.Id); err != nil {
				w.WriteError(err.Error())
				return
			}
		}
		w.WriteInteger(int64(len(toRemove)))
	}
	return
}

func handleLTrim(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("ltrim")
		return
	}

	key := string(cmd.Args[0])
	start, ok1 := shared.ParseInt64(cmd.Args[1])
	stop, ok2 := shared.ParseInt64(cmd.Args[2])
	if !ok1 || !ok2 {
		w.WriteNotInteger()
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, tips, exists, wrongType, err := getListSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteOK()
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		elements := snap.OrderedElements
		listLen := len(elements)

		s := int(start)
		e := int(stop)
		if s < 0 {
			s = listLen + s
		}
		if e < 0 {
			e = listLen + e
		}
		if s < 0 {
			s = 0
		}
		if e >= listLen {
			e = listLen - 1
		}

		var toRemove []*pb.ReducedElement
		for i, elem := range elements {
			if i < s || i > e {
				toRemove = append(toRemove, elem)
			}
		}

		if len(toRemove) == 0 {
			w.WriteOK()
			return
		}

		cmd.Context.BeginTx()
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			w.WriteError(err.Error())
			return
		}
		for _, elem := range toRemove {
			if err := emitListRemove(cmd, key, elem.Data.Id); err != nil {
				w.WriteError(err.Error())
				return
			}
		}
		w.WriteOK()
	}
	return
}

func handleLInsert(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 4 {
		w.WriteWrongNumArguments("linsert")
		return
	}

	key := string(cmd.Args[0])
	position := shared.ToUpper(cmd.Args[1])
	pivot := cmd.Args[2]
	element := cmd.Args[3]

	var before bool
	switch string(position) {
	case "BEFORE":
		before = true
	case "AFTER":
		before = false
	default:
		w.WriteSyntaxError()
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, tips, exists, wrongType, err := getListSnapshot(cmd, key)
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

		var pivotID []byte
		for _, elem := range snap.OrderedElements {
			if bytes.Equal(elem.Data.Decompress(), pivot) {
				pivotID = elem.Data.Id
				break
			}
		}
		if pivotID == nil {
			w.WriteInteger(-1)
			return
		}

		placement := pb.Placement_PLACE_AFTER
		if before {
			placement = pb.Placement_PLACE_BEFORE
		}

		cmd.Context.BeginTx()
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			w.WriteError(err.Error())
			return
		}
		if err := emitListInsertAt(cmd, key, placement, element, pivotID); err != nil {
			w.WriteError(err.Error())
			return
		}
		w.WriteInteger(int64(len(snap.OrderedElements) + 1))
	}
	return
}

func handleLPos(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("lpos")
		return
	}

	key := string(cmd.Args[0])
	// Copy element to avoid aliasing with pooled command buffers
	element := make([]byte, len(cmd.Args[1]))
	copy(element, cmd.Args[1])

	// Parse optional arguments: RANK, COUNT, MAXLEN
	rank := 1
	count := -1 // -1 means COUNT not specified (return single value)
	maxlen := 0 // 0 means unlimited

	for i := 2; i < len(cmd.Args); i++ {
		opt := shared.ToUpper(cmd.Args[i])
		switch string(opt) {
		case "RANK":
			if i+1 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			i++
			r, ok := shared.ParseInt64(cmd.Args[i])
			if !ok {
				w.WriteError("ERR value is not an integer or out of range")
				return
			}
			if r == 0 {
				w.WriteError("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list")
				return
			}
			// Check for overflow: -MinInt64 overflows to MinInt64
			if r == math.MinInt64 {
				w.WriteError("ERR value is out of range")
				return
			}
			rank = int(r)
		case "COUNT":
			if i+1 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			i++
			c, ok := shared.ParseInt64(cmd.Args[i])
			if !ok || c < 0 {
				w.WriteError("ERR COUNT can't be negative")
				return
			}
			count = int(c)
		case "MAXLEN":
			if i+1 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			i++
			m, ok := shared.ParseInt64(cmd.Args[i])
			if !ok || m < 0 {
				w.WriteError("ERR MAXLEN can't be negative")
				return
			}
			maxlen = int(m)
		default:
			w.WriteSyntaxError()
			return
		}
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getListSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			if count >= 0 {
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

		elements := snap.OrderedElements
		listLen := len(elements)
		if listLen == 0 {
			if count >= 0 {
				w.WriteArray(0)
			} else {
				w.WriteNullBulkString()
			}
			return
		}

		// Determine search direction and starting point based on RANK
		var matches []int64
		matchesNeeded := count
		if matchesNeeded == 0 {
			matchesNeeded = listLen // COUNT 0 means return all matches
		}
		if count < 0 {
			matchesNeeded = 1 // No COUNT specified, just need one match
		}

		// Skip count based on RANK (how many matches to skip)
		skipCount := 0
		if rank > 0 {
			skipCount = rank - 1
		} else {
			skipCount = -rank - 1
		}

		comparisons := 0
		if rank > 0 {
			// Search from head to tail
			for idx := range listLen {
				if maxlen > 0 && comparisons >= maxlen {
					break
				}
				comparisons++

				if bytes.Equal(elements[idx].Data.Decompress(), element) {
					if skipCount > 0 {
						skipCount--
						continue
					}
					matches = append(matches, int64(idx))
					if len(matches) >= matchesNeeded {
						break
					}
				}
			}
		} else {
			// Search from tail to head (rank is negative)
			for idx := listLen - 1; idx >= 0; idx-- {
				if maxlen > 0 && comparisons >= maxlen {
					break
				}
				comparisons++

				if bytes.Equal(elements[idx].Data.Decompress(), element) {
					if skipCount > 0 {
						skipCount--
						continue
					}
					matches = append(matches, int64(idx))
					if len(matches) >= matchesNeeded {
						break
					}
				}
			}
		}

		// Write response
		if count >= 0 {
			// COUNT was specified, return array
			w.WriteArray(len(matches))
			for _, pos := range matches {
				w.WriteInteger(pos)
			}
		} else {
			// No COUNT, return single value or nil
			if len(matches) == 0 {
				w.WriteNullBulkString()
			} else {
				w.WriteInteger(matches[0])
			}
		}
	}
	return
}

// handleLMPop implements the LMPOP command (list multi-pop)
// LMPOP numkeys key [key ...] <LEFT | RIGHT> [COUNT count]
func handleLMPop(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("lmpop")
		return
	}

	// Parse numkeys
	numKeys, ok := shared.ParseInt64(cmd.Args[0])
	if !ok || numKeys < 1 {
		w.WriteError("ERR numkeys should be greater than 0")
		return
	}

	// Check we have enough args: numkeys + keys + direction
	if len(cmd.Args) < int(numKeys)+2 {
		w.WriteError("ERR syntax error")
		return
	}

	// Extract keys
	keyList := make([]string, numKeys)
	for i := range numKeys {
		keyList[i] = string(cmd.Args[1+i])
	}

	// Parse direction (LEFT or RIGHT)
	dirIdx := int(numKeys) + 1
	direction := shared.ToUpper(cmd.Args[dirIdx])
	var left bool
	switch string(direction) {
	case "LEFT":
		left = true
	case "RIGHT":
		left = false
	default:
		w.WriteError("ERR syntax error")
		return
	}

	// Parse optional COUNT
	count := int64(1)
	if len(cmd.Args) > dirIdx+1 {
		if len(cmd.Args) != dirIdx+3 {
			w.WriteError("ERR syntax error")
			return
		}
		countArg := shared.ToUpper(cmd.Args[dirIdx+1])
		if string(countArg) != "COUNT" {
			w.WriteError("ERR syntax error")
			return
		}
		var ok bool
		count, ok = shared.ParseInt64(cmd.Args[dirIdx+2])
		if !ok || count < 1 {
			w.WriteError("ERR count should be greater than 0")
			return
		}
	}

	keys = keyList
	valid = true

	runner = func() {
		// Type check all keys first
		for _, key := range keyList {
			_, _, exists, wrongType, err := getListSnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if exists && wrongType {
				w.WriteWrongType()
				return
			}
		}

		// Try each key in order
		for _, key := range keyList {
			snap, tips, exists, _, err := getListSnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if !exists || len(snap.OrderedElements) == 0 {
				continue
			}

			elements := snap.OrderedElements
			n := min(int(count), len(elements))

			cmd.Context.BeginTx()
			if err := cmd.Context.Emit(&pb.Effect{
				Key:  []byte(key),
				Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
			}, tips); err != nil {
				w.WriteError(err.Error())
				return
			}

			popped := make([][]byte, n)
			if left {
				for i := range n {
					popped[i] = elements[i].Data.Decompress()
					if err := emitListRemove(cmd, key, elements[i].Data.Id); err != nil {
						w.WriteError(err.Error())
						return
					}
				}
			} else {
				for i := range n {
					idx := len(elements) - 1 - i
					popped[i] = elements[idx].Data.Decompress()
					if err := emitListRemove(cmd, key, elements[idx].Data.Id); err != nil {
						w.WriteError(err.Error())
						return
					}
				}
			}

			w.WriteArray(2)
			w.WriteBulkStringStr(key)
			w.WriteArray(n)
			for _, elem := range popped {
				w.WriteBulkString(elem)
			}
			return
		}

		w.WriteNullArray()
	}
	return
}

// handleLMove implements the LMOVE command
// LMOVE source destination <LEFT | RIGHT> <LEFT | RIGHT>
func handleLMove(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 4 {
		w.WriteWrongNumArguments("lmove")
		return
	}

	source := string(cmd.Args[0])
	destination := string(cmd.Args[1])
	whereFrom := shared.ToUpper(cmd.Args[2])
	whereTo := shared.ToUpper(cmd.Args[3])

	// Parse directions
	var popLeft, pushLeft bool
	switch string(whereFrom) {
	case "LEFT":
		popLeft = true
	case "RIGHT":
		popLeft = false
	default:
		w.WriteError("ERR syntax error")
		return
	}
	switch string(whereTo) {
	case "LEFT":
		pushLeft = true
	case "RIGHT":
		pushLeft = false
	default:
		w.WriteError("ERR syntax error")
		return
	}

	keys = []string{source, destination}
	valid = true

	runner = func() {
		elem, errMsg, ok, err := doLMove(cmd, source, destination, popLeft, pushLeft)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !ok {
			if errMsg != "" {
				w.WriteError(errMsg)
			} else {
				w.WriteNullBulkString()
			}
			return
		}
		w.WriteBulkString(elem)
	}
	return
}

// handleRPopLPush implements the deprecated RPOPLPUSH command
// RPOPLPUSH source destination
func handleRPopLPush(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 2 {
		w.WriteWrongNumArguments("rpoplpush")
		return
	}

	source := string(cmd.Args[0])
	destination := string(cmd.Args[1])

	keys = []string{source, destination}
	valid = true

	runner = func() {
		// RPOPLPUSH = LMOVE source destination RIGHT LEFT
		elem, errMsg, ok, err := doLMove(cmd, source, destination, false, true)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !ok {
			if errMsg != "" {
				w.WriteError(errMsg)
			} else {
				w.WriteNullBulkString()
			}
			return
		}
		w.WriteBulkString(elem)
	}
	return
}

// doLMove atomically pops from source and pushes to destination using the effects engine.
// Returns (element, "", true, nil) on success
// Returns (nil, "", false, nil) if source is empty/doesn't exist (caller should block)
// Returns (nil, errMsg, false, nil) if wrong type error (caller should return error)
// Returns (nil, "", false, err) if emit fails
func doLMove(cmd *shared.Command, source, destination string, popLeft, pushLeft bool) ([]byte, string, bool, error) {
	// Get source snapshot
	srcSnap, srcTips, srcExists, srcWrongType, err := getListSnapshot(cmd, source)
	if err != nil {
		return nil, "", false, err
	}
	if !srcExists {
		return nil, "", false, nil
	}
	if srcWrongType {
		return nil, "WRONGTYPE Operation against a key holding the wrong kind of value", false, nil
	}
	if len(srcSnap.OrderedElements) == 0 {
		return nil, "", false, nil
	}

	// Check destination type (if different from source)
	var dstTips []effects.Tip
	if source != destination {
		_, dt, _, dstWrongType, err := getListSnapshot(cmd, destination)
		if err != nil {
			return nil, "", false, err
		}
		if dstWrongType {
			return nil, "WRONGTYPE Operation against a key holding the wrong kind of value", false, nil
		}
		dstTips = dt
	}

	// Pick element to pop
	var elem *pb.ReducedElement
	if popLeft {
		elem = srcSnap.OrderedElements[0]
	} else {
		elem = srcSnap.OrderedElements[len(srcSnap.OrderedElements)-1]
	}
	value := elem.Data.Decompress()

	// Begin transaction: read source (+ dest if different), remove from source, insert to dest
	cmd.Context.BeginTx()

	// Depend on source tips
	if err := cmd.Context.Emit(&pb.Effect{
		Key:  []byte(source),
		Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	}, srcTips); err != nil {
		return nil, "", false, err
	}

	// Depend on dest tips (if different key)
	if source != destination && len(dstTips) > 0 {
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(destination),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, dstTips); err != nil {
			return nil, "", false, err
		}
	}

	// Remove from source
	if err := emitListRemove(cmd, source, elem.Data.Id); err != nil {
		return nil, "", false, err
	}

	// Insert to destination
	placement := pb.Placement_PLACE_TAIL
	if pushLeft {
		placement = pb.Placement_PLACE_HEAD
	}
	if err := emitListInsert(cmd, destination, placement, value); err != nil {
		return nil, "", false, err
	}

	return value, "", true, nil
}
