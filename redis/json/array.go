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

package json

import (
	"bytes"
	"errors"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/redis/shared"
)

// arrTarget is one matched location for an array command: the matched value
// and — when it is an array — the partition vid and reduced node needed to emit
// element effects against it.
type arrTarget struct {
	val     *Value
	vid     string
	node    *pb.ReducedEffect
	isArray bool
}

// resolveArrays resolves path to every matched location in document order,
// marking each array match with its partition. Legacy paths keep only the first
// match.
func resolveArrays(root *pb.ReducedEffect, path *Path) []arrTarget {
	ms := path.resolveMatches(assemble(root, ""))
	if !path.JSONPath && len(ms) > 1 {
		ms = ms[:1]
	}
	out := make([]arrTarget, len(ms))
	for i, m := range ms {
		t := arrTarget{val: m.val}
		if m.val.Kind == KindArray {
			if vid, node, ok := walkToParent(root, m.segs); ok && node.TypeTag == pb.ValueType_TYPE_JSON_ARRAY {
				t.vid, t.node, t.isArray = vid, node, true
			}
		}
		out[i] = t
	}
	return out
}

func notArrayErr(m *Value) error {
	return errors.New("ERR wrong type of path value - expected an array but found " + m.TypeName())
}

// --- read-only: ARRLEN, ARRINDEX ---

// handleJSONArrLen — JSON.ARRLEN key [path]
func handleJSONArrLen(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 || len(cmd.Args) > 2 {
		w.WriteWrongNumArguments("json.arrlen")
		return
	}
	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true
	runner = func() {
		pathStr := "$"
		if len(cmd.Args) == 2 {
			pathStr = string(cmd.Args[1])
		}
		jsonReadInt(cmd, w, key, pathStr, "an array", func(m *Value) (int64, bool) {
			if m.Kind != KindArray {
				return 0, false
			}
			return int64(len(m.Arr)), true
		})
	}
	return
}

// handleJSONArrIndex — JSON.ARRINDEX key path value [start [stop]]
func handleJSONArrIndex(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 || len(cmd.Args) > 5 {
		w.WriteWrongNumArguments("json.arrindex")
		return
	}
	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true
	runner = func() {
		pathStr := string(cmd.Args[1])
		needle, err := Parse(cmd.Args[2])
		if err != nil {
			w.WriteError("ERR invalid JSON")
			return
		}
		start, stop := 0, 0
		if len(cmd.Args) >= 4 {
			s, ok := shared.ParseInt64(cmd.Args[3])
			if !ok {
				w.WriteNotInteger()
				return
			}
			start = int(s)
		}
		if len(cmd.Args) == 5 {
			s, ok := shared.ParseInt64(cmd.Args[4])
			if !ok {
				w.WriteNotInteger()
				return
			}
			stop = int(s)
		}
		jsonReadInt(cmd, w, key, pathStr, "an array", func(m *Value) (int64, bool) {
			if m.Kind != KindArray {
				return 0, false
			}
			return arrIndexOf(m.Arr, needle, start, stop), true
		})
	}
	return
}

// arrIndexOf returns the first index in [start, stop) where arr equals needle, or
// -1. Negative bounds count from the end; stop 0 means the end of the array.
func arrIndexOf(arr []*Value, needle *Value, start, stop int) int64 {
	n := len(arr)
	s := start
	if s < 0 {
		s += n
		if s < 0 {
			s = 0
		}
	}
	e := stop
	if e == 0 {
		e = n
	} else if e < 0 {
		e += n
	}
	if e > n {
		e = n
	}
	for i := s; i < e; i++ {
		if valueEqual(arr[i], needle) {
			return int64(i)
		}
	}
	return -1
}

func valueEqual(a, b *Value) bool { return bytes.Equal(Serialize(a), Serialize(b)) }

// --- writes: ARRAPPEND, ARRINSERT, ARRPOP, ARRTRIM ---

// arrIntOp computes the effects to apply to an array partition and the integer
// reply (the new length). It returns an error (e.g. index out of range) before
// any effect is emitted, so the dispatcher can reply with a clean error.
type arrIntOp func(vid string, node *pb.ReducedEffect) (apply func(emitFn) error, reply int64, err error)

// arrayWriteInt drives a per-array integer-returning write inside an implicit
// transaction (Noop carries the read tips). JSONPath replies an array (null for
// non-array matches); legacy replies a bare integer (error on missing/non-array);
// missing key errors.
func arrayWriteInt(cmd *shared.Command, w *shared.Writer, key, pathStr string, op arrIntOp) {
	path, err := ParsePath(pathStr)
	if err != nil {
		w.WriteError("ERR " + err.Error())
		return
	}
	root, tips, exists, wrongType, err := getJSONSnapshot(cmd, key)
	if err != nil {
		w.WriteError(err.Error())
		return
	}
	if wrongType {
		writeWrongType(w)
		return
	}
	if !exists {
		w.WriteError("ERR could not perform this operation on a key that doesn't exist")
		return
	}
	targets := resolveArrays(root, path)

	// Compute each array match's effects and reply up front (an error — e.g. an
	// out-of-range index — aborts before any effect is emitted).
	type planned struct {
		isArray bool
		apply   func(emitFn) error
		reply   int64
	}
	plans := make([]planned, len(targets))
	hasWrite := false
	for i, t := range targets {
		if !t.isArray {
			continue
		}
		apply, reply, err := op(t.vid, t.node)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		plans[i] = planned{true, apply, reply}
		hasWrite = true
	}
	if hasWrite {
		cmd.Context.BeginTx()
		// Record the read for this key (Noop carries its tips).
		if e := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); e != nil {
			w.WriteError(e.Error())
			return
		}
		plain := func(eff *pb.Effect) error { return cmd.Context.Emit(eff) }
		for _, pl := range plans {
			if pl.isArray {
				if e := pl.apply(plain); e != nil {
					w.WriteError(e.Error())
					return
				}
			}
		}
	}

	if path.JSONPath {
		w.WriteArray(len(targets))
		for i := range targets {
			if plans[i].isArray {
				w.WriteInteger(plans[i].reply)
			} else {
				w.WriteNullBulkString()
			}
		}
		return
	}
	if len(targets) == 0 {
		w.WriteError("ERR Path '" + pathStr + "' does not exist")
		return
	}
	if !targets[0].isArray {
		w.WriteError(notArrayErr(targets[0].val).Error())
		return
	}
	w.WriteInteger(plans[0].reply)
}

// handleJSONArrAppend — JSON.ARRAPPEND key path value [value ...]
func handleJSONArrAppend(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("json.arrappend")
		return
	}
	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true
	runner = func() {
		pathStr := string(cmd.Args[1])
		vals, err := parseValues(cmd.Args[2:])
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		arrayWriteInt(cmd, w, key, pathStr, func(vid string, node *pb.ReducedEffect) (func(emitFn) error, int64, error) {
			apply := func(emit emitFn) error {
				for _, v := range vals {
					ref, err := encodeRef(emit, key, v)
					if err != nil {
						return err
					}
					if err := emit(orderedInsertEffect(key, vid, []byte(newVid()), ref)); err != nil {
						return err
					}
				}
				return nil
			}
			return apply, int64(len(node.OrderedElements) + len(vals)), nil
		})
	}
	return
}

// handleJSONArrInsert — JSON.ARRINSERT key path index value [value ...]
func handleJSONArrInsert(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("json.arrinsert")
		return
	}
	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true
	runner = func() {
		pathStr := string(cmd.Args[1])
		idx, ok := shared.ParseInt64(cmd.Args[2])
		if !ok {
			w.WriteNotInteger()
			return
		}
		vals, err := parseValues(cmd.Args[3:])
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		arrayWriteInt(cmd, w, key, pathStr, func(vid string, node *pb.ReducedEffect) (func(emitFn) error, int64, error) {
			n := len(node.OrderedElements)
			i := int(idx)
			if i < 0 {
				i += n
			}
			if i < 0 || i > n {
				return nil, 0, errors.New("ERR index out of range")
			}
			var refID []byte
			if i < n {
				refID = node.OrderedElements[i].GetData().GetId()
			}
			apply := func(emit emitFn) error {
				for _, v := range vals {
					ref, err := encodeRef(emit, key, v)
					if err != nil {
						return err
					}
					id := []byte(newVid())
					if refID == nil {
						if err := emit(orderedInsertEffect(key, vid, id, ref)); err != nil {
							return err
						}
					} else if err := emit(orderedInsertBeforeEffect(key, vid, id, refID, ref)); err != nil {
						return err
					}
				}
				return nil
			}
			return apply, int64(n + len(vals)), nil
		})
	}
	return
}

// handleJSONArrTrim — JSON.ARRTRIM key path start stop
func handleJSONArrTrim(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 4 {
		w.WriteWrongNumArguments("json.arrtrim")
		return
	}
	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true
	runner = func() {
		pathStr := string(cmd.Args[1])
		start, ok1 := shared.ParseInt64(cmd.Args[2])
		stop, ok2 := shared.ParseInt64(cmd.Args[3])
		if !ok1 || !ok2 {
			w.WriteNotInteger()
			return
		}
		arrayWriteInt(cmd, w, key, pathStr, func(vid string, node *pb.ReducedEffect) (func(emitFn) error, int64, error) {
			n := len(node.OrderedElements)
			s, e := int(start), int(stop)
			if s < 0 {
				s += n
				if s < 0 {
					s = 0
				}
			}
			if e < 0 {
				e += n
			}
			if e >= n {
				e = n - 1
			}
			var removeIDs [][]byte
			var keep int64
			if n == 0 || s >= n || s > e {
				for _, el := range node.OrderedElements {
					removeIDs = append(removeIDs, el.GetData().GetId())
				}
				keep = 0
			} else {
				for i := range n {
					if i < s || i > e {
						removeIDs = append(removeIDs, node.OrderedElements[i].GetData().GetId())
					}
				}
				keep = int64(e - s + 1)
			}
			apply := func(emit emitFn) error {
				for _, id := range removeIDs {
					if err := emit(orderedRemoveEffect(key, vid, id)); err != nil {
						return err
					}
				}
				return nil
			}
			return apply, keep, nil
		})
	}
	return
}

// handleJSONArrPop — JSON.ARRPOP key [path [index]]
func handleJSONArrPop(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 || len(cmd.Args) > 3 {
		w.WriteWrongNumArguments("json.arrpop")
		return
	}
	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true
	runner = func() {
		pathStr := "$"
		if len(cmd.Args) >= 2 {
			pathStr = string(cmd.Args[1])
		}
		index := -1
		if len(cmd.Args) == 3 {
			i, ok := shared.ParseInt64(cmd.Args[2])
			if !ok {
				w.WriteNotInteger()
				return
			}
			index = int(i)
		}
		path, err := ParsePath(pathStr)
		if err != nil {
			w.WriteError("ERR " + err.Error())
			return
		}
		root, tips, exists, wrongType, err := getJSONSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			writeWrongType(w)
			return
		}
		if !exists {
			w.WriteError("ERR could not perform this operation on a key that doesn't exist")
			return
		}
		targets := resolveArrays(root, path)

		// popAt resolves the element to remove at the (clamped) index; ok=false
		// for an empty array. Out-of-range indexes round to the array ends.
		popAt := func(node *pb.ReducedEffect) (*Value, []byte, bool) {
			n := len(node.OrderedElements)
			if n == 0 {
				return nil, nil, false
			}
			i := index
			if i < 0 {
				i += n
			}
			if i < 0 {
				i = 0
			}
			if i >= n {
				i = n - 1
			}
			el := node.OrderedElements[i]
			return decodeRef(root, el.GetData()), el.GetData().GetId(), true
		}

		// Resolve each array match's popped element, then remove them all in one
		// transaction.
		type popped struct {
			isArray bool
			val     *Value
			vid     string
			id      []byte
			has     bool
		}
		pops := make([]popped, len(targets))
		any := false
		for i, t := range targets {
			if !t.isArray {
				continue
			}
			v, id, ok := popAt(t.node)
			pops[i] = popped{isArray: true, val: v, vid: t.vid, id: id, has: ok}
			if ok {
				any = true
			}
		}
		if any {
			cmd.Context.BeginTx()
			// Record the read for this key (Noop carries its tips).
			if e := cmd.Context.Emit(&pb.Effect{
				Key:  []byte(key),
				Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
			}, tips); e != nil {
				w.WriteError(e.Error())
				return
			}
			for _, p := range pops {
				if p.isArray && p.has {
					if e := cmd.Context.Emit(orderedRemoveEffect(key, p.vid, p.id)); e != nil {
						w.WriteError(e.Error())
						return
					}
				}
			}
		}

		if path.JSONPath {
			w.WriteArray(len(targets))
			for i := range targets {
				if pops[i].isArray && pops[i].has {
					w.WriteBulkString(Serialize(pops[i].val))
				} else {
					w.WriteNullBulkString()
				}
			}
			return
		}
		if len(targets) == 0 {
			w.WriteError("ERR Path '" + pathStr + "' does not exist")
			return
		}
		if !targets[0].isArray {
			w.WriteError(notArrayErr(targets[0].val).Error())
			return
		}
		if pops[0].has {
			w.WriteBulkString(Serialize(pops[0].val))
		} else {
			w.WriteNullBulkString()
		}
	}
	return
}

// parseValues parses each arg as a JSON value (for the value lists of ARRAPPEND
// and ARRINSERT).
func parseValues(args [][]byte) ([]*Value, error) {
	vals := make([]*Value, len(args))
	for i, a := range args {
		v, err := Parse(a)
		if err != nil {
			return nil, errors.New("ERR invalid JSON")
		}
		vals[i] = v
	}
	return vals, nil
}
