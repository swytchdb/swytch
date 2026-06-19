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
	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

func isJSONNode(t pb.ValueType) bool {
	switch t {
	case pb.ValueType_TYPE_JSON, pb.ValueType_TYPE_JSON_OBJECT, pb.ValueType_TYPE_JSON_ARRAY:
		return true
	}
	return false
}

// getJSONSnapshot reads the key's root node. exists=false when the key is
// absent or deleted; wrongType=true when it holds a non-JSON value.
func getJSONSnapshot(cmd *shared.Command, key string) (root *pb.ReducedEffect, tips []effects.Tip, exists, wrongType bool, err error) {
	snap, t, e := cmd.Context.GetSnapshot(key)
	if e != nil {
		return nil, nil, false, false, e
	}
	if snap == nil || snap.Op == pb.EffectOp_REMOVE_OP {
		return nil, t, false, false, nil
	}
	if !isJSONNode(snap.TypeTag) {
		return nil, t, true, true, nil
	}
	return snap, t, true, false, nil
}

// writeWrongType emits the RedisJSON wrong-type error verbatim (RedisJSON uses
// this exact text, without a WRONGTYPE code prefix).
func writeWrongType(w *shared.Writer) {
	w.WriteError("Existing key has wrong Redis type")
}

// tipEmitter threads the read tips onto the first emitted effect (anchoring the
// write batch on the state the handler read) and chains the rest.
func tipEmitter(cmd *shared.Command, tips []effects.Tip) emitFn {
	first := true
	return func(e *pb.Effect) error {
		if first {
			first = false
			return cmd.Context.Emit(e, tips)
		}
		return cmd.Context.Emit(e)
	}
}

// jsonReadInt drives a read-only per-match integer command. fn returns the
// integer for a match, ok=false when the match is the wrong type (expected
// names the wanted type, e.g. "a string"). JSONPath replies an array (null per
// wrong-type match); legacy replies a bare integer (error on a missing path or
// wrong type); a missing key replies null.
func jsonReadInt(cmd *shared.Command, w *shared.Writer, key, pathStr, expected string, fn func(*Value) (int64, bool)) {
	path, err := ParsePath(pathStr)
	if err != nil {
		w.WriteError("ERR " + err.Error())
		return
	}
	root, _, exists, wrongType, err := getJSONSnapshot(cmd, key)
	if err != nil {
		w.WriteError(err.Error())
		return
	}
	if !exists {
		w.WriteNullBulkString()
		return
	}
	if wrongType {
		writeWrongType(w)
		return
	}
	matches := path.resolve(assemble(root, ""))
	if path.JSONPath {
		w.WriteArray(len(matches))
		for _, m := range matches {
			if n, ok := fn(m); ok {
				w.WriteInteger(n)
			} else {
				w.WriteNullBulkString()
			}
		}
		return
	}
	if len(matches) == 0 {
		w.WriteError("ERR Path '" + pathStr + "' does not exist")
		return
	}
	n, ok := fn(matches[0])
	if !ok {
		w.WriteError("ERR wrong type of path value - expected " + expected + " but found " + matches[0].TypeName())
		return
	}
	w.WriteInteger(n)
}

// scalarIntWrite performs an LWW read-modify-write of the scalar addressed by
// path inside an implicit transaction (Noop carries the read tips), replying an
// integer. fn returns the replacement value to store and the integer reply;
// ok=false marks a wrong-type match (JSONPath → null entry, legacy → wrong-type
// error). The supported path grammar resolves to at most one location, so at
// most one write.
func scalarIntWrite(cmd *shared.Command, w *shared.Writer, key, pathStr, expected string, fn func(*Value) (repl *Value, reply int64, ok bool)) {
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
	matches := path.resolve(assemble(root, ""))

	type result struct {
		reply int64
		ok    bool
	}
	results := make([]result, len(matches))
	var repl *Value
	write := false
	for i, m := range matches {
		r, n, ok := fn(m)
		results[i] = result{n, ok}
		if ok {
			repl, write = r, true
		}
	}
	if write {
		cmd.Context.BeginTx()
		// Record the read for this key (Noop carries its tips).
		if e := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); e != nil {
			w.WriteError(e.Error())
			return
		}
		applied, e := setValueAtPath(func(ef *pb.Effect) error { return cmd.Context.Emit(ef) }, root, key, path, repl, exists)
		if e != nil {
			w.WriteError(e.Error())
			return
		}
		if !applied {
			w.WriteError(errPathNotSet.Error())
			return
		}
	}

	if path.JSONPath {
		w.WriteArray(len(results))
		for _, r := range results {
			if r.ok {
				w.WriteInteger(r.reply)
			} else {
				w.WriteNullBulkString()
			}
		}
		return
	}
	if len(matches) == 0 {
		w.WriteError("ERR Path '" + pathStr + "' does not exist")
		return
	}
	if !results[0].ok {
		w.WriteError("ERR wrong type of path value - expected " + expected + " but found " + matches[0].TypeName())
		return
	}
	w.WriteInteger(results[0].reply)
}

func keyDelEffect(key string) *pb.Effect {
	return &pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_REMOVE_OP, Collection: pb.CollectionKind_SCALAR}},
	}
}

func orderedSelfEffect(key, vid string, id []byte, ref elemRef) *pb.Effect {
	d := &pb.DataEffect{
		Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_LAST_WRITE_WINS,
		Collection: pb.CollectionKind_ORDERED, Placement: pb.Placement_PLACE_SELF,
		Id: id,
	}
	setRefValue(d, ref)
	return &pb.Effect{
		Key:     []byte(key),
		Virtual: virtBytes(vid),
		Kind:    &pb.Effect_Data{Data: d},
	}
}

// orderedInsertBeforeEffect inserts a new element with id immediately before the
// element refID in partition vid (ARRINSERT). Sequential inserts before the same
// refID preserve their emission order.
func orderedInsertBeforeEffect(key, vid string, id, refID []byte, ref elemRef) *pb.Effect {
	d := &pb.DataEffect{
		Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_LAST_WRITE_WINS,
		Collection: pb.CollectionKind_ORDERED, Placement: pb.Placement_PLACE_BEFORE,
		Id: id, Reference: refID,
	}
	setRefValue(d, ref)
	return &pb.Effect{
		Key:     []byte(key),
		Virtual: virtBytes(vid),
		Kind:    &pb.Effect_Data{Data: d},
	}
}

func orderedRemoveEffect(key, vid string, id []byte) *pb.Effect {
	return &pb.Effect{
		Key:     []byte(key),
		Virtual: virtBytes(vid),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op: pb.EffectOp_REMOVE_OP, Collection: pb.CollectionKind_ORDERED,
			Placement: pb.Placement_PLACE_SELF, Id: id,
		}},
	}
}

// --- partition walk for writes ---

func elementByKey(p *pb.ReducedEffect, key string) *pb.ReducedElement {
	for _, el := range p.OrderedElements {
		if string(el.GetData().GetId()) == key {
			return el
		}
	}
	return nil
}

func elementByIndex(p *pb.ReducedEffect, idx int) *pb.ReducedElement {
	n := len(p.OrderedElements)
	if idx < 0 {
		idx += n
	}
	if idx < 0 || idx >= n {
		return nil
	}
	return p.OrderedElements[idx]
}

func containerChildVid(el *pb.ReducedElement) (string, bool) {
	if child := el.GetData().GetChild(); len(child) > 0 {
		return string(child), true
	}
	return "", false
}

// walkToParent descends segs from root following container references, returning
// the partition (and its vid) that should contain a final segment. ok=false for
// a missing or non-container intermediate (the path cannot be created).
func walkToParent(root *pb.ReducedEffect, segs []segment) (parentVid string, parent *pb.ReducedEffect, ok bool) {
	parent = root
	for _, s := range segs {
		var el *pb.ReducedElement
		switch s.kind {
		case segKey:
			if parent.TypeTag != pb.ValueType_TYPE_JSON_OBJECT {
				return "", nil, false
			}
			el = elementByKey(parent, s.key)
		case segIndex:
			if parent.TypeTag != pb.ValueType_TYPE_JSON_ARRAY {
				return "", nil, false
			}
			el = elementByIndex(parent, s.idx)
		}
		if el == nil {
			return "", nil, false
		}
		childVid, isContainer := containerChildVid(el)
		if !isContainer {
			return "", nil, false
		}
		child := nodeAt(root, childVid)
		if child == nil {
			return "", nil, false
		}
		parentVid, parent = childVid, child
	}
	return parentVid, parent, true
}

// setValueAtPath writes v at path. applied=false means the path could not be
// created/addressed (missing intermediate, wrong parent kind, out-of-range
// index) — the caller decides the reply. The root path replaces the whole
// document (key-DEL + re-encode).
func setValueAtPath(emit emitFn, root *pb.ReducedEffect, key string, path *Path, v *Value, keyExists bool) (applied bool, err error) {
	if path.IsRoot() {
		if keyExists {
			if err := emit(keyDelEffect(key)); err != nil {
				return false, err
			}
		}
		return true, encodeRoot(emit, key, v)
	}
	segs := path.segs
	parentVid, parent, ok := walkToParent(root, segs[:len(segs)-1])
	if !ok {
		return false, nil
	}
	last := segs[len(segs)-1]
	switch last.kind {
	case segKey:
		if parent.TypeTag != pb.ValueType_TYPE_JSON_OBJECT {
			return false, nil
		}
		ref, err := encodeRef(emit, key, v)
		if err != nil {
			return false, err
		}
		if elementByKey(parent, last.key) != nil {
			return true, emit(orderedSelfEffect(key, parentVid, []byte(last.key), ref))
		}
		return true, emit(orderedInsertEffect(key, parentVid, []byte(last.key), ref))
	case segIndex:
		if parent.TypeTag != pb.ValueType_TYPE_JSON_ARRAY {
			return false, nil
		}
		el := elementByIndex(parent, last.idx)
		if el == nil {
			return false, nil // SET on an out-of-range index
		}
		ref, err := encodeRef(emit, key, v)
		if err != nil {
			return false, err
		}
		return true, emit(orderedSelfEffect(key, parentVid, el.GetData().GetId(), ref))
	}
	return false, nil
}
