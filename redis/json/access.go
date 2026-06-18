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

func keyDelEffect(key string) *pb.Effect {
	return &pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{Op: pb.EffectOp_REMOVE_OP, Collection: pb.CollectionKind_SCALAR}},
	}
}

func orderedSelfEffect(key, vid string, id, ref []byte) *pb.Effect {
	return &pb.Effect{
		Key:     []byte(key),
		Virtual: virtBytes(vid),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_ORDERED, Placement: pb.Placement_PLACE_SELF,
			Id: id, Value: &pb.DataEffect_Raw{Raw: ref},
		}},
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
	ref := el.GetData().GetRaw()
	if len(ref) > 0 && ref[0] == refContainer {
		return string(ref[1:]), true
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
