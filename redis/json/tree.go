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
	"github.com/swytchdb/swytch/redis/shared"
)

// The tree layer maps a JSON document onto the engine's virtual partitions.
//
// Each container node (object or array) lives in its own partition, addressed
// by an opaque virtual id (the root node's virtual is ""). A node's partition
// is an ORDERED collection whose TypeTag records its kind (object vs array):
//   - object: element id = member name, in insertion order.
//   - array:  element id = a generated handle, in positional order.
//
// An element's value is a reference: 's' + JSON text for a scalar member
// (inline, no child partition), or 'c' + childVid for a container member whose
// contents live in partition childVid. Replacing a node just points its parent
// element at a fresh childVid; the old partition becomes an unreachable orphan.
//
// A root scalar (e.g. JSON.SET k $ 5) is stored as a SCALAR root partition with
// TypeTag TYPE_JSON.

const (
	refScalar    = 's'
	refContainer = 'c'
)

// emitFn emits one effect. Handlers pass a closure over cmd.Context.Emit (with
// the read tips threaded onto the first call); tests pass a collector.
type emitFn func(*pb.Effect) error

// newVid mints a globally-unique virtual id for a container node.
func newVid() string {
	return string(shared.EncodeElementID(shared.NextHLC(), shared.EffectsNodeID))
}

func virtBytes(vid string) []byte {
	if vid == "" {
		return nil
	}
	return []byte(vid)
}

func metaEffect(key, vid string, tag pb.ValueType) *pb.Effect {
	return &pb.Effect{
		Key:     []byte(key),
		Virtual: virtBytes(vid),
		Kind:    &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: tag}},
	}
}

func scalarEffect(key, vid string, raw []byte) *pb.Effect {
	return &pb.Effect{
		Key:     []byte(key),
		Virtual: virtBytes(vid),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR, Value: &pb.DataEffect_Raw{Raw: raw},
		}},
	}
}

func orderedInsertEffect(key, vid string, id, ref []byte) *pb.Effect {
	return &pb.Effect{
		Key:     []byte(key),
		Virtual: virtBytes(vid),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op: pb.EffectOp_INSERT_OP, Merge: pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_ORDERED, Placement: pb.Placement_PLACE_TAIL,
			Id: id, Value: &pb.DataEffect_Raw{Raw: ref},
		}},
	}
}

// encodeRoot writes a whole document to the key's root partition ("").
func encodeRoot(emit emitFn, key string, v *Value) error {
	if v.IsContainer() {
		return encodeContainer(emit, key, "", v)
	}
	if err := emit(metaEffect(key, "", pb.ValueType_TYPE_JSON)); err != nil {
		return err
	}
	return emit(scalarEffect(key, "", Serialize(v)))
}

// encodeContainer writes an object or array node into partition vid.
func encodeContainer(emit emitFn, key, vid string, v *Value) error {
	if v.Kind == KindObject {
		if err := emit(metaEffect(key, vid, pb.ValueType_TYPE_JSON_OBJECT)); err != nil {
			return err
		}
		for _, m := range v.Obj {
			ref, err := encodeRef(emit, key, m.Val)
			if err != nil {
				return err
			}
			if err := emit(orderedInsertEffect(key, vid, []byte(m.Key), ref)); err != nil {
				return err
			}
		}
		return nil
	}
	if err := emit(metaEffect(key, vid, pb.ValueType_TYPE_JSON_ARRAY)); err != nil {
		return err
	}
	for _, iv := range v.Arr {
		ref, err := encodeRef(emit, key, iv)
		if err != nil {
			return err
		}
		if err := emit(orderedInsertEffect(key, vid, []byte(newVid()), ref)); err != nil {
			return err
		}
	}
	return nil
}

// encodeRef returns a member's element-value reference, recursively encoding a
// container member into its own fresh partition.
func encodeRef(emit emitFn, key string, v *Value) ([]byte, error) {
	if v.IsContainer() {
		childVid := newVid()
		if err := encodeContainer(emit, key, childVid, v); err != nil {
			return nil, err
		}
		return append([]byte{refContainer}, childVid...), nil
	}
	return append([]byte{refScalar}, Serialize(v)...), nil
}

// nodeAt returns the partition holding the node at vid (root for "").
func nodeAt(root *pb.ReducedEffect, vid string) *pb.ReducedEffect {
	if vid == "" {
		return root
	}
	return root.Partitions[vid]
}

// assemble reconstructs the JSON value rooted at vid from the partition map,
// following container references and dropping unreachable orphans. Returns nil
// when the node is missing.
func assemble(root *pb.ReducedEffect, vid string) *Value {
	p := nodeAt(root, vid)
	if p == nil {
		return nil
	}
	switch p.TypeTag {
	case pb.ValueType_TYPE_JSON_OBJECT:
		obj := &Value{Kind: KindObject}
		for _, el := range p.OrderedElements {
			if v := decodeRef(root, el.GetData().GetRaw()); v != nil {
				obj.Obj = append(obj.Obj, Member{Key: string(el.GetData().GetId()), Val: v})
			}
		}
		return obj
	case pb.ValueType_TYPE_JSON_ARRAY:
		arr := &Value{Kind: KindArray}
		for _, el := range p.OrderedElements {
			if v := decodeRef(root, el.GetData().GetRaw()); v != nil {
				arr.Arr = append(arr.Arr, v)
			}
		}
		return arr
	default:
		if p.GetScalar() != nil {
			if v, err := Parse(p.GetScalar().GetRaw()); err == nil {
				return v
			}
		}
		return nil
	}
}

// decodeRef resolves a member element-value reference to a Value.
func decodeRef(root *pb.ReducedEffect, ref []byte) *Value {
	if len(ref) == 0 {
		return nil
	}
	switch ref[0] {
	case refScalar:
		if v, err := Parse(ref[1:]); err == nil {
			return v
		}
	case refContainer:
		return assemble(root, string(ref[1:]))
	}
	return nil
}
