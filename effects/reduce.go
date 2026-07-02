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

package effects

import (
	"bytes"
	"maps"
	"slices"

	pb "github.com/swytchdb/swytch/cluster/proto"
)

// ReduceBranch reduces a linear chain of effects (oldest-first) into a single ReducedEffect.
func ReduceBranch(effects []*pb.Effect) *pb.ReducedEffect {
	return ReduceChain(nil, effects)
}

// ReduceChain reduces effects sequentially on top of a seed ReducedEffect.
// If seed is nil, behaves identically to ReduceBranch.
// This is used at DAG merge points: the seed is the merged result of
// concurrent dep subtrees, and effects are the linear chain above.
// The seed is never mutated; a clone is made before any modifications.
func ReduceChain(seed *pb.ReducedEffect, effects []*pb.Effect) *pb.ReducedEffect {
	if seed == nil && len(effects) == 0 {
		return nil
	}
	return reduceChainOwned(cloneReduced(seed), effects)
}

// reduceChainOwned is ReduceChain for a seed the caller exclusively owns: the
// seed is mutated and returned, never cloned. reconstruct accumulates through
// here once per visited DAG node — a defensive clone at each step would copy
// the whole accumulated state, making reconstruct quadratic in chain length.
func reduceChainOwned(r *pb.ReducedEffect, effects []*pb.Effect) *pb.ReducedEffect {
	hadDel := false

	// A REMOVE_OP seed is a DEL marker — subsequent effects start fresh.
	if r != nil && r.Op == pb.EffectOp_REMOVE_OP && len(effects) > 0 {
		r = nil
		hadDel = true
	}

	for _, e := range effects {
		data := e.GetData()
		meta := e.GetMeta()

		// Key-level DEL: op=REMOVE on a SCALAR with no element ID
		if data != nil && data.Op == pb.EffectOp_REMOVE_OP && data.Collection == pb.CollectionKind_SCALAR {
			r = nil
			hadDel = true
			continue
		}

		// MetaEffect: capture type tag and TTL, doesn't affect data reduction
		if meta != nil {
			if r == nil {
				r = &pb.ReducedEffect{}
			}
			if meta.TypeTag != pb.ValueType_TYPE_UNSPECIFIED {
				r.TypeTag = meta.TypeTag
			}
			if len(meta.ElementId) > 0 {
				// Per-element TTL set or persist. Replace the element, never
				// mutate it: element pointers are shared with cached snapshot
				// state (see cloneReduced), so an in-place ExpiresAt write
				// would leak into every observer of the cached copy.
				if r.NetAdds != nil {
					if elem, ok := r.NetAdds[string(meta.ElementId)]; ok {
						r.NetAdds[string(meta.ElementId)] = &pb.ReducedElement{
							Data:           elem.Data,
							Hlc:            elem.Hlc,
							NodeId:         elem.NodeId,
							ForkChoiceHash: elem.ForkChoiceHash,
							ExpiresAt:      meta.ExpiresAt,
						}
					}
				}
			} else if meta.ExpiresAt != nil || meta.TypeTag == pb.ValueType_TYPE_UNSPECIFIED {
				// Key-level TTL set (ExpiresAt>0) or persist (ExpiresAt=0 with no TypeTag).
				// Skip when ExpiresAt=0 and TypeTag is set — that's just a type tag update.
				r.ExpiresAt = meta.ExpiresAt
			}
			continue
		}

		// SubscriptionEffect: subscriber tracking is no longer part of the data
		// reduction pipeline — it lives on leafState.subscribers (authoritative,
		// maintained incrementally) and is stamped into a snapshot's reduced
		// state at emit time, not rebuilt on every read. Skip it here.
		if e.GetSubscription() != nil {
			continue
		}

		// SerializationEffect: track leader state (commutative metadata)
		if ser := e.GetSerialization(); ser != nil {
			if r == nil {
				r = &pb.ReducedEffect{Commutative: true}
			}
			if ser.Release {
				r.SerializationLeader = nil
			} else {
				id := ser.LeaderNodeId
				r.SerializationLeader = &id
			}
			continue
		}

		// Skip non-data effects (bind, etc.)
		if data == nil {
			continue
		}

		if r == nil {
			r = reduceFirst(e, data)
			continue
		}

		r = reduceAccumulate(r, e, data)
	}

	// DEL was the last effective operation
	if r == nil && hadDel {
		last := effects[len(effects)-1]
		return &pb.ReducedEffect{
			Op:     pb.EffectOp_REMOVE_OP,
			Hlc:    last.Hlc,
			NodeId: last.NodeId,
		}
	}

	// Update tip HLC/NodeID from last effect (causality stamp),
	// but ForkChoiceHash from FIRST effect (branch identity at fork point).
	// For X -> a1 -> a2 vs X -> b1 -> b2, the branch is identified by
	// a1 vs b1 — where it diverged — not by the tip.
	if r != nil && len(effects) > 0 {
		last := effects[len(effects)-1]
		r.Hlc = last.Hlc
		r.NodeId = last.NodeId
		r.ForkChoiceHash = effects[0].ForkChoiceHash
	}

	return r
}

func reduceFirst(e *pb.Effect, d *pb.DataEffect) *pb.ReducedEffect {
	r := &pb.ReducedEffect{
		Op:             d.Op,
		Merge:          d.Merge,
		Collection:     d.Collection,
		Hlc:            e.Hlc,
		NodeId:         e.NodeId,
		Commutative:    isCommutative(d.Merge),
		ForkChoiceHash: e.ForkChoiceHash,
	}

	switch d.Collection {
	case pb.CollectionKind_SCALAR:
		r.Scalar = cloneData(d)

	case pb.CollectionKind_KEYED:
		r.NetAdds = make(map[string]*pb.ReducedElement)
		r.NetRemoves = make(map[string]bool)
		key := string(d.Id)
		if d.Op == pb.EffectOp_INSERT_OP {
			r.NetAdds[key] = elementFromData(e, d)
		} else {
			r.NetRemoves[key] = true
		}

	case pb.CollectionKind_ORDERED:
		r.NetRemoves = make(map[string]bool)
		if d.Op == pb.EffectOp_INSERT_OP {
			r.OrderedElements = []*pb.ReducedElement{elementFromData(e, d)}
		}
	}

	return r
}

func reduceAccumulate(r *pb.ReducedEffect, e *pb.Effect, d *pb.DataEffect) *pb.ReducedEffect {
	// When switching to SCALAR, clear KEYED/ORDERED overlay state.
	// A SCALAR write (SET, BITOP) replaces the entire value — any
	// previous KEYED toggles or ORDERED elements are stale.
	if d.Collection == pb.CollectionKind_SCALAR && r.Collection != pb.CollectionKind_SCALAR {
		r.NetAdds = nil
		r.NetRemoves = nil
		r.OrderedElements = nil
	}

	// Update collection kind to match the data effect. This is necessary
	// when a MetaEffect created the ReducedEffect first (defaulting Collection
	// to SCALAR/0) and subsequent data effects use a different collection.
	r.Collection = d.Collection

	switch d.Collection {
	case pb.CollectionKind_SCALAR:
		reduceScalar(r, e, d)
	case pb.CollectionKind_KEYED:
		reduceKeyed(r, e, d)
	case pb.CollectionKind_ORDERED:
		reduceOrdered(r, e, d)
	}
	return r
}

func reduceScalar(r *pb.ReducedEffect, _ *pb.Effect, d *pb.DataEffect) {
	if r.Scalar == nil {
		r.Scalar = cloneData(d)
		r.Merge = d.Merge
		return
	}
	switch d.Merge {
	case pb.MergeRule_ADDITIVE_INT:
		if r.Merge == pb.MergeRule_ADDITIVE_INT {
			r.Scalar.Value = &pb.DataEffect_IntVal{IntVal: r.Scalar.GetIntVal() + d.GetIntVal()}
		} else if r.Merge == pb.MergeRule_LAST_WRITE_WINS {
			var base int64
			if _, ok := r.Scalar.Value.(*pb.DataEffect_IntVal); ok {
				base = r.Scalar.GetIntVal()
			} else {
				base = rawToInt(r.Scalar.GetRaw())
			}
			r.Scalar.Value = &pb.DataEffect_IntVal{IntVal: base + d.GetIntVal()}
			r.Scalar.Merge = pb.MergeRule_LAST_WRITE_WINS
			r.Merge = pb.MergeRule_LAST_WRITE_WINS
			r.Commutative = false
		}

	case pb.MergeRule_ADDITIVE_FLOAT:
		if r.Merge == pb.MergeRule_ADDITIVE_FLOAT {
			r.Scalar.Value = &pb.DataEffect_FloatVal{FloatVal: r.Scalar.GetFloatVal() + d.GetFloatVal()}
		} else if r.Merge == pb.MergeRule_LAST_WRITE_WINS {
			var base float64
			if _, ok := r.Scalar.Value.(*pb.DataEffect_FloatVal); ok {
				base = r.Scalar.GetFloatVal()
			} else {
				base = rawToFloat(r.Scalar.GetRaw())
			}
			r.Scalar.Value = &pb.DataEffect_FloatVal{FloatVal: base + d.GetFloatVal()}
			r.Scalar.Merge = pb.MergeRule_LAST_WRITE_WINS
			r.Merge = pb.MergeRule_LAST_WRITE_WINS
			r.Commutative = false
		}

	case pb.MergeRule_LAST_WRITE_WINS:
		r.Scalar = cloneData(d)
		r.Merge = pb.MergeRule_LAST_WRITE_WINS
		r.Commutative = false
		// A LWW write replaces the entire value — clear any inherited TTL.
		// If a TTL is desired, an explicit MetaEffect follows (EX/PX/KEEPTTL).
		r.ExpiresAt = nil

	case pb.MergeRule_MAX_INT:
		if d.GetIntVal() > r.Scalar.GetIntVal() {
			r.Scalar.Value = &pb.DataEffect_IntVal{IntVal: d.GetIntVal()}
		}

	case pb.MergeRule_MAX_BYTES:
		if r.Scalar == nil {
			// First data effect — clone and make our own copy of the bytes
			r.Scalar = cloneData(d)
			src := d.GetRaw()
			owned := make([]byte, len(src))
			copy(owned, src)
			r.Scalar.Value = &pb.DataEffect_Raw{Raw: owned}
		} else {
			a := r.Scalar.GetRaw()
			b := d.GetRaw()
			if len(a) >= len(b) {
				// Mutate in-place — we own this slice
				for i := range b {
					if b[i] > a[i] {
						a[i] = b[i]
					}
				}
			} else {
				result := make([]byte, len(b))
				copy(result, a)
				for i := range b {
					if b[i] > result[i] {
						result[i] = b[i]
					}
				}
				r.Scalar.Value = &pb.DataEffect_Raw{Raw: result}
			}
		}
	}
}

func reduceKeyed(r *pb.ReducedEffect, e *pb.Effect, d *pb.DataEffect) {
	if r.NetAdds == nil {
		r.NetAdds = make(map[string]*pb.ReducedElement)
	}
	if r.NetRemoves == nil {
		r.NetRemoves = make(map[string]bool)
	}

	key := string(d.Id)

	if d.Op == pb.EffectOp_REMOVE_OP {
		if _, inAdds := r.NetAdds[key]; inAdds {
			delete(r.NetAdds, key)
		} else {
			r.NetRemoves[key] = true
		}
		return
	}

	// INSERT
	existing, exists := r.NetAdds[key]
	if !exists {
		r.NetAdds[key] = elementFromData(e, d)
		delete(r.NetRemoves, key)
		return
	}

	// Accumulate into existing element — always create a new element to
	// avoid mutating shared pointers (e.g. snapshot state in the effect cache).
	switch d.Merge {
	case pb.MergeRule_ADDITIVE_INT:
		base := existing.Data.GetIntVal()
		if raw := existing.Data.GetRaw(); raw != nil {
			base = rawToInt(raw)
		}
		newElem := &pb.ReducedElement{
			Data:      cloneData(existing.Data),
			Hlc:       existing.Hlc,
			NodeId:    existing.NodeId,
			ExpiresAt: existing.ExpiresAt,
		}
		newElem.Data.Value = &pb.DataEffect_IntVal{IntVal: base + d.GetIntVal()}
		r.NetAdds[key] = newElem
	case pb.MergeRule_ADDITIVE_FLOAT:
		base := existing.Data.GetFloatVal()
		if raw := existing.Data.GetRaw(); raw != nil {
			base = rawToFloat(raw)
		}
		newElem := &pb.ReducedElement{
			Data:      cloneData(existing.Data),
			Hlc:       existing.Hlc,
			NodeId:    existing.NodeId,
			ExpiresAt: existing.ExpiresAt,
		}
		newElem.Data.Value = &pb.DataEffect_FloatVal{FloatVal: base + d.GetFloatVal()}
		r.NetAdds[key] = newElem
	case pb.MergeRule_MAX_INT:
		if d.GetIntVal() > existing.Data.GetIntVal() {
			newElem := &pb.ReducedElement{
				Data:      cloneData(existing.Data),
				Hlc:       existing.Hlc,
				NodeId:    existing.NodeId,
				ExpiresAt: existing.ExpiresAt,
			}
			newElem.Data.Value = &pb.DataEffect_IntVal{IntVal: d.GetIntVal()}
			r.NetAdds[key] = newElem
		}
	default:
		// LWW: replace
		r.NetAdds[key] = elementFromData(e, d)
	}
}

// --- Ordered reduction ---

func reduceOrdered(r *pb.ReducedEffect, e *pb.Effect, d *pb.DataEffect) {
	if r.NetRemoves == nil {
		r.NetRemoves = make(map[string]bool)
	}

	id := string(d.Id)

	if d.Op == pb.EffectOp_REMOVE_OP {
		idx := findOrderedByID(r.OrderedElements, d.Id)
		if idx >= 0 {
			r.OrderedElements = slices.Delete(r.OrderedElements, idx, idx+1)
		} else {
			// Element from before this branch — track for merge
			r.NetRemoves[id] = true
		}
		return
	}

	elem := elementFromData(e, d)

	switch d.Placement {
	case pb.Placement_PLACE_HEAD:
		r.OrderedElements = slices.Insert(r.OrderedElements, 0, elem)

	case pb.Placement_PLACE_TAIL, pb.Placement_PLACE_NONE:
		r.OrderedElements = append(r.OrderedElements, elem)

	case pb.Placement_PLACE_BEFORE:
		idx := findOrderedByID(r.OrderedElements, d.Reference)
		if idx >= 0 {
			r.OrderedElements = slices.Insert(r.OrderedElements, idx, elem)
		} else {
			// Reference not found, treat as head (conservative)
			r.OrderedElements = slices.Insert(r.OrderedElements, 0, elem)
		}

	case pb.Placement_PLACE_AFTER:
		idx := findOrderedByID(r.OrderedElements, d.Reference)
		if idx >= 0 {
			r.OrderedElements = slices.Insert(r.OrderedElements, idx+1, elem)
		} else {
			// Reference not found, treat as tail (conservative)
			r.OrderedElements = append(r.OrderedElements, elem)
		}

	case pb.Placement_PLACE_SELF:
		// Update existing element in place (LSET)
		idx := findOrderedByID(r.OrderedElements, d.Id)
		if idx >= 0 {
			r.OrderedElements[idx] = elem
		}
	}
}

func findOrderedByID(elements []*pb.ReducedElement, id []byte) int {
	for i, elem := range elements {
		if bytes.Equal(elem.Data.Id, id) {
			return i
		}
	}
	return -1
}

// --- Helpers ---

func isCommutative(m pb.MergeRule) bool {
	switch m {
	case pb.MergeRule_ADDITIVE_INT, pb.MergeRule_ADDITIVE_FLOAT, pb.MergeRule_MAX_INT, pb.MergeRule_MAX_BYTES:
		return true
	default:
		return false
	}
}

// cloneReduced creates a copy of a ReducedEffect with independent mutable
// sub-structures (Scalar, maps, slices). ReducedElement pointers within
// maps/slices are shared — callers must replace (not mutate) elements
// during reduction to avoid corrupting cached snapshots.
func cloneReduced(r *pb.ReducedEffect) *pb.ReducedEffect {
	if r == nil {
		return nil
	}
	c := &pb.ReducedEffect{
		Op:                  r.Op,
		Merge:               r.Merge,
		Collection:          r.Collection,
		Hlc:                 r.Hlc,
		NodeId:              r.NodeId,
		Commutative:         r.Commutative,
		TypeTag:             r.TypeTag,
		ExpiresAt:           r.ExpiresAt,
		SerializationLeader: r.SerializationLeader,
		ForkChoiceHash:      r.ForkChoiceHash,
	}
	if r.Scalar != nil {
		c.Scalar = cloneData(r.Scalar)
	}
	if r.NetAdds != nil {
		c.NetAdds = make(map[string]*pb.ReducedElement, len(r.NetAdds))
		maps.Copy(c.NetAdds, r.NetAdds)
	}
	if r.NetRemoves != nil {
		c.NetRemoves = make(map[string]bool, len(r.NetRemoves))
		maps.Copy(c.NetRemoves, r.NetRemoves)
	}
	if r.OrderedElements != nil {
		c.OrderedElements = make([]*pb.ReducedElement, len(r.OrderedElements))
		copy(c.OrderedElements, r.OrderedElements)
	}
	// Subscribers is intentionally NOT cloned: it is not part of the read/data
	// reduction pipeline. A snapshot's State.Subscribers is read directly from
	// the cached effect at bootstrap; copying it on every read was the O(N)
	// per-read cost this refactor removes.
	return c
}

// cloneData creates a shallow copy of a DataEffect, sharing byte slices.
func cloneData(d *pb.DataEffect) *pb.DataEffect {
	result := &pb.DataEffect{
		Op:         d.Op,
		Merge:      d.Merge,
		Collection: d.Collection,
		Placement:  d.Placement,
		Reference:  d.Reference,
		Id:         d.Id,
	}
	switch v := d.Value.(type) {
	case *pb.DataEffect_Raw:
		result.Value = &pb.DataEffect_Raw{Raw: v.Raw}
	case *pb.DataEffect_IntVal:
		result.Value = &pb.DataEffect_IntVal{IntVal: v.IntVal}
	case *pb.DataEffect_FloatVal:
		result.Value = &pb.DataEffect_FloatVal{FloatVal: v.FloatVal}
	}
	return result
}

func elementFromData(e *pb.Effect, d *pb.DataEffect) *pb.ReducedElement {
	return &pb.ReducedElement{
		Data:           cloneData(d),
		Hlc:            e.Hlc,
		NodeId:         e.NodeId,
		ForkChoiceHash: e.ForkChoiceHash,
	}
}

func rawToInt(b []byte) int64 {
	if len(b) == 0 {
		return 0
	}
	v, _ := parseInt(b)
	return v
}

func rawToFloat(b []byte) float64 {
	if len(b) == 0 {
		return 0
	}
	v, _ := parseFloat(b)
	return v
}
