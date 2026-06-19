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
	"maps"
	"sort"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Merge2 merges two reduced effects from concurrent branches into one,
// including their virtual partitions (each merged independently; flat keys
// carry no partitions and take the unchanged top-level path). The result is
// always a freshly constructed *pb.ReducedEffect — inputs are never mutated.
func Merge2(a, b *pb.ReducedEffect) *pb.ReducedEffect {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	r := merge2Flat(a, b)
	if len(a.Partitions) > 0 || len(b.Partitions) > 0 {
		if r == nil {
			r = &pb.ReducedEffect{}
		}
		r.Partitions = mergePartitionsN([]*pb.ReducedEffect{a, b})
	}
	return r
}

// mergePartitionsN canonically merges the virtual partitions across N branches:
// each virtual key is merged independently via MergeN over the branches that
// carry it. Merging per virtual (rather than pairwise-folding whole maps) keeps
// a partition with mixed commutativity — e.g. a leaf seeing both JSON.NUMINCRBY
// (additive) and JSON.SET (LWW) — correct. Recursion terminates because
// partition values are flat (their own .Partitions is empty). Returns nil when
// no branch has partitions.
func mergePartitionsN(branches []*pb.ReducedEffect) map[string]*pb.ReducedEffect {
	var byKey map[string][]*pb.ReducedEffect
	for _, b := range branches {
		for v, p := range b.Partitions {
			if byKey == nil {
				byKey = make(map[string][]*pb.ReducedEffect)
			}
			byKey[v] = append(byKey[v], p)
		}
	}
	if byKey == nil {
		return nil
	}
	out := make(map[string]*pb.ReducedEffect, len(byKey))
	for v, subs := range byKey {
		if m := MergeN(subs); m != nil {
			out[v] = m
		}
	}
	return out
}

// merge2Flat merges two reduced effects' top-level (non-partition) state.
// This is the pre-nesting Merge2.
//
// Decision tree:
//   - Both commutative, same collection → accumulate (sum/max/union)
//   - Both non-commutative → HLC tiebreaker
//   - Mixed (comm + non-comm), compatible → apply comm on top of non-comm
//   - Mixed, incompatible → non-comm wins
//   - Cross-collection → HLC tiebreaker
func merge2Flat(a, b *pb.ReducedEffect) *pb.ReducedEffect {
	// Metadata-only branches (subscriptions, serialization) carry no data.
	// Merge their metadata onto the data branch transparently.
	if isMetadataOnly(a) {
		return mergeMetadataOnto(b, a)
	}
	if isMetadataOnly(b) {
		return mergeMetadataOnto(a, b)
	}

	if a.Commutative && b.Commutative {
		return mergeBothCommutative(a, b)
	}
	if !a.Commutative && !b.Commutative {
		return mergeBothNonCommutative(a, b)
	}
	return mergeMixed(a, b)
}

// isMetadataOnly returns true if the ReducedEffect carries only metadata
// (subscribers, serialization leader) with no data content.
func isMetadataOnly(r *pb.ReducedEffect) bool {
	return r.Scalar == nil && len(r.NetAdds) == 0 && len(r.NetRemoves) == 0 && len(r.OrderedElements) == 0 && r.Op == 0
}

// mergeMetadataOnto unions the metadata from meta onto data, returning a new ReducedEffect.
func mergeMetadataOnto(data, meta *pb.ReducedEffect) *pb.ReducedEffect {
	r := cloneReduced(data)
	r.SerializationLeader = mergeSerializationLeader(data, meta)
	return r
}

// MergeN merges N reduced effects from concurrent branches using canonical
// merge ordering. This replaces pairwise Merge2 for multi-tip reconstruction.
//
// Canonical ordering:
//  1. Non-commutative branches first. Among them, LWW applies — highest HLC
//     wins. Losing non-commutative branches are dead and skipped entirely.
//  2. Commutative branches second. All commutative branches accumulate on top
//     of the non-commutative winner (or on top of each other if no non-comm).
//
// The result is deterministic: any node with the same set of branches computes
// the same result regardless of the order branches are provided.
// Inputs are never mutated.
func MergeN(branches []*pb.ReducedEffect) *pb.ReducedEffect {
	// Filter nils
	filtered := make([]*pb.ReducedEffect, 0, len(branches))
	for _, b := range branches {
		if b != nil {
			filtered = append(filtered, b)
		}
	}

	switch len(filtered) {
	case 0:
		return nil
	case 1:
		return filtered[0]
	case 2:
		return Merge2(filtered[0], filtered[1])
	}

	// Partition into non-commutative, commutative, and metadata-only
	var nonComm, comm, meta []*pb.ReducedEffect
	for _, b := range filtered {
		if isMetadataOnly(b) {
			meta = append(meta, b)
		} else if b.Commutative {
			comm = append(comm, b)
		} else {
			nonComm = append(nonComm, b)
		}
	}

	// Sort commutative branches for determinism: by fork_choice_hash ascending
	sort.Slice(comm, func(i, j int) bool {
		return ForkChoiceLess(comm[i].ForkChoiceHash, comm[j].ForkChoiceHash)
	})

	// Sort non-commutative branches for determinism: by fork_choice_hash ascending.
	// For SCALAR, Merge2 picks LWW winner (transitive — fold produces same
	// result as picking the global winner). For ORDERED/KEYED, Merge2
	// merges all branches (contiguous for ORDERED per §2.3).
	sort.Slice(nonComm, func(i, j int) bool {
		return ForkChoiceLess(nonComm[i].ForkChoiceHash, nonComm[j].ForkChoiceHash)
	})

	var result *pb.ReducedEffect

	if len(nonComm) > 0 {
		result = nonComm[0]
		for _, nc := range nonComm[1:] {
			result = Merge2(result, nc)
		}
	}

	// Accumulate commutative branches on top
	if result == nil && len(comm) > 0 {
		// All commutative — fold them
		result = comm[0]
		for _, c := range comm[1:] {
			result = Merge2(result, c)
		}
	} else if len(comm) > 0 {
		for _, c := range comm {
			result = Merge2(result, c)
		}
	}

	// Fold metadata-only branches (subscriptions, serialization) on top
	for _, m := range meta {
		if result == nil {
			result = m
		} else {
			result = mergeMetadataOnto(result, m)
		}
	}

	// Construct final result with metadata from ALL branches (including
	// dead non-comm losers whose subscribers must still be preserved).
	return &pb.ReducedEffect{
		Op:                  result.Op,
		Merge:               result.Merge,
		Collection:          result.Collection,
		Hlc:                 result.Hlc,
		NodeId:              result.NodeId,
		Commutative:         result.Commutative,
		Scalar:              result.Scalar,
		NetAdds:             result.NetAdds,
		NetRemoves:          result.NetRemoves,
		OrderedElements:     result.OrderedElements,
		TypeTag:             result.TypeTag,
		ExpiresAt:           result.ExpiresAt,
		SerializationLeader: mergeSerializationLeaderN(filtered),
		ForkChoiceHash:      result.ForkChoiceHash,
		Partitions:          mergePartitionsN(filtered),
	}
}

func mergeBothCommutative(a, b *pb.ReducedEffect) *pb.ReducedEffect {
	if a.Collection != b.Collection {
		return mergeIncompatible(a, b)
	}

	winner, _ := branchWinner(a, b)
	maxHLC, maxNodeID := maxTip(a, b)

	var r *pb.ReducedEffect
	switch a.Collection {
	case pb.CollectionKind_SCALAR:
		r = mergeCommutativeScalars(a, b, winner, maxHLC, maxNodeID)
	case pb.CollectionKind_KEYED:
		r = mergeKeyedElements(a, b, true, winner, maxHLC, maxNodeID)
	case pb.CollectionKind_ORDERED:
		r = mergeOrderedElements(a, b, true, winner, maxHLC, maxNodeID)
	default:
		return mergeIncompatible(a, b)
	}

	r.SerializationLeader = mergeSerializationLeader(a, b)
	return r
}

func mergeBothNonCommutative(a, b *pb.ReducedEffect) *pb.ReducedEffect {
	if a.Collection != b.Collection {
		return mergeIncompatible(a, b)
	}

	winner, _ := branchWinner(a, b)
	maxHLC, maxNodeID := maxTip(a, b)

	var r *pb.ReducedEffect
	switch a.Collection {
	case pb.CollectionKind_SCALAR:
		return mergeIncompatible(a, b)
	case pb.CollectionKind_KEYED:
		r = mergeKeyedElements(a, b, false, winner, maxHLC, maxNodeID)
	case pb.CollectionKind_ORDERED:
		r = mergeOrderedElements(a, b, false, winner, maxHLC, maxNodeID)
	default:
		return mergeIncompatible(a, b)
	}

	r.SerializationLeader = mergeSerializationLeader(a, b)
	return r
}

func mergeMixed(a, b *pb.ReducedEffect) *pb.ReducedEffect {
	var comm, nonComm *pb.ReducedEffect
	if a.Commutative {
		comm, nonComm = a, b
	} else {
		comm, nonComm = b, a
	}

	winner, _ := branchWinner(a, b)
	maxHLC, maxNodeID := maxTip(a, b)

	var r *pb.ReducedEffect

	// Same collection kind: try to combine
	if comm.Collection == nonComm.Collection {
		switch comm.Collection {
		case pb.CollectionKind_SCALAR:
			r = mergeMixedScalar(comm, nonComm, winner, maxHLC, maxNodeID)
		case pb.CollectionKind_KEYED:
			r = mergeKeyedElements(nonComm, comm, false, winner, maxHLC, maxNodeID)
		case pb.CollectionKind_ORDERED:
			r = mergeOrderedElements(nonComm, comm, false, winner, maxHLC, maxNodeID)
		}
	}

	if r == nil {
		// Cross-collection or unhandled: non-comm wins (type change)
		return mergeIncompatible(a, b)
	}

	r.SerializationLeader = mergeSerializationLeader(a, b)
	return r
}

// --- Scalar merge helpers ---

func mergeCommutativeScalars(a, b, winner *pb.ReducedEffect, hlc time.Time, nodeID uint64) *pb.ReducedEffect {
	if a.Merge != b.Merge {
		return mergeIncompatibleCore(a, b, winner, hlc, nodeID)
	}

	r := &pb.ReducedEffect{
		Op:             pb.EffectOp_INSERT_OP,
		Merge:          a.Merge,
		Collection:     pb.CollectionKind_SCALAR,
		Hlc:            timestamppb.New(hlc),
		NodeId:         nodeID,
		Commutative:    true,
		ForkChoiceHash: winner.ForkChoiceHash,
		Scalar: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      a.Merge,
			Collection: pb.CollectionKind_SCALAR,
		},
		TypeTag:   winner.TypeTag,
		ExpiresAt: winner.ExpiresAt,
	}

	switch a.Merge {
	case pb.MergeRule_ADDITIVE_INT:
		r.Scalar.Value = &pb.DataEffect_IntVal{IntVal: a.Scalar.GetIntVal() + b.Scalar.GetIntVal()}
	case pb.MergeRule_ADDITIVE_FLOAT:
		r.Scalar.Value = &pb.DataEffect_FloatVal{FloatVal: a.Scalar.GetFloatVal() + b.Scalar.GetFloatVal()}
	case pb.MergeRule_MAX_INT:
		av, bv := a.Scalar.GetIntVal(), b.Scalar.GetIntVal()
		if av > bv {
			r.Scalar.Value = &pb.DataEffect_IntVal{IntVal: av}
		} else {
			r.Scalar.Value = &pb.DataEffect_IntVal{IntVal: bv}
		}
	case pb.MergeRule_MAX_BYTES:
		ab, bb := a.Scalar.Decompress(), b.Scalar.Decompress()
		result := make([]byte, max(len(ab), len(bb)))
		copy(result, ab)
		for i := range bb {
			if i < len(result) && bb[i] > result[i] {
				result[i] = bb[i]
			}
		}
		r.Scalar.Value = &pb.DataEffect_Raw{Raw: result}
	}

	return r
}

func mergeMixedScalar(comm, nonComm, winner *pb.ReducedEffect, hlc time.Time, nodeID uint64) *pb.ReducedEffect {
	r := &pb.ReducedEffect{
		Op:             pb.EffectOp_INSERT_OP,
		Merge:          pb.MergeRule_LAST_WRITE_WINS,
		Collection:     pb.CollectionKind_SCALAR,
		Hlc:            timestamppb.New(hlc),
		NodeId:         nodeID,
		Commutative:    false,
		ForkChoiceHash: winner.ForkChoiceHash,
		Scalar: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
		},
		TypeTag:   winner.TypeTag,
		ExpiresAt: winner.ExpiresAt,
	}

	switch comm.Merge {
	case pb.MergeRule_ADDITIVE_INT:
		baseVal := nonComm.Scalar.GetIntVal()
		if raw := nonComm.Scalar.Decompress(); raw != nil {
			baseVal = rawToInt(raw)
		}
		r.Scalar.Value = &pb.DataEffect_IntVal{IntVal: baseVal + comm.Scalar.GetIntVal()}
		return r

	case pb.MergeRule_ADDITIVE_FLOAT:
		baseVal := nonComm.Scalar.GetFloatVal()
		if raw := nonComm.Scalar.Decompress(); raw != nil {
			baseVal = rawToFloat(raw)
		}
		r.Scalar.Value = &pb.DataEffect_FloatVal{FloatVal: baseVal + comm.Scalar.GetFloatVal()}
		return r
	}

	// Incompatible: non-comm wins, but we still construct a new result
	return &pb.ReducedEffect{
		Op:              nonComm.Op,
		Merge:           nonComm.Merge,
		Collection:      nonComm.Collection,
		Hlc:             timestamppb.New(hlc),
		NodeId:          nodeID,
		Commutative:     nonComm.Commutative,
		Scalar:          nonComm.Scalar,
		NetAdds:         nonComm.NetAdds,
		NetRemoves:      nonComm.NetRemoves,
		OrderedElements: nonComm.OrderedElements,
		TypeTag:         nonComm.TypeTag,
		ExpiresAt:       nonComm.ExpiresAt,
		ForkChoiceHash:  nonComm.ForkChoiceHash,
	}
}

// --- Keyed merge helpers ---

func mergeKeyedElements(a, b *pb.ReducedEffect, commutative bool, winner *pb.ReducedEffect, hlc time.Time, nodeID uint64) *pb.ReducedEffect {
	r := &pb.ReducedEffect{
		Op:             pb.EffectOp_INSERT_OP,
		Merge:          a.Merge,
		Collection:     pb.CollectionKind_KEYED,
		Hlc:            timestamppb.New(hlc),
		NodeId:         nodeID,
		Commutative:    commutative,
		ForkChoiceHash: winner.ForkChoiceHash,
		NetAdds:        make(map[string]*pb.ReducedElement),
		NetRemoves:     make(map[string]bool),
		TypeTag:        winner.TypeTag,
		ExpiresAt:      winner.ExpiresAt,
	}

	// Copy a's state
	maps.Copy(r.NetAdds, a.NetAdds)
	for k := range a.NetRemoves {
		r.NetRemoves[k] = true
	}

	// Merge b's adds — intent-based conflict resolution for KEYED collections.
	for k, bElem := range b.NetAdds {
		if r.NetRemoves[k] {
			// Member existed at fork; a removed it. REMOVE wins.
			continue
		}
		if aElem, exists := r.NetAdds[k]; exists {
			r.NetAdds[k] = mergeElements(aElem, bElem)
		} else {
			r.NetAdds[k] = bElem
		}
	}

	// Merge b's removes — intent-based conflict resolution.
	for k := range b.NetRemoves {
		delete(r.NetAdds, k)
		r.NetRemoves[k] = true
	}

	return r
}

func maxTime(a, b *timestamppb.Timestamp) *timestamppb.Timestamp {
	if a.AsTime().After(b.AsTime()) {
		return a
	}
	return b
}

func mergeElements(a, b *pb.ReducedElement) *pb.ReducedElement {
	// Same merge rule: accumulate
	if a.Data.Merge == b.Data.Merge {
		hlcWinner := elementForkChoiceWinner(a, b)
		switch a.Data.Merge {
		case pb.MergeRule_ADDITIVE_INT:
			result := &pb.ReducedElement{
				Data:           cloneData(a.Data),
				Hlc:            maxTime(a.Hlc, b.Hlc),
				NodeId:         forkChoiceWinnerNodeID(a.ForkChoiceHash, a.NodeId, b.ForkChoiceHash, b.NodeId),
				ExpiresAt:      hlcWinner.ExpiresAt,
				ForkChoiceHash: hlcWinner.ForkChoiceHash,
			}
			result.Data.Value = &pb.DataEffect_IntVal{IntVal: a.Data.GetIntVal() + b.Data.GetIntVal()}
			return result
		case pb.MergeRule_ADDITIVE_FLOAT:
			result := &pb.ReducedElement{
				Data:           cloneData(a.Data),
				Hlc:            maxTime(a.Hlc, b.Hlc),
				NodeId:         forkChoiceWinnerNodeID(a.ForkChoiceHash, a.NodeId, b.ForkChoiceHash, b.NodeId),
				ExpiresAt:      hlcWinner.ExpiresAt,
				ForkChoiceHash: hlcWinner.ForkChoiceHash,
			}
			result.Data.Value = &pb.DataEffect_FloatVal{FloatVal: a.Data.GetFloatVal() + b.Data.GetFloatVal()}
			return result
		case pb.MergeRule_MAX_INT:
			winner := a
			if b.Data.GetIntVal() > a.Data.GetIntVal() {
				winner = b
			}
			return &pb.ReducedElement{
				Data:           cloneData(winner.Data),
				Hlc:            maxTime(a.Hlc, b.Hlc),
				NodeId:         forkChoiceWinnerNodeID(a.ForkChoiceHash, a.NodeId, b.ForkChoiceHash, b.NodeId),
				ExpiresAt:      hlcWinner.ExpiresAt,
				ForkChoiceHash: hlcWinner.ForkChoiceHash,
			}
		}
	}

	// Mixed merge rules on same element (e.g., ZADD absolute + ZINCRBY delta)
	if isCommutative(a.Data.Merge) && !isCommutative(b.Data.Merge) {
		return applyElementDelta(b, a)
	}
	if !isCommutative(a.Data.Merge) && isCommutative(b.Data.Merge) {
		return applyElementDelta(a, b)
	}

	// Both non-commutative: lowest fork_choice_hash wins
	if ForkChoiceLess(a.ForkChoiceHash, b.ForkChoiceHash) {
		return a
	}
	return b
}

func applyElementDelta(base, delta *pb.ReducedElement) *pb.ReducedElement {
	fcWinner := elementForkChoiceWinner(base, delta)
	result := &pb.ReducedElement{
		Data:           cloneData(base.Data),
		Hlc:            maxTime(base.Hlc, delta.Hlc),
		NodeId:         forkChoiceWinnerNodeID(base.ForkChoiceHash, base.NodeId, delta.ForkChoiceHash, delta.NodeId),
		ExpiresAt:      fcWinner.ExpiresAt,
		ForkChoiceHash: fcWinner.ForkChoiceHash,
	}

	switch delta.Data.Merge {
	case pb.MergeRule_ADDITIVE_INT:
		result.Data.Value = &pb.DataEffect_IntVal{IntVal: base.Data.GetIntVal() + delta.Data.GetIntVal()}
	case pb.MergeRule_ADDITIVE_FLOAT:
		result.Data.Value = &pb.DataEffect_FloatVal{FloatVal: base.Data.GetFloatVal() + delta.Data.GetFloatVal()}
	}

	return result
}

// --- Ordered merge helpers ---

func mergeOrderedElements(a, b *pb.ReducedEffect, commutative bool, winner *pb.ReducedEffect, hlc time.Time, nodeID uint64) *pb.ReducedEffect {
	// Determine element ordering: winner's chain first, loser's chain second
	var winnerElems, loserElems []*pb.ReducedElement
	var winnerRemoves, loserRemoves map[string]bool
	if winner == a {
		winnerElems, loserElems = a.OrderedElements, b.OrderedElements
		winnerRemoves, loserRemoves = a.NetRemoves, b.NetRemoves
	} else {
		winnerElems, loserElems = b.OrderedElements, a.OrderedElements
		winnerRemoves, loserRemoves = b.NetRemoves, a.NetRemoves
	}

	// Apply cross-branch removes: each branch's removes target elements
	// from before the fork that the other branch may still have.
	winnerElems = applyRemoves(winnerElems, loserRemoves)
	loserElems = applyRemoves(loserElems, winnerRemoves)

	// Winner's chain first, loser's chain second — both intact
	elements := make([]*pb.ReducedElement, 0, len(winnerElems)+len(loserElems))
	elements = append(elements, winnerElems...)
	elements = append(elements, loserElems...)

	// Merge NetRemoves from both branches
	netRemoves := make(map[string]bool)
	for k := range a.NetRemoves {
		netRemoves[k] = true
	}
	for k := range b.NetRemoves {
		netRemoves[k] = true
	}

	return &pb.ReducedEffect{
		Op:              pb.EffectOp_INSERT_OP,
		Merge:           a.Merge,
		Collection:      pb.CollectionKind_ORDERED,
		Hlc:             timestamppb.New(hlc),
		NodeId:          nodeID,
		Commutative:     commutative,
		ForkChoiceHash:  winner.ForkChoiceHash,
		OrderedElements: elements,
		NetRemoves:      netRemoves,
		TypeTag:         winner.TypeTag,
		ExpiresAt:       winner.ExpiresAt,
	}
}

func applyRemoves(elements []*pb.ReducedElement, removes map[string]bool) []*pb.ReducedElement {
	if len(removes) == 0 {
		return elements
	}
	result := make([]*pb.ReducedElement, 0, len(elements))
	for _, elem := range elements {
		if !removes[string(elem.Data.Id)] {
			result = append(result, elem)
		}
	}
	return result
}

// --- Incompatible merge ---

// mergeIncompatible handles cases where two branches cannot be combined
// (different collection kinds, cross-type, etc.) by picking the HLC winner
// and constructing a new ReducedEffect. The new object shares sub-objects
// (Scalar, NetAdds, etc.) by pointer — no deep copy.
func mergeIncompatible(a, b *pb.ReducedEffect) *pb.ReducedEffect {
	winner, _ := branchWinner(a, b)
	maxHLC, maxNodeID := maxTip(a, b)
	r := mergeIncompatibleCore(a, b, winner, maxHLC, maxNodeID)
	r.SerializationLeader = mergeSerializationLeader(a, b)
	return r
}

// mergeIncompatibleCore constructs a new ReducedEffect from the winner's
// data fields without setting metadata (subscribers, serialization leader).
// Used by helpers that set metadata themselves.
func mergeIncompatibleCore(_, _, winner *pb.ReducedEffect, hlc time.Time, nodeID uint64) *pb.ReducedEffect {
	return &pb.ReducedEffect{
		Op:              winner.Op,
		Merge:           winner.Merge,
		Collection:      winner.Collection,
		Hlc:             timestamppb.New(hlc),
		NodeId:          nodeID,
		Commutative:     winner.Commutative,
		Scalar:          winner.Scalar,
		NetAdds:         winner.NetAdds,
		NetRemoves:      winner.NetRemoves,
		OrderedElements: winner.OrderedElements,
		TypeTag:         winner.TypeTag,
		ExpiresAt:       winner.ExpiresAt,
		ForkChoiceHash:  winner.ForkChoiceHash,
	}
}

// --- Utility ---

func elementForkChoiceWinner(a, b *pb.ReducedElement) *pb.ReducedElement {
	if ForkChoiceLess(a.ForkChoiceHash, b.ForkChoiceHash) {
		return a
	}
	return b
}

func forkChoiceWinnerNodeID(hashA []byte, nodeA uint64, hashB []byte, nodeB uint64) uint64 {
	if ForkChoiceLess(hashA, hashB) {
		return nodeA
	}
	return nodeB
}

func maxTip(a, b *pb.ReducedEffect) (time.Time, uint64) {
	if a.Hlc.AsTime().After(b.Hlc.AsTime()) || (a.Hlc == b.Hlc && a.NodeId > b.NodeId) {
		return a.Hlc.AsTime(), a.NodeId
	}
	return b.Hlc.AsTime(), b.NodeId
}

func branchWinner(a, b *pb.ReducedEffect) (winner, loser *pb.ReducedEffect) {
	if ForkChoiceLess(a.ForkChoiceHash, b.ForkChoiceHash) {
		return a, b
	}
	return b, a
}

func mergeSerializationLeader(a, b *pb.ReducedEffect) *uint64 {
	if a.SerializationLeader == nil {
		return b.SerializationLeader
	}
	if b.SerializationLeader == nil {
		return a.SerializationLeader
	}
	// Both have a leader: FWW — lower fork_choice_hash wins
	if ForkChoiceLess(a.ForkChoiceHash, b.ForkChoiceHash) {
		return a.SerializationLeader
	}
	return b.SerializationLeader
}

// mergeSerializationLeaderN picks the serialization leader from N branches
// using FWW (lowest HLC wins).
func mergeSerializationLeaderN(branches []*pb.ReducedEffect) *uint64 {
	var leader *uint64
	var bestHash []byte
	first := true

	for _, b := range branches {
		if b.SerializationLeader == nil {
			continue
		}
		if first || ForkChoiceLess(b.ForkChoiceHash, bestHash) {
			leader = b.SerializationLeader
			bestHash = b.ForkChoiceHash
			first = false
		}
	}

	return leader
}
