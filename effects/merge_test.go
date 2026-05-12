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
	"testing"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// --- helpers for building ReducedEffects with proto values ---

func hlcTs(nanos int64) *timestamppb.Timestamp {
	return timestamppb.New(time.Unix(0, nanos))
}

func reducedScalarRaw(merge pb.MergeRule, commutative bool, raw []byte, hlcNanos int64, nodeID uint64) *pb.ReducedEffect {
	hlc := hlcTs(hlcNanos)
	return &pb.ReducedEffect{
		Commutative:    commutative,
		Merge:          merge,
		Collection:     pb.CollectionKind_SCALAR,
		Hlc:            hlc,
		NodeId:         nodeID,
		ForkChoiceHash: ComputeForkChoiceHash(pb.NodeID(nodeID), hlc),
		Scalar: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      merge,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: raw},
		},
	}
}

func reducedScalarInt(merge pb.MergeRule, commutative bool, v int64, hlcNanos int64, nodeID uint64) *pb.ReducedEffect {
	hlc := hlcTs(hlcNanos)
	return &pb.ReducedEffect{
		Commutative:    commutative,
		Merge:          merge,
		Collection:     pb.CollectionKind_SCALAR,
		Hlc:            hlc,
		NodeId:         nodeID,
		ForkChoiceHash: ComputeForkChoiceHash(pb.NodeID(nodeID), hlc),
		Scalar: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      merge,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_IntVal{IntVal: v},
		},
	}
}

func reducedScalarFloat(merge pb.MergeRule, commutative bool, v float64, hlcNanos int64, nodeID uint64) *pb.ReducedEffect {
	hlc := hlcTs(hlcNanos)
	return &pb.ReducedEffect{
		Commutative:    commutative,
		Merge:          merge,
		Collection:     pb.CollectionKind_SCALAR,
		Hlc:            hlc,
		NodeId:         nodeID,
		ForkChoiceHash: ComputeForkChoiceHash(pb.NodeID(nodeID), hlc),
		Scalar: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      merge,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_FloatVal{FloatVal: v},
		},
	}
}

func reducedKeyed(merge pb.MergeRule, commutative bool, hlcNanos int64, nodeID uint64, adds map[string]*pb.ReducedElement) *pb.ReducedEffect {
	hlc := hlcTs(hlcNanos)
	return &pb.ReducedEffect{
		Commutative:    commutative,
		Merge:          merge,
		Collection:     pb.CollectionKind_KEYED,
		Hlc:            hlc,
		NodeId:         nodeID,
		ForkChoiceHash: ComputeForkChoiceHash(pb.NodeID(nodeID), hlc),
		NetAdds:        adds,
		NetRemoves:     make(map[string]bool),
	}
}

func elemRaw(id string, merge pb.MergeRule, raw []byte, hlcNanos int64, nodeID uint64) *pb.ReducedElement {
	hlc := hlcTs(hlcNanos)
	return &pb.ReducedElement{
		Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      merge,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(id),
			Value:      &pb.DataEffect_Raw{Raw: raw},
		},
		Hlc:            hlc,
		NodeId:         nodeID,
		ForkChoiceHash: ComputeForkChoiceHash(pb.NodeID(nodeID), hlc),
	}
}

func elemFloat(id string, merge pb.MergeRule, v float64, hlcNanos int64, nodeID uint64) *pb.ReducedElement {
	hlc := hlcTs(hlcNanos)
	return &pb.ReducedElement{
		Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      merge,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(id),
			Value:      &pb.DataEffect_FloatVal{FloatVal: v},
		},
		Hlc:            hlc,
		NodeId:         nodeID,
		ForkChoiceHash: ComputeForkChoiceHash(pb.NodeID(nodeID), hlc),
	}
}

func elemInt(id string, merge pb.MergeRule, v int64, hlcNanos int64, nodeID uint64) *pb.ReducedElement {
	hlc := hlcTs(hlcNanos)
	return &pb.ReducedElement{
		Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      merge,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(id),
			Value:      &pb.DataEffect_IntVal{IntVal: v},
		},
		Hlc:            hlc,
		NodeId:         nodeID,
		ForkChoiceHash: ComputeForkChoiceHash(pb.NodeID(nodeID), hlc),
	}
}

// --- Merge2 nil handling ---

func TestMerge2_NilA(t *testing.T) {
	b := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 5, 1, 1)
	r := Merge2(nil, b)
	if r != b {
		t.Fatal("expected b when a is nil")
	}
}

func TestMerge2_NilB(t *testing.T) {
	a := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 5, 1, 1)
	r := Merge2(a, nil)
	if r != a {
		t.Fatal("expected a when b is nil")
	}
}

// --- Both commutative ---

func TestMerge2_BothCommutativeAdditiveInt(t *testing.T) {
	a := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 10, 1, 1)
	b := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 7, 2, 2)
	r := Merge2(a, b)
	if r.Scalar.GetIntVal() != 17 {
		t.Fatalf("expected 17, got %d", r.Scalar.GetIntVal())
	}
	if !r.Commutative {
		t.Fatal("result should be commutative")
	}
	if r.Hlc.GetNanos() != 2 {
		t.Fatalf("expected HLC nanos=2, got %v", r.Hlc)
	}
}

func TestMerge2_BothCommutativeAdditiveFloat(t *testing.T) {
	a := reducedScalarFloat(pb.MergeRule_ADDITIVE_FLOAT, true, 1.5, 1, 1)
	b := reducedScalarFloat(pb.MergeRule_ADDITIVE_FLOAT, true, 2.5, 2, 2)
	r := Merge2(a, b)
	if r.Scalar.GetFloatVal() != 4.0 {
		t.Fatalf("expected 4.0, got %f", r.Scalar.GetFloatVal())
	}
}

func TestMerge2_BothCommutativeMaxInt(t *testing.T) {
	a := reducedScalarInt(pb.MergeRule_MAX_INT, true, 3, 1, 1)
	b := reducedScalarInt(pb.MergeRule_MAX_INT, true, 7, 2, 2)
	r := Merge2(a, b)
	if r.Scalar.GetIntVal() != 7 {
		t.Fatalf("expected 7, got %d", r.Scalar.GetIntVal())
	}
}

func TestMerge2_BothCommutativeDifferentMergeRule(t *testing.T) {
	a := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 10, 1, 1)
	b := reducedScalarFloat(pb.MergeRule_ADDITIVE_FLOAT, true, 2.5, 2, 2)
	r := Merge2(a, b)
	// Different merge rules: lowest fork_choice_hash wins
	if ForkChoiceLess(a.ForkChoiceHash, b.ForkChoiceHash) {
		if r.Scalar.GetIntVal() != 10 {
			t.Fatalf("expected int winner value 10, got %d", r.Scalar.GetIntVal())
		}
	} else {
		if r.Scalar.GetFloatVal() != 2.5 {
			t.Fatalf("expected float winner value 2.5, got %f", r.Scalar.GetFloatVal())
		}
	}
}

// --- Both non-commutative ---

func TestMerge2_BothNonCommutativeScalar_HLCWins(t *testing.T) {
	a := reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("alice"), 5, 1)
	b := reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("bob"), 10, 2)
	r := Merge2(a, b)
	if string(r.Scalar.GetRaw()) != "bob" {
		t.Fatalf("expected 'bob' (higher HLC), got %q", r.Scalar.GetRaw())
	}
}

func TestMerge2_BothNonCommutativeScalar_NodeIDTiebreak(t *testing.T) {
	a := reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("alice"), 5, 2)
	b := reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("bob"), 5, 1)
	r := Merge2(a, b)
	// Winner is determined by lowest fork_choice_hash, not NodeID
	expectedWinner := "alice"
	if ForkChoiceLess(b.ForkChoiceHash, a.ForkChoiceHash) {
		expectedWinner = "bob"
	}
	if string(r.Scalar.GetRaw()) != expectedWinner {
		t.Fatalf("expected %q (lower hash), got %q", expectedWinner, r.Scalar.GetRaw())
	}
}

// --- Mixed: commutative + non-commutative ---

func TestMerge2_MixedSetPlusIncr(t *testing.T) {
	nonComm := reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("100"), 1, 1)
	comm := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 5, 2, 2)
	r := Merge2(nonComm, comm)
	if r.Scalar.GetIntVal() != 105 {
		t.Fatalf("expected 105 (SET 100 + INCR 5), got %d", r.Scalar.GetIntVal())
	}
	if r.Commutative {
		t.Fatal("mixed result should be non-commutative")
	}
}

func TestMerge2_MixedSetPlusIncrFloat(t *testing.T) {
	nonComm := reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("10.5"), 1, 1)
	comm := reducedScalarFloat(pb.MergeRule_ADDITIVE_FLOAT, true, 2.5, 2, 2)
	r := Merge2(nonComm, comm)
	if r.Scalar.GetFloatVal() != 13.0 {
		t.Fatalf("expected 13.0 (SET 10.5 + INCR 2.5), got %f", r.Scalar.GetFloatVal())
	}
}

func TestMerge2_MixedOrderDoesntMatter(t *testing.T) {
	comm := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 5, 2, 2)
	nonComm := reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("100"), 1, 1)
	r1 := Merge2(comm, nonComm)
	r2 := Merge2(nonComm, comm)
	if r1.Scalar.GetIntVal() != r2.Scalar.GetIntVal() {
		t.Fatalf("order should not matter: %d vs %d", r1.Scalar.GetIntVal(), r2.Scalar.GetIntVal())
	}
	if r1.Scalar.GetIntVal() != 105 {
		t.Fatalf("expected 105, got %d", r1.Scalar.GetIntVal())
	}
}

// --- Keyed merges ---

func TestMerge2_KeyedUnion(t *testing.T) {
	a := reducedKeyed(pb.MergeRule_LAST_WRITE_WINS, true, 1, 1, map[string]*pb.ReducedElement{
		"f1": elemRaw("f1", pb.MergeRule_LAST_WRITE_WINS, []byte("v1"), 1, 1),
	})
	b := reducedKeyed(pb.MergeRule_LAST_WRITE_WINS, true, 2, 2, map[string]*pb.ReducedElement{
		"f2": elemRaw("f2", pb.MergeRule_LAST_WRITE_WINS, []byte("v2"), 2, 2),
	})
	r := Merge2(a, b)
	if len(r.NetAdds) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(r.NetAdds))
	}
}

func TestMerge2_KeyedConflictHashWins(t *testing.T) {
	a := reducedKeyed(pb.MergeRule_LAST_WRITE_WINS, false, 1, 1, map[string]*pb.ReducedElement{
		"f1": elemRaw("f1", pb.MergeRule_LAST_WRITE_WINS, []byte("old"), 1, 1),
	})
	b := reducedKeyed(pb.MergeRule_LAST_WRITE_WINS, false, 2, 2, map[string]*pb.ReducedElement{
		"f1": elemRaw("f1", pb.MergeRule_LAST_WRITE_WINS, []byte("new"), 2, 2),
	})
	r := Merge2(a, b)
	// Element winner uses lowest fork_choice_hash
	aElemHash := ComputeForkChoiceHash(1, hlcTs(1))
	bElemHash := ComputeForkChoiceHash(2, hlcTs(2))
	expected := "new"
	if ForkChoiceLess(aElemHash, bElemHash) {
		expected = "old"
	}
	if string(r.NetAdds["f1"].Data.GetRaw()) != expected {
		t.Fatalf("expected %q (lower hash), got %q", expected, r.NetAdds["f1"].Data.GetRaw())
	}
}

func TestMerge2_KeyedAdditiveFloatElements(t *testing.T) {
	a := reducedKeyed(pb.MergeRule_ADDITIVE_FLOAT, true, 1, 1, map[string]*pb.ReducedElement{
		"m": elemFloat("m", pb.MergeRule_ADDITIVE_FLOAT, 1.5, 1, 1),
	})
	b := reducedKeyed(pb.MergeRule_ADDITIVE_FLOAT, true, 2, 2, map[string]*pb.ReducedElement{
		"m": elemFloat("m", pb.MergeRule_ADDITIVE_FLOAT, 2.5, 2, 2),
	})
	r := Merge2(a, b)
	if r.NetAdds["m"].Data.GetFloatVal() != 4.0 {
		t.Fatalf("expected 4.0, got %f", r.NetAdds["m"].Data.GetFloatVal())
	}
}

func TestMerge2_KeyedMixedElementMergeRules(t *testing.T) {
	a := reducedKeyed(pb.MergeRule_LAST_WRITE_WINS, false, 1, 1, map[string]*pb.ReducedElement{
		"m": elemFloat("m", pb.MergeRule_LAST_WRITE_WINS, 10.0, 1, 1),
	})
	b := reducedKeyed(pb.MergeRule_ADDITIVE_FLOAT, true, 2, 2, map[string]*pb.ReducedElement{
		"m": elemFloat("m", pb.MergeRule_ADDITIVE_FLOAT, 3.0, 2, 2),
	})
	r := Merge2(a, b)
	if r.NetAdds["m"].Data.GetFloatVal() != 13.0 {
		t.Fatalf("expected 13.0 (ZADD 10 + ZINCRBY 3), got %f", r.NetAdds["m"].Data.GetFloatVal())
	}
}

func TestMerge2_KeyedMaxIntElements(t *testing.T) {
	a := reducedKeyed(pb.MergeRule_MAX_INT, true, 1, 1, map[string]*pb.ReducedElement{
		"r0": elemInt("r0", pb.MergeRule_MAX_INT, 5, 1, 1),
		"r1": elemInt("r1", pb.MergeRule_MAX_INT, 3, 1, 1),
	})
	b := reducedKeyed(pb.MergeRule_MAX_INT, true, 2, 2, map[string]*pb.ReducedElement{
		"r0": elemInt("r0", pb.MergeRule_MAX_INT, 3, 2, 2),
		"r1": elemInt("r1", pb.MergeRule_MAX_INT, 7, 2, 2),
	})
	r := Merge2(a, b)
	if r.NetAdds["r0"].Data.GetIntVal() != 5 {
		t.Fatalf("expected r0=5, got %d", r.NetAdds["r0"].Data.GetIntVal())
	}
	if r.NetAdds["r1"].Data.GetIntVal() != 7 {
		t.Fatalf("expected r1=7, got %d", r.NetAdds["r1"].Data.GetIntVal())
	}
}

// --- Intent-based keyed conflict resolution ---

func reducedKeyedWithRemoves(merge pb.MergeRule, commutative bool, hlcNanos int64, nodeID uint64, adds map[string]*pb.ReducedElement, removes map[string]bool) *pb.ReducedEffect {
	hlc := hlcTs(hlcNanos)
	if adds == nil {
		adds = make(map[string]*pb.ReducedElement)
	}
	if removes == nil {
		removes = make(map[string]bool)
	}
	return &pb.ReducedEffect{
		Commutative:    commutative,
		Merge:          merge,
		Collection:     pb.CollectionKind_KEYED,
		Hlc:            hlc,
		NodeId:         nodeID,
		ForkChoiceHash: ComputeForkChoiceHash(pb.NodeID(nodeID), hlc),
		NetAdds:        adds,
		NetRemoves:     removes,
	}
}

// Member existed at fork. Branch A removes it, branch B re-adds it.
// REMOVE wins — the ADD was a confirmation, REMOVE had more info.
func TestMerge2_KeyedIntentRemoveWins_ARemovesBAdd(t *testing.T) {
	a := reducedKeyedWithRemoves(pb.MergeRule_LAST_WRITE_WINS, false, 1, 1,
		nil,
		map[string]bool{"x": true},
	)
	b := reducedKeyedWithRemoves(pb.MergeRule_LAST_WRITE_WINS, false, 2, 2,
		map[string]*pb.ReducedElement{
			"x": elemRaw("x", pb.MergeRule_LAST_WRITE_WINS, []byte("val"), 2, 2),
		},
		nil,
	)
	r := Merge2(a, b)
	if _, inAdds := r.NetAdds["x"]; inAdds {
		t.Fatal("expected 'x' NOT in NetAdds — REMOVE should win over ADD when member existed at fork")
	}
	if !r.NetRemoves["x"] {
		t.Fatal("expected 'x' in NetRemoves")
	}
}

// Symmetric: branch B removes, branch A re-adds. REMOVE still wins.
func TestMerge2_KeyedIntentRemoveWins_BRemovesAAdd(t *testing.T) {
	a := reducedKeyedWithRemoves(pb.MergeRule_LAST_WRITE_WINS, false, 2, 2,
		map[string]*pb.ReducedElement{
			"x": elemRaw("x", pb.MergeRule_LAST_WRITE_WINS, []byte("val"), 2, 2),
		},
		nil,
	)
	b := reducedKeyedWithRemoves(pb.MergeRule_LAST_WRITE_WINS, false, 1, 1,
		nil,
		map[string]bool{"x": true},
	)
	r := Merge2(a, b)
	if _, inAdds := r.NetAdds["x"]; inAdds {
		t.Fatal("expected 'x' NOT in NetAdds — REMOVE should win")
	}
	if !r.NetRemoves["x"] {
		t.Fatal("expected 'x' in NetRemoves")
	}
}

// Both branches add the same member (no removes). Merge elements normally.
func TestMerge2_KeyedIntentBothAdd(t *testing.T) {
	a := reducedKeyed(pb.MergeRule_LAST_WRITE_WINS, false, 1, 1, map[string]*pb.ReducedElement{
		"x": elemRaw("x", pb.MergeRule_LAST_WRITE_WINS, []byte("v1"), 1, 1),
	})
	b := reducedKeyed(pb.MergeRule_LAST_WRITE_WINS, false, 2, 2, map[string]*pb.ReducedElement{
		"x": elemRaw("x", pb.MergeRule_LAST_WRITE_WINS, []byte("v2"), 2, 2),
	})
	r := Merge2(a, b)
	if _, inAdds := r.NetAdds["x"]; !inAdds {
		t.Fatal("expected 'x' in NetAdds when both branches add")
	}
	// Lowest fork_choice_hash wins the element merge
	expected := "v1"
	if ForkChoiceLess(ComputeForkChoiceHash(2, hlcTs(2)), ComputeForkChoiceHash(1, hlcTs(1))) {
		expected = "v2"
	}
	if string(r.NetAdds["x"].Data.GetRaw()) != expected {
		t.Fatalf("expected %q (lower hash), got %q", expected, r.NetAdds["x"].Data.GetRaw())
	}
}

// Both branches remove the same member. Both agree.
func TestMerge2_KeyedIntentBothRemove(t *testing.T) {
	a := reducedKeyedWithRemoves(pb.MergeRule_LAST_WRITE_WINS, false, 1, 1,
		nil, map[string]bool{"x": true})
	b := reducedKeyedWithRemoves(pb.MergeRule_LAST_WRITE_WINS, false, 2, 2,
		nil, map[string]bool{"x": true})
	r := Merge2(a, b)
	if _, inAdds := r.NetAdds["x"]; inAdds {
		t.Fatal("expected 'x' NOT in NetAdds")
	}
	if !r.NetRemoves["x"] {
		t.Fatal("expected 'x' in NetRemoves")
	}
}

// Non-conflicting: a adds "x", b adds "y". Both survive.
func TestMerge2_KeyedIntentDisjointAdds(t *testing.T) {
	a := reducedKeyed(pb.MergeRule_LAST_WRITE_WINS, false, 1, 1, map[string]*pb.ReducedElement{
		"x": elemRaw("x", pb.MergeRule_LAST_WRITE_WINS, []byte("vx"), 1, 1),
	})
	b := reducedKeyed(pb.MergeRule_LAST_WRITE_WINS, false, 2, 2, map[string]*pb.ReducedElement{
		"y": elemRaw("y", pb.MergeRule_LAST_WRITE_WINS, []byte("vy"), 2, 2),
	})
	r := Merge2(a, b)
	if len(r.NetAdds) != 2 {
		t.Fatalf("expected 2 adds, got %d", len(r.NetAdds))
	}
}

// a removes "x", b removes "y", a adds "y", b adds "x".
// Both x and y existed at fork. Cross-removes should win.
func TestMerge2_KeyedIntentCrossRemoves(t *testing.T) {
	a := reducedKeyedWithRemoves(pb.MergeRule_LAST_WRITE_WINS, false, 1, 1,
		map[string]*pb.ReducedElement{
			"y": elemRaw("y", pb.MergeRule_LAST_WRITE_WINS, []byte("ya"), 1, 1),
		},
		map[string]bool{"x": true},
	)
	b := reducedKeyedWithRemoves(pb.MergeRule_LAST_WRITE_WINS, false, 2, 2,
		map[string]*pb.ReducedElement{
			"x": elemRaw("x", pb.MergeRule_LAST_WRITE_WINS, []byte("xb"), 2, 2),
		},
		map[string]bool{"y": true},
	)
	r := Merge2(a, b)
	// Both removes should win
	if _, ok := r.NetAdds["x"]; ok {
		t.Fatal("x should be removed (a removed it)")
	}
	if _, ok := r.NetAdds["y"]; ok {
		t.Fatal("y should be removed (b removed it)")
	}
	if !r.NetRemoves["x"] || !r.NetRemoves["y"] {
		t.Fatal("both x and y should be in NetRemoves")
	}
}

// Intent resolution with commutative (ZINCRBY) elements.
// Member existed at fork. One branch increments, other removes. REMOVE wins.
func TestMerge2_KeyedIntentRemoveWinsOverCommutativeAdd(t *testing.T) {
	a := reducedKeyedWithRemoves(pb.MergeRule_ADDITIVE_FLOAT, true, 1, 1,
		map[string]*pb.ReducedElement{
			"score": elemFloat("score", pb.MergeRule_ADDITIVE_FLOAT, 5.0, 1, 1),
		},
		nil,
	)
	b := reducedKeyedWithRemoves(pb.MergeRule_LAST_WRITE_WINS, false, 2, 2,
		nil,
		map[string]bool{"score": true},
	)
	r := Merge2(a, b)
	if _, ok := r.NetAdds["score"]; ok {
		t.Fatal("expected 'score' removed — REMOVE wins over commutative ADD")
	}
	if !r.NetRemoves["score"] {
		t.Fatal("expected 'score' in NetRemoves")
	}
}

// Merge is commutative — order of arguments shouldn't change the result.
func TestMerge2_KeyedIntentSymmetric(t *testing.T) {
	a := reducedKeyedWithRemoves(pb.MergeRule_LAST_WRITE_WINS, false, 1, 1,
		map[string]*pb.ReducedElement{
			"x": elemRaw("x", pb.MergeRule_LAST_WRITE_WINS, []byte("val"), 1, 1),
		},
		map[string]bool{"y": true},
	)
	b := reducedKeyedWithRemoves(pb.MergeRule_LAST_WRITE_WINS, false, 2, 2,
		map[string]*pb.ReducedElement{
			"y": elemRaw("y", pb.MergeRule_LAST_WRITE_WINS, []byte("val2"), 2, 2),
		},
		map[string]bool{"x": true},
	)
	r1 := Merge2(a, b)
	r2 := Merge2(b, a)

	// Both orderings should produce the same result
	if len(r1.NetAdds) != len(r2.NetAdds) {
		t.Fatalf("asymmetric NetAdds: %d vs %d", len(r1.NetAdds), len(r2.NetAdds))
	}
	if len(r1.NetRemoves) != len(r2.NetRemoves) {
		t.Fatalf("asymmetric NetRemoves: %d vs %d", len(r1.NetRemoves), len(r2.NetRemoves))
	}
	// Both x and y should be removed
	if len(r1.NetAdds) != 0 {
		t.Fatalf("expected 0 adds, got %d", len(r1.NetAdds))
	}
	if !r1.NetRemoves["x"] || !r1.NetRemoves["y"] {
		t.Fatal("both x and y should be in NetRemoves")
	}
}

// --- Ordered merges ---

func TestMerge2_OrderedWinnerFirst(t *testing.T) {
	hashA := ComputeForkChoiceHash(1, hlcTs(1))
	hashB := ComputeForkChoiceHash(2, hlcTs(2))
	a := &pb.ReducedEffect{
		Commutative:    true,
		Collection:     pb.CollectionKind_ORDERED,
		Hlc:            hlcTs(1),
		NodeId:         1,
		ForkChoiceHash: hashA,
		OrderedElements: []*pb.ReducedElement{
			{Data: &pb.DataEffect{Id: []byte("e1"), Value: &pb.DataEffect_Raw{Raw: []byte("a")}}, Hlc: hlcTs(1), NodeId: 1, ForkChoiceHash: hashA},
		},
	}
	b := &pb.ReducedEffect{
		Commutative:    true,
		Collection:     pb.CollectionKind_ORDERED,
		Hlc:            hlcTs(2),
		NodeId:         2,
		ForkChoiceHash: hashB,
		OrderedElements: []*pb.ReducedElement{
			{Data: &pb.DataEffect{Id: []byte("e2"), Value: &pb.DataEffect_Raw{Raw: []byte("b")}}, Hlc: hlcTs(2), NodeId: 2, ForkChoiceHash: hashB},
		},
	}
	r := Merge2(a, b)
	if len(r.OrderedElements) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(r.OrderedElements))
	}
	// Winner (lowest hash) elements come first
	first, second := "a", "b"
	if ForkChoiceLess(hashB, hashA) {
		first, second = "b", "a"
	}
	if string(r.OrderedElements[0].Data.GetRaw()) != first {
		t.Fatalf("expected winner first (%q), got %q", first, r.OrderedElements[0].Data.GetRaw())
	}
	if string(r.OrderedElements[1].Data.GetRaw()) != second {
		t.Fatalf("expected loser second (%q), got %q", second, r.OrderedElements[1].Data.GetRaw())
	}
}

func TestMerge2_OrderedCrossBranchRemove(t *testing.T) {
	hashA := ComputeForkChoiceHash(1, hlcTs(1))
	hashB := ComputeForkChoiceHash(2, hlcTs(2))
	a := &pb.ReducedEffect{
		Commutative:    true,
		Collection:     pb.CollectionKind_ORDERED,
		Hlc:            hlcTs(1),
		NodeId:         1,
		ForkChoiceHash: hashA,
		OrderedElements: []*pb.ReducedElement{
			{Data: &pb.DataEffect{Id: []byte("e1"), Value: &pb.DataEffect_Raw{Raw: []byte("a")}}, Hlc: hlcTs(1), NodeId: 1, ForkChoiceHash: hashA},
			{Data: &pb.DataEffect{Id: []byte("shared"), Value: &pb.DataEffect_Raw{Raw: []byte("x")}}, Hlc: hlcTs(1), NodeId: 1, ForkChoiceHash: hashA},
		},
	}
	b := &pb.ReducedEffect{
		Commutative:    true,
		Collection:     pb.CollectionKind_ORDERED,
		Hlc:            hlcTs(2),
		NodeId:         2,
		ForkChoiceHash: hashB,
		OrderedElements: []*pb.ReducedElement{
			{Data: &pb.DataEffect{Id: []byte("e2"), Value: &pb.DataEffect_Raw{Raw: []byte("b")}}, Hlc: hlcTs(2), NodeId: 2, ForkChoiceHash: hashB},
		},
		NetRemoves: map[string]bool{"shared": true},
	}
	r := Merge2(a, b)
	// b removes "shared" from a's elements
	if len(r.OrderedElements) != 2 {
		t.Fatalf("expected 2 elements (shared removed), got %d", len(r.OrderedElements))
	}
	// Winner (lowest hash) elements come first
	first, second := "a", "b"
	if ForkChoiceLess(hashB, hashA) {
		first, second = "b", "a"
	}
	if string(r.OrderedElements[0].Data.GetRaw()) != first {
		t.Fatalf("expected %q first, got %q", first, r.OrderedElements[0].Data.GetRaw())
	}
	if string(r.OrderedElements[1].Data.GetRaw()) != second {
		t.Fatalf("expected %q second, got %q", second, r.OrderedElements[1].Data.GetRaw())
	}
}

// --- Cross-collection ---

func TestMerge2_CrossCollectionHashWins(t *testing.T) {
	a := reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("string"), 1, 1)
	b := reducedKeyed(pb.MergeRule_LAST_WRITE_WINS, false, 2, 2, map[string]*pb.ReducedElement{
		"f1": elemRaw("f1", pb.MergeRule_LAST_WRITE_WINS, []byte("v1"), 2, 2),
	})
	r := Merge2(a, b)
	// Winner is determined by lowest fork_choice_hash
	if ForkChoiceLess(a.ForkChoiceHash, b.ForkChoiceHash) {
		if r.Collection != pb.CollectionKind_SCALAR {
			t.Fatalf("expected SCALAR (lower hash), got %v", r.Collection)
		}
	} else {
		if r.Collection != pb.CollectionKind_KEYED {
			t.Fatalf("expected KEYED (lower hash), got %v", r.Collection)
		}
	}
}

// --- Metadata propagation ---

func TestMerge2_TypeTagFromWinner(t *testing.T) {
	// Winner is determined by lowest fork_choice_hash
	a := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 5, 1, 1)
	a.TypeTag = pb.ValueType_TYPE_STRING
	b := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 3, 2, 2)
	b.TypeTag = pb.ValueType_TYPE_HASH
	r := Merge2(a, b)
	expected := pb.ValueType_TYPE_STRING
	if ForkChoiceLess(b.ForkChoiceHash, a.ForkChoiceHash) {
		expected = pb.ValueType_TYPE_HASH
	}
	if r.TypeTag != expected {
		t.Fatalf("expected %v (from winner), got %v", expected, r.TypeTag)
	}
}

func TestMerge2_TypeTagUnspecifiedIfWinnerHasNone(t *testing.T) {
	// Winner (lowest hash) has no type tag → result has TYPE_UNSPECIFIED
	a := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 5, 1, 1)
	b := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 3, 2, 2)
	// Set type tag only on the loser
	if ForkChoiceLess(a.ForkChoiceHash, b.ForkChoiceHash) {
		b.TypeTag = pb.ValueType_TYPE_STRING // b is loser
	} else {
		a.TypeTag = pb.ValueType_TYPE_STRING // a is loser
	}
	r := Merge2(a, b)
	if r.TypeTag != pb.ValueType_TYPE_UNSPECIFIED {
		t.Fatalf("expected TYPE_UNSPECIFIED (winner has none), got %v", r.TypeTag)
	}
}

func TestMerge2_ExpiryFromWinner(t *testing.T) {
	// Winner (lowest hash) expiry is used
	a := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 5, 1, 1)
	a.ExpiresAt = timestamppb.New(time.UnixMilli(2000))
	b := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 3, 2, 2)
	b.ExpiresAt = timestamppb.New(time.UnixMilli(500))
	r := Merge2(a, b)
	expectedExpiry := timestamppb.New(time.UnixMilli(2000))
	if ForkChoiceLess(b.ForkChoiceHash, a.ForkChoiceHash) {
		expectedExpiry = timestamppb.New(time.UnixMilli(500))
	}
	if r.ExpiresAt.GetSeconds() != expectedExpiry.GetSeconds() || r.ExpiresAt.GetNanos() != expectedExpiry.GetNanos() {
		t.Fatalf("expected ExpiresAt=%v (from winner), got %v", expectedExpiry, r.ExpiresAt)
	}
}

// --- Subscriber merge tests ---

func TestMerge2_SubscribersUnion(t *testing.T) {
	a := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 5, 1, 1)
	a.Subscribers = map[uint64]bool{10: true, 20: true}
	b := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 3, 2, 2)
	b.Subscribers = map[uint64]bool{20: true, 30: true}
	r := Merge2(a, b)
	if len(r.Subscribers) != 3 {
		t.Fatalf("expected 3 subscribers, got %d", len(r.Subscribers))
	}
	for _, id := range []uint64{10, 20, 30} {
		if _, ok := r.Subscribers[id]; !ok {
			t.Fatalf("expected node %d subscribed", id)
		}
	}
}

func TestMerge2_SubscribersNilHandling(t *testing.T) {
	a := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 5, 1, 1)
	a.Subscribers = map[uint64]bool{10: true}
	b := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 3, 2, 2)
	// b has no subscribers
	r := Merge2(a, b)
	if len(r.Subscribers) != 1 {
		t.Fatalf("expected 1 subscriber, got %d", len(r.Subscribers))
	}
	if _, ok := r.Subscribers[10]; !ok {
		t.Fatal("expected node 10 subscribed")
	}
}

func TestMerge2_BothNoSubscribers(t *testing.T) {
	a := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 5, 1, 1)
	b := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 3, 2, 2)
	r := Merge2(a, b)
	if r.Subscribers != nil {
		t.Fatalf("expected nil subscribers, got %v", r.Subscribers)
	}
}

// --- Serialization leader merge tests ---

func TestMerge2_SerializationBothNil(t *testing.T) {
	a := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 5, 1, 1)
	b := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 3, 2, 2)
	r := Merge2(a, b)
	if r.SerializationLeader != nil {
		t.Fatalf("expected nil leader, got %v", *r.SerializationLeader)
	}
}

func TestMerge2_SerializationOneNil(t *testing.T) {
	a := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 5, 1, 1)
	leader := uint64(7)
	a.SerializationLeader = &leader
	b := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 3, 2, 2)
	r := Merge2(a, b)
	if r.SerializationLeader == nil || *r.SerializationLeader != 7 {
		t.Fatalf("expected leader=7, got %v", r.SerializationLeader)
	}
}

func TestMerge2_SerializationFWW(t *testing.T) {
	// Lower fork_choice_hash wins FWW
	a := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 5, 1, 1)
	leaderA := uint64(10)
	a.SerializationLeader = &leaderA
	b := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 3, 2, 2)
	leaderB := uint64(20)
	b.SerializationLeader = &leaderB
	r := Merge2(a, b)
	expectedLeader := uint64(10)
	if ForkChoiceLess(b.ForkChoiceHash, a.ForkChoiceHash) {
		expectedLeader = 20
	}
	if r.SerializationLeader == nil || *r.SerializationLeader != expectedLeader {
		t.Fatalf("expected leader=%d (lower hash FWW), got %v", expectedLeader, r.SerializationLeader)
	}
}

func TestMerge2_SerializationFWW_TiebreakHash(t *testing.T) {
	a := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 5, 1, 1)
	leaderA := uint64(10)
	a.SerializationLeader = &leaderA
	b := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 5, 2, 2)
	leaderB := uint64(20)
	b.SerializationLeader = &leaderB
	r := Merge2(a, b)
	expectedLeader := uint64(10)
	if ForkChoiceLess(b.ForkChoiceHash, a.ForkChoiceHash) {
		expectedLeader = 20
	}
	if r.SerializationLeader == nil || *r.SerializationLeader != expectedLeader {
		t.Fatalf("expected leader=%d (lower hash), got %v", expectedLeader, r.SerializationLeader)
	}
}

// --- End-to-end: ReduceBranch + Merge2 ---

// --- MergeN tests ---

func TestMergeN_Nil(t *testing.T) {
	r := MergeN(nil)
	if r != nil {
		t.Fatal("expected nil")
	}
}

func TestMergeN_AllNils(t *testing.T) {
	r := MergeN([]*pb.ReducedEffect{nil, nil, nil})
	if r != nil {
		t.Fatal("expected nil")
	}
}

func TestMergeN_Single(t *testing.T) {
	a := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 5, 1, 1)
	r := MergeN([]*pb.ReducedEffect{a})
	if r != a {
		t.Fatal("expected same pointer for single branch")
	}
}

func TestMergeN_TwoBranchesMatchesMerge2(t *testing.T) {
	a := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 10, 1, 1)
	b := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 7, 2, 2)
	rN := MergeN([]*pb.ReducedEffect{a, b})
	// Re-create since Merge2 may mutate
	a2 := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 10, 1, 1)
	b2 := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 7, 2, 2)
	r2 := Merge2(a2, b2)
	if rN.Scalar.GetIntVal() != r2.Scalar.GetIntVal() {
		t.Fatalf("MergeN(2) != Merge2: %d vs %d", rN.Scalar.GetIntVal(), r2.Scalar.GetIntVal())
	}
}

func TestMergeN_ThreeCommutativeBranches(t *testing.T) {
	a := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 10, 1, 1)
	b := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 20, 2, 2)
	c := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 30, 3, 3)
	r := MergeN([]*pb.ReducedEffect{a, b, c})
	if r.Scalar.GetIntVal() != 60 {
		t.Fatalf("expected 60, got %d", r.Scalar.GetIntVal())
	}
}

func TestMergeN_NonCommWinsOverOtherNonComm(t *testing.T) {
	// Three non-commutative branches: lowest fork_choice_hash wins
	a := reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("alice"), 5, 1)
	b := reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("bob"), 10, 2)
	c := reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("charlie"), 3, 3)

	// Determine expected winner: lowest fork-choice hash
	branches := []*pb.ReducedEffect{a, b, c}
	names := []string{"alice", "bob", "charlie"}
	winner := 0
	for i := 1; i < len(branches); i++ {
		if ForkChoiceLess(branches[i].ForkChoiceHash, branches[winner].ForkChoiceHash) {
			winner = i
		}
	}

	r := MergeN(branches)
	if string(r.Scalar.GetRaw()) != names[winner] {
		t.Fatalf("expected %q (lowest fork-choice hash), got %q", names[winner], r.Scalar.GetRaw())
	}
}

func TestMergeN_NonCommPlusCommutatives(t *testing.T) {
	// One non-comm SET "100", two comm INCRs
	nonComm := reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("100"), 1, 1)
	comm1 := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 5, 2, 2)
	comm2 := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 3, 3, 3)
	r := MergeN([]*pb.ReducedEffect{nonComm, comm1, comm2})
	if r.Scalar.GetIntVal() != 108 {
		t.Fatalf("expected 108 (SET 100 + 5 + 3), got %d", r.Scalar.GetIntVal())
	}
}

func TestMergeN_MultipleNonCommPlusComm(t *testing.T) {
	// Two non-comm branches: lowest fork_choice_hash wins LWW
	// Plus one commutative branch that accumulates on top
	ncLow := reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("50"), 1, 1)
	ncHigh := reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("200"), 5, 2)
	comm := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 7, 3, 3)
	r := MergeN([]*pb.ReducedEffect{ncLow, ncHigh, comm})
	// Winner is the non-comm with lowest hash, then comm adds 7
	expectedBase := int64(200)
	if ForkChoiceLess(ncLow.ForkChoiceHash, ncHigh.ForkChoiceHash) {
		expectedBase = 50
	}
	expected := expectedBase + 7
	if r.Scalar.GetIntVal() != expected {
		t.Fatalf("expected %d (SET %d + 7), got %d", expected, expectedBase, r.Scalar.GetIntVal())
	}
}

// Determinism: any permutation of the same branches produces the same result
func TestMergeN_DeterministicPermutations(t *testing.T) {
	branches := []*pb.ReducedEffect{
		reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("100"), 1, 1),
		reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 5, 2, 2),
		reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 3, 3, 3),
		reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("200"), 4, 4),
	}

	// Compute reference result
	ref := MergeN(copyBranches(branches, []int{0, 1, 2, 3}))

	// All 24 permutations of 4 elements
	perms := [][]int{
		{0, 1, 2, 3}, {0, 1, 3, 2}, {0, 2, 1, 3}, {0, 2, 3, 1}, {0, 3, 1, 2}, {0, 3, 2, 1},
		{1, 0, 2, 3}, {1, 0, 3, 2}, {1, 2, 0, 3}, {1, 2, 3, 0}, {1, 3, 0, 2}, {1, 3, 2, 0},
		{2, 0, 1, 3}, {2, 0, 3, 1}, {2, 1, 0, 3}, {2, 1, 3, 0}, {2, 3, 0, 1}, {2, 3, 1, 0},
		{3, 0, 1, 2}, {3, 0, 2, 1}, {3, 1, 0, 2}, {3, 1, 2, 0}, {3, 2, 0, 1}, {3, 2, 1, 0},
	}

	for _, perm := range perms {
		r := MergeN(copyBranches(branches, perm))
		if r.Scalar.GetIntVal() != ref.Scalar.GetIntVal() {
			t.Fatalf("perm %v gave %d, expected %d", perm, r.Scalar.GetIntVal(), ref.Scalar.GetIntVal())
		}
	}
}

func TestMergeN_DeterministicKeyed(t *testing.T) {
	branches := []*pb.ReducedEffect{
		reducedKeyed(pb.MergeRule_LAST_WRITE_WINS, false, 1, 1, map[string]*pb.ReducedElement{
			"f1": elemRaw("f1", pb.MergeRule_LAST_WRITE_WINS, []byte("v1a"), 1, 1),
		}),
		reducedKeyed(pb.MergeRule_LAST_WRITE_WINS, false, 2, 2, map[string]*pb.ReducedElement{
			"f1": elemRaw("f1", pb.MergeRule_LAST_WRITE_WINS, []byte("v1b"), 2, 2),
			"f2": elemRaw("f2", pb.MergeRule_LAST_WRITE_WINS, []byte("v2"), 2, 2),
		}),
		reducedKeyed(pb.MergeRule_LAST_WRITE_WINS, false, 3, 3, map[string]*pb.ReducedElement{
			"f3": elemRaw("f3", pb.MergeRule_LAST_WRITE_WINS, []byte("v3"), 3, 3),
		}),
	}

	ref := MergeN(copyBranches(branches, []int{0, 1, 2}))

	perms := [][]int{
		{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0},
	}
	for _, perm := range perms {
		r := MergeN(copyBranches(branches, perm))
		if len(r.NetAdds) != len(ref.NetAdds) {
			t.Fatalf("perm %v: NetAdds len %d, expected %d", perm, len(r.NetAdds), len(ref.NetAdds))
		}
		for k, refElem := range ref.NetAdds {
			rElem, ok := r.NetAdds[k]
			if !ok {
				t.Fatalf("perm %v: missing key %q", perm, k)
			}
			if string(rElem.Data.GetRaw()) != string(refElem.Data.GetRaw()) {
				t.Fatalf("perm %v: key %q = %q, expected %q", perm, k, rElem.Data.GetRaw(), refElem.Data.GetRaw())
			}
		}
	}
}

func TestMergeN_SubscribersUnionAllBranches(t *testing.T) {
	a := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 5, 1, 1)
	a.Subscribers = map[uint64]bool{10: true}
	b := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 3, 2, 2)
	b.Subscribers = map[uint64]bool{20: true}
	c := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 7, 3, 3)
	c.Subscribers = map[uint64]bool{30: true}
	r := MergeN([]*pb.ReducedEffect{a, b, c})
	if len(r.Subscribers) != 3 {
		t.Fatalf("expected 3 subscribers, got %d", len(r.Subscribers))
	}
}

func TestMergeN_SerializationFromAllBranches(t *testing.T) {
	// Leader from branch with lowest fork_choice_hash wins (FWW)
	a := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 5, 3, 3)
	leaderA := uint64(30)
	a.SerializationLeader = &leaderA

	b := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 3, 1, 1)
	leaderB := uint64(10)
	b.SerializationLeader = &leaderB

	c := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 7, 2, 2)
	leaderC := uint64(20)
	c.SerializationLeader = &leaderC

	r := MergeN([]*pb.ReducedEffect{a, b, c})
	// Find the branch with lowest hash
	branches := []*pb.ReducedEffect{a, b, c}
	leaders := []uint64{30, 10, 20}
	bestIdx := 0
	for i := 1; i < len(branches); i++ {
		if ForkChoiceLess(branches[i].ForkChoiceHash, branches[bestIdx].ForkChoiceHash) {
			bestIdx = i
		}
	}
	expectedLeader := leaders[bestIdx]
	if r.SerializationLeader == nil || *r.SerializationLeader != expectedLeader {
		t.Fatalf("expected leader=%d (lowest hash FWW), got %v", expectedLeader, r.SerializationLeader)
	}
}

func TestMergeN_SkipsNils(t *testing.T) {
	a := reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 10, 1, 1)
	r := MergeN([]*pb.ReducedEffect{nil, a, nil})
	if r != a {
		t.Fatal("expected same pointer when others are nil")
	}
}

func TestMergeN_AllCommutativeDeterministic(t *testing.T) {
	branches := []*pb.ReducedEffect{
		reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 10, 1, 1),
		reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 20, 2, 2),
		reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 30, 3, 3),
		reducedScalarInt(pb.MergeRule_ADDITIVE_INT, true, 40, 4, 4),
	}

	perms := [][]int{
		{0, 1, 2, 3}, {3, 2, 1, 0}, {2, 0, 3, 1}, {1, 3, 0, 2},
	}
	for _, perm := range perms {
		r := MergeN(copyBranches(branches, perm))
		if r.Scalar.GetIntVal() != 100 {
			t.Fatalf("perm %v: expected 100, got %d", perm, r.Scalar.GetIntVal())
		}
	}
}

// copyBranches creates deep copies of branches in the given order
// so that merging doesn't mutate the originals for subsequent permutations.
func copyBranches(branches []*pb.ReducedEffect, order []int) []*pb.ReducedEffect {
	result := make([]*pb.ReducedEffect, len(order))
	for i, idx := range order {
		result[i] = proto.Clone(branches[idx]).(*pb.ReducedEffect)
	}
	return result
}

// --- Snapshot safety: merge must not mutate inputs ---

func TestMerge2_DoesNotMutateInputs_NonCommScalar(t *testing.T) {
	// Both non-commutative scalars → hlcWinner path (previously returned input directly)
	a := reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("alice"), 5, 1)
	b := reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("bob"), 10, 2)

	// Capture originals
	aSubs := a.Subscribers
	bSubs := b.Subscribers

	a.Subscribers = map[uint64]bool{10: true}
	b.Subscribers = map[uint64]bool{20: true}

	merged := Merge2(a, b)

	// merged must have union of subscribers
	if len(merged.Subscribers) != 2 {
		t.Fatalf("expected 2 subscribers in merged, got %d", len(merged.Subscribers))
	}

	// Inputs must be unmodified
	if len(a.Subscribers) != 1 || !a.Subscribers[10] {
		t.Fatalf("a.Subscribers was mutated: %v (was %v)", a.Subscribers, aSubs)
	}
	if len(b.Subscribers) != 1 || !b.Subscribers[20] {
		t.Fatalf("b.Subscribers was mutated: %v (was %v)", b.Subscribers, bSubs)
	}
}

func TestMerge2_DoesNotMutateInputs_CrossCollection(t *testing.T) {
	a := reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("string"), 1, 1)
	b := reducedKeyed(pb.MergeRule_LAST_WRITE_WINS, false, 2, 2, map[string]*pb.ReducedElement{
		"f1": elemRaw("f1", pb.MergeRule_LAST_WRITE_WINS, []byte("v1"), 2, 2),
	})

	leader := uint64(7)
	a.SerializationLeader = &leader

	merged := Merge2(a, b)

	// merged should have the leader from a
	if merged.SerializationLeader == nil || *merged.SerializationLeader != 7 {
		t.Fatalf("expected leader=7 in merged, got %v", merged.SerializationLeader)
	}

	// a must still have its original leader pointer
	if a.SerializationLeader == nil || *a.SerializationLeader != 7 {
		t.Fatal("a.SerializationLeader was mutated")
	}
}

func TestMergeN_DoesNotMutateInputs(t *testing.T) {
	// Three non-commutative branches, no commutative — the LWW winner
	// was previously mutated in-place by MergeN's subscriber union.
	a := reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("alice"), 5, 1)
	a.Subscribers = map[uint64]bool{10: true}
	b := reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("bob"), 10, 2)
	b.Subscribers = map[uint64]bool{20: true}
	c := reducedScalarRaw(pb.MergeRule_LAST_WRITE_WINS, false, []byte("charlie"), 3, 3)
	c.Subscribers = map[uint64]bool{30: true}

	merged := MergeN([]*pb.ReducedEffect{a, b, c})

	// merged should have all 3 subscribers
	if len(merged.Subscribers) != 3 {
		t.Fatalf("expected 3 subscribers in merged, got %d", len(merged.Subscribers))
	}

	// b is the LWW winner — it must NOT have been mutated
	if len(b.Subscribers) != 1 || !b.Subscribers[20] {
		t.Fatalf("b.Subscribers was mutated to %v, expected {20:true}", b.Subscribers)
	}
}

// --- Snapshot scoping: reductions must be from the fork point ---
//
// Scenario (t1-t5 from the issue):
//
//	t1: effect A (shared ancestor, HLC=1)
//	t2: effect B depends on A (HLC=2)
//	t3: reduce [A, B] = full reduction (WRONG for merge — includes A)
//	t4: effect C depends on A (HLC=3, concurrent with B)
//	t5: merge(reduce([A,B]), reduce([A,C])) double-counts A
//
// Correct approach: reduce only the fork-to-tip segment.
//
//	reduce([B]) and reduce([C]), then merge.
func TestReduceThenMerge_ForkPointScoping(t *testing.T) {
	// Shared ancestor: SET "100"
	effectA := makeDataEffect("k", 1, 1, scalarInsertRaw([]byte("100")))
	// Branch B: INCR +5 (depends on A)
	effectB := makeDataEffect("k", 2, 1, scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 5))
	// Branch C: INCR +3 (depends on A, concurrent with B)
	effectC := makeDataEffect("k", 3, 2, scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 3))

	// WRONG: reducing full branches including shared ancestor
	wrongA := ReduceBranch([]*pb.Effect{effectA, effectB})
	wrongC := ReduceBranch([]*pb.Effect{effectA, effectC})
	wrongMerged := Merge2(wrongA, wrongC)

	// This produces 105 + 103 merged somehow — the SET "100" is counted in both.
	// The wrong result depends on merge semantics, but it's NOT 108 (the correct answer).

	// CORRECT: reduce only the branch segments after the fork point (A)
	deltaB := ReduceBranch([]*pb.Effect{effectB})
	deltaC := ReduceBranch([]*pb.Effect{effectC})
	correctMerged := Merge2(deltaB, deltaC)

	// Both are pure ADDITIVE_INT deltas: 5 + 3 = 8
	if correctMerged.Scalar.GetIntVal() != 8 {
		t.Fatalf("expected delta merge = 8, got %d", correctMerged.Scalar.GetIntVal())
	}

	// The wrong merge should NOT equal 8 — demonstrating the bug when
	// reductions include the shared ancestor.
	if wrongMerged.Scalar.GetIntVal() == 8 {
		t.Fatal("wrong merge accidentally produced correct result — test is invalid")
	}

	// The caller applies the merged delta (8) on top of the fork state (SET "100")
	// to get the final result: 100 + 8 = 108.
	// This is the reconstruction layer's job (not merge's), but we verify the
	// delta is correct.
	_ = correctMerged
}

// --- End-to-end: ReduceBranch + Merge2 ---

func TestReduceThenMerge_ConcurrentIncr(t *testing.T) {
	branchA := []*pb.Effect{
		makeDataEffect("k", 1, 1, scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 3)),
		makeDataEffect("k", 2, 1, scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 2)),
	}
	branchB := []*pb.Effect{
		makeDataEffect("k", 1, 2, scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 10)),
	}

	rA := ReduceBranch(branchA)
	rB := ReduceBranch(branchB)
	merged := Merge2(rA, rB)

	if merged.Scalar.GetIntVal() != 15 {
		t.Fatalf("expected 15 (3+2+10), got %d", merged.Scalar.GetIntVal())
	}
}

func TestReduceThenMerge_ConcurrentStringSet(t *testing.T) {
	branchA := []*pb.Effect{
		makeDataEffect("k", 5, 1, scalarInsertRaw([]byte("alice"))),
	}
	branchB := []*pb.Effect{
		makeDataEffect("k", 10, 2, scalarInsertRaw([]byte("bob"))),
	}

	rA := ReduceBranch(branchA)
	rB := ReduceBranch(branchB)
	merged := Merge2(rA, rB)

	if string(merged.Scalar.GetRaw()) != "bob" {
		t.Fatalf("expected 'bob' (higher HLC), got %q", merged.Scalar.GetRaw())
	}
}

func TestReduceThenMerge_SetOnOneBranchIncrOnOther(t *testing.T) {
	branchA := []*pb.Effect{
		makeDataEffect("k", 5, 1, scalarInsertRaw([]byte("50"))),
	}
	branchB := []*pb.Effect{
		makeDataEffect("k", 3, 2, scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 7)),
	}

	rA := ReduceBranch(branchA)
	rB := ReduceBranch(branchB)
	merged := Merge2(rA, rB)

	if merged.Scalar.GetIntVal() != 57 {
		t.Fatalf("expected 57, got %d", merged.Scalar.GetIntVal())
	}
}

func TestReduceThenMerge_HashConcurrentFieldWrites(t *testing.T) {
	branchA := []*pb.Effect{
		makeDataEffect("k", 1, 1, keyedInsertRaw("f1", []byte("v1"))),
		makeDataEffect("k", 2, 1, keyedInsertRaw("f2", []byte("v2"))),
	}
	branchB := []*pb.Effect{
		makeDataEffect("k", 3, 2, keyedInsertRaw("f2", []byte("v2b"))),
		makeDataEffect("k", 4, 2, keyedInsertRaw("f3", []byte("v3"))),
	}

	rA := ReduceBranch(branchA)
	rB := ReduceBranch(branchB)
	merged := Merge2(rA, rB)

	if len(merged.NetAdds) != 3 {
		t.Fatalf("expected 3 fields, got %d", len(merged.NetAdds))
	}
	if string(merged.NetAdds["f2"].Data.GetRaw()) != "v2b" {
		t.Fatalf("expected f2=v2b, got %q", merged.NetAdds["f2"].Data.GetRaw())
	}
}
