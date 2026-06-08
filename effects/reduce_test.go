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
	"google.golang.org/protobuf/types/known/timestamppb"
)

// --- test helpers ---

func rTs(nanos int64) *timestamppb.Timestamp {
	return timestamppb.New(time.Unix(0, nanos))
}

func makeDataEffect(key string, hlcNanos int64, nodeID uint64, d *pb.DataEffect) *pb.Effect {
	return &pb.Effect{
		Key:    []byte(key),
		Hlc:    rTs(hlcNanos),
		NodeId: nodeID,
		Kind:   &pb.Effect_Data{Data: d},
	}
}

func makeMetaEffect(key string, hlcNanos int64, nodeID uint64, m *pb.MetaEffect) *pb.Effect {
	return &pb.Effect{
		Key:    []byte(key),
		Hlc:    rTs(hlcNanos),
		NodeId: nodeID,
		Kind:   &pb.Effect_Meta{Meta: m},
	}
}

func scalarInsertRaw(value []byte) *pb.DataEffect {
	return &pb.DataEffect{
		Op:         pb.EffectOp_INSERT_OP,
		Merge:      pb.MergeRule_LAST_WRITE_WINS,
		Collection: pb.CollectionKind_SCALAR,
		Value:      &pb.DataEffect_Raw{Raw: value},
	}
}

func scalarInsertInt(merge pb.MergeRule, v int64) *pb.DataEffect {
	return &pb.DataEffect{
		Op:         pb.EffectOp_INSERT_OP,
		Merge:      merge,
		Collection: pb.CollectionKind_SCALAR,
		Value:      &pb.DataEffect_IntVal{IntVal: v},
	}
}

func scalarInsertFloat(merge pb.MergeRule, v float64) *pb.DataEffect {
	return &pb.DataEffect{
		Op:         pb.EffectOp_INSERT_OP,
		Merge:      merge,
		Collection: pb.CollectionKind_SCALAR,
		Value:      &pb.DataEffect_FloatVal{FloatVal: v},
	}
}

func scalarRemove() *pb.DataEffect {
	return &pb.DataEffect{
		Op:         pb.EffectOp_REMOVE_OP,
		Collection: pb.CollectionKind_SCALAR,
	}
}

func keyedInsertRaw(id string, value []byte) *pb.DataEffect {
	return &pb.DataEffect{
		Op:         pb.EffectOp_INSERT_OP,
		Merge:      pb.MergeRule_LAST_WRITE_WINS,
		Collection: pb.CollectionKind_KEYED,
		Id:         []byte(id),
		Value:      &pb.DataEffect_Raw{Raw: value},
	}
}

func keyedInsertFloat(id string, merge pb.MergeRule, v float64) *pb.DataEffect {
	return &pb.DataEffect{
		Op:         pb.EffectOp_INSERT_OP,
		Merge:      merge,
		Collection: pb.CollectionKind_KEYED,
		Id:         []byte(id),
		Value:      &pb.DataEffect_FloatVal{FloatVal: v},
	}
}

func keyedInsertInt(id string, merge pb.MergeRule, v int64) *pb.DataEffect {
	return &pb.DataEffect{
		Op:         pb.EffectOp_INSERT_OP,
		Merge:      merge,
		Collection: pb.CollectionKind_KEYED,
		Id:         []byte(id),
		Value:      &pb.DataEffect_IntVal{IntVal: v},
	}
}

func keyedRemove(id string) *pb.DataEffect {
	return &pb.DataEffect{
		Op:         pb.EffectOp_REMOVE_OP,
		Merge:      pb.MergeRule_LAST_WRITE_WINS,
		Collection: pb.CollectionKind_KEYED,
		Id:         []byte(id),
	}
}

func orderedInsert(placement pb.Placement, id string, value []byte) *pb.DataEffect {
	return &pb.DataEffect{
		Op:         pb.EffectOp_INSERT_OP,
		Merge:      pb.MergeRule_LAST_WRITE_WINS,
		Collection: pb.CollectionKind_ORDERED,
		Placement:  placement,
		Id:         []byte(id),
		Value:      &pb.DataEffect_Raw{Raw: value},
	}
}

func orderedInsertRef(placement pb.Placement, id string, value []byte, ref []byte) *pb.DataEffect {
	return &pb.DataEffect{
		Op:         pb.EffectOp_INSERT_OP,
		Merge:      pb.MergeRule_LAST_WRITE_WINS,
		Collection: pb.CollectionKind_ORDERED,
		Placement:  placement,
		Id:         []byte(id),
		Value:      &pb.DataEffect_Raw{Raw: value},
		Reference:  ref,
	}
}

func orderedRemove(id string) *pb.DataEffect {
	return &pb.DataEffect{
		Op:         pb.EffectOp_REMOVE_OP,
		Collection: pb.CollectionKind_ORDERED,
		Placement:  pb.Placement_PLACE_SELF,
		Id:         []byte(id),
	}
}

// --- ReduceBranch tests ---

func TestReduceBranch_Empty(t *testing.T) {
	r := ReduceBranch(nil)
	if r != nil {
		t.Fatal("expected nil for empty branch")
	}
}

func TestReduceBranch_SingleStringSet(t *testing.T) {
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, scalarInsertRaw([]byte("hello"))),
	}
	r := ReduceBranch(effects)
	if r == nil {
		t.Fatal("expected non-nil")
	}
	if string(r.Scalar.GetRaw()) != "hello" {
		t.Fatalf("expected 'hello', got %q", r.Scalar.GetRaw())
	}
	if r.Commutative {
		t.Fatal("LWW should not be commutative")
	}
	if r.Collection != pb.CollectionKind_SCALAR {
		t.Fatalf("expected SCALAR, got %v", r.Collection)
	}
}

func TestReduceBranch_StringOverwrite(t *testing.T) {
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, scalarInsertRaw([]byte("first"))),
		makeDataEffect("k", 2, 1, scalarInsertRaw([]byte("second"))),
	}
	r := ReduceBranch(effects)
	if string(r.Scalar.GetRaw()) != "second" {
		t.Fatalf("expected 'second', got %q", r.Scalar.GetRaw())
	}
}

func TestReduceBranch_IncrDeltas(t *testing.T) {
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 5)),
		makeDataEffect("k", 2, 1, scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 3)),
		makeDataEffect("k", 3, 1, scalarInsertInt(pb.MergeRule_ADDITIVE_INT, -1)),
	}
	r := ReduceBranch(effects)
	if r.Scalar.GetIntVal() != 7 {
		t.Fatalf("expected 7, got %d", r.Scalar.GetIntVal())
	}
	if !r.Commutative {
		t.Fatal("ADDITIVE_INT should be commutative")
	}
}

func TestReduceBranch_FloatDeltas(t *testing.T) {
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, scalarInsertFloat(pb.MergeRule_ADDITIVE_FLOAT, 1.5)),
		makeDataEffect("k", 2, 1, scalarInsertFloat(pb.MergeRule_ADDITIVE_FLOAT, 2.5)),
	}
	r := ReduceBranch(effects)
	if r.Scalar.GetFloatVal() != 4.0 {
		t.Fatalf("expected 4.0, got %f", r.Scalar.GetFloatVal())
	}
}

func TestReduceBranch_SetThenIncr(t *testing.T) {
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, scalarInsertRaw([]byte("100"))),
		makeDataEffect("k", 2, 1, scalarInsertInt(pb.MergeRule_ADDITIVE_INT, 5)),
	}
	r := ReduceBranch(effects)
	if r.Scalar.GetIntVal() != 105 {
		t.Fatalf("expected 105, got %d", r.Scalar.GetIntVal())
	}
	if r.Commutative {
		t.Fatal("SET+INCR should be non-commutative")
	}
}

func TestReduceBranch_DelTruncates(t *testing.T) {
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, scalarInsertRaw([]byte("old"))),
		makeDataEffect("k", 2, 1, scalarRemove()),
		makeDataEffect("k", 3, 1, scalarInsertRaw([]byte("new"))),
	}
	r := ReduceBranch(effects)
	if string(r.Scalar.GetRaw()) != "new" {
		t.Fatalf("expected 'new' after DEL+SET, got %q", r.Scalar.GetRaw())
	}
}

func TestReduceBranch_DelIsLast(t *testing.T) {
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, scalarInsertRaw([]byte("value"))),
		makeDataEffect("k", 2, 1, scalarRemove()),
	}
	r := ReduceBranch(effects)
	if r.Op != pb.EffectOp_REMOVE_OP {
		t.Fatalf("expected REMOVE_OP, got %v", r.Op)
	}
}

func TestReduceBranch_MetaEffectCapturesTypeTag(t *testing.T) {
	effects := []*pb.Effect{
		makeMetaEffect("k", 1, 1, &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_HASH}),
		makeDataEffect("k", 2, 1, keyedInsertRaw("field1", []byte("val1"))),
	}
	r := ReduceBranch(effects)
	if r.TypeTag != pb.ValueType_TYPE_HASH {
		t.Fatalf("expected TYPE_HASH, got %v", r.TypeTag)
	}
	if _, ok := r.NetAdds["field1"]; !ok {
		t.Fatal("expected field1 in NetAdds")
	}
}

func TestReduceBranch_MetaEffectCapturesTTL(t *testing.T) {
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, scalarInsertRaw([]byte("val"))),
		makeMetaEffect("k", 2, 1, &pb.MetaEffect{ExpiresAt: timestamppb.New(time.UnixMilli(1000))}),
	}
	r := ReduceBranch(effects)
	if r.ExpiresAt.AsTime().UnixMilli() != 1000 {
		t.Fatalf("expected ExpiresAt=1000ms, got %v", r.ExpiresAt)
	}
}

// --- Keyed reduction tests ---

func TestReduceBranch_HashFields(t *testing.T) {
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, keyedInsertRaw("f1", []byte("v1"))),
		makeDataEffect("k", 2, 1, keyedInsertRaw("f2", []byte("v2"))),
		makeDataEffect("k", 3, 1, keyedInsertRaw("f1", []byte("v1-updated"))),
	}
	r := ReduceBranch(effects)
	if len(r.NetAdds) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(r.NetAdds))
	}
	if string(r.NetAdds["f1"].Data.GetRaw()) != "v1-updated" {
		t.Fatalf("expected f1=v1-updated, got %q", r.NetAdds["f1"].Data.GetRaw())
	}
	if string(r.NetAdds["f2"].Data.GetRaw()) != "v2" {
		t.Fatalf("expected f2=v2, got %q", r.NetAdds["f2"].Data.GetRaw())
	}
}

func TestReduceBranch_HashFieldRemove(t *testing.T) {
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, keyedInsertRaw("f1", []byte("v1"))),
		makeDataEffect("k", 2, 1, keyedInsertRaw("f2", []byte("v2"))),
		makeDataEffect("k", 3, 1, keyedRemove("f1")),
	}
	r := ReduceBranch(effects)
	if len(r.NetAdds) != 1 {
		t.Fatalf("expected 1 field after remove, got %d", len(r.NetAdds))
	}
	if _, ok := r.NetAdds["f1"]; ok {
		t.Fatal("f1 should have been removed")
	}
}

func TestReduceBranch_SetMembers(t *testing.T) {
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, keyedInsertRaw("m1", nil)),
		makeDataEffect("k", 2, 1, keyedInsertRaw("m2", nil)),
		makeDataEffect("k", 3, 1, keyedRemove("m1")),
		makeDataEffect("k", 4, 1, keyedInsertRaw("m3", nil)),
	}
	r := ReduceBranch(effects)
	if len(r.NetAdds) != 2 {
		t.Fatalf("expected 2 members, got %d", len(r.NetAdds))
	}
	if _, ok := r.NetAdds["m2"]; !ok {
		t.Fatal("expected m2")
	}
	if _, ok := r.NetAdds["m3"]; !ok {
		t.Fatal("expected m3")
	}
}

func TestReduceBranch_ZSetIncrBy(t *testing.T) {
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, keyedInsertFloat("member", pb.MergeRule_ADDITIVE_FLOAT, 1.5)),
		makeDataEffect("k", 2, 1, keyedInsertFloat("member", pb.MergeRule_ADDITIVE_FLOAT, 2.5)),
	}
	r := ReduceBranch(effects)
	elem := r.NetAdds["member"]
	if elem == nil {
		t.Fatal("expected member in NetAdds")
	}
	if elem.Data.GetFloatVal() != 4.0 {
		t.Fatalf("expected 4.0, got %f", elem.Data.GetFloatVal())
	}
}

func TestReduceBranch_HLLMaxInt(t *testing.T) {
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, keyedInsertInt("reg0", pb.MergeRule_MAX_INT, 3)),
		makeDataEffect("k", 2, 1, keyedInsertInt("reg0", pb.MergeRule_MAX_INT, 7)),
		makeDataEffect("k", 3, 1, keyedInsertInt("reg0", pb.MergeRule_MAX_INT, 5)),
	}
	r := ReduceBranch(effects)
	elem := r.NetAdds["reg0"]
	if elem == nil {
		t.Fatal("expected reg0 in NetAdds")
	}
	if elem.Data.GetIntVal() != 7 {
		t.Fatalf("expected 7, got %d", elem.Data.GetIntVal())
	}
}

func TestReduceBranch_RemoveUnknownKeyed(t *testing.T) {
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, keyedRemove("ghost")),
	}
	r := ReduceBranch(effects)
	if !r.NetRemoves["ghost"] {
		t.Fatal("expected ghost in NetRemoves")
	}
}

// --- Ordered reduction tests ---

func TestReduceBranch_ListHeadPush(t *testing.T) {
	// LPUSH a, LPUSH b, LPUSH c → [c, b, a]
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, orderedInsert(pb.Placement_PLACE_HEAD, "e1", []byte("a"))),
		makeDataEffect("k", 2, 1, orderedInsert(pb.Placement_PLACE_HEAD, "e2", []byte("b"))),
		makeDataEffect("k", 3, 1, orderedInsert(pb.Placement_PLACE_HEAD, "e3", []byte("c"))),
	}
	r := ReduceBranch(effects)
	if len(r.OrderedElements) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(r.OrderedElements))
	}
	if r.Collection != pb.CollectionKind_ORDERED {
		t.Fatalf("expected ORDERED, got %v", r.Collection)
	}
	// HEAD pushes: each goes to front, so order is c, b, a
	got := make([]string, len(r.OrderedElements))
	for i, elem := range r.OrderedElements {
		got[i] = string(elem.Data.GetRaw())
	}
	want := []string{"c", "b", "a"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
}

func TestReduceBranch_ListTailPush(t *testing.T) {
	// RPUSH a, RPUSH b, RPUSH c → [a, b, c]
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, orderedInsert(pb.Placement_PLACE_TAIL, "e1", []byte("a"))),
		makeDataEffect("k", 2, 1, orderedInsert(pb.Placement_PLACE_TAIL, "e2", []byte("b"))),
		makeDataEffect("k", 3, 1, orderedInsert(pb.Placement_PLACE_TAIL, "e3", []byte("c"))),
	}
	r := ReduceBranch(effects)
	if len(r.OrderedElements) != 3 {
		t.Fatalf("expected 3 elements, got %d", len(r.OrderedElements))
	}
	got := make([]string, len(r.OrderedElements))
	for i, elem := range r.OrderedElements {
		got[i] = string(elem.Data.GetRaw())
	}
	want := []string{"a", "b", "c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
}

func TestReduceBranch_ListMixedHeadTail(t *testing.T) {
	// LPUSH a, RPUSH b, LPUSH c → [c, a, b]
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, orderedInsert(pb.Placement_PLACE_HEAD, "e1", []byte("a"))),
		makeDataEffect("k", 2, 1, orderedInsert(pb.Placement_PLACE_TAIL, "e2", []byte("b"))),
		makeDataEffect("k", 3, 1, orderedInsert(pb.Placement_PLACE_HEAD, "e3", []byte("c"))),
	}
	r := ReduceBranch(effects)
	got := make([]string, len(r.OrderedElements))
	for i, elem := range r.OrderedElements {
		got[i] = string(elem.Data.GetRaw())
	}
	want := []string{"c", "a", "b"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
}

func TestReduceBranch_ListInsertAfter(t *testing.T) {
	// RPUSH a, RPUSH b, LINSERT AFTER a x → [a, x, b]
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, orderedInsert(pb.Placement_PLACE_TAIL, "e1", []byte("a"))),
		makeDataEffect("k", 2, 1, orderedInsert(pb.Placement_PLACE_TAIL, "e2", []byte("b"))),
		makeDataEffect("k", 3, 1, orderedInsertRef(pb.Placement_PLACE_AFTER, "e3", []byte("x"), []byte("e1"))),
	}
	r := ReduceBranch(effects)
	got := make([]string, len(r.OrderedElements))
	for i, elem := range r.OrderedElements {
		got[i] = string(elem.Data.GetRaw())
	}
	want := []string{"a", "x", "b"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
}

func TestReduceBranch_ListInsertBefore(t *testing.T) {
	// RPUSH a, RPUSH b, LINSERT BEFORE b x → [a, x, b]
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, orderedInsert(pb.Placement_PLACE_TAIL, "e1", []byte("a"))),
		makeDataEffect("k", 2, 1, orderedInsert(pb.Placement_PLACE_TAIL, "e2", []byte("b"))),
		makeDataEffect("k", 3, 1, orderedInsertRef(pb.Placement_PLACE_BEFORE, "e3", []byte("x"), []byte("e2"))),
	}
	r := ReduceBranch(effects)
	got := make([]string, len(r.OrderedElements))
	for i, elem := range r.OrderedElements {
		got[i] = string(elem.Data.GetRaw())
	}
	want := []string{"a", "x", "b"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
}

func TestReduceBranch_ListRemove(t *testing.T) {
	// RPUSH a, RPUSH b, RPUSH c, LREM b → [a, c]
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, orderedInsert(pb.Placement_PLACE_TAIL, "e1", []byte("a"))),
		makeDataEffect("k", 2, 1, orderedInsert(pb.Placement_PLACE_TAIL, "e2", []byte("b"))),
		makeDataEffect("k", 3, 1, orderedInsert(pb.Placement_PLACE_TAIL, "e3", []byte("c"))),
		makeDataEffect("k", 4, 1, orderedRemove("e2")),
	}
	r := ReduceBranch(effects)
	if len(r.OrderedElements) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(r.OrderedElements))
	}
	got := make([]string, len(r.OrderedElements))
	for i, elem := range r.OrderedElements {
		got[i] = string(elem.Data.GetRaw())
	}
	want := []string{"a", "c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
}

func TestReduceBranch_ListRemoveUnknown(t *testing.T) {
	// Remove an element from before the fork — goes into NetRemoves
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, orderedInsert(pb.Placement_PLACE_TAIL, "e1", []byte("a"))),
		makeDataEffect("k", 2, 1, orderedRemove("pre-fork-elem")),
	}
	r := ReduceBranch(effects)
	if len(r.OrderedElements) != 1 {
		t.Fatalf("expected 1 element, got %d", len(r.OrderedElements))
	}
	if !r.NetRemoves["pre-fork-elem"] {
		t.Fatal("expected pre-fork-elem in NetRemoves")
	}
}

func TestReduceBranch_ListSelfUpdate(t *testing.T) {
	// RPUSH a, RPUSH b, LSET e1 "a-updated" → [a-updated, b]
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, orderedInsert(pb.Placement_PLACE_TAIL, "e1", []byte("a"))),
		makeDataEffect("k", 2, 1, orderedInsert(pb.Placement_PLACE_TAIL, "e2", []byte("b"))),
		makeDataEffect("k", 3, 1, orderedInsert(pb.Placement_PLACE_SELF, "e1", []byte("a-updated"))),
	}
	r := ReduceBranch(effects)
	if len(r.OrderedElements) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(r.OrderedElements))
	}
	if string(r.OrderedElements[0].Data.GetRaw()) != "a-updated" {
		t.Fatalf("expected 'a-updated', got %q", r.OrderedElements[0].Data.GetRaw())
	}
}

// --- Bind/subscription effects are skipped ---

func TestReduceBranch_SkipsNonDataEffects(t *testing.T) {
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, scalarInsertRaw([]byte("val"))),
		{Key: []byte("k"), Hlc: rTs(2), NodeId: 1, Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}}},
		{Key: []byte("k"), Hlc: rTs(3), NodeId: 1, Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{}}},
	}
	r := ReduceBranch(effects)
	if string(r.Scalar.GetRaw()) != "val" {
		t.Fatalf("expected 'val', got %q", r.Scalar.GetRaw())
	}
}

// --- Subscription reduction tests ---

// --- Serialization reduction tests ---

func TestReduceBranch_SerializationRequest(t *testing.T) {
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, scalarInsertRaw([]byte("val"))),
		{Key: []byte("k"), Hlc: rTs(2), NodeId: 1, Kind: &pb.Effect_Serialization{Serialization: &pb.SerializationEffect{LeaderNodeId: 5}}},
	}
	r := ReduceBranch(effects)
	if r.SerializationLeader == nil || *r.SerializationLeader != 5 {
		t.Fatalf("expected leader=5, got %v", r.SerializationLeader)
	}
}

func TestReduceBranch_SerializationRelease(t *testing.T) {
	effects := []*pb.Effect{
		{Key: []byte("k"), Hlc: rTs(1), NodeId: 1, Kind: &pb.Effect_Serialization{Serialization: &pb.SerializationEffect{LeaderNodeId: 5}}},
		{Key: []byte("k"), Hlc: rTs(2), NodeId: 1, Kind: &pb.Effect_Serialization{Serialization: &pb.SerializationEffect{LeaderNodeId: 5, Release: true}}},
	}
	r := ReduceBranch(effects)
	if r.SerializationLeader != nil {
		t.Fatalf("expected nil leader after release, got %v", *r.SerializationLeader)
	}
}

func TestReduceBranch_SerializationOnly(t *testing.T) {
	effects := []*pb.Effect{
		{Key: []byte("k"), Hlc: rTs(1), NodeId: 1, Kind: &pb.Effect_Serialization{Serialization: &pb.SerializationEffect{LeaderNodeId: 3}}},
	}
	r := ReduceBranch(effects)
	if r == nil {
		t.Fatal("expected non-nil for serialization-only branch")
	}
	if r.SerializationLeader == nil || *r.SerializationLeader != 3 {
		t.Fatalf("expected leader=3, got %v", r.SerializationLeader)
	}
}

func TestReduceBranch_MaxInt(t *testing.T) {
	effects := []*pb.Effect{
		makeDataEffect("k", 1, 1, scalarInsertInt(pb.MergeRule_MAX_INT, 3)),
		makeDataEffect("k", 2, 1, scalarInsertInt(pb.MergeRule_MAX_INT, 7)),
		makeDataEffect("k", 3, 1, scalarInsertInt(pb.MergeRule_MAX_INT, 5)),
	}
	r := ReduceBranch(effects)
	if r.Scalar.GetIntVal() != 7 {
		t.Fatalf("expected 7, got %d", r.Scalar.GetIntVal())
	}
}
