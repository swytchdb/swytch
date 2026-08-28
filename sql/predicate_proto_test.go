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

package sql

import (
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"google.golang.org/protobuf/proto"
)

// roundtripPredicate marshals a predicate through proto (including
// the wire encoding) and returns the reconstructed Go value. Any
// marshalling failure aborts the test.
func roundtripPredicate(t *testing.T, p Predicate) Predicate {
	t.Helper()
	onWire, err := proto.Marshal(predicateToProto(p))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var reloaded pb.Predicate
	if err := proto.Unmarshal(onWire, &reloaded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return predicateFromProto(&reloaded)
}

// assertSameEval runs Evaluate(original, row) and Evaluate(round-
// tripped, row) over a set of rows; the results must match. This is
// the strongest roundtrip assertion — if evaluation outcomes match,
// the wire representation preserves semantics even if the Go struct
// layouts differ at the margins.
func assertSameEval(t *testing.T, original Predicate, rows []*RowWrite) {
	t.Helper()
	roundtripped := roundtripPredicate(t, original)
	for i, row := range rows {
		want := Evaluate(&original, row)
		got := Evaluate(&roundtripped, row)
		if want != got {
			t.Errorf("row %d: original=%v roundtrip=%v", i, want, got)
		}
	}
}

func TestProtoRoundtripBoolConstants(t *testing.T) {
	rows := []*RowWrite{rowOf(IntVal(1))}
	assertSameEval(t, PTrue(), rows)
	assertSameEval(t, PFalse(), rows)
}

func TestProtoRoundtripCmp(t *testing.T) {
	rows := []*RowWrite{
		rowOf(IntVal(5)),
		rowOf(IntVal(10)),
		rowOf(IntVal(-3)),
		rowOf(NullVal()),
	}
	for _, op := range []CmpOp{OpEq, OpLt, OpLe, OpGt, OpGe} {
		pred := PCmp(0, op, IntVal(5))
		assertSameEval(t, pred, rows)
	}
}

func TestProtoRoundtripTypedValues(t *testing.T) {
	cases := []TypedValue{
		IntVal(0), IntVal(42), IntVal(-999),
		FloatVal(0), FloatVal(3.14), FloatVal(-1e10),
		TextVal(""), TextVal("hello"), TextVal("utf-8 ☃"),
		BlobVal([]byte{0, 1, 2, 255}),
		NullVal(),
	}
	for _, v := range cases {
		pb := typedValueToProto(v)
		bytes, err := proto.Marshal(pb)
		if err != nil {
			t.Fatalf("marshal %v: %v", v, err)
		}
		var back TypedValue
		var decoded = pb.ProtoReflect().New().Interface()
		if err := proto.Unmarshal(bytes, decoded); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		back = typedValueFromProto(decoded.(*typedValueMessage))
		if back.Kind != v.Kind {
			t.Errorf("kind drift: %v → %v", v.Kind, back.Kind)
		}
	}
}

// typedValueMessage is an alias for *pb.TypedValue so the test can
// do a typed cast from the proto.Message interface.
type typedValueMessage = pb.TypedValue

func TestProtoRoundtripAndOrNot(t *testing.T) {
	rows := []*RowWrite{
		rowOf(IntVal(5), TextVal("alice")),
		rowOf(IntVal(99), TextVal("bob")),
		rowOf(IntVal(5), TextVal("bob")),
		rowOf(NullVal(), TextVal("alice")),
	}
	pred := POr(
		PAnd(
			PCmp(0, OpEq, IntVal(5)),
			PCmp(1, OpEq, TextVal("alice")),
		),
		PNot(PCmp(1, OpEq, TextVal("bob"))),
	)
	assertSameEval(t, pred, rows)
}

func TestProtoRoundtripIsNull(t *testing.T) {
	rows := []*RowWrite{
		rowOf(IntVal(0)),
		rowOf(NullVal()),
	}
	assertSameEval(t, PIsNull(0), rows)
	assertSameEval(t, PNot(PIsNull(0)), rows)
}

func TestProtoRoundtripColCmp(t *testing.T) {
	rows := []*RowWrite{
		rowOf(IntVal(1), IntVal(2)),
		rowOf(IntVal(5), IntVal(5)),
		rowOf(IntVal(10), IntVal(3)),
	}
	assertSameEval(t, PColCmp(0, OpLt, 1), rows)
	assertSameEval(t, PColCmp(0, OpEq, 1), rows)
}

func TestProtoRoundtripRowWrite(t *testing.T) {
	rw := RowWrite{
		Table: "users",
		PK:    []byte{0, 1, 2, 3},
		Kind:  WriteUpdate,
		Cols: []TypedValue{
			IntVal(42),
			TextVal("alice"),
			NullVal(),
			FloatVal(3.14),
		},
	}
	bytes, err := proto.Marshal(rowWriteToProto(rw))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var reloaded pb.RowWriteEffect
	if err := proto.Unmarshal(bytes, &reloaded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	back := rowWriteFromProto("users", &reloaded)
	if back.Table != rw.Table {
		t.Errorf("Table: %q vs %q", back.Table, rw.Table)
	}
	if string(back.PK) != string(rw.PK) {
		t.Errorf("PK: %x vs %x", back.PK, rw.PK)
	}
	if back.Kind != rw.Kind {
		t.Errorf("Kind: %v vs %v", back.Kind, rw.Kind)
	}
	if len(back.Cols) != len(rw.Cols) {
		t.Fatalf("Cols len: %d vs %d", len(back.Cols), len(rw.Cols))
	}
	for i := range rw.Cols {
		if back.Cols[i].Kind != rw.Cols[i].Kind {
			t.Errorf("col %d Kind drift: %v vs %v", i,
				rw.Cols[i].Kind, back.Cols[i].Kind)
		}
	}
}

func TestProtoRoundtripDeepNesting(t *testing.T) {
	// NOT(AND(OR(a,b), c)) should survive wire roundtrip with
	// identical evaluation semantics on a cross-product of row
	// values.
	pred := PNot(PAnd(
		POr(
			PCmp(0, OpEq, IntVal(1)),
			PCmp(0, OpEq, IntVal(2)),
		),
		PCmp(1, OpEq, TextVal("x")),
	))
	rows := []*RowWrite{
		rowOf(IntVal(1), TextVal("x")),
		rowOf(IntVal(1), TextVal("y")),
		rowOf(IntVal(2), TextVal("x")),
		rowOf(IntVal(3), TextVal("x")),
		rowOf(IntVal(3), TextVal("y")),
	}
	assertSameEval(t, pred, rows)
}
