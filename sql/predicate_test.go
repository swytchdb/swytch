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

	"zombiezen.com/go/sqlite"
)

// rowOf builds a RowWrite with INSERT semantics for tests.
func rowOf(cols ...TypedValue) *RowWrite {
	return &RowWrite{Kind: WriteInsert, Cols: cols}
}

func TestPredicateBoolConstants(t *testing.T) {
	row := rowOf(IntVal(1))
	t_ := PTrue()
	f := PFalse()

	if !Evaluate(&t_, row) {
		t.Error("PTrue should evaluate to true")
	}

	if Evaluate(&f, row) {
		t.Error("PFalse should evaluate to false")
	}
}

func TestPredicateCmpInt(t *testing.T) {
	row := rowOf(IntVal(7))

	cases := []struct {
		op   CmpOp
		lit  int64
		want bool
	}{
		{OpEq, 7, true}, {OpEq, 8, false},
		{OpLt, 8, true}, {OpLt, 7, false},
		{OpLe, 7, true}, {OpLe, 6, false},
		{OpGt, 6, true}, {OpGt, 7, false},
		{OpGe, 7, true}, {OpGe, 8, false},
	}
	for _, c := range cases {
		p := PCmp(0, c.op, IntVal(c.lit))
		if got := Evaluate(&p, row); got != c.want {
			t.Errorf("col=7 %v %d: got %v, want %v", c.op, c.lit, got, c.want)
		}
	}
}

func TestPredicateCmpFloat(t *testing.T) {
	row := rowOf(FloatVal(1.5))

	cases := []struct {
		op   CmpOp
		lit  float64
		want bool
	}{
		{OpEq, 1.5, true}, {OpEq, 2.0, false},
		{OpLt, 2.0, true}, {OpGt, 1.0, true},
	}
	for _, c := range cases {
		p := PCmp(0, c.op, FloatVal(c.lit))
		if got := Evaluate(&p, row); got != c.want {
			t.Errorf("col=1.5 %v %v: got %v, want %v", c.op, c.lit, got, c.want)
		}
	}
}

func TestPredicateCmpText(t *testing.T) {
	row := rowOf(TextVal("hello"))
	if p := PCmp(0, OpEq, TextVal("hello")); !Evaluate(&p, row) {
		t.Error("text eq failed")
	}

	if p := PCmp(0, OpLt, TextVal("world")); !Evaluate(&p, row) {
		t.Error("text lt failed")
	}

	if p := PCmp(0, OpGt, TextVal("apple")); !Evaluate(&p, row) {
		t.Error("text gt failed")
	}
}

func TestPredicateCmpIntFloatPromotion(t *testing.T) {
	// Mixed-type numeric comparison promotes to float.
	row := rowOf(IntVal(5))

	p := PCmp(0, OpLt, FloatVal(5.5))
	if !Evaluate(&p, row) {
		t.Error("int 5 < float 5.5 should be true after promotion")
	}

	rowF := rowOf(FloatVal(5.5))

	p2 := PCmp(0, OpGt, IntVal(5))
	if !Evaluate(&p2, rowF) {
		t.Error("float 5.5 > int 5 should be true after promotion")
	}
}

func TestPredicateNullInComparison(t *testing.T) {
	// NULL in a comparison produces UNKNOWN, which evaluates to
	// "not observed" (false) at the top level.
	row := rowOf(NullVal())

	p := PCmp(0, OpEq, IntVal(5))
	if Evaluate(&p, row) {
		t.Error("NULL = 5 should be unknown → false")
	}

	p2 := PNot(PCmp(0, OpEq, IntVal(5)))
	if Evaluate(&p2, row) {
		t.Error("NOT (NULL = 5) should still be unknown → false")
	}
}

func TestPredicateIsNull(t *testing.T) {
	nullRow := rowOf(NullVal())
	valRow := rowOf(IntVal(3))
	isNull := PIsNull(0)
	isNotNull := PNot(PIsNull(0))

	if !Evaluate(&isNull, nullRow) {
		t.Error("IS NULL should match NULL row")
	}

	if Evaluate(&isNull, valRow) {
		t.Error("IS NULL should not match non-NULL row")
	}

	if Evaluate(&isNotNull, nullRow) {
		t.Error("IS NOT NULL should not match NULL row")
	}

	if !Evaluate(&isNotNull, valRow) {
		t.Error("IS NOT NULL should match non-NULL row")
	}
}

func TestPredicateAnd(t *testing.T) {
	row := rowOf(IntVal(5), TextVal("alice"))

	match := PAnd(
		PCmp(0, OpEq, IntVal(5)),
		PCmp(1, OpEq, TextVal("alice")),
	)
	if !Evaluate(&match, row) {
		t.Error("AND of two true terms should be true")
	}

	mismatch := PAnd(
		PCmp(0, OpEq, IntVal(5)),
		PCmp(1, OpEq, TextVal("bob")),
	)
	if Evaluate(&mismatch, row) {
		t.Error("AND with one false term should be false")
	}
}

func TestPredicateOr(t *testing.T) {
	row := rowOf(IntVal(5))

	match := POr(
		PCmp(0, OpEq, IntVal(5)),
		PCmp(0, OpEq, IntVal(99)),
	)
	if !Evaluate(&match, row) {
		t.Error("OR with first term true should be true")
	}

	allMiss := POr(
		PCmp(0, OpEq, IntVal(99)),
		PCmp(0, OpEq, IntVal(100)),
	)
	if Evaluate(&allMiss, row) {
		t.Error("OR with no true terms should be false")
	}
}

func TestPredicateNot(t *testing.T) {
	row := rowOf(IntVal(5))

	p := PNot(PCmp(0, OpEq, IntVal(5)))
	if Evaluate(&p, row) {
		t.Error("NOT (col=5) on col=5 should be false")
	}

	p2 := PNot(PCmp(0, OpEq, IntVal(99)))
	if !Evaluate(&p2, row) {
		t.Error("NOT (col=99) on col=5 should be true")
	}
}

// Three-valued logic: AND with UNKNOWN and FALSE → FALSE; AND with
// UNKNOWN and TRUE → UNKNOWN (→ not observed); OR with UNKNOWN and
// TRUE → TRUE; OR with UNKNOWN and FALSE → UNKNOWN.
func TestPredicateThreeValuedLogic(t *testing.T) {
	row := rowOf(NullVal(), IntVal(1))
	unknownTerm := PCmp(0, OpEq, IntVal(5))
	falseTerm := PCmp(1, OpEq, IntVal(99))
	trueTerm := PCmp(1, OpEq, IntVal(1))

	andUnkTrue := PAnd(unknownTerm, trueTerm)
	if Evaluate(&andUnkTrue, row) {
		t.Error("UNKNOWN AND TRUE = UNKNOWN → false at top")
	}

	andUnkFalse := PAnd(unknownTerm, falseTerm)
	if Evaluate(&andUnkFalse, row) {
		t.Error("UNKNOWN AND FALSE = FALSE (short-circuit on false)")
	}

	orUnkTrue := POr(unknownTerm, trueTerm)
	if !Evaluate(&orUnkTrue, row) {
		t.Error("UNKNOWN OR TRUE = TRUE (short-circuit on true)")
	}

	orUnkFalse := POr(unknownTerm, falseTerm)
	if Evaluate(&orUnkFalse, row) {
		t.Error("UNKNOWN OR FALSE = UNKNOWN → false at top")
	}
}

func TestPredicateBetweenDecomposition(t *testing.T) {
	// BETWEEN 5 AND 10 decomposes into ≥ 5 AND ≤ 10.
	between := func(col int, lo, hi int64) Predicate {
		return PAnd(
			PCmp(col, OpGe, IntVal(lo)),
			PCmp(col, OpLe, IntVal(hi)),
		)
	}

	cases := []struct {
		val  int64
		want bool
	}{
		{4, false}, {5, true}, {7, true}, {10, true}, {11, false},
	}
	for _, c := range cases {
		row := rowOf(IntVal(c.val))

		p := between(0, 5, 10)
		if got := Evaluate(&p, row); got != c.want {
			t.Errorf("BETWEEN 5 AND 10 on %d: got %v, want %v", c.val, got, c.want)
		}
	}
}

func TestPredicateColCmp(t *testing.T) {
	// col_l < col_r semantics on both-non-NULL and one-NULL cases.
	row := rowOf(IntVal(5), IntVal(10))

	p := PColCmp(0, OpLt, 1)
	if !Evaluate(&p, row) {
		t.Error("5 < 10 should be true")
	}

	p2 := PColCmp(0, OpGt, 1)
	if Evaluate(&p2, row) {
		t.Error("5 > 10 should be false")
	}

	nullRow := rowOf(IntVal(5), NullVal())

	p3 := PColCmp(0, OpLt, 1)
	if Evaluate(&p3, nullRow) {
		t.Error("5 < NULL should be unknown → false at top")
	}
}

func TestPredicateOutOfRangeOrdinal(t *testing.T) {
	// Defensive: a predicate referencing a column beyond the tuple
	// length evaluates to UNKNOWN rather than panicking. The real
	// defense is the schema fork-check at commit time; this just
	// keeps the evaluator from crashing when called on a mismatched
	// tuple.
	row := rowOf(IntVal(1))

	p := PCmp(5, OpEq, IntVal(1))
	if Evaluate(&p, row) {
		t.Error("out-of-range col should not match")
	}

	p2 := PIsNull(5)
	if Evaluate(&p2, row) {
		t.Error("out-of-range IsNull should not match")
	}
}

func TestPredicateConservativeExpansion(t *testing.T) {
	// Any predicate fragment the planner can't translate becomes
	// Bool(TRUE). A write then ALWAYS matches the relevant subtree.
	// This test models `WHERE foo(col) = 5 AND col2 = 7` — the
	// planner drops the foo() term (becomes TRUE), keeps col2=7.
	row := rowOf(IntVal(99), IntVal(7))

	p := PAnd(PTrue(), PCmp(1, OpEq, IntVal(7)))
	if !Evaluate(&p, row) {
		t.Error("expanded (TRUE AND col1=7) should match on col1=7")
	}

	rowMiss := rowOf(IntVal(99), IntVal(99))
	if Evaluate(&p, rowMiss) {
		t.Error("expanded predicate should still require col1=7")
	}
}

// Scenario: write-skew on a meeting-room schedule. Txn A and Txn B
// both observe "room=X AND time=2pm" as empty, both insert. A's
// write must satisfy B's predicate (and vice versa) so fork-choice
// fires.
func TestPredicateWriteSkewMeetingRoom(t *testing.T) {
	// Columns: 0=room, 1=time, 2=meeting
	pred := PAnd(
		PCmp(0, OpEq, TextVal("X")),
		PCmp(1, OpEq, TextVal("2pm")),
	)
	aWrite := &RowWrite{Cols: []TypedValue{TextVal("X"), TextVal("2pm"), TextVal("A")}}
	bWrite := &RowWrite{Cols: []TypedValue{TextVal("X"), TextVal("2pm"), TextVal("B")}}
	otherWrite := &RowWrite{Cols: []TypedValue{TextVal("Y"), TextVal("2pm"), TextVal("C")}}

	if !Evaluate(&pred, aWrite) {
		t.Error("A's write should satisfy predicate")
	}

	if !Evaluate(&pred, bWrite) {
		t.Error("B's write should satisfy predicate")
	}

	if Evaluate(&pred, otherWrite) {
		t.Error("unrelated write (room=Y) should not satisfy predicate")
	}
}

// Scenario: at-least-one-admin. Delete of the only admin should be
// observed by another transaction's `role=admin` observation.
func TestPredicateAtLeastOneAdmin(t *testing.T) {
	// Columns: 0=user_id, 1=role
	pred := PCmp(1, OpEq, TextVal("admin"))
	deletedAdmin := &RowWrite{
		Kind: WriteDelete,
		Cols: []TypedValue{TextVal("alice"), TextVal("admin")},
	}
	deletedUser := &RowWrite{
		Kind: WriteDelete,
		Cols: []TypedValue{TextVal("bob"), TextVal("user")},
	}

	if !Evaluate(&pred, deletedAdmin) {
		t.Error("delete of admin should satisfy role=admin")
	}

	if Evaluate(&pred, deletedUser) {
		t.Error("delete of non-admin should not satisfy role=admin")
	}
}

// Scenario: temporal overlap. Observation predicate is a range
// comparison on start/end times. A new reservation's write tuple
// satisfies the predicate iff its range overlaps.
func TestPredicateTemporalOverlap(t *testing.T) {
	// Columns: 0=start, 1=end
	// Observation: looking for reservations that overlap [2pm, 4pm].
	// An existing reservation [a, b] overlaps iff a < 4pm AND b > 2pm.
	// Using integer-encoded times (hour-of-day).
	pred := PAnd(
		PCmp(0, OpLt, IntVal(16)), // start < 4pm
		PCmp(1, OpGt, IntVal(14)), // end > 2pm
	)

	overlaps := &RowWrite{Cols: []TypedValue{IntVal(15), IntVal(17)}} // 3pm-5pm
	noOverlapBefore := &RowWrite{Cols: []TypedValue{IntVal(10), IntVal(12)}}
	noOverlapAfter := &RowWrite{Cols: []TypedValue{IntVal(16), IntVal(18)}}

	if !Evaluate(&pred, overlaps) {
		t.Error("3pm-5pm should overlap 2pm-4pm")
	}

	if Evaluate(&pred, noOverlapBefore) {
		t.Error("10am-12pm should not overlap 2pm-4pm")
	}

	if Evaluate(&pred, noOverlapAfter) {
		t.Error("4pm-6pm should not overlap 2pm-4pm (exclusive)")
	}
}

// Scenario: NOT applied to a disjunction — the pred `NOT (type = A
// OR type = B)` matches writes whose type is neither A nor B.
func TestPredicateNotOverOr(t *testing.T) {
	pred := PNot(POr(
		PCmp(0, OpEq, TextVal("A")),
		PCmp(0, OpEq, TextVal("B")),
	))
	if !Evaluate(&pred, rowOf(TextVal("C"))) {
		t.Error("NOT (A OR B) should match C")
	}

	if Evaluate(&pred, rowOf(TextVal("A"))) {
		t.Error("NOT (A OR B) should not match A")
	}

	if Evaluate(&pred, rowOf(TextVal("B"))) {
		t.Error("NOT (A OR B) should not match B")
	}
}

// Verify that our TypedValue constructors tag Kind correctly.
func TestTypedValueConstructorsTagKind(t *testing.T) {
	cases := []struct {
		v    TypedValue
		want sqlite.ColumnType
	}{
		{IntVal(0), sqlite.TypeInteger},
		{FloatVal(0), sqlite.TypeFloat},
		{TextVal(""), sqlite.TypeText},
		{BlobVal(nil), sqlite.TypeBlob},
		{NullVal(), sqlite.TypeNull},
	}
	for _, c := range cases {
		if c.v.Kind != c.want {
			t.Errorf("constructor produced Kind=%v, want %v", c.v.Kind, c.want)
		}
	}
}
