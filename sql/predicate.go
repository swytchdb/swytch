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
	"bytes"

	"zombiezen.com/go/sqlite"
)

// Phase 5 — sub-DAG observation predicates.
//
// A Predicate is the derivation function that defines the sub-DAG a
// transaction observed during a read. At fork-choice time, each
// pending write's RowWrite tuple is evaluated against every
// concurrent transaction's observations; a write that satisfies an
// observation's predicate belongs to that observation's sub-DAG and
// triggers a conflict.
//
// The representation here is a Go-level mirror of the forthcoming
// proto. Keeping it in Go for the first pass lets us build the
// planner-side capture logic, the evaluator, and their tests without
// regenerating cluster/proto yet; wire serialisation lands when the
// swytch-core integration does.

// PredKind discriminates the Predicate struct's contents.
type PredKind uint8

const (
	// PredBool is the TRUE/FALSE leaf. Unsupported predicate
	// fragments conservatively expand to Bool(TRUE) so the
	// observation widens rather than missing conflicts.
	PredBool PredKind = iota

	// PredAnd / PredOr are boolean connectives. Short-circuit
	// evaluation with three-valued logic propagates NULL/unknown
	// correctly.
	PredAnd
	PredOr

	// PredNot inverts its child; three-valued with UNKNOWN passing
	// through.
	PredNot

	// PredCmp is `col <op> literal`.
	PredCmp

	// PredColCmp is `col <op> col` — both operands come from the
	// write's tuple.
	PredColCmp

	// PredIsNull is `col IS NULL`. `col IS NOT NULL` is expressed
	// as Not(IsNull(col)).
	PredIsNull
)

// CmpOp is the comparison operator for Cmp / ColCmp terms.
//
// BETWEEN is deliberately not represented here — the planner decomposes
// `col BETWEEN x AND y` into `And(Cmp(col ≥ x), Cmp(col ≤ y))`.
type CmpOp uint8

const (
	OpEq CmpOp = iota
	OpLt
	OpLe
	OpGt
	OpGe
)

// Predicate is a node in a boolean expression tree. Exactly one
// field-group is meaningful, discriminated by Kind:
//
//	Bool                       -> BoolVal
//	And / Or                   -> Children
//	Not                        -> Child
//	Cmp                        -> Col, Op, Literal
//	ColCmp                     -> Col, Op, Col2
//	IsNull                     -> Col
//
// The struct is intentionally a tagged sum rather than an interface
// so it round-trips through protobuf without wrapper types and is
// cheap to evaluate (no type assertions on the hot path).
type Predicate struct {
	Kind     PredKind
	BoolVal  bool
	Children []Predicate
	Child    *Predicate
	Col      int
	Col2     int
	Op       CmpOp
	Literal  TypedValue
}

// TypedValue is a tagged literal value. The Kind tag identifies which
// of the payload fields is meaningful; the evaluator uses it to pick
// a typed comparison path and to detect type mismatches that should
// evaluate to UNKNOWN.
type TypedValue struct {
	Kind  sqlite.ColumnType
	Int   int64
	Float float64
	Text  string
	Blob  []byte
}

// Constructors for TypedValue. These keep call sites readable and
// avoid forgetting to set the Kind tag.

func IntVal(v int64) TypedValue     { return TypedValue{Kind: sqlite.TypeInteger, Int: v} }
func FloatVal(v float64) TypedValue { return TypedValue{Kind: sqlite.TypeFloat, Float: v} }
func TextVal(v string) TypedValue   { return TypedValue{Kind: sqlite.TypeText, Text: v} }
func BlobVal(v []byte) TypedValue   { return TypedValue{Kind: sqlite.TypeBlob, Blob: v} }
func NullVal() TypedValue           { return TypedValue{Kind: sqlite.TypeNull} }

// Constructors for Predicate. Keep the call sites short.

func PTrue() Predicate                     { return Predicate{Kind: PredBool, BoolVal: true} }
func PFalse() Predicate                    { return Predicate{Kind: PredBool, BoolVal: false} }
func PAnd(children ...Predicate) Predicate { return Predicate{Kind: PredAnd, Children: children} }
func POr(children ...Predicate) Predicate  { return Predicate{Kind: PredOr, Children: children} }
func PNot(child Predicate) Predicate       { return Predicate{Kind: PredNot, Child: &child} }
func PIsNull(col int) Predicate            { return Predicate{Kind: PredIsNull, Col: col} }

func PCmp(col int, op CmpOp, lit TypedValue) Predicate {
	return Predicate{Kind: PredCmp, Col: col, Op: op, Literal: lit}
}

func PColCmp(left int, op CmpOp, right int) Predicate {
	return Predicate{Kind: PredColCmp, Col: left, Op: op, Col2: right}
}

// RowWrite is the structured row payload carried by every
// INSERT/UPDATE/DELETE, consumed by the predicate evaluator at
// fork-choice time. The Cols slice is ordinal-aligned with the
// table's column list at the schema tip the observation saw.
//
// For INSERTs, Cols is the new row's state. For UPDATEs, it's the
// effective post-commit state (carry-forward already applied). For
// DELETEs, it's the pre-deletion state so an observation like
// `role = 'admin'` can be evaluated against the row that was there.
type RowWrite struct {
	Table string
	PK    []byte
	Kind  WriteKind
	Cols  []TypedValue
}

// WriteKind identifies which write-path produced a RowWrite.
type WriteKind uint8

const (
	WriteInsert WriteKind = iota
	WriteUpdate
	WriteDelete
)

// triState is three-valued logic: TRUE, FALSE, or UNKNOWN. SQL
// compares involving NULL evaluate to UNKNOWN; for conflict
// detection we treat UNKNOWN as "did not observe" (no conflict) at
// the top level. Internal connectives propagate UNKNOWN per the SQL
// standard:
//
//	FALSE AND UNKNOWN = FALSE
//	TRUE  AND UNKNOWN = UNKNOWN
//	TRUE  OR  UNKNOWN = TRUE
//	FALSE OR  UNKNOWN = UNKNOWN
//	NOT UNKNOWN       = UNKNOWN
type triState uint8

const (
	triFalse triState = iota
	triTrue
	triUnknown
)

// Evaluate reports whether the given row satisfies the predicate
// under SELECT-filter semantics: TRUE rows would have been observed
// by the query, so a concurrent write producing such a row
// constitutes a conflict. UNKNOWN (NULL in comparisons) reduces to
// "not observed" → no conflict.
func Evaluate(p *Predicate, row *RowWrite) bool {
	return evalTri(p, row) == triTrue
}

func evalTri(p *Predicate, row *RowWrite) triState {
	switch p.Kind {
	case PredBool:
		if p.BoolVal {
			return triTrue
		}

		return triFalse

	case PredAnd:
		// AND: short-circuit on the first FALSE. UNKNOWN degrades
		// the final answer only if nothing came back FALSE.
		acc := triTrue

		for i := range p.Children {
			r := evalTri(&p.Children[i], row)
			if r == triFalse {
				return triFalse
			}

			if r == triUnknown {
				acc = triUnknown
			}
		}

		return acc

	case PredOr:
		// OR: short-circuit on the first TRUE. UNKNOWN degrades the
		// answer only if nothing came back TRUE.
		acc := triFalse

		for i := range p.Children {
			r := evalTri(&p.Children[i], row)
			if r == triTrue {
				return triTrue
			}

			if r == triUnknown {
				acc = triUnknown
			}
		}

		return acc

	case PredNot:
		r := evalTri(p.Child, row)
		switch r {
		case triTrue:
			return triFalse
		case triFalse:
			return triTrue
		case triUnknown:
			return triUnknown
		default:
			return triUnknown
		}

	case PredCmp:
		return evalCmp(p.Col, p.Op, p.Literal, row)

	case PredColCmp:
		return evalColCmp(p.Col, p.Op, p.Col2, row)

	case PredIsNull:
		if p.Col < 0 || p.Col >= len(row.Cols) {
			// Out-of-range ordinal means the observation was
			// captured under a different schema than the write;
			// the schema fork-check catches that case at commit
			// time. Defensive UNKNOWN here keeps the evaluator
			// from panicking if it's called on a mismatched tuple.
			return triUnknown
		}

		if row.Cols[p.Col].Kind == sqlite.TypeNull {
			return triTrue
		}

		return triFalse
	}

	return triUnknown
}

// evalCmp handles `col OP literal`. A NULL operand on either side
// makes the result UNKNOWN, matching SQL's NULL semantics.
func evalCmp(col int, op CmpOp, lit TypedValue, row *RowWrite) triState {
	if col < 0 || col >= len(row.Cols) {
		return triUnknown
	}

	left := row.Cols[col]
	if left.Kind == sqlite.TypeNull || lit.Kind == sqlite.TypeNull {
		return triUnknown
	}

	return compareTyped(left, op, lit)
}

// evalColCmp handles `col_l OP col_r`. Both operands are drawn from
// the write's tuple.
func evalColCmp(leftCol int, op CmpOp, rightCol int, row *RowWrite) triState {
	if leftCol < 0 || leftCol >= len(row.Cols) {
		return triUnknown
	}

	if rightCol < 0 || rightCol >= len(row.Cols) {
		return triUnknown
	}

	l := row.Cols[leftCol]

	r := row.Cols[rightCol]
	if l.Kind == sqlite.TypeNull || r.Kind == sqlite.TypeNull {
		return triUnknown
	}

	return compareTyped(l, op, r)
}

// compareTyped applies op to two non-NULL TypedValues. Integer /
// float mixed comparisons promote to float; all other cross-type
// comparisons return UNKNOWN (we don't model SQLite's wider
// implicit-coercion rules — writes and literals captured from the
// planner normally match the column affinity, and UNKNOWN here
// conservatively avoids spurious conflicts that come from a type
// mismatch rather than a genuine sub-DAG hit).
func compareTyped(a TypedValue, op CmpOp, b TypedValue) triState {
	// Same kind: direct compare.
	if a.Kind == b.Kind {
		switch a.Kind {
		case sqlite.TypeNull:
			return triUnknown
		case sqlite.TypeInteger:
			return fromCmp(cmpInt(a.Int, b.Int), op)
		case sqlite.TypeFloat:
			return fromCmp(cmpFloat(a.Float, b.Float), op)
		case sqlite.TypeText:
			return fromCmp(cmpStr(a.Text, b.Text), op)
		case sqlite.TypeBlob:
			return fromCmp(bytes.Compare(a.Blob, b.Blob), op)
		}

		return triUnknown
	}
	// Cross-type numeric promotion.
	if isNumeric(a.Kind) && isNumeric(b.Kind) {
		af := numericAsFloat(a)
		bf := numericAsFloat(b)

		return fromCmp(cmpFloat(af, bf), op)
	}

	return triUnknown
}

func isNumeric(k sqlite.ColumnType) bool {
	return k == sqlite.TypeInteger || k == sqlite.TypeFloat
}

func numericAsFloat(v TypedValue) float64 {
	if v.Kind == sqlite.TypeFloat {
		return v.Float
	}

	return float64(v.Int)
}

// cmpInt / cmpFloat / cmpStr return -1 / 0 / +1 like bytes.Compare.
func cmpInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}

func cmpStr(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}

// fromCmp converts a three-way compare result into a triState under
// the given operator.
func fromCmp(cmp int, op CmpOp) triState {
	var yes bool

	switch op {
	case OpEq:
		yes = cmp == 0
	case OpLt:
		yes = cmp < 0
	case OpLe:
		yes = cmp <= 0
	case OpGt:
		yes = cmp > 0
	case OpGe:
		yes = cmp >= 0
	default:
		return triUnknown
	}

	if yes {
		return triTrue
	}

	return triFalse
}
