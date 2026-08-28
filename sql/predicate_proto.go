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
	pb "github.com/swytchdb/engine/cluster/proto"
	"zombiezen.com/go/sqlite"
)

// Bidirectional conversions between the Go-level Predicate /
// TypedValue / RowWrite types in this package and their proto
// counterparts in cluster/proto.
//
// The two representations mirror each other, so these functions are
// just structural translators. They're kept in their own file to
// make the proto-boundary traffic easy to audit.

func predicateToProto(p Predicate) *pb.Predicate {
	out := &pb.Predicate{}
	switch p.Kind {
	case PredBool:
		out.Kind = pb.Predicate_BOOL
		out.BoolVal = p.BoolVal

	case PredAnd:
		out.Kind = pb.Predicate_AND
		out.Children = make([]*pb.Predicate, len(p.Children))
		for i := range p.Children {
			out.Children[i] = predicateToProto(p.Children[i])
		}

	case PredOr:
		out.Kind = pb.Predicate_OR
		out.Children = make([]*pb.Predicate, len(p.Children))
		for i := range p.Children {
			out.Children[i] = predicateToProto(p.Children[i])
		}

	case PredNot:
		out.Kind = pb.Predicate_NOT
		if p.Child != nil {
			out.Child = predicateToProto(*p.Child)
		}

	case PredCmp:
		out.Kind = pb.Predicate_CMP
		out.Col = uint32(p.Col)
		out.Op = cmpOpToProto(p.Op)
		out.Literal = typedValueToProto(p.Literal)

	case PredColCmp:
		out.Kind = pb.Predicate_COL_CMP
		out.Col = uint32(p.Col)
		out.Col2 = uint32(p.Col2)
		out.Op = cmpOpToProto(p.Op)

	case PredIsNull:
		out.Kind = pb.Predicate_IS_NULL
		out.Col = uint32(p.Col)
	}
	return out
}

func predicateFromProto(p *pb.Predicate) Predicate {
	if p == nil {
		return PTrue()
	}
	switch p.Kind {
	case pb.Predicate_BOOL:
		return Predicate{Kind: PredBool, BoolVal: p.BoolVal}

	case pb.Predicate_AND:
		children := make([]Predicate, len(p.Children))
		for i := range p.Children {
			children[i] = predicateFromProto(p.Children[i])
		}
		return Predicate{Kind: PredAnd, Children: children}

	case pb.Predicate_OR:
		children := make([]Predicate, len(p.Children))
		for i := range p.Children {
			children[i] = predicateFromProto(p.Children[i])
		}
		return Predicate{Kind: PredOr, Children: children}

	case pb.Predicate_NOT:
		if p.Child == nil {
			return PTrue()
		}
		child := predicateFromProto(p.Child)
		return Predicate{Kind: PredNot, Child: &child}

	case pb.Predicate_CMP:
		return Predicate{
			Kind:    PredCmp,
			Col:     int(p.Col),
			Op:      cmpOpFromProto(p.Op),
			Literal: typedValueFromProto(p.Literal),
		}

	case pb.Predicate_COL_CMP:
		return Predicate{
			Kind: PredColCmp,
			Col:  int(p.Col),
			Col2: int(p.Col2),
			Op:   cmpOpFromProto(p.Op),
		}

	case pb.Predicate_IS_NULL:
		return Predicate{Kind: PredIsNull, Col: int(p.Col)}
	}
	return PTrue()
}

func cmpOpToProto(op CmpOp) pb.PredicateCmpOp {
	switch op {
	case OpEq:
		return pb.PredicateCmpOp_CMP_EQ
	case OpLt:
		return pb.PredicateCmpOp_CMP_LT
	case OpLe:
		return pb.PredicateCmpOp_CMP_LE
	case OpGt:
		return pb.PredicateCmpOp_CMP_GT
	case OpGe:
		return pb.PredicateCmpOp_CMP_GE
	}
	return pb.PredicateCmpOp_CMP_EQ
}

func cmpOpFromProto(op pb.PredicateCmpOp) CmpOp {
	switch op {
	case pb.PredicateCmpOp_CMP_EQ:
		return OpEq
	case pb.PredicateCmpOp_CMP_LT:
		return OpLt
	case pb.PredicateCmpOp_CMP_LE:
		return OpLe
	case pb.PredicateCmpOp_CMP_GT:
		return OpGt
	case pb.PredicateCmpOp_CMP_GE:
		return OpGe
	}
	return OpEq
}

func typedValueToProto(v TypedValue) *pb.TypedValue {
	out := &pb.TypedValue{}
	switch v.Kind {
	case sqlite.TypeInteger:
		out.Kind = pb.TypedValue_INT
		out.IntVal = v.Int
	case sqlite.TypeFloat:
		out.Kind = pb.TypedValue_FLOAT
		out.FloatVal = v.Float
	case sqlite.TypeText:
		out.Kind = pb.TypedValue_TEXT
		out.TextVal = v.Text
	case sqlite.TypeBlob:
		out.Kind = pb.TypedValue_BLOB
		out.BlobVal = v.Blob
	default:
		out.Kind = pb.TypedValue_NULL_VALUE
	}
	return out
}

func typedValueFromProto(v *pb.TypedValue) TypedValue {
	if v == nil {
		return NullVal()
	}
	switch v.Kind {
	case pb.TypedValue_INT:
		return IntVal(v.IntVal)
	case pb.TypedValue_FLOAT:
		return FloatVal(v.FloatVal)
	case pb.TypedValue_TEXT:
		return TextVal(v.TextVal)
	case pb.TypedValue_BLOB:
		return BlobVal(v.BlobVal)
	}
	return NullVal()
}

func rowWriteToProto(rw RowWrite) *pb.RowWriteEffect {
	out := &pb.RowWriteEffect{
		Pk:      rw.PK,
		Kind:    writeKindToProto(rw.Kind),
		Columns: make([]*pb.TypedValue, len(rw.Cols)),
	}
	for i := range rw.Cols {
		out.Columns[i] = typedValueToProto(rw.Cols[i])
	}
	return out
}

func rowWriteFromProto(table string, rw *pb.RowWriteEffect) RowWrite {
	if rw == nil {
		return RowWrite{Table: table}
	}
	out := RowWrite{
		Table: table,
		PK:    rw.Pk,
		Kind:  writeKindFromProto(rw.Kind),
		Cols:  make([]TypedValue, len(rw.Columns)),
	}
	for i := range rw.Columns {
		out.Cols[i] = typedValueFromProto(rw.Columns[i])
	}
	return out
}

func writeKindToProto(k WriteKind) pb.RowWriteEffect_Kind {
	switch k {
	case WriteInsert:
		return pb.RowWriteEffect_INSERT
	case WriteUpdate:
		return pb.RowWriteEffect_UPDATE
	case WriteDelete:
		return pb.RowWriteEffect_DELETE
	}
	return pb.RowWriteEffect_INSERT
}

func writeKindFromProto(k pb.RowWriteEffect_Kind) WriteKind {
	switch k {
	case pb.RowWriteEffect_INSERT:
		return WriteInsert
	case pb.RowWriteEffect_UPDATE:
		return WriteUpdate
	case pb.RowWriteEffect_DELETE:
		return WriteDelete
	}
	return WriteInsert
}
