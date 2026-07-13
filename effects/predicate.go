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

	pb "github.com/swytchdb/swytch/cluster/proto"
)

// Engine-side predicate evaluator — mirrors sql/predicate.go but
// operates directly on the proto types (*pb.Predicate,
// *pb.RowWriteEffect) so fork-choice in effects/ can run predicate
// conflict checks without importing sql/.
//
// collectTxnEffectsOnKey and hasPredicateConflict live here too so
// the fork-choice refinement is one import away from its evaluator.

// collectTxnEffectsOnKey walks backward from startTips through Deps,
// pulling effects from the cache / log, and returns the observation
// and row-write payloads whose TxnId matches txnID and whose Key
// matches key. Walk is unbounded — long transactions have already
// paid the cost of their own history; the log retains it either way
// and chain compaction is handled by SnapshotEffects elsewhere.
//
// `allDeps` is used (not `eff.Deps` directly) so that when a start
// tip is a TransactionalBindEffect, the bind's per-key NewTips are
// also followed — entering the bind from its offset reaches the
// tx's entire reachable history.
func (e *Engine) collectTxnEffectsOnKey(txnID, key string, startTips ...Tip) (
	obs []*pb.ObservationEffect, rw []*pb.RowWriteEffect,
) {
	var zero Tip
	visited := make(map[Tip]bool, 16)
	stack := make([]Tip, 0, len(startTips))
	for _, t := range startTips {
		if t != zero {
			stack = append(stack, t)
		}
	}
	for len(stack) > 0 {
		off := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if visited[off] {
			continue
		}
		visited[off] = true
		eff, err := e.getEffect(key, off)
		if err != nil {
			continue
		}
		// A different TxnId terminates this branch — we've crossed
		// into somebody else's history.
		if eff.TxnId != txnID {
			continue
		}
		if string(eff.Key) == key {
			if o := eff.GetObservation(); o != nil {
				obs = append(obs, o)
			}
			if w := eff.GetRowWrite(); w != nil {
				rw = append(rw, w)
			}
		}
		// Use allDeps so that starting from a bind walks its
		// per-key NewTips, not just its top-level Deps.
		for _, dep := range allDeps(eff) {
			d := r(dep)
			if !visited[d] {
				stack = append(stack, d)
			}
		}
	}
	return
}

// hasPredicateEvidence reports whether we found any observation or
// row-write for the tx on the key. Used by fork-choice to decide
// whether predicate refinement applies — if either side has no
// evidence, we fall back to the conservative shared-base conflict
// (the tx touched the key for some other reason, such as a schema
// change, which still needs abort semantics).
func hasPredicateEvidence(obs []*pb.ObservationEffect, rw []*pb.RowWriteEffect) bool {
	return len(obs) > 0 || len(rw) > 0
}

// hasPredicateConflict returns true iff our tx's observations on key
// match any of their row-writes, or vice versa. Both sides must
// have evidence; otherwise false is returned and the caller should
// treat shared-base as a conflict (conservative fallback). That
// preserves the old behaviour for non-SQL consumers whose binds
// don't emit observations / row-writes.
//
// Each side is identified by its txnID plus a set of startTips for
// the walk — typically the bind's per-key NewTip(s) plus the bind
// offset itself. Multiple tips are accepted so callers can pass
// whatever they have locally without having to re-derive a single
// canonical entry point.
func (e *Engine) hasPredicateConflict(
	ourTxnID, theirTxnID, key string,
	ourStartTips, theirStartTips []Tip,
) (conflict bool, bothSidesHadEvidence bool) {
	ourObs, ourRW := e.collectTxnEffectsOnKey(ourTxnID, key, ourStartTips...)
	theirObs, theirRW := e.collectTxnEffectsOnKey(theirTxnID, key, theirStartTips...)

	if !hasPredicateEvidence(ourObs, ourRW) || !hasPredicateEvidence(theirObs, theirRW) {
		return false, false
	}

	for _, o := range ourObs {
		if o.Predicate == nil {
			continue
		}
		for _, w := range theirRW {
			if predicateMatches(o.Predicate, w) {
				return true, true
			}
		}
	}
	for _, o := range theirObs {
		if o.Predicate == nil {
			continue
		}
		for _, w := range ourRW {
			if predicateMatches(o.Predicate, w) {
				return true, true
			}
		}
	}
	return false, true
}

// Three-valued logic: NULL comparisons evaluate to UNKNOWN, which
// reduces to "not a match" at the top level. This conservatively
// widens the set of commits — a write that would have evaluated
// UNKNOWN never triggers a fork conflict, so it's treated as if the
// observation didn't see it. That's the correct conservative stance:
// at worst we allow an extra commit; we never falsely abort.

type predTri uint8

const (
	predFalse predTri = iota
	predTrue
	predUnknown
)

// predicateMatches reports whether the given row satisfies the
// predicate. NULL / UNKNOWN degrades to false.
func predicateMatches(p *pb.Predicate, rw *pb.RowWriteEffect) bool {
	if p == nil || rw == nil {
		return false
	}
	return predEval(p, rw) == predTrue
}

func predEval(p *pb.Predicate, rw *pb.RowWriteEffect) predTri {
	switch p.Kind {
	case pb.Predicate_BOOL:
		if p.BoolVal {
			return predTrue
		}
		return predFalse

	case pb.Predicate_AND:
		acc := predTrue
		for _, child := range p.Children {
			r := predEval(child, rw)
			if r == predFalse {
				return predFalse
			}
			if r == predUnknown {
				acc = predUnknown
			}
		}
		return acc

	case pb.Predicate_OR:
		acc := predFalse
		for _, child := range p.Children {
			r := predEval(child, rw)
			if r == predTrue {
				return predTrue
			}
			if r == predUnknown {
				acc = predUnknown
			}
		}
		return acc

	case pb.Predicate_NOT:
		r := predEval(p.Child, rw)
		switch r {
		case predTrue:
			return predFalse
		case predFalse:
			return predTrue
		}
		return predUnknown

	case pb.Predicate_CMP:
		return predCmp(int(p.Col), p.Op, p.Literal, rw)

	case pb.Predicate_COL_CMP:
		return predColCmp(int(p.Col), p.Op, int(p.Col2), rw)

	case pb.Predicate_IS_NULL:
		col := int(p.Col)
		if col < 0 || col >= len(rw.Columns) {
			return predUnknown
		}
		if rw.Columns[col].Kind == pb.TypedValue_NULL_VALUE {
			return predTrue
		}
		return predFalse
	}
	return predUnknown
}

func predCmp(col int, op pb.PredicateCmpOp, lit *pb.TypedValue, rw *pb.RowWriteEffect) predTri {
	if col < 0 || col >= len(rw.Columns) {
		return predUnknown
	}
	if lit == nil {
		return predUnknown
	}
	left := rw.Columns[col]
	if left.Kind == pb.TypedValue_NULL_VALUE || lit.Kind == pb.TypedValue_NULL_VALUE {
		return predUnknown
	}
	return compareTypedValues(left, op, lit)
}

func predColCmp(leftCol int, op pb.PredicateCmpOp, rightCol int, rw *pb.RowWriteEffect) predTri {
	if leftCol < 0 || leftCol >= len(rw.Columns) {
		return predUnknown
	}
	if rightCol < 0 || rightCol >= len(rw.Columns) {
		return predUnknown
	}
	l := rw.Columns[leftCol]
	r := rw.Columns[rightCol]
	if l.Kind == pb.TypedValue_NULL_VALUE || r.Kind == pb.TypedValue_NULL_VALUE {
		return predUnknown
	}
	return compareTypedValues(l, op, r)
}

func compareTypedValues(a *pb.TypedValue, op pb.PredicateCmpOp, b *pb.TypedValue) predTri {
	if a.Kind == b.Kind {
		switch a.Kind {
		case pb.TypedValue_INT:
			return predCmpFromInt(cmp3Int(a.IntVal, b.IntVal), op)
		case pb.TypedValue_FLOAT:
			return predCmpFromInt(cmp3Float(a.FloatVal, b.FloatVal), op)
		case pb.TypedValue_TEXT:
			return predCmpFromInt(cmp3Str(a.TextVal, b.TextVal), op)
		case pb.TypedValue_BLOB:
			return predCmpFromInt(bytes.Compare(a.BlobVal, b.BlobVal), op)
		}
		return predUnknown
	}
	// Cross-type numeric promotion.
	if isNumericKind(a.Kind) && isNumericKind(b.Kind) {
		af := numericKindAsFloat(a)
		bf := numericKindAsFloat(b)
		return predCmpFromInt(cmp3Float(af, bf), op)
	}
	return predUnknown
}

func isNumericKind(k pb.TypedValue_Kind) bool {
	return k == pb.TypedValue_INT || k == pb.TypedValue_FLOAT
}

func numericKindAsFloat(v *pb.TypedValue) float64 {
	if v.Kind == pb.TypedValue_FLOAT {
		return v.FloatVal
	}
	return float64(v.IntVal)
}

func cmp3Int(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func cmp3Float(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func cmp3Str(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func predCmpFromInt(c int, op pb.PredicateCmpOp) predTri {
	var yes bool
	switch op {
	case pb.PredicateCmpOp_CMP_EQ:
		yes = c == 0
	case pb.PredicateCmpOp_CMP_LT:
		yes = c < 0
	case pb.PredicateCmpOp_CMP_LE:
		yes = c <= 0
	case pb.PredicateCmpOp_CMP_GT:
		yes = c > 0
	case pb.PredicateCmpOp_CMP_GE:
		yes = c >= 0
	default:
		return predUnknown
	}
	if yes {
		return predTrue
	}
	return predFalse
}
