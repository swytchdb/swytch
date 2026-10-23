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

import "testing"

// txn is a simplified transaction shape for testing the commit-time
// conflict evaluator. It contains the observations the transaction
// made (predicates, each scoped to a table) and the RowWrites it
// intends to commit.
type txn struct {
	name         string
	observations []observation
	writes       []RowWrite
}

// observation binds a Predicate to a table. Phase 5's real
// ObservationEffect will have the same structure.
type observation struct {
	table string
	pred  Predicate
}

// checkConflict reports whether two transactions conflict under
// the sub-DAG observation semantics. T_a conflicts with T_b iff any
// of T_a's writes satisfies any of T_b's observations (on the same
// table), OR vice versa.
//
// This is the logic Phase 5's fork-choice will run at commit time.
// The real implementation will then use HLC / fork-choice ordering
// to decide which transaction wins; the test harness just reports
// whether a conflict exists at all.
func checkConflict(a, b *txn) bool {
	if aHitsB := writesHitObs(a.writes, b.observations); aHitsB {
		return true
	}

	if bHitsA := writesHitObs(b.writes, a.observations); bHitsA {
		return true
	}

	return false
}

// writesHitObs returns true if any write satisfies any observation
// whose table matches the write's table.
func writesHitObs(writes []RowWrite, obs []observation) bool {
	for i := range writes {
		w := &writes[i]

		for j := range obs {
			o := &obs[j]
			if w.Table != o.table {
				continue
			}

			if Evaluate(&o.pred, w) {
				return true
			}
		}
	}

	return false
}

// --- Scenarios ---

// Meeting room: A and B both observe an empty (room=X, time=2pm),
// both write a meeting there. Each write satisfies the other's
// predicate; both directions fire a conflict.
func TestConflictMeetingRoom(t *testing.T) {
	// Columns: 0=room, 1=time, 2=meeting_id
	pred := PAnd(
		PCmp(0, OpEq, TextVal("X")),
		PCmp(1, OpEq, TextVal("2pm")),
	)
	ta := &txn{
		name: "A",
		observations: []observation{
			{table: "meetings", pred: pred},
		},
		writes: []RowWrite{
			{Table: "meetings", Kind: WriteInsert, Cols: []TypedValue{
				TextVal("X"), TextVal("2pm"), TextVal("A"),
			}},
		},
	}

	tb := &txn{
		name: "B",
		observations: []observation{
			{table: "meetings", pred: pred},
		},
		writes: []RowWrite{
			{Table: "meetings", Kind: WriteInsert, Cols: []TypedValue{
				TextVal("X"), TextVal("2pm"), TextVal("B"),
			}},
		},
	}
	if !checkConflict(ta, tb) {
		t.Error("meeting-room A and B must conflict")
	}
}

// Phantom insert: A observes (range) x > 5 on table t, writes
// nothing relevant. B inserts a row with x=7 on table t.
// One direction — B's write satisfies A's predicate — triggers the
// conflict. This is the classic SERIALIZABLE case a per-row noop
// scheme misses and predicates catch.
func TestConflictPhantomInsert(t *testing.T) {
	obsPred := PCmp(0, OpGt, IntVal(5))

	ta := &txn{
		name: "A",
		observations: []observation{
			{table: "t", pred: obsPred},
		},
		// A writes to an unrelated table, which never hits B's obs.
		writes: []RowWrite{
			{Table: "log", Kind: WriteInsert, Cols: []TypedValue{
				TextVal("audit"),
			}},
		},
	}

	tb := &txn{
		name: "B",
		// B doesn't care to observe anything; it just inserts.
		writes: []RowWrite{
			{Table: "t", Kind: WriteInsert, Cols: []TypedValue{
				IntVal(7),
			}},
		},
	}
	if !checkConflict(ta, tb) {
		t.Error("phantom insert must conflict with A's observation")
	}
}

// At-least-one admin: two transactions concurrently delete
// different admins. Each delete's RowWrite carries the deleted
// row's column values (`role = 'admin'`), which satisfies the
// other's observation predicate.
func TestConflictAtLeastOneAdmin(t *testing.T) {
	obsPred := PCmp(1, OpEq, TextVal("admin"))

	ta := &txn{
		name: "A",
		observations: []observation{
			{table: "users", pred: obsPred},
		},
		writes: []RowWrite{
			{Table: "users", Kind: WriteDelete, Cols: []TypedValue{
				TextVal("alice"), TextVal("admin"),
			}},
		},
	}

	tb := &txn{
		name: "B",
		observations: []observation{
			{table: "users", pred: obsPred},
		},
		writes: []RowWrite{
			{Table: "users", Kind: WriteDelete, Cols: []TypedValue{
				TextVal("bob"), TextVal("admin"),
			}},
		},
	}
	if !checkConflict(ta, tb) {
		t.Error("concurrent admin deletes must conflict")
	}
}

// Temporal overlap: A reserves [2pm,4pm], B reserves [3pm,5pm].
// The observation predicate for "overlaps my window" is the classic
// `start < my_end AND end > my_start` inequality pair.
func TestConflictTemporalOverlap(t *testing.T) {
	// Columns: 0=start, 1=end
	aObs := PAnd(
		PCmp(0, OpLt, IntVal(16)), // any start < 4pm
		PCmp(1, OpGt, IntVal(14)), // any end   > 2pm
	)
	bObs := PAnd(
		PCmp(0, OpLt, IntVal(17)), // start < 5pm
		PCmp(1, OpGt, IntVal(15)), // end   > 3pm
	)

	ta := &txn{
		name: "A",
		observations: []observation{
			{table: "reservations", pred: aObs},
		},
		writes: []RowWrite{
			{Table: "reservations", Kind: WriteInsert, Cols: []TypedValue{
				IntVal(14), IntVal(16),
			}},
		},
	}

	tb := &txn{
		name: "B",
		observations: []observation{
			{table: "reservations", pred: bObs},
		},
		writes: []RowWrite{
			{Table: "reservations", Kind: WriteInsert, Cols: []TypedValue{
				IntVal(15), IntVal(17),
			}},
		},
	}
	if !checkConflict(ta, tb) {
		t.Error("overlapping reservations must conflict")
	}
}

// No conflict: disjoint predicates on the same table, disjoint writes.
func TestConflictDisjoint(t *testing.T) {
	ta := &txn{
		name: "A",
		observations: []observation{
			{table: "events", pred: PCmp(0, OpEq, TextVal("type_a"))},
		},
		writes: []RowWrite{
			{Table: "events", Kind: WriteInsert, Cols: []TypedValue{
				TextVal("type_a"), IntVal(1),
			}},
		},
	}

	tb := &txn{
		name: "B",
		observations: []observation{
			{table: "events", pred: PCmp(0, OpEq, TextVal("type_b"))},
		},
		writes: []RowWrite{
			{Table: "events", Kind: WriteInsert, Cols: []TypedValue{
				TextVal("type_b"), IntVal(2),
			}},
		},
	}
	if checkConflict(ta, tb) {
		t.Error("disjoint predicates on same table must not conflict")
	}
}

// No conflict: writes to a different table than any observation.
func TestConflictDifferentTable(t *testing.T) {
	ta := &txn{
		name: "A",
		observations: []observation{
			{table: "users", pred: PTrue()},
		},
		writes: []RowWrite{
			{Table: "users", Kind: WriteInsert, Cols: []TypedValue{
				IntVal(1),
			}},
		},
	}

	tb := &txn{
		name: "B",
		observations: []observation{
			{table: "logs", pred: PTrue()},
		},
		writes: []RowWrite{
			{Table: "logs", Kind: WriteInsert, Cols: []TypedValue{
				TextVal("boot"),
			}},
		},
	}
	if checkConflict(ta, tb) {
		t.Error("observations on different tables must not conflict")
	}
}

// Conflict via full-scan: A's PTrue() observation on a table is a
// "table lock" — any concurrent write to the table triggers it.
func TestConflictFullScanTableLock(t *testing.T) {
	ta := &txn{
		name: "A",
		observations: []observation{
			{table: "users", pred: PTrue()},
		},
		writes: []RowWrite{
			{Table: "logs", Kind: WriteInsert, Cols: []TypedValue{TextVal("x")}},
		},
	}

	tb := &txn{
		name: "B",
		writes: []RowWrite{
			{Table: "users", Kind: WriteInsert, Cols: []TypedValue{
				IntVal(99), TextVal("late_joiner"),
			}},
		},
	}
	if !checkConflict(ta, tb) {
		t.Error("full-scan observation must conflict with any write to that table")
	}
}

// Multi-statement txn: A reads `type=A` (obs1) and then `age > 50`
// (obs2) on the same table. B's single write type=A satisfies obs1
// only. Conflict via obs1.
func TestConflictMultipleObservations(t *testing.T) {
	ta := &txn{
		name: "A",
		observations: []observation{
			{table: "people", pred: PCmp(0, OpEq, TextVal("A"))},
			{table: "people", pred: PCmp(1, OpGt, IntVal(50))},
		},
	}

	tb := &txn{
		name: "B",
		writes: []RowWrite{
			{Table: "people", Kind: WriteInsert, Cols: []TypedValue{
				TextVal("A"), IntVal(30), // age doesn't hit obs2
			}},
		},
	}
	if !checkConflict(ta, tb) {
		t.Error("write matching one of A's observations must conflict")
	}
}

// Three-way: A, B, C all concurrent. A writes something B observes;
// B writes something C observes; C writes nothing either cares about.
// Pairwise: (A,B) conflict, (B,C) conflict, (A,C) no conflict.
func TestConflictPairwiseThreeTxns(t *testing.T) {
	ta := &txn{
		name: "A",
		writes: []RowWrite{
			{Table: "t", Kind: WriteInsert, Cols: []TypedValue{
				TextVal("alpha"),
			}},
		},
	}
	tb := &txn{
		name: "B",
		observations: []observation{
			{table: "t", pred: PCmp(0, OpEq, TextVal("alpha"))},
		},
		writes: []RowWrite{
			{Table: "u", Kind: WriteInsert, Cols: []TypedValue{
				TextVal("beta"),
			}},
		},
	}
	tc := &txn{
		name: "C",
		observations: []observation{
			{table: "u", pred: PCmp(0, OpEq, TextVal("beta"))},
		},
		writes: []RowWrite{
			{Table: "v", Kind: WriteInsert, Cols: []TypedValue{
				TextVal("gamma"),
			}},
		},
	}

	if !checkConflict(ta, tb) {
		t.Error("(A,B) must conflict")
	}

	if !checkConflict(tb, tc) {
		t.Error("(B,C) must conflict")
	}

	if checkConflict(ta, tc) {
		t.Error("(A,C) must not conflict — writes hit different tables and no overlapping obs")
	}
}
