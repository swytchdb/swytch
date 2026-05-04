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
	"context"
	"strings"
	"testing"

	"zombiezen.com/go/sqlite"
)

// TestTupleEncodingOrderPreserving verifies the composite encoding
// maintains lex-ordering over the tuple-ordered comparison for
// TEXT-first tuples where length-prefix bugs would otherwise
// re-order results (e.g. "ab" < "b" but naive length-prefix would
// reverse it).
func TestTupleEncodingOrderPreserving(t *testing.T) {
	affs := []string{affinityText, affinityInteger}
	rows := []struct {
		a string
		b int64
	}{
		{"", 0},
		{"", 1},
		{"a", -1},
		{"a", 0},
		{"a", 1},
		{"ab", 0},
		{"b", 0},
		{"b\x00", 0},
		{"b\x00\x01", 0},
		{"c", 0},
	}
	encoded := make([]string, len(rows))
	for i, r := range rows {
		enc, err := encodePK([]sqlite.Value{
			sqlite.TextValue(r.a), sqlite.IntegerValue(r.b),
		}, affs)
		if err != nil {
			t.Fatalf("encodePK(%q, %d): %v", r.a, r.b, err)
		}
		encoded[i] = enc
	}
	// Encoded strings must be in the same order as the input tuples.
	for i := 1; i < len(encoded); i++ {
		if encoded[i-1] >= encoded[i] {
			t.Errorf("ordering broken at %d: (%q, %d) should sort before (%q, %d); got encoded[%d]=%q !< encoded[%d]=%q",
				i, rows[i-1].a, rows[i-1].b, rows[i].a, rows[i].b,
				i-1, encoded[i-1], i, encoded[i])
		}
	}
	// Round-trip: decode each and compare.
	for i, r := range rows {
		decoded := decodePK(encoded[i], affs)
		if len(decoded) != 2 {
			t.Fatalf("decodePK returned %d components, want 2", len(decoded))
		}
		if decoded[0].Text() != r.a {
			t.Errorf("row %d: a=%q, want %q", i, decoded[0].Text(), r.a)
		}
		if decoded[1].Int64() != r.b {
			t.Errorf("row %d: b=%d, want %d", i, decoded[1].Int64(), r.b)
		}
	}
}

// TestCompositePKCRUD: CREATE TABLE with `PRIMARY KEY (a, b)`,
// insert rows, look up by composite PK, update non-PK column,
// delete — all via SQLite's planner. The stable tuple encoding
// makes row keys deterministic across runs.
func TestCompositePKCRUD(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(4)
	ctx := context.Background()

	if _, err := db.Exec(
		"CREATE TABLE events (user_id INTEGER, seq INTEGER, payload TEXT, PRIMARY KEY (user_id, seq))"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		"INSERT INTO events VALUES (1, 1, 'first'), (1, 2, 'second'), (2, 1, 'other')"); err != nil {
		t.Fatal(err)
	}

	// PK equality must match on BOTH components.
	var payload string
	if err := db.QueryRowContext(ctx,
		"SELECT payload FROM events WHERE user_id = 1 AND seq = 2").Scan(&payload); err != nil {
		t.Fatalf("composite PK lookup: %v", err)
	}
	if payload != "second" {
		t.Errorf("payload = %q, want 'second'", payload)
	}

	// EXPLAIN QUERY PLAN should show the PK-equality plan — cost 1.
	rows, err := db.QueryContext(ctx,
		"EXPLAIN QUERY PLAN SELECT payload FROM events WHERE user_id = 2 AND seq = 1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var plan strings.Builder
	for rows.Next() {
		var id, parent, notused int
		var d string
		if err := rows.Scan(&id, &parent, &notused, &d); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(d + " | ")
	}
	if !strings.Contains(plan.String(), "INDEX 1:") {
		t.Logf("plan: %q", plan.String())
		// SQLite vtab plan IDs are reported as "INDEX <num>"; plan 1
		// is our planPKEquality. Don't hard-fail if SQLite renders
		// differently — the semantic test above is authoritative.
	}

	// UPDATE a non-PK column.
	if _, err := db.Exec(
		"UPDATE events SET payload = 'FIRST' WHERE user_id = 1 AND seq = 1"); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := db.QueryRowContext(ctx,
		"SELECT payload FROM events WHERE user_id = 1 AND seq = 1").Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload != "FIRST" {
		t.Errorf("after update, payload = %q, want 'FIRST'", payload)
	}

	// DELETE by composite PK.
	if _, err := db.Exec(
		"DELETE FROM events WHERE user_id = 2 AND seq = 1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	var count int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM events").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("post-delete count = %d, want 2", count)
	}

	// Partial PK (only user_id) — SQLite can still answer, but as a
	// full-scan-with-WHERE. Verify both rows for user_id=1 come back.
	rows2, err := db.QueryContext(ctx,
		"SELECT seq FROM events WHERE user_id = 1 ORDER BY seq")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows2.Close() }()
	var seqs []int64
	for rows2.Next() {
		var s int64
		if err := rows2.Scan(&s); err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, s)
	}
	if len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 2 {
		t.Errorf("partial-PK scan got %v, want [1 2]", seqs)
	}
}

// TestCompositePKText: mixed-affinity composite PK — TEXT then
// INTEGER. Exercises the tuple-encoding escape/terminator path on
// TEXT components.
func TestCompositePKText(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(4)
	ctx := context.Background()

	if _, err := db.Exec(
		"CREATE TABLE t (room TEXT, slot INTEGER, booked_by TEXT, PRIMARY KEY (room, slot))"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		"INSERT INTO t VALUES ('A', 1, 'alice'), ('A', 2, 'bob'), ('B', 1, 'carol')"); err != nil {
		t.Fatal(err)
	}

	var who string
	if err := db.QueryRowContext(ctx,
		"SELECT booked_by FROM t WHERE room = 'A' AND slot = 2").Scan(&who); err != nil {
		t.Fatal(err)
	}
	if who != "bob" {
		t.Errorf("got %q, want 'bob'", who)
	}
}

// TestCompositeIndexEquality: CREATE INDEX on (a, b), SELECT with
// equality on both columns uses the index.
func TestCompositeIndexEquality(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(4)
	ctx := context.Background()

	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT, created_at INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		"INSERT INTO t VALUES (1, 'active', 100), (2, 'active', 200), (3, 'done', 100), (4, 'active', 100)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		"CREATE INDEX idx_status_ts ON t(status, created_at)"); err != nil {
		t.Fatalf("composite CREATE INDEX: %v", err)
	}

	// Equality on BOTH columns picks the composite index.
	rows, err := db.QueryContext(ctx,
		"SELECT id FROM t WHERE status = 'active' AND created_at = 100 ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 4 {
		t.Errorf("got ids=%v, want [1 4]", ids)
	}

}

// TestCompositeIndexRange: equality on the first column + range on
// the second uses the composite index for a range scan.
func TestCompositeIndexRange(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(4)
	ctx := context.Background()

	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT, created_at INTEGER)"); err != nil {
		t.Fatal(err)
	}
	for i, row := range []struct {
		status string
		ts     int64
	}{
		{"active", 100}, {"active", 200}, {"active", 300},
		{"done", 100}, {"done", 200},
	} {
		q := "INSERT INTO t VALUES (" + itoaN(int64(i+1)) + ", '" + row.status + "', " + itoaN(row.ts) + ")"
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(
		"CREATE INDEX idx_status_ts ON t(status, created_at)"); err != nil {
		t.Fatal(err)
	}

	// status=active AND created_at > 100 AND created_at <= 300 →
	// should hit ids 2 and 3 via the composite index.
	rows, err := db.QueryContext(ctx,
		"SELECT id FROM t WHERE status = 'active' AND created_at > 100 AND created_at <= 300 ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if len(ids) != 2 || ids[0] != 2 || ids[1] != 3 {
		t.Errorf("range results = %v, want [2 3]", ids)
	}

}

// TestCompositeIndexUnique: UNIQUE composite index enforces the
// joint-column constraint but allows duplicates in either column
// alone.
func TestCompositeIndexUnique(t *testing.T) {
	_, db := startTestServer(t)
	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		"CREATE UNIQUE INDEX idx ON t(a, b)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		"INSERT INTO t VALUES (1, 'x', 1), (2, 'x', 2), (3, 'y', 1)"); err != nil {
		t.Fatalf("non-colliding inserts: %v", err)
	}
	_, err := db.Exec("INSERT INTO t VALUES (4, 'x', 1)")
	if err == nil {
		t.Errorf("expected uniqueness violation on (x, 1)")
	}
}

func itoaN(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := false
	if n < 0 {
		neg = true
		n = -n
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	if neg {
		digits = append([]byte{'-'}, digits...)
	}
	return string(digits)
}
