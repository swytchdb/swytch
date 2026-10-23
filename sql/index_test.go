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
)

func TestCreateIndexRoundTrip(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")

	for i, u := range []struct {
		id   int
		name string
		age  int
	}{{1, "alice", 30}, {2, "bob", 25}, {3, "carol", 40}, {4, "dave", 30}} {
		_ = i

		mustExec(t, db, "INSERT INTO users VALUES ($1, $2, $3)", u.id, u.name, u.age)
	}

	// Create an index AFTER data is already present; the build-scan
	// path must pick up existing rows.
	if _, err := db.ExecContext(ctx, "CREATE INDEX idx_age ON users(age)"); err != nil {
		t.Fatalf("create index: %v", err)
	}

	// Equality via index — EXPLAIN QUERY PLAN should mention the
	// table (SQLite abstracts plans for vtabs but still names them).
	rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN SELECT name FROM users WHERE age = 30")
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("rows close: %v", err)
		}
	}()

	// Scan results to drain.
	for rows.Next() {
		cols, _ := rows.Columns()
		vals := make([]any, len(cols))

		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
	}

	// Actual query result: should return alice and dave.
	qrows, err := db.QueryContext(ctx, "SELECT name FROM users WHERE age = 30 ORDER BY name")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	defer func() {
		if err := qrows.Close(); err != nil {
			t.Errorf("qrows close: %v", err)
		}
	}()

	var got []string

	for qrows.Next() {
		var n string
		if err := qrows.Scan(&n); err != nil {
			t.Fatal(err)
		}

		got = append(got, n)
	}

	if len(got) != 2 || got[0] != "alice" || got[1] != "dave" {
		t.Errorf("got %v, want [alice dave]", got)
	}
}

func TestUniqueIndexRejectsDuplicate(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")
	mustExec(t, db, "CREATE UNIQUE INDEX idx_email ON users(email)")

	mustExec(t, db, "INSERT INTO users VALUES (1, 'a@example.com')")

	// Duplicate email on a different PK must fail.
	_, err := db.ExecContext(ctx, "INSERT INTO users VALUES (2, 'a@example.com')")
	if err == nil {
		t.Fatal("expected unique violation, got nil")
	}

	if !strings.Contains(err.Error(), "unique") {
		t.Errorf("expected 'unique' in error, got: %v", err)
	}
}

func TestUniqueIndexAllowsUpdateToSameValue(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")
	mustExec(t, db, "CREATE UNIQUE INDEX idx_email ON users(email)")
	mustExec(t, db, "INSERT INTO users VALUES (1, 'a@example.com')")

	// Re-UPDATE with the same value must NOT raise a uniqueness
	// error — the conflicting entry is self.
	if _, err := db.ExecContext(ctx,
		"UPDATE users SET email = 'a@example.com' WHERE id = 1"); err != nil {
		t.Fatalf("update same-value: %v", err)
	}
}

func TestUniqueIndexRejectsUpdateCollision(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)")
	mustExec(t, db, "CREATE UNIQUE INDEX idx_email ON users(email)")
	mustExec(t, db, "INSERT INTO users VALUES (1, 'a@example.com')")
	mustExec(t, db, "INSERT INTO users VALUES (2, 'b@example.com')")

	// UPDATE id=2 to collide with id=1's email.
	_, err := db.ExecContext(ctx, "UPDATE users SET email = 'a@example.com' WHERE id = 2")
	if err == nil {
		t.Fatal("expected unique violation on update collision")
	}

	if !strings.Contains(err.Error(), "unique") {
		t.Errorf("expected 'unique' in error, got: %v", err)
	}
}

func TestCreateIndexRejectsExistingCollision(t *testing.T) {
	_, db := startTestServer(t)

	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'x')")
	mustExec(t, db, "INSERT INTO t VALUES (2, 'x')")

	// Attempting CREATE UNIQUE INDEX on an already-colliding column
	// must fail and not leave a partial index around.
	_, err := db.Exec("CREATE UNIQUE INDEX idx_v ON t(v)")
	if err == nil {
		t.Fatal("expected CREATE UNIQUE INDEX to fail on collision")
	}

	if !strings.Contains(err.Error(), "unique") {
		t.Errorf("expected 'unique' in error, got: %v", err)
	}

	// Non-unique index on the same column should work.
	mustExec(t, db, "CREATE INDEX idx_v2 ON t(v)")
}

func TestIndexedRangeScan(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE nums (id INTEGER PRIMARY KEY, x INTEGER)")

	for i := 1; i <= 10; i++ {
		mustExec(t, db, "INSERT INTO nums VALUES ($1, $2)", i, i*10)
	}

	mustExec(t, db, "CREATE INDEX idx_x ON nums(x)")

	// x > 30 AND x <= 70 should return 40, 50, 60, 70.
	rows, err := db.QueryContext(ctx,
		"SELECT x FROM nums WHERE x > 30 AND x <= 70 ORDER BY x")
	if err != nil {
		t.Fatalf("range select: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("rows close: %v", err)
		}
	}()

	var got []int64

	for rows.Next() {
		var x int64
		if err := rows.Scan(&x); err != nil {
			t.Fatal(err)
		}

		got = append(got, x)
	}

	want := []int64{40, 50, 60, 70}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d: got %d, want %d", i, got[i], want[i])
		}
	}
}

func TestDropIndex(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	mustExec(t, db, "CREATE UNIQUE INDEX idx_v ON t(v)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'alice')")

	if _, err := db.ExecContext(ctx, "DROP INDEX idx_v"); err != nil {
		t.Fatalf("drop index: %v", err)
	}

	// After drop, uniqueness constraint no longer applies.
	mustExec(t, db, "INSERT INTO t VALUES (2, 'alice')")

	// IF EXISTS on absent index must succeed.
	if _, err := db.ExecContext(ctx, "DROP INDEX IF EXISTS idx_v"); err != nil {
		t.Errorf("IF EXISTS: %v", err)
	}
	// Without IF EXISTS, dropping an absent index fails.
	if _, err := db.ExecContext(ctx, "DROP INDEX idx_v"); err == nil {
		t.Error("expected error dropping absent index")
	}
}

func TestDropTableCascadesIndices(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	mustExec(t, db, "CREATE UNIQUE INDEX idx_v ON t(v)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'x')")
	mustExec(t, db, "DROP TABLE t")

	// Recreate the table and the index with the same name — should
	// succeed because DROP TABLE took the index with it.
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	mustExec(t, db, "CREATE UNIQUE INDEX idx_v ON t(v)")

	// And the unique index is fresh: old row doesn't shadow new inserts.
	mustExec(t, db, "INSERT INTO t VALUES (1, 'x')")

	// IF EXISTS against the now-absent original should also be fine.
	if _, err := db.ExecContext(ctx, "DROP INDEX IF EXISTS some_other"); err != nil {
		t.Errorf("IF EXISTS: %v", err)
	}
}

func TestIndexDeleteRemovesEntries(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	mustExec(t, db, "CREATE UNIQUE INDEX idx_v ON t(v)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'alice')")
	mustExec(t, db, "DELETE FROM t WHERE id = 1")

	// After DELETE, a new row with the same unique value must be allowed.
	if _, err := db.ExecContext(ctx, "INSERT INTO t VALUES (2, 'alice')"); err != nil {
		t.Errorf("re-insert after delete: %v", err)
	}
}

func TestIndexUpdateMovesEntry(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, age INTEGER)")
	mustExec(t, db, "CREATE INDEX idx_age ON t(age)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 30)")

	// Query via index finds the row at age=30.
	var id int64
	if err := db.QueryRowContext(ctx, "SELECT id FROM t WHERE age = 30").Scan(&id); err != nil {
		t.Fatalf("initial find: %v", err)
	}

	// UPDATE age to 40.
	if _, err := db.ExecContext(ctx, "UPDATE t SET age = 40 WHERE id = 1"); err != nil {
		t.Fatalf("update: %v", err)
	}

	// Old age value must no longer find the row.
	err := db.QueryRowContext(ctx, "SELECT id FROM t WHERE age = 30").Scan(&id)
	if err == nil {
		t.Error("expected no row at age=30 after update")
	}

	// New age value finds it.
	if err := db.QueryRowContext(ctx, "SELECT id FROM t WHERE age = 40").Scan(&id); err != nil {
		t.Fatalf("find at age=40: %v", err)
	}

	if id != 1 {
		t.Errorf("id=%d, want 1", id)
	}
}
