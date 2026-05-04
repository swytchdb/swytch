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
	"database/sql"
	"strings"
	"testing"
)

// TestAlterAddColumn: after ADD COLUMN, SELECT of the new column on
// pre-existing rows reads NULL (absent field), and new INSERTs can
// populate the column. Verifies only the issuing session — cross-
// session propagation of the new column is a documented limitation.
func TestAlterAddColumn(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(4)
	ctx := context.Background()

	if _, err := db.Exec(
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		"INSERT INTO users VALUES (1, 'alice'), (2, 'bob')"); err != nil {
		t.Fatal(err)
	}

	// Pin to one connection so ALTER + subsequent SELECTs all
	// happen on the same session (the one that redeclared the
	// vtab).
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx,
		"ALTER TABLE users ADD COLUMN age INTEGER"); err != nil {
		t.Fatalf("ALTER ADD COLUMN: %v", err)
	}

	// Existing rows should read NULL for the new column.
	var name string
	var age sql.NullInt64
	if err := conn.QueryRowContext(ctx,
		"SELECT name, age FROM users WHERE id = 1").Scan(&name, &age); err != nil {
		t.Fatalf("select post-alter: %v", err)
	}
	if name != "alice" {
		t.Errorf("name=%q, want alice", name)
	}
	if age.Valid {
		t.Errorf("age on pre-existing row should be NULL, got %d", age.Int64)
	}

	// INSERT with the new column populated.
	if _, err := conn.ExecContext(ctx,
		"INSERT INTO users VALUES (3, 'carol', 42)"); err != nil {
		t.Fatalf("insert with new column: %v", err)
	}
	var ageC sql.NullInt64
	if err := conn.QueryRowContext(ctx,
		"SELECT age FROM users WHERE id = 3").Scan(&ageC); err != nil {
		t.Fatalf("select age: %v", err)
	}
	if !ageC.Valid || ageC.Int64 != 42 {
		t.Errorf("carol.age = %v, want 42", ageC)
	}

	// UPDATE an existing row's new column.
	if _, err := conn.ExecContext(ctx,
		"UPDATE users SET age = 30 WHERE id = 1"); err != nil {
		t.Fatalf("update new column: %v", err)
	}
	var ageA sql.NullInt64
	if err := conn.QueryRowContext(ctx,
		"SELECT age FROM users WHERE id = 1").Scan(&ageA); err != nil {
		t.Fatalf("select updated age: %v", err)
	}
	if !ageA.Valid || ageA.Int64 != 30 {
		t.Errorf("alice.age post-update = %v, want 30", ageA)
	}
}

// TestAlterAddColumnNotNullRejected: NOT NULL on ADD COLUMN without
// a DEFAULT would invalidate existing rows. Reject it cleanly.
func TestAlterAddColumnNotNullRejected(t *testing.T) {
	_, db := startTestServer(t)
	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	_, err := db.Exec("ALTER TABLE t ADD COLUMN v TEXT NOT NULL")
	if err == nil {
		t.Errorf("expected error for ADD COLUMN NOT NULL without DEFAULT")
	} else if !strings.Contains(err.Error(), "NOT NULL") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestAlterAddColumnDuplicate: adding a column with the same name as
// an existing one errors.
func TestAlterAddColumnDuplicate(t *testing.T) {
	_, db := startTestServer(t)
	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatal(err)
	}
	_, err := db.Exec("ALTER TABLE t ADD COLUMN v INTEGER")
	if err == nil {
		t.Errorf("expected duplicate column error")
	} else if !strings.Contains(err.Error(), "duplicate column") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestAlterAddColumnOptionalKeyword: "ADD <col>" and "ADD COLUMN <col>"
// both work.
func TestAlterAddColumnOptionalKeyword(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(4)
	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("ALTER TABLE t ADD v TEXT"); err != nil {
		t.Fatalf("ADD (no COLUMN keyword): %v", err)
	}
}

// TestAlterAddColumnDefault: DEFAULT on an ADD COLUMN is applied to
// pre-existing rows at read time (no row rewrite) and also used when
// subsequent INSERTs omit the column.
func TestAlterAddColumnDefault(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(4)
	ctx := context.Background()

	if _, err := db.Exec(
		"CREATE TABLE u (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO u VALUES (1, 'alice')"); err != nil {
		t.Fatal(err)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx,
		"ALTER TABLE u ADD COLUMN status TEXT DEFAULT 'active'"); err != nil {
		t.Fatalf("ALTER ADD COLUMN DEFAULT: %v", err)
	}

	// Pre-existing row reads the DEFAULT.
	var status string
	if err := conn.QueryRowContext(ctx,
		"SELECT status FROM u WHERE id = 1").Scan(&status); err != nil {
		t.Fatalf("select default: %v", err)
	}
	if status != "active" {
		t.Errorf("pre-existing row status = %q, want 'active'", status)
	}

	// Explicit INSERT overrides the default.
	if _, err := conn.ExecContext(ctx,
		"INSERT INTO u VALUES (2, 'bob', 'pending')"); err != nil {
		t.Fatalf("insert with status: %v", err)
	}
	if err := conn.QueryRowContext(ctx,
		"SELECT status FROM u WHERE id = 2").Scan(&status); err != nil {
		t.Fatalf("select bob: %v", err)
	}
	if status != "pending" {
		t.Errorf("bob status = %q, want 'pending'", status)
	}
}

// TestAlterAddColumnNotNullWithDefault: NOT NULL + DEFAULT is
// accepted — the DEFAULT retroactively satisfies NOT NULL for every
// pre-existing row (at read time) and every future insert that omits
// the column.
func TestAlterAddColumnNotNullWithDefault(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(4)
	ctx := context.Background()

	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1, 'alice')"); err != nil {
		t.Fatal(err)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx,
		"ALTER TABLE t ADD COLUMN ver INTEGER NOT NULL DEFAULT 0"); err != nil {
		t.Fatalf("ALTER ADD COLUMN NOT NULL DEFAULT: %v", err)
	}

	var ver int64
	if err := conn.QueryRowContext(ctx,
		"SELECT ver FROM t WHERE id = 1").Scan(&ver); err != nil {
		t.Fatalf("select ver: %v", err)
	}
	if ver != 0 {
		t.Errorf("pre-existing ver = %d, want 0", ver)
	}
}

// TestAlterAddColumnNotNullNoDefault: NOT NULL without DEFAULT is
// rejected — there's no safe value for pre-existing rows.
func TestAlterAddColumnNotNullNoDefault(t *testing.T) {
	_, db := startTestServer(t)
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	_, err := db.Exec("ALTER TABLE t ADD COLUMN v INTEGER NOT NULL")
	if err == nil {
		t.Errorf("expected error for NOT NULL without DEFAULT")
	}
}

// TestAlterDropColumn: dropped column's field is REMOVEd on every
// row; remaining columns are untouched; subsequent queries don't
// see the dropped name. Both issuing and pre-existing sessions see
// the new shape.
func TestAlterDropColumn(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(4)
	ctx := context.Background()

	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, note TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		"INSERT INTO t VALUES (1, 'alice', 'first'), (2, 'bob', 'second')"); err != nil {
		t.Fatal(err)
	}

	// Session B open before the DROP.
	sessB, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sessB.Close() }()
	var pre string
	if err := sessB.QueryRowContext(ctx,
		"SELECT note FROM t WHERE id = 1").Scan(&pre); err != nil {
		t.Fatalf("session B pre-drop: %v", err)
	}

	// Issue the DROP through another connection.
	if _, err := db.Exec("ALTER TABLE t DROP COLUMN note"); err != nil {
		t.Fatalf("DROP COLUMN: %v", err)
	}

	// Issuing side: column is gone.
	if _, err := db.Exec("SELECT note FROM t"); err == nil {
		t.Errorf("SELECT note should error after DROP COLUMN")
	}
	// Remaining data survives.
	var name string
	if err := db.QueryRowContext(ctx, "SELECT name FROM t WHERE id = 1").Scan(&name); err != nil {
		t.Fatalf("select name: %v", err)
	}
	if name != "alice" {
		t.Errorf("alice.name = %q after drop, want 'alice'", name)
	}

	// Session B on its next query sees the new shape via ensureFreshVTabs.
	if _, err := sessB.ExecContext(ctx, "SELECT note FROM t"); err == nil {
		t.Errorf("session B SELECT note should error after DROP COLUMN")
	}
	if err := sessB.QueryRowContext(ctx,
		"SELECT name FROM t WHERE id = 2").Scan(&name); err != nil {
		t.Fatalf("session B post-drop select: %v", err)
	}
	if name != "bob" {
		t.Errorf("session B id=2 name = %q, want 'bob'", name)
	}

	// Re-ADD the same column and verify old values do NOT resurrect.
	if _, err := db.Exec(
		"ALTER TABLE t ADD COLUMN note TEXT"); err != nil {
		t.Fatalf("re-ADD COLUMN: %v", err)
	}
	var note sql.NullString
	if err := db.QueryRowContext(ctx,
		"SELECT note FROM t WHERE id = 1").Scan(&note); err != nil {
		t.Fatalf("select re-added note: %v", err)
	}
	if note.Valid {
		t.Errorf("re-added note should be NULL, got %q — dropped field not fully removed", note.String)
	}
}

// TestAlterDropColumnCascadesIndex: dropping a column that an index
// references fully tears down the index (entries + metadata).
func TestAlterDropColumnCascadesIndex(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(4)

	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		"INSERT INTO t VALUES (1, 'alice'), (2, 'bob')"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE INDEX idx_name ON t(name)"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("ALTER TABLE t DROP COLUMN name"); err != nil {
		t.Fatalf("DROP COLUMN: %v", err)
	}

	// Index should no longer be registered — DROP INDEX errors cleanly.
	if _, err := db.Exec("DROP INDEX idx_name"); err == nil {
		t.Errorf("DROP INDEX of cascade-dropped index should error")
	}

	// Recreate an index with the same name on another column — must
	// succeed (no stale metadata under the old name).
	if _, err := db.Exec(
		"ALTER TABLE t ADD COLUMN label TEXT"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE INDEX idx_name ON t(label)"); err != nil {
		t.Errorf("recreate index with cascaded name: %v", err)
	}
}

// TestAlterDropColumnRejectsPK: the PK column isn't droppable — rows
// are addressed by PK encoding, so losing it would orphan every row.
func TestAlterDropColumnRejectsPK(t *testing.T) {
	_, db := startTestServer(t)
	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatal(err)
	}
	_, err := db.Exec("ALTER TABLE t DROP COLUMN id")
	if err == nil {
		t.Errorf("expected error dropping PK column")
	} else if !strings.Contains(err.Error(), "primary key") {
		t.Errorf("unexpected error dropping PK: %v", err)
	}
}

// TestAlterDropColumnMissing: dropping a nonexistent column errors.
func TestAlterDropColumnMissing(t *testing.T) {
	_, db := startTestServer(t)
	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	_, err := db.Exec("ALTER TABLE t DROP COLUMN nope")
	if err == nil {
		t.Errorf("expected error dropping missing column")
	}
}

// TestAlterRenameTable: rows + indices survive the rename. Both the
// issuing session and a concurrent session see the table under its
// new name and keep seeing all rows. Rows live at tableID-keyed
// storage, so no data movement happens.
func TestAlterRenameTable(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(4)
	ctx := context.Background()

	if _, err := db.Exec(
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		"INSERT INTO users VALUES (1, 'alice'), (2, 'bob')"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		"CREATE INDEX idx_users_name ON users(name)"); err != nil {
		t.Fatal(err)
	}

	// Session B open before the rename.
	sessB, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sessB.Close() }()
	// Force session B's vtab cache to be populated.
	var name string
	if err := sessB.QueryRowContext(ctx,
		"SELECT name FROM users WHERE id = 1").Scan(&name); err != nil {
		t.Fatalf("session B pre-rename: %v", err)
	}

	// Rename on another session.
	if _, err := db.Exec("ALTER TABLE users RENAME TO members"); err != nil {
		t.Fatalf("RENAME TABLE: %v", err)
	}

	// Issuing session (through the pool): old name is gone, new
	// name works and returns both rows.
	if _, err := db.Exec("SELECT * FROM users"); err == nil {
		t.Errorf("SELECT on old name should error after RENAME")
	}
	var count int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM members").Scan(&count); err != nil {
		t.Fatalf("select new name: %v", err)
	}
	if count != 2 {
		t.Errorf("members row count = %d, want 2", count)
	}

	// Session B (was open before rename): sees new name on next query.
	if err := sessB.QueryRowContext(ctx,
		"SELECT name FROM members WHERE id = 2").Scan(&name); err != nil {
		t.Fatalf("session B post-rename: %v", err)
	}
	if name != "bob" {
		t.Errorf("session B sees id=2 as %q, want 'bob'", name)
	}

	// Index survives: EXPLAIN QUERY PLAN on the new name still uses it.
	rows, err := db.QueryContext(ctx,
		"EXPLAIN QUERY PLAN SELECT id FROM members WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("explain post-rename: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var detail strings.Builder
	for rows.Next() {
		var id, parent, notused int
		var d string
		if err := rows.Scan(&id, &parent, &notused, &d); err != nil {
			t.Fatal(err)
		}
		detail.WriteString(d + " | ")
	}
	if !strings.Contains(detail.String(), "idx_users_name") {
		t.Errorf("index not used post-rename: plan = %q", detail.String())
	}

	// DROP INDEX via the original name still works (x/<idx>.table
	// was rewritten by the rename).
	if _, err := db.Exec("DROP INDEX idx_users_name"); err != nil {
		t.Errorf("DROP INDEX post-rename: %v", err)
	}
}

// TestAlterRenameTableToExisting: renaming to a table name that
// already exists errors.
func TestAlterRenameTableToExisting(t *testing.T) {
	_, db := startTestServer(t)
	if _, err := db.Exec(
		"CREATE TABLE a (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		"CREATE TABLE b (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	_, err := db.Exec("ALTER TABLE a RENAME TO b")
	if err == nil {
		t.Errorf("expected error renaming to existing table name")
	}
}

// TestAlterRenameColumn: existing row values surface under the new
// column name, old name disappears; index that referenced the
// column still works under the new name.
func TestAlterRenameColumn(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(4)
	ctx := context.Background()

	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, note TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		"INSERT INTO t VALUES (1, 'alice', 'hello'), (2, 'bob', 'world')"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE INDEX idx_name ON t(name)"); err != nil {
		t.Fatal(err)
	}

	// Session B open before rename.
	sessB, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sessB.Close() }()
	var scratch string
	if err := sessB.QueryRowContext(ctx,
		"SELECT name FROM t WHERE id = 1").Scan(&scratch); err != nil {
		t.Fatalf("pre-rename select: %v", err)
	}

	if _, err := db.Exec(
		"ALTER TABLE t RENAME COLUMN name TO full_name"); err != nil {
		t.Fatalf("RENAME COLUMN: %v", err)
	}

	// Old name is gone.
	if _, err := db.Exec("SELECT name FROM t"); err == nil {
		t.Errorf("SELECT name should error after RENAME COLUMN")
	}
	// New name carries the values.
	var name string
	if err := db.QueryRowContext(ctx,
		"SELECT full_name FROM t WHERE id = 1").Scan(&name); err != nil {
		t.Fatalf("select new name: %v", err)
	}
	if name != "alice" {
		t.Errorf("full_name = %q, want 'alice'", name)
	}

	// Session B on its next query sees the new column too.
	if err := sessB.QueryRowContext(ctx,
		"SELECT full_name FROM t WHERE id = 2").Scan(&name); err != nil {
		t.Fatalf("session B post-rename select: %v", err)
	}
	if name != "bob" {
		t.Errorf("session B id=2 full_name = %q, want 'bob'", name)
	}

	// Index still works: plan references it and lookup by new name succeeds.
	rows, err := db.QueryContext(ctx,
		"EXPLAIN QUERY PLAN SELECT id FROM t WHERE full_name = 'alice'")
	if err != nil {
		t.Fatalf("explain: %v", err)
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
	if !strings.Contains(plan.String(), "idx_name") {
		t.Errorf("index should still be used post-rename: plan = %q", plan.String())
	}

	// Untouched columns survive.
	var note string
	if err := db.QueryRowContext(ctx,
		"SELECT note FROM t WHERE id = 1").Scan(&note); err != nil {
		t.Fatalf("select note: %v", err)
	}
	if note != "hello" {
		t.Errorf("note = %q, want 'hello'", note)
	}
}

// TestAlterRenameColumnMissing: renaming a column that doesn't exist
// errors.
func TestAlterRenameColumnMissing(t *testing.T) {
	_, db := startTestServer(t)
	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	_, err := db.Exec("ALTER TABLE t RENAME COLUMN nope TO other")
	if err == nil {
		t.Errorf("expected error renaming missing column")
	}
}

// TestAlterRenameColumnCollision: renaming to the name of an
// existing column errors.
func TestAlterRenameColumnCollision(t *testing.T) {
	_, db := startTestServer(t)
	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT)"); err != nil {
		t.Fatal(err)
	}
	_, err := db.Exec("ALTER TABLE t RENAME COLUMN a TO b")
	if err == nil {
		t.Errorf("expected error renaming to existing column name")
	}
}

// TestAlterAddColumnCrossSessionVisible: session B is open before
// session A issues ADD COLUMN. Session B's next query must see the
// new column — without a reconnect.
func TestAlterAddColumnCrossSessionVisible(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(4)
	ctx := context.Background()

	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO t VALUES (1, 'alice')"); err != nil {
		t.Fatal(err)
	}

	// Session B is open before the ALTER.
	sessB, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sessB.Close() }()

	// Force a query on session B so the vtab is fully Connect'd and
	// its declared column list is cached.
	var initialName string
	if err := sessB.QueryRowContext(ctx,
		"SELECT name FROM t WHERE id = 1").Scan(&initialName); err != nil {
		t.Fatalf("session B pre-ALTER select: %v", err)
	}

	// Session A issues the ALTER (through the pool).
	if _, err := db.Exec(
		"ALTER TABLE t ADD COLUMN age INTEGER DEFAULT 0"); err != nil {
		t.Fatalf("ALTER ADD COLUMN: %v", err)
	}

	// Session B selects the new column. Pre-fix this would error
	// with "no such column: age" because SQLite's declared schema
	// on session B was frozen at Connect time.
	var age int64
	if err := sessB.QueryRowContext(ctx,
		"SELECT age FROM t WHERE id = 1").Scan(&age); err != nil {
		t.Fatalf("session B post-ALTER select: %v", err)
	}
	if age != 0 {
		t.Errorf("session B sees age = %d, want 0 (DEFAULT)", age)
	}
}
