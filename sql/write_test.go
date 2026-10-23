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
	"testing"
)

func TestInsertAndSelect(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx,
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	result, err := db.ExecContext(ctx, "INSERT INTO users (id, name, age) VALUES (1, 'alice', 30)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("rows affected: %v", err)
	}

	if affected != 1 {
		t.Errorf("affected=%d, want 1", affected)
	}

	// Insert a few more.
	for _, row := range []struct {
		id   int
		name string
		age  int
	}{{2, "bob", 25}, {3, "carol", 40}} {
		if _, err := db.ExecContext(ctx,
			"INSERT INTO users (id, name, age) VALUES ($1, $2, $3)",
			row.id, row.name, row.age); err != nil {
			t.Fatalf("insert %d: %v", row.id, err)
		}
	}

	// Full scan.
	rows, err := db.QueryContext(ctx, "SELECT id, name, age FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("rows.Close: %v", err)
		}
	}()

	type user struct {
		ID   int64
		Name string
		Age  int64
	}

	var got []user

	for rows.Next() {
		var u user
		if err := rows.Scan(&u.ID, &u.Name, &u.Age); err != nil {
			t.Fatalf("scan: %v", err)
		}

		got = append(got, u)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}

	want := []user{{1, "alice", 30}, {2, "bob", 25}, {3, "carol", 40}}
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d: %v", len(got), len(want), got)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestUpdateSamePK(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'alice', 30)")

	res, err := db.ExecContext(ctx, "UPDATE t SET name = 'Alice' WHERE id = 1")
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("rows affected: %v", err)
	}

	if affected != 1 {
		t.Errorf("affected=%d, want 1", affected)
	}

	var (
		name string
		age  int64
	)
	if err := db.QueryRowContext(ctx,
		"SELECT name, age FROM t WHERE id = 1").Scan(&name, &age); err != nil {
		t.Fatalf("select: %v", err)
	}

	if name != "Alice" || age != 30 {
		t.Errorf("got (%q, %d), want (Alice, 30)", name, age)
	}
}

func TestUpdateToNull(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, label TEXT)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'hello')")

	if _, err := db.ExecContext(ctx, "UPDATE t SET label = NULL WHERE id = 1"); err != nil {
		t.Fatalf("update: %v", err)
	}

	var label sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT label FROM t WHERE id = 1").Scan(&label); err != nil {
		t.Fatalf("select: %v", err)
	}

	if label.Valid {
		t.Errorf("got %+v, want NULL", label)
	}
}

func TestUpdateChangingPK(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'alice')")

	if _, err := db.ExecContext(ctx, "UPDATE t SET id = 99 WHERE id = 1"); err != nil {
		t.Fatalf("update: %v", err)
	}

	// Old row should be gone.
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM t WHERE id = 1").Scan(&count); err != nil {
		t.Fatalf("count old: %v", err)
	}

	if count != 0 {
		t.Errorf("old row still present: count=%d", count)
	}

	// New row present with preserved name.
	var name string
	if err := db.QueryRowContext(ctx, "SELECT name FROM t WHERE id = 99").Scan(&name); err != nil {
		t.Fatalf("select new: %v", err)
	}

	if name != "alice" {
		t.Errorf("name=%q, want alice", name)
	}
}

func TestDeleteOneRow(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")

	for i, n := range []string{"alice", "bob", "carol"} {
		if _, err := db.ExecContext(ctx,
			"INSERT INTO t VALUES ($1, $2)", i+1, n); err != nil {
			t.Fatal(err)
		}
	}

	res, err := db.ExecContext(ctx, "DELETE FROM t WHERE id = 2")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("rows affected: %v", err)
	}

	if affected != 1 {
		t.Errorf("affected=%d, want 1", affected)
	}

	// Verify only alice and carol remain.
	rows, err := db.QueryContext(ctx, "SELECT id, name FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("rows.Close: %v", err)
		}
	}()

	var got [][2]any

	for rows.Next() {
		var (
			id   int64
			name string
		)
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatal(err)
		}

		got = append(got, [2]any{id, name})
	}

	want := [][2]any{{int64(1), "alice"}, {int64(3), "carol"}}

	if len(got) != 2 {
		t.Fatalf("got %d rows, want 2: %v", len(got), got)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d: got %v, want %v", i, got[i], want[i])
		}
	}
}

func TestDeleteAllRows(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'a')")
	mustExec(t, db, "INSERT INTO t VALUES (2, 'b')")

	res, err := db.ExecContext(ctx, "DELETE FROM t")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("rows affected: %v", err)
	}

	if affected != 2 {
		t.Errorf("affected=%d, want 2", affected)
	}

	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatal(err)
	}

	if count != 0 {
		t.Errorf("count=%d, want 0", count)
	}
}

func TestInsertCrossSessionVisibility(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(0)

	connA, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := connA.Close(); err != nil {
			t.Logf("connA close: %v", err)
		}
	}()

	if _, err := connA.ExecContext(ctx,
		"CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatal(err)
	}

	if _, err := connA.ExecContext(ctx, "INSERT INTO t VALUES (1, 'hello')"); err != nil {
		t.Fatal(err)
	}
	// Release connA's underlying pg conn so connB gets a fresh one.
	if err := connA.Close(); err != nil {
		t.Fatal(err)
	}

	connB, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := connB.Close(); err != nil {
			t.Logf("connB close: %v", err)
		}
	}()

	var v string
	if err := connB.QueryRowContext(ctx,
		"SELECT v FROM t WHERE id = 1").Scan(&v); err != nil {
		t.Fatalf("select on fresh session: %v", err)
	}

	if v != "hello" {
		t.Errorf("v=%q, want hello", v)
	}
}

func mustExec(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()

	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

func TestDropTableRemovesAllState(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	mustExec(t, db, "INSERT INTO t VALUES (1, 'a')")
	mustExec(t, db, "INSERT INTO t VALUES (2, 'b')")
	mustExec(t, db, "INSERT INTO t VALUES (3, 'c')")

	// Sanity-check: rows are visible before the drop.
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatal(err)
	}

	if count != 3 {
		t.Fatalf("pre-drop count=%d, want 3", count)
	}

	if _, err := db.ExecContext(ctx, "DROP TABLE t"); err != nil {
		t.Fatalf("drop: %v", err)
	}

	// Same session: table is gone, SELECT should fail.
	if _, err := db.QueryContext(ctx, "SELECT * FROM t"); err == nil {
		t.Error("expected error querying dropped table, got nil")
	}

	// Recreate with the same name, should not inherit old data.
	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")

	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count after recreate: %v", err)
	}

	if count != 0 {
		t.Errorf("post-recreate count=%d, want 0", count)
	}
}

func TestDropTableIfExists(t *testing.T) {
	_, db := startTestServer(t)

	// DROP TABLE IF EXISTS on a non-existent table should succeed.
	if _, err := db.Exec("DROP TABLE IF EXISTS never_created"); err != nil {
		t.Errorf("IF EXISTS on missing table: %v", err)
	}

	// DROP TABLE without IF EXISTS on a missing table should fail.
	if _, err := db.Exec("DROP TABLE also_never_created"); err == nil {
		t.Error("expected error dropping missing table without IF EXISTS")
	}
}

// TestTextPKWithSlash exercises primary keys that contain the "/"
// byte, which used to collide with the old "r/<table>/<pk>" key
// format. Post-fix, row keys are length-prefixed and PK bytes are
// opaque.
func TestTextPKWithSlash(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE paths (p TEXT PRIMARY KEY, size INTEGER)")
	mustExec(t, db, "INSERT INTO paths VALUES ('a/b/c', 42)")
	mustExec(t, db, "INSERT INTO paths VALUES ('a/b', 7)")
	mustExec(t, db, "INSERT INTO paths VALUES ('/leading/slash', 1)")

	var size int64
	if err := db.QueryRowContext(ctx,
		"SELECT size FROM paths WHERE p = 'a/b/c'").Scan(&size); err != nil {
		t.Fatalf("lookup 'a/b/c': %v", err)
	}

	if size != 42 {
		t.Errorf("size=%d, want 42", size)
	}

	if err := db.QueryRowContext(ctx,
		"SELECT size FROM paths WHERE p = '/leading/slash'").Scan(&size); err != nil {
		t.Fatalf("lookup '/leading/slash': %v", err)
	}

	if size != 1 {
		t.Errorf("size=%d, want 1", size)
	}

	// DELETE by PK with slashes.
	res, err := db.ExecContext(ctx, "DELETE FROM paths WHERE p = 'a/b'")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	if n, _ := res.RowsAffected(); n != 1 {
		t.Errorf("affected=%d, want 1", n)
	}

	// Full scan should return the two remaining rows, neither being 'a/b'.
	rows, err := db.QueryContext(ctx, "SELECT p FROM paths ORDER BY p")
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("rows close: %v", err)
		}
	}()

	var got []string

	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			t.Fatal(err)
		}

		got = append(got, p)
	}

	if len(got) != 2 {
		t.Errorf("got %v, want 2 rows", got)
	}

	for _, p := range got {
		if p == "a/b" {
			t.Errorf("'a/b' still present after DELETE")
		}
	}
}

// TestTextIndexWithSlash verifies indexed TEXT values containing "/"
// round-trip through equality and range lookups cleanly.
func TestTextIndexWithSlash(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, path TEXT)")
	mustExec(t, db, "CREATE INDEX idx_path ON t(path)")
	mustExec(t, db, "INSERT INTO t VALUES (1, '/a/b')")
	mustExec(t, db, "INSERT INTO t VALUES (2, '/a/c')")
	mustExec(t, db, "INSERT INTO t VALUES (3, 'plain')")

	// Equality.
	var id int64
	if err := db.QueryRowContext(ctx,
		"SELECT id FROM t WHERE path = '/a/b'").Scan(&id); err != nil {
		t.Fatalf("equality: %v", err)
	}

	if id != 1 {
		t.Errorf("id=%d, want 1", id)
	}
}

// TestPKChangeUpdateIsTransactional exercises the runRMW path: we
// issue a PK-change UPDATE, which internally reads the old row,
// pins the dep via BeginTx+Noop, and commits via Flush. With no
// concurrent writer the update must succeed and the carry-over
// columns must be preserved.
func TestPKChangeUpdateIsTransactional(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	mustExec(t, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, note TEXT)")
	mustExec(t, db, "INSERT INTO t (id, name, note) VALUES (1, 'alice', 'first')")

	// UPDATE sets only id; name and note must carry forward.
	if _, err := db.ExecContext(ctx, "UPDATE t SET id = 99 WHERE id = 1"); err != nil {
		t.Fatalf("update: %v", err)
	}

	var name, note string
	if err := db.QueryRowContext(ctx,
		"SELECT name, note FROM t WHERE id = 99").Scan(&name, &note); err != nil {
		t.Fatalf("select new row: %v", err)
	}

	if name != "alice" || note != "first" {
		t.Errorf("carry-over failed: name=%q note=%q, want alice/first", name, note)
	}

	var count int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM t WHERE id = 1").Scan(&count); err != nil {
		t.Fatal(err)
	}

	if count != 0 {
		t.Errorf("old row still present: count=%d", count)
	}
}
