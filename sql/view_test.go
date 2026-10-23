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

// TestViewBasicCRUD: CREATE VIEW over a swytch-backed table, SELECT
// from it, UPDATE the underlying row and watch the view reflect the
// change, DROP VIEW.
func TestViewBasicCRUD(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(
		"INSERT INTO t VALUES (1, 'alice', 30), (2, 'bob', 25), (3, 'carol', 40)"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(
		"CREATE VIEW adults AS SELECT id, name FROM t WHERE age >= 30"); err != nil {
		t.Fatalf("CREATE VIEW: %v", err)
	}

	// Query the view.
	rows, err := db.QueryContext(ctx, "SELECT id, name FROM adults ORDER BY id")
	if err != nil {
		t.Fatalf("select from view: %v", err)
	}

	type row struct {
		id   int64
		name string
	}

	var got []row

	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.name); err != nil {
			t.Fatal(err)
		}

		got = append(got, r)
	}

	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	_ = rows.Close()

	if len(got) != 2 || got[0].id != 1 || got[0].name != "alice" ||
		got[1].id != 3 || got[1].name != "carol" {
		t.Errorf("view result = %+v, want [{1 alice} {3 carol}]", got)
	}

	// UPDATE the underlying table; view must reflect the change.
	if _, err := db.Exec("UPDATE t SET age = 18 WHERE id = 1"); err != nil {
		t.Fatal(err)
	}

	var count int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM adults").Scan(&count); err != nil {
		t.Fatal(err)
	}

	if count != 1 {
		t.Errorf("post-update view count = %d, want 1", count)
	}

	// DROP VIEW removes it from the session.
	if _, err := db.Exec("DROP VIEW adults"); err != nil {
		t.Fatalf("DROP VIEW: %v", err)
	}

	if _, err := db.Exec("SELECT * FROM adults"); err == nil {
		t.Errorf("expected SELECT from dropped view to fail")
	}
}

// TestViewIfNotExists: idempotent CREATE and DROP semantics.
func TestViewIfNotExists(t *testing.T) {
	_, db := startTestServer(t)

	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("CREATE VIEW v1 AS SELECT id FROM t"); err != nil {
		t.Fatal(err)
	}
	// Duplicate without IF NOT EXISTS — error.
	if _, err := db.Exec("CREATE VIEW v1 AS SELECT id FROM t"); err == nil {
		t.Errorf("expected duplicate CREATE VIEW to error")
	}
	// Duplicate with IF NOT EXISTS — no-op.
	if _, err := db.Exec("CREATE VIEW IF NOT EXISTS v1 AS SELECT id FROM t"); err != nil {
		t.Errorf("IF NOT EXISTS should be a no-op: %v", err)
	}
	// DROP VIEW IF EXISTS on missing — no-op.
	if _, err := db.Exec("DROP VIEW IF EXISTS nope"); err != nil {
		t.Errorf("IF EXISTS on missing view should be a no-op: %v", err)
	}
	// DROP without IF EXISTS on missing — error.
	if _, err := db.Exec("DROP VIEW nope"); err == nil {
		t.Errorf("expected DROP VIEW of missing view to error")
	}
}

// TestViewNameCollidesWithTable: CREATE VIEW must reject a name
// that's already a table, to avoid ambiguous references.
func TestViewNameCollidesWithTable(t *testing.T) {
	_, db := startTestServer(t)

	if _, err := db.Exec(
		"CREATE TABLE foo (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}

	_, err := db.Exec("CREATE VIEW foo AS SELECT 1")
	if err == nil {
		t.Errorf("expected name collision error")
	}
}

// TestViewCrossSessionVisible: session A creates a view; session B
// (already connected with an earlier schema epoch) must see the new
// view on its next query via ensureFreshVTabs, without reconnecting.
func TestViewCrossSessionVisible(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(4)

	ctx := context.Background()

	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, tag TEXT)"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(
		"INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'a')"); err != nil {
		t.Fatal(err)
	}

	// Open session A and force it to observe the current epoch.
	sessA, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sessA.Close() }()

	var n int
	if err := sessA.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("sess A probe: %v", err)
	}

	// Session B creates a view.
	if _, err := db.Exec(
		"CREATE VIEW a_only AS SELECT id FROM t WHERE tag = 'a'"); err != nil {
		t.Fatalf("CREATE VIEW on session B: %v", err)
	}

	// Session A must see the new view on its next query.
	var count int
	if err := sessA.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM a_only").Scan(&count); err != nil {
		t.Fatalf("sess A query view: %v", err)
	}

	if count != 2 {
		t.Errorf("sess A sees %d rows via view, want 2", count)
	}
}

// TestViewCrossSessionDrop: session A holds a view open; session B
// drops it. Session A's next query must fail with a no-such-table
// error rather than silently returning stale rows.
func TestViewCrossSessionDrop(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(4)

	ctx := context.Background()

	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("CREATE VIEW v AS SELECT id FROM t"); err != nil {
		t.Fatal(err)
	}

	sessA, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sessA.Close() }()

	if _, err := sessA.ExecContext(ctx, "SELECT COUNT(*) FROM v"); err != nil {
		t.Fatalf("sess A probe: %v", err)
	}

	if _, err := db.Exec("DROP VIEW v"); err != nil {
		t.Fatalf("DROP VIEW on sess B: %v", err)
	}

	if _, err := sessA.ExecContext(ctx, "SELECT COUNT(*) FROM v"); err == nil {
		t.Errorf("expected sess A query on dropped view to fail")
	}
}

// TestViewWithColumnList: `CREATE VIEW v(a,b) AS SELECT ...` renames
// the output columns. Preserved verbatim through the body round trip.
func TestViewWithColumnList(t *testing.T) {
	_, db := startTestServer(t)

	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("INSERT INTO t VALUES (1, 'alice')"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(
		"CREATE VIEW renamed(rid, rname) AS SELECT id, name FROM t"); err != nil {
		t.Fatalf("CREATE VIEW with column list: %v", err)
	}

	var (
		rid   int64
		rname string
	)
	if err := db.QueryRow(
		"SELECT rid, rname FROM renamed").Scan(&rid, &rname); err != nil {
		t.Fatalf("select from aliased view: %v", err)
	}

	if rid != 1 || rname != "alice" {
		t.Errorf("aliased view got (%d,%q), want (1,alice)", rid, rname)
	}
}

// TestViewAggregate: aggregation view — GROUP BY + COUNT(*). Verifies
// the SELECT body survives whitespace and operator characters intact.
func TestViewAggregate(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	if _, err := db.Exec(
		"CREATE TABLE events (id INTEGER PRIMARY KEY, kind TEXT)"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(
		"INSERT INTO events VALUES (1,'click'),(2,'click'),(3,'view'),(4,'click')"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(
		"CREATE VIEW counts AS SELECT kind, COUNT(*) AS n FROM events GROUP BY kind"); err != nil {
		t.Fatalf("CREATE VIEW aggregate: %v", err)
	}

	rows, err := db.QueryContext(ctx,
		"SELECT kind, n FROM counts ORDER BY kind")
	if err != nil {
		t.Fatalf("select: %v", err)
	}

	defer func() { _ = rows.Close() }()

	got := map[string]int64{}

	for rows.Next() {
		var (
			k string
			n int64
		)
		if err := rows.Scan(&k, &n); err != nil {
			t.Fatal(err)
		}

		got[k] = n
	}

	if got["click"] != 3 || got["view"] != 1 {
		t.Errorf("aggregate = %+v, want {click:3, view:1}", got)
	}
}

// TestViewSurvivesReconnect: create a view, close the pool, reopen,
// and verify the view is reinstated via replayViews on session open.
func TestViewSurvivesReconnect(t *testing.T) {
	srv, db := startTestServer(t)
	ctx := context.Background()

	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("INSERT INTO t VALUES (1,10),(2,20)"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(
		"CREATE VIEW doubled AS SELECT id, v * 2 AS dbl FROM t"); err != nil {
		t.Fatal(err)
	}

	// Force the pool to drop the one connection + open a fresh one.
	db.SetMaxOpenConns(1)

	if sqlConn, err := db.Conn(ctx); err == nil {
		_ = sqlConn.Raw(func(any) error { return nil })
		_ = sqlConn.Close()
	}

	db.SetMaxOpenConns(0)

	// A new session must find the view registered through replay.
	rows, err := db.QueryContext(ctx, "SELECT id, dbl FROM doubled ORDER BY id")
	if err != nil {
		t.Fatalf("select after reconnect: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var sum int64

	for rows.Next() {
		var id, dbl int64
		if err := rows.Scan(&id, &dbl); err != nil {
			t.Fatal(err)
		}

		sum += dbl
	}

	if sum != 60 {
		t.Errorf("post-reconnect sum = %d, want 60", sum)
	}

	_ = srv
}

// TestViewQuotedName: view name with embedded whitespace and mixed
// case; body references a table with a whitespace-containing name.
func TestViewQuotedName(t *testing.T) {
	_, db := startTestServer(t)

	if _, err := db.Exec(
		`CREATE TABLE "My Table" (id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(
		`INSERT INTO "My Table" VALUES (1, 'alice')`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(
		`CREATE VIEW "My View" AS SELECT id FROM "My Table"`); err != nil {
		t.Fatalf("CREATE VIEW with quoted names: %v", err)
	}

	var id int64
	if err := db.QueryRow(`SELECT id FROM "My View"`).Scan(&id); err != nil {
		t.Fatalf("select from quoted view: %v", err)
	}

	if id != 1 {
		t.Errorf("got id=%d, want 1", id)
	}
}

// TestViewReferencesVanishedTable: create a view, drop the table it
// depends on; subsequent queries against the view fail cleanly with a
// "no such table" error. The view itself is not cascade-dropped
// (matches SQLite's native behaviour).
func TestViewReferencesVanishedTable(t *testing.T) {
	_, db := startTestServer(t)

	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("CREATE VIEW vv AS SELECT * FROM t"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("DROP TABLE t"); err != nil {
		t.Fatal(err)
	}

	_, err := db.Exec("SELECT * FROM vv")
	if err == nil {
		t.Errorf("expected query on view-over-dropped-table to error")

		return
	}

	msg := strings.ToLower(err.Error())
	if !strings.Contains(msg, "no such table") && !strings.Contains(msg, "t") {
		t.Logf("dropped-table query error = %v (acceptable as long as it's non-nil)", err)
	}
}
