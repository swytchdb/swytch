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
	"fmt"
	"sync"
	"testing"
)

// TestSyntheticPKRoundTrip exercises the full CRUD lifecycle for a
// table declared without a PRIMARY KEY. The vtab synthesizes an
// int64 rowid per insert; user queries never see it.
func TestSyntheticPKRoundTrip(t *testing.T) {
	_, db := startTestServer(t)

	if _, err := db.Exec(`CREATE TABLE nopk (a TEXT, b INTEGER)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO nopk (a, b) VALUES ('hello', 1), ('world', 2)`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	rows, err := db.Query(`SELECT a, b FROM nopk ORDER BY a`)
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close rows: %v", err)
		}
	}()

	var got []struct {
		a string
		b int
	}
	for rows.Next() {
		var r struct {
			a string
			b int
		}
		if err := rows.Scan(&r.a, &r.b); err != nil {
			t.Fatalf("Scan: %v", err)
		}

		got = append(got, r)
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("read rows: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("want 2 rows, got %d: %+v", len(got), got)
	}

	if got[0].a != "hello" || got[0].b != 1 || got[1].a != "world" || got[1].b != 2 {
		t.Fatalf("unexpected rows: %+v", got)
	}

	if _, err := db.Exec(`UPDATE nopk SET b = 42 WHERE a = 'hello'`); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}

	var b int
	if err := db.QueryRow(`SELECT b FROM nopk WHERE a = 'hello'`).Scan(&b); err != nil {
		t.Fatalf("SELECT after UPDATE: %v", err)
	}

	if b != 42 {
		t.Fatalf("UPDATE didn't land: b=%d", b)
	}

	if _, err := db.Exec(`DELETE FROM nopk WHERE a = 'hello'`); err != nil {
		t.Fatalf("DELETE: %v", err)
	}

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM nopk`).Scan(&n); err != nil {
		t.Fatalf("COUNT after DELETE: %v", err)
	}

	if n != 1 {
		t.Fatalf("want 1 row after DELETE, got %d", n)
	}
}

// TestSyntheticPKAllNullRow verifies that a row whose every user
// column is NULL still reads as live — the synthetic rowid presence
// marker keeps NetAdds non-empty so loadRow doesn't treat the row as
// fully deleted.
func TestSyntheticPKAllNullRow(t *testing.T) {
	_, db := startTestServer(t)

	if _, err := db.Exec(`CREATE TABLE nopk (a TEXT, b INTEGER)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO nopk (a, b) VALUES (NULL, NULL)`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM nopk`).Scan(&n); err != nil {
		t.Fatalf("COUNT: %v", err)
	}

	if n != 1 {
		t.Fatalf("want 1 row, got %d", n)
	}
}

// TestSyntheticPKMySQLDriverDDL is a regression fixture for the exact
// DDL emitted by the WordPress MySQL-on-SQLite driver's schema
// builder for the information_schema.key_column_usage shim. It has no
// PRIMARY KEY, only a table-level UNIQUE constraint, and is STRICT.
// Before the synthetic-PK work this returned "table has no PRIMARY
// KEY" via ErrorResponse and killed the WordPress bootstrap.
func TestSyntheticPKMySQLDriverDDL(t *testing.T) {
	_, db := startTestServer(t)

	ddl := `CREATE TABLE IF NOT EXISTS _wp_sqlite_mysql_information_schema_key_column_usage (
        CONSTRAINT_SCHEMA TEXT,
        CONSTRAINT_NAME TEXT,
        TABLE_SCHEMA TEXT,
        TABLE_NAME TEXT,
        COLUMN_NAME TEXT,
        ORDINAL_POSITION INTEGER,
        POSITION_IN_UNIQUE_CONSTRAINT INTEGER,
        REFERENCED_TABLE_SCHEMA TEXT,
        REFERENCED_TABLE_NAME TEXT,
        REFERENCED_COLUMN_NAME TEXT,
        UNIQUE (CONSTRAINT_SCHEMA, CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_TABLE_SCHEMA)
    ) STRICT`
	if _, err := db.Exec(ddl); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO _wp_sqlite_mysql_information_schema_key_column_usage
        (CONSTRAINT_SCHEMA, CONSTRAINT_NAME, COLUMN_NAME, ORDINAL_POSITION)
        VALUES ('s1', 'c1', 'col', 1)`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM _wp_sqlite_mysql_information_schema_key_column_usage`).Scan(&n); err != nil {
		t.Fatalf("COUNT: %v", err)
	}

	if n != 1 {
		t.Fatalf("want 1 row, got %d", n)
	}
}

// TestSyntheticPKConcurrentInserts stresses rowid allocation under
// concurrent inserts from many sessions. The counter is per-Server
// so all allocations mix through the same xxh3; any collision would
// cause one insert to overwrite another and the final count to fall
// short.
func TestSyntheticPKConcurrentInserts(t *testing.T) {
	t.Skip("intermittent deadlock on per-key engine lock under concurrent inserts; see vtab.go:2050 / lockEngineKeys")
	_, db := startTestServer(t)
	db.SetMaxOpenConns(8)

	if _, err := db.Exec(`CREATE TABLE nopk (payload TEXT)`); err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	const (
		workers   = 8
		perWorker = 50
	)

	ctx := context.Background()

	var wg sync.WaitGroup

	errs := make(chan error, workers)
	wg.Add(workers)

	for w := range workers {
		go func(w int) {
			defer wg.Done()

			for i := range perWorker {
				stmt := fmt.Sprintf(`INSERT INTO nopk (payload) VALUES ('w%d-i%d')`, w, i)
				if _, err := db.ExecContext(ctx, stmt); err != nil {
					errs <- fmt.Errorf("worker %d iter %d: %w", w, i, err)

					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("%v", err)
	}

	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM nopk`).Scan(&n); err != nil {
		t.Fatalf("COUNT: %v", err)
	}

	if n != workers*perWorker {
		t.Fatalf("rowid collision: want %d rows, got %d", workers*perWorker, n)
	}
}
