package sql

import (
	"testing"

	"zombiezen.com/go/sqlite"
)

// TestProbeAutoincrement is a one-shot probe (not an assertion) so we
// can read what SQLite + our parser actually do with AUTOINCREMENT.
func TestProbeAutoincrement(t *testing.T) {
	conn, err := sqlite.OpenConn(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close SQLite connection: %v", err)
		}
	})

	if err := execSimple(conn, `CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`); err != nil {
		t.Logf("native CREATE: %v", err)
	} else {
		t.Logf("native CREATE: ok")
	}

	stmt, _, err := conn.PrepareTransient(
		`SELECT cid, name, type, "notnull", pk FROM pragma_table_info('t')`)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := stmt.Finalize(); err != nil {
			t.Errorf("finalize table info query: %v", err)
		}
	})

	for {
		has, err := stmt.Step()
		if err != nil {
			t.Fatal(err)
		}

		if !has {
			break
		}

		t.Logf("pragma col cid=%d name=%s type=%s notnull=%d pk=%d",
			stmt.ColumnInt64(0), stmt.ColumnText(1), stmt.ColumnText(2),
			stmt.ColumnInt64(3), stmt.ColumnInt64(4))
	}

	stmt2, _, err := conn.PrepareTransient(`SELECT sql FROM sqlite_master WHERE name = 't'`)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := stmt2.Finalize(); err != nil {
			t.Errorf("finalize schema query: %v", err)
		}
	})

	for {
		has, err := stmt2.Step()
		if err != nil {
			t.Fatal(err)
		}

		if !has {
			break
		}

		t.Logf("sqlite_master sql: %s", stmt2.ColumnText(0))
	}

	if err := execSimple(conn, `INSERT INTO t (v) VALUES ('a')`); err != nil {
		t.Logf("insert: %v", err)
	} else {
		t.Logf("insert ok, last_id = %d", conn.LastInsertRowID())
	}

	if err := execSimple(conn, `CREATE VIRTUAL TABLE vt USING dummy(id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`); err != nil {
		t.Logf("vtab CREATE (no module registered): %v", err)
	}

	srv, _ := startTestServer(t)

	schema, err := srv.parseCreateTable(`CREATE TABLE u (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`)
	if err != nil {
		t.Logf("swytch parseCreateTable: %v", err)

		return
	}

	t.Logf("swytch schema: %d cols pk=%v synthetic=%v",
		len(schema.Columns), schema.PKColumns, schema.SyntheticPK)

	for i, c := range schema.Columns {
		t.Logf("  col[%d] name=%s aff=%s declType=%q notnull=%v isPK=%v",
			i, c.Name, c.Affinity, c.DeclType, c.NotNull, c.IsPK)
	}

	// And test whether INSERT-without-PK works on a swytch table
	// declared with AUTOINCREMENT.
	_, db := startTestServer(t)
	if _, err := db.Exec(`CREATE TABLE ai (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`); err != nil {
		t.Logf("CREATE TABLE ai: %v", err)

		return
	}

	if _, err := db.Exec(`INSERT INTO ai (v) VALUES ('hello')`); err != nil {
		t.Logf("INSERT without id: %v", err)
	} else {
		var (
			id int
			v  string
		)
		if err := db.QueryRow(`SELECT id, v FROM ai`).Scan(&id, &v); err != nil {
			t.Logf("SELECT: %v", err)
		} else {
			t.Logf("INSERT auto-allocated id=%d v=%s", id, v)
		}
	}
}
