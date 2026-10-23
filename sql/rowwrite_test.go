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
	"database/sql"
	"testing"
	"time"

	wire "github.com/jeroenrinzema/psql-wire"
	"zombiezen.com/go/sqlite"
)

// newTestVTable builds a swytchVTable wired to a Server's engine,
// with a fresh SQLite conn and a registered schema. Returns the
// vtable plus a cleanup function. This bypasses pg-wire so we can
// exercise Update/DeleteRow directly and inspect lastRowWrites.
func newTestVTable(t *testing.T, srv *Server, schema *tableSchema) (*swytchVTable, func()) {
	t.Helper()

	conn, err := sqlite.OpenConn(":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	if err := conn.SetModule(moduleName, srv.module); err != nil {
		_ = conn.Close()

		t.Fatalf("register module: %v", err)
	}
	// Persist schema via the DDL path so subsequent loadLiveSchema
	// calls inside write helpers find the right metadata.
	ectx := srv.engine.NewContext()
	if err := emitSchema(ectx, schema); err != nil {
		_ = conn.Close()

		t.Fatalf("emit schema: %v", err)
	}

	if err := ectx.Flush(); err != nil {
		_ = conn.Close()

		t.Fatalf("flush schema: %v", err)
	}

	if err := registerVTable(conn, schema); err != nil {
		_ = conn.Close()

		t.Fatalf("register vtable: %v", err)
	}
	// After CREATE VIRTUAL TABLE, SQLite has constructed a
	// swytchVTable on the conn. We can't pluck it out directly from
	// zombiezen's API, so drive writes through SQL execution and
	// expose the result via a helper that SELECTs lastRowWrites.
	// For that reason this test path is currently limited to the
	// fields we can see from outside.
	vt := &swytchVTable{
		server:    srv,
		name:      schema.Name,
		schema:    schema,
		pkByRowID: make(map[int64]string),
	}
	cleanup := func() {
		if err := conn.Close(); err != nil {
			t.Logf("conn close: %v", err)
		}
	}

	return vt, cleanup
}

// TestRowWriteCapturedOnInsert exercises doInsert directly — no
// SQLite — to verify lastRowWrites gets populated correctly.
func TestRowWriteCapturedOnInsert(t *testing.T) {
	srv, db := startTestServer(t)
	if _, err := db.Exec(
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"); err != nil {
		t.Fatal(err)
	}

	// Build a test vtab wired to the server's engine + schema.
	vt, cleanup := newTestVTable(t, srv, &tableSchema{
		Name:    "users",
		TableID: deriveTableID(time.Now(), srv.engine.NodeID(), "users"),
		Columns: []columnSchema{
			{Name: "id", Affinity: affinityInteger, IsPK: true},
			{Name: "name", Affinity: affinityText},
			{Name: "age", Affinity: affinityInteger},
		},
		PKColumns: []int{0},
	})
	defer cleanup()

	cols := []sqlite.Value{
		sqlite.IntegerValue(42),
		sqlite.TextValue("alice"),
		sqlite.IntegerValue(30),
	}
	if _, err := vt.doInsert(cols); err != nil {
		t.Fatalf("doInsert: %v", err)
	}

	if len(vt.lastRowWrites) != 1 {
		t.Fatalf("lastRowWrites len=%d, want 1", len(vt.lastRowWrites))
	}

	rw := vt.lastRowWrites[0]
	if rw.Table != "users" {
		t.Errorf("Table=%q, want users", rw.Table)
	}

	if rw.Kind != WriteInsert {
		t.Errorf("Kind=%v, want WriteInsert", rw.Kind)
	}

	if len(rw.Cols) != 3 {
		t.Fatalf("Cols len=%d, want 3", len(rw.Cols))
	}

	if rw.Cols[0].Kind != sqlite.TypeInteger || rw.Cols[0].Int != 42 {
		t.Errorf("col 0 = %+v, want IntVal(42)", rw.Cols[0])
	}

	if rw.Cols[1].Kind != sqlite.TypeText || rw.Cols[1].Text != "alice" {
		t.Errorf("col 1 = %+v, want TextVal(alice)", rw.Cols[1])
	}

	if rw.Cols[2].Kind != sqlite.TypeInteger || rw.Cols[2].Int != 30 {
		t.Errorf("col 2 = %+v, want IntVal(30)", rw.Cols[2])
	}
}

// TestRowWriteCapturedOnDelete exercises DeleteRow directly by
// first inserting a row, then issuing DeleteRow with the known
// rowid. Verifies that the RowWrite has WriteDelete kind and
// carries the pre-deletion column values.
func TestRowWriteCapturedOnDelete(t *testing.T) {
	srv, db := startTestServer(t)
	if _, err := db.Exec(
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}

	vt, cleanup := newTestVTable(t, srv, &tableSchema{
		Name:    "users",
		TableID: deriveTableID(time.Now(), srv.engine.NodeID(), "users"),
		Columns: []columnSchema{
			{Name: "id", Affinity: affinityInteger, IsPK: true},
			{Name: "name", Affinity: affinityText},
		},
		PKColumns: []int{0},
	})
	defer cleanup()

	// Insert a row, record its rowid.
	cols := []sqlite.Value{sqlite.IntegerValue(7), sqlite.TextValue("bob")}

	rowid, err := vt.doInsert(cols)
	if err != nil {
		t.Fatalf("doInsert: %v", err)
	}

	// Delete by rowid.
	if err := vt.DeleteRow(sqlite.IntegerValue(rowid)); err != nil {
		t.Fatalf("DeleteRow: %v", err)
	}

	if len(vt.lastRowWrites) != 1 {
		t.Fatalf("lastRowWrites len=%d, want 1", len(vt.lastRowWrites))
	}

	rw := vt.lastRowWrites[0]
	if rw.Kind != WriteDelete {
		t.Errorf("Kind=%v, want WriteDelete", rw.Kind)
	}

	if rw.Cols[1].Text != "bob" {
		t.Errorf("Cols[1]=%q, want bob (pre-deletion state)", rw.Cols[1].Text)
	}
}

// Ensure the wire package is linked even when subset tests run in
// isolation — newTestVTable reaches into srv.module which is built
// during NewServer.
var _ = wire.Parameters{}

// Ensure database/sql is referenced — used via startTestServer in
// the tests above.
var _ = sql.ErrNoRows
