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
	"strconv"
	"testing"

	pb "github.com/swytchdb/cache/cluster/proto"
	"github.com/swytchdb/cache/effects"
	"zombiezen.com/go/sqlite"
)

// seedRow emits a row into swytch without going through the vtab.
// Used by tests to pre-populate data so we can exercise the read path
// independently of a (not yet built) insert path.
//
// pkValue is the user-visible PK value (int or string); seedRow
// applies the same order-preserving binary encoding the write path
// would have produced so reads find the row.
func seedRow(t *testing.T, eng *effects.Engine, table string, pkAffinity string, pkValue any, fields map[string]any) {
	t.Helper()
	ectx := eng.NewContext()
	schema, err := loadSchema(ectx, table)
	if err != nil {
		t.Fatalf("seedRow loadSchema %q: %v", table, err)
	}
	var pkEncoded string
	switch v := pkValue.(type) {
	case int:
		pkEncoded = string(encodeIndexValue(sqlite.IntegerValue(int64(v)), pkAffinity))
	case int64:
		pkEncoded = string(encodeIndexValue(sqlite.IntegerValue(v), pkAffinity))
	case float64:
		pkEncoded = string(encodeIndexValue(sqlite.FloatValue(v), pkAffinity))
	case string:
		if pkAffinity == affinityText {
			pkEncoded = v
		} else {
			t.Fatalf("seedRow: string pk with non-text affinity %q", pkAffinity)
		}
	default:
		t.Fatalf("seedRow: unsupported pk type %T", pkValue)
	}
	for field, val := range fields {
		eff := &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(field),
		}
		switch v := val.(type) {
		case int:
			eff.Value = &pb.DataEffect_IntVal{IntVal: int64(v)}
		case int64:
			eff.Value = &pb.DataEffect_IntVal{IntVal: v}
		case float64:
			eff.Value = &pb.DataEffect_FloatVal{FloatVal: v}
		case string:
			eff.Value = &pb.DataEffect_Raw{Raw: []byte(v)}
		case []byte:
			eff.Value = &pb.DataEffect_Raw{Raw: v}
		default:
			t.Fatalf("seedRow: unsupported value type %T", val)
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(rowKey(schema.TableID, pkEncoded)),
			Kind: &pb.Effect_Data{Data: eff},
		}); err != nil {
			t.Fatalf("seedRow emit: %v", err)
		}
	}
	if err := ectx.Flush(); err != nil {
		t.Fatalf("seedRow flush: %v", err)
	}
}

func TestVTabFullScan(t *testing.T) {
	srv, db := startTestServer(t)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx,
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Seed 3 rows directly via effects.
	seedRow(t, srv.Engine(), "users", affinityInteger, 1, map[string]any{"name": "alice", "age": int64(30)})
	seedRow(t, srv.Engine(), "users", affinityInteger, 2, map[string]any{"name": "bob", "age": int64(25)})
	seedRow(t, srv.Engine(), "users", affinityInteger, 3, map[string]any{"name": "carol", "age": int64(40)})

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
			t.Errorf("row %d: got %+v want %+v", i, got[i], want[i])
		}
	}
}

func TestVTabPKEqualityLookup(t *testing.T) {
	srv, db := startTestServer(t)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx,
		"CREATE TABLE widgets (id INTEGER PRIMARY KEY, label TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 1; i <= 5; i++ {
		seedRow(t, srv.Engine(), "widgets", affinityInteger, i, map[string]any{
			"label": "w" + strconv.Itoa(i),
		})
	}

	var label string
	err := db.QueryRowContext(ctx, "SELECT label FROM widgets WHERE id = 3").Scan(&label)
	if err != nil {
		t.Fatalf("pk lookup: %v", err)
	}
	if label != "w3" {
		t.Errorf("label = %q, want w3", label)
	}

	// Missing row — no error, just no rows.
	var nothing sql.NullString
	err = db.QueryRowContext(ctx, "SELECT label FROM widgets WHERE id = 999").Scan(&nothing)
	if err != sql.ErrNoRows {
		t.Errorf("missing row: err=%v, want sql.ErrNoRows", err)
	}

	// EXPLAIN QUERY PLAN should show the PK-equality plan.
	rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN SELECT label FROM widgets WHERE id = 1")
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("rows.Close: %v", err)
		}
	}()
	found := false
	for rows.Next() {
		cols, _ := rows.Columns()
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan plan: %v", err)
		}
		for _, v := range vals {
			if s, ok := v.(string); ok && containsCaseInsensitive(s, "widgets") {
				found = true
			}
		}
	}
	if !found {
		t.Error("EXPLAIN QUERY PLAN did not reference widgets")
	}
}

func TestVTabEmptyTable(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx,
		"CREATE TABLE empties (id INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	rows, err := db.QueryContext(ctx, "SELECT id, v FROM empties")
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("rows.Close: %v", err)
		}
	}()
	if rows.Next() {
		t.Error("expected empty result")
	}
}

func TestVTabTextPK(t *testing.T) {
	srv, db := startTestServer(t)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx,
		"CREATE TABLE items (sku TEXT PRIMARY KEY, qty INTEGER)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	seedRow(t, srv.Engine(), "items", affinityText, "A1", map[string]any{"qty": int64(5)})
	seedRow(t, srv.Engine(), "items", affinityText, "B2", map[string]any{"qty": int64(10)})

	var qty int64
	if err := db.QueryRowContext(ctx, "SELECT qty FROM items WHERE sku = 'B2'").Scan(&qty); err != nil {
		t.Fatalf("pk lookup: %v", err)
	}
	if qty != 10 {
		t.Errorf("qty = %d, want 10", qty)
	}
}

func containsCaseInsensitive(s, sub string) bool {
	ss, subs := []byte(s), []byte(sub)
	for i := 0; i+len(subs) <= len(ss); i++ {
		match := true
		for j := range subs {
			a, b := ss[i+j], subs[j]
			if a >= 'A' && a <= 'Z' {
				a += 'a' - 'A'
			}
			if b >= 'A' && b <= 'Z' {
				b += 'a' - 'A'
			}
			if a != b {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
