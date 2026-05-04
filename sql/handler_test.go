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
	"testing"

	"zombiezen.com/go/sqlite"
)

func TestInterceptShowSet(t *testing.T) {
	// interceptShowSet swallows every SHOW/SET query — the
	// swytch.dialect variants return our dialect-aware answers, and
	// everything else is handled as a generic stub so pgJDBC's
	// session-setup probes (SHOW TRANSACTION ISOLATION LEVEL, etc.)
	// don't blow up SQLite on an unrecognised keyword.
	tests := []struct {
		query string
		want  bool
	}{
		{"SHOW swytch.dialect", true},
		{"show swytch.dialect", true},
		{"  SHOW   swytch.dialect  ;", true},
		{"SHOW SWYTCH.DIALECT;", true},
		{"SET swytch.dialect = 'sqlite'", true},
		{"set swytch.dialect TO sqlite", true},
		{"SHOW timezone", true},
		{"SHOW TRANSACTION ISOLATION LEVEL", true},
		{"SET TIME ZONE 'UTC'", true},
		{"SHOW", false},
		{"SELECT 1", false},
		{"", false},
	}
	for _, tc := range tests {
		stmt, ok := interceptShowSet(tc.query)
		if ok != tc.want {
			t.Errorf("interceptShowSet(%q) intercepted=%v, want %v", tc.query, ok, tc.want)
		}
		if tc.want && stmt == nil {
			t.Errorf("interceptShowSet(%q) returned nil stmt", tc.query)
		}
	}
}

func TestStripPgTypeCasts(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"SELECT 1", "SELECT 1"},
		{"INSERT INTO lists (k, v) VALUES (('3'::int4), ('518'::int8))",
			"INSERT INTO lists (k, v) VALUES (('3'), ('518'))"},
		{"SELECT v FROM t WHERE k = ('0'::int4)",
			"SELECT v FROM t WHERE k = ('0')"},
		{"SELECT 'a::b'", "SELECT 'a::b'"},     // inside literal
		{"SELECT \"a::b\"", "SELECT \"a::b\""}, // inside quoted ident
		{"SELECT 1::varchar(32)", "SELECT 1"},
		{"SELECT '{1,2}'::int[]", "SELECT '{1,2}'"},
	}
	for _, tc := range cases {
		if got := stripPgTypeCasts(tc.in); got != tc.want {
			t.Errorf("stripPgTypeCasts(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestCommandTag(t *testing.T) {
	cases := map[string]string{
		"SELECT 1":                "SELECT",
		"select 1":                "SELECT",
		"   \n\t SELECT 1":        "SELECT",
		"INSERT INTO t VALUES(1)": "INSERT",
		"update t set x=1":        "UPDATE",
		"-- comment\nSELECT 1":    "SELECT",
		"":                        "",
		";":                       "",
		"(WITH x AS SELECT 1)":    "", // parens before keyword
	}
	for q, want := range cases {
		got := commandTag(q)
		if got != want {
			t.Errorf("commandTag(%q)=%q, want %q", q, got, want)
		}
	}
}

func TestFormatTag(t *testing.T) {
	cases := []struct {
		verb string
		rows uint32
		want string
	}{
		{"SELECT", 1, "SELECT 1"},
		{"SELECT", 0, "SELECT 0"},
		{"INSERT", 3, "INSERT 0 3"},
		{"UPDATE", 2, "UPDATE 2"},
		{"DELETE", 5, "DELETE 5"},
		{"CREATE", 0, "CREATE"},
		{"", 0, "OK"},
	}
	for _, tc := range cases {
		got := formatTag(tc.verb, tc.rows)
		if got != tc.want {
			t.Errorf("formatTag(%q, %d)=%q, want %q", tc.verb, tc.rows, got, tc.want)
		}
	}
}

func TestOidForSqliteType(t *testing.T) {
	cases := []struct {
		in   sqlite.ColumnType
		want uint32
	}{
		{sqlite.TypeInteger, oidInt8},
		{sqlite.TypeFloat, oidFloat8},
		{sqlite.TypeText, oidText},
		{sqlite.TypeBlob, oidBytea},
		{sqlite.TypeNull, oidText},
	}
	for _, tc := range cases {
		if got := oidForSqliteType(tc.in); got != tc.want {
			t.Errorf("oidForSqliteType(%v)=%d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestReadColumnRoundtrip(t *testing.T) {
	conn, err := sqlite.OpenConn(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("conn.Close: %v", err)
		}
	}()

	stmt, _, err := conn.PrepareTransient("SELECT 42, 3.14, 'hello', X'deadbeef', NULL")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := stmt.Finalize(); err != nil {
			t.Errorf("stmt.Finalize: %v", err)
		}
	}()

	hasRow, err := stmt.Step()
	if err != nil || !hasRow {
		t.Fatalf("step: hasRow=%v err=%v", hasRow, err)
	}

	intVal := readColumn(stmt, 0).(int64)
	if intVal != 42 {
		t.Errorf("col 0 = %d, want 42", intVal)
	}
	floatVal := readColumn(stmt, 1).(float64)
	if floatVal != 3.14 {
		t.Errorf("col 1 = %v, want 3.14", floatVal)
	}
	textVal := readColumn(stmt, 2).(string)
	if textVal != "hello" {
		t.Errorf("col 2 = %q, want %q", textVal, "hello")
	}
	blobVal := readColumn(stmt, 3).([]byte)
	if string(blobVal) != "\xde\xad\xbe\xef" {
		t.Errorf("col 3 = %x, want deadbeef", blobVal)
	}
	if readColumn(stmt, 4) != nil {
		t.Errorf("col 4 = %v, want nil", readColumn(stmt, 4))
	}
}
