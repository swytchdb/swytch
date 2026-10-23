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

// TestLiveIndexPropagation: session A opens and starts issuing
// queries; session B then creates an index on the same table.
// Session A's next query must pick up the new index in its plan,
// without reconnecting. Pre-refresh behaviour kept v.schema cached
// at Connect time and would have missed the index until reconnect.
func TestLiveIndexPropagation(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(
		"INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')"); err != nil {
		t.Fatal(err)
	}

	// Session A: open and pin via SetMaxOpenConns so db.Exec
	// (session B) opens a separate conn without blocking.
	db.SetMaxOpenConns(4)

	sessA, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = sessA.Close() }()

	var count int
	if err := sessA.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("session A first query: %v", err)
	}

	if count != 3 {
		t.Fatalf("session A sees %d rows, want 3", count)
	}

	// Session B: create an index on 'name'.
	if _, err := db.Exec("CREATE INDEX idx_name ON t(name)"); err != nil {
		t.Fatalf("create index: %v", err)
	}

	// Session A: EXPLAIN QUERY PLAN should now show the index being
	// used for a name-equality lookup — without reconnecting.
	rows, err := sessA.QueryContext(ctx,
		"EXPLAIN QUERY PLAN SELECT id FROM t WHERE name = 'alice'")
	if err != nil {
		t.Fatalf("session A explain: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var planText strings.Builder

	for rows.Next() {
		var (
			id, parent, notused int
			detail              string
		)
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatalf("scan: %v", err)
		}

		planText.WriteString(detail)
		planText.WriteString(" | ")
	}

	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	plan := planText.String()
	t.Logf("session A post-CREATE-INDEX plan: %s", plan)

	if !strings.Contains(plan, "idx_name") {
		t.Errorf("session A's plan should reference idx_name; got %q", plan)
	}
}
