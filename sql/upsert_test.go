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

import "testing"

// TestUpsertOnConflictDoUpdate is the WordPress MySQL-driver pattern:
// INSERT ... ON CONFLICT(col) DO UPDATE SET col = literal.
func TestUpsertOnConflictDoUpdate(t *testing.T) {
	t.Skip("skipped pending fix")
	_, db := startTestServer(t)

	if _, err := db.Exec(
		`CREATE TABLE _wp_sqlite_global_variables (name TEXT PRIMARY KEY, value TEXT)`,
	); err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO _wp_sqlite_global_variables (name, value) VALUES ('k', 'v1')
		ON CONFLICT(name) DO UPDATE SET value = 'v2'`); err != nil {
		t.Fatalf("first upsert (insert branch): %v", err)
	}

	var got string
	if err := db.QueryRow(`SELECT value FROM _wp_sqlite_global_variables WHERE name = 'k'`).Scan(&got); err != nil {
		t.Fatalf("SELECT after insert: %v", err)
	}

	if got != "v1" {
		t.Fatalf("after insert branch: want 'v1', got %q", got)
	}

	if _, err := db.Exec(`INSERT INTO _wp_sqlite_global_variables (name, value) VALUES ('k', 'v3')
		ON CONFLICT(name) DO UPDATE SET value = 'v4'`); err != nil {
		t.Fatalf("second upsert (update branch): %v", err)
	}

	if err := db.QueryRow(`SELECT value FROM _wp_sqlite_global_variables WHERE name = 'k'`).Scan(&got); err != nil {
		t.Fatalf("SELECT after update: %v", err)
	}

	if got != "v4" {
		t.Fatalf("after update branch: want 'v4', got %q", got)
	}
}
