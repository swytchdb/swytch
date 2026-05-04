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

// TestSavepointRollbackDiscardsInnerWrites: BEGIN, insert A,
// SAVEPOINT, insert B, ROLLBACK TO — only A survives after COMMIT.
func TestSavepointRollbackDiscardsInnerWrites(t *testing.T) {
	// Use the existing transaction-test helper: gives us *sql.Conn
	// directly with a clean error path.
	_, db := startTestServer(t)
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Logf("conn close: %v", err)
		}
	}()

	mustExecCtx(t, conn, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	mustExecCtx(t, conn, ctx, "BEGIN")
	mustExecCtx(t, conn, ctx, "INSERT INTO t VALUES (1, 'outer')")
	mustExecCtx(t, conn, ctx, "SAVEPOINT sp1")
	mustExecCtx(t, conn, ctx, "INSERT INTO t VALUES (2, 'inner')")

	// Before rollback: both visible in the tx.
	var count int
	if err := conn.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count pre-rollback: %v", err)
	}
	if count != 2 {
		t.Errorf("pre-rollback count=%d, want 2", count)
	}

	// Roll back to savepoint.
	mustExecCtx(t, conn, ctx, "ROLLBACK TO SAVEPOINT sp1")

	// After rollback to sp1: only the outer row remains visible in the tx.
	if err := conn.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count post-rollback: %v", err)
	}
	if count != 1 {
		t.Errorf("post-rollback count=%d, want 1", count)
	}
	var v string
	if err := conn.QueryRowContext(ctx,
		"SELECT v FROM t WHERE id = 1").Scan(&v); err != nil {
		t.Fatalf("select outer: %v", err)
	}
	if v != "outer" {
		t.Errorf("outer v=%q, want outer", v)
	}

	mustExecCtx(t, conn, ctx, "COMMIT")

	// After commit: only the outer row is durable.
	if err := conn.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count after commit: %v", err)
	}
	if count != 1 {
		t.Errorf("after-commit count=%d, want 1", count)
	}
}

// TestSavepointReleaseKeepsInnerWrites: RELEASE SAVEPOINT commits
// the inner writes into the outer tx.
func TestSavepointReleaseKeepsInnerWrites(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Logf("conn close: %v", err)
		}
	}()

	mustExecCtx(t, conn, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	mustExecCtx(t, conn, ctx, "BEGIN")
	mustExecCtx(t, conn, ctx, "INSERT INTO t VALUES (1)")
	mustExecCtx(t, conn, ctx, "SAVEPOINT sp1")
	mustExecCtx(t, conn, ctx, "INSERT INTO t VALUES (2)")
	mustExecCtx(t, conn, ctx, "RELEASE SAVEPOINT sp1")
	mustExecCtx(t, conn, ctx, "COMMIT")

	var count int
	if err := conn.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Errorf("count=%d, want 2", count)
	}
}

// TestNestedSavepoints: three levels. Rolling back to the outer
// throws away both inner levels.
func TestNestedSavepoints(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Logf("conn close: %v", err)
		}
	}()

	mustExecCtx(t, conn, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	mustExecCtx(t, conn, ctx, "BEGIN")

	mustExecCtx(t, conn, ctx, "INSERT INTO t VALUES (1)")
	mustExecCtx(t, conn, ctx, "SAVEPOINT sp_outer")
	mustExecCtx(t, conn, ctx, "INSERT INTO t VALUES (2)")
	mustExecCtx(t, conn, ctx, "SAVEPOINT sp_mid")
	mustExecCtx(t, conn, ctx, "INSERT INTO t VALUES (3)")
	mustExecCtx(t, conn, ctx, "SAVEPOINT sp_inner")
	mustExecCtx(t, conn, ctx, "INSERT INTO t VALUES (4)")

	// Roll back to sp_outer: discards 2, 3, 4 (and releases the
	// inner savepoints).
	mustExecCtx(t, conn, ctx, "ROLLBACK TO SAVEPOINT sp_outer")

	var count int
	if err := conn.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("count after outer rollback=%d, want 1", count)
	}

	// sp_mid and sp_inner should be gone.
	if err := conn.Close(); err != nil {
		t.Logf("close: %v", err)
	}
}

// TestRollbackToRepeatedly: same savepoint name can be rolled back
// to multiple times.
func TestRollbackToRepeatedly(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Logf("conn close: %v", err)
		}
	}()

	mustExecCtx(t, conn, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	mustExecCtx(t, conn, ctx, "BEGIN")
	mustExecCtx(t, conn, ctx, "INSERT INTO t VALUES (1)")
	mustExecCtx(t, conn, ctx, "SAVEPOINT sp")

	for range 3 {
		mustExecCtx(t, conn, ctx, "INSERT INTO t VALUES (999)")
		mustExecCtx(t, conn, ctx, "ROLLBACK TO SAVEPOINT sp")
	}

	mustExecCtx(t, conn, ctx, "COMMIT")

	var count int
	if err := conn.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("count=%d, want 1", count)
	}
}

// TestSavepointOutsideTransaction: SAVEPOINT / RELEASE / ROLLBACK
// TO without an open transaction all return clean errors.
func TestSavepointOutsideTransaction(t *testing.T) {
	_, db := startTestServer(t)
	for _, stmt := range []string{
		"SAVEPOINT sp",
		"RELEASE SAVEPOINT sp",
		"ROLLBACK TO SAVEPOINT sp",
	} {
		_, err := db.Exec(stmt)
		if err == nil {
			t.Errorf("%q: expected error outside tx", stmt)
			continue
		}
		if !strings.Contains(err.Error(), "transaction") {
			t.Errorf("%q: err=%v, want mention of transaction", stmt, err)
		}
	}
}

// TestRollbackToUnknownSavepoint: referencing a savepoint that was
// never declared returns a clean error.
func TestRollbackToUnknownSavepoint(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Logf("conn close: %v", err)
		}
	}()

	mustExecCtx(t, conn, ctx, "BEGIN")
	if _, err := conn.ExecContext(ctx, "ROLLBACK TO SAVEPOINT nope"); err == nil {
		t.Error("expected error for unknown savepoint")
	}
	mustExecCtx(t, conn, ctx, "ROLLBACK")
}
