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

// TestBeginCommitRoundTrip: BEGIN, INSERT, SELECT-from-own-tx,
// COMMIT. The row must be durable after COMMIT and visible to a
// new session.
func TestBeginCommitRoundTrip(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	// Force a single connection for the entire transaction.
	db.SetMaxOpenConns(1)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	mustExecCtx(t, conn, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")

	mustExecCtx(t, conn, ctx, "BEGIN")
	mustExecCtx(t, conn, ctx, "INSERT INTO t VALUES (1, 'alice')")
	mustExecCtx(t, conn, ctx, "INSERT INTO t VALUES (2, 'bob')")

	// Own writes must be visible inside the tx.
	var count int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count during tx: %v", err)
	}

	if count != 2 {
		t.Errorf("in-tx count=%d, want 2", count)
	}

	mustExecCtx(t, conn, ctx, "COMMIT")

	// Post-commit: same session still sees rows.
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count after commit: %v", err)
	}

	if count != 2 {
		t.Errorf("post-commit count=%d, want 2", count)
	}

	if err := conn.Close(); err != nil {
		t.Logf("conn close: %v", err)
	}

	// Fresh session sees committed rows.
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count new session: %v", err)
	}

	if count != 2 {
		t.Errorf("new-session count=%d, want 2", count)
	}
}

// TestBeginRollbackDiscards: BEGIN, INSERT, ROLLBACK. Nothing lands.
func TestBeginRollbackDiscards(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	db.SetMaxOpenConns(1)

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
	mustExecCtx(t, conn, ctx, "INSERT INTO t VALUES (1, 'discarded')")
	mustExecCtx(t, conn, ctx, "ROLLBACK")

	var count int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}

	if count != 0 {
		t.Errorf("post-rollback count=%d, want 0", count)
	}
}

// TestMultiStatementTxObservesOwnWrites: a tx with INSERT followed
// by SELECT sees the INSERT even before COMMIT.
func TestMultiStatementTxObservesOwnWrites(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	db.SetMaxOpenConns(1)

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
	mustExecCtx(t, conn, ctx, "INSERT INTO t VALUES (1, 'alice')")
	mustExecCtx(t, conn, ctx, "UPDATE t SET v = 'ALICE' WHERE id = 1")

	var v string
	if err := conn.QueryRowContext(ctx,
		"SELECT v FROM t WHERE id = 1").Scan(&v); err != nil {
		t.Fatalf("in-tx select: %v", err)
	}

	if v != "ALICE" {
		t.Errorf("in-tx v=%q, want ALICE", v)
	}

	mustExecCtx(t, conn, ctx, "ROLLBACK")

	// After rollback, nothing landed.
	var count int
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM t").Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}

	if count != 0 {
		t.Errorf("post-rollback count=%d, want 0", count)
	}
}

// TestCommitOutsideTransactionIsNoop: pg tolerates COMMIT with no
// active tx, returning COMMIT with a WARNING; we mirror that (no
// WARNING yet, just success) so stdlib clients that over-ship
// COMMIT aren't broken.
func TestCommitOutsideTransactionIsNoop(t *testing.T) {
	_, db := startTestServer(t)
	if _, err := db.Exec("COMMIT"); err != nil {
		t.Errorf("COMMIT with no tx should succeed, got: %v", err)
	}

	if _, err := db.Exec("ROLLBACK"); err != nil {
		t.Errorf("ROLLBACK with no tx should succeed, got: %v", err)
	}
}

// TestNestedBeginRejected: nested BEGIN returns an error (clients
// should use savepoints).
func TestNestedBeginRejected(t *testing.T) {
	_, db := startTestServer(t)
	ctx := context.Background()

	db.SetMaxOpenConns(1)

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// Clean up: if the nested BEGIN test leaves a tx open, close
		// unwinds it via session CloseConn → Abort.
		if err := conn.Close(); err != nil {
			t.Logf("conn close: %v", err)
		}
	}()

	mustExecCtx(t, conn, ctx, "BEGIN")

	if _, err := conn.ExecContext(ctx, "BEGIN"); err == nil {
		t.Error("nested BEGIN must return an error")
	}

	mustExecCtx(t, conn, ctx, "ROLLBACK")
}

// TestTxSurvivesCrossSession: a tx on session A doesn't affect
// session B's view until COMMIT. Sanity-check that session B sees
// pre-commit data.
func TestTxSurvivesCrossSession(t *testing.T) {
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

	mustExecCtx(t, connA, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
	mustExecCtx(t, connA, ctx, "INSERT INTO t VALUES (1, 'initial')")

	// connA opens a tx and updates.
	mustExecCtx(t, connA, ctx, "BEGIN")
	mustExecCtx(t, connA, ctx, "UPDATE t SET v = 'staged' WHERE id = 1")

	// connB should still see 'initial' because connA hasn't committed.
	// NOTE: today our effects engine's visibility rules mean a
	// different session's in-flight (unflushed) writes are NOT in
	// the reduced state. If this assumption breaks we'll revisit.
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
		t.Fatalf("connB select: %v", err)
	}

	if v != "initial" {
		t.Errorf("connB sees %q pre-commit, want initial", v)
	}

	// Commit on A; B now sees the staged value.
	mustExecCtx(t, connA, ctx, "COMMIT")

	if err := connB.QueryRowContext(ctx,
		"SELECT v FROM t WHERE id = 1").Scan(&v); err != nil {
		t.Fatalf("connB select post-commit: %v", err)
	}

	if v != "staged" {
		t.Errorf("connB sees %q post-commit, want staged", v)
	}
}

// mustExecCtx is a *sql.Conn-scoped variant of mustExec for tests
// that need to pin operations to a single session.
func mustExecCtx(t *testing.T, conn *sql.Conn, ctx context.Context, query string) {
	t.Helper()

	if _, err := conn.ExecContext(ctx, query); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}
