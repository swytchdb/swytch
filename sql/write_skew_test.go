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
	"strings"
	"sync"
	"testing"
)

// TestConcurrentWriteSkewMeetingRoom: two sessions both check that
// room='X' is free, then both insert. Exactly one should commit;
// the other must abort with a serialization error.
func TestConcurrentWriteSkewMeetingRoom(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(4)

	if _, err := db.Exec(
		"CREATE TABLE meetings (id INTEGER PRIMARY KEY, room TEXT, slot INTEGER, owner TEXT)"); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Three-phase barrier: both sessions must complete BEGIN+SELECT
	// before either INSERTs, and both must complete INSERT before
	// either COMMITs. Without the post-INSERT rendezvous the
	// scheduler can let one side fully commit before the other
	// emits its row-write, collapsing the scenario into a
	// sequential one (not a concurrent write-skew) and
	// under-testing the fork-choice path. Both INSERTs must
	// coexist as pending effects in the log before the COMMIT race.
	var bothRead, bothInsert, bothCommit sync.WaitGroup
	bothRead.Add(2)
	bothInsert.Add(2)
	bothCommit.Add(1)

	run := func(id int, owner string) error {
		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()

		if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
			return err
		}

		var count int
		if err := conn.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM meetings WHERE room = 'X' AND slot = 5").Scan(&count); err != nil {
			_, _ = conn.ExecContext(ctx, "ROLLBACK")

			return err
		}

		bothRead.Done()
		bothRead.Wait()

		if count != 0 {
			_, _ = conn.ExecContext(ctx, "ROLLBACK")

			return nil
		}

		insert := fmt.Sprintf(
			"INSERT INTO meetings VALUES (%d, 'X', 5, '%s')", id, owner)
		if _, err := conn.ExecContext(ctx, insert); err != nil {
			_, _ = conn.ExecContext(ctx, "ROLLBACK")

			return err
		}

		bothInsert.Done()
		bothCommit.Wait()

		_, err = conn.ExecContext(ctx, "COMMIT")

		return err
	}

	results := make([]error, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	for i := range 2 {
		go func(idx int) {
			defer wg.Done()

			id := idx + 1
			owner := "owner" + string(rune('A'+idx))
			results[idx] = run(id, owner)
		}(i)
	}

	bothInsert.Wait()
	bothCommit.Done()
	wg.Wait()

	var succeeded, serialization int

	for _, err := range results {
		switch {
		case err == nil:
			succeeded++
		case strings.Contains(err.Error(), "serialization"):
			serialization++
		default:
			t.Errorf("unexpected error: %v", err)
		}
	}

	t.Logf("write-skew outcome: %d committed, %d serialization-aborted",
		succeeded, serialization)

	// Write-skew is caught iff at most one tx commits. If both
	// commit, the observation mechanism failed to detect the
	// concurrent book-the-same-slot case.
	if succeeded > 1 {
		t.Errorf("write-skew violation: %d txs committed, want at most 1", succeeded)
	}

	if succeeded == 0 {
		t.Errorf("both txs aborted — expected exactly one to commit")
	}
}

// TestPhantomInsertAbortsObserver verifies the sequential-commit
// phantom read is caught by fork-choice. The observer's SELECT emits
// an observation at read time (not commit time), so the observation
// pins on pre-writer tips. The writer's commit lands a
// RowWriteEffect on s/<table> that satisfies the observation's
// predicate. At commit time, fork-choice finds shared base between
// the observer's bind and the writer's bind and aborts one.
//
// Previously skipped per a documented gap — observations were
// deferred to commit time, so their causal base reflected
// commit-time tips (post-writer) instead of read-time tips (pre-
// writer). No shared base, no conflict check fired. Now observations
// emit at read time in vtab.go Filter.
func TestPhantomInsertAbortsObserver(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(4)

	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, role TEXT)"); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	observer, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = observer.Close() }()

	mustExecCtx(t, observer, ctx, "BEGIN")

	var count int
	if err := observer.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM t WHERE role = 'admin'").Scan(&count); err != nil {
		t.Fatalf("observer select: %v", err)
	}

	if count != 0 {
		t.Fatalf("observer initial count = %d, want 0", count)
	}

	if _, err := db.Exec("INSERT INTO t VALUES (1, 'admin')"); err != nil {
		t.Fatalf("writer insert: %v", err)
	}

	_, commitErr := observer.ExecContext(ctx, "COMMIT")
	if commitErr == nil {
		t.Errorf("observer commit succeeded, expected phantom-read abort")
	} else if !strings.Contains(commitErr.Error(), "serialization") {
		t.Errorf("observer commit failed with unexpected error: %v", commitErr)
	}
}

// TestConcurrentDisjointWritesBothCommit: two sessions that observe
// and update disjoint rows should NOT conflict. Today's pre-step-4
// fork-choice may over-abort because both binds touch s/meetings.
func TestConcurrentDisjointWritesBothCommit(t *testing.T) {
	t.Skip("skipped pending fork-choice fix (pre-step-4 over-aborts)")
	_, db := startTestServer(t)
	db.SetMaxOpenConns(4)

	if _, err := db.Exec(
		"CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("INSERT INTO t VALUES (1, 10), (2, 20)"); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	var bothRead, bothCommit sync.WaitGroup
	bothRead.Add(2)
	bothCommit.Add(1)

	run := func(id int, newVal int) error {
		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = conn.Close() }()

		if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
			return err
		}

		var v int

		sel := fmt.Sprintf("SELECT v FROM t WHERE id = %d", id)
		if err := conn.QueryRowContext(ctx, sel).Scan(&v); err != nil {
			_, _ = conn.ExecContext(ctx, "ROLLBACK")

			return err
		}

		bothRead.Done()
		bothCommit.Wait()

		upd := fmt.Sprintf("UPDATE t SET v = %d WHERE id = %d", newVal, id)
		if _, err := conn.ExecContext(ctx, upd); err != nil {
			_, _ = conn.ExecContext(ctx, "ROLLBACK")

			return err
		}

		_, err = conn.ExecContext(ctx, "COMMIT")

		return err
	}

	results := make([]error, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	ids := []int{1, 2}
	newVals := []int{100, 200}

	for i := range 2 {
		go func(idx int) {
			defer wg.Done()

			results[idx] = run(ids[idx], newVals[idx])
		}(i)
	}

	bothRead.Wait()
	bothCommit.Done()
	wg.Wait()

	for idx, err := range results {
		if err != nil {
			t.Errorf("disjoint update %d failed: %v", idx, err)
		}
	}
}

// TestNonRepeatableReadAborts is the jepsen :internal pattern:
// observer SELECTs key k, a concurrent writer commits an INSERT on
// k, observer SELECTs k again (sees the new row), observer commits.
// With read-time observations, the first SELECT emits an observation
// on s/t pinned on pre-writer tips. Writer commits a RowWriteEffect
// on s/t that satisfies the predicate. At observer's commit,
// fork-choice finds shared base + predicate match and aborts the
// observer. Observer must NOT see :ok after having been shown an
// intervening row — either it aborts, or the second read matches
// the first.
func TestNonRepeatableReadAborts(t *testing.T) {
	_, db := startTestServer(t)
	db.SetMaxOpenConns(4)

	if _, err := db.Exec(
		"CREATE TABLE t (k INTEGER, v INTEGER, PRIMARY KEY (k, v))"); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	observer, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	defer func() { _ = observer.Close() }()

	mustExecCtx(t, observer, ctx, "BEGIN")

	// Observer's first read: k=4 is empty.
	rows, err := observer.QueryContext(ctx,
		"SELECT v FROM t WHERE k = 4 ORDER BY v")
	if err != nil {
		t.Fatalf("observer read 1: %v", err)
	}

	var firstRead []int

	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}

		firstRead = append(firstRead, v)
	}

	_ = rows.Close()

	if len(firstRead) != 0 {
		t.Fatalf("first read = %v, want []", firstRead)
	}

	// Concurrent writer commits a row at k=4 between the observer's
	// two reads — this is the phantom.
	if _, err := db.Exec("INSERT INTO t (k, v) VALUES (4, 2407)"); err != nil {
		t.Fatalf("writer insert: %v", err)
	}

	// Observer's second read on k=4. With read-time observations the
	// observation chain is tied to pre-writer tips; the writer's
	// RowWriteEffect on s/t must cause the observer's commit to abort.
	// SQL reads are NOT snapshot-isolated (by design — we rely on
	// fork-choice to abort), so this read may see [2407]. That's fine
	// as long as the commit aborts.
	rows2, err := observer.QueryContext(ctx,
		"SELECT v FROM t WHERE k = 4 ORDER BY v")
	if err != nil {
		t.Fatalf("observer read 2: %v", err)
	}

	var secondRead []int

	for rows2.Next() {
		var v int
		if err := rows2.Scan(&v); err != nil {
			t.Fatal(err)
		}

		secondRead = append(secondRead, v)
	}

	_ = rows2.Close()

	_, commitErr := observer.ExecContext(ctx, "COMMIT")

	// Two acceptable outcomes:
	//   1. Commit aborts with a serialization failure (preferred —
	//      matches the fork-choice intent of killing the non-
	//      repeatable-read tx before it returns :ok).
	//   2. The second read matched the first (no phantom actually
	//      observed). Unlikely here because the writer explicitly
	//      inserted and committed.
	if commitErr == nil {
		if len(secondRead) != len(firstRead) {
			t.Errorf("observer committed with non-repeatable read: first=%v, second=%v",
				firstRead, secondRead)
		}
	} else if !strings.Contains(commitErr.Error(), "serialization") {
		t.Errorf("observer commit failed with unexpected error: %v", commitErr)
	}
}
