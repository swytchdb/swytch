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
	"errors"
	"fmt"
	"strings"

	wire "github.com/jeroenrinzema/psql-wire"

	"github.com/swytchdb/swytch/effects"
)

// Transaction-control statements routed through this file: BEGIN,
// START TRANSACTION, COMMIT, ROLLBACK. The pg-wire client's
// isolation-level clauses (SERIALIZABLE, REPEATABLE READ, etc.) are
// parsed but have no effect: SQLite offers exactly one effective
// level, and the sub-DAG observation machinery either gives us
// serializable (once fork-choice predicate evaluation lands) or
// doesn't — there's no ladder to select from.
//
// Auto-commit statements skip this file entirely; only when a
// session has an active txContext do observations and writes share
// a Context across statements.

// maybeInterceptTransaction peeks the leading keywords. Returns
// (stmt, true, nil) when handled, (nil, false, nil) to fall through
// to DDL/DML routing.
func (s *Server) maybeInterceptTransaction(sess *session, query string) (*wire.PreparedStatement, bool, error) {
	trimmed := strings.TrimSpace(stripLeadingComments(query))
	upper := strings.ToUpper(trimmed)

	switch {
	case strings.HasPrefix(upper, "BEGIN"),
		strings.HasPrefix(upper, "START TRANSACTION"):
		return s.handleBegin(sess)
	case strings.HasPrefix(upper, "COMMIT"),
		strings.HasPrefix(upper, "END TRANSACTION"),
		upper == "END":
		return s.handleCommit(sess)
	case strings.HasPrefix(upper, "ROLLBACK TO"):
		return s.handleRollbackTo(sess, trimmed)
	case strings.HasPrefix(upper, "ROLLBACK"),
		strings.HasPrefix(upper, "ABORT"):
		return s.handleRollback(sess)
	case strings.HasPrefix(upper, "SAVEPOINT"):
		return s.handleSavepoint(sess, trimmed)
	case strings.HasPrefix(upper, "RELEASE SAVEPOINT"),
		strings.HasPrefix(upper, "RELEASE "):
		return s.handleReleaseSavepoint(sess, trimmed)
	}
	return nil, false, nil
}

// parseSavepointName extracts the savepoint identifier from a
// SAVEPOINT / RELEASE / ROLLBACK TO statement. Handles optional
// SAVEPOINT keyword in RELEASE/ROLLBACK-TO forms.
func parseSavepointName(stmt string) (string, error) {
	tokens := strings.Fields(stmt)
	if len(tokens) == 0 {
		return "", errors.New("malformed savepoint statement")
	}
	// Skip leading keywords: SAVEPOINT | RELEASE | ROLLBACK | TO | SAVEPOINT
	i := 0
	for i < len(tokens) {
		u := strings.ToUpper(tokens[i])
		if u == "SAVEPOINT" || u == "RELEASE" || u == "ROLLBACK" || u == "TO" {
			i++
			continue
		}
		break
	}
	if i >= len(tokens) {
		return "", errors.New("savepoint statement missing name")
	}
	name := strings.TrimRight(tokens[i], ";")
	// Strip double-quote delimiters on quoted identifiers.
	if len(name) >= 2 && name[0] == '"' && name[len(name)-1] == '"' {
		name = strings.ReplaceAll(name[1:len(name)-1], `""`, `"`)
	}
	if name == "" {
		return "", errors.New("savepoint statement missing name")
	}
	return name, nil
}

// handleSavepoint: `SAVEPOINT <name>`. Captures current tx state
// under name. Requires an active transaction.
func (s *Server) handleSavepoint(sess *session, stmt string) (*wire.PreparedStatement, bool, error) {
	if sess.txContext == nil {
		return nil, true, errors.New("SAVEPOINT: no transaction in progress")
	}
	name, err := parseSavepointName(stmt)
	if err != nil {
		return nil, true, err
	}
	sess.pushSavepoint(name)
	sess.log.Debug("sql savepoint", "name", name)
	fn := func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		return w.Complete("SAVEPOINT")
	}
	return wire.NewStatement(fn), true, nil
}

// handleReleaseSavepoint: `RELEASE SAVEPOINT <name>` or `RELEASE <name>`.
// Discards the frame without reverting — inner writes stay in the
// outer tx.
func (s *Server) handleReleaseSavepoint(sess *session, stmt string) (*wire.PreparedStatement, bool, error) {
	if sess.txContext == nil {
		return nil, true, errors.New("RELEASE: no transaction in progress")
	}
	name, err := parseSavepointName(stmt)
	if err != nil {
		return nil, true, err
	}
	if !sess.releaseSavepoint(name) {
		return nil, true, fmt.Errorf("savepoint %q does not exist", name)
	}
	sess.log.Debug("sql release savepoint", "name", name)
	fn := func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		return w.Complete("RELEASE")
	}
	return wire.NewStatement(fn), true, nil
}

// handleRollbackTo: `ROLLBACK TO [SAVEPOINT] <name>`. Restores the
// captured state and discards inner savepoints. The named savepoint
// itself remains; clients may ROLLBACK TO the same name repeatedly.
func (s *Server) handleRollbackTo(sess *session, stmt string) (*wire.PreparedStatement, bool, error) {
	if sess.txContext == nil {
		return nil, true, errors.New("ROLLBACK TO: no transaction in progress")
	}
	name, err := parseSavepointName(stmt)
	if err != nil {
		return nil, true, err
	}
	if !sess.rollbackToSavepoint(name) {
		return nil, true, fmt.Errorf("savepoint %q does not exist", name)
	}
	sess.log.Debug("sql rollback to savepoint", "name", name)
	fn := func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		return w.Complete("ROLLBACK")
	}
	return wire.NewStatement(fn), true, nil
}

// handleBegin opens a transactional Context on the session. All
// subsequent writes and observation emissions accumulate into this
// Context until COMMIT or ROLLBACK.
//
// Nested BEGIN is a client error (pg returns a WARNING and leaves
// the tx intact; we return an error — clients should use savepoints
// for nesting). We may relax this if a real client proves
// intolerant.
func (s *Server) handleBegin(sess *session) (*wire.PreparedStatement, bool, error) {
	if sess.txContext != nil {
		return nil, true, errors.New("BEGIN: transaction already in progress")
	}
	ectx := s.engine.NewContext()
	ectx.BeginTx()
	sess.txContext = ectx

	sess.log.Debug("sql tx begin")

	fn := func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		return w.Complete("BEGIN")
	}
	return wire.NewStatement(fn), true, nil
}

// handleCommit flushes the session's tx Context. Fork-choice at
// flush time may return ErrTxnAborted if a concurrent committer
// invalidated our reads/writes; we surface that to the client as a
// serialization failure and clear the tx state so the client can
// retry with BEGIN.
//
// Whether Flush succeeds or fails, the tx is over: we always clear
// sess.txContext and decline any further in-tx accumulation.
func (s *Server) handleCommit(sess *session) (*wire.PreparedStatement, bool, error) {
	if sess.txContext == nil {
		// pg emits a WARNING ("there is no transaction in progress")
		// and returns COMMIT; clients tolerate this. Mirror the
		// behaviour so drivers that assume pg semantics don't
		// break.
		fn := func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			return w.Complete("COMMIT")
		}
		return wire.NewStatement(fn), true, nil
	}

	ectx := sess.txContext
	sess.clearTxState()

	// Observations are emitted at read time in Filter (the tips
	// they pin on must be read-time tips, not commit-time tips, to
	// catch sequential-commit phantoms). Nothing to drain here.

	// Lock striped per-key locks across Flush so concurrent commits
	// on the same keys serialise through the fork-choice critical
	// section (see lockEngineKeys). Same pattern redis uses around
	// runner + Flush in its handler.
	unlock := lockEngineKeys(s.engine, ectx.PendingKeys())
	err := ectx.Flush()
	unlock()
	if err != nil {
		if errors.Is(err, effects.ErrTxnAborted) {
			sess.log.Debug("sql tx commit aborted by fork-choice")
			return nil, true, fmt.Errorf("serialization failure: %w", err)
		}
		return nil, true, fmt.Errorf("commit flush: %w", err)
	}
	sess.log.Debug("sql tx commit")

	fn := func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		return w.Complete("COMMIT")
	}
	return wire.NewStatement(fn), true, nil
}

// handleRollback discards the session's tx Context. Anything
// accumulated since BEGIN — writes, observations, per-column
// effects, RowWriteEffects — is dropped. Effects already written to
// the log remain there but aren't applied to the index (Abort's
// contract).
func (s *Server) handleRollback(sess *session) (*wire.PreparedStatement, bool, error) {
	if sess.txContext == nil {
		// pg emits a WARNING and returns ROLLBACK — mirror.
		fn := func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			return w.Complete("ROLLBACK")
		}
		return wire.NewStatement(fn), true, nil
	}
	sess.txContext.Abort()
	sess.clearTxState()
	sess.log.Debug("sql tx rollback")

	fn := func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		return w.Complete("ROLLBACK")
	}
	return wire.NewStatement(fn), true, nil
}
