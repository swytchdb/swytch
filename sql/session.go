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
	"log/slog"
	"slices"
	"sync"

	"github.com/swytchdb/engine/effects"
	"zombiezen.com/go/sqlite"
)

// sessionCtxKey is the typed context key under which the per-connection
// session is stored. We use our own context key (rather than
// psql-wire's SetAttribute) because SessionMiddleware runs before the
// wire.Session itself is attached to the context — SetAttribute would
// fail at that point.
type sessionCtxKey struct{}

// session holds the per-client state: an exclusive SQLite connection
// (serialised access — SQLite's Conn is single-threaded) plus a
// session-scoped logger.
//
// Close may be called from multiple goroutines — psql-wire invokes
// CloseConn when a client disconnects, and Server.Stop walks the
// sessions map during shutdown. closeOnce makes Close idempotent and
// closeErr preserves the result for the late caller.
//
// txContext is non-nil iff the session is inside a pg BEGIN/COMMIT
// block. When set, every vtab write and every observation-emitting
// read accumulates into it instead of creating its own per-statement
// Context; COMMIT calls Flush, ROLLBACK calls Abort. Because
// psql-wire serialises query execution per session, no mutex is
// needed — only the single query-handling goroutine touches this
// field.
//
// txDirtyRows tracks the set of row-key PKs the tx has touched
// (inserted, updated, deleted) per table. Full-scan reads during
// the tx take the union of keytrie-committed rows and this set,
// then call GetSnapshot through the tx Context to resolve each —
// the Context's snapshot reconstruction merges the tx's own
// pending writes, so rows inserted in-tx show up and rows deleted
// in-tx drop out, as a pg client would expect.
type session struct {
	id        uint64
	conn      *sqlite.Conn
	log       *slog.Logger
	closeOnce sync.Once
	closeErr  error

	txContext   *effects.Context
	txDirtyRows map[string]map[string]struct{}
	savepoints  []savepointFrame

	// schemaEpoch is the Server.schemaEpoch this session last
	// synced against. When it's behind the server's current epoch,
	// the handler re-enters ensureFreshVTabs to redeclare any vtabs
	// whose columns diverged from swytch's committed schema (CREATE,
	// ADD COLUMN, DROP TABLE on another session, etc.). Zero-cost
	// in steady state — a single atomic load shows "we're current".
	schemaEpoch uint64
}

// savepointFrame captures what the session needs to restore on
// ROLLBACK TO SAVEPOINT: the Context's pending-key state (which
// includes emitted observations) and the dirty-row set. SAVEPOINT
// pushes a frame; RELEASE pops one without restoring; ROLLBACK TO
// restores the named frame and pops it plus any frames above it
// (matching pg's semantics — rolling back to an outer savepoint
// releases all inner ones).
type savepointFrame struct {
	name      string
	ctxSnap   *effects.ContextSavepoint
	dirtyRows map[string]map[string]struct{}
}

// markRowDirty records that the tx has written to the given row
// key. Callers should invoke this at every INSERT, UPDATE (both
// old and new PK on a pk-change), and DELETE emission. No-op when
// no tx is active.
func (s *session) markRowDirty(table, pk string) {
	if s.txContext == nil {
		return
	}
	if s.txDirtyRows == nil {
		s.txDirtyRows = make(map[string]map[string]struct{})
	}
	set, ok := s.txDirtyRows[table]
	if !ok {
		set = make(map[string]struct{})
		s.txDirtyRows[table] = set
	}
	set[pk] = struct{}{}
}

// dirtyRowPKs returns the set of pks the tx has touched for the
// given table, as a slice. Empty when no tx is active or the
// table has no dirty rows.
func (s *session) dirtyRowPKs(table string) []string {
	if s.txContext == nil || s.txDirtyRows == nil {
		return nil
	}
	set, ok := s.txDirtyRows[table]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(set))
	for pk := range set {
		out = append(out, pk)
	}
	return out
}

// clearTxState resets the tx-scoped fields after commit/rollback.
func (s *session) clearTxState() {
	s.txContext = nil
	s.txDirtyRows = nil
	s.savepoints = nil
}

// cloneDirtyRows returns a deep copy of the tx's dirty-row set so
// the clone can be restored on ROLLBACK TO SAVEPOINT without sharing
// inner maps.
func cloneDirtyRows(src map[string]map[string]struct{}) map[string]map[string]struct{} {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]map[string]struct{}, len(src))
	for table, pks := range src {
		cloned := make(map[string]struct{}, len(pks))
		for pk := range pks {
			cloned[pk] = struct{}{}
		}
		out[table] = cloned
	}
	return out
}

// pushSavepoint records the current in-tx state under the given
// name. Duplicate names are allowed — pg silently shadows earlier
// savepoints with the same name on push, and ROLLBACK TO finds the
// most recent. We match that: later frames shadow earlier ones.
func (s *session) pushSavepoint(name string) {
	if s.txContext == nil {
		return
	}
	s.savepoints = append(s.savepoints, savepointFrame{
		name:      name,
		ctxSnap:   s.txContext.TakeSavepoint(),
		dirtyRows: cloneDirtyRows(s.txDirtyRows),
	})
}

// releaseSavepoint pops the most recent savepoint with the given
// name plus every frame above it. pg's RELEASE SAVEPOINT discards
// intermediate savepoints too. Returns false if no matching frame
// is on the stack.
func (s *session) releaseSavepoint(name string) bool {
	idx := s.findSavepoint(name)
	if idx < 0 {
		return false
	}
	s.savepoints = s.savepoints[:idx]
	return true
}

// rollbackToSavepoint restores the named savepoint's captured state
// onto the session. The named frame itself stays on the stack
// (clients may ROLLBACK TO the same name repeatedly); frames above
// it are discarded. Returns false if no matching frame is found.
func (s *session) rollbackToSavepoint(name string) bool {
	idx := s.findSavepoint(name)
	if idx < 0 {
		return false
	}
	frame := s.savepoints[idx]
	s.txContext.RestoreSavepoint(frame.ctxSnap)
	s.txDirtyRows = cloneDirtyRows(frame.dirtyRows)
	s.savepoints = s.savepoints[:idx+1]
	return true
}

// findSavepoint returns the stack index of the most recent
// savepoint with the given name, or -1 if none.
func (s *session) findSavepoint(name string) int {
	for i, v := range slices.Backward(s.savepoints) {
		if v.name == name {
			return i
		}
	}
	return -1
}

// localSchemaEntry holds the SQLite-side state for a table or view
// discovered in sqlite_master. For tables, cols is the ordered
// column-name list from PRAGMA table_info. For views, sqlText is the
// declared CREATE VIEW text from sqlite_master.sql — compared
// against the reconstructed-from-swytch text to detect body drift.
type localSchemaEntry struct {
	kind    string
	cols    []string
	sqlText string
}

// ensureFreshVTabs brings the session's SQLite-declared tables and
// views in line with swytch's current committed schema. Called
// lazily at query-handler entry when the server-global schema epoch
// has moved past the session's last-seen epoch. Handles:
//
//   - new tables/views from another session (CREATE here);
//   - tables/views that vanished (DROP here);
//   - column-list drift on a table (DROP + re-CREATE VIRTUAL TABLE);
//   - body drift on a view (DROP + re-CREATE VIEW);
//   - kind flip on a name (e.g. CREATE VIEW after DROP TABLE).
//
// Any prepared statement this session held against a replaced
// table/view is invalidated at next Step, matching SQLITE_SCHEMA
// semantics.
func (s *Server) ensureFreshVTabs(sess *session) error {
	currentEpoch := s.schemaEpoch.Load()
	if sess.schemaEpoch >= currentEpoch {
		return nil
	}

	// Snapshot SQLite's view of declared tables + views. For views
	// we additionally pull sqlite_master.sql so we can detect body
	// drift without reconstructing the original text.
	local := make(map[string]localSchemaEntry)
	if err := iterateRows(sess.conn,
		"SELECT name, type, COALESCE(sql, '') FROM sqlite_master WHERE type IN ('table','view')",
		func(stmt *sqlite.Stmt) error {
			name := stmt.ColumnText(0)
			kind := stmt.ColumnText(1)
			entry := localSchemaEntry{kind: kind, sqlText: stmt.ColumnText(2)}
			if kind == "table" {
				innerErr := iterateRows(sess.conn,
					"PRAGMA table_info("+quoteIdent(name)+")",
					func(pstmt *sqlite.Stmt) error {
						entry.cols = append(entry.cols, pstmt.ColumnText(1))
						return nil
					})
				if innerErr != nil {
					return innerErr
				}
			}
			local[name] = entry
			return nil
		}); err != nil {
		return fmt.Errorf("ensure fresh vtabs: enumerate sqlite_master: %w", err)
	}

	// Consult swytch's committed state.
	ectx := s.engine.NewContext()
	swytchTables, err := listTables(ectx)
	if err != nil {
		return fmt.Errorf("ensure fresh vtabs: list tables: %w", err)
	}
	swytchViews, err := listViews(ectx)
	if err != nil {
		return fmt.Errorf("ensure fresh vtabs: list views: %w", err)
	}

	present := make(map[string]string, len(swytchTables)+len(swytchViews))
	for _, name := range swytchTables {
		present[name] = "table"
	}
	for _, name := range swytchViews {
		if _, clash := present[name]; clash {
			// Shouldn't happen — CREATE paths reject name clashes —
			// but if someone wrote directly to storage, prefer the
			// table mapping. Views compile lazily so nothing breaks
			// until the view is queried.
			continue
		}
		present[name] = "view"
	}

	// Reconcile tables: create missing, redeclare drifted.
	for _, name := range swytchTables {
		live, err := loadSchema(ectx, name)
		if err != nil {
			sess.log.Warn("sql: skipping vtab refresh with missing schema",
				"table", name, "err", err)
			continue
		}
		entry, already := local[name]
		if !already {
			if err := registerVTable(sess.conn, live); err != nil {
				return fmt.Errorf("register new vtab %s: %w", name, err)
			}
			continue
		}
		// Kind flip (was a view, now a table) or column drift.
		if entry.kind != "table" || !vtabColumnsMatch(live.Columns, entry.cols) {
			dropStmt := "DROP TABLE " + quoteIdent(name)
			if entry.kind == "view" {
				dropStmt = "DROP VIEW " + quoteIdent(name)
			}
			if err := execSimple(sess.conn, dropStmt); err != nil {
				return fmt.Errorf("drop stale %s %s: %w", entry.kind, name, err)
			}
			if err := registerVTable(sess.conn, live); err != nil {
				return fmt.Errorf("re-register vtab %s: %w", name, err)
			}
		}
	}

	// Reconcile views: create missing, redeclare on body or kind drift.
	for _, name := range swytchViews {
		v, err := loadView(ectx, name)
		if err != nil {
			sess.log.Warn("sql: skipping view refresh with missing body",
				"view", name, "err", err)
			continue
		}
		entry, already := local[name]
		if !already {
			if err := registerView(sess.conn, v); err != nil {
				return fmt.Errorf("register new view %s: %w", name, err)
			}
			continue
		}
		wantSQL := "CREATE VIEW " + quoteIdent(v.Name) + " " + v.Body
		if entry.kind != "view" || entry.sqlText != wantSQL {
			dropStmt := "DROP VIEW " + quoteIdent(name)
			if entry.kind == "table" {
				dropStmt = "DROP TABLE " + quoteIdent(name)
			}
			if err := execSimple(sess.conn, dropStmt); err != nil {
				return fmt.Errorf("drop stale %s %s: %w", entry.kind, name, err)
			}
			if err := registerView(sess.conn, v); err != nil {
				return fmt.Errorf("re-register view %s: %w", name, err)
			}
		}
	}

	// Objects SQLite still holds but swytch no longer knows about:
	// drop them so queries against them fail cleanly instead of
	// hitting a vtab whose storage is gone (or a view whose
	// referenced tables have vanished).
	for name, entry := range local {
		if _, still := present[name]; still {
			continue
		}
		var dropStmt string
		if entry.kind == "view" {
			dropStmt = "DROP VIEW " + quoteIdent(name)
		} else {
			dropStmt = "DROP TABLE " + quoteIdent(name)
		}
		if err := execSimple(sess.conn, dropStmt); err != nil {
			return fmt.Errorf("drop vanished %s %s: %w", entry.kind, name, err)
		}
	}

	sess.schemaEpoch = currentEpoch
	return nil
}

// vtabColumnsMatch reports whether the swytch-side column set
// matches SQLite's declared columns in order. Order matters because
// SQLite addresses vtab columns by ordinal, and an in-between rename
// or reordering must trigger a redeclare.
func vtabColumnsMatch(swytch []columnSchema, sqliteCols []string) bool {
	if len(swytch) != len(sqliteCols) {
		return false
	}
	for i, c := range swytch {
		if c.Name != sqliteCols[i] {
			return false
		}
	}
	return true
}

func newSession(id uint64, log *slog.Logger) (*session, error) {
	conn, err := sqlite.OpenConn(":memory:")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	return &session{
		id:   id,
		conn: conn,
		log:  log.With("session", id),
	}, nil
}

// Close releases the session's SQLite connection. Safe to call
// concurrently; only the first call does work, and subsequent callers
// observe the same error (if any).
func (s *session) Close() error {
	s.closeOnce.Do(func() {
		if s.conn == nil {
			return
		}
		s.closeErr = s.conn.Close()
		s.conn = nil
	})
	return s.closeErr
}

func (s *Server) initSession(ctx context.Context) (context.Context, error) {
	id := s.nextID.Add(1)
	sess, err := newSession(id, s.log)
	if err != nil {
		return ctx, fmt.Errorf("sql: initSession: %w", err)
	}
	// Register the swytch vtab module on this conn so subsequent
	// CREATE VIRTUAL TABLE statements route to us.
	if err := sess.conn.SetModule(moduleName, s.module); err != nil {
		return ctx, errors.Join(
			fmt.Errorf("sql: register vtab module: %w", err),
			closeSessionOnError(sess),
		)
	}
	// Replay the replicated table list: every known table gets a
	// matching CREATE VIRTUAL TABLE on this session's conn so that
	// queries referencing it compile.
	// Register the conn→session mapping BEFORE replayTables so any
	// Connect callbacks fired by the CREATE VIRTUAL TABLE statements
	// in replay can resolve their session through the Server.
	s.registerConnSession(sess.conn, sess)
	if err := s.replayTables(sess); err != nil {
		s.unregisterConnSession(sess.conn)
		return ctx, errors.Join(
			fmt.Errorf("sql: replay tables: %w", err),
			closeSessionOnError(sess),
		)
	}
	// Views must replay after tables so SQLite can compile
	// SELECT bodies that reference the vtab-declared tables.
	if err := s.replayViews(sess); err != nil {
		s.unregisterConnSession(sess.conn)
		return ctx, errors.Join(
			fmt.Errorf("sql: replay views: %w", err),
			closeSessionOnError(sess),
		)
	}
	sess.log.Debug("sql session opened")
	return context.WithValue(ctx, sessionCtxKey{}, sess), nil
}

// registerConnSession records the conn→session mapping. Callers
// must ensure this is called before any vtab Connect callback that
// should see the session.
func (s *Server) registerConnSession(conn *sqlite.Conn, sess *session) {
	s.sessionsByConn.mu.Lock()
	s.sessionsByConn.m[conn] = sess
	s.sessionsByConn.mu.Unlock()
}

// unregisterConnSession removes the conn's entry.
func (s *Server) unregisterConnSession(conn *sqlite.Conn) {
	s.sessionsByConn.mu.Lock()
	delete(s.sessionsByConn.m, conn)
	s.sessionsByConn.mu.Unlock()
}

// sessionForConn resolves a conn to its session, returning nil if
// not registered.
func (s *Server) sessionForConn(conn *sqlite.Conn) *session {
	s.sessionsByConn.mu.RLock()
	defer s.sessionsByConn.mu.RUnlock()
	return s.sessionsByConn.m[conn]
}

// closeSessionOnError cleans up a partially-initialised session and
// wraps any Close error so it's visible via errors.Join rather than
// silently dropped.
func closeSessionOnError(sess *session) error {
	if err := sess.Close(); err != nil {
		return fmt.Errorf("sql: close partial session: %w", err)
	}
	return nil
}

// replayTables reads the current table registry from swytch and
// issues CREATE VIRTUAL TABLE for each on the session's conn.
func (s *Server) replayTables(sess *session) error {
	if s.engine == nil {
		return nil
	}
	ectx := s.engine.NewContext()
	names, err := listTables(ectx)
	if err != nil {
		return err
	}
	for _, name := range names {
		schema, err := loadSchema(ectx, name)
		if err != nil {
			sess.log.Warn("sql: skipping table with missing schema",
				"table", name, "err", err)
			continue
		}
		if err := registerVTable(sess.conn, schema); err != nil {
			return fmt.Errorf("replay %q: %w", name, err)
		}
	}
	return nil
}

// replayViews reads the current view registry from swytch and
// issues CREATE VIEW for each on the session's conn. Must be called
// after replayTables so any view that references a swytch-backed
// table finds it declared. View-on-view chains are sorted by name;
// SQLite's CREATE VIEW is lazy about resolving references, so a
// forward reference across views in the same batch compiles fine.
func (s *Server) replayViews(sess *session) error {
	if s.engine == nil {
		return nil
	}
	ectx := s.engine.NewContext()
	names, err := listViews(ectx)
	if err != nil {
		return err
	}
	for _, name := range names {
		v, err := loadView(ectx, name)
		if err != nil {
			sess.log.Warn("sql: skipping view with missing body",
				"view", name, "err", err)
			continue
		}
		if err := registerView(sess.conn, v); err != nil {
			return fmt.Errorf("replay view %q: %w", name, err)
		}
	}
	return nil
}

func (s *Server) closeSession(ctx context.Context) error {
	sess, ok := ctx.Value(sessionCtxKey{}).(*session)
	if !ok {
		return nil
	}
	s.unregisterConnSession(sess.conn)
	// If the client disconnected mid-transaction, abort the pending
	// Context so its writes don't leak into the log.
	if sess.txContext != nil {
		sess.txContext.Abort()
		sess.clearTxState()
	}
	sess.log.Debug("sql session closing")
	return sess.Close()
}

func sessionFromCtx(ctx context.Context) (*session, error) {
	sess, ok := ctx.Value(sessionCtxKey{}).(*session)
	if !ok {
		return nil, fmt.Errorf("sql: no session attached to context")
	}
	return sess, nil
}
