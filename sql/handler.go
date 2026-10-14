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
	"zombiezen.com/go/sqlite"
)

// Custom GUC-style parameter that dialect-aware ORMs can read via
// startup ParameterStatus or `SHOW swytch.dialect`.
const (
	dialectParamKey = "swytch.dialect"
	dialectValue    = "sqlite"
)

// handle is the psql-wire ParseFn. It is called once per incoming query
// and returns one or more prepared statements that the wire layer will
// then execute.
//
// In phase 1 we only support the simple query protocol: each query
// parses + executes once, and prepared statements are single-use.
func (s *Server) handle(ctx context.Context, incoming wire.Query) (wire.PreparedStatements, error) {
	query := incoming.Query
	sess, err := sessionFromCtx(ctx)
	if err != nil {
		return nil, err
	}

	sess.log.Debug("incoming query", "query", query)

	if stmt, ok := interceptShowSet(query); ok {
		return wire.Prepared(stmt), nil
	}

	// Transaction control (BEGIN/COMMIT/ROLLBACK) must run before
	// DDL/DML so the surrounding-session state flips on/off before
	// any subsequent write consults session.txContext.
	if stmt, handled, err := s.maybeInterceptTransaction(sess, query); handled {
		if err != nil {
			return nil, err
		}
		return wire.Prepared(stmt), nil
	}

	// DDL that needs to route through swytch (CREATE TABLE, later
	// DROP/ALTER/CREATE INDEX) is handled before SQLite sees it.
	if stmt, handled, err := s.maybeInterceptDDL(sess, query); handled {
		if err != nil {
			return nil, err
		}
		return wire.Prepared(stmt), nil
	}

	// If another session's DDL bumped the server's schema epoch
	// since we last synced, redeclare any drifted vtabs on this
	// session's SQLite conn before SQLite parses the query.
	// Steady-state cost: one atomic Load.
	if err := s.ensureFreshVTabs(sess); err != nil {
		return nil, err
	}

	// pgJDBC (and likely other drivers) in simple-query mode inline
	// parameter values with pg `::type` casts — `('3'::int4)`,
	// `('518'::int8)` and friends. SQLite doesn't understand `::`,
	// so a query that reaches PrepareTransient with a cast in it
	// blows up at the colon. Strip the casts before the parser
	// sees them; SQLite's dynamic typing doesn't need the hint
	// anyway.
	query = stripPgTypeCasts(query)

	sqliteStmt, _, err := sess.conn.PrepareTransient(query)
	if err != nil {
		return nil, fmt.Errorf("sqlite prepare: %w", err)
	}

	colCount := sqliteStmt.ColumnCount()

	// Capture the baseline change counter BEFORE the peek Step
	// below, because for INSERT/UPDATE/DELETE that Step already
	// executes the statement and increments Changes().
	changesBefore := sess.conn.Changes()

	// SQLite is dynamically typed — the result columns' types are only
	// known after the first Step. We peek the first row here so the
	// RowDescription we return advertises accurate OIDs. For DML/DDL
	// statements (colCount == 0) this single Step is the execution;
	// we flag the statement as exhausted so the exec loop below is a
	// no-op rather than a re-execution.
	hasFirst, err := sqliteStmt.Step()
	if err != nil {
		stepErr := fmt.Errorf("sqlite step: %w", err)
		if finErr := sqliteStmt.Finalize(); finErr != nil {
			return nil, errors.Join(stepErr, fmt.Errorf("finalize: %w", finErr))
		}
		return nil, stepErr
	}
	exhausted := !hasFirst

	var firstRow []any
	var cols wire.Columns
	if colCount > 0 {
		cols = make(wire.Columns, colCount)
		for i := range colCount {
			var oid uint32
			if hasFirst {
				oid = oidForSqliteType(sqliteStmt.ColumnType(i))
			} else {
				oid = oidUnknown
			}
			cols[i] = wire.Column{
				Name:  sqliteStmt.ColumnName(i),
				Oid:   oid,
				Width: -1,
			}
		}
		if hasFirst {
			firstRow = make([]any, colCount)
			for i := range colCount {
				firstRow[i] = readColumn(sqliteStmt, i)
			}
		}
	}

	tagPrefix := commandTag(query)

	execFn := func(ctx context.Context, writer wire.DataWriter, _ []wire.Parameter) (retErr error) {
		// Finalize must run on every exit path. Merge its error into
		// retErr rather than dropping it.
		defer func() {
			if err := sqliteStmt.Finalize(); err != nil {
				finErr := fmt.Errorf("finalize: %w", err)
				if retErr == nil {
					retErr = finErr
				} else {
					retErr = errors.Join(retErr, finErr)
				}
			}
		}()

		rowsWritten := uint32(0)
		if firstRow != nil {
			if err := writer.Row(firstRow); err != nil {
				return err
			}
			rowsWritten++
		}
		if !exhausted {
			for {
				has, err := sqliteStmt.Step()
				if err != nil {
					return fmt.Errorf("sqlite step: %w", err)
				}
				if !has {
					break
				}
				if colCount == 0 {
					continue
				}
				row := make([]any, colCount)
				for i := range colCount {
					row[i] = readColumn(sqliteStmt, i)
				}
				if err := writer.Row(row); err != nil {
					return err
				}
				rowsWritten++
			}
		}
		// For INSERT/UPDATE/DELETE pg reports affected-row count, not
		// the number of rows we streamed to the client.
		tagCount := rowsWritten
		if colCount == 0 {
			tagCount = uint32(sess.conn.Changes() - changesBefore)
		}
		return writer.Complete(formatTag(tagPrefix, tagCount))
	}

	opts := []wire.PreparedOptionFn{}
	if cols != nil {
		opts = append(opts, wire.WithColumns(cols))
	}
	return wire.Prepared(wire.NewStatement(execFn, opts...)), nil
}

// interceptShowSet handles SHOW / SET queries pg clients emit as part
// of session setup. SQLite has no idea what `SHOW TRANSACTION
// ISOLATION LEVEL` means, so without interception every connection
// from pgJDBC (or psql) fails its handshake probes and never runs
// real SQL.
//
// Two flavours:
//
//   - Our own SHOW / SET swytch.dialect — answered directly.
//   - Everything else with a SHOW / SET prefix — returned as a stub.
//     For SHOW we reply with a minimal one-column row naming the
//     parameter and a well-known-ish value (isolation level =
//     "serializable", everything else empty). For SET we accept and
//     discard; swytch SQL doesn't have pg's session-parameter
//     system, so there's nothing to actually change.
//
// Returns (stmt, true) when the query matches either flavour.
func interceptShowSet(query string) (*wire.PreparedStatement, bool) {
	trimmed := strings.TrimSuffix(strings.TrimSpace(query), ";")
	trimmed = strings.TrimSpace(trimmed)
	upper := strings.ToUpper(trimmed)

	switch {
	case matchesShowDialect(upper):
		cols := wire.Columns{{
			Name:  dialectParamKey,
			Oid:   oidText,
			Width: -1,
		}}
		fn := func(ctx context.Context, writer wire.DataWriter, _ []wire.Parameter) error {
			if err := writer.Row([]any{dialectValue}); err != nil {
				return err
			}
			return writer.Complete("SHOW")
		}
		return wire.NewStatement(fn, wire.WithColumns(cols)), true

	case matchesSetDialect(upper):
		fn := func(ctx context.Context, writer wire.DataWriter, _ []wire.Parameter) error {
			return writer.Complete("SET")
		}
		return wire.NewStatement(fn), true

	case strings.HasPrefix(upper, "SHOW "):
		name, value := showParamResponse(trimmed)
		cols := wire.Columns{{
			Name:  name,
			Oid:   oidText,
			Width: -1,
		}}
		fn := func(ctx context.Context, writer wire.DataWriter, _ []wire.Parameter) error {
			if err := writer.Row([]any{value}); err != nil {
				return err
			}
			return writer.Complete("SHOW")
		}
		return wire.NewStatement(fn, wire.WithColumns(cols)), true

	case strings.HasPrefix(upper, "SET "):
		fn := func(ctx context.Context, writer wire.DataWriter, _ []wire.Parameter) error {
			return writer.Complete("SET")
		}
		return wire.NewStatement(fn), true
	}
	return nil, false
}

func matchesShowDialect(upper string) bool {
	if !strings.HasPrefix(upper, "SHOW") {
		return false
	}
	rest := strings.TrimSpace(strings.TrimPrefix(upper, "SHOW"))
	return rest == "SWYTCH.DIALECT"
}

func matchesSetDialect(upper string) bool {
	if !strings.HasPrefix(upper, "SET") {
		return false
	}
	rest := strings.TrimSpace(strings.TrimPrefix(upper, "SET"))
	return strings.HasPrefix(rest, "SWYTCH.DIALECT")
}

// showParamResponse returns (column-name, value) for an arbitrary
// `SHOW <name>` query. The column name is the parameter the client
// asked for, preserving original case so a client matching on column
// names isn't tripped up. Values are defaults sensible for a
// swytch-SQL deployment; the set is deliberately small because pg
// clients usually only ask about a handful during startup.
func showParamResponse(trimmed string) (string, string) {
	rest := strings.TrimSpace(trimmed[len("SHOW"):])
	// Strip trailing whitespace/semicolon already handled; the
	// remaining token (possibly a multi-word parameter) is the name.
	name := rest
	switch strings.ToUpper(name) {
	case "TRANSACTION ISOLATION LEVEL":
		// Swytch SQL gives serializable isolation via sub-DAG
		// observations; advertise it.
		return "transaction_isolation", "serializable"
	case "DEFAULT_TRANSACTION_ISOLATION":
		return "default_transaction_isolation", "serializable"
	case "STANDARD_CONFORMING_STRINGS":
		return "standard_conforming_strings", "on"
	case "SERVER_VERSION":
		return "server_version", "17.0 (Swytch)"
	case "SERVER_ENCODING", "CLIENT_ENCODING":
		return strings.ToLower(name), "UTF8"
	case "TIMEZONE", "TIME ZONE":
		return "TimeZone", "UTC"
	case "INTEGER_DATETIMES":
		return "integer_datetimes", "on"
	}
	return name, ""
}

// stripPgTypeCasts removes pg-style `::type` casts from a query
// while leaving string literals and identifiers untouched. Casts
// show up in pgJDBC's simple-mode parameter inlining:
//
//	INSERT INTO t (k,v) VALUES (('3'::int4), ('518'::int8))
//
// SQLite's parser chokes on `::`. We walk the query character-by-
// character, skipping over single- and double-quoted regions (so we
// don't accidentally rewrite a `::` inside a string literal, however
// unlikely), and drop the `::type` suffix on bare tokens. The type
// name is read greedily as identifier chars, optionally followed by
// a `(...)` length parameter (e.g. `::varchar(32)`) and optional
// `[]` array markers.
//
// We don't need to validate the type name — if pgJDBC sends
// `::nonsense`, we erase it and SQLite sees the bare value, which
// is what the test harness expects.
func stripPgTypeCasts(query string) string {
	var b strings.Builder
	b.Grow(len(query))
	i := 0
	for i < len(query) {
		c := query[i]
		switch c {
		case '\'':
			// Copy through the string literal, including doubled
			// single quotes `''` which SQL uses as escape.
			b.WriteByte(c)
			i++
			for i < len(query) {
				b.WriteByte(query[i])
				if query[i] == '\'' {
					i++
					if i < len(query) && query[i] == '\'' {
						b.WriteByte('\'')
						i++
						continue
					}
					break
				}
				i++
			}
		case '"':
			b.WriteByte(c)
			i++
			for i < len(query) {
				b.WriteByte(query[i])
				if query[i] == '"' {
					i++
					break
				}
				i++
			}
		case ':':
			if i+1 < len(query) && query[i+1] == ':' {
				// Drop the `::` and the following type name.
				j := i + 2
				for j < len(query) && isTypeNameChar(query[j]) {
					j++
				}
				// Optional (N) or (N,M) length/precision.
				if j < len(query) && query[j] == '(' {
					depth := 1
					j++
					for j < len(query) && depth > 0 {
						if query[j] == '(' {
							depth++
						} else if query[j] == ')' {
							depth--
						}
						j++
					}
				}
				// Optional [] array brackets.
				for j+1 < len(query) && query[j] == '[' && query[j+1] == ']' {
					j += 2
				}
				i = j
				continue
			}
			b.WriteByte(c)
			i++
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}

// isTypeNameChar reports whether c can appear in a pg type name
// (letters, digits, underscore). Whitespace or punctuation ends the
// type.
func isTypeNameChar(c byte) bool {
	return c == '_' ||
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9')
}

// commandTag extracts the SQL command keyword for the pg
// CommandComplete response (e.g. "SELECT", "INSERT", "UPDATE").
// Unknown or unparseable queries get an empty prefix which is later
// combined with the row count in formatTag.
func commandTag(query string) string {
	trimmed := strings.TrimLeft(query, " \t\r\n")
	// Strip leading SQL comments so `-- comment\nSELECT 1` still tags as SELECT.
	for strings.HasPrefix(trimmed, "--") {
		nl := strings.IndexByte(trimmed, '\n')
		if nl < 0 {
			trimmed = ""
			break
		}
		trimmed = strings.TrimLeft(trimmed[nl+1:], " \t\r\n")
	}
	end := len(trimmed)
	for i, r := range trimmed {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '(' || r == ';' {
			end = i
			break
		}
	}
	if end == 0 {
		return ""
	}
	return strings.ToUpper(trimmed[:end])
}

// formatTag builds the CommandComplete tag. For data-modifying
// statements pg includes a row count; for SELECT it's the row count;
// everything else gets just the verb.
func formatTag(verb string, rows uint32) string {
	switch verb {
	case "SELECT":
		return fmt.Sprintf("SELECT %d", rows)
	case "INSERT":
		return fmt.Sprintf("INSERT 0 %d", rows)
	case "UPDATE", "DELETE":
		return fmt.Sprintf("%s %d", verb, rows)
	case "":
		return "OK"
	default:
		return verb
	}
}

// Compile-time assertion that *sqlite.Stmt still exposes the expected
// shape — if zombiezen changes its API this will fail to build.
var _ interface {
	Step() (bool, error)
	Finalize() error
	ColumnCount() int
	ColumnName(int) string
	ColumnType(int) sqlite.ColumnType
} = (*sqlite.Stmt)(nil)
