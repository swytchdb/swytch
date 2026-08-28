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
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	wire "github.com/jeroenrinzema/psql-wire"
	"zombiezen.com/go/sqlite"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

// maybeInterceptDDL peeks at the query's leading keywords and, if
// it's a DDL statement we route through swytch, handles it and
// returns (stmt, true, nil). Returns (nil, false, nil) to signal
// "not a DDL we handle — fall through to the SQLite path".
func (s *Server) maybeInterceptDDL(sess *session, query string) (*wire.PreparedStatement, bool, error) {
	trimmed := strings.TrimSpace(stripLeadingComments(query))
	upper := strings.ToUpper(trimmed)
	switch {
	case strings.HasPrefix(upper, "CREATE TABLE"),
		strings.HasPrefix(upper, "CREATE TEMP TABLE"),
		strings.HasPrefix(upper, "CREATE TEMPORARY TABLE"):
		return s.handleCreateTable(sess, trimmed)
	case strings.HasPrefix(upper, "DROP TABLE"):
		return s.handleDropTable(sess, trimmed)
	case strings.HasPrefix(upper, "CREATE INDEX"),
		strings.HasPrefix(upper, "CREATE UNIQUE INDEX"):
		return s.handleCreateIndex(sess, trimmed)
	case strings.HasPrefix(upper, "DROP INDEX"):
		return s.handleDropIndex(sess, trimmed)
	case strings.HasPrefix(upper, "ALTER TABLE"):
		return s.handleAlterTable(sess, trimmed)
	case strings.HasPrefix(upper, "CREATE VIEW"):
		// CREATE TEMP / TEMPORARY VIEW deliberately falls through to
		// SQLite: temp views are session-local by definition, so
		// there's no propagation work for us to do.
		return s.handleCreateView(sess, trimmed)
	case strings.HasPrefix(upper, "DROP VIEW"):
		return s.handleDropView(sess, trimmed)
	}
	return nil, false, nil
}

// handleAlterTable dispatches the supported ALTER TABLE forms. Phase
// 6 scope:
//
//   - ADD [COLUMN] <col-def>         — metadata-only; existing rows
//     read NULL for the new column. The issuing session redeclares
//     its SQLite vtab so subsequent queries on the same session see
//     the new column. Other sessions pick up the change on next
//     reconnect — SQLite's per-conn declared schema is immutable
//     after CREATE VIRTUAL TABLE and we don't (yet) signal
//     SQLITE_SCHEMA to force a reprepare.
//
// Future forms (RENAME TABLE, RENAME COLUMN, DROP COLUMN) land in
// subsequent steps.
func (s *Server) handleAlterTable(sess *session, ddl string) (*wire.PreparedStatement, bool, error) {
	name, subcmd, rest, err := parseAlterTableHead(ddl)
	if err != nil {
		return nil, true, err
	}
	switch strings.ToUpper(subcmd) {
	case "ADD":
		return s.handleAlterAddColumn(sess, name, rest)
	case "RENAME":
		form, a, b, err := parseRenameSpec(rest)
		if err != nil {
			return nil, true, err
		}
		if form == renameFormTable {
			return s.handleAlterRenameTableInner(sess, name, a)
		}
		return s.handleAlterRenameColumn(sess, name, a, b)
	case "DROP":
		return s.handleAlterDropColumn(sess, name, rest)
	}
	return nil, true, fmt.Errorf("ALTER TABLE: unsupported action %q", subcmd)
}

// handleAlterRenameColumn implements `ALTER TABLE <name> RENAME
// [COLUMN] <old> TO <new>`. Row fields are keyed by column name
// (KEYED collection, DataEffect.Id = column name), so every row
// gets an INSERT for the new name carrying the old value plus a
// REMOVE for the old name. Index-entry keys are backed by indexID +
// encoded value, so they don't move — only the column-name list in
// the schema and in each index's metadata is rewritten.
//
// PK rename has a wrinkle: it emits under a new column-name Id but
// the PK's stored-in-key bytes don't change (rowKey = tableID + pk).
// We only update the schema's Columns[PKIndex].Name; no row rewrite
// for the PK itself beyond the per-row presence-marker field.
func (s *Server) handleAlterRenameColumn(sess *session, tableName, oldCol, newCol string) (*wire.PreparedStatement, bool, error) {
	if strings.EqualFold(oldCol, newCol) {
		fn := func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			return w.Complete("ALTER TABLE")
		}
		return wire.NewStatement(fn), true, nil
	}

	var rowsRewritten int
	if err := runRMW(s.engine, "alter rename column", func(ectx *effects.Context) error {
		_, schemaTips, err := ectx.GetSnapshot(schemaKey(tableName))
		if err != nil {
			return fmt.Errorf("read schema: %w", err)
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(schemaKey(tableName)),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, schemaTips); err != nil {
			return fmt.Errorf("pin schema dep: %w", err)
		}
		tbl, err := loadSchema(ectx, tableName)
		if err != nil {
			return fmt.Errorf("ALTER TABLE RENAME COLUMN %s: %w", tableName, err)
		}

		// Locate the target column and verify the new name is free.
		colIdx := -1
		for i, c := range tbl.Columns {
			if strings.EqualFold(c.Name, oldCol) {
				colIdx = i
			} else if strings.EqualFold(c.Name, newCol) {
				return fmt.Errorf("column %q already exists", newCol)
			}
		}
		if colIdx < 0 {
			return fmt.Errorf("no such column: %s", oldCol)
		}
		canonicalOld := tbl.Columns[colIdx].Name

		// Walk rows, re-emit each stored value under the new name.
		// The PK column is not stored as a KEYED field under its
		// own name in the general case — the presence marker uses
		// the declared PK name, so we still rewrite it for
		// consistency.
		var rowKeys []string
		s.engine.ScanKeys("", rowKeyPattern(tbl.TableID), func(key string) bool {
			rowKeys = append(rowKeys, key)
			return true
		})
		for _, rk := range rowKeys {
			snap, _, err := ectx.GetSnapshot(rk)
			if err != nil {
				return fmt.Errorf("read row %x: %w", rk, err)
			}
			if snap == nil {
				continue
			}
			el, ok := snap.NetAdds[canonicalOld]
			if !ok || el.Data == nil {
				continue
			}
			newEff := &pb.DataEffect{
				Op:         pb.EffectOp_INSERT_OP,
				Merge:      pb.MergeRule_LAST_WRITE_WINS,
				Collection: pb.CollectionKind_KEYED,
				Id:         []byte(newCol),
				Value:      el.Data.Value,
			}
			if err := ectx.Emit(&pb.Effect{
				Key:  []byte(rk),
				Kind: &pb.Effect_Data{Data: newEff},
			}); err != nil {
				return fmt.Errorf("emit renamed field insert: %w", err)
			}
			if err := emitKeyedRemove(ectx, rk, canonicalOld); err != nil {
				return fmt.Errorf("emit old-field remove: %w", err)
			}
			rowsRewritten++
		}

		// Rewrite the column list in the schema and every index
		// that references the column by name.
		tbl.Columns[colIdx].Name = newCol
		for i := range tbl.Indexes {
			for j := range tbl.Indexes[i].Columns {
				if strings.EqualFold(tbl.Indexes[i].Columns[j], canonicalOld) {
					tbl.Indexes[i].Columns[j] = newCol
				}
			}
		}
		if err := emitSchema(ectx, tbl); err != nil {
			return err
		}

		// Re-emit `x/<idx>.cols` for indices that referenced the
		// old name so loadIndexSchema on a fresh context resolves
		// to the new name.
		for _, idx := range tbl.Indexes {
			mustRewrite := slices.Contains(idx.Columns, newCol)
			if !mustRewrite {
				continue
			}
			colsBytes := []byte{}
			for i, c := range idx.Columns {
				if i > 0 {
					colsBytes = append(colsBytes, 0)
				}
				colsBytes = append(colsBytes, []byte(c)...)
			}
			if err := emitRawField(ectx, indexMetaKey(idx.Name),
				indexFieldColumns, colsBytes); err != nil {
				return fmt.Errorf("rewrite index %s.cols: %w", idx.Name, err)
			}
		}
		return nil
	}); err != nil {
		return nil, true, err
	}

	if err := redeclareVTable(sess.conn, tableName, func() (*tableSchema, error) {
		return loadSchema(s.engine.NewContext(), tableName)
	}); err != nil {
		return nil, true, fmt.Errorf("redeclare vtab after RENAME COLUMN: %w", err)
	}

	sess.log.Info("sql column renamed",
		"table", tableName,
		"old", oldCol,
		"new", newCol,
		"rows_rewritten", rowsRewritten,
	)
	s.bumpSchemaEpoch(sess)

	fn := func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		return w.Complete("ALTER TABLE")
	}
	return wire.NewStatement(fn), true, nil
}

// handleAlterRenameTableInner is the rename-table handler after the
// shared RENAME parser has identified the form.
func (s *Server) handleAlterRenameTableInner(sess *session, oldName, newName string) (*wire.PreparedStatement, bool, error) {
	return s.handleAlterRenameTable(sess, oldName, "TO "+newName)
}

// handleAlterDropColumn implements `ALTER TABLE <name> DROP [COLUMN]
// <colname>`. Every existing row is touched to REMOVE the dropped
// field — keeping the field around and masking it in the schema
// would collide with a future ADD COLUMN that reuses the name, so
// there's no free-lunch option. Any index whose leading column is
// the dropped one cascades to full index teardown.
//
// The PK column is not droppable: existing rows are addressed by
// their PK encoding, so losing the column would leave the row
// namespace unaddressable. SQLite has the same restriction.
func (s *Server) handleAlterDropColumn(sess *session, tableName, rest string) (*wire.PreparedStatement, bool, error) {
	colName, err := parseDropColumnSpec(rest)
	if err != nil {
		return nil, true, err
	}

	var (
		rowsTouched    int
		indicesDropped []string
	)
	if err := runRMW(s.engine, "alter drop column", func(ectx *effects.Context) error {
		_, schemaTips, err := ectx.GetSnapshot(schemaKey(tableName))
		if err != nil {
			return fmt.Errorf("read schema: %w", err)
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(schemaKey(tableName)),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, schemaTips); err != nil {
			return fmt.Errorf("pin schema dep: %w", err)
		}
		tbl, err := loadSchema(ectx, tableName)
		if err != nil {
			return fmt.Errorf("ALTER TABLE DROP COLUMN %s: %w", tableName, err)
		}

		// Resolve the column + PK invariant.
		colIdx := -1
		for i, c := range tbl.Columns {
			if strings.EqualFold(c.Name, colName) {
				colIdx = i
				break
			}
		}
		if colIdx < 0 {
			return fmt.Errorf("no such column: %s", colName)
		}
		if slices.Contains(tbl.PKColumns, colIdx) {
			return fmt.Errorf("cannot drop primary key column %q", colName)
		}
		canonicalName := tbl.Columns[colIdx].Name

		// Walk rows once, emitting REMOVE for the field on each.
		var rowKeys []string
		s.engine.ScanKeys("", rowKeyPattern(tbl.TableID), func(key string) bool {
			rowKeys = append(rowKeys, key)
			return true
		})
		for _, rk := range rowKeys {
			if err := emitKeyedRemove(ectx, rk, canonicalName); err != nil {
				return fmt.Errorf("remove %s from row %x: %w", canonicalName, rk, err)
			}
		}
		rowsTouched = len(rowKeys)

		// Cascade indices that reference the dropped column.
		var surviving []indexSchema
		for _, idx := range tbl.Indexes {
			cascade := false
			for _, c := range idx.Columns {
				if strings.EqualFold(c, canonicalName) {
					cascade = true
					break
				}
			}
			if !cascade {
				surviving = append(surviving, idx)
				continue
			}
			if err := dropIndexEntries(s.engine, ectx, idx.IndexID); err != nil {
				return fmt.Errorf("cascade drop index %s entries: %w", idx.Name, err)
			}
			if err := emitIndexMetadataRemove(ectx, idx.Name); err != nil {
				return fmt.Errorf("cascade drop index %s meta: %w", idx.Name, err)
			}
			indicesDropped = append(indicesDropped, idx.Name)
		}

		// Rewrite schema: shrink column list, adjust PKIndex if the
		// drop shifts its position left, keep only surviving indexes.
		newCols := make([]columnSchema, 0, len(tbl.Columns)-1)
		newCols = append(newCols, tbl.Columns[:colIdx]...)
		newCols = append(newCols, tbl.Columns[colIdx+1:]...)
		tbl.Columns = newCols
		// PK column ordinals shift left when a column with a lower
		// ordinal is dropped.
		for i := range tbl.PKColumns {
			if tbl.PKColumns[i] > colIdx {
				tbl.PKColumns[i]--
			}
		}
		tbl.Indexes = surviving
		return emitSchema(ectx, tbl)
	}); err != nil {
		return nil, true, err
	}

	// Redeclare the vtab on the issuing session so SQLite sees the
	// new column list immediately. Cross-session propagation is
	// handled by ensureFreshVTabs on the next query elsewhere.
	if err := redeclareVTable(sess.conn, tableName, func() (*tableSchema, error) {
		return loadSchema(s.engine.NewContext(), tableName)
	}); err != nil {
		return nil, true, fmt.Errorf("redeclare vtab after DROP COLUMN: %w", err)
	}

	sess.log.Info("sql column dropped",
		"table", tableName,
		"column", colName,
		"rows_touched", rowsTouched,
		"indices_dropped", indicesDropped,
	)
	s.bumpSchemaEpoch(sess)

	fn := func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		return w.Complete("ALTER TABLE")
	}
	return wire.NewStatement(fn), true, nil
}

// parseDropColumnSpec accepts `[COLUMN] <colname>` — the tail of an
// ALTER TABLE DROP.
func parseDropColumnSpec(rest string) (string, error) {
	tokens := strings.Fields(rest)
	if len(tokens) == 0 {
		return "", fmt.Errorf("ALTER TABLE DROP: missing column name")
	}
	i := 0
	if strings.EqualFold(tokens[i], "COLUMN") {
		i++
	}
	if i >= len(tokens) {
		return "", fmt.Errorf("ALTER TABLE DROP: missing column name")
	}
	name := trimIdent(strings.TrimRight(tokens[i], ";"))
	if name == "" {
		return "", fmt.Errorf("ALTER TABLE DROP: empty column name")
	}
	return name, nil
}

// handleAlterRenameTable implements `ALTER TABLE <old> RENAME TO
// <new>`. Because row and index-entry keys are tableID/indexID-
// backed (see keys.go), the physical keyspace doesn't move; only
// the name-keyed metadata does:
//
//   - schema meta migrates from s/<old> to s/<new> (same tableID,
//     same columns, Name field updated);
//   - table-list entry L.<old> is REMOVEd and L.<new> INSERTed;
//   - each index's x/<idx>.table field is rewritten so DROP INDEX
//     and other name-based lookups resolve the new parent.
//
// No row data or index entries are touched. The issuing session
// redeclares both vtabs (drop old, create new); other sessions sync
// at their next query via ensureFreshVTabs, which sees the old name
// vanish from swytch's table registry and the new one appear.
func (s *Server) handleAlterRenameTable(sess *session, oldName, rest string) (*wire.PreparedStatement, bool, error) {
	newName, err := parseRenameToSpec(rest)
	if err != nil {
		return nil, true, err
	}
	if strings.EqualFold(oldName, newName) {
		// pg accepts rename-to-self as a no-op; we do too.
		fn := func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
			return w.Complete("ALTER TABLE")
		}
		return wire.NewStatement(fn), true, nil
	}

	if err := runRMW(s.engine, "alter rename table", func(ectx *effects.Context) error {
		// Pin both the old schema key and the table list so a
		// concurrent DROP / RENAME / CREATE aborts cleanly.
		_, schemaTips, err := ectx.GetSnapshot(schemaKey(oldName))
		if err != nil {
			return fmt.Errorf("read old schema: %w", err)
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(schemaKey(oldName)),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, schemaTips); err != nil {
			return fmt.Errorf("pin old schema dep: %w", err)
		}
		listSnap, listTips, err := ectx.GetSnapshot(tableListKey)
		if err != nil {
			return fmt.Errorf("read table list: %w", err)
		}
		if listSnap != nil {
			if _, exists := listSnap.NetAdds[newName]; exists {
				return fmt.Errorf("%w: %s", ErrTableExists, newName)
			}
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(tableListKey),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, listTips); err != nil {
			return fmt.Errorf("pin table-list dep: %w", err)
		}

		tbl, err := loadSchema(ectx, oldName)
		if err != nil {
			return fmt.Errorf("ALTER TABLE RENAME %s: %w", oldName, err)
		}
		// Rewrite names in the schema and its child-index list.
		tbl.Name = newName
		for i := range tbl.Indexes {
			tbl.Indexes[i].Table = newName
		}

		// Emit schema at the new name and cull it from the old.
		if err := emitSchema(ectx, tbl); err != nil {
			return fmt.Errorf("emit new schema: %w", err)
		}
		oldFields := []string{
			schemaFieldTableID, schemaFieldColumns,
			schemaFieldPK, schemaFieldIndexes,
		}
		for _, f := range oldFields {
			if err := emitKeyedRemove(ectx, schemaKey(oldName), f); err != nil {
				return fmt.Errorf("remove old schema field %s: %w", f, err)
			}
		}
		if err := emitKeyedRemove(ectx, tableListKey, oldName); err != nil {
			return fmt.Errorf("remove table-list entry for %s: %w", oldName, err)
		}

		// Update each child index's table-name back-reference so
		// DROP INDEX and other name-based lookups land on the right
		// parent. TableID stays the same, so index-entry keys don't
		// move.
		for _, idx := range tbl.Indexes {
			if err := emitRawField(ectx, indexMetaKey(idx.Name),
				indexFieldTable, []byte(newName)); err != nil {
				return fmt.Errorf("rewrite index %s.table: %w", idx.Name, err)
			}
		}
		return nil
	}); err != nil {
		return nil, true, err
	}

	// On the issuing session, DROP the old vtab (SQLite releases
	// its declared schema) and CREATE the new one. Indices carry
	// over automatically — they're backed by indexID, not name.
	if err := execSimple(sess.conn, "DROP TABLE "+quoteIdent(oldName)); err != nil {
		return nil, true, fmt.Errorf("drop old vtab on session: %w", err)
	}
	newSchema, err := loadSchema(s.engine.NewContext(), newName)
	if err != nil {
		return nil, true, fmt.Errorf("reload schema post-rename: %w", err)
	}
	if err := registerVTable(sess.conn, newSchema); err != nil {
		return nil, true, fmt.Errorf("register renamed vtab on session: %w", err)
	}

	sess.log.Info("sql table renamed", "old", oldName, "new", newName)
	s.bumpSchemaEpoch(sess)

	fn := func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		return w.Complete("ALTER TABLE")
	}
	return wire.NewStatement(fn), true, nil
}

// parseRenameToSpec parses the tail of "RENAME <rest>". Two forms:
//
//	RENAME TO <new>              — table rename (returns form=renameTable)
//	RENAME [COLUMN] <old> TO <new> — column rename (returns form=renameColumn)
//
// The table form returns (form=renameTable, newTable). The column form
// returns (form=renameColumn, oldCol, newCol).
type renameForm int

const (
	renameFormTable renameForm = iota
	renameFormColumn
)

func parseRenameSpec(rest string) (form renameForm, a, b string, err error) {
	tokens := strings.Fields(rest)
	if len(tokens) == 0 {
		return 0, "", "", fmt.Errorf("RENAME: missing clause")
	}
	// "COLUMN" is an optional lead-in for column-rename; "TO" alone
	// signals table-rename.
	if strings.EqualFold(tokens[0], "TO") {
		if len(tokens) < 2 {
			return 0, "", "", fmt.Errorf("RENAME TO: missing new name")
		}
		name := trimIdent(strings.TrimRight(tokens[1], ";"))
		if name == "" {
			return 0, "", "", fmt.Errorf("RENAME TO: empty new name")
		}
		return renameFormTable, name, "", nil
	}
	i := 0
	if strings.EqualFold(tokens[i], "COLUMN") {
		i++
	}
	if i >= len(tokens) {
		return 0, "", "", fmt.Errorf("RENAME COLUMN: missing old name")
	}
	oldCol := trimIdent(tokens[i])
	i++
	if i >= len(tokens) || !strings.EqualFold(tokens[i], "TO") {
		return 0, "", "", fmt.Errorf("RENAME COLUMN: expected TO after old name")
	}
	i++
	if i >= len(tokens) {
		return 0, "", "", fmt.Errorf("RENAME COLUMN: missing new name")
	}
	newCol := trimIdent(strings.TrimRight(tokens[i], ";"))
	if oldCol == "" || newCol == "" {
		return 0, "", "", fmt.Errorf("RENAME COLUMN: empty name")
	}
	return renameFormColumn, oldCol, newCol, nil
}

// parseRenameToSpec is the table-rename entry point. Kept as a
// narrow shim so existing handleAlterRenameTable callers don't need
// to know about the column-rename form.
func parseRenameToSpec(rest string) (string, error) {
	form, a, _, err := parseRenameSpec(rest)
	if err != nil {
		return "", err
	}
	if form != renameFormTable {
		return "", fmt.Errorf("RENAME: expected TO <newname>")
	}
	return a, nil
}

// parseAlterTableHead tokenises "ALTER TABLE <name> <subcmd> <rest>"
// and returns each piece. <subcmd> is the single token after the
// table name (ADD / RENAME / DROP); <rest> is everything after that,
// joined back with single-space separators for subsequent parsing.
func parseAlterTableHead(ddl string) (name, subcmd, rest string, err error) {
	tokens := strings.Fields(ddl)
	if len(tokens) < 4 {
		return "", "", "", fmt.Errorf("malformed ALTER TABLE: %q", ddl)
	}
	if !strings.EqualFold(tokens[0], "ALTER") || !strings.EqualFold(tokens[1], "TABLE") {
		return "", "", "", fmt.Errorf("malformed ALTER TABLE: %q", ddl)
	}
	name = trimIdent(tokens[2])
	if name == "" {
		return "", "", "", fmt.Errorf("ALTER TABLE missing name: %q", ddl)
	}
	subcmd = tokens[3]
	if len(tokens) > 4 {
		rest = strings.Join(tokens[4:], " ")
	}
	rest = strings.TrimRight(rest, ";")
	return name, subcmd, rest, nil
}

// handleAlterAddColumn implements `ALTER TABLE <name> ADD [COLUMN]
// <col-def>`. The new column is appended to the schema; no existing
// rows are rewritten, so reads against them return NULL for the new
// column (the per-column reducer already treats missing fields as
// absent → NULL).
func (s *Server) handleAlterAddColumn(sess *session, name, rest string) (*wire.PreparedStatement, bool, error) {
	colName, declType, notNull, def, err := parseAddColumnSpec(rest)
	if err != nil {
		return nil, true, err
	}
	if notNull && def == nil {
		// NOT NULL without a DEFAULT would invalidate every
		// pre-existing row the moment the ALTER lands. SQLite
		// rejects the same combination; we mirror it.
		return nil, true, fmt.Errorf(
			"ALTER TABLE ADD COLUMN %s NOT NULL requires a DEFAULT", colName)
	}
	if notNull && def != nil && def.Kind == "null" {
		return nil, true, fmt.Errorf(
			"ALTER TABLE ADD COLUMN %s NOT NULL with DEFAULT NULL is contradictory", colName)
	}

	newCol := columnSchema{
		Name:     colName,
		Affinity: sqliteAffinity(declType),
		DeclType: declType,
		NotNull:  notNull,
		Default:  def,
	}

	if err := runRMW(s.engine, "alter add column", func(ectx *effects.Context) error {
		// Pin the schema key so a concurrent ALTER / DROP aborts one
		// of us cleanly.
		_, schemaTips, err := ectx.GetSnapshot(schemaKey(name))
		if err != nil {
			return fmt.Errorf("read schema: %w", err)
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(schemaKey(name)),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, schemaTips); err != nil {
			return fmt.Errorf("pin schema dep: %w", err)
		}
		tbl, err := loadSchema(ectx, name)
		if err != nil {
			return fmt.Errorf("ALTER TABLE %s: %w", name, err)
		}
		for _, c := range tbl.Columns {
			if strings.EqualFold(c.Name, colName) {
				return fmt.Errorf("duplicate column name: %s", colName)
			}
		}
		tbl.Columns = append(tbl.Columns, newCol)
		return emitSchema(ectx, tbl)
	}); err != nil {
		return nil, true, err
	}

	// Redeclare the vtab on the issuing session so its SQLite side
	// learns about the new column. Other sessions pick up v.schema
	// via refreshSchema on their next query plan but will see "no
	// such column" from SQLite's own declared schema until reconnect.
	if err := redeclareVTable(sess.conn, name, func() (*tableSchema, error) {
		return loadSchema(s.engine.NewContext(), name)
	}); err != nil {
		return nil, true, fmt.Errorf("redeclare vtab on session: %w", err)
	}

	sess.log.Info("sql column added", "table", name, "column", colName, "type", declType)
	s.bumpSchemaEpoch(sess)

	fn := func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		return w.Complete("ALTER TABLE")
	}
	return wire.NewStatement(fn), true, nil
}

// bumpSchemaEpoch advances the server-global schema epoch and
// records the new value on the issuing session so its next query
// doesn't redundantly re-sync against changes it just authored.
func (s *Server) bumpSchemaEpoch(sess *session) {
	ep := s.schemaEpoch.Add(1)
	if sess != nil {
		sess.schemaEpoch = ep
	}
}

// parseAddColumnSpec parses the tail of "ADD [COLUMN] <spec>" where
// <spec> is `<name> [<type>] [NOT NULL] [DEFAULT <literal>]`.
// CHECK, COLLATE, REFERENCES, PRIMARY KEY, UNIQUE, GENERATED are
// rejected — they mutate semantics beyond the metadata-only path.
func parseAddColumnSpec(rest string) (name, declType string, notNull bool, def *defaultValue, err error) {
	tokens := strings.Fields(rest)
	if len(tokens) == 0 {
		return "", "", false, nil, fmt.Errorf("ALTER TABLE ADD: missing column spec")
	}
	i := 0
	if strings.EqualFold(tokens[i], "COLUMN") {
		i++
	}
	if i >= len(tokens) {
		return "", "", false, nil, fmt.Errorf("ALTER TABLE ADD: missing column name")
	}
	name = trimIdent(tokens[i])
	i++
	for i < len(tokens) {
		u := strings.ToUpper(tokens[i])
		switch u {
		case "NOT":
			if i+1 >= len(tokens) || !strings.EqualFold(tokens[i+1], "NULL") {
				return "", "", false, nil, fmt.Errorf("ALTER TABLE ADD: expected NULL after NOT")
			}
			notNull = true
			i += 2
			continue
		case "NULL":
			i++
			continue
		case "DEFAULT":
			if i+1 >= len(tokens) {
				return "", "", false, nil, fmt.Errorf("ALTER TABLE ADD: DEFAULT requires a value")
			}
			parsed, consumed, perr := parseDefaultLiteral(tokens[i+1:])
			if perr != nil {
				return "", "", false, nil, fmt.Errorf("ALTER TABLE ADD: %w", perr)
			}
			def = parsed
			i += 1 + consumed
			continue
		case "PRIMARY", "UNIQUE", "CHECK", "COLLATE", "REFERENCES", "GENERATED":
			return "", "", false, nil,
				fmt.Errorf("ALTER TABLE ADD: %s constraint not supported in this phase", u)
		}
		// Anything else is assumed to be the type name. Accept
		// concatenated tokens so `VARCHAR (255)` collapses.
		if declType == "" {
			declType = tokens[i]
		} else {
			declType += " " + tokens[i]
		}
		i++
	}
	if name == "" {
		return "", "", false, nil, fmt.Errorf("ALTER TABLE ADD: empty column name")
	}
	return name, declType, notNull, def, nil
}

// parseDefaultLiteral consumes a default-value literal from the
// token stream: NULL, TRUE/FALSE, a numeric literal, or a
// single-quoted string literal (which the tokeniser may have split
// across multiple tokens if the string contained spaces — we rejoin
// until we see a closing quote). Returns the parsed value and the
// number of tokens consumed.
func parseDefaultLiteral(tokens []string) (*defaultValue, int, error) {
	if len(tokens) == 0 {
		return nil, 0, fmt.Errorf("DEFAULT: missing literal")
	}
	first := strings.TrimRight(tokens[0], ",)")
	upper := strings.ToUpper(first)
	switch upper {
	case "NULL":
		return &defaultValue{Kind: "null"}, 1, nil
	case "TRUE":
		return &defaultValue{Kind: "int", Int: 1}, 1, nil
	case "FALSE":
		return &defaultValue{Kind: "int", Int: 0}, 1, nil
	}
	// Numeric literal?
	if n, err := strconv.ParseInt(first, 10, 64); err == nil {
		return &defaultValue{Kind: "int", Int: n}, 1, nil
	}
	if f, err := strconv.ParseFloat(first, 64); err == nil {
		return &defaultValue{Kind: "float", Float: f}, 1, nil
	}
	// Single-quoted string literal, possibly split across tokens
	// by the simple splitter if the quoted text contains spaces.
	if strings.HasPrefix(first, "'") {
		if strings.HasSuffix(first, "'") && len(first) >= 2 {
			return &defaultValue{
				Kind: "text",
				Text: unescapeSQL(first[1 : len(first)-1]),
			}, 1, nil
		}
		// Re-join subsequent tokens until we find the closing quote.
		joined := tokens[0]
		consumed := 1
		for consumed < len(tokens) && !strings.HasSuffix(joined, "'") {
			joined = joined + " " + tokens[consumed]
			consumed++
		}
		if !strings.HasSuffix(joined, "'") {
			return nil, 0, fmt.Errorf("DEFAULT: unterminated string literal")
		}
		return &defaultValue{
			Kind: "text",
			Text: unescapeSQL(joined[1 : len(joined)-1]),
		}, consumed, nil
	}
	return nil, 0, fmt.Errorf("DEFAULT: unsupported literal %q (only NULL, numbers, 'text', TRUE/FALSE in this phase)", first)
}

// unescapeSQL un-doubles embedded single quotes per SQL string-literal rules.
func unescapeSQL(s string) string {
	return strings.ReplaceAll(s, "''", "'")
}

// redeclareVTable drops and recreates the vtab on the given SQLite
// conn so the latest column list takes effect. Any prepared
// statements the session had open against this table are invalidated
// — zombiezen surfaces that as a prepare-time error on their next
// execution, matching SQLITE_SCHEMA semantics.
func redeclareVTable(conn *sqlite.Conn, name string, loader func() (*tableSchema, error)) error {
	if err := execSimple(conn, "DROP TABLE "+quoteIdent(name)); err != nil {
		return fmt.Errorf("drop vtab: %w", err)
	}
	schema, err := loader()
	if err != nil {
		return fmt.Errorf("reload schema: %w", err)
	}
	return registerVTable(conn, schema)
}

// handleDropTable removes a swytch-backed table: scans its row
// namespace for live keys, emits REMOVE for each row's column fields,
// REMOVEs the schema-metadata fields, REMOVEs the entry from the
// replicated table list, then issues DROP TABLE on the current
// session's SQLite conn so subsequent queries on this session see
// the schema change.
//
// Other sessions still have the vtab registered (cross-session
// schema propagation is a later phase); queries against the dropped
// table from those sessions will start returning empty results once
// the swytch state is gone.
func (s *Server) handleDropTable(sess *session, ddl string) (*wire.PreparedStatement, bool, error) {
	name, ifExists, err := parseDropTable(ddl)
	if err != nil {
		return nil, true, err
	}

	// Pre-flight the schema read outside the transactional body so
	// IF EXISTS on a missing table is a cheap no-op (no tx overhead
	// for the common case). Inside the RMW body we pin the read on
	// the schema key so concurrent DROPs of the same table abort.
	preflight := s.engine.NewContext()
	schema, err := loadSchema(preflight, name)
	if err != nil {
		if errors.Is(err, ErrNoSuchTable) {
			if ifExists {
				fn := func(_ context.Context, writer wire.DataWriter, _ []wire.Parameter) error {
					return writer.Complete("DROP TABLE")
				}
				return wire.NewStatement(fn), true, nil
			}
			return nil, true, fmt.Errorf("%w: %s", ErrNoSuchTable, name)
		}
		return nil, true, err
	}

	var rowCount int
	if err := runRMW(s.engine, "drop table", func(ectx *effects.Context) error {
		// Pin the read dep on the schema key so concurrent DROPs of
		// the same table cause one of us to abort rather than both
		// emitting duplicate REMOVEs.
		_, schemaTips, err := ectx.GetSnapshot(schemaKey(name))
		if err != nil {
			return fmt.Errorf("read schema: %w", err)
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(schemaKey(name)),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, schemaTips); err != nil {
			return fmt.Errorf("pin schema dep: %w", err)
		}

		// Enumerate every live row key for this table and REMOVE
		// its column fields (including the PK presence marker).
		rowCount = 0
		var rowKeys []string
		s.engine.ScanKeys("", rowKeyPattern(schema.TableID), func(key string) bool {
			rowKeys = append(rowKeys, key)
			return true
		})
		for _, key := range rowKeys {
			for _, col := range schema.Columns {
				if err := emitKeyedRemove(ectx, key, col.Name); err != nil {
					return fmt.Errorf("emit row-remove %s.%s: %w", name, col.Name, err)
				}
			}
		}
		rowCount = len(rowKeys)

		// Cascade: drop every index owned by this table in the same
		// transaction. Each cascaded index fans out into entry REMOVEs
		// plus metadata cleanup.
		for _, idx := range schema.Indexes {
			if err := dropIndexEntries(s.engine, ectx, idx.IndexID); err != nil {
				return fmt.Errorf("cascade drop index %s: %w", idx.Name, err)
			}
			if err := emitIndexMetadataRemove(ectx, idx.Name); err != nil {
				return fmt.Errorf("cascade drop index %s metadata: %w", idx.Name, err)
			}
		}

		// Remove schema-metadata fields and the table-list entry.
		for _, field := range []string{schemaFieldTableID, schemaFieldColumns, schemaFieldPK, schemaFieldIndexes} {
			if err := emitKeyedRemove(ectx, schemaKey(name), field); err != nil {
				return fmt.Errorf("emit schema-remove %s.%s: %w", name, field, err)
			}
		}
		if err := emitKeyedRemove(ectx, tableListKey, name); err != nil {
			return fmt.Errorf("emit table-list remove %s: %w", name, err)
		}
		return nil
	}); err != nil {
		return nil, true, err
	}

	// Update the session's SQLite schema cache so subsequent queries
	// on THIS session see the drop immediately. SQLite calls our
	// vtab's Destroy() during this.
	if err := execSimple(sess.conn, "DROP TABLE "+quoteIdent(name)); err != nil {
		return nil, true, fmt.Errorf("drop vtab on session: %w", err)
	}
	s.bumpSchemaEpoch(sess)

	sess.log.Info("sql table dropped",
		"table", name,
		"rows_removed", rowCount,
	)

	fn := func(_ context.Context, writer wire.DataWriter, _ []wire.Parameter) error {
		return writer.Complete("DROP TABLE")
	}
	return wire.NewStatement(fn), true, nil
}

// handleCreateIndex parses CREATE [UNIQUE] INDEX <name> ON
// <table>(<col>), builds the index entries by scanning rows, and
// writes the index metadata + entries + updated table schema inside
// a single runRMW transaction.
func (s *Server) handleCreateIndex(sess *session, ddl string) (*wire.PreparedStatement, bool, error) {
	name, table, columns, unique, err := parseCreateIndex(ddl)
	if err != nil {
		return nil, true, err
	}

	// Preflight the schema read outside the tx so we can produce a
	// clean "no such table" error without burning a retry.
	preflight := s.engine.NewContext()
	tbl, err := loadSchema(preflight, table)
	if err != nil {
		return nil, true, fmt.Errorf("CREATE INDEX %s: %w", name, err)
	}
	// Resolve every indexed column to an ordinal + affinity. Reject
	// duplicates early — SQLite itself allows duplicate columns in an
	// index but it's a foot-gun.
	colOrdinals := make([]int, len(columns))
	colAffinities := make([]string, len(columns))
	for ci, colName := range columns {
		ord := -1
		for i, c := range tbl.Columns {
			if c.Name == colName {
				ord = i
				colAffinities[ci] = c.Affinity
				break
			}
		}
		if ord < 0 {
			return nil, true, fmt.Errorf("CREATE INDEX %s: column %q not found on table %s", name, colName, table)
		}
		for j := range ci {
			if colOrdinals[j] == ord {
				return nil, true, fmt.Errorf("CREATE INDEX %s: duplicate column %q", name, colName)
			}
		}
		colOrdinals[ci] = ord
	}
	// Duplicate index name?
	for _, existing := range tbl.Indexes {
		if existing.Name == name {
			return nil, true, fmt.Errorf("CREATE INDEX: index %q already exists", name)
		}
	}

	idx := indexSchema{
		Name:    name,
		IndexID: deriveIndexID(time.Now(), s.engine.NodeID(), name),
		Table:   table,
		TableID: tbl.TableID,
		Columns: columns,
		Unique:  unique,
	}

	if err := runRMW(s.engine, "create index", func(ectx *effects.Context) error {
		// Pin the table schema so a concurrent DROP TABLE or another
		// CREATE INDEX aborts one of us.
		_, schemaTips, err := ectx.GetSnapshot(schemaKey(table))
		if err != nil {
			return fmt.Errorf("read table schema: %w", err)
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(schemaKey(table)),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, schemaTips); err != nil {
			return fmt.Errorf("pin schema dep: %w", err)
		}

		// Index metadata + registry.
		if err := emitIndexMetadata(ectx, &idx); err != nil {
			return err
		}

		// Scan the table's rows and emit an entry for each row whose
		// indexed column is non-NULL. Track values we've seen so we
		// can reject the CREATE if it would violate UNIQUE.
		var seen map[string]string
		if unique {
			seen = make(map[string]string)
		}
		var rowKeys []string
		s.engine.ScanKeys("", rowKeyPattern(tbl.TableID), func(key string) bool {
			rowKeys = append(rowKeys, key)
			return true
		})
		for _, rk := range rowKeys {
			pk, ok := parseRowKey(rk, tbl.TableID)
			if !ok {
				continue
			}
			rowSnap, _, err := ectx.GetSnapshot(rk)
			if err != nil {
				return fmt.Errorf("read row %s: %w", rk, err)
			}
			if rowSnap == nil {
				continue
			}
			// Assemble the tuple of indexed-column values. SQL
			// semantics: if ANY component is NULL, skip the entry —
			// a row with a NULL in any indexed column is unreachable
			// via the index (NULL ≠ NULL in comparisons).
			vals := make([]sqlite.Value, len(columns))
			anyNull := false
			for ci, cName := range columns {
				el, ok := rowSnap.NetAdds[cName]
				if !ok || el.Data == nil {
					anyNull = true
					break
				}
				vals[ci] = decodeValue(el.Data, colAffinities[ci])
				if vals[ci].Type() == sqlite.TypeNull {
					anyNull = true
					break
				}
			}
			if anyNull {
				continue
			}
			encoded := encodeIndexKeyTuple(vals, colAffinities)
			if unique {
				if otherPK, dup := seen[string(encoded)]; dup {
					return fmt.Errorf("%w: CREATE UNIQUE INDEX %s would collide on (%s, %s)",
						ErrUniqueViolation, name, otherPK, pk)
				}
				seen[string(encoded)] = pk
			}
			if err := emitIndexEntryInsert(ectx, idx.IndexID, encoded, pk); err != nil {
				return fmt.Errorf("emit index entry: %w", err)
			}
		}

		// Update the table schema to include this index.
		tbl.Indexes = append(tbl.Indexes, idx)
		return emitSchema(ectx, tbl)
	}); err != nil {
		return nil, true, err
	}

	sess.log.Info("sql index created",
		"index", name,
		"table", table,
		"columns", columns,
		"unique", unique,
	)
	s.bumpSchemaEpoch(sess)

	fn := func(_ context.Context, writer wire.DataWriter, _ []wire.Parameter) error {
		return writer.Complete("CREATE INDEX")
	}
	return wire.NewStatement(fn), true, nil
}

// handleDropIndex removes an index: enumerates all entry keys,
// REMOVEs each field, REMOVEs metadata, REMOVEs from registry, and
// re-emits the parent table's schema with the index deleted.
func (s *Server) handleDropIndex(sess *session, ddl string) (*wire.PreparedStatement, bool, error) {
	name, ifExists, err := parseDropIndex(ddl)
	if err != nil {
		return nil, true, err
	}

	preflight := s.engine.NewContext()
	meta, err := loadIndexSchema(preflight, name)
	if err != nil {
		if errors.Is(err, ErrNoSuchIndex) {
			if ifExists {
				fn := func(_ context.Context, writer wire.DataWriter, _ []wire.Parameter) error {
					return writer.Complete("DROP INDEX")
				}
				return wire.NewStatement(fn), true, nil
			}
			return nil, true, fmt.Errorf("%w: %s", ErrNoSuchIndex, name)
		}
		return nil, true, err
	}

	if err := runRMW(s.engine, "drop index", func(ectx *effects.Context) error {
		_, metaTips, err := ectx.GetSnapshot(indexMetaKey(name))
		if err != nil {
			return fmt.Errorf("read index meta: %w", err)
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(indexMetaKey(name)),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, metaTips); err != nil {
			return fmt.Errorf("pin index-meta dep: %w", err)
		}

		if err := dropIndexEntries(s.engine, ectx, meta.IndexID); err != nil {
			return err
		}
		if err := emitIndexMetadataRemove(ectx, name); err != nil {
			return err
		}

		// Re-emit the parent table's schema with this index removed.
		tbl, err := loadSchema(ectx, meta.Table)
		if err != nil {
			return fmt.Errorf("reload parent table %s: %w", meta.Table, err)
		}
		filtered := tbl.Indexes[:0]
		for _, existing := range tbl.Indexes {
			if existing.Name != name {
				filtered = append(filtered, existing)
			}
		}
		tbl.Indexes = filtered
		return emitSchema(ectx, tbl)
	}); err != nil {
		return nil, true, err
	}

	sess.log.Info("sql index dropped", "index", name, "table", meta.Table)
	s.bumpSchemaEpoch(sess)

	fn := func(_ context.Context, writer wire.DataWriter, _ []wire.Parameter) error {
		return writer.Complete("DROP INDEX")
	}
	return wire.NewStatement(fn), true, nil
}

// handleCreateView persists a CREATE VIEW statement: parse the name
// and body, reject name collisions with existing tables or views
// (honouring IF NOT EXISTS), and register the view on the calling
// session's SQLite connection. Views are metadata-only — they have
// no physical row or index-entry storage — so the RMW body just
// pins the view registry and table registry (to detect concurrent
// renames into our name), then emits the metadata + registry entry.
//
// SQLite itself compiles the CREATE VIEW lazily: the referenced
// tables/views don't need to exist at CREATE time. That lets us
// replay views on session open without coordinating dependency order
// across view-referencing-view chains, and it means DROP TABLE does
// not cascade-drop dependent views — the view simply starts failing
// at query time with "no such table", matching SQLite's native
// behaviour.
func (s *Server) handleCreateView(sess *session, ddl string) (*wire.PreparedStatement, bool, error) {
	name, ifNotExists, body, err := parseCreateView(ddl)
	if err != nil {
		return nil, true, fmt.Errorf("parse CREATE VIEW: %w", err)
	}

	// Preflight: honour IF NOT EXISTS cheaply and reject name
	// collisions with an existing table up-front. The RMW body
	// re-checks under pinned tips so concurrent creators can't race
	// past this.
	preflight := s.engine.NewContext()
	if _, lerr := loadView(preflight, name); lerr == nil {
		if ifNotExists {
			fn := func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
				return w.Complete("CREATE VIEW")
			}
			return wire.NewStatement(fn), true, nil
		}
		return nil, true, fmt.Errorf("%w: %s", ErrViewExists, name)
	} else if !errors.Is(lerr, ErrNoSuchView) {
		return nil, true, lerr
	}
	if _, lerr := loadSchema(preflight, name); lerr == nil {
		return nil, true, fmt.Errorf("%w: %s (name already in use by a table)", ErrTableExists, name)
	}

	v := &viewSchema{Name: name, Body: body}
	createdInTx := false

	if err := runRMW(s.engine, "create view", func(ectx *effects.Context) error {
		// View registry — detect concurrent creators of the same
		// view and pin the key so one of us aborts cleanly.
		viewSnap, viewTips, err := ectx.GetSnapshot(viewListKey)
		if err != nil {
			return fmt.Errorf("read view list: %w", err)
		}
		if viewSnap != nil {
			if _, present := viewSnap.NetAdds[name]; present {
				if ifNotExists {
					createdInTx = false
					return nil
				}
				return fmt.Errorf("%w: %s", ErrViewExists, name)
			}
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(viewListKey),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, viewTips); err != nil {
			return fmt.Errorf("pin view-list dep: %w", err)
		}
		// Table registry — a racing CREATE TABLE of the same name
		// must also abort one of us so the winner's kind is
		// unambiguous across the cluster.
		tableSnap, tableTips, err := ectx.GetSnapshot(tableListKey)
		if err != nil {
			return fmt.Errorf("read table list: %w", err)
		}
		if tableSnap != nil {
			if _, present := tableSnap.NetAdds[name]; present {
				return fmt.Errorf("%w: %s (name already in use by a table)", ErrTableExists, name)
			}
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(tableListKey),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tableTips); err != nil {
			return fmt.Errorf("pin table-list dep: %w", err)
		}
		createdInTx = true
		return emitView(ectx, v)
	}); err != nil {
		return nil, true, err
	}

	// Register on this session immediately. When IF NOT EXISTS
	// collapsed to a no-op above, the view already existed — we
	// skip the session-side CREATE VIEW and let any other session
	// that issued it keep its registration.
	if createdInTx {
		if err := registerView(sess.conn, v); err != nil {
			return nil, true, fmt.Errorf("register view on session: %w", err)
		}
		s.bumpSchemaEpoch(sess)
		sess.log.Info("sql view created", "view", name)
	}

	fn := func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		return w.Complete("CREATE VIEW")
	}
	return wire.NewStatement(fn), true, nil
}

// handleDropView removes a view from swytch and from the calling
// session's SQLite conn. Other sessions pick the drop up on their
// next query via ensureFreshVTabs (the schema epoch bump triggers a
// refresh that notices the view is gone from the registry).
func (s *Server) handleDropView(sess *session, ddl string) (*wire.PreparedStatement, bool, error) {
	name, ifExists, err := parseDropView(ddl)
	if err != nil {
		return nil, true, err
	}

	preflight := s.engine.NewContext()
	if _, err := loadView(preflight, name); err != nil {
		if errors.Is(err, ErrNoSuchView) {
			if ifExists {
				fn := func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
					return w.Complete("DROP VIEW")
				}
				return wire.NewStatement(fn), true, nil
			}
			return nil, true, fmt.Errorf("%w: %s", ErrNoSuchView, name)
		}
		return nil, true, err
	}

	if err := runRMW(s.engine, "drop view", func(ectx *effects.Context) error {
		_, metaTips, err := ectx.GetSnapshot(viewMetaKey(name))
		if err != nil {
			return fmt.Errorf("read view meta: %w", err)
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(viewMetaKey(name)),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, metaTips); err != nil {
			return fmt.Errorf("pin view-meta dep: %w", err)
		}
		return emitViewRemove(ectx, name)
	}); err != nil {
		return nil, true, err
	}

	if err := execSimple(sess.conn, "DROP VIEW "+quoteIdent(name)); err != nil {
		return nil, true, fmt.Errorf("drop view on session: %w", err)
	}
	s.bumpSchemaEpoch(sess)
	sess.log.Info("sql view dropped", "view", name)

	fn := func(_ context.Context, w wire.DataWriter, _ []wire.Parameter) error {
		return w.Complete("DROP VIEW")
	}
	return wire.NewStatement(fn), true, nil
}

// parseCreateView splits a CREATE VIEW statement into its name and
// the text that follows — typically "AS <select>" or "(col1, col2)
// AS <select>". The body is preserved verbatim so SQLite sees
// byte-identical SQL on every replay, including whitespace and
// quoting inside the SELECT. Character-level parsing (rather than
// strings.Fields) preserves embedded string literals with spaces.
func parseCreateView(ddl string) (name string, ifNotExists bool, body string, err error) {
	s := strings.TrimLeft(ddl, " \t\r\n")
	var ok bool
	s, ok = eatKeyword(s, "CREATE")
	if !ok {
		return "", false, "", fmt.Errorf("malformed CREATE VIEW: %q", ddl)
	}
	s, ok = eatKeyword(s, "VIEW")
	if !ok {
		return "", false, "", fmt.Errorf("malformed CREATE VIEW: expected VIEW: %q", ddl)
	}
	if rest, had := eatKeyword(s, "IF"); had {
		ifNotExists = true
		s = rest
		s, ok = eatKeyword(s, "NOT")
		if !ok {
			return "", false, "", fmt.Errorf("CREATE VIEW IF: expected NOT: %q", ddl)
		}
		s, ok = eatKeyword(s, "EXISTS")
		if !ok {
			return "", false, "", fmt.Errorf("CREATE VIEW IF NOT: expected EXISTS: %q", ddl)
		}
	}
	name, s, err = consumeIdent(s)
	if err != nil {
		return "", false, "", fmt.Errorf("CREATE VIEW: %w", err)
	}
	body = strings.TrimSpace(strings.TrimRight(strings.TrimSpace(s), ";"))
	if body == "" {
		return "", false, "", fmt.Errorf("CREATE VIEW %s: missing body", name)
	}
	return name, ifNotExists, body, nil
}

// parseDropView extracts the view name plus the IF EXISTS flag.
func parseDropView(ddl string) (name string, ifExists bool, err error) {
	s := strings.TrimLeft(ddl, " \t\r\n")
	var ok bool
	s, ok = eatKeyword(s, "DROP")
	if !ok {
		return "", false, fmt.Errorf("malformed DROP VIEW: %q", ddl)
	}
	s, ok = eatKeyword(s, "VIEW")
	if !ok {
		return "", false, fmt.Errorf("malformed DROP VIEW: expected VIEW: %q", ddl)
	}
	if rest, had := eatKeyword(s, "IF"); had {
		s, ok = eatKeyword(rest, "EXISTS")
		if !ok {
			return "", false, fmt.Errorf("DROP VIEW IF: expected EXISTS: %q", ddl)
		}
		ifExists = true
	}
	name, s, err = consumeIdent(s)
	if err != nil {
		return "", false, fmt.Errorf("DROP VIEW: %w", err)
	}
	tail := strings.TrimSpace(strings.TrimRight(strings.TrimSpace(s), ";"))
	if tail != "" {
		return "", false, fmt.Errorf("DROP VIEW: unexpected trailing text %q", tail)
	}
	return name, ifExists, nil
}

// eatKeyword consumes the given keyword (case-insensitive) from the
// start of s. The match requires a word boundary — the character
// immediately after the keyword must not be an identifier character
// — so "CREATE" doesn't swallow the head of "CREATEVIEW". Returns
// the remainder with any following whitespace trimmed.
func eatKeyword(s, keyword string) (string, bool) {
	if len(s) < len(keyword) {
		return s, false
	}
	if !strings.EqualFold(s[:len(keyword)], keyword) {
		return s, false
	}
	if len(s) > len(keyword) {
		c := s[len(keyword)]
		if (c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') ||
			c == '_' {
			return s, false
		}
	}
	return strings.TrimLeft(s[len(keyword):], " \t\r\n"), true
}

// consumeIdent reads a SQL identifier from the start of s — bare,
// "double-quoted", `backtick-quoted`, or [bracket-quoted]. Returns
// the unquoted name plus any remaining input with leading whitespace
// trimmed. Embedded "" in double-quoted identifiers is un-doubled
// per SQL-standard rules.
func consumeIdent(s string) (name, rest string, err error) {
	if s == "" {
		return "", "", fmt.Errorf("expected identifier")
	}
	switch s[0] {
	case '"':
		end := 1
		for end < len(s) {
			if s[end] == '"' {
				if end+1 < len(s) && s[end+1] == '"' {
					end += 2
					continue
				}
				n := strings.ReplaceAll(s[1:end], `""`, `"`)
				return n, strings.TrimLeft(s[end+1:], " \t\r\n"), nil
			}
			end++
		}
		return "", "", fmt.Errorf("unterminated double-quoted identifier")
	case '`':
		idx := strings.IndexByte(s[1:], '`')
		if idx < 0 {
			return "", "", fmt.Errorf("unterminated backtick identifier")
		}
		return s[1 : 1+idx], strings.TrimLeft(s[1+idx+1:], " \t\r\n"), nil
	case '[':
		idx := strings.IndexByte(s[1:], ']')
		if idx < 0 {
			return "", "", fmt.Errorf("unterminated bracket identifier")
		}
		return s[1 : 1+idx], strings.TrimLeft(s[1+idx+1:], " \t\r\n"), nil
	}
	end := 0
	for end < len(s) {
		c := s[end]
		if c == '_' || c == '$' ||
			(c >= 'a' && c <= 'z') ||
			(c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') {
			end++
			continue
		}
		break
	}
	if end == 0 {
		return "", "", fmt.Errorf("expected identifier")
	}
	return s[:end], strings.TrimLeft(s[end:], " \t\r\n"), nil
}

// dropIndexEntries enumerates every key under the index's prefix and
// REMOVEs each field; caller is responsible for transaction framing.
func dropIndexEntries(eng *effects.Engine, ectx *effects.Context, indexID []byte) error {
	var entryKeys []string
	eng.ScanKeys("", indexKeyPattern(indexID), func(key string) bool {
		entryKeys = append(entryKeys, key)
		return true
	})
	for _, ek := range entryKeys {
		snap, _, err := ectx.GetSnapshot(ek)
		if err != nil {
			return fmt.Errorf("read index entry %s: %w", ek, err)
		}
		if snap == nil {
			continue
		}
		for field := range snap.NetAdds {
			if err := emitKeyedRemove(ectx, ek, field); err != nil {
				return fmt.Errorf("remove index entry %s.%s: %w", ek, field, err)
			}
		}
	}
	return nil
}

// parseCreateIndex extracts the index name, table, indexed
// column list and UNIQUE flag from a CREATE [UNIQUE] INDEX
// statement. Composite columns are returned in declaration order —
// an index on `(a, b)` returns ["a", "b"].
func parseCreateIndex(ddl string) (name, table string, columns []string, unique bool, err error) {
	// Normalise whitespace for easier token matching. Minimum valid
	// form is `CREATE INDEX <name> ON <table>(<col>)` = 5 tokens.
	tokens := strings.Fields(ddl)
	if len(tokens) < 5 {
		return "", "", nil, false, fmt.Errorf("malformed CREATE INDEX: %q", ddl)
	}
	i := 1 // skip "CREATE"
	if strings.EqualFold(tokens[i], "UNIQUE") {
		unique = true
		i++
	}
	if !strings.EqualFold(tokens[i], "INDEX") {
		return "", "", nil, false, fmt.Errorf("malformed CREATE INDEX (expected INDEX): %q", ddl)
	}
	i++
	if strings.EqualFold(tokens[i], "IF") {
		// CREATE INDEX IF NOT EXISTS — swallow the three keywords.
		if len(tokens) < i+3 || !strings.EqualFold(tokens[i+1], "NOT") || !strings.EqualFold(tokens[i+2], "EXISTS") {
			return "", "", nil, false, fmt.Errorf("malformed CREATE INDEX IF clause: %q", ddl)
		}
		i += 3
	}
	// Next tokens: <name> ON <table>(<col>, …)
	name = trimIdent(tokens[i])
	i++
	if i >= len(tokens) || !strings.EqualFold(tokens[i], "ON") {
		return "", "", nil, false, fmt.Errorf("CREATE INDEX missing ON: %q", ddl)
	}
	i++
	tail := strings.Join(tokens[i:], " ")
	tail = strings.TrimRight(tail, ";")
	open := strings.Index(tail, "(")
	close := strings.LastIndex(tail, ")")
	if open < 0 || close < 0 || close < open {
		return "", "", nil, false, fmt.Errorf("CREATE INDEX missing (col): %q", ddl)
	}
	table = trimIdent(strings.TrimSpace(tail[:open]))
	inside := strings.TrimSpace(tail[open+1 : close])
	if inside == "" {
		return "", "", nil, false, fmt.Errorf("CREATE INDEX: empty column list: %q", ddl)
	}
	for part := range strings.SplitSeq(inside, ",") {
		col := trimIdent(strings.TrimSpace(part))
		if col == "" {
			return "", "", nil, false, fmt.Errorf("CREATE INDEX: empty column in list: %q", ddl)
		}
		columns = append(columns, col)
	}
	if name == "" || table == "" || len(columns) == 0 {
		return "", "", nil, false, fmt.Errorf("CREATE INDEX: empty name/table/columns: %q", ddl)
	}
	return name, table, columns, unique, nil
}

// parseDropIndex extracts the index name and IF EXISTS flag.
func parseDropIndex(ddl string) (name string, ifExists bool, err error) {
	tokens := strings.Fields(ddl)
	if len(tokens) < 3 {
		return "", false, fmt.Errorf("malformed DROP INDEX: %q", ddl)
	}
	i := 2
	if strings.EqualFold(tokens[i], "IF") {
		if len(tokens) < i+3 || !strings.EqualFold(tokens[i+1], "EXISTS") {
			return "", false, fmt.Errorf("malformed DROP INDEX IF clause: %q", ddl)
		}
		ifExists = true
		i += 2
	}
	raw := strings.TrimRight(tokens[i], ";")
	name = trimIdent(raw)
	if name == "" {
		return "", false, fmt.Errorf("DROP INDEX missing name: %q", ddl)
	}
	return name, ifExists, nil
}

// trimIdent strips double-quotes, backticks, and square brackets
// from a bare SQL identifier token.
func trimIdent(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 {
		if s[0] == '"' && s[len(s)-1] == '"' {
			return strings.ReplaceAll(s[1:len(s)-1], `""`, `"`)
		}
		if s[0] == '`' && s[len(s)-1] == '`' {
			return s[1 : len(s)-1]
		}
		if s[0] == '[' && s[len(s)-1] == ']' {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// parseDropTable extracts the table name and IF EXISTS flag from a
// DROP TABLE statement. SQLite's grammar for DROP TABLE is:
//
//	DROP TABLE [IF EXISTS] [schema.]name
//
// We don't support schema-qualified names (main/temp/attached DBs) in
// phase 3; the bare name is taken.
func parseDropTable(ddl string) (name string, ifExists bool, err error) {
	tokens := strings.Fields(ddl)
	if len(tokens) < 3 {
		return "", false, fmt.Errorf("malformed DROP TABLE: %q", ddl)
	}
	i := 2 // DROP TABLE …
	if strings.EqualFold(tokens[i], "IF") {
		if len(tokens) < 5 || !strings.EqualFold(tokens[i+1], "EXISTS") {
			return "", false, fmt.Errorf("malformed DROP TABLE IF clause: %q", ddl)
		}
		ifExists = true
		i += 2
	}
	raw := strings.TrimRight(tokens[i], ";")
	raw = strings.Trim(raw, `"`)
	if raw == "" {
		return "", false, fmt.Errorf("missing table name in DROP TABLE: %q", ddl)
	}
	return raw, ifExists, nil
}

// handleCreateTable parses the DDL via the server's parser conn,
// writes the table schema to swytch, and registers the corresponding
// virtual table on the calling session's SQLite connection.
//
// The "does this table already exist" check plus the metadata write
// forms a read-modify-write, so the whole storage side runs inside
// runRMW: a concurrent CREATE TABLE for the same name on another
// node would cause one side to abort and return a clean duplicate
// error instead of both committing with fork state.
func (s *Server) handleCreateTable(sess *session, ddl string) (*wire.PreparedStatement, bool, error) {
	schema, err := s.parseCreateTable(ddl)
	if err != nil {
		return nil, true, fmt.Errorf("parse CREATE TABLE: %w", err)
	}

	if err := runRMW(s.engine, "create table", func(ectx *effects.Context) error {
		listSnap, listTips, err := ectx.GetSnapshot(tableListKey)
		if err != nil {
			return fmt.Errorf("read table list: %w", err)
		}
		if listSnap != nil {
			if _, present := listSnap.NetAdds[schema.Name]; present {
				return fmt.Errorf("%w: %s", ErrTableExists, schema.Name)
			}
		}
		// Pin the read dep on the table list so a concurrent
		// CREATE TABLE with the same name aborts one of us.
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(tableListKey),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, listTips); err != nil {
			return fmt.Errorf("pin table-list dep: %w", err)
		}
		return emitSchema(ectx, schema)
	}); err != nil {
		return nil, true, err
	}

	// Register on this session's conn immediately so subsequent
	// queries on the same session can reference the table without
	// reopening. Other sessions will pick it up when they next open.
	if err := registerVTable(sess.conn, schema); err != nil {
		return nil, true, fmt.Errorf("register vtab on session: %w", err)
	}
	s.bumpSchemaEpoch(sess)

	sess.log.Info("sql table created",
		"table", schema.Name,
		"columns", len(schema.Columns),
		"pk_columns", schema.PKColumns,
	)

	fn := func(_ context.Context, writer wire.DataWriter, _ []wire.Parameter) error {
		return writer.Complete("CREATE TABLE")
	}
	return wire.NewStatement(fn), true, nil
}

// parseCreateTable runs the given CREATE TABLE statement against the
// server's dedicated parser connection, queries pragma_table_info to
// extract the column list and primary-key position, drops the table,
// and returns a tableSchema ready to be persisted.
//
// Using SQLite itself to parse the DDL means we accept exactly the
// grammar SQLite accepts without writing our own tokenizer.
func (s *Server) parseCreateTable(ddl string) (*tableSchema, error) {
	s.parserMu.Lock()
	defer s.parserMu.Unlock()

	if err := execSimple(s.parserConn, ddl); err != nil {
		return nil, fmt.Errorf("sqlite parse: %w", err)
	}

	// Find the table name. SQLite stores it in sqlite_master; we
	// fetch the most-recently-created table (which must be ours).
	var tableName string
	err := iterateRows(s.parserConn,
		"SELECT name FROM sqlite_master WHERE type='table' ORDER BY rowid DESC LIMIT 1",
		func(stmt *sqlite.Stmt) error {
			tableName = stmt.ColumnText(0)
			return nil
		})
	if err != nil {
		return nil, fmt.Errorf("sqlite_master: %w", err)
	}
	if tableName == "" {
		return nil, fmt.Errorf("unable to determine table name from DDL")
	}

	// pragma_table_info gives column metadata including declared
	// type and primary-key position. SQLite reports pk=1 for the
	// first PK column, pk=2 for the second, etc. For composite
	// PRIMARY KEY (a, b) we collect [ord(a), ord(b)] in pk order.
	var cols []columnSchema
	type pkEntry struct{ pkOrder, cid int }
	var pkEntries []pkEntry
	err = iterateRows(s.parserConn,
		"SELECT cid, name, type, \"notnull\", pk FROM pragma_table_info('"+tableName+"')",
		func(stmt *sqlite.Stmt) error {
			cid := int(stmt.ColumnInt64(0))
			name := stmt.ColumnText(1)
			declType := stmt.ColumnText(2)
			notNull := stmt.ColumnInt64(3) != 0
			pk := int(stmt.ColumnInt64(4))
			col := columnSchema{
				Name:     name,
				Affinity: sqliteAffinity(declType),
				DeclType: declType,
				NotNull:  notNull,
				IsPK:     pk > 0,
			}
			cols = append(cols, col)
			if pk > 0 {
				pkEntries = append(pkEntries, pkEntry{pkOrder: pk, cid: cid})
			}
			return nil
		})
	if err != nil {
		// Attempt to clean the parser conn so the next DDL that
		// reuses this name doesn't fail with "table already exists".
		// If the cleanup itself fails, surface both errors.
		if dropErr := execSimple(s.parserConn, "DROP TABLE IF EXISTS "+quoteIdent(tableName)); dropErr != nil {
			return nil, errors.Join(
				fmt.Errorf("pragma_table_info: %w", err),
				fmt.Errorf("parser cleanup: %w", dropErr),
			)
		}
		return nil, fmt.Errorf("pragma_table_info: %w", err)
	}

	// Always drop — we don't want the parser conn's schema to grow
	// unbounded and we must ensure the next DDL that reuses the
	// table name succeeds.
	if err := execSimple(s.parserConn, "DROP TABLE "+quoteIdent(tableName)); err != nil {
		return nil, fmt.Errorf("drop parser copy: %w", err)
	}

	if len(cols) == 0 {
		return nil, fmt.Errorf("no columns in table %q", tableName)
	}

	// No user-declared PRIMARY KEY: synthesize an int64 rowid behind
	// the vtab. The row-key suffix will be built from a per-session
	// allocated int64 in the same encoding as a single-column INTEGER
	// PK, so integerRowidFastPath handles these rows unchanged.
	if len(pkEntries) == 0 {
		return &tableSchema{
			Name:        tableName,
			TableID:     deriveTableID(time.Now(), s.engine.NodeID(), tableName),
			Columns:     cols,
			SyntheticPK: true,
		}, nil
	}

	sort.Slice(pkEntries, func(i, j int) bool {
		return pkEntries[i].pkOrder < pkEntries[j].pkOrder
	})
	pkColumns := make([]int, len(pkEntries))
	for i, e := range pkEntries {
		pkColumns[i] = e.cid
	}

	return &tableSchema{
		Name:      tableName,
		TableID:   deriveTableID(time.Now(), s.engine.NodeID(), tableName),
		Columns:   cols,
		PKColumns: pkColumns,
	}, nil
}

// registerVTable issues CREATE VIRTUAL TABLE ... USING swytch(...) on
// the given conn, declaring each column as a vtab argument. For
// composite primary keys the per-column `PRIMARY KEY` clause is
// suppressed in favour of a trailing table-level `PRIMARY KEY (a,
// b)` constraint — SQLite rejects multiple column-level PK clauses.
func registerVTable(conn *sqlite.Conn, schema *tableSchema) error {
	composite := len(schema.PKColumns) > 1
	parts := make([]string, 0, len(schema.Columns)+1)
	for i, c := range schema.Columns {
		parts = append(parts, buildColumnDef(&c, composite && schema.isPKColumn(i)))
	}
	if composite {
		quoted := make([]string, len(schema.PKColumns))
		for i, ord := range schema.PKColumns {
			quoted[i] = quoteIdent(schema.Columns[ord].Name)
		}
		parts = append(parts, "PRIMARY KEY ("+strings.Join(quoted, ", ")+")")
	}
	ddl := fmt.Sprintf(
		"CREATE VIRTUAL TABLE %s USING %s(%s)",
		quoteIdent(schema.Name),
		moduleName,
		strings.Join(parts, ", "),
	)
	return execSimple(conn, ddl)
}

// buildColumnDef reconstructs a column definition string of the form
// "name TYPE [PRIMARY KEY] [NOT NULL]" — sufficient for SQLite to
// understand the column when re-declared inside a CREATE VIRTUAL
// TABLE arg list. When `suppressInlinePK` is true, the column-level
// PRIMARY KEY clause is omitted — used when the caller will emit a
// trailing table-level PK constraint for composite primary keys.
func buildColumnDef(c *columnSchema, suppressInlinePK bool) string {
	var b strings.Builder
	b.WriteString(quoteIdent(c.Name))
	if c.DeclType != "" {
		b.WriteByte(' ')
		b.WriteString(c.DeclType)
	}
	if c.IsPK && !suppressInlinePK {
		b.WriteString(" PRIMARY KEY")
	}
	if c.NotNull {
		b.WriteString(" NOT NULL")
	}
	return b.String()
}

// quoteIdent wraps a SQL identifier in double quotes, escaping any
// embedded quotes per SQL standard rules.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// stripLeadingComments removes any number of -- line comments from the
// start of a query, returning the bytes that follow.
func stripLeadingComments(query string) string {
	s := strings.TrimLeft(query, " \t\r\n")
	for strings.HasPrefix(s, "--") {
		nl := strings.IndexByte(s, '\n')
		if nl < 0 {
			return ""
		}
		s = strings.TrimLeft(s[nl+1:], " \t\r\n")
	}
	return s
}

// execSimple runs a no-result SQL statement on the given conn. If the
// compiled statement does yield rows, they're drained. Finalize
// errors are joined with any step error so neither is dropped.
func execSimple(conn *sqlite.Conn, query string) (retErr error) {
	stmt, _, err := conn.PrepareTransient(query)
	if err != nil {
		return err
	}
	defer func() {
		if err := stmt.Finalize(); err != nil {
			finErr := fmt.Errorf("finalize: %w", err)
			if retErr == nil {
				retErr = finErr
			} else {
				retErr = errors.Join(retErr, finErr)
			}
		}
	}()
	for {
		has, err := stmt.Step()
		if err != nil {
			return err
		}
		if !has {
			return nil
		}
	}
}

// iterateRows runs a SELECT on the given conn and invokes fn for each
// row. fn should not retain the Stmt across calls.
func iterateRows(conn *sqlite.Conn, query string, fn func(*sqlite.Stmt) error) (retErr error) {
	stmt, _, err := conn.PrepareTransient(query)
	if err != nil {
		return err
	}
	defer func() {
		if err := stmt.Finalize(); err != nil {
			finErr := fmt.Errorf("finalize: %w", err)
			if retErr == nil {
				retErr = finErr
			} else {
				retErr = errors.Join(retErr, finErr)
			}
		}
	}()
	for {
		has, err := stmt.Step()
		if err != nil {
			return err
		}
		if !has {
			return nil
		}
		if err := fn(stmt); err != nil {
			return err
		}
	}
}
