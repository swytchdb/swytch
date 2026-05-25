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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
	"github.com/zeebo/xxh3"
	"zombiezen.com/go/sqlite"
)

// moduleName is the SQLite vtab module name advertised via
// CREATE VIRTUAL TABLE foo USING swytch(...).
const moduleName = "swytch"

// newVTabModule constructs the *sqlite.Module instance that is
// registered on every session conn. The same struct can be reused
// across sessions because Connect captures *Server via closure, not
// any per-session state.
func (s *Server) newVTabModule() *sqlite.Module {
	connect := func(conn *sqlite.Conn, opts *sqlite.VTableConnectOptions) (sqlite.VTable, *sqlite.VTableConfig, error) {
		// In zombiezen's API, Args contains only the parenthesised
		// arguments from CREATE VIRTUAL TABLE ... USING swytch(...),
		// i.e. just the column definitions — not the module/db/table
		// prefix that C SQLite gives you. If the caller passed none,
		// we still want to succeed for an empty declaration, but in
		// practice phase 2 always supplies at least one column.
		s.log.Debug("vtab connect",
			"module", opts.ModuleName,
			"db", opts.DatabaseName,
			"table", opts.VTableName,
			"args", opts.Args,
		)
		colDefs := opts.Args
		if len(colDefs) == 0 {
			return nil, nil, fmt.Errorf("swytch vtab %q requires at least one column", opts.VTableName)
		}

		schema, err := loadSchema(s.engine.NewContext(), opts.VTableName)
		if err != nil {
			return nil, nil, fmt.Errorf("load schema %q: %w", opts.VTableName, err)
		}

		declaration := fmt.Sprintf("CREATE TABLE x(%s)", strings.Join(colDefs, ", "))
		return &swytchVTable{
				server:    s,
				name:      opts.VTableName,
				schema:    schema,
				conn:      conn,
				pkByRowID: make(map[int64]string),
			},
			&sqlite.VTableConfig{
				Declaration: declaration,
				// AllowIndirect lets CREATE VIEW reference this vtab.
				// Without it SQLite rejects the SELECT body with
				// "unsafe use of virtual table" to guard against
				// attacker-controlled schemas embedding a hostile
				// module. Our vtabs only read/write through the
				// effects engine and have no side effects outside
				// the swytch keyspace, so they're safe for indirect
				// use.
				AllowIndirect: true,
				// ConstraintSupport is forced off as a workaround
				// for a bug in zombiezen/go/sqlite v1.4.2 where
				// setting it to true re-applies SQLITE_VTAB_DIRECTONLY
				// (instead of CONSTRAINT_SUPPORT), negating the
				// AllowIndirect flag above and breaking CREATE VIEW.
				// We don't rely on SQLite's INSERT-OR-IGNORE /
				// ON CONFLICT handling anyway — unique violations
				// surface as errors from runRMW and propagate up as
				// generic statement failures, which is what pg
				// clients see either way.
				ConstraintSupport: false,
			},
			nil
	}

	return &sqlite.Module{
		Connect:            connect,
		UseConnectAsCreate: true,
	}
}

// swytchVTable implements sqlite.WritableVTable for a single
// swytch-backed table. Reads and writes all go through the effects
// engine; no data lives in SQLite's own storage.
//
// pkByRowID is a reverse index: cursor.RowID() records each (rowid,
// pk) pair it hands out so a subsequent Update/DeleteRow on the same
// vtable can translate the int64 rowid back to the swytch key. It is
// only populated for non-INTEGER PK affinities — INTEGER PK values
// round-trip through strconv directly.
//
// lastRowWrites holds the RowWrite payloads produced by the most
// recent Update/DeleteRow call. Phase 5 will thread these into the
// transactional context for emission inside an ObservationEffect-
// aware Bind; for now the field is scaffolding that lets tests
// inspect the write payload shape.
//
// No mutex: a vtable is tied to a single SQLite conn, which zombiezen
// documents as "A Conn can only be used by one goroutine at a time."
// psql-wire enforces this by never running concurrent queries on a
// session; our own codepaths (initSession, closeSession) never touch
// the conn in parallel with queries.
type swytchVTable struct {
	server *Server
	name   string
	schema *tableSchema

	// conn is the SQLite connection this vtab is attached to, used
	// to resolve the owning pg session at query time (so writes can
	// consult the session's txContext). We can't store the session
	// directly at Connect time because Connect may fire before the
	// conn→session mapping is registered (e.g. during session
	// initialisation's table replay); lazy lookup via the Server's
	// map is initialisation-order agnostic.
	conn *sqlite.Conn

	pkByRowID map[int64]string

	lastRowWrites []RowWrite
}

// refreshSchema pulls the current committed schema for this vtab's
// table from swytch and replaces the cached v.schema. Called from
// BestIndex and Filter so concurrent DDL on other sessions
// (notably CREATE INDEX today, ALTER TABLE in the future) is
// observed without a session reconnect.
//
// Routes the read through the session's txContext when a tx is
// open: inside the tx, prior emits (row-writes, observations) on
// `s/<table>` have chained the committed schema effects through
// their Deps, so reconstruct-with-pending follows the chain back
// to the committed meta fields. Reading through a fresh Context
// instead would run into reconstruct's tip-pruning interaction
// with uncommitted pending tips and miss the schema.
func (v *swytchVTable) refreshSchema() error {
	ectx := v.readContext()
	schema, err := loadSchema(ectx, v.name)
	if err != nil {
		return fmt.Errorf("refresh schema %q: %w", v.name, err)
	}
	v.schema = schema
	return nil
}

// session returns the pg session owning this vtab's SQLite conn,
// or nil if the session hasn't been registered yet (the vtab was
// Connect'd during table replay before registration). Callers that
// need the session to exist should treat nil as "no active
// transaction" and fall back to the auto-commit path.
func (v *swytchVTable) session() *session {
	if v.conn == nil {
		return nil
	}
	return v.server.sessionForConn(v.conn)
}

// Query plan discriminators communicated from BestIndex to Filter
// via IndexID.Num.
const (
	// planFullScan means the cursor will scan every row key matching
	// the table's prefix via Engine.ScanKeys.
	planFullScan int32 = 0
	// planPKEquality means Filter was given a PK value and should
	// fetch exactly that row via GetSnapshot.
	planPKEquality int32 = 1
	// planIndexEquality: indexed equality lookup. IndexID.String
	// carries the index name. Filter receives one argv: the value.
	planIndexEquality int32 = 2
	// planIndexRange: indexed range lookup. IndexID.String carries
	// "<indexName>|<bounds>" where bounds is a 2-char code describing
	// which of lower/upper bounds are supplied and whether each is
	// inclusive. Filter receives 1 or 2 argv values (lower then
	// upper, in that fixed order).
	planIndexRange int32 = 3
)

// BestIndex advertises query plans that satisfy the query. The
// lowest-cost plan SQLite can pair with a satisfying constraint set
// wins. Order preference:
//
//  1. PK equality (cost 1).
//  2. Indexed equality on a registered index (cost 2).
//  3. Indexed range on a registered index (cost sqrt(N)).
//  4. Full scan (cost 1e6).
func (v *swytchVTable) BestIndex(in *sqlite.IndexInputs) (*sqlite.IndexOutputs, error) {
	// Live schema propagation: always re-read the schema at plan
	// time so indices (or future schema changes) created by other
	// sessions after this session connected are picked up without
	// reconnecting. Cheap: the reduced schema is cache-hot after
	// the first read of the session.
	if err := v.refreshSchema(); err != nil {
		return nil, err
	}

	out := &sqlite.IndexOutputs{
		ID:              sqlite.IndexID{Num: planFullScan},
		EstimatedCost:   1_000_000,
		EstimatedRows:   1_000,
		ConstraintUsage: make([]sqlite.IndexConstraintUsage, len(in.Constraints)),
	}

	// Pass 1: PK equality — strictly better than any secondary index
	// lookup. For a composite PK this requires equality on EVERY PK
	// column; otherwise fall through. Argv positions are assigned in
	// PK order so Filter can reassemble the tuple.
	if pkSlots := v.matchPKEquality(in.Constraints); pkSlots != nil {
		out.ID = sqlite.IndexID{Num: planPKEquality}
		out.EstimatedCost = 1
		out.EstimatedRows = 1
		for pkPos, cstrIdx := range pkSlots {
			out.ConstraintUsage[cstrIdx] = sqlite.IndexConstraintUsage{
				ArgvIndex: pkPos + 1,
				Omit:      true,
			}
		}
		return out, nil
	}

	// Pass 2: composite (or single-column) index equality. An index
	// is usable for equality iff EVERY indexed column has a Usable
	// Eq constraint. First index to fully match wins.
	for idxI := range v.schema.Indexes {
		idx := &v.schema.Indexes[idxI]
		slots := v.matchIndexEquality(idx, in.Constraints)
		if slots == nil {
			continue
		}
		out.ID = sqlite.IndexID{Num: planIndexEquality, String: idx.Name}
		if idx.Unique {
			out.EstimatedCost = 2
			out.EstimatedRows = 1
		} else {
			out.EstimatedCost = 10
			out.EstimatedRows = 8
		}
		for pos, cstrIdx := range slots {
			out.ConstraintUsage[cstrIdx] = sqlite.IndexConstraintUsage{
				ArgvIndex: pos + 1,
				Omit:      true,
			}
		}
		return out, nil
	}

	// Pass 3: composite-or-single indexed range. We support the
	// classic shape: equality on the first N-1 components + a range
	// on the Nth component. Argv layout is [eq1, eq2, …, eqN-1,
	// lower?, upper?] so Filter can reassemble.
	if res := v.bestRangeIndex(in); res != nil {
		code := rangeCode(res.lowerI >= 0, res.upperI >= 0, res.lowerInc, res.upperInc)
		out.ID = sqlite.IndexID{Num: planIndexRange, String: res.idx.Name + "|" + code}
		out.EstimatedCost = 100
		out.EstimatedRows = 50
		argv := 1
		for _, cstrIdx := range res.eqSlots {
			out.ConstraintUsage[cstrIdx] = sqlite.IndexConstraintUsage{
				ArgvIndex: argv, Omit: true,
			}
			argv++
		}
		if res.lowerI >= 0 {
			out.ConstraintUsage[res.lowerI] = sqlite.IndexConstraintUsage{
				ArgvIndex: argv, Omit: false,
			}
			argv++
		}
		if res.upperI >= 0 {
			out.ConstraintUsage[res.upperI] = sqlite.IndexConstraintUsage{
				ArgvIndex: argv, Omit: false,
			}
		}
		return out, nil
	}

	return out, nil
}

// matchIndexEquality returns the slot indices (in idx column order)
// for a fully-constrained composite index equality, or nil when any
// indexed column is missing a Usable Eq constraint.
func (v *swytchVTable) matchIndexEquality(idx *indexSchema, constraints []sqlite.IndexConstraint) []int {
	ords := idx.columnIndexes(v.schema)
	slots := make([]int, len(ords))
	for i := range slots {
		slots[i] = -1
	}
	for i, c := range constraints {
		if !c.Usable || c.Op != sqlite.IndexConstraintEq {
			continue
		}
		for pos, ord := range ords {
			if ord == c.Column && slots[pos] == -1 {
				slots[pos] = i
				break
			}
		}
	}
	if slices.Contains(slots, -1) {
		return nil
	}
	return slots
}

// matchPKEquality checks whether every PK column has a Usable Eq
// constraint. On a full match it returns a slice aligned with
// v.schema.PKColumns — result[pkPos] = the index into in.Constraints
// of the matching equality — so the caller can assign ArgvIndex in
// PK order. Returns nil when any PK column is unconstrained.
func (v *swytchVTable) matchPKEquality(constraints []sqlite.IndexConstraint) []int {
	pkCols := v.schema.PKColumns
	if len(pkCols) == 0 {
		return nil
	}
	result := make([]int, len(pkCols))
	for i := range result {
		result[i] = -1
	}
	for i, c := range constraints {
		if !c.Usable || c.Op != sqlite.IndexConstraintEq {
			continue
		}
		for pkPos, col := range pkCols {
			if c.Column == col && result[pkPos] == -1 {
				result[pkPos] = i
				break
			}
		}
	}
	if slices.Contains(result, -1) {
		return nil
	}
	return result
}

// indexOnColumn returns the first registered index whose leading
// column matches the given column index, or nil.
func (v *swytchVTable) indexOnColumn(col int) *indexSchema {
	if col < 0 || col >= len(v.schema.Columns) {
		return nil
	}
	name := v.schema.Columns[col].Name
	for i := range v.schema.Indexes {
		idx := &v.schema.Indexes[i]
		if len(idx.Columns) > 0 && idx.Columns[0] == name {
			return idx
		}
	}
	return nil
}

// rangeMatch captures the result of a composite-index range probe.
type rangeMatch struct {
	idx      *indexSchema
	eqSlots  []int // constraint indices of the equality prefix, in index-column order
	lowerI   int   // constraint index of the < / <= / >= / > on the last indexed col, or -1
	upperI   int   // ditto for the upper bound
	lowerInc bool
	upperInc bool
}

// bestRangeIndex picks the best index that can service the query's
// WHERE as "equality on prefix components + range on the Nth". For a
// single-column index N=1, so it's just a range on the sole column.
// Returns nil when no index fits.
func (v *swytchVTable) bestRangeIndex(in *sqlite.IndexInputs) *rangeMatch {
	var best *rangeMatch
	for idxI := range v.schema.Indexes {
		idx := &v.schema.Indexes[idxI]
		ords := idx.columnIndexes(v.schema)
		if len(ords) == 0 {
			continue
		}
		// Build eq prefix greedily: for each leading component,
		// require a Usable Eq constraint on that column.
		eqSlots := make([]int, 0, len(ords))
		for _, ord := range ords[:len(ords)-1] {
			slot := -1
			for i, c := range in.Constraints {
				if !c.Usable || c.Op != sqlite.IndexConstraintEq {
					continue
				}
				if c.Column == ord {
					slot = i
					break
				}
			}
			if slot < 0 {
				break
			}
			eqSlots = append(eqSlots, slot)
		}
		if len(eqSlots) != len(ords)-1 {
			continue
		}
		lastOrd := ords[len(ords)-1]
		lowerI, upperI := -1, -1
		var lowerInc, upperInc bool
		for i, c := range in.Constraints {
			if !c.Usable || c.Column != lastOrd {
				continue
			}
			switch c.Op {
			case sqlite.IndexConstraintGT:
				if lowerI < 0 {
					lowerI, lowerInc = i, false
				}
			case sqlite.IndexConstraintGE:
				if lowerI < 0 {
					lowerI, lowerInc = i, true
				}
			case sqlite.IndexConstraintLT:
				if upperI < 0 {
					upperI, upperInc = i, false
				}
			case sqlite.IndexConstraintLE:
				if upperI < 0 {
					upperI, upperInc = i, true
				}
			}
		}
		if lowerI < 0 && upperI < 0 {
			continue
		}
		best = &rangeMatch{
			idx:      idx,
			eqSlots:  eqSlots,
			lowerI:   lowerI,
			upperI:   upperI,
			lowerInc: lowerInc,
			upperInc: upperInc,
		}
		// First match wins — SQLite retries with different Usable
		// bits, so no scoring complexity needed.
		return best
	}
	return nil
}

// rangeCode encodes which bounds are supplied and whether each is
// inclusive into a short two-character tag Filter can parse.
// Positions are [lower][upper]; values are 'x' (absent), 'i'
// (inclusive), 'e' (exclusive).
func rangeCode(hasLower, hasUpper, lowerInc, upperInc bool) string {
	var l, u byte = 'x', 'x'
	if hasLower {
		if lowerInc {
			l = 'i'
		} else {
			l = 'e'
		}
	}
	if hasUpper {
		if upperInc {
			u = 'i'
		} else {
			u = 'e'
		}
	}
	return string([]byte{l, u})
}

// Open returns a fresh cursor. Cursors materialise rows eagerly inside
// Filter so that later Next/Column calls don't need the effects engine
// any more — they just iterate the in-memory snapshot.
func (v *swytchVTable) Open() (sqlite.VTableCursor, error) {
	return &swytchCursor{vt: v}, nil
}

func (v *swytchVTable) Disconnect() error { return nil }
func (v *swytchVTable) Destroy() error    { return nil }

// rowIDForPK maps a primary-key byte string to the int64 rowid
// SQLite uses to identify the row. INTEGER PKs round-trip 1:1 — we
// decode the 8-byte encoded PK back into the original int64 so
// rowid == PK value, which matches user expectations and needs no
// reverse cache. Non-INTEGER PKs are hashed with xxh3 (matching the
// rest of the codebase); the cursor maintains a reverse map so
// Update/DeleteRow can translate back.
func (v *swytchVTable) rowIDForPK(pk string) int64 {
	if v.schema.integerRowidFastPath() && len(pk) == 8 {
		return int64(binary.BigEndian.Uint64([]byte(pk)) ^ (1 << 63))
	}
	return int64(xxh3.HashString(pk))
}

// rememberRowID records the PK associated with a rowid we handed out
// via cursor.RowID(). Only meaningful for non-INTEGER PKs where the
// mapping is lossy.
func (v *swytchVTable) rememberRowID(rowid int64, pk string) {
	v.pkByRowID[rowid] = pk
}

// pkForRowID recovers the PK for a rowid previously yielded by a
// cursor. For INTEGER PKs we re-encode the int64 into the binary PK
// format — no cache lookup needed because the mapping is 1:1. For
// other affinities we consult the reverse map and report ok=false
// if SQLite hands us a rowid we never produced.
func (v *swytchVTable) pkForRowID(rowid int64) (string, bool) {
	if v.schema.integerRowidFastPath() {
		return string(encodeIndexValue(sqlite.IntegerValue(rowid), affinityInteger)), true
	}
	pk, ok := v.pkByRowID[rowid]
	return pk, ok
}

// forgetRowID clears an entry after a DELETE or PK-change UPDATE so
// the map doesn't grow unbounded for long-lived sessions. No-op for
// INTEGER PKs.
func (v *swytchVTable) forgetRowID(rowid int64) {
	if v.schema.integerRowidFastPath() {
		return
	}
	delete(v.pkByRowID, rowid)
}

// Update implements INSERT and UPDATE. SQLite routes both through a
// single callback; IsInsert discriminates by observing whether
// OldRowID is NULL.
//
// Every branch runs inside runRMW so that concurrent writers racing
// on the same row (or on a UNIQUE index's target entry) cause one
// side to abort and retry rather than silently corrupting each
// other. INSERT checks uniqueness; UPDATE carries forward Unchanged
// columns; both maintain secondary indices in lock-step with the
// row field writes.
func (v *swytchVTable) Update(params sqlite.VTableUpdateParams) (int64, error) {
	if params.IsInsert() {
		return v.doInsert(params.Columns)
	}
	return v.doUpdate(params)
}

func (v *swytchVTable) doInsert(cols []sqlite.Value) (int64, error) {
	if !v.schema.hasPK() {
		return 0, fmt.Errorf("table %q has no primary key", v.name)
	}
	if len(cols) != len(v.schema.Columns) {
		return 0, fmt.Errorf("insert column count %d, want %d", len(cols), len(v.schema.Columns))
	}

	var pkStr string
	var syntheticRowID int64
	if v.schema.SyntheticPK {
		seq := v.server.rowIDSeq.Add(1)
		syntheticRowID = nextSyntheticRowID(time.Now(), v.server.engine.NodeID(), seq)
		pkStr = string(encodeIndexValue(sqlite.IntegerValue(syntheticRowID), affinityInteger))
	} else {
		pkVals := v.schema.pkValues(cols)
		pkAffs := v.schema.pkAffinities()
		var err error
		pkStr, err = encodePK(pkVals, pkAffs)
		if err != nil {
			return 0, err
		}
	}

	if err := v.runWrite("insert", func(ectx *effects.Context) error {
		// Re-read the table schema inside the transaction so index
		// maintenance always uses the current index list, even if
		// another session issued CREATE INDEX after this one
		// connected. Pin a dep on it so a concurrent schema change
		// during this insert aborts one of us cleanly.
		tbl, tips, err := v.loadLiveSchema(ectx)
		if err != nil {
			return err
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(schemaKey(v.name)),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			return fmt.Errorf("pin schema dep: %w", err)
		}

		// Uniqueness checks + index entries. Each UNIQUE index reads
		// its target entry (Noop-pinned) so a concurrent insert of
		// the same indexed value aborts one transaction.
		if err := v.emitIndexInserts(ectx, tbl, pkStr, cols); err != nil {
			return err
		}

		// Row field effects.
		key := rowKey(v.schema.TableID, pkStr)
		// Subscribe to the row key before writing. GetSnapshot is
		// how the engine learns a node cares about a key, and a
		// write without a preceding subscription has no causal
		// standing — dep chains reference tips the writer never
		// saw, and peers treat the write as unsubscribed noise.
		// For a fresh PK this returns nil, but the subscription
		// side effect is what matters.
		if _, _, err := ectx.GetSnapshot(key); err != nil {
			return fmt.Errorf("subscribe row key: %w", err)
		}
		if v.schema.SyntheticPK {
			if err := emitSyntheticPKPresence(ectx, key, syntheticRowID); err != nil {
				return fmt.Errorf("emit synthetic rowid presence: %w", err)
			}
		} else if err := v.emitPKPresence(ectx, key, cols); err != nil {
			return err
		}
		for i, col := range tbl.Columns {
			if tbl.isPKColumn(i) {
				continue
			}
			if cols[i].Type() == sqlite.TypeNull {
				continue
			}
			eff := &pb.DataEffect{
				Op:         pb.EffectOp_INSERT_OP,
				Merge:      pb.MergeRule_LAST_WRITE_WINS,
				Collection: pb.CollectionKind_KEYED,
				Id:         []byte(col.Name),
			}
			if !setEncodedValue(eff, cols[i]) {
				continue
			}
			if err := ectx.Emit(&pb.Effect{
				Key:  []byte(key),
				Kind: &pb.Effect_Data{Data: eff},
			}); err != nil {
				return fmt.Errorf("emit insert %s.%s: %w", v.name, col.Name, err)
			}
		}

		// RowWriteEffect at the table-identity key — commit evidence
		// for predicate-based fork-choice (Phase 5 proper).
		return v.emitRowWriteEffect(ectx, v.rowWriteForTuple(WriteInsert, pkStr, cols))
	}); err != nil {
		return 0, err
	}

	rowid := v.rowIDForPK(pkStr)
	if !v.schema.integerRowidFastPath() {
		v.rememberRowID(rowid, pkStr)
	}
	v.recordRowWrites(v.rowWriteForTuple(WriteInsert, pkStr, cols))
	if sess := v.session(); sess != nil {
		sess.markRowDirty(v.name, pkStr)
	}
	return rowid, nil
}

// loadLiveSchema reads the current table schema from swytch inside
// the caller's transactional context. Returns the schema plus the
// tips that were current at read time, for dependency pinning.
func (v *swytchVTable) loadLiveSchema(ectx *effects.Context) (*tableSchema, []effects.Tip, error) {
	snap, tips, err := ectx.GetSnapshot(schemaKey(v.name))
	if err != nil {
		return nil, nil, fmt.Errorf("read schema for %s: %w", v.name, err)
	}
	if snap == nil {
		return nil, nil, fmt.Errorf("%w: %s", ErrNoSuchTable, v.name)
	}
	// Reuse the reducedToSchema decoding from schema.go — inline it
	// here since loadSchema does its own GetSnapshot; we want the
	// tips from OUR snapshot, not a fresh one.
	tbl, err := schemaFromSnapshot(v.name, snap)
	if err != nil {
		return nil, nil, err
	}
	return tbl, tips, nil
}

// emitIndexInserts emits index entry INSERTs for every index that
// references a non-NULL column of the new row. For UNIQUE indices,
// checks the target entry for conflicting PKs and returns
// ErrUniqueViolation if one exists.
func (v *swytchVTable) emitIndexInserts(ectx *effects.Context, tbl *tableSchema, pk string, vals []sqlite.Value) error {
	for _, idx := range tbl.Indexes {
		encoded, skip := v.indexTupleForRow(&idx, tbl, vals)
		if skip {
			continue
		}
		entryKey := indexKey(idx.IndexID, encoded)

		// Pin the entry key as part of the tx, always, so a
		// concurrent INSERT for the same indexed value aborts.
		_, tips, err := ectx.GetSnapshot(entryKey)
		if err != nil {
			return fmt.Errorf("read index entry: %w", err)
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(entryKey),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			return fmt.Errorf("pin index entry dep: %w", err)
		}

		if idx.Unique {
			if err := checkUniqueBeforeInsert(ectx, idx.IndexID, encoded, pk); err != nil {
				return err
			}
		}
		if err := emitIndexEntryInsert(ectx, idx.IndexID, encoded, pk); err != nil {
			return err
		}
	}
	return nil
}

// emitIndexRemoves emits index entry REMOVEs for every index whose
// columns were all non-NULL on the row being removed. Caller must
// already have the old row's values in vals.
func (v *swytchVTable) emitIndexRemoves(ectx *effects.Context, tbl *tableSchema, pk string, vals []sqlite.Value) error {
	for _, idx := range tbl.Indexes {
		encoded, skip := v.indexTupleForRow(&idx, tbl, vals)
		if skip {
			continue
		}
		if err := emitIndexEntryRemove(ectx, idx.IndexID, encoded, pk); err != nil {
			return err
		}
	}
	return nil
}

// indexTupleForRow builds the composite-or-single index key payload
// for a given row. Returns skip=true when any component is NULL or
// the index is stale against the current schema — in either case
// the row isn't part of the index.
func (v *swytchVTable) indexTupleForRow(idx *indexSchema, tbl *tableSchema, vals []sqlite.Value) ([]byte, bool) {
	ords := idx.columnIndexes(tbl)
	affs := idx.columnAffinities(tbl)
	compVals := make([]sqlite.Value, len(ords))
	for i, ord := range ords {
		if ord < 0 {
			return nil, true
		}
		v := vals[ord]
		if v.Type() == sqlite.TypeNull {
			return nil, true
		}
		compVals[i] = v
	}
	return encodeIndexKeyTuple(compVals, affs), false
}

// emitPresenceField writes the PK column value as a regular KEYED
// INSERT field. loadRow ignores this field for row reconstruction (it
// decodes the PK from the key itself), but its existence guarantees
// the row has non-empty NetAdds and therefore survives snapshot
// reduction even when every other column is NULL.
// emitPKPresence emits a DataEffect for every PK column's value
// under the row's key. For composite PKs each column becomes a
// separate KEYED field; for single-column PKs it's one. The PK
// values are mandatory (SQL PK semantics disallow NULL), so a NULL
// component surfaces as an error via setEncodedValue.
//
// For SyntheticPK tables there is no user PK column; instead callers
// emit a rowFieldSyntheticRowID marker via emitSyntheticPKPresence so
// a row whose user columns are all NULL still reads as live.
func (v *swytchVTable) emitPKPresence(ectx *effects.Context, key string, cols []sqlite.Value) error {
	for _, ord := range v.schema.PKColumns {
		colName := v.schema.Columns[ord].Name
		if err := emitPresenceField(ectx, key, colName, cols[ord]); err != nil {
			return fmt.Errorf("emit presence field %s.%s: %w", v.name, colName, err)
		}
	}
	return nil
}

// emitSyntheticPKPresence writes the rowFieldSyntheticRowID marker
// for a SyntheticPK row. The rowid is both the identity evidence and
// the minimum NetAdds field needed to keep the row from reading as
// fully-deleted in loadRow when every user column is NULL.
func emitSyntheticPKPresence(ectx *effects.Context, key string, rowid int64) error {
	return emitPresenceField(ectx, key, rowFieldSyntheticRowID, sqlite.IntegerValue(rowid))
}

func emitPresenceField(ectx *effects.Context, key, pkColName string, pkVal sqlite.Value) error {
	eff := &pb.DataEffect{
		Op:         pb.EffectOp_INSERT_OP,
		Merge:      pb.MergeRule_LAST_WRITE_WINS,
		Collection: pb.CollectionKind_KEYED,
		Id:         []byte(pkColName),
	}
	if !setEncodedValue(eff, pkVal) {
		return fmt.Errorf("primary key value cannot be NULL")
	}
	return ectx.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Data{Data: eff},
	})
}

func (v *swytchVTable) doUpdate(params sqlite.VTableUpdateParams) (int64, error) {
	if !v.schema.hasPK() {
		return 0, fmt.Errorf("table %q has no primary key", v.name)
	}
	if len(params.Columns) != len(v.schema.Columns) {
		return 0, fmt.Errorf("update column count %d, want %d", len(params.Columns), len(v.schema.Columns))
	}

	oldRowID := params.OldRowID.Int64()
	oldPK, ok := v.pkForRowID(oldRowID)
	if !ok {
		return 0, fmt.Errorf("update: unknown rowid %d (cursor did not yield it in this session)", oldRowID)
	}

	// For synthetic-PK tables the rowid is the only identity and is
	// immutable from SQL: newPK == oldPK by construction. Skip the
	// PK-change branch entirely. pkCol is only used by the PK-change
	// path, so leaving it nil here is safe.
	var pkCol *columnSchema
	newPK := oldPK
	if !v.schema.SyntheticPK {
		pkCol = v.schema.pkColumn()
		newPKVals := v.schema.pkValues(params.Columns)
		pkAffs := v.schema.pkAffinities()
		var err error
		newPK, err = encodePK(newPKVals, pkAffs)
		if err != nil {
			return 0, err
		}
	}

	if newPK != oldPK {
		// PK-change is read-modify-write: we read the old row
		// snapshot to carry forward Unchanged columns. runRMW
		// serialises this via BeginTx so a concurrent mutation of
		// the old row causes a retryable abort rather than silent
		// data loss.
		var pkChangeNewVals []sqlite.Value
		var pkChangeOldVals []sqlite.Value
		if err := v.runWrite("pk-change update", func(ectx *effects.Context) error {
			newVals, oldVals, err := v.emitPKChange(ectx, oldPK, newPK, pkCol, params.Columns)
			if err != nil {
				return err
			}
			pkChangeNewVals = newVals
			pkChangeOldVals = oldVals
			// Two RowWriteEffects: one for the old row's DELETE,
			// one for the new row's INSERT. Observations on either
			// the old or the new state can match.
			if err := v.emitRowWriteEffect(ectx,
				v.rowWriteForTuple(WriteDelete, oldPK, oldVals)); err != nil {
				return err
			}
			return v.emitRowWriteEffect(ectx,
				v.rowWriteForTuple(WriteInsert, newPK, newVals))
		}); err != nil {
			return 0, err
		}
		newRowID := v.rowIDForPK(newPK)
		v.forgetRowID(oldRowID)
		if !v.schema.integerRowidFastPath() {
			v.rememberRowID(newRowID, newPK)
		}
		// Record two RowWrites: a DELETE of the old row (for
		// observations predicated on the old PK's column values)
		// and an INSERT of the new row. A PK-change is semantically
		// "delete row R_old; insert row R_new" — observations of
		// either one are valid.
		v.recordRowWrites(
			v.rowWriteForTuple(WriteDelete, oldPK, pkChangeOldVals),
			v.rowWriteForTuple(WriteInsert, newPK, pkChangeNewVals),
		)
		if sess := v.session(); sess != nil {
			sess.markRowDirty(v.name, oldPK)
			sess.markRowDirty(v.name, newPK)
		}
		return newRowID, nil
	}

	// Same-PK update is a read-modify-write once indices are in the
	// mix: we need the old column values to REMOVE the right index
	// entries before INSERTing new ones. Wrap in runRMW so a
	// concurrent update to the same row aborts one of us.
	var effectiveNewVals []sqlite.Value
	if err := v.runWrite("same-pk update", func(ectx *effects.Context) error {
		tbl, schemaTips, err := v.loadLiveSchema(ectx)
		if err != nil {
			return err
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(schemaKey(v.name)),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, schemaTips); err != nil {
			return fmt.Errorf("pin schema dep: %w", err)
		}

		oldSnap, rowTips, err := ectx.GetSnapshot(rowKey(v.schema.TableID, oldPK))
		if err != nil {
			return fmt.Errorf("read old row for update: %w", err)
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(rowKey(v.schema.TableID, oldPK)),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, rowTips); err != nil {
			return fmt.Errorf("pin row dep: %w", err)
		}

		if err := v.emitIndexDeltasSamePK(ectx, tbl, oldPK, oldSnap, params.Columns); err != nil {
			return err
		}

		// Compute the effective post-commit row so we can record a
		// RowWrite: start from the old snapshot, override with new
		// values for columns that were actually SET (NoChange
		// preserves old values). PK column comes from the key.
		effectiveNewVals = v.mergeSamePKRow(tbl, oldPK, oldSnap, params.Columns)

		if err := v.emitSamePKUpdate(ectx, oldPK, params.Columns); err != nil {
			return err
		}
		// RowWriteEffect for commit evidence.
		return v.emitRowWriteEffect(ectx,
			v.rowWriteForTuple(WriteUpdate, oldPK, effectiveNewVals))
	}); err != nil {
		return 0, err
	}
	v.recordRowWrites(v.rowWriteForTuple(WriteUpdate, newPK, effectiveNewVals))
	if sess := v.session(); sess != nil {
		sess.markRowDirty(v.name, newPK)
	}
	return v.rowIDForPK(newPK), nil
}

// mergeSamePKRow builds the effective post-commit column tuple for a
// same-PK UPDATE by combining the new SET values (non-NoChange) with
// the old row's values for Unchanged columns. The PK column is
// filled from the key since it isn't stored as a separate field.
func (v *swytchVTable) mergeSamePKRow(
	tbl *tableSchema,
	pk string,
	oldSnap *pb.ReducedEffect,
	newParams []sqlite.Value,
) []sqlite.Value {
	out := make([]sqlite.Value, len(tbl.Columns))
	pkVals := decodePK(pk, tbl.pkAffinities())
	for i, col := range tbl.Columns {
		if pkPos := pkPosition(tbl, i); pkPos >= 0 {
			out[i] = pkVals[pkPos]
			continue
		}
		val := newParams[i]
		if !val.NoChange() {
			out[i] = val
			continue
		}
		// NoChange: pull from the old snapshot, or NULL if absent.
		if oldSnap != nil {
			if el, ok := oldSnap.NetAdds[col.Name]; ok && el.Data != nil {
				out[i] = decodeValue(el.Data, col.Affinity)
				continue
			}
		}
		out[i] = sqlite.Value{}
	}
	return out
}

// emitIndexDeltasSamePK emits REMOVE/INSERT index entries for a
// same-PK UPDATE. For each index: if the indexed column is
// Unchanged, no delta. Otherwise REMOVE the old entry (if the old
// value was non-NULL) and INSERT the new entry (if the new value is
// non-NULL, and with a uniqueness check for UNIQUE indices).
func (v *swytchVTable) emitIndexDeltasSamePK(
	ectx *effects.Context,
	tbl *tableSchema,
	pk string,
	oldSnap *pb.ReducedEffect,
	newVals []sqlite.Value,
) error {
	for _, idx := range tbl.Indexes {
		ords := idx.columnIndexes(tbl)
		affs := idx.columnAffinities(tbl)
		// Any indexed column changed (or was unknown)? If every
		// component is NoChange (user didn't touch any of them),
		// skip this index — entries are unchanged.
		stale := false
		anyChanged := false
		for _, ord := range ords {
			if ord < 0 {
				stale = true
				break
			}
			if !newVals[ord].NoChange() {
				anyChanged = true
			}
		}
		if stale || !anyChanged {
			continue
		}

		// Resolve the OLD tuple from the row snapshot. The indexed
		// old entry exists iff every component was non-NULL.
		oldTuple := make([]sqlite.Value, len(ords))
		oldAllPresent := true
		if oldSnap != nil {
			for i, ord := range ords {
				el, ok := oldSnap.NetAdds[tbl.Columns[ord].Name]
				if !ok || el.Data == nil {
					oldAllPresent = false
					break
				}
				oldTuple[i] = decodeValue(el.Data, affs[i])
				if oldTuple[i].Type() == sqlite.TypeNull {
					oldAllPresent = false
					break
				}
			}
		} else {
			oldAllPresent = false
		}
		if oldAllPresent {
			oldEnc := encodeIndexKeyTuple(oldTuple, affs)
			if err := emitIndexEntryRemove(ectx, idx.IndexID, oldEnc, pk); err != nil {
				return fmt.Errorf("emit index remove %s: %w", idx.Name, err)
			}
		}

		// Resolve the NEW tuple: take supplied values; for NoChange
		// components carry forward the old value. If any final
		// component is NULL, skip the insert (indexed NULL is
		// unreachable).
		newTuple := make([]sqlite.Value, len(ords))
		newAllPresent := true
		for i, ord := range ords {
			nv := newVals[ord]
			if nv.NoChange() {
				nv = oldTuple[i]
			}
			if nv.Type() == sqlite.TypeNull {
				newAllPresent = false
				break
			}
			newTuple[i] = nv
		}
		if !newAllPresent {
			continue
		}
		newEnc := encodeIndexKeyTuple(newTuple, affs)
		entryKey := indexKey(idx.IndexID, newEnc)
		_, tips, err := ectx.GetSnapshot(entryKey)
		if err != nil {
			return fmt.Errorf("read index entry for uniqueness: %w", err)
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(entryKey),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			return fmt.Errorf("pin index entry dep: %w", err)
		}
		if idx.Unique {
			if err := checkUniqueBeforeInsert(ectx, idx.IndexID, newEnc, pk); err != nil {
				return err
			}
		}
		if err := emitIndexEntryInsert(ectx, idx.IndexID, newEnc, pk); err != nil {
			return fmt.Errorf("emit index insert %s: %w", idx.Name, err)
		}
	}
	return nil
}

// emitSamePKUpdate emits INSERT/REMOVE effects for columns that
// changed, leaving Unchanged columns untouched in storage.
func (v *swytchVTable) emitSamePKUpdate(ectx *effects.Context, pk string, cols []sqlite.Value) error {
	key := rowKey(v.schema.TableID, pk)
	for i, col := range v.schema.Columns {
		if v.schema.isPKColumn(i) {
			continue
		}
		val := cols[i]
		if val.NoChange() {
			continue
		}
		if val.Type() == sqlite.TypeNull {
			if err := emitKeyedRemove(ectx, key, col.Name); err != nil {
				return fmt.Errorf("emit remove %s.%s: %w", v.name, col.Name, err)
			}
			continue
		}
		eff := &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(col.Name),
		}
		if !setEncodedValue(eff, val) {
			continue
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Data{Data: eff},
		}); err != nil {
			return fmt.Errorf("emit update %s.%s: %w", v.name, col.Name, err)
		}
	}
	return nil
}

// emitPKChange performs the read-modify-write body of a PK-change
// UPDATE inside the caller-supplied transactional context. It reads
// the old row under oldPK, pins the read, removes the old row (+
// its index entries), and writes the new row (+ new index entries),
// carrying forward Unchanged column values from the old snapshot.
//
// Returns the effective new-row column tuple and the old-row column
// tuple so the caller can record RowWrite payloads. Both tuples are
// column-ordinal-aligned with the table schema; missing columns are
// zero-valued (NULL).
func (v *swytchVTable) emitPKChange(
	ectx *effects.Context,
	oldPK, newPK string,
	pkCol *columnSchema,
	cols []sqlite.Value,
) ([]sqlite.Value, []sqlite.Value, error) {
	tbl, schemaTips, err := v.loadLiveSchema(ectx)
	if err != nil {
		return nil, nil, err
	}
	if err := ectx.Emit(&pb.Effect{
		Key:  []byte(schemaKey(v.name)),
		Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	}, schemaTips); err != nil {
		return nil, nil, fmt.Errorf("pin schema dep: %w", err)
	}

	oldKey := rowKey(v.schema.TableID, oldPK)
	oldSnap, tips, err := ectx.GetSnapshot(oldKey)
	if err != nil {
		return nil, nil, fmt.Errorf("read old row for pk-change: %w", err)
	}
	if err := ectx.Emit(&pb.Effect{
		Key:  []byte(oldKey),
		Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	}, tips); err != nil {
		return nil, nil, fmt.Errorf("pin old row dep: %w", err)
	}

	// Reconstruct the old row's column values — used both for index
	// REMOVEs and for the observation-side RowWrite payload.
	var oldVals []sqlite.Value
	if oldSnap != nil {
		oldVals = v.valuesFromSnapshot(tbl, oldPK, oldSnap)
		if err := v.emitIndexRemoves(ectx, tbl, oldPK, oldVals); err != nil {
			return nil, nil, err
		}
	} else {
		oldVals = make([]sqlite.Value, len(tbl.Columns))
	}

	if err := v.emitRowDelete(ectx, oldPK); err != nil {
		return nil, nil, err
	}

	// Build the effective new-row values by merging the new params
	// (non-Unchanged) with the old snapshot (Unchanged carry-forward).
	// This is what the new row will look like after commit — used
	// both for index inserts and for the row-field effect emission.
	newVals := make([]sqlite.Value, len(tbl.Columns))
	for i, col := range tbl.Columns {
		val := cols[i]
		if val.NoChange() && oldSnap != nil {
			if el, ok := oldSnap.NetAdds[col.Name]; ok && el.Data != nil {
				newVals[i] = decodeValue(el.Data, col.Affinity)
				continue
			}
			newVals[i] = sqlite.Value{}
			continue
		}
		newVals[i] = val
	}

	// Index inserts for the new row (with uniqueness checks).
	if err := v.emitIndexInserts(ectx, tbl, newPK, newVals); err != nil {
		return nil, nil, err
	}

	newKey := rowKey(v.schema.TableID, newPK)
	// Subscribe to the destination row key before writing (same
	// reason as doInsert: no write without a preceding subscription
	// so peers recognise this node as an authorised writer on the
	// key and our dep chain references tips we've actually seen).
	if _, _, err := ectx.GetSnapshot(newKey); err != nil {
		return nil, nil, fmt.Errorf("subscribe new row key: %w", err)
	}
	if err := v.emitPKPresence(ectx, newKey, newVals); err != nil {
		return nil, nil, err
	}
	for i, col := range tbl.Columns {
		if tbl.isPKColumn(i) {
			continue
		}
		val := newVals[i]
		if val.Type() == sqlite.TypeNull {
			continue
		}
		eff := &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(col.Name),
		}
		if !setEncodedValue(eff, val) {
			continue
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(newKey),
			Kind: &pb.Effect_Data{Data: eff},
		}); err != nil {
			return nil, nil, fmt.Errorf("emit pk-change insert %s.%s: %w", v.name, col.Name, err)
		}
	}
	return newVals, oldVals, nil
}

// DeleteRow removes a row identified by the rowid SQLite handed us
// (originally from cursor.RowID()). The row snapshot is read inside
// the transaction so secondary-index entries pointing at this row
// can be REMOVEd from the correct keys, and so a concurrent writer
// mutating the row aborts one of us.
func (v *swytchVTable) DeleteRow(rowID sqlite.Value) error {
	id := rowID.Int64()
	pk, ok := v.pkForRowID(id)
	if !ok {
		return fmt.Errorf("delete: unknown rowid %d", id)
	}

	var deletedVals []sqlite.Value
	if err := v.runWrite("delete row", func(ectx *effects.Context) error {
		tbl, schemaTips, err := v.loadLiveSchema(ectx)
		if err != nil {
			return err
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(schemaKey(v.name)),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, schemaTips); err != nil {
			return fmt.Errorf("pin schema dep: %w", err)
		}

		oldSnap, rowTips, err := ectx.GetSnapshot(rowKey(v.schema.TableID, pk))
		if err != nil {
			return fmt.Errorf("read row %s: %w", pk, err)
		}
		if err := ectx.Emit(&pb.Effect{
			Key:  []byte(rowKey(v.schema.TableID, pk)),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, rowTips); err != nil {
			return fmt.Errorf("pin row dep: %w", err)
		}

		if oldSnap != nil {
			deletedVals = v.valuesFromSnapshot(tbl, pk, oldSnap)
			if err := v.emitIndexRemoves(ectx, tbl, pk, deletedVals); err != nil {
				return err
			}
		} else {
			deletedVals = make([]sqlite.Value, len(tbl.Columns))
		}
		if err := v.emitRowDelete(ectx, pk); err != nil {
			return err
		}
		return v.emitRowWriteEffect(ectx,
			v.rowWriteForTuple(WriteDelete, pk, deletedVals))
	}); err != nil {
		return err
	}
	v.forgetRowID(id)
	v.recordRowWrites(v.rowWriteForTuple(WriteDelete, pk, deletedVals))
	if sess := v.session(); sess != nil {
		sess.markRowDirty(v.name, pk)
	}
	return nil
}

// valuesFromSnapshot reconstructs a []sqlite.Value in column order
// from a row's NetAdds snapshot. The PK column is filled from pk
// (it isn't stored as a separate field in the snapshot logic — it's
// the presence marker). NULL columns become zero Values.
func (v *swytchVTable) valuesFromSnapshot(tbl *tableSchema, pk string, snap *pb.ReducedEffect) []sqlite.Value {
	out := make([]sqlite.Value, len(tbl.Columns))
	pkVals := decodePK(pk, tbl.pkAffinities())
	for i, col := range tbl.Columns {
		if pos := pkPosition(tbl, i); pos >= 0 {
			out[i] = pkVals[pos]
			continue
		}
		el, ok := snap.NetAdds[col.Name]
		if !ok || el.Data == nil {
			out[i] = sqlite.Value{}
			continue
		}
		out[i] = decodeValue(el.Data, col.Affinity)
	}
	return out
}

// emitRowDelete emits a REMOVE effect for every column field of the
// row at pk, including the PK presence-marker field. Without removing
// the presence marker, the reduced state would still look like a
// live row with all non-PK columns NULL.
//
// It does not Flush — callers collect this with other effects in the
// same Context.
func (v *swytchVTable) emitRowDelete(ectx *effects.Context, pk string) error {
	key := rowKey(v.schema.TableID, pk)
	var errs []error
	for _, col := range v.schema.Columns {
		if err := emitKeyedRemove(ectx, key, col.Name); err != nil {
			errs = append(errs, fmt.Errorf("emit remove %s.%s: %w", v.name, col.Name, err))
		}
	}
	if v.schema.SyntheticPK {
		if err := emitKeyedRemove(ectx, key, rowFieldSyntheticRowID); err != nil {
			errs = append(errs, fmt.Errorf("emit remove %s.%s: %w", v.name, rowFieldSyntheticRowID, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// emitKeyedRemove emits a KEYED REMOVE for a single (key, field)
// pair. It is the row-deletion counterpart of emitRawField/emitIntField
// in schema.go.
func emitKeyedRemove(ectx *effects.Context, key, field string) error {
	return ectx.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(field),
		}},
	})
}

// swytchCursor walks the materialised result of a Filter call.
//
// observation is the Predicate captured at Filter time describing
// the sub-DAG this scan read. It is the first-class commit evidence
// used by fork-choice to detect concurrent writes that would have
// been part of the observation.
type swytchCursor struct {
	vt          *swytchVTable
	rows        []materialisedRow
	i           int
	observation Predicate
}

// materialisedRow is a single row snapshot already decoded from the
// effects layer. Columns are indexed by their position in the table
// schema.
type materialisedRow struct {
	pk   string
	cols []sqlite.Value
}

func (c *swytchCursor) Filter(id sqlite.IndexID, argv []sqlite.Value) error {
	c.rows = c.rows[:0]
	c.i = 0

	// Live schema propagation: refresh before executing the plan.
	// Keeps the cursor's view consistent with the BestIndex call
	// that produced this IndexID in the face of concurrent DDL on
	// other sessions.
	if err := c.vt.refreshSchema(); err != nil {
		return err
	}

	// Capture the observation predicate before running the filter.
	// What we observe during this scan is exactly what our chosen
	// plan navigates — not the user's full WHERE clause. Constraints
	// SQLite did NOT hand into our argv are re-applied by the core
	// post-cursor and don't narrow our sub-DAG.
	pred, err := c.buildObservationPredicate(id, argv)
	if err != nil {
		return err
	}
	c.observation = pred

	// Read through the session's txContext when a transaction is
	// open so GetSnapshot sees our own unflushed writes; otherwise
	// an ephemeral Context.
	ectx := c.vt.readContext()

	// Emit the observation immediately at read time. The observation
	// must capture the tips this read actually saw — deferring to
	// COMMIT would pin the observation on tips that include any
	// concurrent writer that committed between our read and our
	// commit, defeating fork-choice on the sequential-commit phantom
	// case (observer SELECTs k=4, writer commits v=2407 to k=4,
	// observer's second read sees 2407 but elle flags the tx as
	// non-repeatable-read because the observation didn't catch the
	// intervening commit). Outside a transaction the observation
	// has nowhere to live so we skip emission.
	if sess := c.vt.session(); sess != nil && sess.txContext != nil {
		if err := sess.txContext.Emit(&pb.Effect{
			Key: []byte(schemaKey(c.vt.name)),
			Kind: &pb.Effect_Observation{Observation: &pb.ObservationEffect{
				Predicate: predicateToProto(pred),
			}},
		}); err != nil {
			return fmt.Errorf("emit observation: %w", err)
		}
	}

	switch id.Num {
	case planPKEquality:
		return c.filterPKEquality(ectx, argv)
	case planFullScan:
		return c.filterFullScan(ectx)
	case planIndexEquality:
		return c.filterIndexEquality(ectx, id.String, argv)
	case planIndexRange:
		return c.filterIndexRange(ectx, id.String, argv)
	default:
		return fmt.Errorf("unknown plan id %d", id.Num)
	}
}

// buildObservationPredicate translates the plan identifier SQLite
// picked plus the bound rvalues into a Predicate describing what
// this scan observed. The plan ID encodes which navigation strategy
// we used; the schema tells us the column ordinals and affinities.
//
// Full-scan observations collapse to PTrue: the scan would have seen
// any row of the table, so any concurrent write to the table is in
// the observation's sub-DAG. That is the correct "table lock"
// degenerate case.
func (c *swytchCursor) buildObservationPredicate(id sqlite.IndexID, argv []sqlite.Value) (Predicate, error) {
	switch id.Num {
	case planFullScan:
		return PTrue(), nil

	case planPKEquality:
		pkCols := c.vt.schema.PKColumns
		if len(argv) != len(pkCols) {
			return Predicate{}, fmt.Errorf("pk-equality plan: expected %d args, got %d", len(pkCols), len(argv))
		}
		if len(pkCols) == 1 {
			return PCmp(pkCols[0], OpEq, typedValueFromSqlite(argv[0])), nil
		}
		// Composite PK equality is an AND of per-column equalities.
		terms := make([]Predicate, len(pkCols))
		for i, ord := range pkCols {
			terms[i] = PCmp(ord, OpEq, typedValueFromSqlite(argv[i]))
		}
		return PAnd(terms...), nil

	case planIndexEquality:
		idx := c.vt.findIndex(id.String)
		if idx == nil {
			return Predicate{}, fmt.Errorf("index %q not found in schema", id.String)
		}
		ords := idx.columnIndexes(c.vt.schema)
		if len(argv) != len(ords) {
			return Predicate{}, fmt.Errorf("index-equality plan: expected %d args, got %d", len(ords), len(argv))
		}
		for i, ord := range ords {
			if ord < 0 {
				return Predicate{}, fmt.Errorf("index %q column %d missing from schema", id.String, i)
			}
		}
		if len(ords) == 1 {
			return PCmp(ords[0], OpEq, typedValueFromSqlite(argv[0])), nil
		}
		terms := make([]Predicate, len(ords))
		for i, ord := range ords {
			terms[i] = PCmp(ord, OpEq, typedValueFromSqlite(argv[i]))
		}
		return PAnd(terms...), nil

	case planIndexRange:
		sep := strings.LastIndex(id.String, "|")
		if sep < 0 || sep+3 != len(id.String) {
			return Predicate{}, fmt.Errorf("malformed range plan tag %q", id.String)
		}
		name := id.String[:sep]
		code := id.String[sep+1:]
		idx := c.vt.findIndex(name)
		if idx == nil {
			return Predicate{}, fmt.Errorf("index %q not found in schema", name)
		}
		ords := idx.columnIndexes(c.vt.schema)
		if len(ords) == 0 {
			return Predicate{}, fmt.Errorf("index %q has no columns", name)
		}
		// Equality prefix: argv[0..N-2] == idx col[0..N-2]; range is on col[N-1].
		eqCount := len(ords) - 1
		if len(argv) < eqCount {
			return Predicate{}, fmt.Errorf("index-range plan: missing equality prefix")
		}
		terms := make([]Predicate, 0, len(ords)+1)
		for i := range eqCount {
			if ords[i] < 0 {
				return Predicate{}, fmt.Errorf("index %q column %d missing", name, i)
			}
			terms = append(terms, PCmp(ords[i], OpEq, typedValueFromSqlite(argv[i])))
		}
		lastOrd := ords[len(ords)-1]
		if lastOrd < 0 {
			return Predicate{}, fmt.Errorf("index %q last column missing", name)
		}
		argPos := eqCount
		switch code[0] { // lower
		case 'i':
			terms = append(terms, PCmp(lastOrd, OpGe, typedValueFromSqlite(argv[argPos])))
			argPos++
		case 'e':
			terms = append(terms, PCmp(lastOrd, OpGt, typedValueFromSqlite(argv[argPos])))
			argPos++
		}
		switch code[1] { // upper
		case 'i':
			terms = append(terms, PCmp(lastOrd, OpLe, typedValueFromSqlite(argv[argPos])))
		case 'e':
			terms = append(terms, PCmp(lastOrd, OpLt, typedValueFromSqlite(argv[argPos])))
		}
		if len(terms) == 0 {
			return PTrue(), nil
		}
		if len(terms) == 1 {
			return terms[0], nil
		}
		return PAnd(terms...), nil

	default:
		return Predicate{}, fmt.Errorf("unknown plan id %d", id.Num)
	}
}

// rowWriteForTuple assembles a RowWrite from a column-value tuple
// aligned with the table schema. It's the common shape consumed by
// each write-path helper below.
func (v *swytchVTable) rowWriteForTuple(kind WriteKind, pk string, cols []sqlite.Value) RowWrite {
	tuple := make([]TypedValue, len(cols))
	for i := range cols {
		tuple[i] = typedValueFromSqlite(cols[i])
	}
	return RowWrite{
		Table: v.name,
		PK:    []byte(pk),
		Kind:  kind,
		Cols:  tuple,
	}
}

// emitRowWriteEffect writes the RowWriteEffect into the given
// transactional Context at the table-identity key s/<table>. Every
// row-level INSERT/UPDATE/DELETE emits one of these alongside its
// per-column DataEffects. At fork-choice time (Phase 5 proper), the
// effects engine evaluates concurrent ObservationEffects' predicates
// against this tuple; until then the effect is inert — reduce
// ignores unknown kinds and the record just lives in the log.
func (v *swytchVTable) emitRowWriteEffect(ectx *effects.Context, rw RowWrite) error {
	return ectx.Emit(&pb.Effect{
		Key:  []byte(schemaKey(v.name)),
		Kind: &pb.Effect_RowWrite{RowWrite: rowWriteToProto(rw)},
	})
}

// runWrite runs a write-path body either inside the session's
// active transactional Context (when a pg BEGIN/COMMIT is open) or
// as an auto-committed RMW (otherwise).
//
// When in-transaction: body is called once with session.txContext
// and its writes accumulate into that Context. No retry here —
// retry (if needed) happens at COMMIT via runRMW's retry loop over
// the full transaction, which the session-layer commit logic owns.
//
// When auto-commit: delegates to runRMW, which wraps body in
// BeginTx/Flush with retry on ErrTxnAborted. This is the shape
// Phase 3/4 write paths always used; staying on this path when no
// pg transaction is open preserves the existing per-statement
// atomicity guarantees.
func (v *swytchVTable) runWrite(label string, body func(*effects.Context) error) error {
	if sess := v.session(); sess != nil && sess.txContext != nil {
		return body(sess.txContext)
	}
	return runRMW(v.server.engine, label, body)
}

// readContext returns the *effects.Context that reads (cursor
// filters, index probes) should use. When a transaction is active
// on the session, this is the session's txContext so GetSnapshot
// sees the tx's own unflushed writes (effects.Context reconstructs
// from the log when the Context has pending keys). Outside a
// transaction, a fresh short-lived Context is returned.
func (v *swytchVTable) readContext() *effects.Context {
	if sess := v.session(); sess != nil && sess.txContext != nil {
		return sess.txContext
	}
	return v.server.engine.NewContext()
}

// recordRowWrites overwrites the vtable's lastRowWrites slice with
// the given RowWrites. Called once per logical write-path
// invocation; the caller holds the snapshot for the duration of
// that invocation. Phase 5 will replace this with per-transaction
// accumulation.
func (v *swytchVTable) recordRowWrites(writes ...RowWrite) {
	v.lastRowWrites = append(v.lastRowWrites[:0], writes...)
}

// typedValueFromSqlite converts a sqlite.Value into our tagged
// TypedValue. This is the boundary where SQLite's opaque Value
// becomes a type we can serialise into an ObservationEffect.
func typedValueFromSqlite(v sqlite.Value) TypedValue {
	switch v.Type() {
	case sqlite.TypeInteger:
		return IntVal(v.Int64())
	case sqlite.TypeFloat:
		return FloatVal(v.Float())
	case sqlite.TypeText:
		return TextVal(v.Text())
	case sqlite.TypeBlob:
		return BlobVal(v.Blob())
	case sqlite.TypeNull:
		return NullVal()
	}
	return NullVal()
}

func (c *swytchCursor) filterPKEquality(ectx *effects.Context, argv []sqlite.Value) error {
	pkCols := c.vt.schema.PKColumns
	if len(pkCols) == 0 {
		return fmt.Errorf("table %q has no primary key", c.vt.name)
	}
	if len(argv) != len(pkCols) {
		return fmt.Errorf("pk-equality plan expects %d args, got %d", len(pkCols), len(argv))
	}
	pk, err := encodePK(argv, c.vt.schema.pkAffinities())
	if err != nil {
		return err
	}
	row, err := c.loadRow(ectx, pk)
	if err != nil {
		return err
	}
	if row != nil {
		c.rows = append(c.rows, *row)
	}
	return nil
}

func (c *swytchCursor) filterFullScan(ectx *effects.Context) error {
	seen := map[string]struct{}{}
	var pks []string
	var trieHits, pkMatches int
	// Committed rows from the keytrie.
	c.vt.server.engine.ScanKeys("", rowKeyPattern(c.vt.schema.TableID), func(key string) bool {
		trieHits++
		if pk, ok := parseRowKey(key, c.vt.schema.TableID); ok {
			pkMatches++
			if _, dup := seen[pk]; !dup {
				seen[pk] = struct{}{}
				pks = append(pks, pk)
			}
		}
		return true
	})
	// Tx-dirty rows: Emits in the current tx haven't hit the
	// keytrie yet (updateIndex only runs at Flush). Union them in
	// so the tx sees its own inserts.
	dirtyCount := 0
	if sess := c.vt.session(); sess != nil {
		for _, pk := range sess.dirtyRowPKs(c.vt.name) {
			dirtyCount++
			if _, dup := seen[pk]; !dup {
				seen[pk] = struct{}{}
				pks = append(pks, pk)
			}
		}
	}
	loadedNonNil := 0
	for _, pk := range pks {
		row, err := c.loadRow(ectx, pk)
		if err != nil {
			return err
		}
		if row != nil {
			loadedNonNil++
			c.rows = append(c.rows, *row)
		}
	}
	c.vt.server.log.Debug("filterFullScan",
		"table", c.vt.name,
		"trie_hits", trieHits,
		"pk_matches", pkMatches,
		"unique_pks", len(pks),
		"dirty_pks", dirtyCount,
		"loaded_rows", loadedNonNil,
		"pattern", rowKeyPattern(c.vt.schema.TableID))
	return nil
}

func (c *swytchCursor) filterIndexEquality(ectx *effects.Context, indexName string, argv []sqlite.Value) error {
	idx := c.vt.findIndex(indexName)
	if idx == nil {
		return fmt.Errorf("index %q no longer registered on table %q", indexName, c.vt.name)
	}
	if len(argv) != len(idx.Columns) {
		return fmt.Errorf("index-equality plan expects %d args, got %d", len(idx.Columns), len(argv))
	}
	affs := idx.columnAffinities(c.vt.schema)
	encoded := encodeIndexKeyTuple(argv, affs)
	entryKey := indexKey(idx.IndexID, encoded)
	snap, _, err := ectx.GetSnapshot(entryKey)
	if err != nil {
		return fmt.Errorf("read index entry %s: %w", entryKey, err)
	}
	if snap == nil {
		return nil
	}
	return c.loadRowsForPKs(ectx, netAddPKs(snap))
}

func (c *swytchCursor) filterIndexRange(ectx *effects.Context, planTag string, argv []sqlite.Value) error {
	// Plan tag: "<indexName>|<rangeCode>"
	sep := strings.LastIndex(planTag, "|")
	if sep < 0 || sep+3 != len(planTag) {
		return fmt.Errorf("malformed range plan tag %q", planTag)
	}
	indexName := planTag[:sep]
	code := planTag[sep+1:]
	idx := c.vt.findIndex(indexName)
	if idx == nil {
		return fmt.Errorf("index %q no longer registered", indexName)
	}
	affs := idx.columnAffinities(c.vt.schema)
	if len(affs) == 0 {
		return fmt.Errorf("index %q has no columns", indexName)
	}
	eqCount := len(affs) - 1 // all but the last; last takes the range
	lastAff := affs[len(affs)-1]

	// Argv layout: [eq0, eq1, …, eq{N-1}, lower?, upper?].
	if len(argv) < eqCount {
		return fmt.Errorf("range plan missing equality prefix args (need %d)", eqCount)
	}
	eqArgs := argv[:eqCount]
	var eqPrefix []byte
	if eqCount > 0 {
		eqPrefix = encodeIndexKeyPrefix(eqArgs, affs, eqCount)
	}
	argPos := eqCount
	var lowerEnc, upperEnc []byte
	var lowerInc, upperInc bool
	lowerSpec := code[0]
	upperSpec := code[1]
	if lowerSpec != 'x' {
		if argPos >= len(argv) {
			return fmt.Errorf("range plan missing lower-bound arg")
		}
		lowerEnc = encodeTupleComponent(argv[argPos], lastAff)
		// Strip the trailing terminator for TEXT/BLOB so the
		// half-tuple comparison is against the raw value ordering.
		// INTEGER/REAL have no terminator. For uniformity we just
		// use the raw encode here; the prefix compare below only
		// sees full component bytes via byte-wise compare, which
		// works because terminators sort lower than any escape.
		if len(affs) == 1 && (lastAff == affinityText || lastAff == affinityBlob) {
			lowerEnc = encodeIndexValue(argv[argPos], lastAff)
		}
		lowerInc = lowerSpec == 'i'
		argPos++
	}
	if upperSpec != 'x' {
		if argPos >= len(argv) {
			return fmt.Errorf("range plan missing upper-bound arg")
		}
		upperEnc = encodeTupleComponent(argv[argPos], lastAff)
		if len(affs) == 1 && (lastAff == affinityText || lastAff == affinityBlob) {
			upperEnc = encodeIndexValue(argv[argPos], lastAff)
		}
		upperInc = upperSpec == 'i'
	}

	// Start key for ScanKeys: if we have a lower bound, jump
	// straight to the prefix+lower offset. Otherwise start from
	// the index's own prefix.
	prefix := indexKeyPrefix(idx.IndexID) + string(eqPrefix)
	start := prefix
	if lowerEnc != nil && !lowerInc {
		start = indexKeyPrefix(idx.IndexID) + string(eqPrefix) + string(lowerEnc)
	}

	pkSet := map[string]struct{}{}
	c.vt.server.engine.ScanKeys(start, glob(prefix), func(key string) bool {
		if !strings.HasPrefix(key, prefix) {
			return false
		}
		valueBytes := key[len(prefix):]

		if lowerEnc != nil && lowerInc {
			if bytes.Compare([]byte(valueBytes), lowerEnc) < 0 {
				return true
			}
		}
		if upperEnc != nil {
			cmp := bytes.Compare([]byte(valueBytes), upperEnc)
			if cmp > 0 || (cmp == 0 && !upperInc) {
				return false // past the upper bound; stop iterating
			}
		}

		snap, _, err := ectx.GetSnapshot(key)
		if err != nil || snap == nil {
			return err == nil
		}
		for pk := range snap.NetAdds {
			pkSet[pk] = struct{}{}
		}
		return true
	})

	pks := make([]string, 0, len(pkSet))
	for pk := range pkSet {
		pks = append(pks, pk)
	}
	return c.loadRowsForPKs(ectx, pks)
}

// loadRowsForPKs fetches and appends the rows for a slice of PKs. PKs
// that don't resolve (already deleted) are silently skipped.
func (c *swytchCursor) loadRowsForPKs(ectx *effects.Context, pks []string) error {
	for _, pk := range pks {
		row, err := c.loadRow(ectx, pk)
		if err != nil {
			return err
		}
		if row != nil {
			c.rows = append(c.rows, *row)
		}
	}
	return nil
}

// findIndex returns a pointer to the cached indexSchema with the
// given name, or nil.
func (v *swytchVTable) findIndex(name string) *indexSchema {
	for i := range v.schema.Indexes {
		if v.schema.Indexes[i].Name == name {
			return &v.schema.Indexes[i]
		}
	}
	return nil
}

// netAddPKs collects the field ids (= PKs) from a reduced index
// entry snapshot.
func netAddPKs(snap *pb.ReducedEffect) []string {
	out := make([]string, 0, len(snap.NetAdds))
	for pk := range snap.NetAdds {
		out = append(out, pk)
	}
	return out
}

func (c *swytchCursor) Next() error {
	c.i++
	return nil
}

func (c *swytchCursor) Column(i int, _ bool) (sqlite.Value, error) {
	if c.i >= len(c.rows) {
		return sqlite.Value{}, io.EOF
	}
	row := c.rows[c.i]
	if i < 0 || i >= len(row.cols) {
		return sqlite.Value{}, fmt.Errorf("column %d out of range (have %d)", i, len(row.cols))
	}
	return row.cols[i], nil
}

func (c *swytchCursor) RowID() (int64, error) {
	if c.i >= len(c.rows) {
		return 0, io.EOF
	}
	pk := c.rows[c.i].pk
	id := c.vt.rowIDForPK(pk)
	// Any PK shape that isn't single-column INTEGER uses a hashed
	// rowid, so Update / DeleteRow need the reverse-map entry. The
	// single-column INTEGER case round-trips 1:1 with no cache.
	if !c.vt.schema.integerRowidFastPath() {
		c.vt.rememberRowID(id, pk)
	}
	return id, nil
}

func (c *swytchCursor) EOF() bool { return c.i >= len(c.rows) }

func (c *swytchCursor) Close() error { return nil }

// loadRow pulls a single row out of swytch, decoded per the schema.
// Returns (nil, nil) if the row does not exist.
func (c *swytchCursor) loadRow(ectx *effects.Context, pk string) (*materialisedRow, error) {
	if !c.vt.schema.hasPK() {
		return nil, fmt.Errorf("table %q has no primary key", c.vt.name)
	}

	reduced, _, err := ectx.GetSnapshot(rowKey(c.vt.schema.TableID, pk))
	if err != nil {
		return nil, err
	}
	if reduced == nil {
		return nil, nil
	}
	// A row with no remaining fields has been fully deleted.
	if len(reduced.NetAdds) == 0 {
		return nil, nil
	}

	cols := make([]sqlite.Value, len(c.vt.schema.Columns))
	pkVals := decodePK(pk, c.vt.schema.pkAffinities())
	for i, col := range c.vt.schema.Columns {
		if pos := pkPosition(c.vt.schema, i); pos >= 0 {
			cols[i] = pkVals[pos]
			continue
		}
		el, ok := reduced.NetAdds[col.Name]
		if !ok || el.Data == nil {
			cols[i] = defaultValueToSqlite(col.Default)
			continue
		}
		cols[i] = decodeValue(el.Data, col.Affinity)
	}
	return &materialisedRow{pk: pk, cols: cols}, nil
}

// defaultValueToSqlite converts a column's DEFAULT into the
// sqlite.Value materialised at read time. Nil / "null" kind reads as
// NULL; everything else constructs the typed value. Used when a row
// has no stored field for the column, letting ALTER TABLE ADD COLUMN
// DEFAULT take effect without rewriting existing rows.
func defaultValueToSqlite(d *defaultValue) sqlite.Value {
	if d == nil || d.Kind == "null" {
		return sqlite.Value{}
	}
	switch d.Kind {
	case "int":
		return sqlite.IntegerValue(d.Int)
	case "float":
		return sqlite.FloatValue(d.Float)
	case "text":
		return sqlite.TextValue(d.Text)
	case "blob":
		return sqlite.BlobValue(d.Blob)
	}
	return sqlite.Value{}
}

// maxRMWRetries caps transaction retries on ErrTxnAborted. A real
// concurrent contention spike would retry a handful of times; this
// limit protects against pathological loops without giving up on the
// first transient conflict.
const maxRMWRetries = 16

// runRMW executes a read-modify-write body inside a swytch
// transaction, retrying on ErrTxnAborted. The body receives a fresh
// Context each attempt and must perform its reads (via GetSnapshot)
// and writes on that Context; runRMW handles BeginTx and Flush.
//
// This is orthogonal to SQL-level transactions (BEGIN/COMMIT over pg
// wire). Every storage-level RMW — whether inside an explicit SQL
// transaction or a single auto-committed statement — goes through
// this helper so concurrent writers can't silently clobber a
// read-stale value.
func runRMW(engine *effects.Engine, label string, body func(*effects.Context) error) error {
	var lastErr error
	for range maxRMWRetries {
		ectx := engine.NewContext()
		ectx.BeginTx()
		if err := body(ectx); err != nil {
			ectx.Abort()
			return err
		}
		// Hold striped per-key locks across Flush. Without this,
		// two concurrent runRMW calls on overlapping keys can both
		// pass checkCompetingBinds (each sees only the other's
		// pre-bind DATA tip, not a bind) and both emit binds; the
		// loser is only caught at flushTx's evaluateBindForkChoice
		// AFTER one has already returned success to the caller on
		// the winner side. Redis avoids this by locking the first
		// key around runner + Flush in its handler
		// (redis/handler.go around line 514); SQL does the same
		// here, covering every key the tx touches. Locks are taken
		// in sorted order to avoid deadlock when two multi-key txs
		// overlap on a subset of keys.
		unlock := lockEngineKeys(engine, ectx.PendingKeys())
		err := ectx.Flush()
		unlock()
		if err == nil {
			return nil
		}
		if !errors.Is(err, effects.ErrTxnAborted) {
			return fmt.Errorf("%s flush: %w", label, err)
		}
		lastErr = err
		// Loop: reads will pick up fresh state on the next attempt.
	}
	return fmt.Errorf("%s: exceeded %d retries: %w", label, maxRMWRetries, lastErr)
}

// lockEngineKeys acquires the engine's striped per-key locks for
// each name, in sorted order, and returns an unlock function. Sort
// order is required for deadlock-freedom: two callers with
// overlapping key sets always take locks in the same order.
//
// This is the SQL analog of the redis handler's per-command key
// lock — it serialises Flush's fork-choice critical section against
// other commits on the same key, so concurrent local txns compete
// honestly (one wins, the other sees the winner's bind at
// checkCompetingBinds and aborts before emitting).
func lockEngineKeys(engine *effects.Engine, keys []string) func() {
	if len(keys) == 0 {
		return func() {}
	}
	sorted := make([]string, len(keys))
	copy(sorted, keys)
	sort.Strings(sorted)
	for _, k := range sorted {
		engine.GetLock(k).Lock()
	}
	unlocked := false
	return func() {
		if unlocked {
			return
		}
		unlocked = true
		for i := len(sorted) - 1; i >= 0; i-- {
			engine.GetLock(sorted[i]).Unlock()
		}
	}
}
