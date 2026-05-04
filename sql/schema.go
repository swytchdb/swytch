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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"time"

	"github.com/zeebo/xxh3"
	"zombiezen.com/go/sqlite"

	pb "github.com/swytchdb/cache/cluster/proto"
	"github.com/swytchdb/cache/effects"
)

// columnSchema describes a single column in a swytch-backed table.
//
// Default, when non-nil, is applied at read time: if a row has no
// stored value for this column, the Default is materialised into the
// result. That mirrors SQLite's ADD COLUMN DEFAULT semantics without
// having to rewrite every row on ALTER. NOT NULL columns require a
// Default at CREATE/ALTER time so existing (and future) rows without
// an explicit value still satisfy the constraint.
type columnSchema struct {
	Name     string        `json:"name"`
	Affinity string        `json:"affinity"` // INTEGER / REAL / TEXT / BLOB / NUMERIC
	DeclType string        `json:"decl_type"`
	NotNull  bool          `json:"not_null"`
	IsPK     bool          `json:"is_pk"`
	Default  *defaultValue `json:"default,omitempty"`
}

// defaultValue is the on-disk representation of a column DEFAULT.
// Kept separate from TypedValue / sqlite.Value so JSON round-tripping
// in the schema blob is explicit and robust to API churn in either
// dependency.
type defaultValue struct {
	// Kind identifies which payload field carries the value.
	// "null" means DEFAULT NULL (explicit).
	Kind  string  `json:"kind"` // "int" | "float" | "text" | "blob" | "null"
	Int   int64   `json:"int,omitempty"`
	Float float64 `json:"float,omitempty"`
	Text  string  `json:"text,omitempty"`
	Blob  []byte  `json:"blob,omitempty"`
}

// tableSchema is the on-disk representation of a CREATE TABLE. It is
// persisted under the "s/<table>" key and replayed on session open so
// that every node can issue the matching CREATE VIRTUAL TABLE.
//
// TableID is the stable 16-byte identifier that backs row keys (see
// keys.go). It's generated at CREATE TABLE time from the HLC +
// originating NodeID + name and never changes — RENAME TABLE moves
// the `s/<name>` metadata entry but keeps the tableID, so row keys
// don't move.
//
// PKColumns lists the column ordinals that together form the primary
// key, in PK-order (e.g. `PRIMARY KEY (a, b)` gives [a_ord, b_ord]).
// Length 1 is the common single-column case; ≥2 is composite. The
// row-key suffix is `encodePK(pkValues, pkAffinities)`.
//
// SyntheticPK is true for tables declared without any PRIMARY KEY. In
// that case PKColumns is empty and row identity is a per-insert int64
// allocated by the vtab; the row-key suffix is
// `encodeIndexValue(int64(rowid), INTEGER)` — the same encoding used
// for a single-column INTEGER PK, which lets the read-side
// `integerRowidFastPath` handle these rows unchanged.
type tableSchema struct {
	Name        string         `json:"name"`
	TableID     []byte         `json:"table_id"`
	Columns     []columnSchema `json:"columns"`
	PKColumns   []int          `json:"pk_columns"`
	Indexes     []indexSchema  `json:"indexes"`
	SyntheticPK bool           `json:"synthetic_pk,omitempty"`
}

// indexSchema describes a single secondary index over a table.
// Phase 4 supports single-column indices only; Columns is kept as a
// slice so composite indices can be added later without migrating
// the schema representation.
//
// IndexID is the stable 16-byte identifier that backs index-entry
// keys. TableID points at the table's stable identifier so RENAME
// TABLE doesn't have to rewrite any per-index metadata.
type indexSchema struct {
	Name    string   `json:"name"`
	IndexID []byte   `json:"index_id"`
	Table   string   `json:"table"`
	TableID []byte   `json:"table_id"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique"`
}

// deriveTableID produces a stable 16-byte identifier for a table from
// its creation HLC + originating NodeID + user-visible name. The
// inputs are unique by construction: HLC is monotonic per node and
// distinguished across nodes by NodeID, so collisions require a
// 128-bit hash collision on guaranteed-unique inputs (astronomically
// rare). The result goes into row keys and is stored as-is in
// `s/<name>.tableID`.
func deriveTableID(hlc time.Time, nodeID pb.NodeID, name string) []byte {
	return deriveID(hlc, nodeID, name)
}

// deriveIndexID is deriveTableID for indices. Separate function so
// future ID domain changes (e.g. a versioning prefix) can diverge.
func deriveIndexID(hlc time.Time, nodeID pb.NodeID, name string) []byte {
	return deriveID(hlc, nodeID, name)
}

func deriveID(hlc time.Time, nodeID pb.NodeID, name string) []byte {
	// Layout: HLC_ns(8) || nodeID(8) || name
	payload := make([]byte, 0, 8+8+len(name))
	payload = binary.BigEndian.AppendUint64(payload, uint64(hlc.UnixNano()))
	payload = binary.BigEndian.AppendUint64(payload, uint64(nodeID))
	payload = append(payload, name...)
	h := xxh3.Hash128(payload).Bytes()
	out := make([]byte, 16)
	copy(out, h[:])
	return out
}

// nextSyntheticRowID allocates a positive int64 rowid for a row in a
// SyntheticPK table. The seq is expected to be a monotonic per-Server
// counter so that (HLC_ns, nodeID, seq) is unique per allocation and
// deriveID's xxh3 produces distinct outputs. The low 63 bits of the
// digest become the rowid; collisions at 2^-63 per pair are the
// practical upper bound.
func nextSyntheticRowID(hlc time.Time, nodeID pb.NodeID, seq uint64) int64 {
	name := "row:" + strconv.FormatUint(seq, 10)
	digest := deriveID(hlc, nodeID, name)
	return int64(binary.BigEndian.Uint64(digest[:8]) & 0x7fff_ffff_ffff_ffff)
}

// columnAffinity returns the affinity of the indexed column within
// the given table schema, or "" if the column name isn't found.
func (s *indexSchema) columnAffinity(tbl *tableSchema) string {
	if len(s.Columns) == 0 {
		return ""
	}
	name := s.Columns[0]
	for _, c := range tbl.Columns {
		if c.Name == name {
			return c.Affinity
		}
	}
	return ""
}

// columnIndex returns the index into tbl.Columns for the indexed
// column's leading entry, or -1 if not found. Kept for callers that
// only care about the first column (typically the planner's
// leftmost-column matcher).
func (s *indexSchema) columnIndex(tbl *tableSchema) int {
	if len(s.Columns) == 0 {
		return -1
	}
	name := s.Columns[0]
	for i, c := range tbl.Columns {
		if c.Name == name {
			return i
		}
	}
	return -1
}

// columnIndexes resolves every indexed column to its ordinal in
// tbl.Columns. Any column not found returns -1 in its slot; callers
// should treat that as the index being stale against the current
// schema (e.g. a concurrent RENAME COLUMN that our metadata hasn't
// caught up with yet).
func (s *indexSchema) columnIndexes(tbl *tableSchema) []int {
	out := make([]int, len(s.Columns))
	for i, name := range s.Columns {
		out[i] = -1
		for j, c := range tbl.Columns {
			if c.Name == name {
				out[i] = j
				break
			}
		}
	}
	return out
}

// columnAffinities returns the affinities of every indexed column
// in index-declaration order.
func (s *indexSchema) columnAffinities(tbl *tableSchema) []string {
	out := make([]string, len(s.Columns))
	for i, name := range s.Columns {
		for _, c := range tbl.Columns {
			if c.Name == name {
				out[i] = c.Affinity
				break
			}
		}
	}
	return out
}

// isComposite reports whether the PK has more than one column.
func (t *tableSchema) isComposite() bool {
	return len(t.PKColumns) > 1
}

// hasPK reports whether the table has any form of primary-key identity
// — either user-declared PK columns or a synthesized per-row rowid.
func (t *tableSchema) hasPK() bool {
	return t.SyntheticPK || len(t.PKColumns) > 0
}

// pkAffinities returns the affinities of the PK columns in PK order.
// For synthetic-PK tables the identity is a single INTEGER rowid.
func (t *tableSchema) pkAffinities() []string {
	if t.SyntheticPK {
		return []string{affinityInteger}
	}
	out := make([]string, len(t.PKColumns))
	for i, ord := range t.PKColumns {
		out[i] = t.Columns[ord].Affinity
	}
	return out
}

// pkValues extracts PK component values from a full row tuple in
// column order. Assumes cols is ordinal-aligned with t.Columns.
func (t *tableSchema) pkValues(cols []sqlite.Value) []sqlite.Value {
	out := make([]sqlite.Value, len(t.PKColumns))
	for i, ord := range t.PKColumns {
		out[i] = cols[ord]
	}
	return out
}

// primaryPKOrd returns the ordinal of the first PK column. For the
// common single-column case that's the whole story; the legacy
// IntegerRowid optimization (where the PK column's int64 doubles as
// the SQLite rowid) only applies when len(PKColumns) == 1 and that
// column's affinity is INTEGER.
func (t *tableSchema) primaryPKOrd() int {
	if len(t.PKColumns) == 0 {
		return -1
	}
	return t.PKColumns[0]
}

// pkColumn returns the first PK column (for the common single-column
// case). Composite callers should use PKColumns directly.
func (t *tableSchema) pkColumn() *columnSchema {
	ord := t.primaryPKOrd()
	if ord < 0 || ord >= len(t.Columns) {
		return nil
	}
	return &t.Columns[ord]
}

// isPKColumn reports whether the given column ordinal is part of
// the primary key.
func (t *tableSchema) isPKColumn(ord int) bool {
	return slices.Contains(t.PKColumns, ord)
}

// pkPosition returns the ordinal position of ord within t.PKColumns,
// or -1 when ord isn't a PK column. Useful when reconstructing a
// row's column tuple from the decoded PK components.
func pkPosition(t *tableSchema, ord int) int {
	for i, o := range t.PKColumns {
		if o == ord {
			return i
		}
	}
	return -1
}

// integerRowidFastPath reports whether this table qualifies for the
// "rowid == pk" optimization that lets us skip the reverse-lookup
// map: single-column PK with INTEGER affinity, or a synthesized
// int64 rowid. Composite PKs always need the xxh3-hashed rowid + map.
func (t *tableSchema) integerRowidFastPath() bool {
	if t.SyntheticPK {
		return true
	}
	if len(t.PKColumns) != 1 {
		return false
	}
	return t.Columns[t.PKColumns[0]].Affinity == affinityInteger
}

// emitSchema writes the schema for a table as keyed fields under
// "s/<name>" and marks the table present in the replicated table-list
// key "L". Callers are expected to Flush the context afterward.
func emitSchema(ctx *effects.Context, schema *tableSchema) error {
	if len(schema.TableID) == 0 {
		return fmt.Errorf("sql: emitSchema: table %q has no TableID", schema.Name)
	}
	colsJSON, err := json.Marshal(schema.Columns)
	if err != nil {
		return fmt.Errorf("sql: marshal schema columns: %w", err)
	}
	idxJSON, err := json.Marshal(schema.Indexes)
	if err != nil {
		return fmt.Errorf("sql: marshal schema indexes: %w", err)
	}

	pkJSON, err := json.Marshal(schema.PKColumns)
	if err != nil {
		return fmt.Errorf("sql: marshal schema pk: %w", err)
	}

	sk := schemaKey(schema.Name)
	if err := emitRawField(ctx, sk, schemaFieldTableID, schema.TableID); err != nil {
		return err
	}
	if err := emitRawField(ctx, sk, schemaFieldColumns, colsJSON); err != nil {
		return err
	}
	if err := emitRawField(ctx, sk, schemaFieldPK, pkJSON); err != nil {
		return err
	}
	if err := emitRawField(ctx, sk, schemaFieldIndexes, idxJSON); err != nil {
		return err
	}
	if schema.SyntheticPK {
		if err := emitRawField(ctx, sk, schemaFieldSyntheticPK, []byte{1}); err != nil {
			return err
		}
	}
	// Presence marker in the replicated table list.
	return emitRawField(ctx, tableListKey, schema.Name, []byte{1})
}

// loadSchema reads the schema for a table from swytch, or returns
// ErrNoSuchTable if it isn't registered.
func loadSchema(ctx *effects.Context, name string) (*tableSchema, error) {
	reduced, _, err := ctx.GetSnapshot(schemaKey(name))
	if err != nil {
		return nil, fmt.Errorf("sql: read schema %q: %w", name, err)
	}
	if reduced == nil || len(reduced.NetAdds) == 0 {
		return nil, ErrNoSuchTable
	}
	return schemaFromSnapshot(name, reduced)
}

// schemaFromSnapshot decodes a tableSchema from an already-fetched
// ReducedEffect. Callers that need the tips of the snapshot (for
// dependency pinning) can fetch directly and reuse this decoder.
func schemaFromSnapshot(name string, reduced *pb.ReducedEffect) (*tableSchema, error) {
	if reduced == nil || len(reduced.NetAdds) == 0 {
		return nil, ErrNoSuchTable
	}

	colsEl, ok := reduced.NetAdds[schemaFieldColumns]
	if !ok || colsEl.Data == nil {
		return nil, fmt.Errorf("sql: schema for %q missing columns field", name)
	}
	var cols []columnSchema
	if err := json.Unmarshal(colsEl.Data.GetRaw(), &cols); err != nil {
		return nil, fmt.Errorf("sql: unmarshal schema columns for %q: %w", name, err)
	}

	var pkCols []int
	if pkEl, ok := reduced.NetAdds[schemaFieldPK]; ok && pkEl.Data != nil {
		raw := pkEl.Data.GetRaw()
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &pkCols); err != nil {
				return nil, fmt.Errorf("sql: unmarshal pk_columns for %q: %w", name, err)
			}
		}
	}

	var syntheticPK bool
	if spkEl, ok := reduced.NetAdds[schemaFieldSyntheticPK]; ok && spkEl.Data != nil {
		raw := spkEl.Data.GetRaw()
		if len(raw) > 0 && raw[0] != 0 {
			syntheticPK = true
		}
	}

	if len(pkCols) == 0 && !syntheticPK {
		return nil, fmt.Errorf("sql: schema for %q missing pk_columns", name)
	}

	var tableID []byte
	if idEl, ok := reduced.NetAdds[schemaFieldTableID]; ok && idEl.Data != nil {
		tableID = idEl.Data.GetRaw()
	}
	if len(tableID) == 0 {
		return nil, fmt.Errorf("sql: schema for %q missing tableID field", name)
	}

	var indexes []indexSchema
	if idxEl, ok := reduced.NetAdds[schemaFieldIndexes]; ok && idxEl.Data != nil {
		raw := idxEl.Data.GetRaw()
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &indexes); err != nil {
				return nil, fmt.Errorf("sql: unmarshal schema indexes for %q: %w", name, err)
			}
		}
	}

	return &tableSchema{
		Name:        name,
		TableID:     tableID,
		Columns:     cols,
		PKColumns:   pkCols,
		Indexes:     indexes,
		SyntheticPK: syntheticPK,
	}, nil
}

// listTables returns the names of all tables currently present in the
// replicated "L" key, sorted for determinism.
func listTables(ctx *effects.Context) ([]string, error) {
	reduced, _, err := ctx.GetSnapshot(tableListKey)
	if err != nil {
		return nil, fmt.Errorf("sql: read table list: %w", err)
	}
	if reduced == nil || len(reduced.NetAdds) == 0 {
		return nil, nil
	}
	names := make([]string, 0, len(reduced.NetAdds))
	for name := range reduced.NetAdds {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// emitRawField emits a KEYED INSERT for a raw byte value under a
// given field id.
func emitRawField(ctx *effects.Context, key, field string, value []byte) error {
	return ctx.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(field),
			Value:      &pb.DataEffect_Raw{Raw: value},
		}},
	})
}

// emitIntField emits a KEYED INSERT for an integer field value.
func emitIntField(ctx *effects.Context, key, field string, value int64) error {
	return ctx.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(field),
			Value:      &pb.DataEffect_IntVal{IntVal: value},
		}},
	})
}
