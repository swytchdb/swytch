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
	"errors"
	"fmt"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

// ErrUniqueViolation is returned from write paths when an INSERT or
// UPDATE would put two rows under the same value of a UNIQUE index.
var ErrUniqueViolation = errors.New("sql: unique constraint violation")

// loadIndexSchema reads an index's metadata from swytch, returning
// ErrNoSuchIndex if unregistered.
func loadIndexSchema(ctx *effects.Context, name string) (*indexSchema, error) {
	reduced, _, err := ctx.GetSnapshot(indexMetaKey(name))
	if err != nil {
		return nil, fmt.Errorf("sql: read index %q: %w", name, err)
	}
	if reduced == nil || len(reduced.NetAdds) == 0 {
		return nil, ErrNoSuchIndex
	}

	idx := &indexSchema{Name: name}
	if el, ok := reduced.NetAdds[indexFieldIndexID]; ok && el.Data != nil {
		idx.IndexID = el.Data.Decompress()
	}
	if el, ok := reduced.NetAdds[indexFieldTable]; ok && el.Data != nil {
		idx.Table = string(el.Data.Decompress())
	}
	if el, ok := reduced.NetAdds[indexFieldTableID]; ok && el.Data != nil {
		idx.TableID = el.Data.Decompress()
	}
	if el, ok := reduced.NetAdds[indexFieldColumns]; ok && el.Data != nil {
		raw := el.Data.Decompress()
		if len(raw) > 0 {
			for _, part := range splitNUL(raw) {
				idx.Columns = append(idx.Columns, string(part))
			}
		}
	}
	if el, ok := reduced.NetAdds[indexFieldUnique]; ok && el.Data != nil {
		idx.Unique = el.Data.GetIntVal() != 0
	}
	if len(idx.IndexID) == 0 {
		return nil, fmt.Errorf("sql: index %q missing IndexID field", name)
	}
	return idx, nil
}

// splitNUL splits a byte slice on NUL separators, returning the
// component slices. Used to decode the composite column-name list
// stored under indexFieldColumns.
func splitNUL(b []byte) [][]byte {
	var out [][]byte
	start := 0
	for i, c := range b {
		if c == 0 {
			out = append(out, b[start:i])
			start = i + 1
		}
	}
	out = append(out, b[start:])
	return out
}

// emitIndexMetadata writes the metadata fields under x/<index> and
// marks the index name present in the I registry. Not flushed.
func emitIndexMetadata(ctx *effects.Context, idx *indexSchema) error {
	if len(idx.IndexID) == 0 {
		return fmt.Errorf("sql: emitIndexMetadata: index %q has no IndexID", idx.Name)
	}
	if len(idx.TableID) == 0 {
		return fmt.Errorf("sql: emitIndexMetadata: index %q has no TableID", idx.Name)
	}
	mkey := indexMetaKey(idx.Name)

	if err := emitRawField(ctx, mkey, indexFieldIndexID, idx.IndexID); err != nil {
		return err
	}
	if err := emitRawField(ctx, mkey, indexFieldTable, []byte(idx.Table)); err != nil {
		return err
	}
	if err := emitRawField(ctx, mkey, indexFieldTableID, idx.TableID); err != nil {
		return err
	}
	// Columns serialised as a NUL-separated list; single column in
	// Phase 4 but the format supports composite later.
	colsBytes := []byte{}
	for i, c := range idx.Columns {
		if i > 0 {
			colsBytes = append(colsBytes, 0)
		}
		colsBytes = append(colsBytes, []byte(c)...)
	}
	if err := emitRawField(ctx, mkey, indexFieldColumns, colsBytes); err != nil {
		return err
	}
	uniq := int64(0)
	if idx.Unique {
		uniq = 1
	}
	if err := emitIntField(ctx, mkey, indexFieldUnique, uniq); err != nil {
		return err
	}
	return emitRawField(ctx, indexListKey, idx.Name, []byte{1})
}

// emitIndexMetadataRemove REMOVEs all metadata fields of an index
// plus the list entry. Not flushed.
func emitIndexMetadataRemove(ctx *effects.Context, name string) error {
	mkey := indexMetaKey(name)
	fields := []string{
		indexFieldIndexID, indexFieldTable, indexFieldTableID,
		indexFieldColumns, indexFieldUnique,
	}
	for _, f := range fields {
		if err := emitKeyedRemove(ctx, mkey, f); err != nil {
			return fmt.Errorf("remove %s.%s: %w", name, f, err)
		}
	}
	return emitKeyedRemove(ctx, indexListKey, name)
}

// emitIndexEntryInsert writes a single index entry: key = i/<idxID>/
// <encoded-value>, field = <pk string>, value = empty marker.
func emitIndexEntryInsert(ctx *effects.Context, indexID []byte, encoded []byte, pk string) error {
	return ctx.Emit(&pb.Effect{
		Key: []byte(indexKey(indexID, encoded)),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(pk),
			Value:      &pb.DataEffect_Raw{Raw: []byte{1}},
		}},
	})
}

// emitIndexEntryRemove REMOVEs a single index entry.
func emitIndexEntryRemove(ctx *effects.Context, indexID []byte, encoded []byte, pk string) error {
	return ctx.Emit(&pb.Effect{
		Key: []byte(indexKey(indexID, encoded)),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(pk),
		}},
	})
}

// checkUniqueBeforeInsert reads the target index key and returns
// ErrUniqueViolation if any other PK is already present there. The
// selfPK argument lets UPDATE call this without flagging its own
// existing entry as a conflict.
//
// Callers must have already emitted a Noop(tips) for the index key
// so a concurrent inserter's commit causes this transaction to
// abort rather than both of us proceeding.
func checkUniqueBeforeInsert(ctx *effects.Context, indexID []byte, encoded []byte, selfPK string) error {
	snap, _, err := ctx.GetSnapshot(indexKey(indexID, encoded))
	if err != nil {
		return fmt.Errorf("read unique index entry: %w", err)
	}
	if snap == nil {
		return nil
	}
	for pk := range snap.NetAdds {
		if pk != selfPK {
			return ErrUniqueViolation
		}
	}
	return nil
}
