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
)

// Key space layout.
//
// Row keys and index-entry keys name their target table/index by a
// stable 16-byte ID (see schema.deriveTableID / deriveIndexID), NOT
// by the user-visible name. That lets RENAME TABLE and RENAME INDEX
// be metadata-only: the physical row/entry keys never move.
//
// Row key:     "r" + <be32 len(tableID)> + tableID + pk-bytes
// Index entry: "i" + <be32 len(indexID)> + indexID + encoded-value-bytes
//
// Keys that reach the user directly (schema metadata, registries)
// stay name-keyed — those are the lookup points the DDL layer
// resolves before it ever reaches a row:
//
//	s/<table> — schema metadata
//	x/<index> — index metadata
//	v/<view>  — view metadata (SELECT body text)
//	L         — table registry (fields: table name → marker)
//	I         — index registry
//	V         — view registry
const (
	prefixRow    = "r"
	prefixIndex  = "i"
	prefixSchema = "s"
	prefixXIndex = "x"
	prefixView   = "v"

	tableListKey = "L"
	viewListKey  = "V"
)

// Schema-metadata field names under the "s/<table>" key.
const (
	schemaFieldColumns     = "cols"
	schemaFieldPK          = "pk"
	schemaFieldIndexes     = "idx"
	schemaFieldTableID     = "tid"
	schemaFieldSyntheticPK = "spk"

	// rowFieldSyntheticRowID is the keyed field under a row's
	// "<tableID>/<pk>" key that carries the synthetic rowid for
	// SyntheticPK tables. Serves as the row-presence marker so a
	// row whose user columns are all NULL still reads as live.
	rowFieldSyntheticRowID = "__rowid"
)

// Index-metadata field names under the "x/<index>" key.
const (
	indexFieldTable   = "table"
	indexFieldTableID = "tid"
	indexFieldColumns = "cols"
	indexFieldUnique  = "unique"
	indexFieldIndexID = "iid"
)

// View-metadata field names under the "v/<view>" key. Views are pure
// SQLite objects — the persisted body is the text that follows the
// view name in CREATE VIEW (typically "AS <select>", optionally with a
// leading "(col1, col2)" projection list). On session open / schema
// refresh we rebuild "CREATE VIEW <name> <body>" and let SQLite
// compile it against that session's declared schema.
const (
	viewFieldSQL = "sql"
)

// indexKey returns the swytch key that holds the entries for rows
// whose indexed value encodes to `encoded` under the given index.
// The encoded bytes are opaque; any byte value is legal.
func indexKey(indexID []byte, encoded []byte) string {
	header := encodeIDHeader(prefixIndex, indexID)
	buf := make([]byte, len(header)+len(encoded))
	copy(buf, header)
	copy(buf[len(header):], encoded)

	return string(buf)
}

// indexKeyPrefix returns the prefix every entry key under the given
// index starts with.
func indexKeyPrefix(indexID []byte) string {
	return string(encodeIDHeader(prefixIndex, indexID))
}

// indexKeyPattern returns a ScanKeys glob matching every entry of the
// given index, with glob metacharacters in the prefix bytes escaped.
func indexKeyPattern(indexID []byte) string {
	return escapeGlob(indexKeyPrefix(indexID)) + "*"
}

// indexMetaKey returns the swytch key that holds index-metadata
// fields for the given index name. This stays name-keyed because the
// DDL layer starts from the user-visible name.
func indexMetaKey(index string) string {
	return prefixXIndex + "/" + index
}

// viewMetaKey returns the swytch key that holds view-metadata fields
// for the given view name. Like indexMetaKey it stays name-keyed:
// views have no physical row/entry keyspace, so there's nothing to
// insulate from a RENAME, and CREATE-after-DROP of the same name is
// naturally handled by the keyed collection's REMOVE + INSERT flow.
func viewMetaKey(view string) string {
	return prefixView + "/" + view
}

// indexListKey is the replicated list of registered index names. We
// separate this from the table list "L" so CREATE INDEX doesn't
// churn the table membership key and vice versa.
const indexListKey = "I"

// glob returns the input wrapped as a ScanKeys glob pattern that
// matches anything starting with the given literal prefix — glob
// meta-characters in the prefix are escaped so random ID bytes
// can't be reinterpreted.
func glob(prefix string) string {
	return escapeGlob(prefix) + "*"
}

// encodeIDHeader builds the length-prefix header: prefix byte plus a
// 4-byte big-endian length. The ID itself is opaque bytes; we keep
// the length prefix (rather than a fixed 16-byte inline) so the
// format can absorb future ID sizes without a second migration.
func encodeIDHeader(prefix string, id []byte) []byte {
	buf := make([]byte, 1+4+len(id))
	buf[0] = prefix[0]
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(id)))
	copy(buf[5:], id)

	return buf
}

// rowKey returns the swytch key that holds the row with the given
// primary key in the given table. pk is treated as opaque bytes.
func rowKey(tableID []byte, pk string) string {
	header := encodeIDHeader(prefixRow, tableID)
	buf := make([]byte, len(header)+len(pk))
	copy(buf, header)
	copy(buf[len(header):], pk)

	return string(buf)
}

// rowKeyPrefix returns the prefix every row key in the given table
// starts with.
func rowKeyPrefix(tableID []byte) string {
	return string(encodeIDHeader(prefixRow, tableID))
}

// rowKeyPattern returns a ScanKeys glob matching every row of a table.
// Glob meta-characters in the prefix bytes are escaped so random
// tableID bytes can't be misinterpreted as pattern syntax.
func rowKeyPattern(tableID []byte) string {
	return escapeGlob(rowKeyPrefix(tableID)) + "*"
}

// parseRowKey extracts the primary key bytes from a row key belonging
// to the given table. Returns ok=false when the key's prefix doesn't
// match (wrong tableID, wrong namespace, or too short).
func parseRowKey(key string, tableID []byte) (string, bool) {
	header := rowKeyPrefix(tableID)
	if len(key) < len(header) {
		return "", false
	}

	if key[:len(header)] != header {
		return "", false
	}

	return key[len(header):], true
}

// escapeGlob escapes the keytrie.MatchGlob meta-characters so a
// literal byte in a prefix doesn't get interpreted as a pattern.
func escapeGlob(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '*', '?', '[', '\\':
			out = append(out, '\\', c)
		default:
			out = append(out, c)
		}
	}

	return string(out)
}

// schemaKey returns the swytch key that holds the schema-metadata
// fields for the given table. Name-keyed — the DDL layer resolves
// name → schema → tableID here.
func schemaKey(table string) string {
	return prefixSchema + "/" + table
}
