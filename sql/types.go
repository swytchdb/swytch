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
	"zombiezen.com/go/sqlite"
)

// Well-known PostgreSQL type OIDs. These are stable values defined by
// pg's system catalog; hardcoded here to avoid a runtime dependency on
// pgx/pgtype in the server binary.
const (
	oidBytea   uint32 = 17
	oidInt8    uint32 = 20
	oidText    uint32 = 25
	oidFloat8  uint32 = 701
	oidUnknown uint32 = 705
)

// oidForSqliteType maps a SQLite runtime storage class to the pg OID
// we advertise on the wire. Called after stepping a row, when the
// actual value's type is known.
func oidForSqliteType(t sqlite.ColumnType) uint32 {
	switch t {
	case sqlite.TypeInteger:
		return oidInt8
	case sqlite.TypeFloat:
		return oidFloat8
	case sqlite.TypeText:
		return oidText
	case sqlite.TypeBlob:
		return oidBytea
	case sqlite.TypeNull:
		return oidText
	}

	return oidUnknown
}

// readColumn extracts the Go value for the given column index of a
// stepped SQLite statement, boxing it to the type the pg wire layer
// expects for the corresponding OID.
func readColumn(stmt *sqlite.Stmt, i int) any {
	switch stmt.ColumnType(i) {
	case sqlite.TypeInteger:
		return stmt.ColumnInt64(i)
	case sqlite.TypeFloat:
		return stmt.ColumnFloat(i)
	case sqlite.TypeText:
		return stmt.ColumnText(i)
	case sqlite.TypeBlob:
		n := stmt.ColumnLen(i)
		buf := make([]byte, n)
		stmt.ColumnBytes(i, buf)

		return buf
	case sqlite.TypeNull:
		return nil
	}

	return nil
}
