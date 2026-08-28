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

package hash

import (
	"strings"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/swytch/redis/shared"
)

func handleHGetDel(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	// HGETDEL key FIELDS numfields field [field ...]
	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("hgetdel")
		return
	}

	key := string(cmd.Args[0])

	// Check for FIELDS keyword
	if strings.ToUpper(string(cmd.Args[1])) != "FIELDS" {
		w.WriteError("ERR mandatory argument FIELDS is missing, or not at the right position")
		return
	}

	// Parse numfields
	numFields, ok := shared.ParseInt64(cmd.Args[2])
	if !ok || numFields <= 0 {
		w.WriteError("ERR Number of fields must be a positive integer")
		return
	}

	// Validate numfields matches actual field count
	actualFields := len(cmd.Args) - 3
	if actualFields != int(numFields) {
		w.WriteError("ERR The `numfields` parameter must match the number of arguments")
		return
	}

	fields := make([]string, numFields)
	for i := range numFields {
		fields[i] = string(cmd.Args[3+i])
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, tips, exists, wrongType, err := getHashSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			// Return array of nils
			w.WriteArray(int(numFields))
			for range numFields {
				w.WriteNullBulkString()
			}
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		cmd.Context.BeginTx()

		// Emit Noop to record the read
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			w.WriteError(err.Error())
			return
		}

		// Get values and emit removes for existing fields
		w.WriteArray(int(numFields))
		for _, field := range fields {
			elem, fieldExists := snap.NetAdds[field]
			if fieldExists {
				w.WriteBulkString(elementValueBytes(elem.Data))
				if err := emitHashRemove(cmd, key, []byte(field)); err != nil {
					w.WriteError(err.Error())
					return
				}
			} else {
				w.WriteNullBulkString()
			}
		}
	}
	return
}
