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

package json

import (
	"strings"

	"github.com/swytchdb/swytch/redis/shared"
)

// handleJSONDebug — JSON.DEBUG <MEMORY <key> [path] | HELP>
//
// MEMORY reports an estimate of the in-memory footprint of the value(s) at path
// (JSONPath → an integer per match; legacy → a bare integer; missing key →
// empty array for JSONPath, 0 for legacy). The byte figure is an estimate of our
// representation — exact parity with ReJSON's internal sizing is neither possible
// across engines nor part of the command's contract.
func handleJSONDebug(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("json.debug")
		return
	}
	switch strings.ToUpper(string(cmd.Args[0])) {
	case "HELP":
		valid = true
		runner = func() {
			w.WriteArray(2)
			w.WriteBulkStringStr("MEMORY <key> [path] - reports memory usage")
			w.WriteBulkStringStr("HELP                - this message")
		}
		return
	case "MEMORY":
		if len(cmd.Args) < 2 || len(cmd.Args) > 3 {
			w.WriteWrongNumArguments("json.debug")
			return
		}
		key := string(cmd.Args[1])
		keys = []string{key}
		valid = true
		runner = func() {
			pathStr := "."
			if len(cmd.Args) == 3 {
				pathStr = string(cmd.Args[2])
			}
			path, err := ParsePath(pathStr)
			if err != nil {
				w.WriteError("ERR " + err.Error())
				return
			}
			root, _, exists, wrongType, err := getJSONSnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if wrongType {
				writeWrongType(w)
				return
			}
			if !exists {
				if path.JSONPath {
					w.WriteArray(0)
				} else {
					w.WriteInteger(0)
				}
				return
			}
			matches := path.resolve(assemble(root, ""))
			if path.JSONPath {
				w.WriteArray(len(matches))
				for _, m := range matches {
					w.WriteInteger(jsonSizeOf(m))
				}
				return
			}
			if len(matches) == 0 {
				w.WriteError("ERR Path '" + pathStr + "' does not exist")
				return
			}
			w.WriteInteger(jsonSizeOf(matches[0]))
		}
		return
	default:
		w.WriteError("ERR unknown subcommand - try `JSON.DEBUG HELP`")
		return
	}
}

// scalarFootprint is the per-scalar and per-container base size used by the
// JSON.DEBUG MEMORY estimate (a flat scalar reports 8, matching ReJSON's common
// case).
const scalarFootprint = 8

// jsonSizeOf estimates the in-memory footprint of v in bytes: a base cost per
// node plus string contents and member keys.
func jsonSizeOf(v *Value) int64 {
	switch v.Kind {
	case KindString:
		return scalarFootprint + int64(len(v.Str))
	case KindArray:
		size := int64(scalarFootprint)
		for _, e := range v.Arr {
			size += jsonSizeOf(e)
		}
		return size
	case KindObject:
		size := int64(scalarFootprint)
		for _, m := range v.Obj {
			size += scalarFootprint + int64(len(m.Key)) + jsonSizeOf(m.Val)
		}
		return size
	default:
		return scalarFootprint
	}
}
