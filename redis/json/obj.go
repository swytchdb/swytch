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

import "github.com/swytchdb/swytch/redis/shared"

// handleJSONObjLen — JSON.OBJLEN key [path]
func handleJSONObjLen(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 || len(cmd.Args) > 2 {
		w.WriteWrongNumArguments("json.objlen")
		return
	}
	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true
	runner = func() {
		pathStr := "$"
		if len(cmd.Args) == 2 {
			pathStr = string(cmd.Args[1])
		}
		jsonReadInt(cmd, w, key, pathStr, "an object", func(m *Value) (int64, bool) {
			if m.Kind != KindObject {
				return 0, false
			}
			return int64(len(m.Obj)), true
		})
	}
	return
}

// handleJSONObjKeys — JSON.OBJKEYS key [path]
func handleJSONObjKeys(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 || len(cmd.Args) > 2 {
		w.WriteWrongNumArguments("json.objkeys")
		return
	}
	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true
	runner = func() {
		pathStr := "$"
		if len(cmd.Args) == 2 {
			pathStr = string(cmd.Args[1])
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
		if !exists {
			w.WriteNullArray()
			return
		}
		if wrongType {
			writeWrongType(w)
			return
		}
		matches := path.resolve(assemble(root, ""))
		if path.JSONPath {
			// One entry per match: the member-key array, or null for a
			// non-object match.
			w.WriteArray(len(matches))
			for _, m := range matches {
				if m.Kind == KindObject {
					writeObjKeys(w, m)
				} else {
					w.WriteNullArray()
				}
			}
			return
		}
		if len(matches) == 0 {
			w.WriteError("ERR Path '" + pathStr + "' does not exist")
			return
		}
		if matches[0].Kind != KindObject {
			w.WriteError("ERR wrong type of path value - expected an object but found " + matches[0].TypeName())
			return
		}
		writeObjKeys(w, matches[0])
	}
	return
}

// writeObjKeys writes an object's member names as a RESP array, in insertion
// order.
func writeObjKeys(w *shared.Writer, obj *Value) {
	w.WriteArray(len(obj.Obj))
	for _, m := range obj.Obj {
		w.WriteBulkString([]byte(m.Key))
	}
}
