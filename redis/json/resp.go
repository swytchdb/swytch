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

// handleJSONResp — JSON.RESP key [path]
//
// Renders the matched value(s) in RedisJSON's RESP form: null → null bulk;
// boolean → the simple string "true"/"false"; integer → RESP integer; number →
// bulk string of its text; string → bulk string; array → a RESP array led by the
// simple string "[" then the elements; object → a RESP array led by the simple
// string "{" then alternating key (bulk string) and value. JSONPath wraps the
// matches in an outer array; legacy reports the first match bare. The default
// path is the legacy root ".".
func handleJSONResp(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 || len(cmd.Args) > 2 {
		w.WriteWrongNumArguments("json.resp")
		return
	}
	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true
	runner = func() {
		pathStr := "."
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
			w.WriteNullBulkString()
			return
		}
		if wrongType {
			writeWrongType(w)
			return
		}
		matches := path.resolve(assemble(root, ""))
		if path.JSONPath {
			w.WriteArray(len(matches))
			for _, m := range matches {
				writeRESPValue(w, m)
			}
			return
		}
		if len(matches) == 0 {
			w.WriteError("ERR Path '" + pathStr + "' does not exist")
			return
		}
		writeRESPValue(w, matches[0])
	}
	return
}

// writeRESPValue emits one JSON value in RedisJSON's RESP encoding.
func writeRESPValue(w *shared.Writer, v *Value) {
	switch v.Kind {
	case KindNull:
		w.WriteNullBulkString()
	case KindBool:
		if v.Bool {
			w.WriteSimpleString("true")
		} else {
			w.WriteSimpleString("false")
		}
	case KindInt:
		w.WriteInteger(v.Int)
	case KindFloat:
		w.WriteBulkString(Serialize(v))
	case KindString:
		w.WriteBulkStringStr(v.Str)
	case KindArray:
		w.WriteArray(len(v.Arr) + 1)
		w.WriteSimpleString("[")
		for _, e := range v.Arr {
			writeRESPValue(w, e)
		}
	case KindObject:
		w.WriteArray(len(v.Obj)*2 + 1)
		w.WriteSimpleString("{")
		for _, m := range v.Obj {
			w.WriteBulkStringStr(m.Key)
			writeRESPValue(w, m.Val)
		}
	}
}
