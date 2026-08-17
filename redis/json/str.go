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

// handleJSONStrLen — JSON.STRLEN key [path]
func handleJSONStrLen(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 || len(cmd.Args) > 2 {
		w.WriteWrongNumArguments("json.strlen")
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
		jsonReadInt(cmd, w, key, pathStr, "a string", func(m *Value) (int64, bool) {
			if m.Kind != KindString {
				return 0, false
			}
			return int64(len(m.Str)), true
		})
	}
	return
}

// handleJSONStrAppend — JSON.STRAPPEND key [path] value
func handleJSONStrAppend(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 || len(cmd.Args) > 3 {
		w.WriteWrongNumArguments("json.strappend")
		return
	}
	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true
	runner = func() {
		// The value is always the last argument; the path is optional and
		// defaults to the root.
		pathStr := "$"
		valArg := cmd.Args[1]
		if len(cmd.Args) == 3 {
			pathStr = string(cmd.Args[1])
			valArg = cmd.Args[2]
		}
		suffix, err := Parse(valArg)
		if err != nil || suffix.Kind != KindString {
			w.WriteError("ERR value is not a string")
			return
		}
		scalarIntWrite(cmd, w, key, pathStr, "a string", func(m *Value) (*Value, int64, bool) {
			if m.Kind != KindString {
				return nil, 0, false
			}
			ns := m.Str + suffix.Str
			return newString(ns), int64(len(ns)), true
		})
	}
	return
}
