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

// handleJSONToggle — JSON.TOGGLE key path
func handleJSONToggle(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 2 {
		w.WriteWrongNumArguments("json.toggle")
		return
	}
	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true
	runner = func() {
		pathStr := string(cmd.Args[1])
		scalarIntWrite(cmd, w, key, pathStr, "a boolean", func(m *Value) (*Value, int64, bool) {
			if m.Kind != KindBool {
				return nil, 0, false
			}
			nb := !m.Bool
			var n int64
			if nb {
				n = 1
			}
			return newBool(nb), n, true
		})
	}
	return
}
