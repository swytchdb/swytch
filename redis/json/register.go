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

// keysJSONMSet extracts the keys from a JSON.MSET (every third arg: the key of
// each key/path/value triplet).
func keysJSONMSet(cmd *shared.Command) []string {
	keys := make([]string, 0, len(cmd.Args)/3)
	for i := 0; i+2 < len(cmd.Args); i += 3 {
		keys = append(keys, string(cmd.Args[i]))
	}
	return keys
}

func init() {
	shared.RegisterModuleCommands(
		// Read-only commands (no Keys/Flags → read-only context fast path).
		shared.ModuleEntry{Cmd: shared.CmdJSONGet, Entry: &shared.CommandEntry{Handler: handleJSONGet}},
		shared.ModuleEntry{Cmd: shared.CmdJSONMGet, Entry: &shared.CommandEntry{Handler: handleJSONMGet, Keys: shared.KeysAllButLast}},
		shared.ModuleEntry{Cmd: shared.CmdJSONType, Entry: &shared.CommandEntry{Handler: handleJSONType}},
		shared.ModuleEntry{Cmd: shared.CmdJSONArrLen, Entry: &shared.CommandEntry{Handler: handleJSONArrLen}},
		shared.ModuleEntry{Cmd: shared.CmdJSONArrIndex, Entry: &shared.CommandEntry{Handler: handleJSONArrIndex}},
		shared.ModuleEntry{Cmd: shared.CmdJSONStrLen, Entry: &shared.CommandEntry{Handler: handleJSONStrLen}},
		shared.ModuleEntry{Cmd: shared.CmdJSONObjKeys, Entry: &shared.CommandEntry{Handler: handleJSONObjKeys}},
		shared.ModuleEntry{Cmd: shared.CmdJSONObjLen, Entry: &shared.CommandEntry{Handler: handleJSONObjLen}},
		shared.ModuleEntry{Cmd: shared.CmdJSONResp, Entry: &shared.CommandEntry{Handler: handleJSONResp}},

		// Write commands.
		shared.ModuleEntry{Cmd: shared.CmdJSONSet, Entry: &shared.CommandEntry{Handler: handleJSONSet, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdJSONMSet, Entry: &shared.CommandEntry{Handler: handleJSONMSet, Keys: keysJSONMSet, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdJSONMerge, Entry: &shared.CommandEntry{Handler: handleJSONMerge, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdJSONDel, Entry: &shared.CommandEntry{Handler: handleJSONDel, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdJSONForget, Entry: &shared.CommandEntry{Handler: handleJSONDel, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdJSONClear, Entry: &shared.CommandEntry{Handler: handleJSONClear, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdJSONNumIncrBy, Entry: &shared.CommandEntry{Handler: handleJSONNumIncrBy, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdJSONNumMultBy, Entry: &shared.CommandEntry{Handler: handleJSONNumMultBy, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdJSONArrAppend, Entry: &shared.CommandEntry{Handler: handleJSONArrAppend, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdJSONArrInsert, Entry: &shared.CommandEntry{Handler: handleJSONArrInsert, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdJSONArrPop, Entry: &shared.CommandEntry{Handler: handleJSONArrPop, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdJSONArrTrim, Entry: &shared.CommandEntry{Handler: handleJSONArrTrim, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdJSONStrAppend, Entry: &shared.CommandEntry{Handler: handleJSONStrAppend, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdJSONToggle, Entry: &shared.CommandEntry{Handler: handleJSONToggle, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
	)
}
