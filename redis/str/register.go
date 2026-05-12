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

package str

import (
	"github.com/swytchdb/swytch/redis/shared"
)

func init() {
	shared.RegisterModuleCommands(
		// String read commands
		shared.ModuleEntry{Cmd: shared.CmdGet, Entry: &shared.CommandEntry{Handler: handleGet, Flags: shared.FlagTrackGet}},
		shared.ModuleEntry{Cmd: shared.CmdMGet, Entry: &shared.CommandEntry{Handler: handleMGet}},
		shared.ModuleEntry{Cmd: shared.CmdStrLen, Entry: &shared.CommandEntry{Handler: handleStrLen}},
		shared.ModuleEntry{Cmd: shared.CmdGetRange, Entry: &shared.CommandEntry{Handler: handleGetRange}},
		shared.ModuleEntry{Cmd: shared.CmdSubstr, Entry: &shared.CommandEntry{Handler: handleGetRange}},
		shared.ModuleEntry{Cmd: shared.CmdLcs, Entry: &shared.CommandEntry{Handler: handleLcs}},
		shared.ModuleEntry{Cmd: shared.CmdDigest, Entry: &shared.CommandEntry{Handler: handleDigest}},

		// String write commands
		shared.ModuleEntry{Cmd: shared.CmdSet, Entry: &shared.CommandEntry{Handler: handleSet, Keys: shared.KeysFirst, Flags: shared.FlagWrite | shared.FlagTrackSet}},
		shared.ModuleEntry{Cmd: shared.CmdSetNX, Entry: &shared.CommandEntry{Handler: handleSetNX, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdSetEX, Entry: &shared.CommandEntry{Handler: handleSetEX, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdPSetEX, Entry: &shared.CommandEntry{Handler: handlePSetEX, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdGetSet, Entry: &shared.CommandEntry{Handler: handleGetSet, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdGetDel, Entry: &shared.CommandEntry{Handler: handleGetDel, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdGetEx, Entry: &shared.CommandEntry{Handler: handleGetEx, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdMSet, Entry: &shared.CommandEntry{Handler: handleMSet, Keys: shared.KeysMSetStyle, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdMSetNX, Entry: &shared.CommandEntry{Handler: handleMSetNX, Keys: shared.KeysMSetStyle, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdMSetEX, Entry: &shared.CommandEntry{Handler: handleMSetEX, Keys: shared.KeysMSetStyle, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdSetRange, Entry: &shared.CommandEntry{Handler: handleSetRange, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdAppend, Entry: &shared.CommandEntry{Handler: handleAppend, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdIncr, Entry: &shared.CommandEntry{Handler: handleIncr, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdDecr, Entry: &shared.CommandEntry{Handler: handleDecr, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdIncrBy, Entry: &shared.CommandEntry{Handler: handleIncrBy, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdDecrBy, Entry: &shared.CommandEntry{Handler: handleDecrBy, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdIncrByFloat, Entry: &shared.CommandEntry{Handler: handleIncrByFloat, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},

		// Key commands
		shared.ModuleEntry{Cmd: shared.CmdDel, Entry: &shared.CommandEntry{Handler: handleDel, Keys: shared.KeysAll, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdDelEx, Entry: &shared.CommandEntry{Handler: handleDelEx, Keys: shared.KeysAll, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdExists, Entry: &shared.CommandEntry{Handler: handleExists}},
		shared.ModuleEntry{Cmd: shared.CmdRename, Entry: &shared.CommandEntry{Handler: handleRename, Keys: shared.KeysFirstTwo, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdRenameNX, Entry: &shared.CommandEntry{Handler: handleRenameNX, Keys: shared.KeysFirstTwo, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdCopy, Entry: &shared.CommandEntry{Handler: handleCopy, Keys: shared.KeysFirstTwo, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdMove, Entry: &shared.CommandEntry{Handler: handleMove, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdType, Entry: &shared.CommandEntry{Handler: handleType}},

		// Expiration commands
		shared.ModuleEntry{Cmd: shared.CmdExpire, Entry: &shared.CommandEntry{Handler: handleExpire, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdExpireAt, Entry: &shared.CommandEntry{Handler: handleExpireAt, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdPExpire, Entry: &shared.CommandEntry{Handler: handlePExpire, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdPExpireAt, Entry: &shared.CommandEntry{Handler: handlePExpireAt, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdTTL, Entry: &shared.CommandEntry{Handler: handleTTL}},
		shared.ModuleEntry{Cmd: shared.CmdPTTL, Entry: &shared.CommandEntry{Handler: handlePTTL}},
		shared.ModuleEntry{Cmd: shared.CmdExpireTime, Entry: &shared.CommandEntry{Handler: handleExpireTime}},
		shared.ModuleEntry{Cmd: shared.CmdPExpireTime, Entry: &shared.CommandEntry{Handler: handlePExpireTime}},
		shared.ModuleEntry{Cmd: shared.CmdPersist, Entry: &shared.CommandEntry{Handler: handlePersist, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
	)
}
