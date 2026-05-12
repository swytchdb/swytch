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
	"github.com/swytchdb/swytch/redis/shared"
)

func init() {
	shared.RegisterModuleCommands(
		// Basic CRUD
		shared.ModuleEntry{Cmd: shared.CmdHGet, Entry: &shared.CommandEntry{Handler: handleHGet}},
		shared.ModuleEntry{Cmd: shared.CmdHSet, Entry: &shared.CommandEntry{Handler: handleHSet, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdHSetNX, Entry: &shared.CommandEntry{Handler: handleHSetNX, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdHDel, Entry: &shared.CommandEntry{Handler: handleHDel, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdHExists, Entry: &shared.CommandEntry{Handler: handleHExists}},
		shared.ModuleEntry{Cmd: shared.CmdHGetAll, Entry: &shared.CommandEntry{Handler: handleHGetAll}},
		shared.ModuleEntry{Cmd: shared.CmdHKeys, Entry: &shared.CommandEntry{Handler: handleHKeys}},
		shared.ModuleEntry{Cmd: shared.CmdHVals, Entry: &shared.CommandEntry{Handler: handleHVals}},
		shared.ModuleEntry{Cmd: shared.CmdHLen, Entry: &shared.CommandEntry{Handler: handleHLen}},
		shared.ModuleEntry{Cmd: shared.CmdHMGet, Entry: &shared.CommandEntry{Handler: handleHMGet}},
		shared.ModuleEntry{Cmd: shared.CmdHMSet, Entry: &shared.CommandEntry{Handler: handleHMSet, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdHIncrBy, Entry: &shared.CommandEntry{Handler: handleHIncrBy, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdHIncrByFloat, Entry: &shared.CommandEntry{Handler: handleHIncrByFloat, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdHStrLen, Entry: &shared.CommandEntry{Handler: handleHStrLen}},

		// Scan and random
		shared.ModuleEntry{Cmd: shared.CmdHRandField, Entry: &shared.CommandEntry{Handler: handleHRandField}},
		shared.ModuleEntry{Cmd: shared.CmdHScan, Entry: &shared.CommandEntry{Handler: handleHScan}},

		// Get and delete
		shared.ModuleEntry{Cmd: shared.CmdHGetDel, Entry: &shared.CommandEntry{Handler: handleHGetDel, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},

		// Field-level TTL
		shared.ModuleEntry{Cmd: shared.CmdHExpire, Entry: &shared.CommandEntry{Handler: handleHExpire, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdHPExpire, Entry: &shared.CommandEntry{Handler: handleHPExpire, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdHExpireAt, Entry: &shared.CommandEntry{Handler: handleHExpireAt, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdHPExpireAt, Entry: &shared.CommandEntry{Handler: handleHPExpireAt, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdHTTL, Entry: &shared.CommandEntry{Handler: handleHTTL}},
		shared.ModuleEntry{Cmd: shared.CmdHPTTL, Entry: &shared.CommandEntry{Handler: handleHPTTL}},
		shared.ModuleEntry{Cmd: shared.CmdHExpireTime, Entry: &shared.CommandEntry{Handler: handleHExpireTime}},
		shared.ModuleEntry{Cmd: shared.CmdHPExpireTime, Entry: &shared.CommandEntry{Handler: handleHPExpireTime}},
		shared.ModuleEntry{Cmd: shared.CmdHPersist, Entry: &shared.CommandEntry{Handler: handleHPersist, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdHSetEx, Entry: &shared.CommandEntry{Handler: handleHSetEx, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdHGetEx, Entry: &shared.CommandEntry{Handler: handleHGetEx}},
	)
}
