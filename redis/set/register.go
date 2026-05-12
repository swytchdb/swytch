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

package set

import (
	"github.com/swytchdb/swytch/redis/shared"
)

func init() {
	shared.RegisterModuleCommands(
		// Set read commands
		shared.ModuleEntry{Cmd: shared.CmdSCard, Entry: &shared.CommandEntry{Handler: handleSCard}},
		shared.ModuleEntry{Cmd: shared.CmdSIsMember, Entry: &shared.CommandEntry{Handler: handleSIsMember}},
		shared.ModuleEntry{Cmd: shared.CmdSMIsMember, Entry: &shared.CommandEntry{Handler: handleSMIsMember}},
		shared.ModuleEntry{Cmd: shared.CmdSMembers, Entry: &shared.CommandEntry{Handler: handleSMembers}},
		shared.ModuleEntry{Cmd: shared.CmdSRandMember, Entry: &shared.CommandEntry{Handler: handleSRandMember}},
		shared.ModuleEntry{Cmd: shared.CmdSInter, Entry: &shared.CommandEntry{Handler: handleSInter}},
		shared.ModuleEntry{Cmd: shared.CmdSInterCard, Entry: &shared.CommandEntry{Handler: handleSInterCard}},
		shared.ModuleEntry{Cmd: shared.CmdSUnion, Entry: &shared.CommandEntry{Handler: handleSUnion}},
		shared.ModuleEntry{Cmd: shared.CmdSDiff, Entry: &shared.CommandEntry{Handler: handleSDiff}},

		// Set write commands
		shared.ModuleEntry{Cmd: shared.CmdSAdd, Entry: &shared.CommandEntry{Handler: handleSAdd, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdSRem, Entry: &shared.CommandEntry{Handler: handleSRem, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdSPop, Entry: &shared.CommandEntry{Handler: handleSPop, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdSMove, Entry: &shared.CommandEntry{Handler: handleSMove, Keys: shared.KeysFirstTwo, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdSInterStore, Entry: &shared.CommandEntry{Handler: handleSInterStore, Keys: shared.KeysAll, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdSUnionStore, Entry: &shared.CommandEntry{Handler: handleSUnionStore, Keys: shared.KeysAll, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdSDiffStore, Entry: &shared.CommandEntry{Handler: handleSDiffStore, Keys: shared.KeysAll, Flags: shared.FlagWrite}},
	)
}
