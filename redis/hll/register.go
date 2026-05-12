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

package hll

import (
	"github.com/swytchdb/swytch/redis/shared"
)

func init() {
	shared.RegisterModuleCommands(
		shared.ModuleEntry{Cmd: shared.CmdPFAdd, Entry: &shared.CommandEntry{Handler: handlePFAdd, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdPFCount, Entry: &shared.CommandEntry{Handler: handlePFCount}},
		shared.ModuleEntry{Cmd: shared.CmdPFMerge, Entry: &shared.CommandEntry{Handler: handlePFMerge, Keys: shared.KeysAll, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdPFSelfTest, Entry: &shared.CommandEntry{Handler: handlePFSelfTest}},
		shared.ModuleEntry{Cmd: shared.CmdPFDebug, Entry: &shared.CommandEntry{Handler: handlePFDebug}},
	)
}
