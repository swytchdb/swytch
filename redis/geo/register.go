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

package geo

import (
	"github.com/swytchdb/swytch/redis/shared"
)

func init() {
	shared.RegisterModuleCommands(
		// Geo write commands
		shared.ModuleEntry{Cmd: shared.CmdGeoAdd, Entry: &shared.CommandEntry{Handler: handleGeoAdd, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdGeoSearchStore, Entry: &shared.CommandEntry{Handler: handleGeoSearchStore, Keys: shared.KeysFirstTwo, Flags: shared.FlagWrite}},

		// Geo read commands
		shared.ModuleEntry{Cmd: shared.CmdGeoDist, Entry: &shared.CommandEntry{Handler: handleGeoDist}},
		shared.ModuleEntry{Cmd: shared.CmdGeoHash, Entry: &shared.CommandEntry{Handler: handleGeoHash}},
		shared.ModuleEntry{Cmd: shared.CmdGeoPos, Entry: &shared.CommandEntry{Handler: handleGeoPos}},
		shared.ModuleEntry{Cmd: shared.CmdGeoRadius, Entry: &shared.CommandEntry{Handler: handleGeoRadius, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdGeoRadiusRO, Entry: &shared.CommandEntry{Handler: handleGeoRadius}},
		shared.ModuleEntry{Cmd: shared.CmdGeoRadiusByMember, Entry: &shared.CommandEntry{Handler: handleGeoRadiusByMember, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdGeoRadiusByMemberRO, Entry: &shared.CommandEntry{Handler: handleGeoRadiusByMember}},
		shared.ModuleEntry{Cmd: shared.CmdGeoSearch, Entry: &shared.CommandEntry{Handler: handleGeoSearch}},
	)
}
