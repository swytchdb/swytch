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

package bitmap

import (
	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/swytch/redis/shared"
)

func init() {
	shared.RegisterBytesReconstructor(pb.ValueType_TYPE_BITMAP, func(snap *pb.ReducedEffect) []byte {
		return reconstructBitmapBytes(snap)
	})

	shared.RegisterModuleCommands(
		// Bitmap read commands
		shared.ModuleEntry{Cmd: shared.CmdGetBit, Entry: &shared.CommandEntry{Handler: handleGetBit}},
		shared.ModuleEntry{Cmd: shared.CmdBitCount, Entry: &shared.CommandEntry{Handler: handleBitCount}},
		shared.ModuleEntry{Cmd: shared.CmdBitPos, Entry: &shared.CommandEntry{Handler: handleBitPos}},
		shared.ModuleEntry{Cmd: shared.CmdBitFieldRO, Entry: &shared.CommandEntry{Handler: handleBitFieldRO}},

		// Bitmap write commands
		shared.ModuleEntry{Cmd: shared.CmdSetBit, Entry: &shared.CommandEntry{Handler: handleSetBit, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdBitOp, Entry: &shared.CommandEntry{Handler: handleBitOp, Keys: shared.KeysAll, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdBitField, Entry: &shared.CommandEntry{Handler: handleBitField, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
	)
}
