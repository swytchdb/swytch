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

package zset

import (
	"github.com/swytchdb/swytch/redis/shared"
)

func init() {
	shared.RegisterModuleCommands(
		// Sorted set read commands
		shared.ModuleEntry{Cmd: shared.CmdZScore, Entry: &shared.CommandEntry{Handler: handleZScore}},
		shared.ModuleEntry{Cmd: shared.CmdZMScore, Entry: &shared.CommandEntry{Handler: handleZMScore}},
		shared.ModuleEntry{Cmd: shared.CmdZCard, Entry: &shared.CommandEntry{Handler: handleZCard}},
		shared.ModuleEntry{Cmd: shared.CmdZRank, Entry: &shared.CommandEntry{Handler: handleZRank}},
		shared.ModuleEntry{Cmd: shared.CmdZRevRank, Entry: &shared.CommandEntry{Handler: handleZRevRank}},
		shared.ModuleEntry{Cmd: shared.CmdZRange, Entry: &shared.CommandEntry{Handler: handleZRange}},
		shared.ModuleEntry{Cmd: shared.CmdZRevRange, Entry: &shared.CommandEntry{Handler: handleZRevRange}},
		shared.ModuleEntry{Cmd: shared.CmdZRangeByScore, Entry: &shared.CommandEntry{Handler: handleZRangeByScore}},
		shared.ModuleEntry{Cmd: shared.CmdZRevRangeByScore, Entry: &shared.CommandEntry{Handler: handleZRevRangeByScore}},
		shared.ModuleEntry{Cmd: shared.CmdZCount, Entry: &shared.CommandEntry{Handler: handleZCount}},
		shared.ModuleEntry{Cmd: shared.CmdZLexCount, Entry: &shared.CommandEntry{Handler: handleZLexCount}},
		shared.ModuleEntry{Cmd: shared.CmdZRangeByLex, Entry: &shared.CommandEntry{Handler: handleZRangeByLex}},
		shared.ModuleEntry{Cmd: shared.CmdZRevRangeByLex, Entry: &shared.CommandEntry{Handler: handleZRevRangeByLex}},
		shared.ModuleEntry{Cmd: shared.CmdZRandMember, Entry: &shared.CommandEntry{Handler: handleZRandMember}},
		shared.ModuleEntry{Cmd: shared.CmdZUnion, Entry: &shared.CommandEntry{Handler: handleZUnion}},
		shared.ModuleEntry{Cmd: shared.CmdZInter, Entry: &shared.CommandEntry{Handler: handleZInter}},
		shared.ModuleEntry{Cmd: shared.CmdZInterCard, Entry: &shared.CommandEntry{Handler: handleZInterCard}},
		shared.ModuleEntry{Cmd: shared.CmdZDiff, Entry: &shared.CommandEntry{Handler: handleZDiff}},

		// Sorted set write commands
		shared.ModuleEntry{Cmd: shared.CmdZAdd, Entry: &shared.CommandEntry{Handler: handleZAdd, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdZRem, Entry: &shared.CommandEntry{Handler: handleZRem, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdZIncrBy, Entry: &shared.CommandEntry{Handler: handleZIncrBy, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdZPopMin, Entry: &shared.CommandEntry{Handler: handleZPopMin, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdZPopMax, Entry: &shared.CommandEntry{Handler: handleZPopMax, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdZMPop, Entry: &shared.CommandEntry{Handler: handleZMPop, Keys: func(cmd *shared.Command) []string { return shared.KeysNumkeysAtOffset(cmd, 0) }, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdZRemRangeByScore, Entry: &shared.CommandEntry{Handler: handleZRemRangeByScore, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdZRemRangeByRank, Entry: &shared.CommandEntry{Handler: handleZRemRangeByRank, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdZRemRangeByLex, Entry: &shared.CommandEntry{Handler: handleZRemRangeByLex, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdZUnionStore, Entry: &shared.CommandEntry{Handler: handleZUnionStore, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdZInterStore, Entry: &shared.CommandEntry{Handler: handleZInterStore, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdZDiffStore, Entry: &shared.CommandEntry{Handler: handleZDiffStore, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdZRangeStore, Entry: &shared.CommandEntry{Handler: handleZRangeStore, Keys: shared.KeysFirstTwo, Flags: shared.FlagWrite}},

		// Blocking sorted set commands
		shared.ModuleEntry{Cmd: shared.CmdBZPopMin, Entry: &shared.CommandEntry{
			ConnHandler: handleBZPopMin,
			TxnPrepare: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, _ *shared.Connection) (bool, []string, shared.CommandRunner) {
				return handleBZPopMinNonBlocking(cmd, w, db)
			},
			Keys:  shared.KeysAllButLast,
			Flags: shared.FlagWrite,
		}},
		shared.ModuleEntry{Cmd: shared.CmdBZPopMax, Entry: &shared.CommandEntry{
			ConnHandler: handleBZPopMax,
			TxnPrepare: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, _ *shared.Connection) (bool, []string, shared.CommandRunner) {
				return handleBZPopMaxNonBlocking(cmd, w, db)
			},
			Keys:  shared.KeysAllButLast,
			Flags: shared.FlagWrite,
		}},
		shared.ModuleEntry{Cmd: shared.CmdBZMPop, Entry: &shared.CommandEntry{
			ConnHandler: handleBZMPop,
			TxnPrepare: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, _ *shared.Connection) (bool, []string, shared.CommandRunner) {
				return handleBZMPopNonBlocking(cmd, w, db)
			},
			Keys:  func(cmd *shared.Command) []string { return shared.KeysNumkeysAtOffset(cmd, 1) },
			Flags: shared.FlagWrite,
		}},
	)
}
