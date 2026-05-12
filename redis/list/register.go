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

package list

import (
	"github.com/swytchdb/swytch/redis/shared"
)

func init() {
	shared.RegisterModuleCommands(
		// Non-blocking list commands
		shared.ModuleEntry{Cmd: shared.CmdLPush, Entry: &shared.CommandEntry{Handler: handleLPush, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdLPushX, Entry: &shared.CommandEntry{Handler: handleLPushX, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdRPush, Entry: &shared.CommandEntry{Handler: handleRPush, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdRPushX, Entry: &shared.CommandEntry{Handler: handleRPushX, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdLPop, Entry: &shared.CommandEntry{Handler: handleLPop, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdRPop, Entry: &shared.CommandEntry{Handler: handleRPop, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdLMPop, Entry: &shared.CommandEntry{Handler: handleLMPop, Keys: func(cmd *shared.Command) []string { return shared.KeysNumkeysAtOffset(cmd, 0) }, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdLLen, Entry: &shared.CommandEntry{Handler: handleLLen}},
		shared.ModuleEntry{Cmd: shared.CmdLRange, Entry: &shared.CommandEntry{Handler: handleLRange}},
		shared.ModuleEntry{Cmd: shared.CmdLIndex, Entry: &shared.CommandEntry{Handler: handleLIndex}},
		shared.ModuleEntry{Cmd: shared.CmdLSet, Entry: &shared.CommandEntry{Handler: handleLSet, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdLRem, Entry: &shared.CommandEntry{Handler: handleLRem, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdLTrim, Entry: &shared.CommandEntry{Handler: handleLTrim, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdLInsert, Entry: &shared.CommandEntry{Handler: handleLInsert, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdLPos, Entry: &shared.CommandEntry{Handler: handleLPos}},
		shared.ModuleEntry{Cmd: shared.CmdLMove, Entry: &shared.CommandEntry{Handler: handleLMove, Keys: shared.KeysFirstTwo, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdRPopLPush, Entry: &shared.CommandEntry{Handler: handleRPopLPush, Keys: shared.KeysFirstTwo, Flags: shared.FlagWrite}},

		// Blocking list commands
		shared.ModuleEntry{Cmd: shared.CmdBLPop, Entry: &shared.CommandEntry{
			ConnHandler: handleBLPop,
			TxnPrepare: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, _ *shared.Connection) (bool, []string, shared.CommandRunner) {
				return handleLPopNonBlocking(cmd, w, db, true)
			},
			Keys:  shared.KeysAllButLast,
			Flags: shared.FlagWrite,
		}},
		shared.ModuleEntry{Cmd: shared.CmdBRPop, Entry: &shared.CommandEntry{
			ConnHandler: handleBRPop,
			TxnPrepare: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, _ *shared.Connection) (bool, []string, shared.CommandRunner) {
				return handleLPopNonBlocking(cmd, w, db, false)
			},
			Keys:  shared.KeysAllButLast,
			Flags: shared.FlagWrite,
		}},
		shared.ModuleEntry{Cmd: shared.CmdBLMPop, Entry: &shared.CommandEntry{
			ConnHandler: handleBLMPop,
			TxnPrepare: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, _ *shared.Connection) (bool, []string, shared.CommandRunner) {
				return handleBLMPopNonBlocking(cmd, w, db)
			},
			Keys:  func(cmd *shared.Command) []string { return shared.KeysNumkeysAtOffset(cmd, 1) },
			Flags: shared.FlagWrite,
		}},
		shared.ModuleEntry{Cmd: shared.CmdBLMove, Entry: &shared.CommandEntry{
			ConnHandler: handleBLMove,
			TxnPrepare: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, _ *shared.Connection) (bool, []string, shared.CommandRunner) {
				return handleBLMoveNonBlocking(cmd, w, db)
			},
			Keys:  shared.KeysFirstTwo,
			Flags: shared.FlagWrite,
		}},
		shared.ModuleEntry{Cmd: shared.CmdBRPopLPush, Entry: &shared.CommandEntry{
			ConnHandler: handleBRPopLPush,
			TxnPrepare: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, _ *shared.Connection) (bool, []string, shared.CommandRunner) {
				return handleBRPopLPushNonBlocking(cmd, w, db)
			},
			Keys:  shared.KeysFirstTwo,
			Flags: shared.FlagWrite,
		}},
	)
}
