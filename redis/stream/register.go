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

package stream

import (
	"github.com/swytchdb/swytch/redis/shared"
)

func init() {
	shared.RegisterModuleCommands(
		// Non-blocking stream commands
		shared.ModuleEntry{Cmd: shared.CmdXAdd, Entry: &shared.CommandEntry{Handler: handleXAdd, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdXLen, Entry: &shared.CommandEntry{Handler: handleXLen}},
		shared.ModuleEntry{Cmd: shared.CmdXRange, Entry: &shared.CommandEntry{Handler: handleXRange}},
		shared.ModuleEntry{Cmd: shared.CmdXRevRange, Entry: &shared.CommandEntry{Handler: handleXRevRange}},
		shared.ModuleEntry{Cmd: shared.CmdXGroup, Entry: &shared.CommandEntry{Handler: handleXGroup, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdXAck, Entry: &shared.CommandEntry{Handler: handleXAck, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdXPending, Entry: &shared.CommandEntry{Handler: handleXPending}},
		shared.ModuleEntry{Cmd: shared.CmdXTrim, Entry: &shared.CommandEntry{Handler: handleXTrim, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdXDel, Entry: &shared.CommandEntry{Handler: handleXDel, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdXInfo, Entry: &shared.CommandEntry{Handler: handleXInfo}},
		shared.ModuleEntry{Cmd: shared.CmdXSetId, Entry: &shared.CommandEntry{Handler: handleXSetId, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdXDelEx, Entry: &shared.CommandEntry{Handler: handleXDelEx, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdXAckDel, Entry: &shared.CommandEntry{Handler: handleXAckDel, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdXClaim, Entry: &shared.CommandEntry{Handler: handleXClaim, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdXAutoClaim, Entry: &shared.CommandEntry{Handler: handleXAutoClaim, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdXIdmpRecord, Entry: &shared.CommandEntry{Handler: handleXIdmpRecord, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},
		shared.ModuleEntry{Cmd: shared.CmdXCfgSet, Entry: &shared.CommandEntry{Handler: handleXCfgSet, Keys: shared.KeysFirst, Flags: shared.FlagWrite}},

		// Blocking stream commands
		shared.ModuleEntry{Cmd: shared.CmdXRead, Entry: &shared.CommandEntry{
			ConnHandler: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
				handleXRead(cmd, w, db, conn)
			},
			TxnPrepare: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, _ *shared.Connection) (bool, []string, shared.CommandRunner) {
				return handleXReadNonBlocking(cmd, w, db)
			},
			Keys: shared.KeysAfterStreams,
		}},
		shared.ModuleEntry{Cmd: shared.CmdXReadGroup, Entry: &shared.CommandEntry{
			ConnHandler: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
				handleXReadGroup(cmd, w, db, conn)
			},
			TxnPrepare: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, _ *shared.Connection) (bool, []string, shared.CommandRunner) {
				return handleXReadGroupNonBlocking(cmd, w, db)
			},
			Keys:  shared.KeysAfterStreams,
			Flags: shared.FlagWrite,
		}},
	)
}
