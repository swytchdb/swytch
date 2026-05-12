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

package pubsub

import (
	"github.com/swytchdb/swytch/redis/shared"
)

func init() {
	shared.RegisterModuleCommands(
		shared.ModuleEntry{Cmd: shared.CmdSubscribe, Entry: &shared.CommandEntry{
			ConnHandler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, conn *shared.Connection) {
				handleSubscribe(cmd, w, conn)
			},
			TxnPrepare: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, conn *shared.Connection) (bool, []string, shared.CommandRunner) {
				return true, nil, func() { handleSubscribe(cmd, w, conn) }
			},
			Flags: shared.FlagPubSub,
		}},
		shared.ModuleEntry{Cmd: shared.CmdUnsubscribe, Entry: &shared.CommandEntry{
			ConnHandler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, conn *shared.Connection) {
				handleUnsubscribe(cmd, w, conn)
			},
			TxnPrepare: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, conn *shared.Connection) (bool, []string, shared.CommandRunner) {
				return true, nil, func() { handleUnsubscribe(cmd, w, conn) }
			},
			Flags: shared.FlagPubSub,
		}},
		shared.ModuleEntry{Cmd: shared.CmdPSubscribe, Entry: &shared.CommandEntry{
			ConnHandler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, conn *shared.Connection) {
				handlePSubscribe(cmd, w, conn)
			},
			TxnPrepare: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, conn *shared.Connection) (bool, []string, shared.CommandRunner) {
				return true, nil, func() { handlePSubscribe(cmd, w, conn) }
			},
			Flags: shared.FlagPubSub,
		}},
		shared.ModuleEntry{Cmd: shared.CmdPUnsubscribe, Entry: &shared.CommandEntry{
			ConnHandler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, conn *shared.Connection) {
				handlePUnsubscribe(cmd, w, conn)
			},
			TxnPrepare: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, conn *shared.Connection) (bool, []string, shared.CommandRunner) {
				return true, nil, func() { handlePUnsubscribe(cmd, w, conn) }
			},
			Flags: shared.FlagPubSub,
		}},
		shared.ModuleEntry{Cmd: shared.CmdPublish, Entry: &shared.CommandEntry{
			Handler: handlePublish,
		}},
		shared.ModuleEntry{Cmd: shared.CmdPubSub, Entry: &shared.CommandEntry{
			ConnHandler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, _ *shared.Connection) {
				handlePubSub(cmd, w)
			},
		}},
	)
}
