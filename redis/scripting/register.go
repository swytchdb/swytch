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

package scripting

import (
	"github.com/swytchdb/swytch/redis/shared"
)

func getEngine(w *shared.Writer) shared.ScriptingEngine {
	e := shared.GetScriptingEngine()
	if e == nil {
		w.WriteError("ERR scripting engine not initialized")
		return nil
	}
	return e
}

func init() {
	scriptKeys := func(cmd *shared.Command) []string { return shared.KeysNumkeysAtOffset(cmd, 1) }

	shared.RegisterModuleCommands(
		// FUNCTION and SCRIPT (no connection context needed)
		shared.ModuleEntry{Cmd: shared.CmdFunction, Entry: &shared.CommandEntry{
			Handler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database) (bool, []string, shared.CommandRunner) {
				e := getEngine(w)
				if e == nil {
					return true, nil, func() {}
				}
				return true, nil, func() { e.HandleFunction(cmd, w) }
			},
		}},
		shared.ModuleEntry{Cmd: shared.CmdScript, Entry: &shared.CommandEntry{
			Handler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database) (bool, []string, shared.CommandRunner) {
				e := getEngine(w)
				if e == nil {
					return true, nil, func() {}
				}
				return true, nil, func() { e.HandleScript(cmd, w) }
			},
		}},

		// EVAL
		shared.ModuleEntry{Cmd: shared.CmdEval, Entry: &shared.CommandEntry{
			ConnHandler: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
				if e := getEngine(w); e != nil {
					e.HandleEval(cmd, w, conn, db, false)
				}
			},
			TxnPrepare: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) (bool, []string, shared.CommandRunner) {
				return true, nil, func() {
					if e := getEngine(w); e != nil {
						e.HandleEval(cmd, w, conn, db, false)
					}
				}
			},
			Keys:  scriptKeys,
			Flags: shared.FlagWrite,
		}},

		// EVALSHA
		shared.ModuleEntry{Cmd: shared.CmdEvalSha, Entry: &shared.CommandEntry{
			ConnHandler: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
				if e := getEngine(w); e != nil {
					e.HandleEval(cmd, w, conn, db, false)
				}
			},
			TxnPrepare: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) (bool, []string, shared.CommandRunner) {
				return true, nil, func() {
					if e := getEngine(w); e != nil {
						e.HandleEval(cmd, w, conn, db, false)
					}
				}
			},
			Keys:  scriptKeys,
			Flags: shared.FlagWrite,
		}},

		// EVAL_RO
		shared.ModuleEntry{Cmd: shared.CmdEvalRO, Entry: &shared.CommandEntry{
			ConnHandler: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
				if e := getEngine(w); e != nil {
					e.HandleEval(cmd, w, conn, db, true)
				}
			},
			TxnPrepare: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) (bool, []string, shared.CommandRunner) {
				return true, nil, func() {
					if e := getEngine(w); e != nil {
						e.HandleEval(cmd, w, conn, db, true)
					}
				}
			},
			Keys: scriptKeys,
		}},

		// EVALSHA_RO
		shared.ModuleEntry{Cmd: shared.CmdEvalShaRO, Entry: &shared.CommandEntry{
			ConnHandler: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
				if e := getEngine(w); e != nil {
					e.HandleEval(cmd, w, conn, db, true)
				}
			},
			TxnPrepare: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) (bool, []string, shared.CommandRunner) {
				return true, nil, func() {
					if e := getEngine(w); e != nil {
						e.HandleEval(cmd, w, conn, db, true)
					}
				}
			},
			Keys: scriptKeys,
		}},

		// FCALL
		shared.ModuleEntry{Cmd: shared.CmdFcall, Entry: &shared.CommandEntry{
			ConnHandler: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
				if e := getEngine(w); e != nil {
					e.HandleFcall(cmd, w, conn, db, false)
				}
			},
			TxnPrepare: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) (bool, []string, shared.CommandRunner) {
				return true, nil, func() {
					if e := getEngine(w); e != nil {
						e.HandleFcall(cmd, w, conn, db, false)
					}
				}
			},
			Keys:  scriptKeys,
			Flags: shared.FlagWrite,
		}},

		// FCALL_RO
		shared.ModuleEntry{Cmd: shared.CmdFcallRO, Entry: &shared.CommandEntry{
			ConnHandler: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
				if e := getEngine(w); e != nil {
					e.HandleFcall(cmd, w, conn, db, true)
				}
			},
			TxnPrepare: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) (bool, []string, shared.CommandRunner) {
				return true, nil, func() {
					if e := getEngine(w); e != nil {
						e.HandleFcall(cmd, w, conn, db, true)
					}
				}
			},
			Keys: scriptKeys,
		}},
	)
}
