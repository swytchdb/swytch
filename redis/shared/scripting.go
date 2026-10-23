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

package shared

import "sync/atomic"

// CommandExecutor dispatches a command for execution (used by scripting for redis.call/pcall).
type CommandExecutor func(cmd *Command, w *Writer, conn *Connection)

// ScriptingEngine is the interface for the scripting subsystem.
// This allows the scripting module to register commands via init() without circular imports.
type ScriptingEngine interface {
	HandleEval(cmd *Command, w *Writer, conn *Connection, db *Database, readOnly bool)
	HandleScript(cmd *Command, w *Writer)
	HandleFunction(cmd *Command, w *Writer)
	HandleFcall(cmd *Command, w *Writer, conn *Connection, db *Database, readOnly bool)
	IsScriptTimedOut() bool
	Close()
}

type scriptingEngineHolder struct {
	engine ScriptingEngine
}

var scriptingEngine atomic.Pointer[scriptingEngineHolder]

// SetScriptingEngine sets the scripting engine instance.
func SetScriptingEngine(e ScriptingEngine) {
	scriptingEngine.Store(&scriptingEngineHolder{engine: e})
}

// GetScriptingEngine returns the current scripting engine instance (may be nil).
func GetScriptingEngine() ScriptingEngine {
	h := scriptingEngine.Load()
	if h == nil || h.engine == nil {
		return nil
	}

	return h.engine
}

// ClearScriptingEngine fully clears the global scripting engine pointer.
func ClearScriptingEngine() {
	scriptingEngine.Store(nil)
}
