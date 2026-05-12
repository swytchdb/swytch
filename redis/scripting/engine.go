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
	"sync"
	"sync/atomic"
	"time"

	"github.com/swytchdb/swytch/redis/shared"
)

const (
	// DefaultScriptTimeout is the default timeout for script execution.
	DefaultScriptTimeout = 5 * time.Second
)

// Engine holds all scripting state and implements shared.ScriptingEngine.
type Engine struct {
	scriptRegistry   *ScriptRegistry
	luaVMPool        *LuaVMPool
	scriptMutex      sync.Mutex
	runningScript    atomic.Pointer[ScriptContext]
	scriptTimeout    atomic.Int64 // stores nanoseconds
	functionRegistry *FunctionRegistry
	executor         shared.CommandExecutor
}

// NewEngine creates a new scripting engine.
func NewEngine(executor shared.CommandExecutor) *Engine {
	e := &Engine{
		scriptRegistry:   NewScriptRegistry(),
		luaVMPool:        NewLuaVMPool(DefaultVMPoolSize),
		functionRegistry: NewFunctionRegistry(),
		executor:         executor,
	}
	e.scriptTimeout.Store(int64(DefaultScriptTimeout))
	return e
}

// SetExecutor sets the command executor for redis.call/pcall.
func (e *Engine) SetExecutor(executor shared.CommandExecutor) {
	e.executor = executor
}

// SetScriptTimeout sets the script timeout duration.
func (e *Engine) SetScriptTimeout(d time.Duration) {
	e.scriptTimeout.Store(int64(d))
}

// getScriptTimeout returns the current script timeout duration.
func (e *Engine) getScriptTimeout() time.Duration {
	return time.Duration(e.scriptTimeout.Load())
}

// IsScriptTimedOut returns true if there is a running script that has timed out.
func (e *Engine) IsScriptTimedOut() bool {
	sc := e.runningScript.Load()
	return sc != nil && sc.IsTimedOut()
}

// Close releases resources used by the engine.
func (e *Engine) Close() {
	if e.luaVMPool != nil {
		e.luaVMPool.Close()
	}
}
