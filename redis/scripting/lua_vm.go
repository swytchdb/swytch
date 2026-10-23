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

	lua "github.com/yuin/gopher-lua"
)

const (
	// DefaultVMPoolSize is the default number of VMs to keep in the pool.
	DefaultVMPoolSize = 10
)

// LuaVMPool manages a pool of reusable Lua VMs for script execution.
type LuaVMPool struct {
	pool    chan *lua.LState
	maxSize int
	mu      sync.Mutex
}

// NewLuaVMPool creates a new VM pool with the specified maximum size.
func NewLuaVMPool(maxSize int) *LuaVMPool {
	if maxSize <= 0 {
		maxSize = DefaultVMPoolSize
	}

	return &LuaVMPool{
		pool:    make(chan *lua.LState, maxSize),
		maxSize: maxSize,
	}
}

// Get returns a Lua VM from the pool, or creates a new one if the pool is empty.
func (p *LuaVMPool) Get() *lua.LState {
	select {
	case L := <-p.pool:
		return L
	default:
		return p.newVM()
	}
}

// Put returns a Lua VM to the pool. If the pool is full, the VM is closed.
func (p *LuaVMPool) Put(L *lua.LState) {
	if L == nil {
		return
	}

	// Reset the VM stack
	L.SetTop(0)

	select {
	case p.pool <- L:
		// Returned to pool
	default:
		// Pool is full, close the VM
		L.Close()
	}
}

// Close closes all VMs in the pool.
func (p *LuaVMPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	close(p.pool)

	for L := range p.pool {
		L.Close()
	}
}

// newVM creates a new sandboxed Lua VM.
// Libraries loaded: base, string, table, math, os (limited).
// Dangerous libraries are excluded: io, debug, package, coroutine.
func (p *LuaVMPool) newVM() *lua.LState {
	opts := lua.Options{
		SkipOpenLibs: true,
	}
	L := lua.NewState(opts)

	// Load only safe libraries
	for _, pair := range []struct {
		name string
		fn   lua.LGFunction
	}{
		{lua.BaseLibName, lua.OpenBase},
		{lua.TabLibName, lua.OpenTable},
		{lua.StringLibName, lua.OpenString},
		{lua.MathLibName, lua.OpenMath},
		{lua.OsLibName, lua.OpenOs},
	} {
		L.Push(L.NewFunction(pair.fn))
		L.Push(lua.LString(pair.name))
		L.Call(1, 0)
	}

	// Remove dangerous functions from base library
	removeDangerousFunctions(L)

	// Remove dangerous functions from os library (keep only timing functions)
	removeUnsafeOsFunctions(L)

	// Register the struct library (for binary packing/unpacking, used by JuiceFS)
	RegisterStructLib(L)

	// Set up globals protection (like Redis)
	setupGlobalsProtection(L)

	return L
}

// removeDangerousFunctions removes potentially dangerous functions from the global scope.
func removeDangerousFunctions(L *lua.LState) {
	// Remove functions that could be used to escape the sandbox
	dangerousFuncs := []string{
		"dofile",
		"loadfile",
		"load",       // Can load bytecode
		"loadstring", // Deprecated alias for load
	}

	for _, name := range dangerousFuncs {
		L.SetGlobal(name, lua.LNil)
	}
}

// removeUnsafeOsFunctions removes dangerous os functions, keeping only os.clock().
// Redis only allows: os.clock()
// All other os functions are removed for sandbox security.
func removeUnsafeOsFunctions(L *lua.LState) {
	osTable := L.GetGlobal("os")
	if osTable == lua.LNil {
		return
	}

	osTbl, ok := osTable.(*lua.LTable)
	if !ok {
		return
	}

	// Save the clock function
	clockFn := osTbl.RawGetString("clock")

	// Clear the entire os table
	osTbl.ForEach(func(key, _ lua.LValue) {
		osTbl.RawSet(key, lua.LNil)
	})

	// Restore only clock
	osTbl.RawSetString("clock", clockFn)

	// Add a metatable to provide better error messages for removed functions
	mt := L.NewTable()
	mt.RawSetString("__index", L.NewFunction(func(L *lua.LState) int {
		key := L.CheckAny(2)

		keyStr := ""
		if s, ok := key.(lua.LString); ok {
			keyStr = string(s)
		}

		L.RaiseError("attempt to call field '%s' (a nil value)", keyStr)

		return 0
	}))
	L.SetMetatable(osTbl, mt)
}

// setupGlobalsProtection sets up a metatable on _G to prevent access to undeclared globals.
// This matches Redis behavior where scripts cannot read or write undefined global variables.
func setupGlobalsProtection(L *lua.LState) {
	// Store the current globals as "declared" in a shadow table
	declared := L.NewTable()
	globals := L.Get(lua.GlobalsIndex).(*lua.LTable)

	// Mark all current globals as declared
	globals.ForEach(func(key, _ lua.LValue) {
		declared.RawSet(key, lua.LTrue)
	})

	// Also mark common internal names that we'll add later
	internalNames := []string{
		"redis", "KEYS", "ARGV", "struct",
		// Internal function storage
	}
	for _, name := range internalNames {
		declared.RawSetString(name, lua.LTrue)
	}

	// Create metatable for _G
	mt := L.NewTable()

	// __index: raise error when reading undeclared globals
	mt.RawSetString("__index", L.NewFunction(func(L *lua.LState) int {
		key := L.CheckAny(2)

		keyStr := ""
		if s, ok := key.(lua.LString); ok {
			keyStr = string(s)
		}
		// Allow internal names starting with __
		if len(keyStr) >= 2 && keyStr[:2] == "__" {
			L.Push(lua.LNil)

			return 1
		}

		L.RaiseError("Script attempted to access nonexistent global variable '%s'", keyStr)

		return 0
	}))

	// __newindex: raise error when setting new globals (unless internal)
	mt.RawSetString("__newindex", L.NewFunction(func(L *lua.LState) int {
		key := L.CheckAny(2)
		value := L.CheckAny(3)

		keyStr := ""
		if s, ok := key.(lua.LString); ok {
			keyStr = string(s)
		}
		// Allow internal names starting with __ (for function storage)
		if len(keyStr) >= 2 && keyStr[:2] == "__" {
			globals.RawSet(key, value)

			return 0
		}
		// Allow setting declared globals (redis, KEYS, ARGV)
		if declared.RawGet(key) != lua.LNil {
			globals.RawSet(key, value)

			return 0
		}

		L.RaiseError("Attempt to modify a readonly table script: %s", keyStr)

		return 0
	}))

	L.SetMetatable(globals, mt)
}

// PrewarmPool creates and adds VMs to the pool up to the specified count.
func (p *LuaVMPool) PrewarmPool(count int) {
	if count > p.maxSize {
		count = p.maxSize
	}

	for range count {
		select {
		case p.pool <- p.newVM():
		default:
			return
		}
	}
}
