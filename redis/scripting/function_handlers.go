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
	"context"
	"strings"
	"time"

	"github.com/swytchdb/swytch/redis/shared"
	lua "github.com/yuin/gopher-lua"
)

// HandleFunction handles FUNCTION subcommands.
func (e *Engine) HandleFunction(cmd *shared.Command, w *shared.Writer) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("function")
		return
	}

	subCmd := strings.ToUpper(string(cmd.Args[0]))

	switch subCmd {
	case "LOAD":
		e.handleFunctionLoad(cmd, w)
	case "DELETE":
		e.handleFunctionDelete(cmd, w)
	case "LIST":
		e.handleFunctionList(cmd, w)
	case "FLUSH":
		e.handleFunctionFlush(cmd, w)
	case "STATS":
		e.handleFunctionStats(cmd, w)
	case "KILL":
		e.handleFunctionKill(cmd, w)
	case "DUMP":
		e.handleFunctionDump(cmd, w)
	case "RESTORE":
		e.handleFunctionRestore(cmd, w)
	default:
		w.WriteError("ERR unknown subcommand '" + string(cmd.Args[0]) + "'")
	}
}

// handleFunctionLoad handles FUNCTION LOAD command.
// Syntax: FUNCTION LOAD [REPLACE] function_code
func (e *Engine) handleFunctionLoad(cmd *shared.Command, w *shared.Writer) {
	if e.functionRegistry == nil {
		w.WriteError("ERR function support not initialized")
		return
	}

	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("function load")
		return
	}

	// Parse arguments
	replace := false
	codeIdx := 1

	// Check for REPLACE option
	if strings.ToUpper(string(cmd.Args[1])) == "REPLACE" {
		replace = true
		codeIdx = 2
		if len(cmd.Args) < 3 {
			w.WriteWrongNumArguments("function load")
			return
		}
	}

	code := string(cmd.Args[codeIdx])

	// Parse shebang to get library name
	engine, libName, err := parseShebang(code)
	if err != nil {
		w.WriteError("ERR " + err.Error())
		return
	}

	// Check if library already exists and REPLACE was not specified
	if !replace && e.functionRegistry.GetLibrary(libName) != nil {
		w.WriteError("ERR Library '" + libName + "' already exists")
		return
	}

	// Create a new Lua VM for loading
	L := e.luaVMPool.Get()
	defer e.luaVMPool.Put(L)

	// Clear any stale context from previous operations
	L.SetContext(context.Background())

	// Create load context
	loadCtx := &LibraryLoadContext{
		LibraryName: libName,
		Functions:   make(map[string]*FunctionDef),
	}

	// Setup redis global for library loading
	setupRedisGlobalForLibraryLoad(L, loadCtx)

	// Strip shebang before executing (Lua doesn't understand shebang syntax)
	luaCode := stripShebang(code)

	// Execute the library code to collect function registrations
	if err := L.DoString(luaCode); err != nil {
		if loadCtx.Error != "" {
			w.WriteError(loadCtx.Error)
		} else {
			w.WriteError("ERR Error loading library: " + err.Error())
		}
		return
	}

	// Check for errors during registration
	if loadCtx.Error != "" {
		w.WriteError(loadCtx.Error)
		return
	}

	// Check that at least one function was registered
	if len(loadCtx.Functions) == 0 {
		w.WriteError("ERR No functions registered in library")
		return
	}

	// Create the library
	lib := &Library{
		Name:      libName,
		Engine:    engine,
		Source:    code,
		Functions: loadCtx.Functions,
	}

	// Load into registry
	if err := e.functionRegistry.Load(lib, replace); err != nil {
		w.WriteError("ERR " + err.Error())
		return
	}

	// Return the library name
	w.WriteBulkStringStr(libName)
}

// handleFunctionDelete handles FUNCTION DELETE command.
// Syntax: FUNCTION DELETE library_name
func (e *Engine) handleFunctionDelete(cmd *shared.Command, w *shared.Writer) {
	if e.functionRegistry == nil {
		w.WriteError("ERR function support not initialized")
		return
	}

	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("function delete")
		return
	}

	libName := string(cmd.Args[1])

	if !e.functionRegistry.Delete(libName) {
		w.WriteError("ERR Library not found")
		return
	}

	w.WriteOK()
}

// handleFunctionList handles FUNCTION LIST command.
// Syntax: FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE]
func (e *Engine) handleFunctionList(cmd *shared.Command, w *shared.Writer) {
	if e.functionRegistry == nil {
		w.WriteError("ERR function support not initialized")
		return
	}

	// Parse options
	pattern := ""
	withCode := false

	for i := 1; i < len(cmd.Args); i++ {
		arg := strings.ToUpper(string(cmd.Args[i]))
		switch arg {
		case "LIBRARYNAME":
			if i+1 >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			pattern = string(cmd.Args[i+1])
			i++
		case "WITHCODE":
			withCode = true
		}
	}

	libraries := e.functionRegistry.List(pattern)

	// Write array of libraries
	w.WriteArray(len(libraries))
	for _, lib := range libraries {
		// Each library is a map with: library_name, engine, functions, [library_code]
		numFields := 3 // library_name, engine, functions
		if withCode {
			numFields = 4
		}
		w.WriteMap(numFields)

		w.WriteBulkStringStr("library_name")
		w.WriteBulkStringStr(lib.Name)

		w.WriteBulkStringStr("engine")
		w.WriteBulkStringStr(lib.Engine)

		w.WriteBulkStringStr("functions")
		// Write array of functions
		w.WriteArray(len(lib.Functions))
		for _, fn := range lib.Functions {
			// Each function has: name, description, flags
			w.WriteMap(3)

			w.WriteBulkStringStr("name")
			w.WriteBulkStringStr(fn.Name)

			w.WriteBulkStringStr("description")
			if fn.Description != "" {
				w.WriteBulkStringStr(fn.Description)
			} else {
				w.WriteNullBulkString()
			}

			w.WriteBulkStringStr("flags")
			w.WriteArray(len(fn.Flags))
			for _, flag := range fn.Flags {
				w.WriteBulkStringStr(flag)
			}
		}

		if withCode {
			w.WriteBulkStringStr("library_code")
			w.WriteBulkStringStr(lib.Source)
		}
	}
}

// handleFunctionFlush handles FUNCTION FLUSH command.
// Syntax: FUNCTION FLUSH [ASYNC|SYNC]
func (e *Engine) handleFunctionFlush(cmd *shared.Command, w *shared.Writer) {
	if e.functionRegistry == nil {
		w.WriteError("ERR function support not initialized")
		return
	}

	// ASYNC/SYNC option is ignored (we always do sync)
	e.functionRegistry.Flush()
	w.WriteOK()
}

// handleFunctionStats handles FUNCTION STATS command.
func (e *Engine) handleFunctionStats(cmd *shared.Command, w *shared.Writer) {
	if e.functionRegistry == nil {
		w.WriteError("ERR function support not initialized")
		return
	}

	// Check if there's a running function
	sc := e.runningScript.Load()

	w.WriteMap(2)

	// running_script - information about currently running script
	w.WriteBulkStringStr("running_script")
	if sc != nil {
		// Return info about running script
		w.WriteMap(2)
		w.WriteBulkStringStr("name")
		w.WriteNullBulkString() // We don't track script names
		w.WriteBulkStringStr("command")
		w.WriteArray(0) // Would contain the command that triggered it
	} else {
		w.WriteNullBulkString()
	}

	// engines - stats per engine
	w.WriteBulkStringStr("engines")
	w.WriteMap(1)
	w.WriteBulkStringStr("LUA")
	w.WriteMap(2)
	w.WriteBulkStringStr("libraries_count")
	w.WriteInteger(int64(e.functionRegistry.LibraryCount()))
	w.WriteBulkStringStr("functions_count")
	w.WriteInteger(int64(e.functionRegistry.Count()))
}

// handleFunctionKill handles FUNCTION KILL command.
func (e *Engine) handleFunctionKill(cmd *shared.Command, w *shared.Writer) {
	// Reuse the script kill logic
	sc := e.runningScript.Load()
	if sc == nil {
		w.WriteError("NOTBUSY No scripts in execution right now.")
		return
	}

	sc.Kill()
	w.WriteOK()
}

// handleFunctionDump handles FUNCTION DUMP command.
// Returns a serialized representation of all libraries.
func (e *Engine) handleFunctionDump(cmd *shared.Command, w *shared.Writer) {
	if e.functionRegistry == nil {
		w.WriteError("ERR function support not initialized")
		return
	}

	// Get all libraries
	libraries := e.functionRegistry.List("")

	if len(libraries) == 0 {
		// Empty dump - return empty bulk string
		w.WriteBulkStringStr("")
		return
	}

	// Simple serialization format: JSON-like string representation
	// In a real implementation, this would be a binary format
	var dump strings.Builder
	for i, lib := range libraries {
		if i > 0 {
			dump.WriteString("\n---\n")
		}
		dump.WriteString(lib.Source)
	}

	w.WriteBulkStringStr(dump.String())
}

// handleFunctionRestore handles FUNCTION RESTORE command.
// Syntax: FUNCTION RESTORE serialized_data [FLUSH|APPEND|REPLACE]
func (e *Engine) handleFunctionRestore(cmd *shared.Command, w *shared.Writer) {
	if e.functionRegistry == nil {
		w.WriteError("ERR function support not initialized")
		return
	}

	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("function restore")
		return
	}

	data := string(cmd.Args[1])

	// Parse policy
	policy := "APPEND" // default
	if len(cmd.Args) >= 3 {
		policy = strings.ToUpper(string(cmd.Args[2]))
	}

	switch policy {
	case "FLUSH":
		e.functionRegistry.Flush()
	case "APPEND", "REPLACE":
		// APPEND: fail if library exists
		// REPLACE: overwrite existing libraries
	default:
		w.WriteError("ERR invalid restore policy")
		return
	}

	// Split by delimiter and restore each library
	replace := policy == "REPLACE"
	parts := strings.SplitSeq(data, "\n---\n")

	for source := range parts {
		source = strings.TrimSpace(source)
		if source == "" {
			continue
		}

		// Parse shebang
		engine, libName, err := parseShebang(source)
		if err != nil {
			w.WriteError("ERR " + err.Error())
			return
		}

		// Create a new Lua VM for loading
		L := e.luaVMPool.Get()

		// Clear any stale context from previous operations
		L.SetContext(context.Background())

		// Create load context
		loadCtx := &LibraryLoadContext{
			LibraryName: libName,
			Functions:   make(map[string]*FunctionDef),
		}

		// Setup redis global for library loading
		setupRedisGlobalForLibraryLoad(L, loadCtx)

		// Strip shebang before executing
		luaCode := stripShebang(source)

		// Execute the library code
		if err := L.DoString(luaCode); err != nil {
			e.luaVMPool.Put(L)
			if loadCtx.Error != "" {
				w.WriteError(loadCtx.Error)
			} else {
				w.WriteError("ERR Error loading library: " + err.Error())
			}
			return
		}

		e.luaVMPool.Put(L)

		if loadCtx.Error != "" {
			w.WriteError(loadCtx.Error)
			return
		}

		if len(loadCtx.Functions) == 0 {
			w.WriteError("ERR No functions registered in library")
			return
		}

		// Create and load the library
		lib := &Library{
			Name:      libName,
			Engine:    engine,
			Source:    source,
			Functions: loadCtx.Functions,
		}

		if err := e.functionRegistry.Load(lib, replace); err != nil {
			w.WriteError("ERR " + err.Error())
			return
		}
	}

	w.WriteOK()
}

// HandleFcall handles FCALL and FCALL_RO commands.
// Syntax: FCALL function_name numkeys [key ...] [arg ...]
func (e *Engine) HandleFcall(cmd *shared.Command, w *shared.Writer, conn *shared.Connection, db *shared.Database, readOnly bool) {
	if e.functionRegistry == nil {
		w.WriteError("ERR function support not initialized")
		return
	}

	if len(cmd.Args) < 2 {
		if readOnly {
			w.WriteWrongNumArguments("fcall_ro")
		} else {
			w.WriteWrongNumArguments("fcall")
		}
		return
	}

	funcName := string(cmd.Args[0])

	// Parse numkeys
	numKeys, ok := shared.ParseInt64(cmd.Args[1])
	if !ok {
		w.WriteError("ERR value is not an integer or out of range")
		return
	}
	if numKeys < 0 {
		w.WriteError("ERR Number of keys can't be negative")
		return
	}

	// Validate argument count
	if len(cmd.Args) < 2+int(numKeys) {
		w.WriteError("ERR Number of keys can't be greater than number of args")
		return
	}

	// Look up the function
	funcDef, lib := e.functionRegistry.GetFunction(funcName)
	if funcDef == nil || lib == nil {
		w.WriteError("ERR Function not found")
		return
	}

	// Note: For FCALL_RO, Redis allows calling any function but errors on write commands.
	// Write command validation happens during script execution, not here.

	// Extract KEYS and ARGV
	keys := make([]string, numKeys)
	for i := range numKeys {
		keys[i] = string(cmd.Args[2+i])
	}

	args := make([]string, len(cmd.Args)-2-int(numKeys))
	for i := range args {
		args[i] = string(cmd.Args[2+int(numKeys)+i])
	}

	// Execute the function
	e.executeFunction(funcName, lib.Source, keys, args, w, conn, db, readOnly)
}

// executeFunction runs a registered function with the given keys and arguments.
func (e *Engine) executeFunction(funcName, libSource string, keys, args []string, w *shared.Writer, conn *shared.Connection, db *shared.Database, readOnly bool) {
	// Acquire script lock for atomicity
	e.scriptMutex.Lock()
	defer e.scriptMutex.Unlock()

	// Create context with timeout (or cancel-only when timeout is 0)
	timeout := e.getScriptTimeout()
	var ctx context.Context
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	defer cancel()

	// Create script context
	sc := &ScriptContext{
		engine:    e,
		db:        db,
		conn:      conn,
		keys:      keys,
		args:      args,
		ctx:       ctx,
		cancel:    cancel,
		readOnly:  readOnly,
		startTime: time.Now(),
		timeout:   timeout,
	}

	// Store as running script (for FUNCTION KILL)
	e.runningScript.Store(sc)
	defer e.runningScript.Store((*ScriptContext)(nil))

	// Get a Lua VM from the pool
	L := e.luaVMPool.Get()
	defer e.luaVMPool.Put(L)

	// Set up context for cancellation
	L.SetContext(ctx)

	// First, load the library to define functions
	loadCtx := &LibraryLoadContext{
		Functions: make(map[string]*FunctionDef),
	}
	setupRedisGlobalForLibraryLoad(L, loadCtx)

	// Strip shebang before executing
	luaCode := stripShebang(libSource)

	if err := L.DoString(luaCode); err != nil {
		if sc.killed.Load() {
			w.WriteError("ERR Script killed by user with FUNCTION KILL")
			return
		}
		if ctx.Err() != nil {
			w.WriteError("ERR Script timed out")
			return
		}
		w.WriteError("ERR Error loading library: " + err.Error())
		return
	}

	// Now set up the real redis global with call/pcall
	setupRedisGlobal(L, sc)
	setupKeysAndArgv(L, keys, args)

	// Look up the function
	funcVal := L.GetGlobal("__func_" + funcName)
	fn, ok := funcVal.(*lua.LFunction)
	if !ok {
		w.WriteError("ERR Function not found in library")
		return
	}

	// Call the function with KEYS and ARGV tables
	keysTbl := L.GetGlobal("KEYS")
	argvTbl := L.GetGlobal("ARGV")

	if err := L.CallByParam(lua.P{
		Fn:      fn,
		NRet:    1,
		Protect: true,
	}, keysTbl, argvTbl); err != nil {
		if sc.killed.Load() {
			w.WriteError("ERR Script killed by user with FUNCTION KILL")
			return
		}
		if ctx.Err() != nil {
			w.WriteError("ERR Script timed out")
			return
		}
		// Extract just the first line of the error (RESP errors must be single-line)
		errMsg := err.Error()
		if idx := strings.Index(errMsg, "\n"); idx != -1 {
			errMsg = errMsg[:idx]
		}
		// Translate gopher-lua errors to Redis-compatible messages
		errMsg = translateLuaError(errMsg)
		// Check if this is a Redis error that should be returned directly
		errMsg = extractRedisError(errMsg)
		w.WriteError(errMsg)
		return
	}

	// Get the return value
	ret := L.Get(-1)
	L.Pop(1)

	// Convert Lua value to RESP
	luaToRESP(L, ret, w)
}

// translateLuaError translates gopher-lua error messages to Redis-compatible ones.
func translateLuaError(errMsg string) string {
	// gopher-lua says "registry overflow" where Redis says "too many results to unpack"
	if strings.Contains(errMsg, "registry overflow") {
		return strings.Replace(errMsg, "registry overflow", "too many results to unpack", 1)
	}
	return errMsg
}

// extractRedisError extracts the Redis error from a Lua error message.
// Lua errors have the format "<string>:line: error_message".
// If the error_message is a Redis error (starts with ERR, WRONGTYPE, etc.),
// we extract it and return it directly.
func extractRedisError(errMsg string) string {
	// Look for ": ERR " or similar patterns in the error
	// Format: "<string>:2: ERR Write commands are not allowed..."
	patterns := []string{": ERR ", ": WRONGTYPE ", ": NOSCRIPT ", ": BUSY ", ": NOTBUSY "}
	for _, pattern := range patterns {
		if idx := strings.Index(errMsg, pattern); idx != -1 {
			// Extract everything after the ": " prefix (including ERR)
			return errMsg[idx+2:]
		}
	}
	// No Redis error found, wrap it
	return "ERR Error running function: " + errMsg
}
