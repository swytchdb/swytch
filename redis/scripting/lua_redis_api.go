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
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/swytchdb/swytch/redis/shared"
	lua "github.com/yuin/gopher-lua"
)

// LibraryLoadContext holds state during library loading to collect function registrations.
type LibraryLoadContext struct {
	LibraryName string
	Functions   map[string]*FunctionDef
	Error       string
}

// ScriptContext holds the execution context for a Lua script.
type ScriptContext struct {
	engine    *Engine
	db        *shared.Database
	conn      *shared.Connection
	keys      []string // KEYS array
	args      []string // ARGV array
	ctx       context.Context
	cancel    context.CancelFunc
	killed    atomic.Bool
	readOnly  bool          // True for EVAL_RO, EVALSHA_RO, FCALL_RO
	startTime time.Time     // When the script started executing
	timeout   time.Duration // Script timeout duration
}

// blockedCommands are commands that cannot be used inside scripts.
// Note: Blocking commands (BLPOP, BRPOP, etc.) are NOT blocked - they execute
// in non-blocking mode when InScript is true on the connection.
// WAIT is allowed - it returns 0 immediately (no replication).
var blockedCommands = map[string]bool{
	"SUBSCRIBE":  true,
	"PSUBSCRIBE": true,
	"MULTI":      true,
	"EXEC":       true,
	"DISCARD":    true,
	"WATCH":      true,
	"UNWATCH":    true,
}

// readOnlyCommands are commands that don't modify data and can be used in read-only scripts.
var readOnlyCommands = map[shared.CommandType]bool{
	// String read commands
	shared.CmdGet: true, shared.CmdMGet: true, shared.CmdStrLen: true, shared.CmdGetRange: true, shared.CmdSubstr: true,
	// Key read commands
	shared.CmdExists: true, shared.CmdType: true, shared.CmdKeys: true, shared.CmdScan: true, shared.CmdRandomKey: true,
	shared.CmdTTL: true, shared.CmdPTTL: true, shared.CmdExpireTime: true, shared.CmdPExpireTime: true,
	shared.CmdSort: true, shared.CmdSortRO: true,
	// Hash read commands
	shared.CmdHGet: true, shared.CmdHMGet: true, shared.CmdHGetAll: true, shared.CmdHKeys: true, shared.CmdHVals: true,
	shared.CmdHLen: true, shared.CmdHExists: true, shared.CmdHScan: true, shared.CmdHStrLen: true, shared.CmdHRandField: true,
	// List read commands
	shared.CmdLLen: true, shared.CmdLRange: true, shared.CmdLIndex: true, shared.CmdLPos: true,
	// Set read commands
	shared.CmdSMembers: true, shared.CmdSIsMember: true, shared.CmdSMIsMember: true, shared.CmdSCard: true,
	shared.CmdSRandMember: true, shared.CmdSInter: true, shared.CmdSUnion: true, shared.CmdSDiff: true,
	// Sorted set read commands
	shared.CmdZCard: true, shared.CmdZCount: true, shared.CmdZRange: true, shared.CmdZRangeByScore: true,
	shared.CmdZRevRange: true, shared.CmdZRevRangeByScore: true, shared.CmdZRank: true, shared.CmdZRevRank: true,
	shared.CmdZScore: true, shared.CmdZMScore: true, shared.CmdZLexCount: true,
	shared.CmdZRangeByLex: true, shared.CmdZRevRangeByLex: true, shared.CmdZRandMember: true,
	// Stream read commands
	shared.CmdXLen: true, shared.CmdXRange: true, shared.CmdXRevRange: true, shared.CmdXRead: true,
	shared.CmdXInfo: true, shared.CmdXPending: true,
	// HyperLogLog read commands
	shared.CmdPFCount: true,
	// Bitmap read commands
	shared.CmdGetBit: true, shared.CmdBitCount: true, shared.CmdBitPos: true,
	// Server/info commands
	shared.CmdPing: true, shared.CmdEcho: true, shared.CmdTime: true,
	shared.CmdInfo: true, shared.CmdDebug: true, shared.CmdMemory: true,
	shared.CmdConfig: true, shared.CmdClient: true, shared.CmdCommand: true,
}

// isWriteCommand returns true if the command modifies data.
func isWriteCommand(cmdType shared.CommandType) bool {
	return !readOnlyCommands[cmdType]
}

// setupRedisGlobal registers the 'redis' table with call/pcall functions.
func setupRedisGlobal(L *lua.LState, sc *ScriptContext) {
	// Create redis table
	redisTbl := L.NewTable()

	// Register redis.call
	L.SetField(redisTbl, "call", L.NewFunction(func(L *lua.LState) int {
		return redisCall(L, sc, true)
	}))

	// Register redis.pcall
	L.SetField(redisTbl, "pcall", L.NewFunction(func(L *lua.LState) int {
		return redisCall(L, sc, false)
	}))

	// Register redis.error_reply
	L.SetField(redisTbl, "error_reply", L.NewFunction(redisErrorReply))

	// Register redis.status_reply
	L.SetField(redisTbl, "status_reply", L.NewFunction(redisStatusReply))

	// Register redis.log (no-op for now)
	L.SetField(redisTbl, "log", L.NewFunction(func(L *lua.LState) int {
		return 0
	}))

	// Log levels
	L.SetField(redisTbl, "LOG_DEBUG", lua.LNumber(0))
	L.SetField(redisTbl, "LOG_VERBOSE", lua.LNumber(1))
	L.SetField(redisTbl, "LOG_NOTICE", lua.LNumber(2))
	L.SetField(redisTbl, "LOG_WARNING", lua.LNumber(3))

	// Set global redis table
	L.SetGlobal("redis", redisTbl)
}

// setupKeysAndArgv sets up the KEYS and ARGV global arrays.
func setupKeysAndArgv(L *lua.LState, keys, args []string) {
	// Create KEYS table
	keysTbl := L.NewTable()
	for i, key := range keys {
		keysTbl.RawSetInt(i+1, lua.LString(key))
	}
	L.SetGlobal("KEYS", keysTbl)

	// Create ARGV table
	argvTbl := L.NewTable()
	for i, arg := range args {
		argvTbl.RawSetInt(i+1, lua.LString(arg))
	}
	L.SetGlobal("ARGV", argvTbl)
}

// redisCall implements redis.call() and redis.pcall().
// If raiseError is true (redis.call), errors are raised as Lua errors.
// If raiseError is false (redis.pcall), errors are returned as {err=...} tables.
func redisCall(L *lua.LState, sc *ScriptContext, raiseError bool) int {
	// Check if script was killed
	if sc.killed.Load() {
		if raiseError {
			L.RaiseError("Script killed by user with SCRIPT KILL")
		}
		tbl := L.NewTable()
		tbl.RawSetString("err", lua.LString("Script killed by user with SCRIPT KILL"))
		L.Push(tbl)
		return 1
	}

	// Check context cancellation
	select {
	case <-sc.ctx.Done():
		if raiseError {
			L.RaiseError("Script timed out")
		}
		tbl := L.NewTable()
		tbl.RawSetString("err", lua.LString("Script timed out"))
		L.Push(tbl)
		return 1
	default:
	}

	// Get command name (first argument)
	if L.GetTop() < 1 {
		if raiseError {
			L.RaiseError("Please specify at least one argument for redis.call()")
		}
		tbl := L.NewTable()
		tbl.RawSetString("err", lua.LString("Please specify at least one argument for redis.call()"))
		L.Push(tbl)
		return 1
	}

	cmdName := strings.ToUpper(L.CheckString(1))

	// Check for blocked commands
	if blockedCommands[cmdName] {
		errMsg := "ERR This Redis command is not allowed from script"
		if raiseError {
			L.RaiseError("%s", errMsg)
		}
		tbl := L.NewTable()
		tbl.RawSetString("err", lua.LString(errMsg))
		L.Push(tbl)
		return 1
	}

	// Convert Lua arguments to byte slices
	args := luaArgsToBytes(L, 2)

	// Look up command type
	cmdType := shared.ParseCommandType([]byte(cmdName))
	if cmdType == shared.CmdUnknown {
		errMsg := "ERR Unknown Redis command called from script"
		if raiseError {
			L.RaiseError("%s", errMsg)
		}
		tbl := L.NewTable()
		tbl.RawSetString("err", lua.LString(errMsg))
		L.Push(tbl)
		return 1
	}

	// Check for write commands in read-only mode
	if sc.readOnly && isWriteCommand(cmdType) {
		errMsg := "ERR Write commands are not allowed from read-only scripts"
		if raiseError {
			L.RaiseError("%s", errMsg)
		}
		tbl := L.NewTable()
		tbl.RawSetString("err", lua.LString(errMsg))
		L.Push(tbl)
		return 1
	}

	// Create command
	cmd := &shared.Command{
		Type: cmdType,
		Args: args,
	}

	// Create temporary writer
	var buf bytes.Buffer
	w := shared.NewWriter(&buf)
	w.SetProtocol(sc.conn.Protocol)

	// Set InScript flag so blocking commands become non-blocking
	wasInScript := sc.conn.InScript
	sc.conn.InScript = true

	// Execute the command via the executor
	sc.engine.executor(cmd, w, sc.conn)

	// Restore InScript flag
	sc.conn.InScript = wasInScript

	// Parse the RESP response back to Lua
	respData := buf.Bytes()
	result, _, err := respToLua(L, respData)
	if err != nil {
		if raiseError {
			L.RaiseError("Error parsing Redis response: %v", err)
		}
		tbl := L.NewTable()
		tbl.RawSetString("err", lua.LString("Error parsing Redis response"))
		L.Push(tbl)
		return 1
	}

	// Check if the result is an error table and raiseError is true
	if raiseError {
		if tbl, ok := result.(*lua.LTable); ok {
			if errVal := tbl.RawGetString("err"); errVal != lua.LNil {
				if errStr, ok := errVal.(lua.LString); ok {
					errMsg := string(errStr)
					// Transform arity errors to script-specific format
					if strings.Contains(errMsg, "wrong number of args for") {
						errMsg = "Wrong number of args calling Redis command from script"
					}
					L.RaiseError("%s", errMsg)
				}
			}
		}
	}

	L.Push(result)
	return 1
}

// redisErrorReply creates an error reply table.
func redisErrorReply(L *lua.LState) int {
	msg := L.CheckString(1)
	tbl := L.NewTable()
	tbl.RawSetString("err", lua.LString(msg))
	L.Push(tbl)
	return 1
}

// redisStatusReply creates a status reply table.
func redisStatusReply(L *lua.LState) int {
	msg := L.CheckString(1)
	tbl := L.NewTable()
	tbl.RawSetString("ok", lua.LString(msg))
	L.Push(tbl)
	return 1
}

// Kill marks the script as killed.
func (sc *ScriptContext) Kill() {
	sc.killed.Store(true)
	if sc.cancel != nil {
		sc.cancel()
	}
}

// IsKilled returns whether the script has been killed.
func (sc *ScriptContext) IsKilled() bool {
	return sc.killed.Load()
}

// IsTimedOut returns whether the script has exceeded its timeout.
func (sc *ScriptContext) IsTimedOut() bool {
	if sc.timeout <= 0 {
		return false
	}
	return time.Since(sc.startTime) > sc.timeout
}

// parseShebang extracts library name and engine from the shebang line.
// Format: #!<engine> name=<name> [key=value...]
// Example: #!lua name=mylib
func parseShebang(source string) (engine, libName string, err error) {
	lines := strings.SplitN(source, "\n", 2)
	if len(lines) == 0 {
		return "", "", fmt.Errorf("empty library source")
	}

	shebang := strings.TrimSpace(lines[0])
	if !strings.HasPrefix(shebang, "#!") {
		return "", "", fmt.Errorf("missing shebang header")
	}

	// Remove #! prefix
	shebang = strings.TrimPrefix(shebang, "#!")
	parts := strings.Fields(shebang)
	if len(parts) == 0 {
		return "", "", fmt.Errorf("missing engine in shebang")
	}

	engine = strings.ToLower(parts[0])
	if engine != "lua" {
		return "", "", fmt.Errorf("unsupported engine '%s'", engine)
	}

	// Parse key=value pairs
	for _, part := range parts[1:] {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.ToLower(kv[0])
		value := kv[1]

		switch key {
		case "name":
			libName = value
		}
	}

	if libName == "" {
		return "", "", fmt.Errorf("missing library name in shebang")
	}

	return engine, libName, nil
}

// stripShebang removes the shebang line from Lua source code.
// The shebang line is not valid Lua syntax and must be stripped before execution.
func stripShebang(source string) string {
	if !strings.HasPrefix(source, "#!") {
		return source
	}
	_, after, ok := strings.Cut(source, "\n")
	if !ok {
		return "" // Only shebang, no code
	}
	return after
}

// setupRedisGlobalForLibraryLoad sets up the redis table for library loading.
// This variant includes redis.register_function() for function registration.
func setupRedisGlobalForLibraryLoad(L *lua.LState, ctx *LibraryLoadContext) {
	// Create redis table
	redisTbl := L.NewTable()

	// Register redis.register_function
	L.SetField(redisTbl, "register_function", L.NewFunction(func(L *lua.LState) int {
		return redisRegisterFunction(L, ctx)
	}))

	// Register redis.log (no-op for now)
	L.SetField(redisTbl, "log", L.NewFunction(func(L *lua.LState) int {
		return 0
	}))

	// Log levels
	L.SetField(redisTbl, "LOG_DEBUG", lua.LNumber(0))
	L.SetField(redisTbl, "LOG_VERBOSE", lua.LNumber(1))
	L.SetField(redisTbl, "LOG_NOTICE", lua.LNumber(2))
	L.SetField(redisTbl, "LOG_WARNING", lua.LNumber(3))

	// Set global redis table
	L.SetGlobal("redis", redisTbl)
}

// redisRegisterFunction implements redis.register_function().
// Supports two forms:
// 1. redis.register_function('name', callback)
// 2. redis.register_function{function_name='name', callback=func, flags={...}, description='...'}
func redisRegisterFunction(L *lua.LState, ctx *LibraryLoadContext) int {
	if ctx.Error != "" {
		// Already have an error, don't process more registrations
		return 0
	}

	nArgs := L.GetTop()
	if nArgs == 0 {
		ctx.Error = "ERR redis.register_function requires at least one argument"
		L.RaiseError("%s", ctx.Error)
		return 0
	}

	arg1 := L.Get(1)

	// Check which form is used
	if tbl, ok := arg1.(*lua.LTable); ok {
		// Table form: redis.register_function{function_name='...', callback=...}
		return registerFunctionFromTable(L, ctx, tbl)
	}

	// Positional form: redis.register_function('name', callback)
	if nArgs < 2 {
		ctx.Error = "ERR redis.register_function requires function name and callback"
		L.RaiseError("%s", ctx.Error)
		return 0
	}

	funcName, ok := arg1.(lua.LString)
	if !ok {
		ctx.Error = "ERR function name must be a string"
		L.RaiseError("%s", ctx.Error)
		return 0
	}

	callback := L.Get(2)
	if _, ok := callback.(*lua.LFunction); !ok {
		ctx.Error = "ERR callback must be a function"
		L.RaiseError("%s", ctx.Error)
		return 0
	}

	// Register the function
	name := string(funcName)
	if _, exists := ctx.Functions[name]; exists {
		ctx.Error = fmt.Sprintf("ERR Function '%s' already registered in this library", name)
		L.RaiseError("%s", ctx.Error)
		return 0
	}

	ctx.Functions[name] = &FunctionDef{
		Name:  name,
		Flags: nil,
	}

	// Store the function globally so it can be looked up during FCALL
	L.SetGlobal("__func_"+name, callback)

	return 0
}

// registerFunctionFromTable handles the table form of redis.register_function.
func registerFunctionFromTable(L *lua.LState, ctx *LibraryLoadContext, tbl *lua.LTable) int {
	// Extract function_name
	funcNameVal := tbl.RawGetString("function_name")
	if funcNameVal == lua.LNil {
		ctx.Error = "ERR missing function_name in registration table"
		L.RaiseError("%s", ctx.Error)
		return 0
	}
	funcName, ok := funcNameVal.(lua.LString)
	if !ok {
		ctx.Error = "ERR function_name must be a string"
		L.RaiseError("%s", ctx.Error)
		return 0
	}

	// Extract callback
	callbackVal := tbl.RawGetString("callback")
	if callbackVal == lua.LNil {
		ctx.Error = "ERR missing callback in registration table"
		L.RaiseError("%s", ctx.Error)
		return 0
	}
	callback, ok := callbackVal.(*lua.LFunction)
	if !ok {
		ctx.Error = "ERR callback must be a function"
		L.RaiseError("%s", ctx.Error)
		return 0
	}

	// Extract optional description
	var description string
	descVal := tbl.RawGetString("description")
	if descStr, ok := descVal.(lua.LString); ok {
		description = string(descStr)
	}

	// Extract optional flags
	var flags []string
	flagsVal := tbl.RawGetString("flags")
	if flagsTbl, ok := flagsVal.(*lua.LTable); ok {
		flagsTbl.ForEach(func(_, v lua.LValue) {
			if flagStr, ok := v.(lua.LString); ok {
				flags = append(flags, string(flagStr))
			}
		})
	}

	// Register the function
	name := string(funcName)
	if _, exists := ctx.Functions[name]; exists {
		ctx.Error = fmt.Sprintf("ERR Function '%s' already registered in this library", name)
		L.RaiseError("%s", ctx.Error)
		return 0
	}

	ctx.Functions[name] = &FunctionDef{
		Name:        name,
		Description: description,
		Flags:       flags,
	}

	// Store the function globally so it can be looked up during FCALL
	L.SetGlobal("__func_"+name, callback)

	return 0
}
