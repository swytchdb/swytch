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

// HandleEval handles EVAL, EVALSHA, EVAL_RO, and EVALSHA_RO commands.
func (e *Engine) HandleEval(cmd *shared.Command, w *shared.Writer, conn *shared.Connection, db *shared.Database, readOnly bool) {
	if e.scriptRegistry == nil || e.luaVMPool == nil {
		w.WriteError("ERR scripting not initialized")
		return
	}

	isEvalSha := cmd.Type == shared.CmdEvalSha || cmd.Type == shared.CmdEvalShaRO

	if len(cmd.Args) < 2 {
		if isEvalSha {
			w.WriteWrongNumArguments("evalsha")
		} else {
			w.WriteWrongNumArguments("eval")
		}
		return
	}

	// Get script or SHA1
	scriptOrSha := cmd.Args[0]

	// Parse numkeys
	numKeys, ok := shared.ParseInt64(cmd.Args[1])
	if !ok || numKeys < 0 {
		w.WriteError("ERR value is not an integer or out of range")
		return
	}

	// Validate argument count
	if len(cmd.Args) < 2+int(numKeys) {
		w.WriteError("ERR Number of keys can't be greater than number of args")
		return
	}

	// Get script source
	var script []byte

	if isEvalSha {
		sha := strings.ToLower(string(scriptOrSha))
		var found bool
		script, found = e.scriptRegistry.Get(sha)
		if !found {
			w.WriteError("NOSCRIPT No matching script. Please use EVAL.")
			return
		}
	} else {
		script = scriptOrSha
		e.scriptRegistry.Load(script) // store script, sha not needed here
	}

	// Extract KEYS and ARGV
	keys := make([]string, numKeys)
	for i := range numKeys {
		keys[i] = string(cmd.Args[2+i])
	}

	args := make([]string, len(cmd.Args)-2-int(numKeys))
	for i := range args {
		args[i] = string(cmd.Args[2+int(numKeys)+i])
	}

	// Execute the script
	e.executeScript(script, keys, args, w, conn, db, readOnly)
}

// executeScript runs a Lua script with the given keys and arguments.
func (e *Engine) executeScript(script []byte, keys, args []string, w *shared.Writer, conn *shared.Connection, db *shared.Database, readOnly bool) {
	// Acquire script lock for atomicity (matches Redis behavior)
	e.scriptMutex.Lock()
	defer e.scriptMutex.Unlock()

	// Save the original database selection - SELECT inside scripts should not affect caller
	originalDB := conn.SelectedDB
	defer func() { conn.SelectedDB = originalDB }()

	// Get timeout for BUSY checking (not for automatic termination)
	// In Redis, lua-time-limit makes the script killable and causes BUSY for other clients,
	// but does NOT automatically terminate the script.
	// A zero timeout means scripts never become killable via SCRIPT KILL.
	timeout := e.getScriptTimeout()

	// Create context that only cancels when SCRIPT KILL is called (no timeout)
	ctx, cancel := context.WithCancel(context.Background())
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

	// Store as running script (for SCRIPT KILL)
	e.runningScript.Store(sc)
	defer e.runningScript.Store((*ScriptContext)(nil))

	// Get a Lua VM from the pool
	L := e.luaVMPool.Get()
	defer e.luaVMPool.Put(L)

	// Set up context for cancellation (only triggered by SCRIPT KILL)
	L.SetContext(ctx)

	// Set up redis global and KEYS/ARGV
	setupRedisGlobal(L, sc)
	setupKeysAndArgv(L, keys, args)

	// Execute the script
	if err := L.DoString(string(script)); err != nil {
		// Check if it was killed via SCRIPT KILL
		if sc.killed.Load() {
			w.WriteError("ERR Script killed by user with SCRIPT KILL")
			return
		}
		if ctx.Err() != nil {
			// Context was cancelled (by SCRIPT KILL)
			w.WriteError("ERR Script killed by user with SCRIPT KILL")
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
		errMsg = extractRedisScriptError(errMsg)
		w.WriteError(errMsg)
		return
	}

	// Get the return value (if any)
	if L.GetTop() == 0 {
		w.WriteNullBulkString()
		return
	}
	ret := L.Get(-1)
	L.Pop(1)

	// Convert Lua value to RESP
	luaToRESP(L, ret, w)
}

// HandleScript handles SCRIPT subcommands.
func (e *Engine) HandleScript(cmd *shared.Command, w *shared.Writer) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("script")
		return
	}

	subCmd := strings.ToUpper(string(cmd.Args[0]))

	switch subCmd {
	case "LOAD":
		e.handleScriptLoad(cmd, w)
	case "EXISTS":
		e.handleScriptExists(cmd, w)
	case "FLUSH":
		e.handleScriptFlush(cmd, w)
	case "KILL":
		e.handleScriptKill(cmd, w)
	case "DEBUG":
		// DEBUG mode not supported, just acknowledge
		w.WriteOK()
	default:
		w.WriteError("ERR Unknown SCRIPT subcommand or wrong number of arguments for '" + string(cmd.Args[0]) + "'")
	}
}

// handleScriptLoad handles SCRIPT LOAD command.
func (e *Engine) handleScriptLoad(cmd *shared.Command, w *shared.Writer) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("script load")
		return
	}

	if e.scriptRegistry == nil {
		w.WriteError("ERR scripting not initialized")
		return
	}

	script := cmd.Args[1]

	// Validate the script by trying to compile it
	L := lua.NewState(lua.Options{SkipOpenLibs: true})
	defer L.Close()

	_, err := L.LoadString(string(script))
	if err != nil {
		w.WriteError("ERR Error compiling script: " + err.Error())
		return
	}

	// Store and return SHA1
	sha := e.scriptRegistry.Load(script)
	w.WriteBulkStringStr(sha)
}

// handleScriptExists handles SCRIPT EXISTS command.
func (e *Engine) handleScriptExists(cmd *shared.Command, w *shared.Writer) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("script exists")
		return
	}

	if e.scriptRegistry == nil {
		w.WriteError("ERR scripting not initialized")
		return
	}

	// Collect SHA1 hashes
	shas := make([]string, len(cmd.Args)-1)
	for i := 1; i < len(cmd.Args); i++ {
		shas[i-1] = strings.ToLower(string(cmd.Args[i]))
	}

	// Check existence
	results := e.scriptRegistry.Exists(shas)

	// Write results
	w.WriteArray(len(results))
	for _, exists := range results {
		if exists {
			w.WriteOne()
		} else {
			w.WriteZero()
		}
	}
}

// handleScriptFlush handles SCRIPT FLUSH command.
func (e *Engine) handleScriptFlush(cmd *shared.Command, w *shared.Writer) {
	if e.scriptRegistry == nil {
		w.WriteError("ERR scripting not initialized")
		return
	}

	// Parse ASYNC/SYNC option (we ignore it, always sync)
	// SCRIPT FLUSH [ASYNC|SYNC]

	e.scriptRegistry.Flush()
	w.WriteOK()
}

// handleScriptKill handles SCRIPT KILL command.
func (e *Engine) handleScriptKill(cmd *shared.Command, w *shared.Writer) {
	sc := e.runningScript.Load()
	if sc == nil {
		w.WriteError("NOTBUSY No scripts in execution right now.")
		return
	}

	// Kill the running script
	sc.Kill()
	w.WriteOK()
}

// extractRedisScriptError extracts the Redis error from a Lua error message.
// Lua errors have the format "<string>:line: error_message".
// If the error_message is a Redis error (starts with ERR, WRONGTYPE, etc.),
// we extract it and return it directly.
func extractRedisScriptError(errMsg string) string {
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
	return "ERR Error running script: " + errMsg
}
