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

import "strings"

// CommandFlags describes command behavior for dispatch.
type CommandFlags uint16

const (
	FlagWrite    CommandFlags = 1 << iota // Mutates data
	FlagNoAuth                            // Allowed without authentication (AUTH, QUIT, HELLO)
	FlagNoQueue                           // Not queueable in MULTI (MULTI, EXEC, DISCARD, WATCH, UNWATCH, QUIT)
	FlagPubSub                            // Allowed in RESP2 pub/sub mode
	FlagTrackGet                          // Track GET latency
	FlagTrackSet                          // Track SET latency
)

// TxnPrepareFunc validates and prepares a command for transaction queueing.
// It has access to conn for commands that need connection state (scripting, pubsub, etc.).
type TxnPrepareFunc func(cmd *Command, w *Writer, db *Database, conn *Connection) (valid bool, keys []string, runner CommandRunner)

// KeyExtractFunc extracts keys from a command for ACL checking.
// Used by ConnHandler entries that touch data keys but bypass the standard Handler path.
type KeyExtractFunc func(cmd *Command) []string

// CommandEntry holds everything needed to dispatch a command.
type CommandEntry struct {
	// Handler is the standard handler: validate → return keys and a runner closure.
	// Used for both live execution and transaction queueing (when TxnPrepare is nil).
	Handler HandlerFunc

	// ConnHandler is for commands needing connection state (blocking, auth, pubsub, scripting).
	// When set, live dispatch calls this instead of Handler.
	ConnHandler ConnHandlerFunc

	// TxnPrepare overrides Handler for transaction queueing.
	// Used for blocking→non-blocking fallbacks and conn-needing commands in transactions.
	// If nil, Handler is used for transaction queueing.
	// Commands with neither Handler nor TxnPrepare cannot be queued (aborts transaction).
	TxnPrepare TxnPrepareFunc

	// Keys extracts data keys from a command for per-key ACL enforcement.
	// Required for ConnHandler entries that touch data keys (blocking commands, SORT, scripting).
	// Nil means the command does not touch data keys (auth, pubsub channels, server commands).
	Keys KeyExtractFunc

	Flags CommandFlags
}

// Registry is the command dispatch table, indexed by CommandType for O(1) lookup.
// Stored as a field on Handler since entries reference handler methods.
type Registry [CmdMax]*CommandEntry

// Register adds a command entry to the Registry.
func (r *Registry) Register(cmd CommandType, entry *CommandEntry) {
	if int(cmd) < 0 || int(cmd) >= len(r) {
		panic("redis: CommandType out of registry bounds; update CmdMax")
	}
	r[cmd] = entry
}

// Lookup returns the entry for a command type, or nil if unregistered.
func (r *Registry) Lookup(cmd CommandType) *CommandEntry {
	if int(cmd) >= len(r) {
		return nil
	}
	return r[cmd]
}

// WrapHandler wraps a direct-execute function as a HandlerFunc (no keys, always valid).
func WrapHandler(fn func()) HandlerFunc {
	return func(_ *Command, _ *Writer, _ *Database) (bool, []string, CommandRunner) {
		return true, nil, fn
	}
}

// Common key-extraction helpers for ConnHandler commands.

// KeysAllButLast extracts keys from all args except the last (timeout).
// Used by BLPOP, BRPOP, BZPOPMIN, BZPOPMAX.
func KeysAllButLast(cmd *Command) []string {
	if len(cmd.Args) < 2 {
		return nil
	}
	keys := make([]string, len(cmd.Args)-1)
	for i := 0; i < len(cmd.Args)-1; i++ {
		keys[i] = string(cmd.Args[i])
	}
	return keys
}

// KeysNumkeysAtOffset extracts keys using a numkeys argument at a given offset.
// Used by BLMPOP (offset=1), BZMPOP (offset=1), EVAL/EVALSHA/FCALL (offset=1).
func KeysNumkeysAtOffset(cmd *Command, offset int) []string {
	if len(cmd.Args) <= offset {
		return nil
	}
	numKeys, ok := ParseInt64(cmd.Args[offset])
	if !ok || numKeys <= 0 {
		return nil
	}
	start := offset + 1
	if int64(len(cmd.Args)-start) < numKeys {
		return nil
	}
	keys := make([]string, numKeys)
	for i := range numKeys {
		keys[i] = string(cmd.Args[start+int(i)])
	}
	return keys
}

// KeysFirstTwo extracts the first two args as keys (source, destination).
// Used by BLMOVE, BRPOPLPUSH.
func KeysFirstTwo(cmd *Command) []string {
	if len(cmd.Args) < 2 {
		return nil
	}
	return []string{string(cmd.Args[0]), string(cmd.Args[1])}
}

// KeysFirst extracts the first arg as a key.
func KeysFirst(cmd *Command) []string {
	if len(cmd.Args) < 1 {
		return nil
	}
	cmd.SetSingleKey(cmd.Args[0])
	return cmd.SingleKeySlice()
}

// KeysAfterStreams extracts keys after the STREAMS keyword.
// Used by XREAD and XREADGROUP.
func KeysAfterStreams(cmd *Command) []string {
	// Find STREAMS keyword
	streamsIdx := -1
	for i, arg := range cmd.Args {
		if strings.EqualFold(string(arg), "STREAMS") {
			streamsIdx = i
			break
		}
	}
	if streamsIdx < 0 {
		return nil
	}
	remaining := cmd.Args[streamsIdx+1:]
	if len(remaining) == 0 || len(remaining)%2 != 0 {
		return nil
	}
	numStreams := len(remaining) / 2
	keys := make([]string, numStreams)
	for i := range numStreams {
		keys[i] = string(remaining[i])
	}
	return keys
}

// ModuleEntry pairs a CommandType with its CommandEntry for module registration.
type ModuleEntry struct {
	Cmd   CommandType
	Entry *CommandEntry
}

var moduleRegistrations []ModuleEntry

// RegisterModuleCommands appends command entries from an external module.
// Called from init() functions in module packages (e.g., redis/zset).
func RegisterModuleCommands(entries ...ModuleEntry) {
	moduleRegistrations = append(moduleRegistrations, entries...)
}

// GetModuleRegistrations returns all module-registered command entries.
func GetModuleRegistrations() []ModuleEntry {
	return moduleRegistrations
}

// KeysAll extracts all args as keys.
// Used by DEL, DELEX, PFMERGE, SINTERSTORE, SUNIONSTORE, SDIFFSTORE.
func KeysAll(cmd *Command) []string {
	keys := make([]string, len(cmd.Args))
	for i, arg := range cmd.Args {
		keys[i] = string(arg)
	}
	return keys
}

// KeysMSetStyle extracts keys from MSET-style commands where args are key-value pairs.
// Every other arg starting at index 0 is a key.
func KeysMSetStyle(cmd *Command) []string {
	if len(cmd.Args) < 2 {
		return nil
	}
	keys := make([]string, 0, len(cmd.Args)/2)
	for i := 0; i < len(cmd.Args); i += 2 {
		keys = append(keys, string(cmd.Args[i]))
	}
	return keys
}

// KeysSortCmd extracts keys from SORT/SORT_RO (source key + optional STORE destination).
func KeysSortCmd(cmd *Command) []string {
	if len(cmd.Args) < 1 {
		return nil
	}
	keys := []string{string(cmd.Args[0])}
	// Scan for STORE option
	for i := 1; i < len(cmd.Args); i++ {
		if strings.EqualFold(string(cmd.Args[i]), "STORE") && i+1 < len(cmd.Args) {
			keys = append(keys, string(cmd.Args[i+1]))
			break
		}
	}
	return keys
}
