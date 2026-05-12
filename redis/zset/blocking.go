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

package zset

import (
	"strconv"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/redis/shared"
)

// handleBZPopMin implements BZPOPMIN key [key ...] timeout
func handleBZPopMin(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
	handleBlockingZPop(cmd, w, db, conn, true)
}

// handleBZPopMax implements BZPOPMAX key [key ...] timeout
func handleBZPopMax(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
	handleBlockingZPop(cmd, w, db, conn, false)
}

// handleBlockingZPop is the shared implementation for BZPOPMIN and BZPOPMAX
func handleBlockingZPop(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection, popMin bool) {
	if len(cmd.Args) < 2 {
		if popMin {
			w.WriteWrongNumArguments("bzpopmin")
		} else {
			w.WriteWrongNumArguments("bzpopmax")
		}
		return
	}

	timeout, errMsg := shared.ParseBlockingTimeout(string(cmd.Args[len(cmd.Args)-1]))
	if errMsg != "" {
		w.WriteError(errMsg)
		return
	}

	keys := cmd.Args[:len(cmd.Args)-1]

	// Check for WRONGTYPE before blocking
	for _, key := range keys {
		keyStr := string(key)
		_, _, exists, wrongType, err := getZSetSnapshot(cmd, keyStr)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if exists && wrongType {
			w.WriteWrongType()
			return
		}
	}

	// In script context, use non-blocking behavior (same as transactions)
	if conn.InScript {
		var valid bool
		var runner shared.CommandRunner
		if popMin {
			valid, _, runner = handleBZPopMinNonBlocking(cmd, w, db)
		} else {
			valid, _, runner = handleBZPopMaxNonBlocking(cmd, w, db)
		}
		if valid && runner != nil {
			runner()
		}
		return
	}

	// Setup blocking context with timeout and CLIENT UNBLOCK support
	ctx, cleanup := conn.Block(timeout)
	defer cleanup()

	// Register for wake signals on all keys
	reg, err := db.Manager().Subscriptions.Register(ctx, keys...)
	if err != nil {
		w.WriteError("ERR " + err.Error())
		return
	}
	defer reg.Cancel()

	// Main loop: try pop, if empty wait for wake signal
	for {
		// Get current database (may change after SWAPDB)
		currentDB := db.Manager().GetDB(conn.SelectedDB)

		// Try to pop from each key in order
		for _, key := range keys {
			keyStr := string(key)
			entry, found, err := tryZPop(cmd, keyStr, currentDB, popMin, reg)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if found {
				reg.Cancel()

				w.WriteArray(3)
				w.WriteBulkStringStr(keyStr)
				w.WriteBulkStringStr(entry.Member)
				w.WriteScore(entry.Score)
				return
			}
		}

		// Wait for wake signal or timeout/cancel
		if _, err := reg.Wait(); err != nil {
			if conn.WasUnblockedWithError() {
				w.WriteError("UNBLOCKED client unblocked via CLIENT UNBLOCK")
				return
			}
			w.WriteNullArray()
			return
		}
		// Woken up, loop back to try pop again
	}
}

// tryZPop attempts to pop an element from a zset key.
// Only pops if the registration is the oldest waiter for this key (fairness).
// Returns the entry and true if successful, empty and false otherwise.
func tryZPop(cmd *shared.Command, key string, db *shared.Database, popMin bool, reg *shared.Registration[struct{}]) (shared.ZSetEntry, bool, error) {
	if reg == nil {
		panic("tryZPop called without registration")
	}

	// Fairness: only pop if we're the oldest waiter
	if !reg.IsOldestWaiter([]byte(key)) {
		return shared.ZSetEntry{}, false, nil
	}

	snap, tips, exists, wrongType, err := getZSetSnapshot(cmd, key)
	if err != nil {
		return shared.ZSetEntry{}, false, err
	}
	if !exists || wrongType {
		return shared.ZSetEntry{}, false, nil
	}

	sorted := zsetSortedMembers(snap)
	if len(sorted) == 0 {
		return shared.ZSetEntry{}, false, nil
	}

	cmd.Context.BeginTx()

	if err := cmd.Context.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	}, tips); err != nil {
		return shared.ZSetEntry{}, false, err
	}

	var entry shared.ZSetEntry
	if popMin {
		entry = sorted[0]
	} else {
		entry = sorted[len(sorted)-1]
	}

	if err := emitZSetRemove(cmd, key, entry.Member); err != nil {
		return shared.ZSetEntry{}, false, err
	}

	return entry, true, nil
}

// handleBZMPop implements BZMPOP timeout numkeys key [key ...] <MIN | MAX> [COUNT count]
func handleBZMPop(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
	// In script context, use non-blocking behavior (same as transactions)
	if conn.InScript {
		valid, _, runner := handleBZMPopNonBlocking(cmd, w, db)
		if valid && runner != nil {
			runner()
		}
		return
	}

	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("bzmpop")
		return
	}

	timeout, errMsg := shared.ParseBlockingTimeout(string(cmd.Args[0]))
	if errMsg != "" {
		w.WriteError(errMsg)
		return
	}

	numKeys, err := strconv.Atoi(string(cmd.Args[1]))
	if err != nil || numKeys <= 0 {
		w.WriteError("ERR numkeys should be greater than 0")
		return
	}

	if len(cmd.Args) < numKeys+3 {
		w.WriteSyntaxError()
		return
	}

	keyStrs := make([]string, numKeys)
	keyBytes := make([][]byte, numKeys)
	for i := range numKeys {
		keyStrs[i] = string(cmd.Args[2+i])
		keyBytes[i] = cmd.Args[2+i]
	}

	// Parse MIN/MAX
	directionArg := shared.ToUpper(cmd.Args[2+numKeys])
	var popMin bool
	switch string(directionArg) {
	case "MIN":
		popMin = true
	case "MAX":
		popMin = false
	default:
		w.WriteSyntaxError()
		return
	}

	// Parse optional COUNT
	count := 1
	hasCount := false
	argIdx := 3 + numKeys
	for argIdx < len(cmd.Args) {
		opt := shared.ToUpper(cmd.Args[argIdx])
		if string(opt) == "COUNT" {
			if hasCount {
				w.WriteSyntaxError()
				return
			}
			hasCount = true
			argIdx++
			if argIdx >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			count, err = strconv.Atoi(string(cmd.Args[argIdx]))
			if err != nil || count <= 0 {
				w.WriteError("ERR count must be positive")
				return
			}
			argIdx++
		} else {
			w.WriteSyntaxError()
			return
		}
	}

	// Check for WRONGTYPE before blocking
	for _, key := range keyStrs {
		_, _, exists, wrongType, err := getZSetSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if exists && wrongType {
			w.WriteWrongType()
			return
		}
	}

	// Setup blocking context
	ctx, cleanup := conn.Block(timeout)
	defer cleanup()

	reg, regErr := db.Manager().Subscriptions.Register(ctx, keyBytes...)
	if regErr != nil {
		w.WriteError("ERR " + regErr.Error())
		return
	}
	defer reg.Cancel()

	// Main loop: try pop, if empty wait for wake signal
	for {
		// Get current database (may change after SWAPDB)
		currentDB := db.Manager().GetDB(conn.SelectedDB)
		_ = currentDB

		// Try to pop from each key in order
		for _, key := range keyStrs {
			entries, found, err := tryZMPop(cmd, key, popMin, count, reg)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if found {
				reg.Cancel()

				// Return [key, [[member, score], ...]]
				w.WriteArray(2)
				w.WriteBulkStringStr(key)
				w.WriteArray(len(entries))
				for _, entry := range entries {
					w.WriteArray(2)
					w.WriteBulkStringStr(entry.Member)
					w.WriteScore(entry.Score)
				}
				return
			}
		}

		// Wait for wake signal or timeout/cancel
		if _, err := reg.Wait(); err != nil {
			if conn.WasUnblockedWithError() {
				w.WriteError("UNBLOCKED client unblocked via CLIENT UNBLOCK")
				return
			}
			w.WriteNullArray()
			return
		}
		// Woken up, loop back to try pop again
	}
}

// tryZMPop attempts to pop multiple elements from a zset key.
// Only pops if the registration is the oldest waiter for this key (fairness).
// Returns the entries and true if successful, nil and false otherwise.
func tryZMPop(cmd *shared.Command, key string, popMin bool, count int, reg *shared.Registration[struct{}]) ([]shared.ZSetEntry, bool, error) {
	if reg == nil {
		panic("tryZMPop called without registration")
	}

	// Fairness: only pop if we're the oldest waiter
	if !reg.IsOldestWaiter([]byte(key)) {
		return nil, false, nil
	}

	snap, tips, exists, wrongType, err := getZSetSnapshot(cmd, key)
	if err != nil {
		return nil, false, err
	}
	if !exists || wrongType {
		return nil, false, nil
	}

	sorted := zsetSortedMembers(snap)
	if len(sorted) == 0 {
		return nil, false, nil
	}

	cmd.Context.BeginTx()

	if err := cmd.Context.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	}, tips); err != nil {
		return nil, false, err
	}

	n := min(count, len(sorted))

	var entries []shared.ZSetEntry
	if popMin {
		entries = sorted[:n]
	} else {
		entries = make([]shared.ZSetEntry, n)
		for i := range n {
			entries[i] = sorted[len(sorted)-1-i]
		}
	}

	for _, e := range entries {
		if err := emitZSetRemove(cmd, key, e.Member); err != nil {
			return nil, false, err
		}
	}

	return entries, true, nil
}

// handleBZPopMinNonBlocking handles BZPOPMIN inside transactions (non-blocking mode)
func handleBZPopMinNonBlocking(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	return handleZPopNonBlocking(cmd, w, db, true, "bzpopmin")
}

// handleBZPopMaxNonBlocking handles BZPOPMAX inside transactions (non-blocking mode)
func handleBZPopMaxNonBlocking(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	return handleZPopNonBlocking(cmd, w, db, false, "bzpopmax")
}

// handleZPopNonBlocking is the shared implementation for non-blocking BZPOPMIN/BZPOPMAX
func handleZPopNonBlocking(cmd *shared.Command, w *shared.Writer, db *shared.Database, popMin bool, cmdName string) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments(cmdName)
		return
	}

	// Last arg is timeout (ignored in non-blocking mode)
	keyArgs := cmd.Args[:len(cmd.Args)-1]
	keys = make([]string, len(keyArgs))
	for i, arg := range keyArgs {
		keys[i] = string(arg)
	}

	valid = true
	runner = func() {
		for _, keyStr := range keys {
			snap, tips, exists, wrongType, err := getZSetSnapshot(cmd, keyStr)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if !exists {
				continue
			}
			if wrongType {
				w.WriteWrongType()
				return
			}

			sorted := zsetSortedMembers(snap)
			if len(sorted) == 0 {
				continue
			}

			cmd.Context.BeginTx()

			if err := cmd.Context.Emit(&pb.Effect{
				Key:  []byte(keyStr),
				Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
			}, tips); err != nil {
				w.WriteError(err.Error())
				return
			}

			var entry shared.ZSetEntry
			if popMin {
				entry = sorted[0]
			} else {
				entry = sorted[len(sorted)-1]
			}

			if err := emitZSetRemove(cmd, keyStr, entry.Member); err != nil {
				w.WriteError(err.Error())
				return
			}

			w.WriteArray(3)
			w.WriteBulkStringStr(keyStr)
			w.WriteBulkStringStr(entry.Member)
			w.WriteScore(entry.Score)
			return
		}

		// No data found
		w.WriteNullArray()
	}
	return
}

// handleBZMPopNonBlocking handles BZMPOP inside transactions (non-blocking mode)
func handleBZMPopNonBlocking(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("bzmpop")
		return
	}

	// Skip timeout (args[0])
	numKeys, err := strconv.Atoi(string(cmd.Args[1]))
	if err != nil || numKeys <= 0 {
		w.WriteError("ERR numkeys should be greater than 0")
		return
	}

	if len(cmd.Args) < numKeys+3 {
		w.WriteSyntaxError()
		return
	}

	keys = make([]string, numKeys)
	for i := range numKeys {
		keys[i] = string(cmd.Args[2+i])
	}

	// Parse MIN/MAX
	directionArg := shared.ToUpper(cmd.Args[2+numKeys])
	var popMin bool
	switch string(directionArg) {
	case "MIN":
		popMin = true
	case "MAX":
		popMin = false
	default:
		w.WriteSyntaxError()
		return
	}

	// Parse optional COUNT
	count := 1
	hasCount := false
	argIdx := 3 + numKeys
	for argIdx < len(cmd.Args) {
		opt := shared.ToUpper(cmd.Args[argIdx])
		if string(opt) == "COUNT" {
			if hasCount {
				w.WriteSyntaxError()
				return
			}
			hasCount = true
			argIdx++
			if argIdx >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			count, err = strconv.Atoi(string(cmd.Args[argIdx]))
			if err != nil || count <= 0 {
				w.WriteError("ERR count must be positive")
				return
			}
			argIdx++
		} else {
			w.WriteSyntaxError()
			return
		}
	}

	valid = true
	runner = func() {
		// Try each key
		for _, key := range keys {
			snap, tips, exists, wrongType, err := getZSetSnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if !exists {
				continue
			}
			if wrongType {
				w.WriteWrongType()
				return
			}

			sorted := zsetSortedMembers(snap)
			if len(sorted) == 0 {
				continue
			}

			cmd.Context.BeginTx()

			if err := cmd.Context.Emit(&pb.Effect{
				Key:  []byte(key),
				Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
			}, tips); err != nil {
				w.WriteError(err.Error())
				return
			}

			n := min(count, len(sorted))

			var entries []shared.ZSetEntry
			if popMin {
				entries = sorted[:n]
			} else {
				entries = make([]shared.ZSetEntry, n)
				for i := range n {
					entries[i] = sorted[len(sorted)-1-i]
				}
			}

			for _, e := range entries {
				if err := emitZSetRemove(cmd, key, e.Member); err != nil {
					w.WriteError(err.Error())
					return
				}
			}

			// Return [key, [[member, score], ...]]
			w.WriteArray(2)
			w.WriteBulkStringStr(key)
			w.WriteArray(len(entries))
			for _, entry := range entries {
				w.WriteArray(2)
				w.WriteBulkStringStr(entry.Member)
				w.WriteScore(entry.Score)
			}
			return
		}

		// No data found
		w.WriteNullArray()
	}
	return
}
