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

package list

import (
	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/redis/shared"
)

// handleBLPop implements the BLPOP command (blocking left pop)
// BLPOP key [key ...] timeout
func handleBLPop(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
	handleBlockingPop(cmd, w, db, conn, true)
}

// handleBRPop implements the BRPOP command (blocking right pop)
// BRPOP key [key ...] timeout
func handleBRPop(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
	handleBlockingPop(cmd, w, db, conn, false)
}

// handleBlockingPop is the shared implementation for BLPOP and BRPOP
func handleBlockingPop(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection, left bool) {
	if len(cmd.Args) < 2 {
		if left {
			w.WriteWrongNumArguments("blpop")
		} else {
			w.WriteWrongNumArguments("brpop")
		}
		return
	}

	// Last argument is timeout (in seconds)
	timeout, errMsg := shared.ParseBlockingTimeout(string(cmd.Args[len(cmd.Args)-1]))
	if errMsg != "" {
		w.WriteError(errMsg)
		return
	}

	keys := cmd.Args[:len(cmd.Args)-1]

	// Check for WRONGTYPE before blocking
	for _, key := range keys {
		keyStr := string(key)
		_, _, exists, wrongType, err := getListSnapshot(cmd, keyStr)
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
		handleLPopNonBlocking(cmd, w, db, left)
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
		// Try to pop from each key in order
		for _, key := range keys {
			keyStr := string(key)
			elem, found, err := tryPop(cmd, keyStr, left, reg)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if found {
				reg.Cancel()

				w.WriteArray(2)
				w.WriteBulkString(key)
				w.WriteBulkString(elem)
				return
			}
		}

		// Wait for wake signal or timeout/cancel
		if _, err := reg.Wait(); err != nil {
			if conn.WasUnblockedWithError() {
				w.WriteError("UNBLOCKED client unblocked via CLIENT UNBLOCK")
			} else {
				w.WriteNullArray()
			}
			return
		}
		// Woken up, loop back to try pop again
	}
}

// tryPop attempts to pop an element from a list key using the effects engine.
// Only pops if the registration is the oldest waiter for this key (fairness).
// Returns the element and true if successful, nil and false otherwise.
func tryPop(cmd *shared.Command, key string, left bool, reg *shared.Registration[struct{}]) ([]byte, bool, error) {
	if reg == nil {
		panic("tryPop called without registration")
	}

	if !reg.IsOldestWaiter([]byte(key)) {
		return nil, false, nil
	}

	snap, tips, exists, wrongType, err := getListSnapshot(cmd, key)
	if err != nil {
		return nil, false, err
	}
	if !exists || wrongType || len(snap.OrderedElements) == 0 {
		return nil, false, nil
	}

	cmd.Context.BeginTx()
	if err := cmd.Context.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	}, tips); err != nil {
		return nil, false, err
	}

	var elem *pb.ReducedElement
	if left {
		elem = snap.OrderedElements[0]
	} else {
		elem = snap.OrderedElements[len(snap.OrderedElements)-1]
	}

	if err := emitListRemove(cmd, key, elem.Data.Id); err != nil {
		return nil, false, err
	}

	return elem.Data.GetRaw(), true, nil
}

// handleBRPopLPushNonBlocking handles BRPOPLPUSH inside transactions (non-blocking mode)
// BRPOPLPUSH source destination timeout -> behaves like RPOPLPUSH
func handleBRPopLPushNonBlocking(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("brpoplpush")
		return
	}

	source := string(cmd.Args[0])
	destination := string(cmd.Args[1])
	// Args[2] is timeout, ignored in transaction
	keys = []string{source, destination}

	valid = true
	runner = func() {
		// BRPOPLPUSH = LMOVE source destination RIGHT LEFT
		elem, errMsg, ok, err := doLMove(cmd, source, destination, false, true)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !ok {
			if errMsg != "" {
				w.WriteError(errMsg)
			} else {
				w.WriteNullBulkString()
			}
			return
		}
		w.WriteBulkString(elem)
	}
	return
}

// handleBLMoveNonBlocking handles BLMOVE inside transactions (non-blocking mode)
// BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout -> behaves like LMOVE
func handleBLMoveNonBlocking(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 5 {
		w.WriteWrongNumArguments("blmove")
		return
	}

	source := string(cmd.Args[0])
	destination := string(cmd.Args[1])
	whereFrom := shared.ToUpper(cmd.Args[2])
	whereTo := shared.ToUpper(cmd.Args[3])
	// Args[4] is timeout, ignored in transaction
	keys = []string{source, destination}

	var popLeft, pushLeft bool
	switch string(whereFrom) {
	case "LEFT":
		popLeft = true
	case "RIGHT":
		popLeft = false
	default:
		w.WriteError("ERR syntax error")
		return
	}

	switch string(whereTo) {
	case "LEFT":
		pushLeft = true
	case "RIGHT":
		pushLeft = false
	default:
		w.WriteError("ERR syntax error")
		return
	}

	valid = true
	runner = func() {
		elem, errMsg, ok, err := doLMove(cmd, source, destination, popLeft, pushLeft)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !ok {
			if errMsg != "" {
				w.WriteError(errMsg)
			} else {
				w.WriteNullBulkString()
			}
			return
		}
		w.WriteBulkString(elem)
	}
	return
}

// handleLPopNonBlocking handles BLPOP/BRPOP inside transactions (non-blocking mode)
// In a transaction, blocking commands try once and return nil if no data
func handleLPopNonBlocking(cmd *shared.Command, w *shared.Writer, db *shared.Database, left bool) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		if left {
			w.WriteWrongNumArguments("blpop")
		} else {
			w.WriteWrongNumArguments("brpop")
		}
		return
	}

	// Last arg is timeout (ignored in transaction), rest are keys
	keyArgs := cmd.Args[:len(cmd.Args)-1]
	keys = make([]string, len(keyArgs))
	for i, arg := range keyArgs {
		keys[i] = string(arg)
	}

	valid = true
	runner = func() {
		for _, keyStr := range keys {
			snap, tips, exists, wrongType, err := getListSnapshot(cmd, keyStr)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if exists && wrongType {
				w.WriteWrongType()
				return
			}
			if !exists || len(snap.OrderedElements) == 0 {
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

			var elem *pb.ReducedElement
			if left {
				elem = snap.OrderedElements[0]
			} else {
				elem = snap.OrderedElements[len(snap.OrderedElements)-1]
			}
			if err := emitListRemove(cmd, keyStr, elem.Data.Id); err != nil {
				w.WriteError(err.Error())
				return
			}

			w.WriteArray(2)
			w.WriteBulkStringStr(keyStr)
			w.WriteBulkString(elem.Data.GetRaw())
			return
		}

		w.WriteNullArray()
	}
	return
}

// handleBLMPopNonBlocking handles BLMPOP inside transactions (non-blocking mode)
// In a transaction, blocking commands try once and return nil if no data
// BLMPOP timeout numkeys key [key ...] <LEFT | RIGHT> [COUNT count]
func handleBLMPopNonBlocking(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("blmpop")
		return
	}

	// Skip timeout (Args[0]), parse numkeys from Args[1]
	numKeys, ok := shared.ParseInt64(cmd.Args[1])
	if !ok || numKeys < 1 {
		w.WriteError("ERR numkeys should be greater than 0")
		return
	}

	// Check we have enough args: timeout + numkeys + keys + direction
	if len(cmd.Args) < int(numKeys)+3 {
		w.WriteError("ERR syntax error")
		return
	}

	// Extract keys (offset by 2 for timeout and numkeys)
	keys = make([]string, numKeys)
	for i := range numKeys {
		keys[i] = string(cmd.Args[2+i])
	}

	// Parse direction (LEFT or RIGHT)
	dirIdx := int(numKeys) + 2
	direction := shared.ToUpper(cmd.Args[dirIdx])
	var left bool
	switch string(direction) {
	case "LEFT":
		left = true
	case "RIGHT":
		left = false
	default:
		w.WriteError("ERR syntax error")
		return
	}

	// Parse optional COUNT
	count := int64(1)
	if len(cmd.Args) > dirIdx+1 {
		if len(cmd.Args) != dirIdx+3 {
			w.WriteError("ERR syntax error")
			return
		}
		countArg := shared.ToUpper(cmd.Args[dirIdx+1])
		if string(countArg) != "COUNT" {
			w.WriteError("ERR syntax error")
			return
		}
		count, ok = shared.ParseInt64(cmd.Args[dirIdx+2])
		if !ok || count < 1 {
			w.WriteError("ERR count should be greater than 0")
			return
		}
	}

	valid = true
	runner = func() {
		// Type check all keys first
		for _, key := range keys {
			_, _, exists, wrongType, err := getListSnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if exists && wrongType {
				w.WriteWrongType()
				return
			}
		}

		// Try each key in order
		for _, key := range keys {
			snap, tips, exists, _, err := getListSnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if !exists || len(snap.OrderedElements) == 0 {
				continue
			}

			elements := snap.OrderedElements
			n := min(int(count), len(elements))

			cmd.Context.BeginTx()
			if err := cmd.Context.Emit(&pb.Effect{
				Key:  []byte(key),
				Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
			}, tips); err != nil {
				w.WriteError(err.Error())
				return
			}

			popped := make([][]byte, n)
			if left {
				for i := range n {
					popped[i] = elements[i].Data.GetRaw()
					if err := emitListRemove(cmd, key, elements[i].Data.Id); err != nil {
						w.WriteError(err.Error())
						return
					}
				}
			} else {
				for i := range n {
					idx := len(elements) - 1 - i
					popped[i] = elements[idx].Data.GetRaw()
					if err := emitListRemove(cmd, key, elements[idx].Data.Id); err != nil {
						w.WriteError(err.Error())
						return
					}
				}
			}

			w.WriteArray(2)
			w.WriteBulkStringStr(key)
			w.WriteArray(len(popped))
			for _, elem := range popped {
				w.WriteBulkString(elem)
			}
			return
		}

		w.WriteNullArray()
	}
	return
}

// handleBLMPop implements the BLMPOP command (blocking list multi-pop)
// BLMPOP timeout numkeys key [key ...] <LEFT | RIGHT> [COUNT count]
func handleBLMPop(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
	// In script context, use non-blocking behavior (same as transactions)
	if conn.InScript {
		handleBLMPopNonBlocking(cmd, w, db)
		return
	}

	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("blmpop")
		return
	}

	// Parse timeout (first argument)
	timeout, errMsg := shared.ParseBlockingTimeout(string(cmd.Args[0]))
	if errMsg != "" {
		w.WriteError(errMsg)
		return
	}

	// Parse numkeys
	numKeys, ok := shared.ParseInt64(cmd.Args[1])
	if !ok || numKeys < 1 {
		w.WriteError("ERR numkeys should be greater than 0")
		return
	}

	// Check we have enough args: timeout + numkeys + keys + direction
	if len(cmd.Args) < int(numKeys)+3 {
		w.WriteError("ERR syntax error")
		return
	}

	// Extract keys
	keys := make([]string, numKeys)
	for i := range numKeys {
		keys[i] = string(cmd.Args[2+i])
	}

	// Parse direction (LEFT or RIGHT)
	dirIdx := int(numKeys) + 2
	direction := shared.ToUpper(cmd.Args[dirIdx])
	var left bool
	switch string(direction) {
	case "LEFT":
		left = true
	case "RIGHT":
		left = false
	default:
		w.WriteError("ERR syntax error")
		return
	}

	// Parse optional COUNT
	count := int64(1)
	if len(cmd.Args) > dirIdx+1 {
		if len(cmd.Args) != dirIdx+3 {
			w.WriteError("ERR syntax error")
			return
		}
		countArg := shared.ToUpper(cmd.Args[dirIdx+1])
		if string(countArg) != "COUNT" {
			w.WriteError("ERR syntax error")
			return
		}
		var ok bool
		count, ok = shared.ParseInt64(cmd.Args[dirIdx+2])
		if !ok || count < 1 {
			w.WriteError("ERR count should be greater than 0")
			return
		}
	}

	// Check for WRONGTYPE before blocking
	for _, key := range keys {
		_, _, exists, wrongType, err := getListSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if exists && wrongType {
			w.WriteWrongType()
			return
		}
	}

	// Setup blocking context with timeout and CLIENT UNBLOCK support
	ctx, cleanup := conn.Block(timeout)
	defer cleanup()

	// Convert keys to []byte for Register
	keyBytes := make([][]byte, len(keys))
	for i, k := range keys {
		keyBytes[i] = []byte(k)
	}

	// Register for wake signals on all keys
	reg, err := db.Manager().Subscriptions.Register(ctx, keyBytes...)
	if err != nil {
		w.WriteError("ERR " + err.Error())
		return
	}
	defer reg.Cancel()

	// Main loop: try pop, if empty wait for wake signal
	for {
		// Try to pop from each key in order
		for _, key := range keys {
			elements, found, err := tryMultiPop(cmd, key, left, int(count), reg)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if found {
				reg.Cancel()

				w.WriteArray(2)
				w.WriteBulkStringStr(key)
				w.WriteArray(len(elements))
				for _, elem := range elements {
					w.WriteBulkString(elem)
				}
				return
			}
		}

		// Wait for wake signal or timeout/cancel
		if _, err := reg.Wait(); err != nil {
			if conn.WasUnblockedWithError() {
				w.WriteError("UNBLOCKED client unblocked via CLIENT UNBLOCK")
			} else {
				w.WriteNullArray()
			}
			return
		}
		// Woken up, loop back to try pop again
	}
}

// tryMultiPop attempts to pop multiple elements from a list key using the effects engine.
// Only pops if the registration is the oldest waiter for this key (fairness).
// Returns the elements and true if successful, nil and false otherwise.
func tryMultiPop(cmd *shared.Command, key string, left bool, count int, reg *shared.Registration[struct{}]) ([][]byte, bool, error) {
	if reg == nil {
		panic("tryMultiPop called without registration")
	}

	if !reg.IsOldestWaiter([]byte(key)) {
		return nil, false, nil
	}

	snap, tips, exists, wrongType, err := getListSnapshot(cmd, key)
	if err != nil {
		return nil, false, err
	}
	if !exists || wrongType || len(snap.OrderedElements) == 0 {
		return nil, false, nil
	}

	elements := snap.OrderedElements
	n := min(count, len(elements))

	cmd.Context.BeginTx()
	if err := cmd.Context.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	}, tips); err != nil {
		return nil, false, err
	}

	popped := make([][]byte, n)
	if left {
		for i := range n {
			popped[i] = elements[i].Data.GetRaw()
			if err := emitListRemove(cmd, key, elements[i].Data.Id); err != nil {
				return nil, false, err
			}
		}
	} else {
		for i := range n {
			idx := len(elements) - 1 - i
			popped[i] = elements[idx].Data.GetRaw()
			if err := emitListRemove(cmd, key, elements[idx].Data.Id); err != nil {
				return nil, false, err
			}
		}
	}

	return popped, true, nil
}

// handleBLMove implements the BLMOVE command (blocking version)
// BLMOVE source destination <LEFT | RIGHT> <LEFT | RIGHT> timeout
func handleBLMove(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
	// In script context, use non-blocking behavior (same as transactions)
	if conn.InScript {
		handleBLMoveNonBlocking(cmd, w, db)
		return
	}

	if len(cmd.Args) != 5 {
		w.WriteWrongNumArguments("blmove")
		return
	}

	source := string(cmd.Args[0])
	destination := string(cmd.Args[1])
	whereFrom := shared.ToUpper(cmd.Args[2])
	whereTo := shared.ToUpper(cmd.Args[3])

	// Parse timeout
	timeout, errMsg := shared.ParseBlockingTimeout(string(cmd.Args[4]))
	if errMsg != "" {
		w.WriteError(errMsg)
		return
	}

	// Parse directions
	var popLeft, pushLeft bool
	switch string(whereFrom) {
	case "LEFT":
		popLeft = true
	case "RIGHT":
		popLeft = false
	default:
		w.WriteError("ERR syntax error")
		return
	}
	switch string(whereTo) {
	case "LEFT":
		pushLeft = true
	case "RIGHT":
		pushLeft = false
	default:
		w.WriteError("ERR syntax error")
		return
	}

	// Setup blocking context with timeout and CLIENT UNBLOCK support
	ctx, cleanup := conn.Block(timeout)
	defer cleanup()

	// Register for wake signals on source key
	reg, err := db.Manager().Subscriptions.Register(ctx, []byte(source))
	if err != nil {
		w.WriteError("ERR " + err.Error())
		return
	}
	defer reg.Cancel()

	// Main loop: try move, if empty wait for wake signal
	for {
		elem, errMsg, ok, emitErr := doLMove(cmd, source, destination, popLeft, pushLeft)
		if emitErr != nil {
			w.WriteError(emitErr.Error())
			return
		}
		if ok {
			reg.Cancel()

			w.WriteBulkString(elem)
			return
		} else if errMsg != "" {
			// Element was not consumed (e.g. WRONGTYPE on destination).
			// Re-notify so the next blocked client gets a chance.
			reg.CancelAndRenotify()
			w.WriteError(errMsg)
			return
		}

		// Wait for wake signal or timeout/cancel
		if _, err := reg.Wait(); err != nil {
			if conn.WasUnblockedWithError() {
				w.WriteError("UNBLOCKED client unblocked via CLIENT UNBLOCK")
			} else {
				w.WriteNullBulkString()
			}
			return
		}
	}
}

// handleBRPopLPush implements the deprecated BRPOPLPUSH command
// BRPOPLPUSH source destination timeout
func handleBRPopLPush(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
	// In script context, use non-blocking behavior (same as transactions)
	if conn.InScript {
		handleBRPopLPushNonBlocking(cmd, w, db)
		return
	}

	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("brpoplpush")
		return
	}

	source := string(cmd.Args[0])
	destination := string(cmd.Args[1])

	// Parse timeout
	timeout, errMsg := shared.ParseBlockingTimeout(string(cmd.Args[2]))
	if errMsg != "" {
		w.WriteError(errMsg)
		return
	}

	// BRPOPLPUSH = BLMOVE source destination RIGHT LEFT timeout
	// Setup blocking context with timeout and CLIENT UNBLOCK support
	ctx, cleanup := conn.Block(timeout)
	defer cleanup()

	// Register for wake signals on source key
	reg, err := db.Manager().Subscriptions.Register(ctx, []byte(source))
	if err != nil {
		w.WriteError("ERR " + err.Error())
		return
	}
	defer reg.Cancel()

	// Main loop: try move, if empty wait for wake signal
	for {
		elem, errMsg, ok, emitErr := doLMove(cmd, source, destination, false, true)
		if emitErr != nil {
			w.WriteError(emitErr.Error())
			return
		}
		if ok {
			reg.Cancel()

			w.WriteBulkString(elem)
			return
		} else if errMsg != "" {
			// Element was not consumed (e.g. WRONGTYPE on destination).
			// Re-notify so the next blocked client gets a chance.
			reg.CancelAndRenotify()
			w.WriteError(errMsg)
			return
		}

		// Wait for wake signal or timeout/cancel
		if _, err := reg.Wait(); err != nil {
			if conn.WasUnblockedWithError() {
				w.WriteError("UNBLOCKED client unblocked via CLIENT UNBLOCK")
			} else {
				w.WriteNullBulkString()
			}
			return
		}
	}
}
