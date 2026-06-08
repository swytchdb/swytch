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

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/swytchdb/swytch/cache"
	"github.com/swytchdb/swytch/effects"
)

// ProtocolVersion represents the RESP protocol version
type ProtocolVersion int

const (
	RESP2 ProtocolVersion = 2
	RESP3 ProtocolVersion = 3
)

// Connection represents the state of a client connection
type Connection struct {
	SelectedDB    int             // Currently selected database index
	User          *ACLUser        // nil = unauthenticated
	Username      string          // Cached username for logging
	Protocol      ProtocolVersion // RESP protocol version (default RESP2)
	ClientName    string          // Client name set via CLIENT SETNAME or HELLO
	ClientLibName string          // Client library name (from HELLO)
	ClientLibVer  string          // Client library version (from HELLO)
	RemoteAddr    string          // Client remote address for logging

	// Transaction state
	InTransaction bool            // True if inside MULTI block
	TxnAborted    bool            // True if watched key was modified
	QueuedCmds    []QueuedCommand // Commands queued for EXEC
	WatchedKeys   []WatchedKey    // Keys being watched with their versions

	// Script state
	InScript bool // True if executing commands from a Lua script (blocking commands become non-blocking)

	// SI Transaction state (for CRDT handler with Snapshot Isolation)
	SITransaction   any      // Active SI transaction (nil if not in SI mode)
	BufferedResults [][]byte // Results captured during non-SI MULTI for EXEC

	// Blocking command support
	Ctx           context.Context // Context for blocking operations (cancellation on disconnect)
	Stats         *Stats          // Server stats (set at connection creation)
	blocked       atomic.Bool
	unblockMu     sync.Mutex
	unblockCancel context.CancelFunc
	unblockError  bool

	// Pub/Sub state
	PubSubClient *PubSubClient // nil if not in pubsub mode

	// Effects engine context for migrated modules
	EffectsCtx *effects.Context

	// ReadOnlyCtx serves read-only, non-transactional commands. It answers
	// read-misses from the cluster key filters without subscribing (free
	// misses). Lazily created alongside EffectsCtx.
	ReadOnlyCtx *effects.Context
}

// QueuedCommand stores a command queued during a transaction
type QueuedCommand struct {
	Type   CommandType
	Args   [][]byte
	Keys   []string      // Keys extracted during validation (for ACL checking at EXEC)
	Runner CommandRunner // Pre-validated runner to execute at EXEC time
}

// WatchedKey stores a key being watched with its version
type WatchedKey struct {
	DB      int // Database index
	Key     string
	Version int64 // CreatedAt timestamp when watched (0 if key didn't exist)
}

// ResetTransaction clears transaction state
func (c *Connection) ResetTransaction() {
	c.InTransaction = false
	c.TxnAborted = false
	c.QueuedCmds = c.QueuedCmds[:0]
	c.WatchedKeys = c.WatchedKeys[:0]
	c.SITransaction = nil
	c.BufferedResults = c.BufferedResults[:0]
}

// ClearWatches clears watched keys without affecting transaction state
func (c *Connection) ClearWatches() {
	c.WatchedKeys = c.WatchedKeys[:0]
}

// IsRESP3 returns true if the connection is using RESP3
func (c *Connection) IsRESP3() bool {
	return c.Protocol == RESP3
}

// InPubSubMode returns true if the connection is in pub/sub mode
func (c *Connection) InPubSubMode() bool {
	return c.PubSubClient != nil
}

// Block sets up a blocking context with an optional timeout.
// Returns (ctx, cleanup) — caller MUST defer cleanup() immediately.
func (c *Connection) Block(timeout float64) (context.Context, func()) {
	ctx := c.Ctx

	var cancels []context.CancelFunc

	// Add timeout if specified
	if timeout > 0 {
		var timeoutCancel context.CancelFunc
		ctx, timeoutCancel = context.WithTimeout(ctx, time.Duration(timeout*float64(time.Second)))
		cancels = append(cancels, timeoutCancel)
	}

	// Add cancellable context for CLIENT UNBLOCK support
	var unblockCancel context.CancelFunc
	ctx, unblockCancel = context.WithCancel(ctx)
	cancels = append(cancels, unblockCancel)

	// Track blocked state — set unblockCancel under the mutex BEFORE
	// marking blocked, so a concurrent Unblock() never sees blocked==true
	// with unblockCancel==nil.
	c.unblockMu.Lock()
	c.unblockCancel = unblockCancel
	c.unblockError = false
	c.unblockMu.Unlock()
	c.blocked.Store(true)

	// Track in stats
	if c.Stats != nil {
		c.Stats.ClientBlocked()
	}

	cleanup := func() {
		// Cancel contexts in reverse order
		for i := len(cancels) - 1; i >= 0; i-- {
			cancels[i]()
		}
		// Clear blocking state — set blocked to false BEFORE clearing
		// unblockCancel, so Unblock() never sees blocked==true with
		// unblockCancel==nil during cleanup.
		c.blocked.Store(false)
		c.unblockMu.Lock()
		c.unblockCancel = nil
		c.unblockMu.Unlock()

		// Track in stats
		if c.Stats != nil {
			c.Stats.ClientUnblocked()
		}
	}

	return ctx, cleanup
}

// IsBlocked returns true if the connection is currently blocked.
func (c *Connection) IsBlocked() bool {
	return c.blocked.Load()
}

// Unblock cancels a blocked operation. If withError is true, the client
// receives an error message instead of nil.
func (c *Connection) Unblock(withError bool) bool {
	if !c.blocked.Load() {
		return false
	}

	c.unblockMu.Lock()
	cancel := c.unblockCancel
	if cancel != nil {
		c.unblockError = withError
	}
	c.unblockMu.Unlock()

	if cancel != nil {
		cancel()
		return true
	}
	return false
}

// WasUnblockedWithError returns true if CLIENT UNBLOCK ERROR was used.
func (c *Connection) WasUnblockedWithError() bool {
	c.unblockMu.Lock()
	defer c.unblockMu.Unlock()
	return c.unblockError
}

// CommandHandler defines the interface for Redis command handlers.
// Implementations can use different storage backends:
//   - Handler: in-memory CloxCache (fast, volatile)
//   - CRDTHandler: persistent CRDT data plane (durable, replicated) [future]
type CommandHandler interface {
	// ExecuteInto processes a command and writes the response to the writer
	ExecuteInto(cmd *Command, w *Writer, conn *Connection)

	// Close releases resources used by the handler
	Close()

	// SetStats sets the shared stats tracker
	SetStats(stats *Stats)

	// GetAdaptiveStats returns per-shard adaptive cache statistics
	GetAdaptiveStats() []cache.AdaptiveStats

	// GetCacheBytes returns current bytes used by cached items
	GetCacheBytes() int64

	// GetItemCount returns current number of items in cache
	GetItemCount() int

	// GetArenaBytes returns the critbit index's slot-array footprint (trie
	// skeleton), distinct from GetCacheBytes (vertex pool effect bytes)
	GetArenaBytes() int64

	// GetVertexCount returns the number of effects resident in the vertex pool
	GetVertexCount() int

	// GetCacheEvictions returns keys evicted under memory pressure (evicted_keys)
	GetCacheEvictions() uint64

	// GetVerticesReclaimed returns vertices freed by reclaim (storage churn)
	GetVerticesReclaimed() uint64

	// RequiresAuth returns true if authentication is required
	RequiresAuth() bool

	// NumDatabases returns the number of databases available
	NumDatabases() int

	// DebugEnabled returns true if debug logging is enabled
	DebugEnabled() bool
}
