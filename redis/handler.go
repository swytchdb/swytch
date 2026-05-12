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

package redis

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/swytchdb/swytch/cache"
	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/redis/pubsub"
	"github.com/swytchdb/swytch/redis/scripting"
	"github.com/swytchdb/swytch/redis/shared"
	"github.com/swytchdb/swytch/tracing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	_ "github.com/swytchdb/swytch/redis/bitmap" // Register bitmap module commands via init()
	_ "github.com/swytchdb/swytch/redis/geo"    // Register geo module commands via init()
	_ "github.com/swytchdb/swytch/redis/hash"   // Register hash module commands via init()
	_ "github.com/swytchdb/swytch/redis/hll"    // Register hll module commands via init()
	_ "github.com/swytchdb/swytch/redis/list"   // Register list module commands via init()
	_ "github.com/swytchdb/swytch/redis/set"    // Register set module commands via init()
	_ "github.com/swytchdb/swytch/redis/str"    // Register string/key/expiry module commands via init()
	_ "github.com/swytchdb/swytch/redis/stream" // Register stream module commands via init()
	_ "github.com/swytchdb/swytch/redis/zset"   // Register zset module commands via init()
)

const (
	// defaultMemoryLimit is the default memory limit (64MB)
	defaultMemoryLimit = 64 * 1024 * 1024
)

// HandlerConfig contains configuration for the handler
type HandlerConfig struct {
	NumDatabases  int
	CapacityPerDB int
	MemoryLimit   int64
	Password      string // Deprecated: use ACLFile instead
	RequireAuth   bool   // Deprecated: use ACLFile instead
	ACLFile       string // Path to ACL file for user authentication
	DebugLogging  bool
	// Engine, if set, enables effects-based data access for migrated modules
	Engine *effects.Engine
}

// DefaultHandlerConfig returns a default configuration
func DefaultHandlerConfig() HandlerConfig {
	return HandlerConfig{
		NumDatabases:  shared.DefaultNumDatabases,
		CapacityPerDB: 100000,
		MemoryLimit:   defaultMemoryLimit,
	}
}

// Handler processes Redis commands using CloxCache as the backend
type Handler struct {
	cmds shared.Registry // Command dispatch table

	dbManager *shared.DatabaseManager

	// Authentication
	password    string      // Deprecated: use aclManager
	requireAuth bool        // Deprecated: use aclManager
	aclManager  *ACLManager // ACL manager for user authentication and permissions

	// Stats tracks server-wide statistics
	stats *shared.Stats

	// Logger for debug logging
	logger *Logger

	// protoMaxBulkLen is the maximum size of a bulk string (default 512MB)
	protoMaxBulkLen int64

	// clientRegistry tracks all active connections for CLIENT LIST
	clientRegistry *ClientRegistry

	// scriptEngine holds the scripting subsystem
	scriptEngine *scripting.Engine

	// engine is the effects engine for migrated modules (nil if not configured)
	engine *effects.Engine
}

// NewHandler creates a new Redis command handler
func NewHandler(cfg HandlerConfig) *Handler {
	if cfg.NumDatabases <= 0 {
		cfg.NumDatabases = shared.DefaultNumDatabases
	}
	if cfg.CapacityPerDB <= 0 {
		cfg.CapacityPerDB = 100000
	}

	dbManager := shared.NewDatabaseManagerWithConfig(shared.DatabaseManagerConfig{
		NumDBs:        cfg.NumDatabases,
		TotalCapacity: cfg.CapacityPerDB,
		MemoryLimit:   cfg.MemoryLimit,
	})

	aclMgr := NewACLManager()

	// Configure ACL based on config
	if cfg.ACLFile != "" {
		// Load ACL from file
		if err := aclMgr.LoadFromFile(cfg.ACLFile); err != nil {
			// If file doesn't exist, that's OK - we'll use defaults
			// and the file will be created on ACL SAVE
			aclMgr.aclFile = cfg.ACLFile
		}
	} else if cfg.Password != "" {
		// Legacy mode: configure default user with password
		defaultUser := aclMgr.GetUser("default")
		if defaultUser != nil {
			hash, err := HashPassword(cfg.Password)
			if err != nil {
				// Log error but continue - server can still run, just without password auth
				// In practice, bcrypt failures are extremely rare (only on entropy exhaustion)
				panic(fmt.Sprintf("failed to hash password for default user: %v", err))
			}
			defaultUser.NoPass = false
			defaultUser.PasswordHashes = [][]byte{hash}
		}
	}

	h := &Handler{
		dbManager:       dbManager,
		password:        cfg.Password,
		requireAuth:     cfg.RequireAuth || cfg.Password != "" || cfg.ACLFile != "",
		aclManager:      aclMgr,
		logger:          NewLogger(cfg.DebugLogging),
		protoMaxBulkLen: 512 * 1024 * 1024,        // defaultProtoMaxBulkLen
		scriptEngine:    scripting.NewEngine(nil), // executor set below after h is created
		engine:          cfg.Engine,
	}

	// Set up the pub/sub broker via shared accessor
	shared.SetPubSubBroker(pubsub.NewManager())

	// Now set the executor that closes over h
	h.scriptEngine.SetExecutor(h.ExecuteInto)
	shared.SetScriptingEngine(h.scriptEngine)

	// Set package-level shared vars for module access
	shared.SetServerStats(h.stats)
	shared.SetProtoMaxBulkLen(h.protoMaxBulkLen)

	// Build command dispatch table
	h.initRegistry()

	// Register callback to notify blocking commands when keys change
	subs := dbManager.Subscriptions
	dbManager.SetOnChange(func(dbIndex int, key string, event shared.KeyEvent) {
		switch event {
		case shared.KeyEventSet:
			// Set: data added to list/zset/stream, or key type changed
			// Wake oldest waiter (normal data flow)
			subs.Enqueue([]byte(key), struct{}{})
		case shared.KeyEventDelete:
			// Delete: key removed, ALL blocked clients should wake up and return errors
			subs.NotifyAllWaiters([]byte(key))
		}
	})

	// Wire effects engine notification callbacks for blocking command wake-up
	if cfg.Engine != nil {
		cfg.Engine.OnKeyDataAdded = func(key string) {
			subs.Notify([]byte(key))
		}
		cfg.Engine.OnKeyDeleted = func(key string) {
			subs.NotifyAllWaiters([]byte(key))
		}
		cfg.Engine.OnFlushAll = func() {
			subs.NotifyAll()
		}
	}

	return h
}

// NewHandlerWithDefaults creates a handler with default configuration
// Close releases resources used by the handler
func (h *Handler) Close() {
	h.dbManager.Close()
	if h.scriptEngine != nil {
		h.scriptEngine.Close()
		shared.ClearScriptingEngine()
	}
}

// SetStats sets the shared stats tracker
func (h *Handler) SetStats(stats *shared.Stats) {
	h.stats = stats
	shared.SetServerStats(stats)
}

// SetClientRegistry sets the client registry for CLIENT LIST support
func (h *Handler) SetClientRegistry(registry *ClientRegistry) {
	h.clientRegistry = registry
}

// GetDatabaseManager returns the database manager for cluster handler setup.
func (h *Handler) GetDatabaseManager() *shared.DatabaseManager {
	return h.dbManager
}

// GetAdaptiveStats returns per-shard adaptive cache statistics
func (h *Handler) GetAdaptiveStats() []cache.AdaptiveStats {
	// Return stats from database 0 as representative
	if db := h.dbManager.GetDB(0); db != nil {
		return db.AdaptiveStats()
	}
	return nil
}

// GetCacheBytes returns current bytes used by cached items
func (h *Handler) GetCacheBytes() int64 {
	return h.dbManager.GetCacheBytes()
}

// GetItemCount returns current number of items in cache
func (h *Handler) GetItemCount() int {
	return h.dbManager.TotalItemCount()
}

// GetCacheEvictions returns total evictions from the cache
func (h *Handler) GetCacheEvictions() uint64 {
	return h.dbManager.TotalEvictions()
}

// RequiresAuth returns true if authentication is required
func (h *Handler) RequiresAuth() bool {
	return h.aclManager.RequiresAuth()
}

// NumDatabases returns the number of databases available
func (h *Handler) NumDatabases() int {
	return h.dbManager.NumDatabases()
}

// DebugEnabled returns true if debug logging is enabled
func (h *Handler) DebugEnabled() bool {
	return h.logger.IsDebug()
}

// ExecuteInto processes a command and writes the response to the writer
func (h *Handler) ExecuteInto(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	// Start OTel span for this command
	spanAttrs := []trace.SpanStartOption{
		trace.WithAttributes(attribute.String("redis.command", cmd.Type.String())),
	}
	if len(cmd.Args) > 0 {
		spanAttrs = append(spanAttrs, trace.WithAttributes(attribute.String("redis.key", string(cmd.Args[0]))))
	}
	ctx, span := tracing.Tracer().Start(conn.Ctx, "redis.command", spanAttrs...)
	defer span.End()
	if conn.EffectsCtx != nil {
		conn.EffectsCtx.SetTraceCtx(ctx)
	}

	// Log command if debug mode is enabled
	h.logger.LogCommand(cmd, conn.SelectedDB, conn.RemoteAddr)

	// Log response when done
	defer h.logger.LogResponse(conn.RemoteAddr, w.Buffer())

	// Buffer the response so it only becomes visible after effects are flushed.
	// This prevents clients from seeing stale state when racing against chained
	// notifications (e.g. Circular BRPOPLPUSH).
	origBuf := w.Buffer()
	var tmpBuf bytes.Buffer
	w.Reset(&tmpBuf)
	defer func() {
		origBuf.Write(tmpBuf.Bytes())
		w.Reset(origBuf)
	}()

	// Track command stats
	cmdStart := time.Now()
	w.ResetErrorFlag()
	defer func() {
		if h.stats != nil && cmd.Type != shared.CmdUnknown {
			usec := uint64(time.Since(cmdStart).Microseconds())
			h.stats.RecordCommand(strings.ToLower(cmd.Type.String()), usec, w.WroteError())
		}
	}()

	entry := h.cmds.Lookup(cmd.Type)

	// Check authentication — NoAuth commands bypass this check
	if conn.User == nil {
		noAuth := entry != nil && entry.Flags&shared.FlagNoAuth != 0
		if !noAuth {
			if defaultUser, ok := h.aclManager.AuthenticateDefault(); ok {
				conn.User = defaultUser
				conn.Username = defaultUser.Name
			} else {
				w.WriteNoAuth()
				return
			}
		}
	}

	// Check ACL command permission (skip for AUTH and QUIT)
	if conn.User != nil && cmd.Type != shared.CmdAuth && cmd.Type != shared.CmdQuit {
		if err := h.aclManager.CheckCommand(conn.User, cmd.Type); err != nil {
			h.aclManager.LogDenied("command", "toplevel", cmd.Type.String(), conn.Username, conn.RemoteAddr)
			w.WriteError(fmt.Sprintf("NOPERM this user has no permissions to run the '%s' command", cmd.Type.String()))
			return
		}
	}

	// In pub/sub mode (RESP2 only), only allow specific commands
	if conn.InPubSubMode() && conn.Protocol != shared.RESP3 {
		allowed := entry != nil && entry.Flags&shared.FlagPubSub != 0
		if !allowed {
			w.WriteError("ERR only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context")
			return
		}
	}

	// Check if a script is running and has timed out (BUSY state)
	if h.scriptEngine.IsScriptTimedOut() {
		switch cmd.Type {
		case shared.CmdScript, shared.CmdFunction:
			if len(cmd.Args) > 0 {
				sub := shared.ToUpper(cmd.Args[0])
				if string(sub) == "KILL" {
					break
				}
			}
			fallthrough
		case shared.CmdShutdown:
			// SHUTDOWN is always allowed
		case shared.CmdMulti, shared.CmdExec, shared.CmdDiscard:
			// Transaction commands are allowed through
		default:
			w.WriteError("BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.")
			if conn.InTransaction {
				conn.TxnAborted = true
			}
			return
		}
	}

	// Get the selected database
	db := h.dbManager.GetDB(conn.SelectedDB)
	if db == nil {
		w.WriteError("ERR invalid DB index")
		return
	}

	// Populate effects engine fields if available (must happen before dispatch and queueing)
	if h.engine != nil && conn.EffectsCtx != nil {
		cmd.Runtime = h.engine
		cmd.Context = conn.EffectsCtx
	}

	// If in transaction mode, queue commands instead of executing
	if conn.InTransaction {
		noQueue := entry != nil && entry.Flags&shared.FlagNoQueue != 0
		if !noQueue {
			if cmd.Type == shared.CmdUnknown {
				conn.TxnAborted = true
				h.logger.LogUnknownCommand(cmd, conn.RemoteAddr)
				if len(cmd.Args) > 0 {
					w.WriteUnknownCommand(cmd.Type.String(), cmd.Args)
				} else {
					w.WriteError("ERR unknown command")
				}
				return
			}
			h.queueCommand(cmd, w, conn, db)
			return
		}
	}

	// Dispatch via registry
	if entry == nil {
		h.logger.LogUnknownCommand(cmd, conn.RemoteAddr)
		if len(cmd.Args) > 0 {
			w.WriteUnknownCommand(cmd.Type.String(), cmd.Args)
		} else {
			w.WriteError("ERR unknown command")
		}
		return
	}

	// ConnHandler: command handles everything internally (blocking, auth, pubsub, scripting)
	if entry.ConnHandler != nil {
		// Per-key ACL check for ConnHandler commands that touch data keys
		if entry.Keys != nil && conn.User != nil {
			isWrite := entry.Flags&shared.FlagWrite != 0
			for _, key := range entry.Keys(cmd) {
				if err := h.aclManager.CheckKey(conn.User, key, isWrite); err != nil {
					h.aclManager.LogDenied("key", key, cmd.Type.String(), conn.Username, conn.RemoteAddr)
					w.WriteError(fmt.Sprintf("NOPERM this user has no permissions to access key '%s'", key))
					return
				}
			}
		}
		const maxTxnRetries = 10
		for attempt := 0; ; attempt++ {
			savedLen := w.Buffer().Len()
			entry.ConnHandler(cmd, w, db, conn)
			if cmd.Context != nil && !conn.InTransaction {
				err := cmd.Context.Flush()
				if errors.Is(err, effects.ErrTxnAborted) && attempt < maxTxnRetries {
					// Only retry implicit transactions (single-command BeginTx like INCR).
					// EXEC consumes and destroys the MULTI state — retrying would
					// hit "ERR EXEC without MULTI" and leak partially-applied effects.
					if cmd.Type == shared.CmdExec {
						w.Buffer().Truncate(savedLen)
						w.WriteNullArray() // abort: client sees nil (same as WATCH conflict)
						break
					}
					w.Buffer().Truncate(savedLen)
					continue
				}
				if err != nil {
					slog.Error("effects flush failed", "error", err)
					w.Buffer().Truncate(savedLen)
					w.WriteError("TRYAGAIN " + err.Error())
				}
			}
			break
		}
		return
	}

	// Standard handler: validate → ACL key check → execute
	var trackStart time.Time
	if entry.Flags&(shared.FlagTrackGet|shared.FlagTrackSet) != 0 {
		trackStart = time.Now()
	}

	var valid bool
	var keys []string
	var runner shared.CommandRunner
	func() {
		_, prepSpan := tracing.Tracer().Start(ctx, "handler.prepare")
		defer prepSpan.End()
		valid, keys, runner = entry.Handler(cmd, w, db)
	}()

	// Per-key ACL check
	if valid && len(keys) > 0 && conn.User != nil {
		isWrite := entry.Flags&shared.FlagWrite != 0
		for _, key := range keys {
			if err := h.aclManager.CheckKey(conn.User, key, isWrite); err != nil {
				h.aclManager.LogDenied("key", key, cmd.Type.String(), conn.Username, conn.RemoteAddr)
				w.WriteError(fmt.Sprintf("NOPERM this user has no permissions to access key '%s'", key))
				return
			}
		}
	}

	// Block writes to internal __swytch:* keys — these are reserved for
	// cluster membership and other internal effects.
	if valid && entry.Flags&shared.FlagWrite != 0 && len(keys) > 0 {
		for _, key := range keys {
			if strings.HasPrefix(key, "__swytch:") {
				w.WriteError("ERR cannot write to internal key '" + key + "'")
				return
			}
		}
	}

	// Adaptive serialization: forward write commands to the serialization leader
	if valid && entry.Flags&shared.FlagWrite != 0 && cmd.Runtime != nil && entry.Keys != nil {
		_, fwdSpan := tracing.Tracer().Start(ctx, "handler.forward_check")
		fwdKeys := entry.Keys(cmd)
		if len(fwdKeys) > 0 {
			if result := cmd.Runtime.Forward(cmd.Type.String(), cmd.Args, fwdKeys, conn.Username); result != nil {
				w.WriteRaw(result)
				fwdSpan.End()
				return
			}
		}
		fwdSpan.End()
	}

	if valid {
		// Per-key striped lock to serialize local access and prevent
		// transaction abort storms on hot keys.
		var keyLock *sync.Mutex
		if cmd.Runtime != nil && entry.Keys != nil && !conn.InTransaction {
			_, lockSpan := tracing.Tracer().Start(ctx, "handler.key_lock")
			if fwdKeys := entry.Keys(cmd); len(fwdKeys) > 0 {
				keyLock = cmd.Runtime.GetLock(fwdKeys[0])
				keyLock.Lock()
			}
			lockSpan.End()
		}

		_, runSpan := tracing.Tracer().Start(ctx, "handler.run")
		const maxTxnRetries = 10
		for attempt := 0; ; attempt++ {
			savedLen := w.Buffer().Len()
			runner()
			if cmd.Context != nil && !conn.InTransaction {
				err := cmd.Context.Flush()
				if errors.Is(err, effects.ErrTxnAborted) && attempt < maxTxnRetries {
					w.Buffer().Truncate(savedLen)
					continue
				}
				if err != nil {
					slog.Error("effects flush failed", "error", err)
					w.Buffer().Truncate(savedLen)
					w.WriteError("TRYAGAIN " + err.Error())
				}
			}
			break
		}
		runSpan.End()

		if keyLock != nil {
			keyLock.Unlock()
		}
	}

	// Latency tracking
	if entry.Flags&shared.FlagTrackGet != 0 && h.stats != nil {
		h.stats.RecordGetLatency(time.Since(trackStart))
	}
	if entry.Flags&shared.FlagTrackSet != 0 && h.stats != nil {
		h.stats.RecordSetLatency(time.Since(trackStart))
	}
}

// Connection command handlers

func (h *Handler) handlePing(cmd *shared.Command, w *shared.Writer) {
	if len(cmd.Args) == 0 {
		w.WritePong()
	} else if len(cmd.Args) == 1 {
		w.WriteBulkString(cmd.Args[0])
	} else {
		w.WriteWrongNumArguments("ping")
	}
}

// handlePingPubSub handles PING in pub/sub mode
func (h *Handler) handlePingPubSub(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	// RESP3: PING in pub/sub returns simple PONG (or bulk with argument)
	// RESP2: PING in pub/sub returns ["pong", <argument>] array format
	if conn.Protocol == shared.RESP3 {
		// RESP3 pub/sub: same as regular PING
		if len(cmd.Args) == 0 {
			w.WritePong()
		} else if len(cmd.Args) == 1 {
			w.WriteBulkString(cmd.Args[0])
		} else {
			w.WriteWrongNumArguments("ping")
		}
	} else {
		// RESP2 pub/sub: returns ["pong", <argument>] array
		w.WriteArray(2)
		w.WriteBulkStringStr("pong")
		if len(cmd.Args) > 0 {
			w.WriteBulkString(cmd.Args[0])
		} else {
			w.WriteBulkStringStr("")
		}
	}
}

func (h *Handler) handleEcho(cmd *shared.Command, w *shared.Writer) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("echo")
		return
	}
	w.WriteBulkString(cmd.Args[0])
}

func (h *Handler) handleAuth(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	var username, password string

	switch len(cmd.Args) {
	case 1:
		// AUTH password (use default user)
		username = "default"
		password = string(cmd.Args[0])
	case 2:
		// AUTH username password
		username = string(cmd.Args[0])
		password = string(cmd.Args[1])
	default:
		w.WriteWrongNumArguments("auth")
		return
	}

	user, err := h.aclManager.Authenticate(username, password)
	if err != nil {
		conn.User = nil
		conn.Username = ""
		h.aclManager.LogDenied("auth", "toplevel", "", username, conn.RemoteAddr)
		w.WriteError("WRONGPASS invalid username-password pair or user is disabled")
		return
	}

	conn.User = user
	conn.Username = user.Name
	w.WriteOK()
}

func (h *Handler) handleSelect(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("select")
		return
	}

	index, ok := shared.ParseInt64(cmd.Args[0])
	if !ok || index < 0 || index >= int64(h.dbManager.NumDatabases()) {
		w.WriteError("ERR invalid DB index")
		return
	}

	// In effects mode, all data lives in database 0 for replication.
	// Accept the SELECT but pin to db 0.
	if effectsSelectOverride() {
		w.WriteOK()
		return
	}

	conn.SelectedDB = int(index)
	w.WriteOK()
}

func (h *Handler) handleHello(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	// HELLO [protover [AUTH username password] [SETNAME clientname]]

	requestedVersion := shared.ProtocolVersion(2) // Default to RESP2

	// Parse arguments
	i := 0
	if len(cmd.Args) > 0 {
		ver, ok := shared.ParseInt64(cmd.Args[0])
		if !ok || (ver != 2 && ver != 3) {
			w.WriteError("NOPROTO unsupported protocol version")
			return
		}
		requestedVersion = shared.ProtocolVersion(ver)
		i = 1
	}

	// Parse optional AUTH and SETNAME
	for i < len(cmd.Args) {
		arg := shared.ToUpper(cmd.Args[i])
		switch string(arg) {
		case "AUTH":
			if i+2 >= len(cmd.Args) {
				w.WriteWrongNumArguments("hello")
				return
			}
			// AUTH username password
			username := string(cmd.Args[i+1])
			password := string(cmd.Args[i+2])
			user, err := h.aclManager.Authenticate(username, password)
			if err != nil {
				conn.User = nil
				conn.Username = ""
				h.aclManager.LogDenied("auth", "toplevel", "", username, conn.RemoteAddr)
				w.WriteError("WRONGPASS invalid username-password pair or user is disabled")
				return
			}
			conn.User = user
			conn.Username = user.Name
			i += 3
		case "SETNAME":
			if i+1 >= len(cmd.Args) {
				w.WriteWrongNumArguments("hello")
				return
			}
			conn.ClientName = string(cmd.Args[i+1])
			i += 2
		default:
			i++
		}
	}

	// If no AUTH was provided and user is nil, try default user
	if conn.User == nil {
		if defaultUser, ok := h.aclManager.AuthenticateDefault(); ok {
			conn.User = defaultUser
			conn.Username = defaultUser.Name
		}
	}

	// Switch protocol version
	conn.Protocol = requestedVersion
	w.SetProtocol(requestedVersion)

	// Return server info as a map (RESP3) or flat array (RESP2)
	w.WriteMap(7) // server, version, proto, id, mode, role, modules

	w.WriteBulkStringStr("server")
	w.WriteBulkStringStr("swytch")

	w.WriteBulkStringStr("version")
	w.WriteBulkStringStr("8.4.0")

	w.WriteBulkStringStr("proto")
	w.WriteInteger(int64(conn.Protocol))

	w.WriteBulkStringStr("id")
	w.WriteInteger(1) // Connection ID (simplified)

	w.WriteBulkStringStr("mode")
	w.WriteBulkStringStr("standalone")

	w.WriteBulkStringStr("role")
	w.WriteBulkStringStr("master")

	w.WriteBulkStringStr("modules")
	w.WriteArray(0) // No modules
}

// Transaction command handlers

// queueCommand adds a command to the transaction queue using the registry.
func (h *Handler) queueCommand(cmd *shared.Command, w *shared.Writer, conn *shared.Connection, db *shared.Database) {
	entry := h.cmds.Lookup(cmd.Type)
	if entry == nil {
		w.WriteError("ERR unknown command '" + cmd.Type.String() + "'")
		conn.TxnAborted = true
		return
	}

	// Copy the command args since they may be pooled and the original
	// cmd will be returned to the pool after ExecuteInto returns.
	argsCopy := make([][]byte, len(cmd.Args))
	for i, arg := range cmd.Args {
		argsCopy[i] = make([]byte, len(arg))
		copy(argsCopy[i], arg)
	}

	// Create a stable copy so runner closures don't reference a recycled cmd.
	cmdCopy := &shared.Command{
		Type:    cmd.Type,
		Args:    argsCopy,
		Context: cmd.Context,
		Runtime: cmd.Runtime,
	}

	// Use TxnPrepare if available (blocking->non-blocking fallbacks, conn-needing commands),
	// otherwise fall back to Handler.
	var valid bool
	var keys []string
	var runner shared.CommandRunner

	if entry.TxnPrepare != nil {
		valid, keys, runner = entry.TxnPrepare(cmdCopy, w, db, conn)
	} else if entry.Handler != nil {
		valid, keys, runner = entry.Handler(cmdCopy, w, db)
	} else {
		// ConnHandler-only commands without TxnPrepare cannot be queued
		w.WriteError("ERR command '" + cmd.Type.String() + "' cannot be used inside a transaction")
		conn.TxnAborted = true
		return
	}

	if !valid {
		conn.TxnAborted = true
		return
	}

	conn.QueuedCmds = append(conn.QueuedCmds, shared.QueuedCommand{
		Type:   cmdCopy.Type,
		Args:   argsCopy,
		Keys:   keys,
		Runner: runner,
	})

	w.WriteQueued()
}

// handleMulti starts a transaction
func (h *Handler) handleMulti(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	if len(cmd.Args) != 0 {
		w.WriteWrongNumArguments("multi")
		return
	}

	if conn.InTransaction {
		w.WriteError("ERR MULTI calls can not be nested")
		return
	}

	conn.InTransaction = true
	conn.TxnAborted = false
	w.WriteOK()
}

// handleDiscard aborts a transaction
func (h *Handler) handleDiscard(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	if len(cmd.Args) != 0 {
		w.WriteWrongNumArguments("discard")
		return
	}

	if !conn.InTransaction {
		w.WriteError("ERR DISCARD without MULTI")
		return
	}

	conn.ResetTransaction()
	cmd.Context.ClearWatches()
	w.WriteOK()
}

// handleWatch watches keys for modifications
func (h *Handler) handleWatch(cmd *shared.Command, w *shared.Writer, conn *shared.Connection, db *shared.Database) {
	if len(cmd.Args) == 0 {
		w.WriteWrongNumArguments("watch")
		return
	}

	if conn.InTransaction {
		w.WriteError("ERR WATCH inside MULTI is not allowed")
		return
	}

	// Register each key on the effects context for optimistic locking.
	// BeginTx (called at EXEC time) will emit NOOPs to establish causal deps.
	for _, keyArg := range cmd.Args {
		cmd.Context.Watch(string(keyArg))
	}

	w.WriteOK()
}

// handleUnwatch clears all watched keys
func (h *Handler) handleUnwatch(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	if len(cmd.Args) != 0 {
		w.WriteWrongNumArguments("unwatch")
		return
	}

	cmd.Context.ClearWatches()
	w.WriteOK()
}

// handleExec executes a transaction
func (h *Handler) handleExec(cmd *shared.Command, w *shared.Writer, conn *shared.Connection, db *shared.Database) {
	if len(cmd.Args) != 0 {
		w.WriteWrongNumArguments("exec")
		return
	}

	if !conn.InTransaction {
		w.WriteError("ERR EXEC without MULTI")
		return
	}

	// Check if transaction was aborted due to command errors during queueing
	if conn.TxnAborted {
		conn.ResetTransaction()
		cmd.Context.ClearWatches()
		// Check if the abort was due to BUSY (script timeout)
		if h.scriptEngine.IsScriptTimedOut() {
			w.WriteError("EXECABORT Transaction discarded because of previous errors. BUSY Redis is busy running a script.")
		} else {
			w.WriteError("EXECABORT Transaction discarded because of previous errors.")
		}
		return
	}

	// Check if there's a timed-out script running - EXEC must fail with BUSY
	if h.scriptEngine.IsScriptTimedOut() {
		conn.ResetTransaction()
		cmd.Context.ClearWatches()
		w.WriteError("EXECABORT Transaction discarded because of: BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.")
		return
	}

	// Adaptive serialization: forward the entire EXEC to the leader if any
	// queued command touches a serialized key.
	if cmd.Runtime != nil {
		fwdCmds := make([]effects.ForwardCommand, len(conn.QueuedCmds))
		for i, qcmd := range conn.QueuedCmds {
			fwdCmds[i] = effects.ForwardCommand{
				Name: qcmd.Type.String(),
				Args: qcmd.Args,
				Keys: qcmd.Keys,
			}
		}
		var watchKeys []string
		for _, wk := range conn.WatchedKeys {
			watchKeys = append(watchKeys, wk.Key)
		}
		if result := cmd.Runtime.ForwardExec(fwdCmds, watchKeys, conn.Username); result != nil {
			conn.ResetTransaction()
			cmd.Context.ClearWatches()
			w.WriteRaw(result)
			return
		}
	}

	// Begin a transaction on the effects context, then validate any
	// WATCH observations. CheckWatches must be called after BeginTx so
	// the transactional NOOPs get IsTransactional=true in the Bind.
	cmd.Context.BeginTx()
	if !cmd.Context.CheckWatches() {
		conn.ResetTransaction()
		w.WriteNullArray()
		return
	}

	// Execute all queued commands
	numCmds := len(conn.QueuedCmds)
	w.WriteArray(numCmds)

	for _, qcmd := range conn.QueuedCmds {
		if qcmd.Runner != nil {
			qcmd.Runner()
		} else {
			w.WriteError("ERR internal error: no runner for command")
		}
	}

	// Clear transaction state (but NOT the effects context — Flush
	// happens in the handler loop after we return)
	conn.ResetTransaction()
}

// initRegistry builds the command dispatch table by calling per-file registration functions.
func (h *Handler) initRegistry() {
	h.registerConnectionCommands()
	h.registerTransactionCommands()
	h.registerSortCommands()
	h.registerServerCommands()
	h.registerAclCommands()
	h.registerReplicationStubs()

	// Merge module-registered commands (e.g., zset)
	for _, me := range shared.GetModuleRegistrations() {
		h.cmds.Register(me.Cmd, me.Entry)
	}
}

// registerConnectionCommands registers PING, ECHO, AUTH, SELECT, QUIT, HELLO.
func (h *Handler) registerConnectionCommands() {
	h.cmds.Register(shared.CmdPing, &shared.CommandEntry{
		ConnHandler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, conn *shared.Connection) {
			if conn.InPubSubMode() {
				h.handlePingPubSub(cmd, w, conn)
			} else {
				h.handlePing(cmd, w)
			}
		},
		TxnPrepare: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, _ *shared.Connection) (bool, []string, shared.CommandRunner) {
			return true, nil, func() { h.handlePing(cmd, w) }
		},
		Flags: shared.FlagPubSub,
	})
	h.cmds.Register(shared.CmdEcho, &shared.CommandEntry{
		Handler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database) (bool, []string, shared.CommandRunner) {
			return true, nil, func() { h.handleEcho(cmd, w) }
		},
	})
	h.cmds.Register(shared.CmdAuth, &shared.CommandEntry{
		ConnHandler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, conn *shared.Connection) {
			h.handleAuth(cmd, w, conn)
		},
		Flags: shared.FlagNoAuth,
	})
	h.cmds.Register(shared.CmdSelect, &shared.CommandEntry{
		ConnHandler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, conn *shared.Connection) {
			h.handleSelect(cmd, w, conn)
		},
		TxnPrepare: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, conn *shared.Connection) (bool, []string, shared.CommandRunner) {
			return true, nil, func() { h.handleSelect(cmd, w, conn) }
		},
	})
	h.cmds.Register(shared.CmdQuit, &shared.CommandEntry{
		ConnHandler: func(_ *shared.Command, _ *shared.Writer, _ *shared.Database, _ *shared.Connection) {},
		Flags:       shared.FlagNoAuth | shared.FlagNoQueue | shared.FlagPubSub,
	})
	h.cmds.Register(shared.CmdHello, &shared.CommandEntry{
		ConnHandler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, conn *shared.Connection) {
			h.handleHello(cmd, w, conn)
		},
		Flags: shared.FlagNoAuth,
	})
	h.cmds.Register(shared.CmdNoop, &shared.CommandEntry{
		ConnHandler: func(_ *shared.Command, _ *shared.Writer, _ *shared.Database, _ *shared.Connection) {},
	})
}

// registerTransactionCommands registers MULTI, EXEC, DISCARD, WATCH, UNWATCH.
func (h *Handler) registerTransactionCommands() {
	h.cmds.Register(shared.CmdMulti, &shared.CommandEntry{
		ConnHandler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, conn *shared.Connection) {
			h.handleMulti(cmd, w, conn)
		},
		Flags: shared.FlagNoQueue,
	})
	h.cmds.Register(shared.CmdExec, &shared.CommandEntry{
		ConnHandler: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
			h.handleExec(cmd, w, conn, db)
		},
		Flags: shared.FlagNoQueue,
	})
	h.cmds.Register(shared.CmdDiscard, &shared.CommandEntry{
		ConnHandler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, conn *shared.Connection) {
			h.handleDiscard(cmd, w, conn)
		},
		Flags: shared.FlagNoQueue,
	})
	h.cmds.Register(shared.CmdWatch, &shared.CommandEntry{
		ConnHandler: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, conn *shared.Connection) {
			h.handleWatch(cmd, w, conn, db)
		},
		Flags: shared.FlagNoQueue,
	})
	h.cmds.Register(shared.CmdUnwatch, &shared.CommandEntry{
		ConnHandler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, conn *shared.Connection) {
			h.handleUnwatch(cmd, w, conn)
		},
		Flags: shared.FlagNoQueue,
	})
}

// HandleForwardedTransaction implements cluster.ForwardHandler.
// Executes forwarded commands locally and returns raw RESP bytes.
func (h *Handler) HandleForwardedTransaction(tx *pb.ForwardedTransaction) *pb.ForwardedResponse {
	if len(tx.Commands) == 0 {
		return &pb.ForwardedResponse{Error: true, ErrorMessage: "empty transaction"}
	}

	db := h.dbManager.GetDB(0)
	if db == nil {
		return &pb.ForwardedResponse{Error: true, ErrorMessage: "database unavailable"}
	}

	// Create a synthetic connection with InScript=true (non-blocking, no pubsub)
	conn := &shared.Connection{
		InScript: true,
	}
	if h.engine != nil {
		conn.EffectsCtx = h.engine.NewContext()
	}

	// Apply the authorization context from the origin node
	username := tx.GetAuthorizedUser()
	if username == "" {
		username = "default"
	}
	user := h.aclManager.GetUser(username)
	if user == nil {
		return &pb.ForwardedResponse{Error: true, ErrorMessage: "NOPERM unknown user '" + username + "'"}
	}
	if !user.Enabled {
		return &pb.ForwardedResponse{Error: true, ErrorMessage: "NOPERM user '" + username + "' is disabled"}
	}
	conn.User = user
	conn.Username = username

	var respBuf bytes.Buffer
	w := shared.NewWriter(&respBuf)

	if len(tx.Commands) == 1 {
		// Single command: execute directly via ExecuteInto
		fc := tx.Commands[0]
		cmdType := shared.ParseCommandType(fc.CommandName)
		cmd := &shared.Command{
			Type: cmdType,
			Args: fc.Args,
		}
		h.ExecuteInto(cmd, w, conn)
	} else {
		// Multi-command transaction: queue all commands, then execute via handleExec
		conn.InTransaction = true

		// Queue each command
		for _, fc := range tx.Commands {
			cmdType := shared.ParseCommandType(fc.CommandName)
			cmd := &shared.Command{
				Type: cmdType,
				Args: fc.Args,
			}
			if h.engine != nil && conn.EffectsCtx != nil {
				cmd.Runtime = h.engine
				cmd.Context = conn.EffectsCtx
			}

			// Use a discard writer for queuing (QUEUED responses are not forwarded)
			var discardBuf bytes.Buffer
			discardW := shared.NewWriter(&discardBuf)
			h.queueCommand(cmd, discardW, conn, db)
		}

		// Set up WATCH keys if present
		if conn.EffectsCtx != nil {
			for _, wk := range tx.WatchedKeys {
				conn.EffectsCtx.Watch(string(wk))
			}
		}

		// Execute the transaction
		execCmd := &shared.Command{Type: shared.CmdExec}
		if h.engine != nil && conn.EffectsCtx != nil {
			execCmd.Runtime = h.engine
			execCmd.Context = conn.EffectsCtx
		}
		h.handleExec(execCmd, w, conn, db)

		// Flush effects
		if conn.EffectsCtx != nil {
			if err := conn.EffectsCtx.Flush(); err != nil {
				slog.Debug("forwarded transaction flush failed", "error", err)
			}
		}
	}

	return &pb.ForwardedResponse{RespData: respBuf.Bytes()}
}
