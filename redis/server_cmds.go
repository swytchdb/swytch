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
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

// Server command implementations

func (h *Handler) handleInfo(cmd *shared.Command, w *shared.Writer) {
	// Build info string
	info := h.buildInfoString(cmd.Args)
	w.WriteBulkString([]byte(info))
}

func (h *Handler) buildInfoString(args [][]byte) string {
	// Determine which sections to include
	var section string
	if len(args) > 0 {
		section = string(shared.ToUpper(args[0]))
	}

	var info strings.Builder

	// Server section
	if section == "" || section == "SERVER" || section == "ALL" {
		info.WriteString("# Server\r\n")
		info.WriteString("redis_version:8.4.0-swytch\r\n")
		info.WriteString("redis_git_sha1:00000000\r\n")
		info.WriteString("redis_git_dirty:0\r\n")
		info.WriteString("redis_build_id:swytch\r\n")
		info.WriteString("redis_mode:standalone\r\n")
		fmt.Fprintf(&info, "os:%s %s\r\n", runtime.GOOS, runtime.GOARCH)
		fmt.Fprintf(&info, "arch_bits:%d\r\n", 32<<(^uint(0)>>63))
		info.WriteString("gcc_version:0.0.0\r\n")
		fmt.Fprintf(&info, "process_id:%d\r\n", os.Getpid())
		if h.stats != nil {
			fmt.Fprintf(&info, "uptime_in_seconds:%d\r\n", int(h.stats.Uptime().Seconds()))
			fmt.Fprintf(&info, "uptime_in_days:%d\r\n", int(h.stats.Uptime().Hours()/24))
		}
		info.WriteString("tcp_port:6379\r\n")
		info.WriteString("\r\n")
	}

	// Clients section
	if section == "" || section == "CLIENTS" || section == "ALL" {
		info.WriteString("# Clients\r\n")
		if h.stats != nil {
			fmt.Fprintf(&info, "connected_clients:%d\r\n", h.stats.CurrConnections.Load())
			fmt.Fprintf(&info, "blocked_clients:%d\r\n", h.stats.BlockedClients.Load())
		} else {
			info.WriteString("connected_clients:0\r\n")
			info.WriteString("blocked_clients:0\r\n")
		}
		// Count blocking keys and keys where clients want to be unblocked on deletion
		totalBlocking, nokeyBlocking := h.dbManager.Subscriptions.BlockingKeyCounts()
		fmt.Fprintf(&info, "total_blocking_keys:%d\r\n", totalBlocking)
		fmt.Fprintf(&info, "total_blocking_keys_on_nokey:%d\r\n", nokeyBlocking)
		info.WriteString("\r\n")
	}

	// Memory section
	if section == "" || section == "MEMORY" || section == "ALL" {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		info.WriteString("# Memory\r\n")
		fmt.Fprintf(&info, "used_memory:%d\r\n", m.Alloc)
		fmt.Fprintf(&info, "used_memory_human:%s\r\n", formatBytes(m.Alloc))
		fmt.Fprintf(&info, "used_memory_peak:%d\r\n", m.TotalAlloc)
		fmt.Fprintf(&info, "used_memory_peak_human:%s\r\n", formatBytes(m.TotalAlloc))
		fmt.Fprintf(&info, "total_system_memory:%d\r\n", m.Sys)
		// maxmemory is the vertex pool's byte budget (0 = unbounded); the
		// policy is the engine's per-key frequency+LRU eviction, which maps to
		// allkeys-lfu, or noeviction when no budget is set.
		var maxMemory int64
		if h.engine != nil {
			maxMemory = h.engine.MemoryTarget()
		}
		policy := "noeviction"
		if maxMemory > 0 {
			policy = "allkeys-lfu"
		}
		fmt.Fprintf(&info, "maxmemory:%d\r\n", maxMemory)
		fmt.Fprintf(&info, "maxmemory_human:%s\r\n", formatBytes(uint64(maxMemory)))
		fmt.Fprintf(&info, "maxmemory_policy:%s\r\n", policy)
		info.WriteString("\r\n")
	}

	// Stats section
	if section == "" || section == "STATS" || section == "ALL" {
		info.WriteString("# Stats\r\n")
		if h.stats != nil {
			fmt.Fprintf(&info, "total_connections_received:%d\r\n", h.stats.TotalConnections.Load())
			fmt.Fprintf(&info, "total_commands_processed:%d\r\n",
				h.stats.CmdGet.Load()+h.stats.CmdSet.Load()+h.stats.CmdDel.Load())
			fmt.Fprintf(&info, "keyspace_hits:%d\r\n", h.stats.CmdGetHits.Load())
			fmt.Fprintf(&info, "keyspace_misses:%d\r\n", h.stats.CmdGetMisses.Load())
			fmt.Fprintf(&info, "total_error_replies:%d\r\n", h.stats.TotalErrorReplies.Load())
		}
		fmt.Fprintf(&info, "evicted_keys:%d\r\n", h.GetCacheEvictions())
		info.WriteString("\r\n")
	}

	// Persistence section
	if section == "" || section == "PERSISTENCE" || section == "ALL" {
		info.WriteString("# Persistence\r\n")
		info.WriteString("loading:0\r\n")
		info.WriteString("rdb_changes_since_last_save:0\r\n")
		info.WriteString("rdb_bgsave_in_progress:0\r\n")
		info.WriteString("rdb_last_save_time:0\r\n")
		info.WriteString("rdb_last_bgsave_status:ok\r\n")
		info.WriteString("rdb_last_bgsave_time_sec:-1\r\n")
		info.WriteString("rdb_current_bgsave_time_sec:-1\r\n")
		info.WriteString("aof_enabled:0\r\n")
		info.WriteString("aof_rewrite_in_progress:0\r\n")
		info.WriteString("aof_rewrite_scheduled:0\r\n")
		info.WriteString("aof_last_rewrite_time_sec:-1\r\n")
		info.WriteString("aof_current_rewrite_time_sec:-1\r\n")
		info.WriteString("aof_last_bgrewrite_status:ok\r\n")
		info.WriteString("\r\n")
	}

	// Replication section
	if section == "" || section == "REPLICATION" || section == "ALL" {
		info.WriteString("# Replication\r\n")
		info.WriteString("role:master\r\n")
		info.WriteString("connected_slaves:0\r\n")
		info.WriteString("\r\n")
	}

	// CPU section
	if section == "" || section == "CPU" || section == "ALL" {
		info.WriteString("# CPU\r\n")
		info.WriteString("used_cpu_sys:0.0\r\n")
		info.WriteString("used_cpu_user:0.0\r\n")
		info.WriteString("\r\n")
	}

	// Errorstats section
	if section == "ERRORSTATS" || section == "ALL" {
		info.WriteString("# Errorstats\r\n")
		if h.stats != nil {
			errorStats := h.stats.GetErrorStats()
			for prefix, count := range errorStats {
				fmt.Fprintf(&info, "errorstat_%s:count=%d\r\n", prefix, count)
			}
		}
		info.WriteString("\r\n")
	}

	// Commandstats section
	if section == "COMMANDSTATS" || section == "ALL" {
		info.WriteString("# Commandstats\r\n")
		if h.stats != nil {
			cmdStats := h.stats.GetCommandStats()
			for cmd, stats := range cmdStats {
				var usecPerCall float64
				if stats.Calls > 0 {
					usecPerCall = float64(stats.Usec) / float64(stats.Calls)
				}
				fmt.Fprintf(&info, "cmdstat_%s:calls=%d,usec=%d,usec_per_call=%.2f,rejected_calls=%d,failed_calls=%d\r\n",
					cmd, stats.Calls, stats.Usec, usecPerCall, stats.RejectedCalls, stats.FailedCalls)
			}
		}
		info.WriteString("\r\n")
	}

	// Keyspace section
	if section == "" || section == "KEYSPACE" || section == "ALL" {
		info.WriteString("# Keyspace\r\n")
		if h.engine != nil {
			keyCount := h.engine.KeyCount()
			if keyCount > 0 {
				fmt.Fprintf(&info, "db0:keys=%d,expires=0,avg_ttl=0\r\n", keyCount)
			}
		}
	}

	return info.String()
}

func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f%cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func (h *Handler) handleDBSize(cmd *shared.Command, w *shared.Writer, db *shared.Database) {
	if h.engine != nil {
		w.WriteInteger(h.engine.KeyCount())
	} else {
		w.WriteInteger(0)
	}
}

func (h *Handler) emitFlushEffect(cmd *shared.Command) error {
	if err := cmd.Context.Emit(&pb.Effect{
		Key: []byte(effects.FlushKey),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_SCALAR,
		}},
	}); err != nil {
		return err
	}
	return cmd.Context.Flush()
}

func (h *Handler) handleFlushDB(cmd *shared.Command, w *shared.Writer, db *shared.Database) {
	if err := h.emitFlushEffect(cmd); err != nil {
		w.WriteError("ERR flush failed: " + err.Error())
		return
	}
	w.WriteOK()
}

func (h *Handler) handleFlushAll(cmd *shared.Command, w *shared.Writer) {
	if err := h.emitFlushEffect(cmd); err != nil {
		w.WriteError("ERR flush failed: " + err.Error())
		return
	}
	w.WriteOK()
}

func (h *Handler) handleSwapDB(cmd *shared.Command, w *shared.Writer) {
	w.WriteError("ERR SWAPDB is not supported: there is only one database")
}

func (h *Handler) handleTime(cmd *shared.Command, w *shared.Writer) {
	now := time.Now()
	secs := now.Unix()
	micros := now.Nanosecond() / 1000

	w.WriteArray(2)
	w.WriteBulkStringStr(strconv.FormatInt(secs, 10))
	w.WriteBulkStringStr(strconv.Itoa(micros))
}

func (h *Handler) handleKeys(cmd *shared.Command, w *shared.Writer, db *shared.Database) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("keys")
		return
	}

	pattern := string(cmd.Args[0])
	if h.engine == nil {
		w.WriteArray(0)
		return
	}
	// KEYS is inherently O(N); snapshot for consistency then match
	keys := h.engine.MatchKeys(pattern)
	w.WriteArray(len(keys))
	for _, key := range keys {
		w.WriteBulkStringStr(key)
	}
}

func (h *Handler) handleRandomKey(cmd *shared.Command, w *shared.Writer, db *shared.Database) {
	if len(cmd.Args) != 0 {
		w.WriteWrongNumArguments("randomkey")
		return
	}

	if h.engine == nil {
		w.WriteNullBulkString()
		return
	}
	count := int(h.engine.KeyCount())
	if count == 0 {
		w.WriteNullBulkString()
		return
	}
	target := rand.Intn(count)
	i := 0
	h.engine.ScanKeys("", "*", func(key string) bool {
		if i == target {
			w.WriteBulkStringStr(key)
			return false
		}
		i++
		return true
	})
}

func (h *Handler) handleScan(cmd *shared.Command, w *shared.Writer, db *shared.Database) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("scan")
		return
	}

	// Parse options
	pattern := "*"
	count := 10

	for i := 1; i < len(cmd.Args)-1; i += 2 {
		opt := shared.ToUpper(cmd.Args[i])
		switch string(opt) {
		case "MATCH":
			pattern = string(cmd.Args[i+1])
		case "COUNT":
			c, ok := shared.ParseInt64(cmd.Args[i+1])
			if !ok || c <= 0 {
				w.WriteNotInteger()
				return
			}
			count = int(c)
		case "TYPE":
			// Type filtering not implemented yet
		}
	}

	if h.engine == nil {
		w.WriteArray(2)
		w.WriteBulkStringStr("0")
		w.WriteArray(0)
		return
	}

	// Stateless cursor: the raw cursor arg is the last key returned.
	// "0" means start from beginning.
	cursorStr := string(cmd.Args[0])
	after := ""
	if cursorStr != "0" {
		after = cursorStr
	}

	// Collect bounded batch (at most count keys)
	batch := make([]string, 0, count)
	h.engine.ScanKeys(after, pattern, func(key string) bool {
		batch = append(batch, key)
		return len(batch) < count
	})

	nextCursor := "0"
	if len(batch) == count {
		nextCursor = batch[len(batch)-1]
	}

	w.WriteArray(2)
	w.WriteBulkStringStr(nextCursor)
	w.WriteArray(len(batch))
	for _, key := range batch {
		w.WriteBulkStringStr(key)
	}
}

func (h *Handler) handleCommand(cmd *shared.Command, w *shared.Writer) {
	if len(cmd.Args) == 0 {
		// COMMAND with no args - return all commands info
		// Simplified response
		w.WriteArray(shared.CommandCount())
		shared.CommandNamesList(func(name string, _ shared.CommandType) bool {
			w.WriteArray(6)
			w.WriteBulkStringStr(name)
			w.WriteInteger(-1) // arity
			w.WriteArray(0)    // flags
			w.WriteInteger(0)  // first key
			w.WriteInteger(0)  // last key
			w.WriteInteger(0)  // step
			return true
		})
		return
	}

	subCmd := shared.ToUpper(cmd.Args[0])
	switch string(subCmd) {
	case "COUNT":
		w.WriteInteger(int64(shared.CommandCount()))
	case "DOCS":
		// Simplified - return empty array
		w.WriteArray(0)
	case "INFO":
		// Return info for specified commands
		if len(cmd.Args) < 2 {
			w.WriteArray(0)
			return
		}
		w.WriteArray(len(cmd.Args) - 1)
		for _, cmdName := range cmd.Args[1:] {
			upper := string(shared.ToUpper(cmdName))
			if _, exists := shared.LookupCommandName(upper); exists {
				w.WriteArray(6)
				w.WriteBulkStringStr(upper)
				w.WriteInteger(-1)
				w.WriteArray(0)
				w.WriteInteger(0)
				w.WriteInteger(0)
				w.WriteInteger(0)
			} else {
				w.WriteNullBulkString()
			}
		}
	default:
		w.WriteError("ERR unknown subcommand")
	}
}

func (h *Handler) handleConfig(cmd *shared.Command, w *shared.Writer) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("config")
		return
	}

	subCmd := shared.ToUpper(cmd.Args[0])
	switch string(subCmd) {
	case "GET":
		if len(cmd.Args) < 2 {
			w.WriteWrongNumArguments("config get")
			return
		}
		pattern := string(cmd.Args[1])
		h.handleConfigGet(pattern, w)
	case "SET":
		if len(cmd.Args) < 3 || len(cmd.Args)%2 != 1 {
			w.WriteWrongNumArguments("config set")
			return
		}
		h.handleConfigSet(cmd.Args[1:], w)
	case "RESETSTAT":
		// Reset stats
		if h.stats != nil {
			h.stats.Reset()
		}
		w.WriteOK()
	case "REWRITE":
		w.WriteOK()
	default:
		w.WriteError("ERR unknown subcommand")
	}
}

// configMatchPattern checks if a config name matches a glob pattern
func configMatchPattern(pattern, name string) bool {
	if pattern == "*" {
		return true
	}
	if pattern == name {
		return true
	}
	// Simple prefix matching for patterns like "proto-*"
	if len(pattern) > 1 && pattern[len(pattern)-1] == '*' {
		prefix := pattern[:len(pattern)-1]
		return len(name) >= len(prefix) && name[:len(prefix)] == prefix
	}
	return false
}

func (h *Handler) handleConfigGet(pattern string, w *shared.Writer) {
	// Build list of matching config key-value pairs
	var results []string

	// proto-max-bulk-len
	if configMatchPattern(pattern, "proto-max-bulk-len") {
		results = append(results, "proto-max-bulk-len", fmt.Sprintf("%d", h.protoMaxBulkLen))
	}

	// stream-node-max-entries
	if configMatchPattern(pattern, "stream-node-max-entries") {
		results = append(results, "stream-node-max-entries", fmt.Sprintf("%d", shared.GetStreamNodeMaxEntries()))
	}

	// stream-idmp-duration
	if configMatchPattern(pattern, "stream-idmp-duration") {
		results = append(results, "stream-idmp-duration", fmt.Sprintf("%d", shared.GetDefaultIDMPDuration()))
	}

	// stream-idmp-maxsize
	if configMatchPattern(pattern, "stream-idmp-maxsize") {
		results = append(results, "stream-idmp-maxsize", fmt.Sprintf("%d", shared.GetDefaultIDMPMaxSize()))
	}

	// Write as map (RESP3) or array (RESP2)
	w.WriteMap(len(results) / 2)
	for i := 0; i < len(results); i += 2 {
		w.WriteBulkStringStr(results[i])
		w.WriteBulkStringStr(results[i+1])
	}
}

func (h *Handler) handleConfigSet(args [][]byte, w *shared.Writer) {
	// Process key-value pairs
	for i := 0; i < len(args); i += 2 {
		key := string(args[i])
		value := string(args[i+1])

		switch key {
		case "proto-max-bulk-len":
			val, err := shared.ParseInt64([]byte(value))
			if !err {
				w.WriteError("ERR Invalid argument '" + value + "' for CONFIG SET 'proto-max-bulk-len'")
				return
			}
			if val < 1024*1024 {
				w.WriteError("ERR proto-max-bulk-len must be at least 1MB")
				return
			}
			h.protoMaxBulkLen = val
			shared.SetProtoMaxBulkLen(val)
		case "stream-node-max-entries":
			val, err := shared.ParseInt64([]byte(value))
			if !err {
				w.WriteError("ERR Invalid argument '" + value + "' for CONFIG SET 'stream-node-max-entries'")
				return
			}
			if val < 0 {
				w.WriteError("ERR stream-node-max-entries must be non-negative")
				return
			}
			shared.SetStreamNodeMaxEntries(val)
		case "lua-time-limit":
			val, err := shared.ParseInt64([]byte(value))
			if !err {
				w.WriteError("ERR Invalid argument '" + value + "' for CONFIG SET 'lua-time-limit'")
				return
			}
			// Redis accepts 0 to mean no limit, we store it in milliseconds
			h.scriptEngine.SetScriptTimeout(time.Duration(val) * time.Millisecond)
		case "stream-idmp-duration":
			val, err := shared.ParseInt64([]byte(value))
			if !err {
				w.WriteError("ERR Invalid argument '" + value + "' for CONFIG SET 'stream-idmp-duration'")
				return
			}
			if val < 1 || val > 86400 {
				w.WriteError("ERR stream-idmp-duration must be between 1 and 86400")
				return
			}
			shared.SetDefaultIDMPDuration(val)
		case "stream-idmp-maxsize":
			val, err := shared.ParseInt64([]byte(value))
			if !err {
				w.WriteError("ERR Invalid argument '" + value + "' for CONFIG SET 'stream-idmp-maxsize'")
				return
			}
			if val < 1 || val > 10000 {
				w.WriteError("ERR stream-idmp-maxsize must be between 1 and 10000")
				return
			}
			shared.SetDefaultIDMPMaxSize(val)
		default:
			// Silently accept unknown config options for compatibility
		}
	}
	w.WriteOK()
}

func (h *Handler) handleClient(cmd *shared.Command, w *shared.Writer, conn *shared.Connection) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("client")
		return
	}

	subCmd := shared.ToUpper(cmd.Args[0])
	switch string(subCmd) {
	case "LIST":
		// Return client list from registry
		if h.clientRegistry != nil {
			w.WriteBulkStringStr(h.clientRegistry.FormatClientList())
		} else {
			// Fallback if no registry
			w.WriteBulkStringStr("id=1 addr=127.0.0.1:0 fd=0 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 watch=0 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=client\r\n")
		}
	case "SETNAME":
		if len(cmd.Args) >= 2 {
			conn.ClientName = string(cmd.Args[1])
		}
		w.WriteOK()
	case "GETNAME":
		if conn.ClientName != "" {
			w.WriteBulkStringStr(conn.ClientName)
		} else {
			w.WriteNullBulkString()
		}
	case "ID":
		// Return the client ID from registry
		if h.clientRegistry != nil {
			if info := h.clientRegistry.GetByConn(conn); info != nil {
				w.WriteInteger(info.ID)
				return
			}
		}
		w.WriteInteger(1)
	case "INFO":
		// Return info for the current client
		if h.clientRegistry != nil {
			if info := h.clientRegistry.GetByConn(conn); info != nil {
				w.WriteBulkStringStr(h.clientRegistry.formatClientInfo(info))
				return
			}
		}
		w.WriteBulkStringStr("id=1 addr=127.0.0.1:0 fd=0 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 watch=0 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=client\r\n")
	case "KILL":
		w.WriteOK()
	case "PAUSE":
		w.WriteOK()
	case "UNPAUSE":
		w.WriteOK()
	case "REPLY":
		w.WriteOK()
	case "TRACKINGINFO":
		w.WriteArray(0)
	case "UNBLOCK":
		if len(cmd.Args) < 2 {
			w.WriteWrongNumArguments("client unblock")
			return
		}
		clientID, err := strconv.ParseInt(string(cmd.Args[1]), 10, 64)
		if err != nil {
			w.WriteError("ERR value is not an integer or out of range")
			return
		}
		// Optional TIMEOUT or ERROR argument
		withError := false
		if len(cmd.Args) > 2 {
			unblockType := shared.ToUpper(cmd.Args[2])
			switch string(unblockType) {
			case "TIMEOUT":
				withError = false
			case "ERROR":
				withError = true
			default:
				w.WriteError("ERR syntax error")
				return
			}
		}
		if h.clientRegistry != nil && h.clientRegistry.Unblock(clientID, withError) {
			w.WriteOne()
		} else {
			w.WriteZero()
		}
	default:
		w.WriteError("ERR unknown subcommand")
	}
}

func (h *Handler) handleDebug(cmd *shared.Command, w *shared.Writer) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("debug")
		return
	}

	subCmd := shared.ToUpper(cmd.Args[0])
	switch string(subCmd) {
	case "SLEEP":
		if len(cmd.Args) < 2 {
			w.WriteWrongNumArguments("debug sleep")
			return
		}
		secs, err := strconv.ParseFloat(string(cmd.Args[1]), 64)
		if err != nil {
			w.WriteError("ERR invalid sleep time")
			return
		}
		time.Sleep(time.Duration(secs * float64(time.Second)))
		w.WriteOK()
	case "OBJECT":
		w.WriteBulkStringStr("Value at:0x0 refcount:1 encoding:raw serializedlength:1 lru:0 lru_seconds_idle:0")
	case "SEGFAULT":
		w.WriteError("ERR not supported in swytch")
	default:
		w.WriteOK()
	}
}

func (h *Handler) handleMemory(cmd *shared.Command, w *shared.Writer, db *shared.Database) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("memory")
		return
	}

	subCmd := shared.ToUpper(cmd.Args[0])
	switch string(subCmd) {
	case "USAGE":
		if len(cmd.Args) < 2 {
			w.WriteWrongNumArguments("memory usage")
			return
		}
		key := string(cmd.Args[1])

		if h.engine == nil || cmd.Context == nil {
			w.WriteNullBulkString()
			return
		}
		// Route through Context.GetSnapshot so filterSnapshot drops
		// expired keys + empty-collection tombstones uniformly —
		// direct Engine.GetSnapshot returns the unfiltered
		// ReducedEffect and would report nonzero size for a TTL-
		// expired key.
		snap, _, err := cmd.Context.GetSnapshot(key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if snap == nil {
			w.WriteNullBulkString()
			return
		}
		size := snapshotMemoryUsage(snap, key)
		w.WriteInteger(size)
	case "STATS":
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		w.WriteArray(10)
		w.WriteBulkStringStr("peak.allocated")
		w.WriteInteger(int64(m.TotalAlloc))
		w.WriteBulkStringStr("total.allocated")
		w.WriteInteger(int64(m.Alloc))
		w.WriteBulkStringStr("startup.allocated")
		w.WriteInteger(0)
		w.WriteBulkStringStr("replication.backlog")
		w.WriteInteger(0)
		w.WriteBulkStringStr("clients.normal")
		w.WriteInteger(0)
	case "DOCTOR":
		w.WriteBulkStringStr("Sam, I have no memory problems")
	case "MALLOC-SIZE":
		w.WriteInteger(0)
	case "PURGE":
		runtime.GC()
		w.WriteOK()
	default:
		w.WriteError("ERR unknown subcommand")
	}
}

// snapshotMemoryUsage estimates memory usage from an effects snapshot.
// Returns an approximate byte count compatible with Redis MEMORY USAGE expectations:
// base overhead (16 bytes on 64-bit) + key length + value data length.
func snapshotMemoryUsage(snap *pb.ReducedEffect, key string) int64 {
	const baseOverhead = 16 // SDS header overhead (64-bit)
	size := int64(baseOverhead) + int64(len(key))

	switch snap.Collection {
	case pb.CollectionKind_SCALAR:
		size += valueMemorySize(snap.GetScalar())
	case pb.CollectionKind_KEYED:
		for id, elem := range snap.GetNetAdds() {
			size += int64(len(id)) + 16 // element ID + overhead
			size += valueMemorySize(elem.GetData())
		}
	case pb.CollectionKind_ORDERED:
		for _, elem := range snap.GetOrderedElements() {
			size += 16 // per-element overhead
			size += valueMemorySize(elem.GetData())
		}
	}

	return size
}

// valueMemorySize is one value's contribution to MEMORY USAGE: the bytes the
// value actually occupies in reduced state — a compressed value counts its
// stored (compressed) length, not what it would inflate to.
func valueMemorySize(d *pb.DataEffect) int64 {
	if d == nil {
		return 0
	}
	switch v := d.Value.(type) {
	case *pb.DataEffect_Raw:
		return int64(len(v.Raw))
	case *pb.DataEffect_Compressed:
		return int64(len(v.Compressed.GetData()))
	case *pb.DataEffect_IntVal, *pb.DataEffect_FloatVal:
		return 8
	}
	return 0
}

// registerServerCommands registers server/admin commands.
func (h *Handler) registerServerCommands() {
	r := &h.cmds

	// Server commands with standard Handler wrapper (queueable in transactions)
	r.Register(shared.CmdInfo, &shared.CommandEntry{
		Handler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database) (bool, []string, shared.CommandRunner) {
			return true, nil, func() { h.handleInfo(cmd, w) }
		},
	})
	r.Register(shared.CmdDBSize, &shared.CommandEntry{
		Handler: func(cmd *shared.Command, w *shared.Writer, db *shared.Database) (bool, []string, shared.CommandRunner) {
			return true, nil, func() { h.handleDBSize(cmd, w, db) }
		},
	})
	r.Register(shared.CmdFlushDB, &shared.CommandEntry{
		Handler: func(cmd *shared.Command, w *shared.Writer, db *shared.Database) (bool, []string, shared.CommandRunner) {
			return true, nil, func() { h.handleFlushDB(cmd, w, db) }
		},
		Flags: shared.FlagWrite,
	})
	r.Register(shared.CmdFlushAll, &shared.CommandEntry{
		Handler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database) (bool, []string, shared.CommandRunner) {
			return true, nil, func() { h.handleFlushAll(cmd, w) }
		},
		Flags: shared.FlagWrite,
	})
	r.Register(shared.CmdSwapDB, &shared.CommandEntry{
		Handler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database) (bool, []string, shared.CommandRunner) {
			return true, nil, func() { h.handleSwapDB(cmd, w) }
		},
		Flags: shared.FlagWrite,
	})
	r.Register(shared.CmdTime, &shared.CommandEntry{
		Handler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database) (bool, []string, shared.CommandRunner) {
			return true, nil, func() { h.handleTime(cmd, w) }
		},
	})
	r.Register(shared.CmdKeys, &shared.CommandEntry{
		Handler: func(cmd *shared.Command, w *shared.Writer, db *shared.Database) (bool, []string, shared.CommandRunner) {
			return true, nil, func() { h.handleKeys(cmd, w, db) }
		},
	})
	r.Register(shared.CmdRandomKey, &shared.CommandEntry{
		Handler: func(cmd *shared.Command, w *shared.Writer, db *shared.Database) (bool, []string, shared.CommandRunner) {
			return true, nil, func() { h.handleRandomKey(cmd, w, db) }
		},
	})
	r.Register(shared.CmdScan, &shared.CommandEntry{
		Handler: func(cmd *shared.Command, w *shared.Writer, db *shared.Database) (bool, []string, shared.CommandRunner) {
			return true, nil, func() { h.handleScan(cmd, w, db) }
		},
	})

	// Server commands that are NOT queueable (ConnHandler only)
	r.Register(shared.CmdCommand, &shared.CommandEntry{
		ConnHandler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, _ *shared.Connection) {
			h.handleCommand(cmd, w)
		},
	})
	r.Register(shared.CmdConfig, &shared.CommandEntry{
		ConnHandler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, _ *shared.Connection) {
			h.handleConfig(cmd, w)
		},
	})
	r.Register(shared.CmdClient, &shared.CommandEntry{
		ConnHandler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, conn *shared.Connection) {
			h.handleClient(cmd, w, conn)
		},
	})
	r.Register(shared.CmdDebug, &shared.CommandEntry{
		ConnHandler: func(cmd *shared.Command, w *shared.Writer, _ *shared.Database, _ *shared.Connection) {
			h.handleDebug(cmd, w)
		},
	})
	r.Register(shared.CmdMemory, &shared.CommandEntry{
		ConnHandler: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, _ *shared.Connection) {
			h.handleMemory(cmd, w, db)
		},
	})

}

// registerReplicationStubs registers replication command stubs.
func (h *Handler) registerReplicationStubs() {
	r := &h.cmds

	r.Register(shared.CmdSync, &shared.CommandEntry{
		ConnHandler: func(_ *shared.Command, w *shared.Writer, _ *shared.Database, _ *shared.Connection) {
			w.WriteError("ERR This instance has cluster support disabled")
		},
	})
	r.Register(shared.CmdPSync, &shared.CommandEntry{
		ConnHandler: func(_ *shared.Command, w *shared.Writer, _ *shared.Database, _ *shared.Connection) {
			w.WriteError("ERR This instance has cluster support disabled")
		},
	})
	r.Register(shared.CmdReplConf, &shared.CommandEntry{
		ConnHandler: func(_ *shared.Command, w *shared.Writer, _ *shared.Database, _ *shared.Connection) {
			w.WriteOK()
		},
	})
	r.Register(shared.CmdWait, &shared.CommandEntry{
		ConnHandler: func(_ *shared.Command, w *shared.Writer, _ *shared.Database, _ *shared.Connection) {
			w.WriteInteger(0)
		},
	})
	r.Register(shared.CmdWaitAof, &shared.CommandEntry{
		ConnHandler: func(_ *shared.Command, w *shared.Writer, _ *shared.Database, _ *shared.Connection) {
			w.WriteArray(2)
			w.WriteInteger(1)
			w.WriteInteger(0)
		},
	})
}
