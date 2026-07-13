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
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
)

const (
	latencyHistogramSize = 10000
)

// serverStats is the package-level server stats instance, set during handler init.
// Uses atomic.Pointer for safe concurrent access.
var serverStats atomic.Pointer[Stats]

// protoMaxBulkLen is the maximum bulk string size, set during handler init (default 512MB).
// Uses atomic.Int64 for safe concurrent access (CONFIG SET can update at runtime).
var protoMaxBulkLen atomic.Int64

// streamNodeMaxEntries is the max entries per radix tree node for streams (default 100).
// Uses atomic.Int64 for safe concurrent access (CONFIG SET can update at runtime).
var streamNodeMaxEntries atomic.Int64

func init() {
	protoMaxBulkLen.Store(512 * 1024 * 1024)
	streamNodeMaxEntries.Store(100)
}

// GetServerStats returns the current server stats instance (may be nil).
func GetServerStats() *Stats {
	return serverStats.Load()
}

// SetServerStats sets the server stats instance atomically.
func SetServerStats(s *Stats) {
	serverStats.Store(s)
}

// GetProtoMaxBulkLen returns the current proto-max-bulk-len value.
func GetProtoMaxBulkLen() int64 {
	return protoMaxBulkLen.Load()
}

// SetProtoMaxBulkLen updates the proto-max-bulk-len value atomically.
func SetProtoMaxBulkLen(v int64) {
	protoMaxBulkLen.Store(v)
}

// GetStreamNodeMaxEntries returns the current stream-node-max-entries value.
func GetStreamNodeMaxEntries() int64 {
	return streamNodeMaxEntries.Load()
}

// SetStreamNodeMaxEntries updates the stream-node-max-entries value atomically.
func SetStreamNodeMaxEntries(v int64) {
	streamNodeMaxEntries.Store(v)
}

// IDMP config defaults
var defaultIDMPDuration atomic.Int64
var defaultIDMPMaxSize atomic.Int64

func init() {
	defaultIDMPDuration.Store(100)
	defaultIDMPMaxSize.Store(100)
}

func GetDefaultIDMPDuration() int64  { return defaultIDMPDuration.Load() }
func GetDefaultIDMPMaxSize() int64   { return defaultIDMPMaxSize.Load() }
func SetDefaultIDMPDuration(v int64) { defaultIDMPDuration.Store(v) }
func SetDefaultIDMPMaxSize(v int64)  { defaultIDMPMaxSize.Store(v) }

// Stats tracks Redis server statistics
type Stats struct {
	startTime time.Time
	pid       int

	// Connection stats
	CurrConnections  atomic.Int64
	TotalConnections atomic.Uint64
	BlockedClients   atomic.Int64

	// Command counts
	CmdGet atomic.Uint64
	CmdSet atomic.Uint64
	CmdDel atomic.Uint64

	// Hit/miss tracking
	CmdGetHits   atomic.Uint64
	CmdGetMisses atomic.Uint64

	// Latency tracking
	getLatencies []time.Duration
	setLatencies []time.Duration
	cmdLatencies []time.Duration
	latencyMu    sync.Mutex
	getLatIdx    int
	setLatIdx    int
	cmdLatIdx    int

	// Bytes read/written
	BytesRead    atomic.Uint64
	BytesWritten atomic.Uint64

	// Total error replies
	TotalErrorReplies atomic.Uint64

	// Error stats (error prefix -> count), uses xsync.Map for lock-free reads
	errorStats *xsync.Map[string, *atomic.Uint64]

	// Command stats, indexed by CommandType. A flat array of atomics keeps
	// the record path free of locks, map lookups, and allocations.
	cmdStats [CmdMax]CommandStats
}

// CommandStats tracks statistics for a single command
type CommandStats struct {
	Calls         atomic.Uint64
	Usec          atomic.Uint64
	RejectedCalls atomic.Uint64
	FailedCalls   atomic.Uint64
}

// NewStats creates a new Stats instance
func NewStats() *Stats {
	return &Stats{
		startTime:    time.Now(),
		pid:          os.Getpid(),
		getLatencies: make([]time.Duration, latencyHistogramSize),
		setLatencies: make([]time.Duration, latencyHistogramSize),
		cmdLatencies: make([]time.Duration, latencyHistogramSize),
		errorStats:   xsync.NewMap[string, *atomic.Uint64](),
	}
}

// Reset resets all statistics
func (s *Stats) Reset() {
	s.CurrConnections.Store(0)
	s.TotalConnections.Store(0)
	s.BlockedClients.Store(0)
	s.CmdGet.Store(0)
	s.CmdSet.Store(0)
	s.CmdDel.Store(0)
	s.CmdGetHits.Store(0)
	s.CmdGetMisses.Store(0)
	s.BytesRead.Store(0)
	s.BytesWritten.Store(0)
	s.TotalErrorReplies.Store(0)

	s.latencyMu.Lock()
	s.getLatIdx = 0
	s.setLatIdx = 0
	s.cmdLatIdx = 0
	for i := range s.getLatencies {
		s.getLatencies[i] = 0
	}
	for i := range s.setLatencies {
		s.setLatencies[i] = 0
	}
	for i := range s.cmdLatencies {
		s.cmdLatencies[i] = 0
	}
	s.latencyMu.Unlock()

	// Clear errorStats by deleting all keys
	s.errorStats.Range(func(key string, _ *atomic.Uint64) bool {
		s.errorStats.Delete(key)
		return true
	})

	for i := range s.cmdStats {
		st := &s.cmdStats[i]
		st.Calls.Store(0)
		st.Usec.Store(0)
		st.RejectedCalls.Store(0)
		st.FailedCalls.Store(0)
	}
}

// ConnectionOpened records a new connection
func (s *Stats) ConnectionOpened() {
	s.CurrConnections.Add(1)
	s.TotalConnections.Add(1)
}

// ConnectionClosed records a closed connection
func (s *Stats) ConnectionClosed() {
	s.CurrConnections.Add(-1)
}

// ClientBlocked records a client entering a blocking state
func (s *Stats) ClientBlocked() {
	s.BlockedClients.Add(1)
}

// ClientUnblocked records a client leaving a blocking state
func (s *Stats) ClientUnblocked() {
	s.BlockedClients.Add(-1)
}

// RecordGetLatency records a GET command latency
func (s *Stats) RecordGetLatency(d time.Duration) {
	s.CmdGet.Add(1)
	s.latencyMu.Lock()
	s.getLatencies[s.getLatIdx%latencyHistogramSize] = d
	s.getLatIdx++
	s.latencyMu.Unlock()
}

// RecordSetLatency records a SET command latency
func (s *Stats) RecordSetLatency(d time.Duration) {
	s.CmdSet.Add(1)
	s.latencyMu.Lock()
	s.setLatencies[s.setLatIdx%latencyHistogramSize] = d
	s.setLatIdx++
	s.latencyMu.Unlock()
}

// GetLatencyP50 returns the 50th percentile GET latency
func (s *Stats) GetLatencyP50() time.Duration {
	return s.getPercentile(s.getLatencies, s.getLatIdx, 50)
}

// GetLatencyP99 returns the 99th percentile GET latency
func (s *Stats) GetLatencyP99() time.Duration {
	return s.getPercentile(s.getLatencies, s.getLatIdx, 99)
}

// SetLatencyP50 returns the 50th percentile SET latency
func (s *Stats) SetLatencyP50() time.Duration {
	return s.getPercentile(s.setLatencies, s.setLatIdx, 50)
}

// SetLatencyP99 returns the 99th percentile SET latency
func (s *Stats) SetLatencyP99() time.Duration {
	return s.getPercentile(s.setLatencies, s.setLatIdx, 99)
}

// CmdLatencyP50 returns the 50th percentile command latency (all commands)
func (s *Stats) CmdLatencyP50() time.Duration {
	return s.getPercentile(s.cmdLatencies, s.cmdLatIdx, 50)
}

// CmdLatencyP99 returns the 99th percentile command latency (all commands)
func (s *Stats) CmdLatencyP99() time.Duration {
	return s.getPercentile(s.cmdLatencies, s.cmdLatIdx, 99)
}

func (s *Stats) getPercentile(latencies []time.Duration, idx int, percentile int) time.Duration {
	s.latencyMu.Lock()
	defer s.latencyMu.Unlock()

	count := min(idx, latencyHistogramSize)
	if count == 0 {
		return 0
	}

	// Copy and sort
	sorted := make([]time.Duration, count)
	copy(sorted, latencies[:count])
	slices.Sort(sorted)

	idx = (count * percentile) / 100
	if idx >= count {
		idx = count - 1
	}
	return sorted[idx]
}

// Uptime returns the server uptime
func (s *Stats) Uptime() time.Duration {
	return time.Since(s.startTime)
}

// HitRate returns the cache hit rate (0.0 to 1.0)
func (s *Stats) HitRate() float64 {
	hits := s.CmdGetHits.Load()
	misses := s.CmdGetMisses.Load()
	total := hits + misses
	if total == 0 {
		return 0
	}
	return float64(hits) / float64(total)
}

// RecordError records an error by its prefix (e.g., "NOGROUP", "ERR", "WRONGTYPE")
func (s *Stats) RecordError(prefix string) {
	s.TotalErrorReplies.Add(1)

	// Fast path: counter already exists
	if counter, ok := s.errorStats.Load(prefix); ok {
		counter.Add(1)
		return
	}

	// Slow path: create new counter using LoadOrStore to avoid races
	newCounter := &atomic.Uint64{}
	actual, loaded := s.errorStats.LoadOrStore(prefix, newCounter)
	if loaded {
		// Another goroutine created it first, use theirs
		actual.Add(1)
	} else {
		// We won the race, initialize our counter
		newCounter.Store(1)
	}
}

// GetErrorStats returns a copy of error statistics
func (s *Stats) GetErrorStats() map[string]uint64 {
	result := make(map[string]uint64)
	s.errorStats.Range(func(key string, value *atomic.Uint64) bool {
		result[key] = value.Load()
		return true
	})
	return result
}

// RecordCommand records a command execution
func (s *Stats) RecordCommand(cmd CommandType, usec uint64, failed bool) {
	if cmd < 0 || cmd >= CmdMax {
		return
	}
	stats := &s.cmdStats[cmd]
	stats.Calls.Add(1)
	stats.Usec.Add(usec)
	if failed {
		stats.FailedCalls.Add(1)
	}

	d := time.Duration(usec) * time.Microsecond
	s.latencyMu.Lock()
	s.cmdLatencies[s.cmdLatIdx%latencyHistogramSize] = d
	s.cmdLatIdx++
	s.latencyMu.Unlock()
}

// CommandStatsSnapshot is a non-atomic snapshot of command stats
type CommandStatsSnapshot struct {
	Calls         uint64
	Usec          uint64
	RejectedCalls uint64
	FailedCalls   uint64
}

// GetCommandStats returns a copy of command statistics, keyed by command
// name, for commands that have been called at least once.
func (s *Stats) GetCommandStats() map[string]CommandStatsSnapshot {
	result := make(map[string]CommandStatsSnapshot)
	for cmd := range CmdMax {
		stats := &s.cmdStats[cmd]
		calls := stats.Calls.Load()
		if calls == 0 {
			continue
		}
		result[cmd.String()] = CommandStatsSnapshot{
			Calls:         calls,
			Usec:          stats.Usec.Load(),
			RejectedCalls: stats.RejectedCalls.Load(),
			FailedCalls:   stats.FailedCalls.Load(),
		}
	}
	return result
}

// Snapshot represents a point-in-time snapshot of statistics
type Snapshot struct {
	Time             time.Time
	Uptime           time.Duration
	CurrConnections  int64
	TotalConnections uint64
	CmdGet           uint64
	CmdSet           uint64
	CmdDel           uint64
	CmdGetHits       uint64
	CmdGetMisses     uint64
	HitRate          float64
	GetLatencyP50    time.Duration
	GetLatencyP99    time.Duration
	SetLatencyP50    time.Duration
	SetLatencyP99    time.Duration
	CmdLatencyP50    time.Duration
	CmdLatencyP99    time.Duration
	BytesRead        uint64
	BytesWritten     uint64
}

// Snapshot takes a point-in-time snapshot of statistics
func (s *Stats) Snapshot() Snapshot {
	return Snapshot{
		Time:             time.Now(),
		Uptime:           s.Uptime(),
		CurrConnections:  s.CurrConnections.Load(),
		TotalConnections: s.TotalConnections.Load(),
		CmdGet:           s.CmdGet.Load(),
		CmdSet:           s.CmdSet.Load(),
		CmdDel:           s.CmdDel.Load(),
		CmdGetHits:       s.CmdGetHits.Load(),
		CmdGetMisses:     s.CmdGetMisses.Load(),
		HitRate:          s.HitRate(),
		GetLatencyP50:    s.GetLatencyP50(),
		GetLatencyP99:    s.GetLatencyP99(),
		SetLatencyP50:    s.SetLatencyP50(),
		SetLatencyP99:    s.SetLatencyP99(),
		CmdLatencyP50:    s.CmdLatencyP50(),
		CmdLatencyP99:    s.CmdLatencyP99(),
		BytesRead:        s.BytesRead.Load(),
		BytesWritten:     s.BytesWritten.Load(),
	}
}
