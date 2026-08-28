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
	"github.com/swytchdb/engine/cache"
	"github.com/swytchdb/swytch/metrics"
	"github.com/swytchdb/swytch/redis/shared"
)

// MetricsAdapter adapts Redis Stats and Handler to the metrics.StatsProvider interface.
type MetricsAdapter struct {
	stats       *shared.Stats
	handler     shared.CommandHandler
	memoryLimit int64
}

// NewMetricsAdapter creates a new metrics adapter for the Redis server.
func NewMetricsAdapter(stats *shared.Stats, handler shared.CommandHandler, memoryLimit int64) *MetricsAdapter {
	return &MetricsAdapter{
		stats:       stats,
		handler:     handler,
		memoryLimit: memoryLimit,
	}
}

// Subsystem returns "redis" for the Redis adapter.
func (a *MetricsAdapter) Subsystem() string {
	return "redis"
}

// CurrentConnections returns the current number of client connections.
func (a *MetricsAdapter) CurrentConnections() int64 {
	return a.stats.CurrConnections.Load()
}

// TotalConnections returns the total number of connections since start.
func (a *MetricsAdapter) TotalConnections() uint64 {
	return a.stats.TotalConnections.Load()
}

// CommandCounts returns command execution counts by command name.
func (a *MetricsAdapter) CommandCounts() map[string]uint64 {
	cmdStats := a.stats.GetCommandStats()
	result := make(map[string]uint64, len(cmdStats))
	for cmd, stats := range cmdStats {
		result[cmd] = stats.Calls
	}
	return result
}

// CacheHits returns the total number of cache hits.
func (a *MetricsAdapter) CacheHits() uint64 {
	return a.stats.CmdGetHits.Load()
}

// CacheMisses returns the total number of cache misses.
func (a *MetricsAdapter) CacheMisses() uint64 {
	return a.stats.CmdGetMisses.Load()
}

// HitRate returns the cache hit rate (0-1).
func (a *MetricsAdapter) HitRate() float64 {
	return a.stats.HitRate()
}

// Evictions returns keys evicted under memory pressure (evicted_keys).
func (a *MetricsAdapter) Evictions() uint64 {
	return a.handler.GetCacheEvictions()
}

// Reclaimed returns vertices freed by reclaim (storage churn, not eviction).
func (a *MetricsAdapter) Reclaimed() uint64 {
	return a.handler.GetVerticesReclaimed()
}

// ItemCount returns the number of keys resident in the index.
func (a *MetricsAdapter) ItemCount() int {
	return a.handler.GetItemCount()
}

// ReleaseQueueDepth returns the number of cold-evicted keys whose deferred
// ref-release walk has not yet run.
func (a *MetricsAdapter) ReleaseQueueDepth() int {
	return a.handler.GetReleaseQueueDepth()
}

// ArenaBytes returns the critbit index's slot-array footprint (trie skeleton).
func (a *MetricsAdapter) ArenaBytes() int64 {
	return a.handler.GetArenaBytes()
}

// VertexCount returns the number of effects resident in the vertex pool.
func (a *MetricsAdapter) VertexCount() int {
	return a.handler.GetVertexCount()
}

// MemoryBytes returns the current memory used by cached items.
func (a *MetricsAdapter) MemoryBytes() int64 {
	return a.handler.GetCacheBytes()
}

// MaxMemoryBytes returns the maximum configured memory limit.
func (a *MetricsAdapter) MaxMemoryBytes() int64 {
	return a.memoryLimit
}

// GetLatencyP50 returns the 50th percentile GET latency in seconds.
func (a *MetricsAdapter) GetLatencyP50() float64 {
	return a.stats.GetLatencyP50().Seconds()
}

// GetLatencyP99 returns the 99th percentile GET latency in seconds.
func (a *MetricsAdapter) GetLatencyP99() float64 {
	return a.stats.GetLatencyP99().Seconds()
}

// SetLatencyP50 returns the 50th percentile SET latency in seconds.
func (a *MetricsAdapter) SetLatencyP50() float64 {
	return a.stats.SetLatencyP50().Seconds()
}

// SetLatencyP99 returns the 99th percentile SET latency in seconds.
func (a *MetricsAdapter) SetLatencyP99() float64 {
	return a.stats.SetLatencyP99().Seconds()
}

// CmdLatencyP50 returns the 50th percentile command latency in seconds (all commands).
func (a *MetricsAdapter) CmdLatencyP50() float64 {
	return a.stats.CmdLatencyP50().Seconds()
}

// CmdLatencyP99 returns the 99th percentile command latency in seconds (all commands).
func (a *MetricsAdapter) CmdLatencyP99() float64 {
	return a.stats.CmdLatencyP99().Seconds()
}

// AdaptiveStats returns the per-domain adaptive eviction statistics (K plus the
// internal graduation-rate / learned-threshold state that drives it).
func (a *MetricsAdapter) AdaptiveStats() []cache.AdaptiveStats {
	return a.handler.GetAdaptiveStats()
}

// BytesRead returns the total bytes read from network.
func (a *MetricsAdapter) BytesRead() uint64 {
	return a.stats.BytesRead.Load()
}

// BytesWritten returns the total bytes written to network.
func (a *MetricsAdapter) BytesWritten() uint64 {
	return a.stats.BytesWritten.Load()
}

// UptimeSeconds returns the server uptime in seconds.
func (a *MetricsAdapter) UptimeSeconds() float64 {
	return a.stats.Uptime().Seconds()
}

// BlockedClients returns the number of clients blocked on blocking commands.
func (a *MetricsAdapter) BlockedClients() int64 {
	return a.stats.BlockedClients.Load()
}

// CommandErrors returns command error counts by command name.
func (a *MetricsAdapter) CommandErrors() map[string]uint64 {
	cmdStats := a.stats.GetCommandStats()
	result := make(map[string]uint64)
	for cmd, stats := range cmdStats {
		if stats.FailedCalls > 0 {
			result[cmd] = stats.FailedCalls
		}
	}
	return result
}

// CasHits returns 0 (not applicable for Redis).
func (a *MetricsAdapter) CasHits() uint64 {
	return 0
}

// CasMisses returns 0 (not applicable for Redis).
func (a *MetricsAdapter) CasMisses() uint64 {
	return 0
}

// DeleteHits returns 0 (not applicable for Redis).
func (a *MetricsAdapter) DeleteHits() uint64 {
	return 0
}

// DeleteMisses returns 0 (not applicable for Redis).
func (a *MetricsAdapter) DeleteMisses() uint64 {
	return 0
}

// Ensure MetricsAdapter implements metrics.StatsProvider.
var _ metrics.StatsProvider = (*MetricsAdapter)(nil)
