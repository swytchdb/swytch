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

package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/swytchdb/engine/cache"
)

// StatsProvider defines the interface for retrieving server statistics.
// Both Redis and Memcached servers implement this interface via adapters.
type StatsProvider interface {
	// Subsystem returns the metrics subsystem name ("redis" or "memcached")
	Subsystem() string

	// Connection stats
	CurrentConnections() int64
	TotalConnections() uint64

	// Command stats
	CommandCounts() map[string]uint64

	// Cache stats
	CacheHits() uint64
	CacheMisses() uint64
	HitRate() float64
	Evictions() uint64
	Reclaimed() uint64
	ItemCount() int
	// ReleaseQueueDepth is the number of cold-evicted keys whose deferred
	// ref-release walk has not yet run — pending entries pin their key's
	// resident chain, so depth is memory eviction can't free yet.
	ReleaseQueueDepth() int
	MemoryBytes() int64
	MaxMemoryBytes() int64
	// ArenaBytes is the critbit index slot-array footprint (trie skeleton);
	// VertexCount is the number of effects resident in the vertex pool. Together
	// they isolate the two memory consumers — trie nodes vs effect payloads.
	ArenaBytes() int64
	VertexCount() int

	// Latency stats (in seconds)
	GetLatencyP50() float64
	GetLatencyP99() float64
	SetLatencyP50() float64
	SetLatencyP99() float64
	CmdLatencyP50() float64
	CmdLatencyP99() float64

	// Adaptive eviction stats per domain (K plus the internal graduation-rate
	// and learned-threshold state that drives it)
	AdaptiveStats() []cache.AdaptiveStats

	// Network stats
	BytesRead() uint64
	BytesWritten() uint64

	// Uptime
	UptimeSeconds() float64

	// Server-specific stats (optional - can return nil if not applicable)
	// Redis-specific
	BlockedClients() int64
	CommandErrors() map[string]uint64

	// Memcached-specific
	CasHits() uint64
	CasMisses() uint64
	DeleteHits() uint64
	DeleteMisses() uint64
}

// Collector implements prometheus.Collector to expose metrics on scrape.
type Collector struct {
	provider StatsProvider

	// Descriptors
	connectionsCurrent *prometheus.Desc
	connectionsTotal   *prometheus.Desc
	commandsTotal      *prometheus.Desc
	cacheHitsTotal     *prometheus.Desc
	cacheMissesTotal   *prometheus.Desc
	cacheHitRate       *prometheus.Desc
	evictionsTotal     *prometheus.Desc
	reclaimedTotal     *prometheus.Desc
	itemsCount         *prometheus.Desc
	releaseQueueDepth  *prometheus.Desc
	memoryBytes        *prometheus.Desc
	memoryMaxBytes     *prometheus.Desc
	arenaBytes         *prometheus.Desc
	vertexCount        *prometheus.Desc
	latencySeconds     *prometheus.Desc
	adaptiveKThreshold *prometheus.Desc
	graduationRate     *prometheus.Desc
	rateLow            *prometheus.Desc
	rateHigh           *prometheus.Desc
	evictedUnprotected *prometheus.Desc
	evictedProtected   *prometheus.Desc
	reachedProtected   *prometheus.Desc
	windowHitRate      *prometheus.Desc
	ghostCount         *prometheus.Desc
	bytesReadTotal     *prometheus.Desc
	bytesWrittenTotal  *prometheus.Desc
	uptimeSeconds      *prometheus.Desc

	// Redis-specific
	blockedClients     *prometheus.Desc
	commandErrorsTotal *prometheus.Desc

	// Memcached-specific
	casHitsTotal      *prometheus.Desc
	casMissesTotal    *prometheus.Desc
	deleteHitsTotal   *prometheus.Desc
	deleteMissesTotal *prometheus.Desc
}

// NewCollector creates a new Prometheus collector for the given stats provider.
func NewCollector(provider StatsProvider) *Collector {
	subsystem := provider.Subsystem()
	namespace := "swytch"

	c := &Collector{
		provider: provider,

		connectionsCurrent: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "connections_current"),
			"Current number of client connections",
			nil, nil,
		),
		connectionsTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "connections_total"),
			"Total number of client connections since server start",
			nil, nil,
		),
		commandsTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "commands_total"),
			"Total commands processed by type",
			[]string{"command"}, nil,
		),
		cacheHitsTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "cache_hits_total"),
			"Total cache hits",
			nil, nil,
		),
		cacheMissesTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "cache_misses_total"),
			"Total cache misses",
			nil, nil,
		),
		cacheHitRate: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "cache_hit_rate"),
			"Cache hit rate (0-1)",
			nil, nil,
		),
		evictionsTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "evictions_total"),
			"Keys evicted under memory pressure by the bounded sweep (evicted_keys)",
			nil, nil,
		),
		reclaimedTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "reclaimed_total"),
			"Vertices freed by reclaim (below-LCA history/orphans) — storage churn, not eviction",
			nil, nil,
		),
		itemsCount: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "items_count"),
			"Keys resident in the index (the set eviction operates on; distinct from vertex_count)",
			nil, nil,
		),
		releaseQueueDepth: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "release_queue_depth"),
			"Cold-evicted keys whose deferred ref-release walk has not yet run; persistently deep means reclaim is falling behind eviction",
			nil, nil,
		),
		memoryBytes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "memory_bytes"),
			"Current memory used by cache in bytes",
			nil, nil,
		),
		memoryMaxBytes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "memory_max_bytes"),
			"Maximum memory configured for cache in bytes",
			nil, nil,
		),
		arenaBytes: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "arena_bytes"),
			"Critbit index slot-array footprint in bytes (trie skeleton; distinct from memory_bytes which is vertex-pool effect payloads)",
			nil, nil,
		),
		vertexCount: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "vertex_count"),
			"Effects resident in the vertex pool",
			nil, nil,
		),
		latencySeconds: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "latency_seconds"),
			"Operation latency in seconds",
			[]string{"operation", "quantile"}, nil,
		),
		adaptiveKThreshold: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "adaptive_k_threshold"),
			"Adaptive K threshold per shard",
			[]string{"shard"}, nil,
		),
		graduationRate: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "adaptive_graduation_rate"),
			"Fraction of evicted candidates that graduated past k — drives k up when above rate_high, down when below rate_low",
			[]string{"shard"}, nil,
		),
		rateLow: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "adaptive_rate_low"),
			"Learned graduation-rate threshold below which k decreases",
			[]string{"shard"}, nil,
		),
		rateHigh: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "adaptive_rate_high"),
			"Learned graduation-rate threshold above which k increases",
			[]string{"shard"}, nil,
		),
		evictedUnprotected: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "adaptive_evicted_unprotected"),
			"Windowed count of victims evicted with freq <= k (cold, expected)",
			[]string{"shard"}, nil,
		),
		evictedProtected: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "adaptive_evicted_protected"),
			"Windowed count of victims evicted with freq > k (forced — pressure too high)",
			[]string{"shard"}, nil,
		),
		reachedProtected: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "adaptive_reached_protected"),
			"Windowed count of leaves that graduated past k (graduation-rate numerator)",
			[]string{"shard"}, nil,
		),
		windowHitRate: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "adaptive_window_hit_rate"),
			"Current adapt-window hit rate (self-tuning gradient input)",
			[]string{"shard"}, nil,
		),
		ghostCount: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "ghost_count"),
			"Ghost leaves retained for warm restart after eviction",
			[]string{"shard"}, nil,
		),
		bytesReadTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "bytes_read_total"),
			"Total bytes read from network",
			nil, nil,
		),
		bytesWrittenTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "bytes_written_total"),
			"Total bytes written to network",
			nil, nil,
		),
		uptimeSeconds: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "uptime_seconds"),
			"Server uptime in seconds",
			nil, nil,
		),

		// Redis-specific
		blockedClients: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "blocked_clients"),
			"Number of clients blocked on BLPOP, BRPOP, etc.",
			nil, nil,
		),
		commandErrorsTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "command_errors_total"),
			"Total command errors by type",
			[]string{"command"}, nil,
		),

		// Memcached-specific
		casHitsTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "cas_hits_total"),
			"Total CAS command hits",
			nil, nil,
		),
		casMissesTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "cas_misses_total"),
			"Total CAS command misses",
			nil, nil,
		),
		deleteHitsTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "delete_hits_total"),
			"Total DELETE command hits",
			nil, nil,
		),
		deleteMissesTotal: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "delete_misses_total"),
			"Total DELETE command misses",
			nil, nil,
		),
	}

	return c
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.connectionsCurrent
	ch <- c.connectionsTotal
	ch <- c.commandsTotal
	ch <- c.cacheHitsTotal
	ch <- c.cacheMissesTotal
	ch <- c.cacheHitRate
	ch <- c.evictionsTotal
	ch <- c.reclaimedTotal
	ch <- c.itemsCount
	ch <- c.releaseQueueDepth
	ch <- c.memoryBytes
	ch <- c.memoryMaxBytes
	ch <- c.arenaBytes
	ch <- c.vertexCount
	ch <- c.latencySeconds
	ch <- c.adaptiveKThreshold
	ch <- c.graduationRate
	ch <- c.rateLow
	ch <- c.rateHigh
	ch <- c.evictedUnprotected
	ch <- c.evictedProtected
	ch <- c.reachedProtected
	ch <- c.windowHitRate
	ch <- c.ghostCount
	ch <- c.bytesReadTotal
	ch <- c.bytesWrittenTotal
	ch <- c.uptimeSeconds

	// Redis-specific
	if c.provider.Subsystem() == "redis" {
		ch <- c.blockedClients
		ch <- c.commandErrorsTotal
	}

	// Memcached-specific
	if c.provider.Subsystem() == "memcached" {
		ch <- c.casHitsTotal
		ch <- c.casMissesTotal
		ch <- c.deleteHitsTotal
		ch <- c.deleteMissesTotal
	}

}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	// Connection stats
	ch <- prometheus.MustNewConstMetric(c.connectionsCurrent, prometheus.GaugeValue, float64(c.provider.CurrentConnections()))
	ch <- prometheus.MustNewConstMetric(c.connectionsTotal, prometheus.CounterValue, float64(c.provider.TotalConnections()))

	// Command stats
	for cmd, count := range c.provider.CommandCounts() {
		ch <- prometheus.MustNewConstMetric(c.commandsTotal, prometheus.CounterValue, float64(count), cmd)
	}

	// Cache stats
	ch <- prometheus.MustNewConstMetric(c.cacheHitsTotal, prometheus.CounterValue, float64(c.provider.CacheHits()))
	ch <- prometheus.MustNewConstMetric(c.cacheMissesTotal, prometheus.CounterValue, float64(c.provider.CacheMisses()))
	ch <- prometheus.MustNewConstMetric(c.cacheHitRate, prometheus.GaugeValue, c.provider.HitRate())
	ch <- prometheus.MustNewConstMetric(c.evictionsTotal, prometheus.CounterValue, float64(c.provider.Evictions()))
	ch <- prometheus.MustNewConstMetric(c.reclaimedTotal, prometheus.CounterValue, float64(c.provider.Reclaimed()))
	ch <- prometheus.MustNewConstMetric(c.itemsCount, prometheus.GaugeValue, float64(c.provider.ItemCount()))
	ch <- prometheus.MustNewConstMetric(c.releaseQueueDepth, prometheus.GaugeValue, float64(c.provider.ReleaseQueueDepth()))
	ch <- prometheus.MustNewConstMetric(c.memoryBytes, prometheus.GaugeValue, float64(c.provider.MemoryBytes()))
	ch <- prometheus.MustNewConstMetric(c.memoryMaxBytes, prometheus.GaugeValue, float64(c.provider.MaxMemoryBytes()))
	ch <- prometheus.MustNewConstMetric(c.arenaBytes, prometheus.GaugeValue, float64(c.provider.ArenaBytes()))
	ch <- prometheus.MustNewConstMetric(c.vertexCount, prometheus.GaugeValue, float64(c.provider.VertexCount()))

	// Latency stats
	ch <- prometheus.MustNewConstMetric(c.latencySeconds, prometheus.GaugeValue, c.provider.GetLatencyP50(), "get", "0.5")
	ch <- prometheus.MustNewConstMetric(c.latencySeconds, prometheus.GaugeValue, c.provider.GetLatencyP99(), "get", "0.99")
	ch <- prometheus.MustNewConstMetric(c.latencySeconds, prometheus.GaugeValue, c.provider.SetLatencyP50(), "set", "0.5")
	ch <- prometheus.MustNewConstMetric(c.latencySeconds, prometheus.GaugeValue, c.provider.SetLatencyP99(), "set", "0.99")
	ch <- prometheus.MustNewConstMetric(c.latencySeconds, prometheus.GaugeValue, c.provider.CmdLatencyP50(), "cmd", "0.5")
	ch <- prometheus.MustNewConstMetric(c.latencySeconds, prometheus.GaugeValue, c.provider.CmdLatencyP99(), "cmd", "0.99")

	// Adaptive eviction internals (K plus the graduation-rate / learned-threshold
	// state that drives it, per domain).
	for _, s := range c.provider.AdaptiveStats() {
		shard := itoa(s.ShardID)
		ch <- prometheus.MustNewConstMetric(c.adaptiveKThreshold, prometheus.GaugeValue, float64(s.K), shard)
		ch <- prometheus.MustNewConstMetric(c.graduationRate, prometheus.GaugeValue, s.GraduationRate, shard)
		ch <- prometheus.MustNewConstMetric(c.rateLow, prometheus.GaugeValue, s.LearnedRateLow, shard)
		ch <- prometheus.MustNewConstMetric(c.rateHigh, prometheus.GaugeValue, s.LearnedRateHigh, shard)
		ch <- prometheus.MustNewConstMetric(c.evictedUnprotected, prometheus.GaugeValue, float64(s.EvictedUnprotected), shard)
		ch <- prometheus.MustNewConstMetric(c.evictedProtected, prometheus.GaugeValue, float64(s.EvictedProtected), shard)
		ch <- prometheus.MustNewConstMetric(c.reachedProtected, prometheus.GaugeValue, float64(s.ReachedProtected), shard)
		ch <- prometheus.MustNewConstMetric(c.windowHitRate, prometheus.GaugeValue, s.WindowHitRate, shard)
		ch <- prometheus.MustNewConstMetric(c.ghostCount, prometheus.GaugeValue, float64(s.GhostCount), shard)
	}

	// Network stats
	ch <- prometheus.MustNewConstMetric(c.bytesReadTotal, prometheus.CounterValue, float64(c.provider.BytesRead()))
	ch <- prometheus.MustNewConstMetric(c.bytesWrittenTotal, prometheus.CounterValue, float64(c.provider.BytesWritten()))

	// Uptime
	ch <- prometheus.MustNewConstMetric(c.uptimeSeconds, prometheus.GaugeValue, c.provider.UptimeSeconds())

	// Redis-specific
	if c.provider.Subsystem() == "redis" {
		ch <- prometheus.MustNewConstMetric(c.blockedClients, prometheus.GaugeValue, float64(c.provider.BlockedClients()))
		for cmd, count := range c.provider.CommandErrors() {
			ch <- prometheus.MustNewConstMetric(c.commandErrorsTotal, prometheus.CounterValue, float64(count), cmd)
		}
	}

	// Memcached-specific
	if c.provider.Subsystem() == "memcached" {
		ch <- prometheus.MustNewConstMetric(c.casHitsTotal, prometheus.CounterValue, float64(c.provider.CasHits()))
		ch <- prometheus.MustNewConstMetric(c.casMissesTotal, prometheus.CounterValue, float64(c.provider.CasMisses()))
		ch <- prometheus.MustNewConstMetric(c.deleteHitsTotal, prometheus.CounterValue, float64(c.provider.DeleteHits()))
		ch <- prometheus.MustNewConstMetric(c.deleteMissesTotal, prometheus.CounterValue, float64(c.provider.DeleteMisses()))
	}

}

// itoa converts an int to a string.
func itoa(i int) string {
	return strconv.Itoa(i)
}
