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

package cache

import (
	"log/slog"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

// GetAvailableMemory returns the total memory available to this process.
// It checks (in order):
//  1. Cgroup v2 memory limit (containers)
//  2. Cgroup v1 memory limit (older containers)
//  3. Total system RAM via /proc/meminfo (Linux)
//  4. Fallback: estimate from runtime (all platforms)
func GetAvailableMemory() uint64 {
	if mem := getCgroupV2MemoryLimit(); mem > 0 {
		return mem
	}
	if mem := getCgroupV1MemoryLimit(); mem > 0 {
		return mem
	}
	if mem := getProcMemTotal(); mem > 0 {
		return mem
	}
	// Fallback: use Go's view of available memory
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Sys + 1024*1024*1024 // Sys + 1GB headroom estimate
}

// getCgroupV2MemoryLimit reads the cgroup v2 memory limit.
func getCgroupV2MemoryLimit() uint64 {
	data, err := os.ReadFile("/sys/fs/cgroup/memory.max")
	if err != nil {
		return 0
	}
	s := strings.TrimSpace(string(data))
	if s == "max" {
		return 0 // no limit set
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

// getCgroupV1MemoryLimit reads the cgroup v1 memory limit.
func getCgroupV1MemoryLimit() uint64 {
	data, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes")
	if err != nil {
		return 0
	}
	s := strings.TrimSpace(string(data))
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	// Cgroup v1 returns a huge number when no limit is set
	if v > uint64(1)<<62 {
		return 0
	}
	return v
}

// getProcMemTotal reads total memory from /proc/meminfo.
func getProcMemTotal() uint64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, err := strconv.ParseUint(fields[1], 10, 64)
				if err == nil {
					return kb * 1024
				}
			}
		}
	}
	return 0
}

// GetProcessRSS returns the current resident set size (actual physical memory used)
// of this process. Falls back to runtime.MemStats.Sys if /proc is unavailable.
func GetProcessRSS() uint64 {
	// Try /proc/self/statm (field 1 is RSS in pages)
	data, err := os.ReadFile("/proc/self/statm")
	if err == nil {
		fields := strings.Fields(string(data))
		if len(fields) >= 2 {
			pages, err := strconv.ParseUint(fields[1], 10, 64)
			if err == nil {
				return pages * uint64(os.Getpagesize())
			}
		}
	}
	// Fallback
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Sys
}

// enforceMemoryWithTarget is the shared implementation for both percentage-based
// and absolute memory enforcement. It sets GOMEMLIMIT, starts the ghost sweeper,
// and launches the RSS-based capacity adjustment loop.
func (c *CloxCache[K, V]) enforceMemoryWithTarget(targetBytes, avgItemSize int64) {
	c.memoryLimit.Store(targetBytes)

	// Configure GOMEMLIMIT if not already set.
	// SetMemoryLimit(-1) returns the current limit without changing it.
	// math.MaxInt64 means "no limit" (the default).
	currentGoLimit := debug.SetMemoryLimit(-1)
	if currentGoLimit == math.MaxInt64 {
		// Reserve 5% of target for non-heap memory (stacks, mmapped regions, etc.)
		goMemLimit := targetBytes * 95 / 100
		debug.SetMemoryLimit(goMemLimit)
		slog.Debug("set GOMEMLIMIT", "limit", FormatMemory(uint64(goMemLimit)))
	} else {
		slog.Debug("GOMEMLIMIT already set", "limit", FormatMemory(uint64(currentGoLimit)))
	}

	// Start the ghost sweeper so ghosts get culled under memory pressure.
	// Uses the same tick rate as the memory enforcer.
	c.StartGhostSweeper(time.Second)

	c.wg.Add(1)
	go c.memoryTargetLoop(targetBytes, avgItemSize)
}

// EnforceMemoryTarget starts a background goroutine that keeps the process's total
// memory usage at approximately targetPercent of available system memory.
//
// targetPercent is a fraction (0.0 to 1.0), e.g., 0.9 means 90%.
// avgItemSize is the estimated average memory per cache entry (used when sizeFunc is not set).
//
// This also configures GOMEMLIMIT (runtime/debug.SetMemoryLimit) to the target bytes
// if it hasn't been explicitly set, so the Go GC cooperates with the memory target.
//
// Designed for nearcache deployments where the cache sits beside an application
// and must share available memory without knowing the exact budget at startup.
func (c *CloxCache[K, V]) EnforceMemoryTarget(targetPercent float64, avgItemSize int64) {
	if targetPercent <= 0 || targetPercent > 1.0 || avgItemSize <= 0 {
		return
	}

	availableMemory := GetAvailableMemory()
	targetBytes := int64(float64(availableMemory) * targetPercent)

	slog.Debug("memory target configured",
		"available_memory", FormatMemory(availableMemory),
		"target_percent", targetPercent,
		"target_bytes", FormatMemory(uint64(targetBytes)))

	c.enforceMemoryWithTarget(targetBytes, avgItemSize)
}

// EnforceAbsoluteMemoryLimit starts a background goroutine that keeps the
// process's total memory usage at approximately targetBytes. Uses process RSS
// to drive capacity adjustments, sets GOMEMLIMIT, and starts the ghost sweeper.
//
// Use this when the caller knows an exact byte budget (e.g. --maxmemory 1gb)
// rather than a fraction of available memory.
func (c *CloxCache[K, V]) EnforceAbsoluteMemoryLimit(targetBytes, avgItemSize int64) {
	if targetBytes <= 0 || avgItemSize <= 0 {
		return
	}

	slog.Debug("absolute memory limit configured",
		"target_bytes", FormatMemory(uint64(targetBytes)))

	c.enforceMemoryWithTarget(targetBytes, avgItemSize)
}

func (c *CloxCache[K, V]) memoryTargetLoop(targetBytes, avgItemSize int64) {
	defer c.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			c.adjustForMemoryTarget(targetBytes, avgItemSize)
		}
	}
}

func (c *CloxCache[K, V]) adjustForMemoryTarget(targetBytes, avgItemSize int64) {
	processRSS := int64(GetProcessRSS())
	entryCount := int64(c.EntryCount())

	if entryCount == 0 {
		return
	}

	// Estimate how much of the process memory is cache vs application.
	// cacheBytes = tracked bytes if sizeFunc is set, else entryCount * avgItemSize.
	var cacheBytes int64
	if c.sizeFunc != nil {
		cacheBytes = c.bytes.Load()
	}
	if cacheBytes <= 0 {
		cacheBytes = entryCount * avgItemSize
	}

	// Non-cache memory = process RSS minus cache bytes
	nonCacheBytes := max(processRSS-cacheBytes, 0)

	// How much memory can the cache use?
	cacheTarget := max(targetBytes-nonCacheBytes, avgItemSize*int64(c.numShards))

	// Calculate actual average item size for capacity planning
	actualAvgSize := avgItemSize
	if cacheBytes > 0 && entryCount > 0 {
		actualAvgSize = max(cacheBytes/entryCount, 64)
	}

	targetCapacity := max(cacheTarget/actualAvgSize, int64(c.numShards))
	currentCapacity := int64(c.Capacity())

	// Usage ratio: how close process memory is to the target
	usageRatio := float64(processRSS) / float64(targetBytes)

	slog.Debug("memory target check",
		"process_rss", FormatMemory(uint64(processRSS)),
		"target", FormatMemory(uint64(targetBytes)),
		"usage_ratio", usageRatio,
		"cache_bytes", FormatMemory(uint64(cacheBytes)),
		"non_cache_bytes", FormatMemory(uint64(nonCacheBytes)),
		"cache_target", FormatMemory(uint64(cacheTarget)),
		"capacity", currentCapacity,
		"target_capacity", targetCapacity)

	if usageRatio > 1.0 {
		// Over target — shrink aggressively. The more over, the more aggressive.
		// At 110% usage, reduce by up to 20%; at 150%, reduce by up to 50%.
		shrinkFactor := min(usageRatio-1.0, 0.5) + 0.1 // 10-60% reduction
		maxReduction := int64(float64(currentCapacity) * (1.0 - shrinkFactor))
		newCapacity := max(min(targetCapacity, maxReduction), int64(c.numShards))

		if newCapacity < currentCapacity {
			slog.Debug("memory pressure: reducing capacity",
				"from", currentCapacity, "to", newCapacity,
				"usage_ratio", usageRatio)
			c.SetCapacity(int(newCapacity))
		}
	} else if usageRatio < 0.85 && targetCapacity > currentCapacity {
		// Cap growth at 4x actual entry count so we don't pre-allocate
		// hundreds of thousands of empty slots on a nearly-empty cache.
		usageCap := max(entryCount*4, int64(c.numShards))
		growTarget := min(targetCapacity, usageCap)
		if growTarget <= currentCapacity {
			return
		}
		maxIncrease := currentCapacity + max(1, currentCapacity/10)
		newCapacity := min(growTarget, maxIncrease)
		slog.Debug("headroom available: increasing capacity",
			"from", currentCapacity, "to", newCapacity,
			"usage_ratio", usageRatio)
		c.SetCapacity(int(newCapacity))
	}
}
