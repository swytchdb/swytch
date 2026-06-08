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
	"runtime/debug"
	"testing"
)

func TestGetAvailableMemory(t *testing.T) {
	mem := GetAvailableMemory()
	if mem == 0 {
		t.Fatal("GetAvailableMemory returned 0")
	}
	t.Logf("available memory: %s", FormatMemory(mem))

	// Sanity check: should be at least 64MB and less than 1TB
	if mem < 64*1024*1024 {
		t.Errorf("available memory suspiciously low: %d bytes", mem)
	}
	if mem > 1024*1024*1024*1024 {
		t.Errorf("available memory suspiciously high: %d bytes", mem)
	}
}

func TestEnforceMemoryTarget_SetsGOMEMLIMIT(t *testing.T) {
	// Save and restore GOMEMLIMIT
	origLimit := debug.SetMemoryLimit(-1) // read current
	defer debug.SetMemoryLimit(origLimit)

	// Reset to "unset" (math.MaxInt64 means no limit)
	debug.SetMemoryLimit(-1)

	cfg := ConfigFromCapacity(10000)
	cfg.CollectStats = true
	c := NewCloxCache[string, []byte](cfg)
	defer c.Close()

	c.EnforceMemoryTarget(0.5, 256) // 50% target

	// GOMEMLIMIT should now be set
	newLimit := debug.SetMemoryLimit(-1)
	if newLimit <= 0 {
		t.Errorf("GOMEMLIMIT was not set, got %d", newLimit)
	}

	availMem := GetAvailableMemory()
	expectedLimit := int64(float64(availMem) * 0.5)

	// Should be within 10% of expected
	if newLimit < expectedLimit*85/100 || newLimit > expectedLimit*115/100 {
		t.Errorf("GOMEMLIMIT %d not close to expected %d (50%% of %d)",
			newLimit, expectedLimit, availMem)
	}
	t.Logf("GOMEMLIMIT set to %s (target 50%% of %s = %s)",
		FormatMemory(uint64(newLimit)), FormatMemory(availMem), FormatMemory(uint64(expectedLimit)))
}

func TestEnforceMemoryTarget_DoesNotOverrideExistingGOMEMLIMIT(t *testing.T) {
	// Save and restore GOMEMLIMIT
	origLimit := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(origLimit)

	// Set an explicit limit
	customLimit := int64(256 * 1024 * 1024) // 256MB
	debug.SetMemoryLimit(customLimit)

	cfg := ConfigFromCapacity(10000)
	c := NewCloxCache[string, []byte](cfg)
	defer c.Close()

	c.EnforceMemoryTarget(0.9, 256)

	// GOMEMLIMIT should still be our custom value
	currentLimit := debug.SetMemoryLimit(-1)
	if currentLimit != customLimit {
		t.Errorf("GOMEMLIMIT was overridden: got %d, expected %d", currentLimit, customLimit)
	}
}

func TestEnforceMemoryTarget_InvalidPercent(t *testing.T) {
	cfg := ConfigFromCapacity(1000)
	c := NewCloxCache[string, []byte](cfg)
	defer c.Close()

	// Should not panic, just no-op
	c.EnforceMemoryTarget(0, 256)
	c.EnforceMemoryTarget(-0.5, 256)
	c.EnforceMemoryTarget(1.5, 256)
}

func TestEnforceMemoryTarget_AdjustsCapacity(t *testing.T) {
	cfg := ConfigFromCapacity(100000)
	cfg.CollectStats = true
	c := NewCloxCache[string, []byte](cfg)
	c.SetSizeFunc(func(k string, v []byte) int64 {
		return int64(len(k) + len(v))
	})
	defer c.Close()

	// Fill the cache with data
	value := make([]byte, 1024) // 1KB values
	for i := range 80000 {
		key := string(rune('A'+i%26)) + string(rune('0'+i/26%10)) + string(rune(i))
		c.Put(key, value)
	}

	initialCapacity := c.Capacity()
	t.Logf("initial: capacity=%d, bytes=%s, entries=%d",
		initialCapacity, FormatMemory(uint64(c.Bytes())), c.EntryCount())

	// Directly call adjustForMemoryTarget with a very low target
	// to simulate memory pressure without depending on system memory size.
	// Target = 1MB, which is way below process RSS, forcing capacity reduction.
	tinyTarget := int64(1 * 1024 * 1024) // 1MB
	c.adjustForMemoryTarget(tinyTarget, 1024)

	afterShrink := c.Capacity()
	t.Logf("after shrink: capacity=%d (was %d)", afterShrink, initialCapacity)

	if afterShrink >= initialCapacity {
		t.Errorf("expected capacity to decrease under memory pressure, got %d >= %d",
			afterShrink, initialCapacity)
	}

	// Now call with a very high target to verify it can grow back
	hugeTarget := int64(100 * 1024 * 1024 * 1024) // 100GB
	c.adjustForMemoryTarget(hugeTarget, 1024)

	afterGrow := c.Capacity()
	t.Logf("after grow: capacity=%d (was %d)", afterGrow, afterShrink)

	if afterGrow <= afterShrink {
		t.Errorf("expected capacity to increase with headroom, got %d <= %d",
			afterGrow, afterShrink)
	}
}

func TestEnforceMemoryTarget_StoresMemoryLimit(t *testing.T) {
	cfg := ConfigFromCapacity(1000)
	c := NewCloxCache[string, []byte](cfg)
	defer c.Close()

	c.EnforceMemoryTarget(0.8, 256)

	limit := c.MemoryLimit()
	if limit <= 0 {
		t.Errorf("expected positive memory limit, got %d", limit)
	}

	availMem := GetAvailableMemory()
	expected := int64(float64(availMem) * 0.8)

	// Should be within 10%
	if limit < expected*85/100 || limit > expected*115/100 {
		t.Errorf("memory limit %d not close to expected %d", limit, expected)
	}
	t.Logf("memory limit: %s (80%% of %s)", FormatMemory(uint64(limit)), FormatMemory(availMem))
}
