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
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCloxCacheBasicOperations(t *testing.T) {
	cfg := Config{
		NumShards:     16,
		SlotsPerShard: 256,
	}
	cache := NewCloxCache[uint64, string](cfg)
	defer cache.Close()

	// Test Put and Get
	key := uint64(42)
	value := "test-value"

	if success, _, _, _ := cache.Put(key, value); !success {
		t.Fatal("Put failed")
	}

	got, ok := cache.Get(key, 0)
	if !ok {
		t.Fatal("Get failed: key not found")
	}
	if got != value {
		t.Fatalf("Get returned wrong value: got %q, want %q", got, value)
	}

	// Test Get on non-existent key
	_, ok = cache.Get(uint64(99999), 0)
	if ok {
		t.Fatal("Get succeeded on non-existent key")
	}
}

func TestCloxCacheUpdate(t *testing.T) {
	cfg := Config{
		NumShards:     16,
		SlotsPerShard: 256,
	}
	cache := NewCloxCache[uint64, int](cfg)
	defer cache.Close()

	key := uint64(1)

	// Insert initial value
	cache.Put(key, 1)

	// Update value
	cache.Put(key, 2)
	cache.Put(key, 3)

	// Verify final value
	got, ok := cache.Get(key, 0)
	if !ok {
		t.Fatal("Get failed after update")
	}
	if got != 3 {
		t.Fatalf("Get returned wrong value: got %d, want %d", got, 3)
	}
}

func TestCloxCacheConcurrentAccess(t *testing.T) {
	const numGoroutines = 100
	const numOps = 1000
	totalKeys := numGoroutines * numOps // 100,000

	cfg := Config{
		NumShards:     64,
		SlotsPerShard: 2048,
		Capacity:      totalKeys * 2, // Extra capacity to prevent eviction during concurrent writes
	}
	cache := NewCloxCache[uint64, int](cfg)
	defer cache.Close()

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrent writes
	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()
			for j := range numOps {
				key := uint64(id*numOps + j)
				cache.Put(key, int(key))
			}
		}(i)
	}

	wg.Wait()

	// Verify some values
	for i := range 10 {
		key := uint64(i * numOps)
		val, ok := cache.Get(key, 0)
		if !ok {
			t.Errorf("Key %d not found", key)
			continue
		}
		if val != int(key) {
			t.Errorf("Wrong value for key %d: got %d, want %d", key, val, int(key))
		}
	}
}

func TestCloxCacheConcurrentReadWrite(t *testing.T) {
	cfg := Config{
		NumShards:     32,
		SlotsPerShard: 512,
		CollectStats:  true,
	}
	cache := NewCloxCache[uint64, int](cfg)
	defer cache.Close()

	const numKeys = 100

	// Pre-populate cache
	for i := range numKeys {
		cache.Put(uint64(i), i)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Start readers
	wg.Add(50)
	for range 50 {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for j := range numKeys {
						cache.Get(uint64(j), 0)
					}
				}
			}
		}()
	}

	// Start writers
	wg.Add(10)
	for i := range 10 {
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for j := range numKeys {
						cache.Put(uint64(j), id*numKeys+j)
					}
				}
			}
		}(i)
	}

	// Run for a short duration
	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Verify cache is still functional
	hits, misses, evictions := cache.Stats()
	t.Logf("Stats: hits=%d, misses=%d, evictions=%d", hits, misses, evictions)

	if hits == 0 {
		t.Error("Expected some cache hits")
	}
}

func TestCloxCacheFrequencyIncrement(t *testing.T) {
	cfg := Config{
		NumShards:     4,
		SlotsPerShard: 16,
	}
	cache := NewCloxCache[uint64, int](cfg)
	defer cache.Close()

	key := uint64(77)
	cache.Put(key, 42)

	// Access the key multiple times to bump frequency
	for range 20 {
		cache.Get(key, 0)
	}

	// Verify key is still there (high frequency should protect it)
	val, ok := cache.Get(key, 0)
	if !ok {
		t.Fatal("Hot key was evicted")
	}
	if val != 42 {
		t.Fatalf("Wrong value: got %d, want 42", val)
	}
}

func TestCloxCacheEviction(t *testing.T) {
	cfg := Config{
		NumShards:     4,
		SlotsPerShard: 16, // Small cache
		CollectStats:  true,
	}
	cache := NewCloxCache[uint64, int](cfg)
	defer cache.Close()

	// Fill cache beyond capacity
	const numKeys = 200
	for i := range numKeys {
		cache.Put(uint64(i), i)
	}

	// Wait for sweeper to run
	time.Sleep(200 * time.Millisecond)

	// Check evictions occurred
	_, _, evictions := cache.Stats()
	if evictions == 0 {
		t.Log("Warning: No evictions recorded yet (sweeper may not have run)")
	}

	// Cache should still be functional
	val, ok := cache.Get(uint64(100), 0)
	if ok && val != 100 {
		t.Fatalf("Wrong value: got %d, want 100", val)
	}
}

func TestCloxCacheStats(t *testing.T) {
	cfg := Config{
		NumShards:     8,
		SlotsPerShard: 64,
		CollectStats:  true,
	}
	cache := NewCloxCache[uint64, string](cfg)
	defer cache.Close()

	// Generate some hits and misses
	cache.Put(uint64(1), "value1")
	cache.Put(uint64(2), "value2")

	cache.Get(uint64(1), 0) // hit
	cache.Get(uint64(2), 0) // hit
	cache.Get(uint64(3), 0) // miss

	hits, misses, _ := cache.Stats()

	if hits != 2 {
		t.Errorf("Expected 2 hits, got %d", hits)
	}
	if misses != 1 {
		t.Errorf("Expected 1 miss, got %d", misses)
	}
}

func TestCloxCachePointerTypes(t *testing.T) {
	type Record struct {
		ID   int
		Data string
	}

	cfg := Config{
		NumShards:     16,
		SlotsPerShard: 256,
	}
	cache := NewCloxCache[uint64, *Record](cfg)
	defer cache.Close()

	key := uint64(1)
	record := &Record{ID: 1, Data: "test data"}

	cache.Put(key, record)

	got, ok := cache.Get(key, 0)
	if !ok {
		t.Fatal("Get failed")
	}

	if got.ID != record.ID || got.Data != record.Data {
		t.Fatalf("Got wrong record: %+v, want %+v", got, record)
	}

	// Verify it's the same pointer
	if got != record {
		t.Fatal("Expected same pointer")
	}
}

func TestCloxCacheHashCollisions(t *testing.T) {
	cfg := Config{
		NumShards:     2,
		SlotsPerShard: 4, // Very small to force collisions
	}
	cache := NewCloxCache[uint64, int](cfg)
	defer cache.Close()

	// Insert many keys (will likely collide in such a small cache)
	const numKeys = 50
	for i := range numKeys {
		cache.Put(uint64(i), i)
	}

	// Verify we can retrieve keys despite collisions
	retrieved := 0
	for i := range numKeys {
		val, ok := cache.Get(uint64(i), 0)
		if ok {
			retrieved++
			if val != i {
				t.Errorf("Wrong value for key %d: got %d, want %d", i, val, i)
			}
		}
	}

	t.Logf("Retrieved %d/%d keys with collisions", retrieved, numKeys)
	if retrieved == 0 {
		t.Error("Expected to retrieve at least some keys")
	}
}

func TestCloxCacheInvalidConfig(t *testing.T) {
	tests := []struct {
		name      string
		cfg       Config
		wantPanic string
	}{
		{
			name:      "Zero NumShards",
			cfg:       Config{NumShards: 0, SlotsPerShard: 256},
			wantPanic: "NumShards must be positive",
		},
		{
			name:      "Negative NumShards",
			cfg:       Config{NumShards: -1, SlotsPerShard: 256},
			wantPanic: "NumShards must be positive",
		},
		{
			name:      "Zero SlotsPerShard",
			cfg:       Config{NumShards: 16, SlotsPerShard: 0},
			wantPanic: "SlotsPerShard must be positive",
		},
		{
			name:      "Negative SlotsPerShard",
			cfg:       Config{NumShards: 16, SlotsPerShard: -1},
			wantPanic: "SlotsPerShard must be positive",
		},
		{
			name:      "NumShards not power of 2",
			cfg:       Config{NumShards: 15, SlotsPerShard: 256},
			wantPanic: "NumShards must be a power of 2",
		},
		{
			name:      "SlotsPerShard not power of 2",
			cfg:       Config{NumShards: 16, SlotsPerShard: 255},
			wantPanic: "SlotsPerShard must be a power of 2",
		},
		{
			name:      "Both invalid",
			cfg:       Config{NumShards: 0, SlotsPerShard: 0},
			wantPanic: "NumShards must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("Expected panic but got none")
				}
				panicMsg := fmt.Sprint(r)
				if panicMsg != tt.wantPanic {
					t.Errorf("Wrong panic message: got %q, want %q", panicMsg, tt.wantPanic)
				}
			}()

			// This should panic
			_ = NewCloxCache[uint64, int](tt.cfg)
		})
	}
}

func TestCloxCacheUint64Keys(t *testing.T) {
	cfg := Config{
		NumShards:     16,
		SlotsPerShard: 256,
	}
	cache := NewCloxCache[uint64, string](cfg)
	defer cache.Close()

	// Test Put and Get with uint64 keys
	key := uint64(12345)
	value := "test-value"

	if success, _, _, _ := cache.Put(key, value); !success {
		t.Fatal("Put failed")
	}

	got, ok := cache.Get(key, 0)
	if !ok {
		t.Fatal("Get failed: key not found")
	}
	if got != value {
		t.Fatalf("Get returned wrong value: got %q, want %q", got, value)
	}

	// Test Get on non-existent key
	_, ok = cache.Get(uint64(99999), 0)
	if ok {
		t.Fatal("Get succeeded on non-existent key")
	}

	// Test update
	cache.Put(key, "updated-value")
	got, ok = cache.Get(key, 0)
	if !ok {
		t.Fatal("Get failed after update")
	}
	if got != "updated-value" {
		t.Fatalf("Get returned wrong value after update: got %q, want %q", got, "updated-value")
	}
}

func TestCloxCacheUint64KeysSlotOffset(t *testing.T) {
	// This test simulates the FasterLog index use case:
	// slot (uint64) -> offset (uint64)
	cfg := Config{
		NumShards:     64,
		SlotsPerShard: 1024,
		Capacity:      10000,
	}
	cache := NewCloxCache[uint64, uint64](cfg)
	defer cache.Close()

	// Insert slot -> offset mappings
	numEntries := 5000
	for i := range numEntries {
		slot := uint64(i)
		offset := uint64(i * 100) // simulate offset
		if success, _, _, _ := cache.Put(slot, offset); !success {
			t.Fatalf("Put failed for slot %d", slot)
		}
	}

	// Verify all mappings
	for i := range numEntries {
		slot := uint64(i)
		expectedOffset := uint64(i * 100)
		got, ok := cache.Get(slot, 0)
		if !ok {
			t.Fatalf("Get failed for slot %d", slot)
		}
		if got != expectedOffset {
			t.Fatalf("Wrong offset for slot %d: got %d, want %d", slot, got, expectedOffset)
		}
	}
}

func TestCloxCacheUint64KeysConcurrent(t *testing.T) {
	cfg := Config{
		NumShards:     64,
		SlotsPerShard: 1024,
		Capacity:      50000,
	}
	cache := NewCloxCache[uint64, uint64](cfg)
	defer cache.Close()

	// Concurrent writers
	var wg sync.WaitGroup
	numWriters := 8
	entriesPerWriter := 5000

	for w := range numWriters {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			base := uint64(workerID * entriesPerWriter)
			for i := range entriesPerWriter {
				slot := base + uint64(i)
				offset := slot * 100
				cache.Put(slot, offset)
			}
		}(w)
	}

	wg.Wait()

	// Verify entries from each worker
	for w := range numWriters {
		base := uint64(w * entriesPerWriter)
		for i := range entriesPerWriter {
			slot := base + uint64(i)
			expectedOffset := slot * 100
			got, ok := cache.Get(slot, 0)
			if !ok {
				t.Errorf("Get failed for slot %d (worker %d)", slot, w)
				continue
			}
			if got != expectedOffset {
				t.Errorf("Wrong offset for slot %d: got %d, want %d", slot, got, expectedOffset)
			}
		}
	}
}

func TestCloxCacheUint64KeysCompareAndSwap(t *testing.T) {
	cfg := Config{
		NumShards:     16,
		SlotsPerShard: 256,
	}
	cache := NewCloxCache[uint64, uint64](cfg)
	defer cache.Close()

	slot := uint64(42)
	initialOffset := uint64(1000)

	// Insert initial value
	cache.Put(slot, initialOffset)

	// CAS with correct expected value should succeed
	swapped, current, exists, _ := cache.CompareAndSwap(slot, initialOffset, uint64(2000))
	if !swapped {
		t.Fatalf("CAS failed: expected swap to succeed, got current=%d, exists=%v", current, exists)
	}
	if current != 2000 {
		t.Fatalf("CAS returned wrong current value: got %d, want 2000", current)
	}

	// CAS with wrong expected value should fail
	swapped, current, exists, _ = cache.CompareAndSwap(slot, initialOffset, uint64(3000))
	if swapped {
		t.Fatal("CAS should have failed with wrong expected value")
	}
	if !exists {
		t.Fatal("CAS should report key exists")
	}
	if current != 2000 {
		t.Fatalf("CAS returned wrong current value: got %d, want 2000", current)
	}

	// CAS on non-existent key
	swapped, _, exists, _ = cache.CompareAndSwap(uint64(99999), uint64(100), uint64(200))
	if swapped {
		t.Fatal("CAS should fail on non-existent key")
	}
	if exists {
		t.Fatal("CAS should report key does not exist")
	}
}

func TestCloxCacheEvict(t *testing.T) {
	cfg := Config{
		NumShards:     16,
		SlotsPerShard: 256,
	}
	cache := NewCloxCache[uint64, string](cfg)
	defer cache.Close()

	key := uint64(42)
	value := "evict-test-value"

	if success, _, _, _ := cache.Put(key, value); !success {
		t.Fatal("Put failed")
	}

	if _, ok := cache.Get(key, 0); !ok {
		t.Fatal("Get failed before evict")
	}

	if !cache.Evict(key) {
		t.Fatal("Evict returned false for existing key")
	}

	if _, ok := cache.Get(key, 0); ok {
		t.Fatal("Get succeeded after evict - should have been ghosted")
	}

	if cache.Evict(uint64(99999)) {
		t.Fatal("Evict returned true for non-existent key")
	}
}

func TestCloxCacheEvictWithSizeFunc(t *testing.T) {
	cfg := Config{
		NumShards:     16,
		SlotsPerShard: 256,
	}
	cache := NewCloxCache[uint64, uint64](cfg)
	defer cache.Close()

	cache.SetSizeFunc(func(key uint64, value uint64) int64 {
		return 16 // 8 bytes key + 8 bytes value
	})

	key := uint64(42)
	value := uint64(100)

	cache.Put(key, value)
	bytesAfterPut := cache.Bytes()
	if bytesAfterPut == 0 {
		t.Fatal("Bytes should be > 0 after Put")
	}

	cache.Evict(key)
	bytesAfterEvict := cache.Bytes()
	if bytesAfterEvict != 0 {
		t.Fatalf("Bytes should be 0 after Evict, got %d", bytesAfterEvict)
	}
}

func TestCloxCacheArrayKey(t *testing.T) {
	// Test with [2]uint64 keys (the TipRef/EffectSeq use case)
	cfg := Config{
		NumShards:     16,
		SlotsPerShard: 256,
	}
	cache := NewCloxCache[[2]uint64, string](cfg)
	defer cache.Close()

	key1 := [2]uint64{1, 100}
	key2 := [2]uint64{2, 200}
	key3 := [2]uint64{1, 100} // same as key1

	cache.Put(key1, "first")
	cache.Put(key2, "second")

	got, ok := cache.Get(key1, 0)
	if !ok {
		t.Fatal("Get failed for key1")
	}
	if got != "first" {
		t.Fatalf("Wrong value for key1: got %q, want %q", got, "first")
	}

	got, ok = cache.Get(key3, 0)
	if !ok {
		t.Fatal("Get failed for key3 (same as key1)")
	}
	if got != "first" {
		t.Fatalf("Wrong value for key3: got %q, want %q", got, "first")
	}

	// Update via equal key
	cache.Put(key3, "updated")
	got, ok = cache.Get(key1, 0)
	if !ok {
		t.Fatal("Get failed after update")
	}
	if got != "updated" {
		t.Fatalf("Wrong value after update: got %q, want %q", got, "updated")
	}
}

// TestCloxCacheEvictDeciderSkipsPinned verifies that a key the decider
// refuses to evict survives memory pressure: it stays in the cache while
// unpinned siblings are evicted to make room for new writes. Uses
// [2]uint64 keys to match production usage of the effects engine cache.
func TestCloxCacheEvictDeciderSkipsPinned(t *testing.T) {
	cfg := Config{
		NumShards:     1,
		SlotsPerShard: 8,
		CollectStats:  true,
	}
	cache := NewCloxCache[[2]uint64, int](cfg)
	defer cache.Close()

	// Pin any key whose first word is 1 (treat as "membership").
	pinned := [2]uint64{1, 99}
	cache.SetEvictDecider(func(key [2]uint64, _ int) bool {
		return key[0] != 1
	})

	// Seed the pinned entry and bump frequency so the protected-freq
	// fallback would also try to evict it — the decider must override.
	cache.Put(pinned, 42)
	for range 5 {
		_, _ = cache.Get(pinned, 0)
	}

	// Fill the cache with unpinned entries until evictions start.
	for i := range uint64(200) {
		cache.Put([2]uint64{2, i}, int(i))
	}

	if _, ok := cache.Get(pinned, 0); !ok {
		t.Fatal("pinned key was evicted; decider veto did not hold")
	}

	_, _, evictions := cache.Stats()
	if evictions == 0 {
		t.Fatal("expected evictions on unpinned keys; got 0")
	}
}

// TestCloxCacheEvictNotifyFires verifies that the post-eviction
// callback receives the key + value of each evicted entry. Used by
// the engine to drop the corresponding key from its index and
// broadcast a cluster-wide unsubscribe.
func TestCloxCacheEvictNotifyFires(t *testing.T) {
	cfg := Config{
		NumShards:     1,
		SlotsPerShard: 8,
		CollectStats:  true,
	}
	cache := NewCloxCache[[2]uint64, int](cfg)
	defer cache.Close()

	var mu sync.Mutex
	evicted := map[[2]uint64]int{}
	cache.SetEvictNotify(func(key [2]uint64, value int) {
		mu.Lock()
		evicted[key] = value
		mu.Unlock()
	})

	// Force evictions by overfilling.
	for i := range uint64(200) {
		cache.Put([2]uint64{2, i}, int(i))
	}

	mu.Lock()
	defer mu.Unlock()
	if len(evicted) == 0 {
		t.Fatal("evictNotify was never called; expected at least one eviction")
	}
	// Spot-check: every notified value matches the key's intended int.
	for k, v := range evicted {
		if int(k[1]) != v {
			t.Fatalf("evictNotify mismatch: key %v reported value %d", k, v)
		}
	}
}

// TestCloxCacheEvictPartialSweepExtends reproduces the
// pin-pressure-in-the-sweep-window bug: with the default partial
// sweep, the chosen window can happen to contain only pinned entries
// even when evictable entries exist elsewhere in the shard. The
// previous behavior returned victim==nil and Put returned false,
// stranding the new key (the effect-cache callers ignore the false
// return and index the effect with no cached bytes). The fix extends
// the scan across remaining slots when the partial sweep is empty;
// every Put against a shard that holds at least one unpinned entry
// must succeed.
func TestCloxCacheEvictPartialSweepExtends(t *testing.T) {
	cfg := Config{
		NumShards:     1,
		SlotsPerShard: 64,
		Capacity:      64,
		SweepPercent:  2, // maxScan = max(64*2/100, 1) = 1 slot
		CollectStats:  true,
	}
	cache := NewCloxCache[[2]uint64, int](cfg)
	defer cache.Close()

	cache.SetEvictDecider(func(key [2]uint64, _ int) bool { return key[0] != 1 })

	for i := range uint64(60) {
		if ok, _, _, _ := cache.Put([2]uint64{1, i}, int(i)); !ok {
			t.Fatalf("seed pinned {1,%d}: Put returned !ok", i)
		}
	}
	for i := range uint64(4) {
		if ok, _, _, _ := cache.Put([2]uint64{2, i}, int(i)); !ok {
			t.Fatalf("seed unpinned {2,%d}: Put returned !ok", i)
		}
	}

	// With a 1-slot sweep window and 4 unpinned entries spread across
	// 64 slots, the partial sweep frequently lands on a pinned slot.
	// Pre-fix this leaves victim==nil and Put fails; post-fix the
	// fallback scan finds one of the unpinned entries and Put succeeds.
	for i := uint64(100); i < 200; i++ {
		ok, _, _, _ := cache.Put([2]uint64{2, i}, int(i))
		if !ok {
			t.Fatalf("Put {2,%d} failed despite unpinned entries in the shard", i)
		}
	}
}

// TestCloxCacheEvictDeciderAllPinnedRefusesEviction verifies the
// "every candidate pinned" path: when nothing in a sweep is evictable,
// the cache returns success=false from Put rather than evicting a
// pinned entry. Cache stays at capacity, no spurious evictions.
func TestCloxCacheEvictDeciderAllPinnedRefusesEviction(t *testing.T) {
	cfg := Config{
		NumShards:     1,
		SlotsPerShard: 4, // tiny — fills fast
		Capacity:      4,
		CollectStats:  true,
		SweepPercent:  100, // scan the whole shard every eviction
	}
	cache := NewCloxCache[[2]uint64, int](cfg)
	defer cache.Close()

	// Pin everything.
	cache.SetEvictDecider(func(_ [2]uint64, _ int) bool { return false })

	// Fill to capacity.
	for i := range uint64(4) {
		ok, _, _, _ := cache.Put([2]uint64{0, i}, int(i))
		if !ok {
			t.Fatalf("initial fill: Put {0,%d} returned !ok", i)
		}
	}

	// Try to insert beyond capacity. Eviction can't pick a victim →
	// Put returns success=false.
	ok, _, _, _ := cache.Put([2]uint64{0, 999}, 999)
	if ok {
		t.Fatal("Put succeeded despite every entry being pinned")
	}

	_, _, evictions := cache.Stats()
	if evictions != 0 {
		t.Fatalf("expected 0 evictions while all entries pinned; got %d", evictions)
	}

	// All original entries are still present.
	for i := range uint64(4) {
		if _, found := cache.Get([2]uint64{0, i}, 0); !found {
			t.Fatalf("pinned entry {0,%d} was evicted", i)
		}
	}
}
