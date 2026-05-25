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

/*
 * This file is part of CloxCache.
 *
 * CloxCache is licensed under the MIT License.
 * See LICENSE file for details.
 */

package keytrie

import (
	"fmt"
	"sync"
	"testing"
)

// r is a test helper: r(v) creates an EffectRef with nodeID=0 and offset=v.
func r(v uint64) EffectRef { return EffectRef{0, v} }

// --- TipSet unit tests ---

func TestTipSetCreate(t *testing.T) {
	ts := NewTipSet(r(3), r(1), r(2), r(1), r(3))
	if ts.Len() != 3 {
		t.Fatalf("Expected 3 tips, got %d", ts.Len())
	}
}

func TestTipSetEmpty(t *testing.T) {
	ts := NewTipSet()
	if ts.Len() != 0 {
		t.Fatalf("Expected 0 tips, got %d", ts.Len())
	}
	if ts.Contains(r(1)) {
		t.Error("Empty tip set should not contain anything")
	}
}

func TestTipSetContains(t *testing.T) {
	ts := NewTipSet(r(10), r(20), r(30))
	if !ts.Contains(r(10)) {
		t.Error("Expected to contain 10")
	}
	if !ts.Contains(r(30)) {
		t.Error("Expected to contain 30")
	}
	if ts.Contains(r(15)) {
		t.Error("Should not contain 15")
	}
}

func TestTipSetContainsAll(t *testing.T) {
	ts := NewTipSet(r(10), r(20), r(30))
	if !ts.ContainsAll([]EffectRef{r(10), r(30)}) {
		t.Error("Expected to contain all of {10, 30}")
	}
	if ts.ContainsAll([]EffectRef{r(10), r(25)}) {
		t.Error("Should not contain all of {10, 25}")
	}
	if !ts.ContainsAll(nil) {
		t.Error("Should contain all of empty set")
	}
}

func TestTipSetSingle(t *testing.T) {
	ts1 := NewTipSet(r(42))
	v, ok := ts1.Single()
	if !ok || v != r(42) {
		t.Errorf("Expected ({0,42}, true), got (%v, %v)", v, ok)
	}

	ts2 := NewTipSet(r(1), r(2))
	_, ok = ts2.Single()
	if ok {
		t.Error("Multi-tip set should not be single")
	}

	var ts3 *TipSet
	_, ok = ts3.Single()
	if ok {
		t.Error("Nil tip set should not be single")
	}
}

func TestTipSetNilReceiver(t *testing.T) {
	var ts *TipSet
	if ts.Len() != 0 {
		t.Error("Nil TipSet Len should be 0")
	}
	if ts.Contains(r(1)) {
		t.Error("Nil TipSet should not contain anything")
	}
	if !ts.ContainsAll(nil) {
		t.Error("Nil TipSet should ContainsAll of empty")
	}
	if ts.ContainsAll([]EffectRef{r(1)}) {
		t.Error("Nil TipSet should not ContainsAll of non-empty")
	}
	if ts.Tips() != nil {
		t.Error("Nil TipSet Tips should be nil")
	}
}

// --- Critbit tests adapted for TipSet ---

func TestCritbitBasicOperations(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	// Empty tree
	if c.Size() != 0 {
		t.Errorf("Expected size 0, got %d", c.Size())
	}
	if ts := c.Contains("foo"); ts != nil {
		t.Error("Expected nil for non-existent key")
	}

	// Insert new key (old=nil)
	ts100 := NewTipSet(r(100))
	conflict, ok := c.Insert("foo", nil, ts100)
	if !ok {
		t.Fatalf("Insert new key failed, conflict=%v", conflict)
	}
	if c.Size() != 1 {
		t.Errorf("Expected size 1, got %d", c.Size())
	}
	ts := c.Contains("foo")
	if ts == nil {
		t.Fatal("Expected tip set for 'foo'")
	}
	if !ts.Contains(r(100)) {
		t.Error("Expected tip set to contain 100")
	}

	// Update existing key (CAS with correct old)
	ts200 := NewTipSet(r(200))
	conflict, ok = c.Insert("foo", ts100, ts200)
	if !ok {
		t.Fatalf("Insert update failed, conflict=%v", conflict)
	}
	if c.Size() != 1 {
		t.Errorf("Expected size 1 after update, got %d", c.Size())
	}
	ts = c.Contains("foo")
	if !ts.Contains(r(200)) {
		t.Error("Expected tip set to contain 200 after update")
	}

	// Delete
	if !c.Delete("foo") {
		t.Error("Expected delete to return true")
	}
	if c.Size() != 0 {
		t.Errorf("Expected size 0 after delete, got %d", c.Size())
	}
	if ts := c.Contains("foo"); ts != nil {
		t.Error("Expected nil for deleted key")
	}

	// Delete non-existent
	if c.Delete("bar") {
		t.Error("Expected delete to return false for non-existent key")
	}

	// Re-insert after delete (revive tombstone). Delete atomically clears
	// tips to nil, so revival uses old=nil — matching what Contains
	// returns for a deleted key. A stale non-nil old would fail.
	ts300 := NewTipSet(r(300))
	conflict, ok = c.Insert("foo", nil, ts300)
	if !ok {
		t.Fatalf("Insert revive failed, conflict=%v", conflict)
	}
	if c.Size() != 1 {
		t.Errorf("Expected size 1 after re-insert, got %d", c.Size())
	}
	ts = c.Contains("foo")
	if ts == nil || !ts.Contains(r(300)) {
		t.Error("Expected key to be revived with 300")
	}
}

func TestCritbitInsertCASFailure(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	ts1 := NewTipSet(r(100))
	c.Insert("foo", nil, ts1)

	// Try to update with wrong old pointer — should fail
	wrongOld := NewTipSet(r(999))
	conflict, ok := c.Insert("foo", wrongOld, NewTipSet(r(200)))
	if ok {
		t.Fatal("Expected CAS failure with wrong old pointer")
	}
	if conflict == nil {
		t.Fatal("Expected conflicting tip set on CAS failure")
	}
	if conflict != ts1 {
		t.Error("Expected conflict to be the current tip set")
	}

	// Verify original value unchanged
	ts := c.Contains("foo")
	if ts != ts1 {
		t.Error("Expected original tip set to be unchanged")
	}
}

func TestCritbitInsertNewKeySuccess(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	ts := NewTipSet(r(42))
	conflict, ok := c.Insert("newkey", nil, ts)
	if !ok {
		t.Fatalf("Insert new key failed, conflict=%v", conflict)
	}
	if conflict != nil {
		t.Error("Expected nil conflict on success")
	}

	got := c.Contains("newkey")
	if got != ts {
		t.Error("Expected to get back same tip set pointer")
	}
}

func TestCritbitCloseBehavior(t *testing.T) {
	c := NewCritbit()

	ts := NewTipSet(r(100))
	c.Insert("foo", nil, ts)
	if err := c.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second close failed: %v", err)
	}

	if c.Size() != 0 {
		t.Fatalf("expected size 0 after close, got %d", c.Size())
	}
	if c.Contains("foo") != nil {
		t.Fatal("expected contains to return nil after close")
	}
	if c.Delete("foo") {
		t.Fatal("expected delete to return false after close")
	}

	_, ok := c.Insert("bar", nil, NewTipSet(r(200)))
	if ok {
		t.Fatal("expected insert to fail after close")
	}
}

func TestCritbitNilCallbacks(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	c.Insert("foo", nil, NewTipSet(r(1)))
	c.Range(nil)
	c.RangePrefix("f", nil)
}

func TestCritbitMultipleKeys(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	tipSets := make([]*TipSet, len(keys))
	for i, key := range keys {
		tipSets[i] = NewTipSet(r(uint64(i * 100)))
		c.Insert(key, nil, tipSets[i])
	}

	if c.Size() != int64(len(keys)) {
		t.Errorf("Expected size %d, got %d", len(keys), c.Size())
	}

	for i, key := range keys {
		ts := c.Contains(key)
		if ts == nil {
			t.Errorf("Key %s not found", key)
			continue
		}
		if !ts.Contains(r(uint64(i * 100))) {
			t.Errorf("Key %s: expected tip %d", key, i*100)
		}
	}
}

func TestCritbitRange(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	keys := []string{"c", "a", "b", "e", "d"}
	for _, key := range keys {
		c.Insert(key, nil, NewTipSet(EffectRef{0, 0}))
	}

	var result []string
	c.Range(func(key string) bool {
		result = append(result, key)
		return true
	})

	expected := []string{"a", "b", "c", "d", "e"}
	if len(result) != len(expected) {
		t.Fatalf("Expected %d keys, got %d", len(expected), len(result))
	}
	for i, key := range expected {
		if result[i] != key {
			t.Errorf("Position %d: expected %s, got %s", i, key, result[i])
		}
	}
}

func TestCritbitRangeWithDeleted(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	for _, key := range []string{"a", "b", "c", "d", "e"} {
		c.Insert(key, nil, NewTipSet(EffectRef{0, 0}))
	}

	c.Delete("b")
	c.Delete("d")

	var result []string
	c.Range(func(key string) bool {
		result = append(result, key)
		return true
	})

	expected := []string{"a", "c", "e"}
	if len(result) != len(expected) {
		t.Fatalf("Expected %d keys, got %d: %v", len(expected), len(result), result)
	}
	for i, key := range expected {
		if result[i] != key {
			t.Errorf("Position %d: expected %s, got %s", i, key, result[i])
		}
	}
}

func TestCritbitRangeFrom(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	for _, key := range []string{"a", "b", "c", "d", "e"} {
		c.Insert(key, nil, NewTipSet(EffectRef{0, 0}))
	}

	// From "b" should yield c, d, e
	var result []string
	c.RangeFrom("b", func(key string) bool {
		result = append(result, key)
		return true
	})
	if expected := []string{"c", "d", "e"}; len(result) != len(expected) {
		t.Fatalf("RangeFrom(b): expected %v, got %v", expected, result)
	} else {
		for i, k := range expected {
			if result[i] != k {
				t.Errorf("RangeFrom(b)[%d]: expected %s, got %s", i, k, result[i])
			}
		}
	}

	// From "" should yield all keys (same as Range)
	result = nil
	c.RangeFrom("", func(key string) bool {
		result = append(result, key)
		return true
	})
	if len(result) != 5 {
		t.Fatalf("RangeFrom(empty): expected 5 keys, got %d", len(result))
	}

	// From "e" should yield nothing (no keys after last)
	result = nil
	c.RangeFrom("e", func(key string) bool {
		result = append(result, key)
		return true
	})
	if len(result) != 0 {
		t.Fatalf("RangeFrom(e): expected 0 keys, got %v", result)
	}

	// From "aa" should yield b, c, d, e
	result = nil
	c.RangeFrom("aa", func(key string) bool {
		result = append(result, key)
		return true
	})
	if expected := []string{"b", "c", "d", "e"}; len(result) != len(expected) {
		t.Fatalf("RangeFrom(aa): expected %v, got %v", expected, result)
	}

	// Early stop
	result = nil
	c.RangeFrom("b", func(key string) bool {
		result = append(result, key)
		return len(result) < 2
	})
	if expected := []string{"c", "d"}; len(result) != len(expected) {
		t.Fatalf("RangeFrom(b) with stop: expected %v, got %v", expected, result)
	}
}

func TestCritbitRangeFromWithDeleted(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	for _, key := range []string{"a", "b", "c", "d", "e"} {
		c.Insert(key, nil, NewTipSet(EffectRef{0, 0}))
	}
	c.Delete("c")

	var result []string
	c.RangeFrom("b", func(key string) bool {
		result = append(result, key)
		return true
	})
	if expected := []string{"d", "e"}; len(result) != len(expected) {
		t.Fatalf("RangeFrom(b) with deleted c: expected %v, got %v", expected, result)
	}
}

func TestCritbitRangePrefix(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	keys := []string{"foo:1", "foo:2", "bar:1", "foo:3", "baz:1"}
	for _, key := range keys {
		c.Insert(key, nil, NewTipSet(EffectRef{0, 0}))
	}

	var result []string
	c.RangePrefix("foo:", func(key string) bool {
		result = append(result, key)
		return true
	})

	if len(result) != 3 {
		t.Fatalf("Expected 3 keys with prefix foo:, got %d: %v", len(result), result)
	}
}

func TestCritbitFirstLastWithPrefix(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	keys := []string{"memtier-1", "memtier-2", "memtier-3", "other-1"}
	for _, key := range keys {
		c.Insert(key, nil, NewTipSet(EffectRef{0, 0}))
	}

	first, ok, _ := c.FirstWithPrefix("memtier-", false)
	if !ok || first != "memtier-1" {
		t.Errorf("Expected first=memtier-1, got %s, ok=%v", first, ok)
	}

	last, ok, _ := c.LastWithPrefix("memtier-", false)
	if !ok || last != "memtier-3" {
		t.Errorf("Expected last=memtier-3, got %s, ok=%v", last, ok)
	}
}

func TestCritbitFirstWithDeletedLeaf(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	c.Insert("memtier-1", nil, NewTipSet(EffectRef{0, 0}))
	c.Insert("memtier-2", nil, NewTipSet(r(1)))
	c.Insert("memtier-3", nil, NewTipSet(r(2)))

	c.Delete("memtier-1")

	first, ok, _ := c.FirstWithPrefix("memtier-", false)
	if !ok {
		t.Fatal("Expected to find a key with prefix memtier-")
	}
	if first != "memtier-2" {
		t.Errorf("Expected first=memtier-2 after deleting memtier-1, got %s", first)
	}

	c.Delete("memtier-2")

	first, ok, _ = c.FirstWithPrefix("memtier-", false)
	if !ok {
		t.Fatal("Expected to find a key with prefix memtier-")
	}
	if first != "memtier-3" {
		t.Errorf("Expected first=memtier-3, got %s", first)
	}
}

func TestCritbitLastWithDeletedLeaf(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	c.Insert("memtier-1", nil, NewTipSet(EffectRef{0, 0}))
	c.Insert("memtier-2", nil, NewTipSet(r(1)))
	c.Insert("memtier-3", nil, NewTipSet(r(2)))

	c.Delete("memtier-3")

	last, ok, _ := c.LastWithPrefix("memtier-", false)
	if !ok {
		t.Fatal("Expected to find a key with prefix memtier-")
	}
	if last != "memtier-2" {
		t.Errorf("Expected last=memtier-2 after deleting memtier-3, got %s", last)
	}
}

func TestCritbitNextPrevWithPrefix(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	keys := []string{"key:1", "key:2", "key:3", "key:4", "key:5"}
	for _, key := range keys {
		c.Insert(key, nil, NewTipSet(EffectRef{0, 0}))
	}

	// Next
	next, ok, _ := c.NextWithPrefix("key:", "key:2", false)
	if !ok || next != "key:3" {
		t.Errorf("Expected next=key:3, got %s, ok=%v", next, ok)
	}

	next, ok, _ = c.NextWithPrefix("key:", "key:5", false)
	if ok {
		t.Errorf("Expected no next after key:5, got %s", next)
	}

	// Prev
	prev, ok, _ := c.PrevWithPrefix("key:", "key:3", false)
	if !ok || prev != "key:2" {
		t.Errorf("Expected prev=key:2, got %s, ok=%v", prev, ok)
	}

	prev, ok, _ = c.PrevWithPrefix("key:", "key:1", false)
	if ok {
		t.Errorf("Expected no prev before key:1, got %s", prev)
	}
}

func TestCritbitClaim(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	c.Insert("foo", nil, NewTipSet(r(100)))

	exists, release := c.TryClaimKey("foo")
	if !exists {
		t.Fatal("Expected key to exist")
	}
	if release == nil {
		t.Fatal("Expected release function")
	}
	release()

	exists2, release2 := c.TryClaimKey("foo")
	if !exists2 {
		t.Error("Expected key to exist")
	}
	if release2 == nil {
		t.Error("Expected release function")
	}
	if release2 != nil {
		release2()
	}
}

func TestCritbitClaimDeletedKey(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	c.Insert("foo", nil, NewTipSet(r(100)))
	c.Delete("foo")

	exists, release := c.TryClaimKey("foo")
	if exists {
		t.Error("Expected deleted key to not exist for claim")
	}
	if release != nil {
		t.Error("Expected nil release for deleted key")
	}
}

func TestCritbitConcurrentInsert(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	const numGoroutines = 10
	const keysPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := range numGoroutines {
		go func(gid int) {
			defer wg.Done()
			for i := range keysPerGoroutine {
				key := fmt.Sprintf("g%d:key%d", gid, i)
				ts := NewTipSet(r(uint64(gid*1000 + i)))
				// New key insert: old=nil
				c.Insert(key, nil, ts)
			}
		}(g)
	}

	wg.Wait()

	expectedSize := int64(numGoroutines * keysPerGoroutine)
	if c.Size() != expectedSize {
		t.Errorf("Expected size %d, got %d", expectedSize, c.Size())
	}
}

func TestCritbitConcurrentInsertDelete(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	const numGoroutines = 10
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Inserters
	for g := range numGoroutines {
		go func(gid int) {
			defer wg.Done()
			for i := range iterations {
				key := fmt.Sprintf("key%d", i%20)
				ts := NewTipSet(r(uint64(gid*1000 + i)))
				// Try insert; on CAS failure just retry with current
				for {
					old := c.Contains(key)
					_, ok := c.Insert(key, old, ts)
					if ok {
						break
					}
				}
			}
		}(g)
	}

	// Deleters
	for range numGoroutines {
		go func() {
			defer wg.Done()
			for i := range iterations {
				key := fmt.Sprintf("key%d", i%20)
				c.Delete(key)
			}
		}()
	}

	wg.Wait()

	// Just verify no crashes - size can vary due to race between insert/delete
	_ = c.Size()
}

func TestCritbitConcurrentInsertThenContains(t *testing.T) {
	for iter := range 100 {
		c := NewCritbit()

		const numGoroutines = 10
		const keysPerGoroutine = 10

		keys := make([][]string, numGoroutines)
		for g := range numGoroutines {
			keys[g] = make([]string, keysPerGoroutine)
			for i := range keysPerGoroutine {
				keys[g][i] = fmt.Sprintf("key_%d_%d", g, i)
			}
		}

		var wg sync.WaitGroup
		wg.Add(numGoroutines)
		for g := range numGoroutines {
			go func(gid int) {
				defer wg.Done()
				for i, key := range keys[gid] {
					ts := NewTipSet(r(uint64(gid*100 + i)))
					c.Insert(key, nil, ts)
				}
			}(g)
		}
		wg.Wait()

		for g := range numGoroutines {
			for i, key := range keys[g] {
				ts := c.Contains(key)
				if ts == nil {
					t.Fatalf("iter %d: Contains(%q) returned nil after concurrent insert (g=%d, i=%d)", iter, key, g, i)
				}
			}
		}

		c.Close()
	}
}

func TestCritbitConcurrentInsertCASStress(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	// Single key, many concurrent writers — all should eventually succeed
	const numGoroutines = 10
	const key = "contested"

	// Seed the key
	c.Insert(key, nil, NewTipSet(EffectRef{0, 0}))

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for g := range numGoroutines {
		go func(gid int) {
			defer wg.Done()
			newTS := NewTipSet(r(uint64(gid + 1)))
			for range 1000 {
				old := c.Contains(key)
				conflict, ok := c.Insert(key, old, newTS)
				if ok {
					return
				}
				// CAS failed — conflict should be a valid tip set
				if conflict == nil {
					t.Errorf("goroutine %d: CAS failure returned nil conflict", gid)
					return
				}
			}
		}(g)
	}

	wg.Wait()

	// Key should still exist
	ts := c.Contains(key)
	if ts == nil {
		t.Fatal("Key should exist after concurrent inserts")
	}
}

func TestCritbitMatchPattern(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	keys := []string{"foo", "foobar", "bar", "baz", "food"}
	for _, key := range keys {
		c.Insert(key, nil, NewTipSet(EffectRef{0, 0}))
	}

	all := c.MatchPattern("*")
	if len(all) != len(keys) {
		t.Errorf("Expected %d keys for *, got %d", len(keys), len(all))
	}

	fooMatches := c.MatchPattern("foo*")
	if len(fooMatches) != 3 { // foo, foobar, food
		t.Errorf("Expected 3 matches for foo*, got %d: %v", len(fooMatches), fooMatches)
	}

	matches := c.MatchPattern("ba?")
	if len(matches) != 2 { // bar, baz
		t.Errorf("Expected 2 matches for ba?, got %d: %v", len(matches), matches)
	}
}

func TestCritbitEdgeCases(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	// Empty string key
	c.Insert("", nil, NewTipSet(EffectRef{0, 0}))
	if ts := c.Contains(""); ts == nil {
		t.Error("Expected empty string key to work")
	}

	// Single character keys
	c.Insert("a", nil, NewTipSet(r(1)))
	c.Insert("b", nil, NewTipSet(r(2)))
	ts := c.Contains("a")
	if ts == nil || !ts.Contains(r(1)) {
		t.Error("Single char key 'a' not found correctly")
	}

	// Keys that are prefixes of each other
	c.Insert("test", nil, NewTipSet(r(10)))
	c.Insert("testing", nil, NewTipSet(r(11)))
	c.Insert("tester", nil, NewTipSet(r(12)))

	if ts := c.Contains("test"); ts == nil || !ts.Contains(r(10)) {
		t.Error("Prefix key 'test' not found correctly")
	}
	if ts := c.Contains("testing"); ts == nil || !ts.Contains(r(11)) {
		t.Error("Key 'testing' not found correctly")
	}
}

func TestCritbitHighestBit(t *testing.T) {
	tests := []struct {
		input    byte
		expected byte
	}{
		{0b00000001, 0b00000001},
		{0b00000010, 0b00000010},
		{0b00000100, 0b00000100},
		{0b00001000, 0b00001000},
		{0b00010000, 0b00010000},
		{0b00100000, 0b00100000},
		{0b01000000, 0b01000000},
		{0b10000000, 0b10000000},
		{0b11111111, 0b10000000},
		{0b10101010, 0b10000000},
		{0b01010100, 0b01000000},
		{0b00000011, 0b00000010},
	}

	for _, tc := range tests {
		result := highestBit(tc.input)
		if result != tc.expected {
			t.Errorf("highestBit(%08b) = %08b, expected %08b", tc.input, result, tc.expected)
		}
	}
}

func BenchmarkCritbitInsert(b *testing.B) {
	c := NewCritbit()
	defer c.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts := NewTipSet(r(uint64(i)))
		c.Insert(fmt.Sprintf("key%d", i), nil, ts)
	}
}

func BenchmarkCritbitContains(b *testing.B) {
	c := NewCritbit()
	defer c.Close()

	for i := range 10000 {
		c.Insert(fmt.Sprintf("key%d", i), nil, NewTipSet(r(uint64(i))))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Contains(fmt.Sprintf("key%d", i%10000))
	}
}

func BenchmarkCritbitDelete(b *testing.B) {
	c := NewCritbit()
	defer c.Close()

	for i := 0; i < b.N; i++ {
		c.Insert(fmt.Sprintf("key%d", i), nil, NewTipSet(r(uint64(i))))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Delete(fmt.Sprintf("key%d", i))
	}
}

func TestCritbitBinaryKeys(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	makeKey := func(ms, seq uint64) string {
		buf := make([]byte, 16)
		buf[0] = byte(ms >> 56)
		buf[1] = byte(ms >> 48)
		buf[2] = byte(ms >> 40)
		buf[3] = byte(ms >> 32)
		buf[4] = byte(ms >> 24)
		buf[5] = byte(ms >> 16)
		buf[6] = byte(ms >> 8)
		buf[7] = byte(ms)
		buf[8] = byte(seq >> 56)
		buf[9] = byte(seq >> 48)
		buf[10] = byte(seq >> 40)
		buf[11] = byte(seq >> 32)
		buf[12] = byte(seq >> 24)
		buf[13] = byte(seq >> 16)
		buf[14] = byte(seq >> 8)
		buf[15] = byte(seq)
		return string(buf)
	}

	c.Insert(makeKey(1, 0), nil, NewTipSet(r(1)))
	c.Insert(makeKey(2, 0), nil, NewTipSet(r(2)))
	c.Insert(makeKey(3, 0), nil, NewTipSet(r(3)))

	if c.Size() != 3 {
		t.Fatalf("Expected size 3, got %d", c.Size())
	}

	first, found, _ := c.FirstWithPrefix("", false)
	if !found {
		t.Fatal("FirstWithPrefix returned not found")
	}
	if first != makeKey(1, 0) {
		t.Errorf("Expected first to be 1-0, got different key")
	}

	last, found, _ := c.LastWithPrefix("", false)
	if !found {
		t.Fatal("LastWithPrefix returned not found")
	}
	if last != makeKey(3, 0) {
		t.Errorf("Expected last to be 3-0, got different key")
	}

	var keys []string
	c.Range(func(key string) bool {
		keys = append(keys, key)
		return true
	})

	if len(keys) != 3 {
		t.Fatalf("Range returned %d keys, expected 3", len(keys))
	}

	expected := []string{makeKey(1, 0), makeKey(2, 0), makeKey(3, 0)}
	for i, key := range keys {
		if key != expected[i] {
			t.Errorf("Position %d: expected key for %d-0, got different", i, i+1)
		}
	}

	next, found, _ := c.NextWithPrefix("", makeKey(1, 0), false)
	if !found {
		t.Fatal("NextWithPrefix returned not found")
	}
	if next != makeKey(2, 0) {
		t.Errorf("Expected next after 1-0 to be 2-0")
	}

	prev, found, _ := c.PrevWithPrefix("", makeKey(3, 0), false)
	if !found {
		t.Fatal("PrevWithPrefix returned not found")
	}
	if prev != makeKey(2, 0) {
		t.Errorf("Expected prev before 3-0 to be 2-0")
	}
}

func TestCritbitBinaryKeysDebug(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	makeKey := func(ms, seq uint64) string {
		buf := make([]byte, 16)
		buf[0] = byte(ms >> 56)
		buf[1] = byte(ms >> 48)
		buf[2] = byte(ms >> 40)
		buf[3] = byte(ms >> 32)
		buf[4] = byte(ms >> 24)
		buf[5] = byte(ms >> 16)
		buf[6] = byte(ms >> 8)
		buf[7] = byte(ms)
		buf[8] = byte(seq >> 56)
		buf[9] = byte(seq >> 48)
		buf[10] = byte(seq >> 40)
		buf[11] = byte(seq >> 32)
		buf[12] = byte(seq >> 24)
		buf[13] = byte(seq >> 16)
		buf[14] = byte(seq >> 8)
		buf[15] = byte(seq)
		return string(buf)
	}

	parseKey := func(key string) (uint64, uint64) {
		if len(key) != 16 {
			return 0, 0
		}
		ms := uint64(key[0])<<56 | uint64(key[1])<<48 | uint64(key[2])<<40 | uint64(key[3])<<32 |
			uint64(key[4])<<24 | uint64(key[5])<<16 | uint64(key[6])<<8 | uint64(key[7])
		seq := uint64(key[8])<<56 | uint64(key[9])<<48 | uint64(key[10])<<40 | uint64(key[11])<<32 |
			uint64(key[12])<<24 | uint64(key[13])<<16 | uint64(key[14])<<8 | uint64(key[15])
		return ms, seq
	}

	c.Insert(makeKey(1, 0), nil, NewTipSet(r(1)))
	t.Logf("After insert 1-0: size=%d", c.Size())

	c.Insert(makeKey(2, 0), nil, NewTipSet(r(2)))
	t.Logf("After insert 2-0: size=%d", c.Size())

	c.Insert(makeKey(3, 0), nil, NewTipSet(r(3)))
	t.Logf("After insert 3-0: size=%d", c.Size())

	for _, ms := range []uint64{1, 2, 3} {
		ts := c.Contains(makeKey(ms, 0))
		if ts == nil {
			t.Errorf("Contains(%d-0) returned nil!", ms)
		} else {
			v, _ := ts.Single()
			t.Logf("Contains(%d-0) = tip %d", ms, v)
		}
	}

	first, found, _ := c.FirstWithPrefix("", false)
	if !found {
		t.Fatal("FirstWithPrefix returned not found")
	}
	ms, seq := parseKey(first)
	t.Logf("FirstWithPrefix: %d-%d", ms, seq)
	if ms != 1 || seq != 0 {
		t.Errorf("Expected first to be 1-0, got %d-%d", ms, seq)
	}

	t.Log("Range iteration:")
	var count int
	c.Range(func(key string) bool {
		ms, seq := parseKey(key)
		t.Logf("  [%d] %d-%d", count, ms, seq)
		count++
		return true
	})
	if count != 3 {
		t.Errorf("Range returned %d keys, expected 3", count)
	}

	zeroKey := makeKey(0, 0)
	next, found, _ := c.NextWithPrefix("", zeroKey, false)
	if !found {
		t.Fatal("NextWithPrefix from 0-0 returned not found")
	}
	ms, seq = parseKey(next)
	t.Logf("NextWithPrefix from 0-0: %d-%d", ms, seq)
	if ms != 1 || seq != 0 {
		t.Errorf("Expected NextWithPrefix(0-0) to return 1-0, got %d-%d", ms, seq)
	}
}

// --- Binary key tests ---

func TestCritbitFindCritBitEdgeCases(t *testing.T) {
	tests := []struct {
		name          string
		a, b          string
		wantBytePos   uint32
		wantOtherbits uint8
	}{
		// XOR = 0xFF → highestBit = 0x80 → otherbits = 0x7F
		{"0x00 vs 0xFF", "\x00", "\xFF", 0, 0x7F},
		{"0xFF vs 0x00", "\xFF", "\x00", 0, 0x7F},
		// XOR = 0x01 → highestBit = 0x01 → otherbits = 0xFE
		{"0xFE vs 0xFF", "\xFE", "\xFF", 0, 0xFE},
		{"0xFF vs 0xFE", "\xFF", "\xFE", 0, 0xFE},
		// XOR = 0x80 → highestBit = 0x80 → otherbits = 0x7F
		{"0x00 vs 0x80", "\x00", "\x80", 0, 0x7F},
		// Differ at byte 1
		{"differ at byte 1", "\x01\x00", "\x01\xFF", 1, 0x7F},
		// Different lengths, extra byte is non-zero
		{"short vs long 0x80", "\x01", "\x01\x80", 1, 0x7F},
		{"short vs long 0x01", "\x01", "\x01\x01", 1, 0xFE},
		// Different lengths, extra byte is 0x00 → length-discrimination sentinel otherbits=0xFF
		{"short vs long 0x00", "\x01", "\x01\x00", 1, 0xFF},
		{"long 0x00 vs short", "\x01\x00", "\x01", 1, 0xFF},
		// Identical keys
		{"identical single", "abc", "abc", 0, 0},
		{"identical binary", "\x00\xFF", "\x00\xFF", 0, 0},
		// Both empty
		{"both empty", "", "", 0, 0},
		// Empty vs null byte → bytePos=0, diffByte=0x00 → length-discrimination sentinel otherbits=0xFF
		{"empty vs 0x00", "", "\x00", 0, 0xFF},
		{"0x00 vs empty", "\x00", "", 0, 0xFF},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bytePos, otherbits := findCritBit(tc.a, tc.b)
			if bytePos != tc.wantBytePos || otherbits != tc.wantOtherbits {
				t.Errorf("findCritBit(%q, %q) = (%d, 0x%02X), want (%d, 0x%02X)",
					tc.a, tc.b, bytePos, otherbits, tc.wantBytePos, tc.wantOtherbits)
			}
		})
	}
}

func TestCritbitGetDirectionEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		bytePos   uint32
		otherbits uint8
		wantDir   int
	}{
		// otherbits=0xFE means critical bit is bit 0.
		// c=0x00: 1 + (0xFE|0x00) = 0xFF → >>8 = 0
		{"0x00 bit0", "\x00", 0, 0xFE, 0},
		// c=0x01: 1 + (0xFE|0x01) = 0x100 → >>8 = 1
		{"0x01 bit0", "\x01", 0, 0xFE, 1},
		// otherbits=0x7F means critical bit is bit 7.
		// c=0xFF: 1 + (0x7F|0xFF) = 0x100 → >>8 = 1
		{"0xFF bit7", "\xFF", 0, 0x7F, 1},
		// c=0x00: 1 + (0x7F|0x00) = 0x80 → >>8 = 0
		{"0x00 bit7", "\x00", 0, 0x7F, 0},
		// c=0x80: 1 + (0x7F|0x80) = 0x100 → >>8 = 1
		{"0x80 bit7", "\x80", 0, 0x7F, 1},
		// bytePos beyond key length → c defaults to 0
		{"beyond key len", "", 0, 0xFE, 0},
		{"beyond key len 2", "a", 5, 0xFE, 0},
		// bytePos=1 in two-byte key
		{"second byte", "\x00\xFF", 1, 0x7F, 1},
		{"second byte zero", "\x00\x00", 1, 0x7F, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := getDirection(tc.key, tc.bytePos, tc.otherbits)
			if dir != tc.wantDir {
				t.Errorf("getDirection(%q, %d, 0x%02X) = %d, want %d",
					tc.key, tc.bytePos, tc.otherbits, dir, tc.wantDir)
			}
		})
	}
}

func TestCritbitBinaryKeys0xFF(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	keys := []string{"\xFF", "\x00", "\xFF\xFF", "\xFF\x00", "\x00\xFF"}
	for i, key := range keys {
		c.Insert(key, nil, NewTipSet(r(uint64(i+1))))
	}

	if c.Size() != 5 {
		t.Fatalf("Expected size 5, got %d", c.Size())
	}

	for i, key := range keys {
		ts := c.Contains(key)
		if ts == nil {
			t.Errorf("Contains(%q) returned nil", key)
		} else if !ts.Contains(r(uint64(i + 1))) {
			t.Errorf("Contains(%q) has wrong TipSet", key)
		}
	}

	// Missing key
	if ts := c.Contains("\x80"); ts != nil {
		t.Error("Contains(0x80) should be nil")
	}

	// Verify lexicographic order
	expected := []string{"\x00", "\x00\xFF", "\xFF", "\xFF\x00", "\xFF\xFF"}
	var got []string
	c.Range(func(key string) bool {
		got = append(got, key)
		return true
	})
	if len(got) != len(expected) {
		t.Fatalf("Range returned %d keys, expected %d", len(got), len(expected))
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Errorf("Range[%d] = %q, want %q", i, got[i], expected[i])
		}
	}
}

func TestCritbitBinaryKeysFullByteRange(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	for i := range 256 {
		key := string([]byte{0x00, byte(i)})
		c.Insert(key, nil, NewTipSet(r(uint64(i))))
	}

	if c.Size() != 256 {
		t.Fatalf("Expected size 256, got %d", c.Size())
	}

	for i := range 256 {
		key := string([]byte{0x00, byte(i)})
		ts := c.Contains(key)
		if ts == nil {
			t.Errorf("Contains(0x00 0x%02X) returned nil", i)
		} else if !ts.Contains(r(uint64(i))) {
			t.Errorf("Contains(0x00 0x%02X) has wrong TipSet", i)
		}
	}

	var got []string
	c.Range(func(key string) bool {
		got = append(got, key)
		return true
	})
	if len(got) != 256 {
		t.Fatalf("Range returned %d keys, expected 256", len(got))
	}
	for i := range 256 {
		want := string([]byte{0x00, byte(i)})
		if got[i] != want {
			t.Errorf("Range[%d] = %q, want %q", i, got[i], want)
		}
	}
}

func TestCritbitBinaryKeyDelete(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	c.Insert("\x00\x01", nil, NewTipSet(r(1)))
	c.Insert("\x00\x02", nil, NewTipSet(r(2)))
	c.Insert("\x00\x03", nil, NewTipSet(r(3)))

	if !c.Delete("\x00\x02") {
		t.Fatal("Delete returned false for existing key")
	}
	if c.Contains("\x00\x02") != nil {
		t.Error("Contains should return nil after delete")
	}
	if c.Size() != 2 {
		t.Errorf("Expected size 2, got %d", c.Size())
	}

	var got []string
	c.Range(func(key string) bool {
		got = append(got, key)
		return true
	})
	if len(got) != 2 || got[0] != "\x00\x01" || got[1] != "\x00\x03" {
		t.Errorf("Range after delete = %q, want [\\x00\\x01, \\x00\\x03]", got)
	}

	// Double delete
	if c.Delete("\x00\x02") {
		t.Error("Double delete should return false")
	}

	// Re-insert deleted key
	_, ok := c.Insert("\x00\x02", nil, NewTipSet(r(22)))
	if !ok {
		t.Fatal("Re-insert after delete failed")
	}
	ts := c.Contains("\x00\x02")
	if ts == nil || !ts.Contains(r(22)) {
		t.Error("Re-inserted key has wrong TipSet")
	}
	if c.Size() != 3 {
		t.Errorf("Expected size 3 after re-insert, got %d", c.Size())
	}
}

func TestCritbitBinaryContainsMissing(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	c.Insert("\x01\x02\x03", nil, NewTipSet(r(1)))

	missing := []string{
		"\x01\x02\x04",     // differ in last byte
		"\x01\x02",         // prefix of existing
		"\x01\x02\x03\x00", // existing is prefix of query
		"\x01\x02\x02",     // off by one in last byte
		"",                 // empty key not inserted
		"\x01\x03\x03",     // differ in middle byte
		"\x00\x02\x03",     // differ in first byte
	}
	for _, key := range missing {
		if ts := c.Contains(key); ts != nil {
			t.Errorf("Contains(%q) should be nil, got %v", key, ts)
		}
	}
}

func TestCritbitBinaryKeysVaryingLength(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	keys := []string{
		"",                                 // 0 bytes
		"\x01",                             // 1 byte
		"\x01\x02",                         // 2 bytes
		"\x01\x02\x03\x04",                 // 4 bytes
		"\x01\x02\x03\x04\x05\x06\x07\x08", // 8 bytes
		"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x0C\x0D\x0E\x0F\x10", // 16 bytes
	}
	for i, key := range keys {
		c.Insert(key, nil, NewTipSet(r(uint64(i))))
	}

	if c.Size() != 6 {
		t.Fatalf("Expected size 6, got %d", c.Size())
	}
	for i, key := range keys {
		ts := c.Contains(key)
		if ts == nil {
			t.Errorf("Contains(len=%d) returned nil", len(key))
		} else if !ts.Contains(r(uint64(i))) {
			t.Errorf("Contains(len=%d) has wrong TipSet", len(key))
		}
	}

	// Verify ordering: "" < "\x01" < "\x01\x02" < "\x01\x02\x03\x04" < ...
	var got []string
	c.Range(func(key string) bool {
		got = append(got, key)
		return true
	})
	if len(got) != 6 {
		t.Fatalf("Range returned %d keys, expected 6", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i] <= got[i-1] {
			t.Errorf("Range not sorted: [%d]=%q >= [%d]=%q", i-1, got[i-1], i, got[i])
		}
	}

	// FirstWithPrefix("") should return "" (empty string key)
	first, found, _ := c.FirstWithPrefix("", false)
	if !found {
		t.Fatal("FirstWithPrefix returned not found")
	}
	if first != "" {
		t.Errorf("FirstWithPrefix = %q, want empty string", first)
	}
}

func TestCritbitBinaryKeysPrefixOfEachOther(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	c.Insert("\x01\x02", nil, NewTipSet(r(1)))
	c.Insert("\x01\x02\x03", nil, NewTipSet(r(2)))
	c.Insert("\x01\x02\x03\x04", nil, NewTipSet(r(3)))

	if c.Size() != 3 {
		t.Fatalf("Expected size 3, got %d", c.Size())
	}
	for _, key := range []string{"\x01\x02", "\x01\x02\x03", "\x01\x02\x03\x04"} {
		if c.Contains(key) == nil {
			t.Errorf("Contains(%q) returned nil", key)
		}
	}

	// RangePrefix with the shared prefix
	var got []string
	c.RangePrefix("\x01\x02", func(key string) bool {
		got = append(got, key)
		return true
	})
	if len(got) != 3 {
		t.Fatalf("RangePrefix(\\x01\\x02) returned %d keys, want 3", len(got))
	}
	expected := []string{"\x01\x02", "\x01\x02\x03", "\x01\x02\x03\x04"}
	for i := range expected {
		if got[i] != expected[i] {
			t.Errorf("RangePrefix[%d] = %q, want %q", i, got[i], expected[i])
		}
	}

	// Edge case: \x01 vs \x01\x00 — extension byte is 0x00, triggers hb=0x80 fallback
	c2 := NewCritbit()
	defer c2.Close()

	c2.Insert("\x01", nil, NewTipSet(r(10)))
	c2.Insert("\x01\x00", nil, NewTipSet(r(11)))

	if c2.Size() != 2 {
		t.Fatalf("Expected size 2, got %d", c2.Size())
	}
	if c2.Contains("\x01") == nil {
		t.Error("Contains(\\x01) returned nil")
	}
	if c2.Contains("\x01\x00") == nil {
		t.Error("Contains(\\x01\\x00) returned nil")
	}

	var got2 []string
	c2.Range(func(key string) bool {
		got2 = append(got2, key)
		return true
	})
	if len(got2) != 2 || got2[0] != "\x01" || got2[1] != "\x01\x00" {
		t.Errorf("Range = %q, want [\\x01, \\x01\\x00]", got2)
	}
}

func TestCritbitBinaryKeysEmbeddedNulls(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	keys := map[string]uint64{
		"\x00abc": 1,
		"ab\x00c": 2,
		"abc\x00": 3,
		"abc":     4,
	}
	for key, v := range keys {
		c.Insert(key, nil, NewTipSet(r(v)))
	}

	if c.Size() != 4 {
		t.Fatalf("Expected size 4, got %d", c.Size())
	}
	for key, v := range keys {
		ts := c.Contains(key)
		if ts == nil {
			t.Errorf("Contains(%q) returned nil", key)
		} else if !ts.Contains(r(v)) {
			t.Errorf("Contains(%q) has wrong TipSet", key)
		}
	}

	// Verify ordering: \x00abc < ab\x00c < abc < abc\x00
	expected := []string{"\x00abc", "ab\x00c", "abc", "abc\x00"}
	var got []string
	c.Range(func(key string) bool {
		got = append(got, key)
		return true
	})
	if len(got) != 4 {
		t.Fatalf("Range returned %d keys, expected 4", len(got))
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Errorf("Range[%d] = %q, want %q", i, got[i], expected[i])
		}
	}

	// Delete one with embedded null, verify others remain
	c.Delete("ab\x00c")
	if c.Contains("ab\x00c") != nil {
		t.Error("Deleted key should not be found")
	}
	if c.Contains("\x00abc") == nil || c.Contains("abc") == nil || c.Contains("abc\x00") == nil {
		t.Error("Non-deleted keys should still be found")
	}
}

func TestCritbitBinaryPrefixOperations(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	c.Insert("\x01\x00\x01", nil, NewTipSet(r(1)))
	c.Insert("\x01\x00\x02", nil, NewTipSet(r(2)))
	c.Insert("\x01\x01\x01", nil, NewTipSet(r(3)))
	c.Insert("\x02\x00\x01", nil, NewTipSet(r(4)))
	c.Insert("\x02\x00\x02", nil, NewTipSet(r(5)))

	// RangePrefix(\x01) → 3 keys
	var got []string
	c.RangePrefix("\x01", func(key string) bool {
		got = append(got, key)
		return true
	})
	if len(got) != 3 {
		t.Fatalf("RangePrefix(\\x01) = %d keys, want 3", len(got))
	}
	expected := []string{"\x01\x00\x01", "\x01\x00\x02", "\x01\x01\x01"}
	for i := range expected {
		if got[i] != expected[i] {
			t.Errorf("RangePrefix(\\x01)[%d] = %q, want %q", i, got[i], expected[i])
		}
	}

	// RangePrefix(\x01\x00) → 2 keys
	got = nil
	c.RangePrefix("\x01\x00", func(key string) bool {
		got = append(got, key)
		return true
	})
	if len(got) != 2 {
		t.Fatalf("RangePrefix(\\x01\\x00) = %d keys, want 2", len(got))
	}

	// RangePrefix(\x02) → 2 keys
	got = nil
	c.RangePrefix("\x02", func(key string) bool {
		got = append(got, key)
		return true
	})
	if len(got) != 2 {
		t.Fatalf("RangePrefix(\\x02) = %d keys, want 2", len(got))
	}

	// RangePrefix(\x03) → 0 keys
	got = nil
	c.RangePrefix("\x03", func(key string) bool {
		got = append(got, key)
		return true
	})
	if len(got) != 0 {
		t.Errorf("RangePrefix(\\x03) = %d keys, want 0", len(got))
	}

	// FirstWithPrefix / LastWithPrefix
	first, found, _ := c.FirstWithPrefix("\x01", false)
	if !found || first != "\x01\x00\x01" {
		t.Errorf("FirstWithPrefix(\\x01) = %q, found=%v", first, found)
	}
	last, found, _ := c.LastWithPrefix("\x01", false)
	if !found || last != "\x01\x01\x01" {
		t.Errorf("LastWithPrefix(\\x01) = %q, found=%v", last, found)
	}
}

func TestCritbitRangeFromBinaryKeys(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	for i := 1; i <= 5; i++ {
		c.Insert(string([]byte{byte(i)}), nil, NewTipSet(r(uint64(i))))
	}

	// RangeFrom(\x02) → \x03, \x04, \x05 (strictly after)
	var got []string
	c.RangeFrom("\x02", func(key string) bool {
		got = append(got, key)
		return true
	})
	if len(got) != 3 {
		t.Fatalf("RangeFrom(\\x02) = %d keys, want 3", len(got))
	}
	for i, want := range []string{"\x03", "\x04", "\x05"} {
		if got[i] != want {
			t.Errorf("RangeFrom(\\x02)[%d] = %q, want %q", i, got[i], want)
		}
	}

	// RangeFrom(\x00) → all 5
	got = nil
	c.RangeFrom("\x00", func(key string) bool {
		got = append(got, key)
		return true
	})
	if len(got) != 5 {
		t.Errorf("RangeFrom(\\x00) = %d keys, want 5", len(got))
	}

	// RangeFrom(\x05) → nothing
	got = nil
	c.RangeFrom("\x05", func(key string) bool {
		got = append(got, key)
		return true
	})
	if len(got) != 0 {
		t.Errorf("RangeFrom(\\x05) = %d keys, want 0", len(got))
	}

	// RangeFrom with between-key cursor of different length
	got = nil
	c.RangeFrom("\x02\x80", func(key string) bool {
		got = append(got, key)
		return true
	})
	if len(got) != 3 {
		t.Errorf("RangeFrom(\\x02\\x80) = %d keys, want 3", len(got))
	}

	// RangeFrom("") → all 5 (falls through to Range)
	got = nil
	c.RangeFrom("", func(key string) bool {
		got = append(got, key)
		return true
	})
	if len(got) != 5 {
		t.Errorf("RangeFrom(\"\") = %d keys, want 5", len(got))
	}
}

func TestCritbitBinaryKeysSingleBitDifference(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	// Insert keys that are powers of 2 (single bit set) plus zero
	singleBitKeys := []string{
		"\x00", "\x01", "\x02", "\x04", "\x08",
		"\x10", "\x20", "\x40", "\x80",
	}
	for i, key := range singleBitKeys {
		c.Insert(key, nil, NewTipSet(r(uint64(i))))
	}

	if c.Size() != 9 {
		t.Fatalf("Expected size 9, got %d", c.Size())
	}
	for i, key := range singleBitKeys {
		ts := c.Contains(key)
		if ts == nil {
			t.Errorf("Contains(0x%02X) returned nil", key[0])
		} else if !ts.Contains(r(uint64(i))) {
			t.Errorf("Contains(0x%02X) has wrong TipSet", key[0])
		}
	}

	// Verify order: 0x00, 0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80
	var got []string
	c.Range(func(key string) bool {
		got = append(got, key)
		return true
	})
	if len(got) != 9 {
		t.Fatalf("Range returned %d keys, expected 9", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i] <= got[i-1] {
			t.Errorf("Range not sorted: [%d]=0x%02X >= [%d]=0x%02X", i-1, got[i-1][0], i, got[i][0])
		}
	}

	// Next/Prev navigation
	next, found, _ := c.NextWithPrefix("", "\x04", false)
	if !found || next != "\x08" {
		t.Errorf("NextWithPrefix after \\x04 = %q, want \\x08", next)
	}
	prev, found, _ := c.PrevWithPrefix("", "\x08", false)
	if !found || prev != "\x04" {
		t.Errorf("PrevWithPrefix before \\x08 = %q, want \\x04", prev)
	}

	// Also test two-byte keys differing in bit 0 of second byte
	c.Insert("\x01\x00", nil, NewTipSet(r(100)))
	c.Insert("\x01\x01", nil, NewTipSet(r(101)))
	if c.Contains("\x01\x00") == nil || c.Contains("\x01\x01") == nil {
		t.Error("Two-byte keys differing in one bit should both exist")
	}
}

func TestCritbitBinaryKeysNavigationStress(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	// Insert 50 two-byte keys
	for i := range 50 {
		key := string([]byte{byte(i / 10), byte(i % 10)})
		c.Insert(key, nil, NewTipSet(r(uint64(i))))
	}

	if c.Size() != 50 {
		t.Fatalf("Expected size 50, got %d", c.Size())
	}

	// Walk forward
	var forward []string
	current, found, _ := c.FirstWithPrefix("", false)
	if !found {
		t.Fatal("FirstWithPrefix returned not found")
	}
	forward = append(forward, current)
	for {
		next, ok, _ := c.NextWithPrefix("", current, false)
		if !ok {
			break
		}
		forward = append(forward, next)
		current = next
	}
	if len(forward) != 50 {
		t.Fatalf("Forward walk got %d keys, want 50", len(forward))
	}
	for i := 1; i < len(forward); i++ {
		if forward[i] <= forward[i-1] {
			t.Errorf("Forward walk not strictly ascending at %d", i)
		}
	}

	// Walk backward
	var backward []string
	current, found, _ = c.LastWithPrefix("", false)
	if !found {
		t.Fatal("LastWithPrefix returned not found")
	}
	backward = append(backward, current)
	for {
		prev, ok, _ := c.PrevWithPrefix("", current, false)
		if !ok {
			break
		}
		backward = append(backward, prev)
		current = prev
	}
	if len(backward) != 50 {
		t.Fatalf("Backward walk got %d keys, want 50", len(backward))
	}

	// Backward should be reverse of forward
	for i := range forward {
		if forward[i] != backward[len(backward)-1-i] {
			t.Errorf("Forward[%d]=%q != Backward[%d]=%q", i, forward[i], len(backward)-1-i, backward[len(backward)-1-i])
		}
	}

	// Walk with binary prefix \x01 → should only yield 10 keys (i=10..19, first byte=1)
	var prefixed []string
	current, found, _ = c.FirstWithPrefix("\x01", false)
	if !found {
		t.Fatal("FirstWithPrefix(\\x01) returned not found")
	}
	prefixed = append(prefixed, current)
	for {
		next, ok, _ := c.NextWithPrefix("\x01", current, false)
		if !ok {
			break
		}
		prefixed = append(prefixed, next)
		current = next
	}
	if len(prefixed) != 10 {
		t.Fatalf("Prefixed walk got %d keys, want 10", len(prefixed))
	}
}

func TestCritbitMixedBinaryAndASCII(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	ascii := []string{"foo:bar", "hello", "world"}
	binary := []string{"\x00\x01\x02", "\xFF\xFE\xFD", string([]byte{0, 0, 0})}

	for i, key := range ascii {
		c.Insert(key, nil, NewTipSet(r(uint64(i+1))))
	}
	for i, key := range binary {
		c.Insert(key, nil, NewTipSet(r(uint64(i+10))))
	}

	if c.Size() != 6 {
		t.Fatalf("Expected size 6, got %d", c.Size())
	}
	for _, key := range append(ascii, binary...) {
		if c.Contains(key) == nil {
			t.Errorf("Contains(%q) returned nil", key)
		}
	}

	// Binary keys with low bytes should come before ASCII keys
	var got []string
	c.Range(func(key string) bool {
		got = append(got, key)
		return true
	})
	// \x00\x00\x00 and \x00\x01\x02 come first (byte 0 < 'f','h','w')
	if len(got) != 6 {
		t.Fatalf("Range returned %d keys", len(got))
	}
	if got[0][0] != 0x00 {
		t.Errorf("First key should start with \\x00, got %q", got[0])
	}

	// RangePrefix for ASCII
	var fooPrefixed []string
	c.RangePrefix("foo:", func(key string) bool {
		fooPrefixed = append(fooPrefixed, key)
		return true
	})
	if len(fooPrefixed) != 1 || fooPrefixed[0] != "foo:bar" {
		t.Errorf("RangePrefix(foo:) = %q, want [foo:bar]", fooPrefixed)
	}

	// RangePrefix for binary
	var nullPrefixed []string
	c.RangePrefix("\x00", func(key string) bool {
		nullPrefixed = append(nullPrefixed, key)
		return true
	})
	if len(nullPrefixed) != 2 {
		t.Errorf("RangePrefix(\\x00) = %d keys, want 2", len(nullPrefixed))
	}

	// MatchPattern("*") returns all
	all := c.MatchPattern("*")
	if len(all) != 6 {
		t.Errorf("MatchPattern(*) = %d, want 6", len(all))
	}
}

func TestCritbitMatchPatternBinaryKeys(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	c.Insert("\x01\x02\x03", nil, NewTipSet(r(1)))
	c.Insert("\x01\x02\x04", nil, NewTipSet(r(2)))
	c.Insert("\x01\x03\x03", nil, NewTipSet(r(3)))
	c.Insert("\x02\x02\x03", nil, NewTipSet(r(4)))

	// * matches all
	all := c.MatchPattern("*")
	if len(all) != 4 {
		t.Errorf("MatchPattern(*) = %d, want 4", len(all))
	}

	// \x01\x02? — ? matches single byte → 2 matches
	pattern := "\x01\x02?"
	matches := c.MatchPattern(pattern)
	if len(matches) != 2 {
		t.Errorf("MatchPattern(\\x01\\x02?) = %d, want 2: %q", len(matches), matches)
	}

	// \x01* → 3 matches
	matches = c.MatchPattern("\x01*")
	if len(matches) != 3 {
		t.Errorf("MatchPattern(\\x01*) = %d, want 3", len(matches))
	}

	// \x01?\x03 — middle byte wildcard → 2 matches (\x01\x02\x03 and \x01\x03\x03)
	matches = c.MatchPattern("\x01?\x03")
	if len(matches) != 2 {
		t.Errorf("MatchPattern(\\x01?\\x03) = %d, want 2: %q", len(matches), matches)
	}
}

func TestCritbitTryClaimBinaryKeys(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	c.Insert("\x00\x01", nil, NewTipSet(r(1)))
	c.Insert("\xFF\xFE", nil, NewTipSet(r(2)))

	exists, release := c.TryClaimKey("\x00\x01")
	if !exists || release == nil {
		t.Error("TryClaimKey for existing binary key should return true")
	}
	release() // should not panic

	exists, release = c.TryClaimKey("\xFF\xFE")
	if !exists || release == nil {
		t.Error("TryClaimKey for existing binary key should return true")
	}

	// Non-existent key
	exists, release = c.TryClaimKey("\x00\x02")
	if exists || release != nil {
		t.Error("TryClaimKey for missing key should return false, nil")
	}

	// Deleted key
	c.Delete("\x00\x01")
	exists, release = c.TryClaimKey("\x00\x01")
	if exists || release != nil {
		t.Error("TryClaimKey for deleted key should return false, nil")
	}
}

func TestCritbitSnapshotBinaryKeys(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	c.Insert("\x00\x01", nil, NewTipSet(r(1)))
	c.Insert("\x80\x80", nil, NewTipSet(r(2)))
	c.Insert("\xFF\xFF", nil, NewTipSet(r(3)))

	snap := c.Snapshot()
	defer snap.Close()

	if snap.Size() != 3 {
		t.Fatalf("Snapshot size = %d, want 3", snap.Size())
	}
	for _, key := range []string{"\x00\x01", "\x80\x80", "\xFF\xFF"} {
		if snap.Contains(key) == nil {
			t.Errorf("Snapshot missing key %q", key)
		}
	}

	// Mutation isolation: insert into original
	c.Insert("\xAA\xBB", nil, NewTipSet(r(4)))
	if snap.Contains("\xAA\xBB") != nil {
		t.Error("Snapshot should not see key inserted after snapshot")
	}

	// Mutation isolation: delete from original
	c.Delete("\x00\x01")
	if snap.Contains("\x00\x01") == nil {
		t.Error("Snapshot should still have key deleted from original")
	}

	// Verify snapshot keys in order
	expected := []string{"\x00\x01", "\x80\x80", "\xFF\xFF"}
	got := snap.Keys()
	if len(got) != 3 {
		t.Fatalf("Snapshot Keys() = %d, want 3", len(got))
	}
	for i := range expected {
		if got[i] != expected[i] {
			t.Errorf("Snapshot Keys()[%d] = %q, want %q", i, got[i], expected[i])
		}
	}
}

func TestCritbitRemoveTipsBinaryKeys(t *testing.T) {
	c := NewCritbit()
	defer c.Close()

	c.Insert("\x00\x01", nil, NewTipSet(r(10), r(20), r(30)))

	// Remove one tip
	c.RemoveTips("\x00\x01", []EffectRef{r(20)})
	ts := c.Contains("\x00\x01")
	if ts == nil {
		t.Fatal("Key should still exist after RemoveTips")
	}
	if ts.Contains(r(20)) {
		t.Error("Removed tip should not be present")
	}
	if !ts.Contains(r(10)) || !ts.Contains(r(30)) {
		t.Error("Non-removed tips should still be present")
	}

	// Remove remaining tips
	c.RemoveTips("\x00\x01", []EffectRef{r(10), r(30)})
	ts = c.Contains("\x00\x01")
	if ts == nil {
		t.Fatal("Key should still exist even with no tips")
	}
	if ts.Len() != 0 {
		t.Errorf("Expected 0 tips, got %d", ts.Len())
	}

	// RemoveTips on non-existent key — should not panic
	c.RemoveTips("\xFF\xFF", []EffectRef{r(1)})

	// RemoveTips on deleted key — should not panic
	c.Insert("\x80", nil, NewTipSet(r(5)))
	c.Delete("\x80")
	c.RemoveTips("\x80", []EffectRef{r(5)})
}
