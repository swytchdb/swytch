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
	"testing"
)

func TestZSetValueBasicOperations(t *testing.T) {
	zset := &ZSetValue{Members: make(map[string]float64)}

	// Test ZAdd
	added := zset.ZAdd(map[string]float64{
		"a": 1.0,
		"b": 2.0,
		"c": 3.0,
	})
	if added != 3 {
		t.Errorf("ZAdd: expected 3 added, got %d", added)
	}

	// Test ZCard
	if zset.ZCard() != 3 {
		t.Errorf("ZCard: expected 3, got %d", zset.ZCard())
	}

	// Test ZScore
	score, exists := zset.ZScore("b")
	if !exists || score != 2.0 {
		t.Errorf("ZScore: expected 2.0, got %v (exists: %v)", score, exists)
	}

	// Test non-existent member
	_, exists = zset.ZScore("nonexistent")
	if exists {
		t.Error("ZScore: expected non-existent member to return false")
	}

	// Test ZRem
	removed := zset.ZRem("b")
	if removed != 1 {
		t.Errorf("ZRem: expected 1 removed, got %d", removed)
	}

	if zset.ZCard() != 2 {
		t.Errorf("ZCard after ZRem: expected 2, got %d", zset.ZCard())
	}

	// Test ZRem non-existent
	removed = zset.ZRem("nonexistent")
	if removed != 0 {
		t.Errorf("ZRem non-existent: expected 0, got %d", removed)
	}
}

func TestZSetValueSorting(t *testing.T) {
	zset := &ZSetValue{Members: make(map[string]float64)}
	zset.ZAdd(map[string]float64{
		"c": 3.0,
		"a": 1.0,
		"b": 2.0,
		"d": 2.0, // Same score as b, should sort lexicographically
	})

	sorted := zset.Sorted()
	expected := []struct {
		member string
		score  float64
	}{
		{"a", 1.0},
		{"b", 2.0},
		{"d", 2.0},
		{"c", 3.0},
	}

	if len(sorted) != len(expected) {
		t.Fatalf("Sorted: expected %d entries, got %d", len(expected), len(sorted))
	}

	for i, exp := range expected {
		if sorted[i].Member != exp.member || sorted[i].Score != exp.score {
			t.Errorf("Sorted[%d]: expected (%s, %f), got (%s, %f)",
				i, exp.member, exp.score, sorted[i].Member, sorted[i].Score)
		}
	}

	// Test reverse sorting
	reversed := zset.SortedReverse()
	expectedRev := []struct {
		member string
		score  float64
	}{
		{"c", 3.0},
		{"d", 2.0},
		{"b", 2.0},
		{"a", 1.0},
	}

	for i, exp := range expectedRev {
		if reversed[i].Member != exp.member || reversed[i].Score != exp.score {
			t.Errorf("SortedReverse[%d]: expected (%s, %f), got (%s, %f)",
				i, exp.member, exp.score, reversed[i].Member, reversed[i].Score)
		}
	}
}

func TestZSetValueRank(t *testing.T) {
	zset := &ZSetValue{Members: make(map[string]float64)}
	zset.ZAdd(map[string]float64{
		"a": 1.0,
		"b": 2.0,
		"c": 3.0,
	})

	// Test ZRank (ascending)
	if rank := zset.ZRank("a"); rank != 0 {
		t.Errorf("ZRank('a'): expected 0, got %d", rank)
	}

	if rank := zset.ZRank("b"); rank != 1 {
		t.Errorf("ZRank('b'): expected 1, got %d", rank)
	}

	if rank := zset.ZRank("c"); rank != 2 {
		t.Errorf("ZRank('c'): expected 2, got %d", rank)
	}

	if rank := zset.ZRank("nonexistent"); rank != -1 {
		t.Errorf("ZRank('nonexistent'): expected -1, got %d", rank)
	}

	// Test ZRevRank (descending)
	if rank := zset.ZRevRank("c"); rank != 0 {
		t.Errorf("ZRevRank('c'): expected 0, got %d", rank)
	}

	if rank := zset.ZRevRank("b"); rank != 1 {
		t.Errorf("ZRevRank('b'): expected 1, got %d", rank)
	}

	if rank := zset.ZRevRank("a"); rank != 2 {
		t.Errorf("ZRevRank('a'): expected 2, got %d", rank)
	}
}

func TestZSetValueRange(t *testing.T) {
	zset := &ZSetValue{Members: make(map[string]float64)}
	zset.ZAdd(map[string]float64{
		"a": 1.0,
		"b": 2.0,
		"c": 3.0,
		"d": 4.0,
		"e": 5.0,
	})

	// Test ZRange with positive indices
	entries := zset.ZRange(1, 3, false)
	if len(entries) != 3 {
		t.Fatalf("ZRange(1,3): expected 3 entries, got %d", len(entries))
	}

	if entries[0].Member != "b" || entries[1].Member != "c" || entries[2].Member != "d" {
		t.Errorf("ZRange(1,3): unexpected members")
	}

	// Test ZRange with negative indices
	entries = zset.ZRange(-2, -1, false)
	if len(entries) != 2 {
		t.Fatalf("ZRange(-2,-1): expected 2 entries, got %d", len(entries))
	}

	if entries[0].Member != "d" || entries[1].Member != "e" {
		t.Errorf("ZRange(-2,-1): expected d,e, got %s,%s", entries[0].Member, entries[1].Member)
	}

	// Test ZRange reverse
	entries = zset.ZRange(0, 2, true)
	if len(entries) != 3 {
		t.Fatalf("ZRange reverse: expected 3 entries, got %d", len(entries))
	}

	if entries[0].Member != "e" || entries[1].Member != "d" || entries[2].Member != "c" {
		t.Errorf("ZRange reverse: unexpected order")
	}
}

func TestZSetValueRangeByScore(t *testing.T) {
	zset := &ZSetValue{Members: make(map[string]float64)}
	zset.ZAdd(map[string]float64{
		"a": 1.0,
		"b": 2.0,
		"c": 3.0,
		"d": 4.0,
		"e": 5.0,
	})

	// Test inclusive range
	entries := zset.ZRangeByScore(2.0, 4.0, false, false, false, 0, -1)
	if len(entries) != 3 {
		t.Fatalf("ZRangeByScore(2,4): expected 3 entries, got %d", len(entries))
	}

	if entries[0].Member != "b" || entries[1].Member != "c" || entries[2].Member != "d" {
		t.Errorf("ZRangeByScore(2,4): unexpected members")
	}

	// Test exclusive min
	entries = zset.ZRangeByScore(2.0, 4.0, true, false, false, 0, -1)
	if len(entries) != 2 {
		t.Fatalf("ZRangeByScore((2,4]: expected 2 entries, got %d", len(entries))
	}

	if entries[0].Member != "c" || entries[1].Member != "d" {
		t.Errorf("ZRangeByScore((2,4]: unexpected members")
	}

	// Test exclusive max
	entries = zset.ZRangeByScore(2.0, 4.0, false, true, false, 0, -1)
	if len(entries) != 2 {
		t.Fatalf("ZRangeByScore([2,4): expected 2 entries, got %d", len(entries))
	}

	// Test with limit
	entries = zset.ZRangeByScore(1.0, 5.0, false, false, false, 1, 2)
	if len(entries) != 2 {
		t.Fatalf("ZRangeByScore with LIMIT: expected 2 entries, got %d", len(entries))
	}

	if entries[0].Member != "b" || entries[1].Member != "c" {
		t.Errorf("ZRangeByScore with LIMIT: unexpected members")
	}
}

func TestZSetValueCount(t *testing.T) {
	zset := &ZSetValue{Members: make(map[string]float64)}
	zset.ZAdd(map[string]float64{
		"a": 1.0,
		"b": 2.0,
		"c": 3.0,
		"d": 4.0,
		"e": 5.0,
	})

	// Inclusive count
	if count := zset.ZCount(2.0, 4.0, false, false); count != 3 {
		t.Errorf("ZCount(2,4): expected 3, got %d", count)
	}

	// Exclusive min
	if count := zset.ZCount(2.0, 4.0, true, false); count != 2 {
		t.Errorf("ZCount((2,4]: expected 2, got %d", count)
	}

	// Exclusive max
	if count := zset.ZCount(2.0, 4.0, false, true); count != 2 {
		t.Errorf("ZCount([2,4): expected 2, got %d", count)
	}

	// Both exclusive
	if count := zset.ZCount(2.0, 4.0, true, true); count != 1 {
		t.Errorf("ZCount((2,4)): expected 1, got %d", count)
	}
}

func TestZSetValueIncrBy(t *testing.T) {
	zset := &ZSetValue{Members: make(map[string]float64)}
	zset.ZAdd(map[string]float64{"a": 1.0})

	// Increment existing member
	newScore := zset.ZIncrBy("a", 2.5)
	if newScore != 3.5 {
		t.Errorf("ZIncrBy existing: expected 3.5, got %f", newScore)
	}

	// Increment non-existent member (creates it)
	newScore = zset.ZIncrBy("b", 5.0)
	if newScore != 5.0 {
		t.Errorf("ZIncrBy new: expected 5.0, got %f", newScore)
	}

	score, exists := zset.ZScore("b")
	if !exists || score != 5.0 {
		t.Errorf("ZIncrBy new member score: expected 5.0, got %f", score)
	}
}

func TestZSetValuePopMin(t *testing.T) {
	zset := &ZSetValue{Members: make(map[string]float64)}
	zset.ZAdd(map[string]float64{
		"a": 1.0,
		"b": 2.0,
		"c": 3.0,
	})

	// Pop single element
	entry, ok := zset.ZPopMin()
	if !ok {
		t.Fatal("ZPopMin: expected ok=true")
	}

	if entry.Member != "a" || entry.Score != 1.0 {
		t.Errorf("ZPopMin: expected (a, 1.0), got (%s, %f)", entry.Member, entry.Score)
	}

	if zset.ZCard() != 2 {
		t.Errorf("ZCard after ZPopMin: expected 2, got %d", zset.ZCard())
	}

	// Pop multiple elements
	entries := zset.ZPopMinN(2)
	if len(entries) != 2 {
		t.Fatalf("ZPopMinN(2): expected 2 entries, got %d", len(entries))
	}

	if entries[0].Member != "b" || entries[1].Member != "c" {
		t.Errorf("ZPopMinN(2): unexpected members")
	}

	if zset.ZCard() != 0 {
		t.Errorf("ZCard after ZPopMinN: expected 0, got %d", zset.ZCard())
	}
}

func TestZSetValuePopMax(t *testing.T) {
	zset := &ZSetValue{Members: make(map[string]float64)}
	zset.ZAdd(map[string]float64{
		"a": 1.0,
		"b": 2.0,
		"c": 3.0,
	})

	// Pop single element
	entry, ok := zset.ZPopMax()
	if !ok {
		t.Fatal("ZPopMax: expected ok=true")
	}

	if entry.Member != "c" || entry.Score != 3.0 {
		t.Errorf("ZPopMax: expected (c, 3.0), got (%s, %f)", entry.Member, entry.Score)
	}

	// Pop multiple elements
	entries := zset.ZPopMaxN(2)
	if len(entries) != 2 {
		t.Fatalf("ZPopMaxN(2): expected 2 entries, got %d", len(entries))
	}

	if entries[0].Member != "b" || entries[1].Member != "a" {
		t.Errorf("ZPopMaxN(2): unexpected members, got %s, %s", entries[0].Member, entries[1].Member)
	}
}

func TestZSetValueAddWithOptions(t *testing.T) {
	// Test NX (only add if not exists)
	zset := &ZSetValue{Members: make(map[string]float64)}
	zset.Members["a"] = 1.0

	added, changed := zset.ZAddWithOptions("a", 2.0, true, false, false, false) // NX
	if added || changed {
		t.Error("NX on existing member should return (false, false)")
	}

	if score, _ := zset.ZScore("a"); score != 1.0 {
		t.Errorf("NX should not change score, got %f", score)
	}

	added, changed = zset.ZAddWithOptions("b", 2.0, true, false, false, false) // NX
	if !added || !changed {
		t.Error("NX on new member should return (true, true)")
	}

	// Test XX (only update if exists)
	zset2 := &ZSetValue{Members: make(map[string]float64)}
	zset2.Members["a"] = 1.0

	added, changed = zset2.ZAddWithOptions("b", 2.0, false, true, false, false) // XX
	if added || changed {
		t.Error("XX on non-existent member should return (false, false)")
	}

	added, changed = zset2.ZAddWithOptions("a", 2.0, false, true, false, false) // XX
	if added || !changed {
		t.Error("XX on existing member should return (false, true)")
	}

	// Test GT (only update if greater)
	zset3 := &ZSetValue{Members: make(map[string]float64)}
	zset3.Members["a"] = 5.0

	added, changed = zset3.ZAddWithOptions("a", 3.0, false, false, true, false) // GT, new < old
	if added || changed {
		t.Error("GT with lower score should return (false, false)")
	}

	added, changed = zset3.ZAddWithOptions("a", 7.0, false, false, true, false) // GT, new > old
	if added || !changed {
		t.Error("GT with higher score should return (false, true)")
	}

	// Test LT (only update if less)
	zset4 := &ZSetValue{Members: make(map[string]float64)}
	zset4.Members["a"] = 5.0

	added, changed = zset4.ZAddWithOptions("a", 7.0, false, false, false, true) // LT, new > old
	if added || changed {
		t.Error("LT with higher score should return (false, false)")
	}

	added, changed = zset4.ZAddWithOptions("a", 3.0, false, false, false, true) // LT, new < old
	if added || !changed {
		t.Error("LT with lower score should return (false, true)")
	}
}
