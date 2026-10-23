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

package sql

import (
	"bytes"
	"math"
	"sort"
	"testing"

	"zombiezen.com/go/sqlite"
)

// TestEncodeIndexValueInteger verifies that encoding int64 values and
// then sorting them lex-wise as bytes produces the same order as
// sorting them numerically. This is the core property the index
// range-scan relies on.
func TestEncodeIndexValueInteger(t *testing.T) {
	nums := []int64{
		math.MinInt64, -1000, -1, 0, 1, 1000, math.MaxInt64,
	}

	encoded := make([][]byte, len(nums))
	for i, n := range nums {
		encoded[i] = encodeIndexValue(sqlite.IntegerValue(n), affinityInteger)
		if len(encoded[i]) != 8 {
			t.Errorf("int %d: encoded length %d, want 8", n, len(encoded[i]))
		}
	}

	// Check lex order matches numeric order.
	for i := 1; i < len(encoded); i++ {
		if bytes.Compare(encoded[i-1], encoded[i]) >= 0 {
			t.Errorf("lex order broken between %d and %d: %x vs %x",
				nums[i-1], nums[i], encoded[i-1], encoded[i])
		}
	}
}

func TestEncodeIndexValueReal(t *testing.T) {
	nums := []float64{
		math.Inf(-1), -1e308, -1.5, -0.001, 0, 0.001, 1.5, 1e308, math.Inf(1),
	}

	encoded := make([][]byte, len(nums))
	for i, n := range nums {
		encoded[i] = encodeIndexValue(sqlite.FloatValue(n), affinityReal)
		if len(encoded[i]) != 8 {
			t.Errorf("float %v: encoded length %d, want 8", n, len(encoded[i]))
		}
	}

	for i := 1; i < len(encoded); i++ {
		if bytes.Compare(encoded[i-1], encoded[i]) >= 0 {
			t.Errorf("lex order broken between %v and %v: %x vs %x",
				nums[i-1], nums[i], encoded[i-1], encoded[i])
		}
	}
}

func TestEncodeIndexValueText(t *testing.T) {
	strs := []string{"", "a", "apple", "banana", "foo", "foo/bar", "zebra"}

	encoded := make([][]byte, len(strs))
	for i, s := range strs {
		encoded[i] = encodeIndexValue(sqlite.TextValue(s), affinityText)
	}

	// Verify lex byte-compare matches Go string compare (which matches
	// SQL BINARY collation).
	want := append([]string(nil), strs...)
	sort.Strings(want)

	if !sortedEqual(strs, want) {
		t.Fatalf("input not pre-sorted: %v vs %v", strs, want)
	}

	for i := 1; i < len(encoded); i++ {
		if bytes.Compare(encoded[i-1], encoded[i]) > 0 {
			t.Errorf("lex order broken between %q and %q", strs[i-1], strs[i])
		}
	}
}

func TestEncodeIndexValueRangeScan(t *testing.T) {
	// Pick a random-looking set, encode them, sort by encoded bytes,
	// verify the resulting numeric order is ascending.
	nums := []int64{42, -7, 0, 999, -2, 1, -1000, math.MaxInt64 / 2}

	type pair struct {
		n   int64
		enc []byte
	}

	pairs := make([]pair, len(nums))
	for i, n := range nums {
		pairs[i] = pair{n, encodeIndexValue(sqlite.IntegerValue(n), affinityInteger)}
	}

	sort.Slice(pairs, func(i, j int) bool {
		return bytes.Compare(pairs[i].enc, pairs[j].enc) < 0
	})

	for i := 1; i < len(pairs); i++ {
		if pairs[i-1].n >= pairs[i].n {
			t.Errorf("order broken at %d: %d should come before %d",
				i, pairs[i-1].n, pairs[i].n)
		}
	}
}

func sortedEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
