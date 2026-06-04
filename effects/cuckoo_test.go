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

package effects

import (
	"fmt"
	"testing"
)

// TestCuckooNoFalseNegatives is the load-bearing property: every key
// successfully Added must be reported present.
func TestCuckooNoFalseNegatives(t *testing.T) {
	const n = 50_000
	c := NewCuckooFilter(n)
	added := make([]string, 0, n)
	for i := range n {
		k := fmt.Sprintf("key:%d", i)
		if err := c.Add(k); err != nil {
			t.Fatalf("Add(%q) failed at load %.3f: %v", k, c.LoadFactor(), err)
		}
		added = append(added, k)
	}
	for _, k := range added {
		if !c.MaybeContains(k) {
			t.Fatalf("false negative for %q", k)
		}
	}
}

// TestCuckooFalsePositiveRate checks the FPR for absent keys stays well
// below 1% with 16-bit fingerprints.
func TestCuckooFalsePositiveRate(t *testing.T) {
	const n = 50_000
	c := NewCuckooFilter(n)
	for i := range n {
		if err := c.Add(fmt.Sprintf("key:%d", i)); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}
	const probes = 100_000
	fp := 0
	for i := range probes {
		if c.MaybeContains(fmt.Sprintf("absent:%d", i)) {
			fp++
		}
	}
	rate := float64(fp) / float64(probes)
	if rate > 0.01 {
		t.Fatalf("false-positive rate %.4f exceeds 1%%", rate)
	}
	t.Logf("false-positive rate: %.4f", rate)
}

// TestCuckooMarshalRoundTrip verifies a serialized filter answers queries
// identically after decoding — the cross-node shipping requirement.
func TestCuckooMarshalRoundTrip(t *testing.T) {
	const n = 10_000
	c := NewCuckooFilter(n)
	for i := range n {
		if err := c.Add(fmt.Sprintf("key:%d", i)); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}
	data, err := c.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	var dec CuckooFilter
	if err := dec.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if dec.Count() != c.Count() {
		t.Fatalf("count mismatch: got %d want %d", dec.Count(), c.Count())
	}
	for i := range n {
		k := fmt.Sprintf("key:%d", i)
		if !dec.MaybeContains(k) {
			t.Fatalf("decoded filter false negative for %q", k)
		}
	}
	// And it agrees with the original on absent keys (same hashing/layout).
	for i := range 5_000 {
		k := fmt.Sprintf("absent:%d", i)
		if c.MaybeContains(k) != dec.MaybeContains(k) {
			t.Fatalf("decode disagreement on %q", k)
		}
	}
}

func TestCuckooUnmarshalRejectsGarbage(t *testing.T) {
	var c CuckooFilter
	if err := c.UnmarshalBinary([]byte{1, 2, 3}); err == nil {
		t.Fatal("expected error on short buffer")
	}
	// numBuckets=3 (not power of two)
	bad := make([]byte, 8)
	bad[7] = 3
	if err := c.UnmarshalBinary(bad); err == nil {
		t.Fatal("expected error on non-power-of-two numBuckets")
	}
}

// TestCuckooChainNoFalseNegativesAcrossRotation is the regression for the
// victim-loss bug: a chain spanning many segments must never drop a key. With
// >> cuckooChainChunk distinct keys the chain rotates repeatedly; every key
// must still report present, and the marshal round-trip must preserve them.
func TestCuckooChainNoFalseNegativesAcrossRotation(t *testing.T) {
	const n = cuckooChainChunk*5 + 123 // force several rotations
	var cc cuckooChain
	for i := range n {
		cc.add(fmt.Sprintf("key:%d", i))
	}
	for i := range n {
		k := fmt.Sprintf("key:%d", i)
		if !cc.maybeContains(k) {
			t.Fatalf("false negative across rotation for %q", k)
		}
	}
	// Round-trip the whole multi-segment chain.
	data, err := cc.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	var dec cuckooChain
	if err := dec.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	for i := range n {
		k := fmt.Sprintf("key:%d", i)
		if !dec.maybeContains(k) {
			t.Fatalf("decoded chain false negative for %q", k)
		}
	}
}

// TestCuckooChainAddReportsChange checks add's return: true for a new key,
// false for an idempotent re-add (drives the version-bump skip).
func TestCuckooChainAddReportsChange(t *testing.T) {
	var cc cuckooChain
	if !cc.add("x") {
		t.Fatal("first add of a key should report a change")
	}
	if cc.add("x") {
		t.Fatal("re-adding the same key should report no change")
	}
}

func TestCuckooAddIdempotent(t *testing.T) {
	c := NewCuckooFilter(100)
	if err := c.Add("x"); err != nil {
		t.Fatal(err)
	}
	before := c.Count()
	if err := c.Add("x"); err != nil {
		t.Fatal(err)
	}
	if c.Count() != before {
		t.Fatalf("re-adding changed count: %d -> %d", before, c.Count())
	}
}
