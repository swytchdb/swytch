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

// Every name ever Set must answer Has — the free-miss gate turns a false
// negative into durable data no read ever sees.
func TestBloomNoFalseNegatives(t *testing.T) {
	const n = 100_000
	b := NewBloomForCount(n)
	for i := range n {
		b.Set(fmt.Appendf(nil, "key-%d", i))
	}
	for i := range n {
		if !b.Has(fmt.Appendf(nil, "key-%d", i)) {
			t.Fatalf("false negative for key-%d", i)
		}
	}
}

// At the build density the FP rate stays near 1%; well under the resize
// threshold's ~2% bound.
func TestBloomFalsePositiveRate(t *testing.T) {
	const n = 100_000
	b := NewBloomForCount(n)
	for i := range n {
		b.Set(fmt.Appendf(nil, "key-%d", i))
	}
	if f := b.Fill(); f > 0.5 {
		t.Fatalf("fill %.3f above build target", f)
	}
	fp := 0
	const probes = 100_000
	for i := range probes {
		if b.Has(fmt.Appendf(nil, "absent-%d", i)) {
			fp++
		}
	}
	if rate := float64(fp) / probes; rate > 0.02 {
		t.Fatalf("FP rate %.4f exceeds 2%%", rate)
	}
}

// SetHash reports a flip only when the filter didn't already cover the name.
func TestBloomSetReportsFlips(t *testing.T) {
	b := NewBloom(BloomMinBytes)
	if !b.Set([]byte("fresh")) {
		t.Fatal("first Set of a name reported no flip")
	}
	if b.Set([]byte("fresh")) {
		t.Fatal("re-Set of a covered name reported a flip")
	}
}

// A frame decodes into a filter that answers identically, including the
// set-bit count driving the resize signal.
func TestBloomFrameRoundTrip(t *testing.T) {
	b := NewBloom(BloomMinBytes)
	names := [][]byte{[]byte("alpha"), []byte("bravo"), []byte("charlie")}
	for _, name := range names {
		b.Set(name)
	}

	got, err := ParseBloomFrame(b.Frame())
	if err != nil {
		t.Fatalf("ParseBloomFrame: %v", err)
	}
	for _, name := range names {
		if !got.Has(name) {
			t.Fatalf("round trip lost %q", name)
		}
	}
	if got.Has([]byte("never-seen-name")) {
		t.Fatal("round trip fabricated a name (FP on a near-empty filter is effectively impossible)")
	}
	if got.Fill() != b.Fill() {
		t.Fatalf("fill diverged: %v != %v", got.Fill(), b.Fill())
	}
}

// The decoder must reject malformed input — frames cross process boundaries.
func TestBloomFrameRejectsMalformed(t *testing.T) {
	cases := map[string][]byte{
		"empty":            nil,
		"bad version":      append([]byte{0x7f}, make([]byte, BloomMinBytes)...),
		"undersized body":  append([]byte{bloomFrameVersion}, make([]byte, BloomMinBytes/2)...),
		"non-power-of-two": append([]byte{bloomFrameVersion}, make([]byte, BloomMinBytes+1)...),
	}
	for name, frame := range cases {
		if _, err := ParseBloomFrame(frame); err == nil {
			t.Fatalf("%s frame decoded without error", name)
		}
	}
}

// Sizing rounds up to a power of two and never below the floor.
func TestBloomSizing(t *testing.T) {
	if got := NewBloom(0).SizeBytes(); got != BloomMinBytes {
		t.Fatalf("zero-size request: got %d, want floor %d", got, BloomMinBytes)
	}
	if got := NewBloom(BloomMinBytes + 1).SizeBytes(); got != 2*BloomMinBytes {
		t.Fatalf("floor+1 request: got %d, want %d", got, 2*BloomMinBytes)
	}
	// 2M keys at 10 bits/key needs 2.5MB → 4MB after rounding.
	if got := NewBloomForCount(2_000_000).SizeBytes(); got != 4*1024*1024 {
		t.Fatalf("2M-key build: got %d, want 4MiB", got)
	}
}
