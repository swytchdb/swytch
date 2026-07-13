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
	"errors"
	"math/bits"

	"github.com/cespare/xxhash/v2"
)

// Bloom is an approximate set of key names with no false negatives: Has
// returns true for every name ever Set, and false-positives at a rate set by
// the fill fraction (FP ≈ fill^bloomK). It backs the cloud key-name filter on
// both sides of the wire — the dataplane builds one per cluster and ships it
// as a frame; the node queries it to gate read-miss cloud consults — so the
// hashing is fixed: a filter built on one process and queried on another must
// agree. Unlike CuckooChain it cannot grow; the holder rebuilds at a larger
// size when Fill crosses BloomResizeFill.
//
// Not internally synchronized: a holder either treats a built filter as
// immutable (build fully, then publish by pointer swap — the frame path) or
// guards Set/Has under its own lock (CloudSync.filterMu).
type Bloom struct {
	bits    []byte
	mask    uint64 // m-1, where m = len(bits)*8 (a power of two)
	setBits uint64
}

const (
	// bloomK is the number of bit positions per name. At the
	// BloomTargetBitsPerKey build density (fill ≈ 0.5) it puts the FP rate
	// near 1%.
	bloomK = 7

	// BloomMinBytes is the smallest bitmap: 4KB ≈ 3k keys at target density.
	BloomMinBytes = 4096

	// BloomTargetBitsPerKey sizes a build: m = next power of two ≥ 10·n bits
	// lands the fill between ~0.3 and ~0.5 for n keys.
	BloomTargetBitsPerKey = 10

	// BloomResizeFill is the set-bit fraction past which the holder should
	// rebuild larger — above it the FP rate degrades past ~2%.
	BloomResizeFill = 0.55

	// bloomFrameVersion tags the wire frame; the decoder rejects anything
	// else, so a frame from a different scheme can never be misread as bits.
	bloomFrameVersion = 0x01
)

// NewBloom returns a filter over sizeBytes of bitmap, rounded up to a power
// of two no smaller than BloomMinBytes.
func NewBloom(sizeBytes int) *Bloom {
	n := BloomMinBytes
	for n < sizeBytes {
		n <<= 1
	}
	return &Bloom{bits: make([]byte, n), mask: uint64(n)*8 - 1}
}

// NewBloomForCount returns a filter sized for count names at the target
// build density.
func NewBloomForCount(count int) *Bloom {
	return NewBloom((count*BloomTargetBitsPerKey + 7) / 8)
}

// BloomHash is the one hash of a name every position derives from — the only
// thing a caller needs to retain per name to rebuild a filter.
func BloomHash(name []byte) uint64 { return xxhash.Sum64(name) }

// position returns the i-th bit index for a hash: double hashing, with the
// stride forced odd so it walks the full power-of-two bitmap.
func (b *Bloom) position(h, i uint64) uint64 {
	return (h + i*((h>>32)|1)) & b.mask
}

// SetHash sets the name's bits, reporting whether any bit flipped (false
// means the filter already covered the name — nothing to re-push).
func (b *Bloom) SetHash(h uint64) bool {
	flipped := false
	for i := range uint64(bloomK) {
		g := b.position(h, i)
		bit := byte(1) << (g & 7)
		if b.bits[g>>3]&bit == 0 {
			b.bits[g>>3] |= bit
			b.setBits++
			flipped = true
		}
	}
	return flipped
}

// Set is SetHash over a raw name.
func (b *Bloom) Set(name []byte) bool { return b.SetHash(BloomHash(name)) }

// HasHash reports whether the name may be in the set; false is authoritative.
func (b *Bloom) HasHash(h uint64) bool {
	for i := range uint64(bloomK) {
		g := b.position(h, i)
		if b.bits[g>>3]&(byte(1)<<(g&7)) == 0 {
			return false
		}
	}
	return true
}

// Has is HasHash over a raw name.
func (b *Bloom) Has(name []byte) bool { return b.HasHash(BloomHash(name)) }

// Fill is the set-bit fraction — the resize signal and the FP-rate base.
func (b *Bloom) Fill() float64 {
	return float64(b.setBits) / float64(len(b.bits)*8)
}

// SizeBytes is the bitmap size.
func (b *Bloom) SizeBytes() int { return len(b.bits) }

// Frame serializes the filter for the wire: one version byte, then the bitmap.
func (b *Bloom) Frame() []byte {
	out := make([]byte, 1+len(b.bits))
	out[0] = bloomFrameVersion
	copy(out[1:], b.bits)
	return out
}

// ParseBloomFrame decodes a Frame, copying the bitmap out of the (possibly
// transport-owned) buffer. Anything malformed — wrong version, undersized or
// non-power-of-two body — is rejected; the input crosses process boundaries.
func ParseBloomFrame(data []byte) (*Bloom, error) {
	if len(data) < 1 || data[0] != bloomFrameVersion {
		return nil, errors.New("bloom frame: bad version")
	}
	body := data[1:]
	n := len(body)
	if n < BloomMinBytes || n&(n-1) != 0 {
		return nil, errors.New("bloom frame: body is not a power-of-two bitmap")
	}
	b := &Bloom{bits: make([]byte, n), mask: uint64(n)*8 - 1}
	copy(b.bits, body)
	for _, v := range b.bits {
		b.setBits += uint64(bits.OnesCount8(v))
	}
	return b, nil
}
