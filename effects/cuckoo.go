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
	"encoding/binary"
	"errors"
	"log/slog"
	"slices"

	"github.com/cespare/xxhash/v2"
)

// CuckooFilter is an approximate set of keys with no false negatives:
// MaybeContains returns true for every key ever successfully Added, and
// false-positives at a bounded rate. It backs the per-node cluster key
// filters that let a read-only handler answer a miss without subscribing.
//
// The filter is serialized and shipped between nodes (on NACKs), so the
// hashing and layout are fixed: a filter built on one node and queried on
// another must agree. Item placement (which candidate bucket a fingerprint
// landed in) does not affect queries — MaybeContains always probes both
// candidate buckets — so Add may relocate fingerprints freely.
type CuckooFilter struct {
	buckets    []uint16 // flat: numBuckets * bucketSize, 0 = empty slot
	numBuckets uint64   // power of two
	count      uint64   // live fingerprints
	kickSeed   uint64   // rotates eviction-slot choice; not load-bearing
}

const (
	cuckooBucketSize = 4
	cuckooMaxKicks   = 500
)

// ErrCuckooFull is returned by Add when the filter could not place a
// fingerprint after the maximum number of evictions. The own-filter
// recovers by rebuilding at a larger capacity from the index, which is the
// authoritative key set; peer filters are never Added to (they arrive
// pre-built) so they never see this.
var ErrCuckooFull = errors.New("cuckoo filter is full")

// NewCuckooFilter returns a filter sized to hold at least capacity keys
// before relocations begin to fail. numBuckets is rounded up to a power of
// two so the index masks are cheap.
func NewCuckooFilter(capacity int) *CuckooFilter {
	// Target ~0.95 load: capacity / (bucketSize * 0.95).
	want := uint64(capacity)/cuckooBucketSize + 1
	nb := uint64(1)
	for nb < want {
		nb <<= 1
	}
	return &CuckooFilter{
		buckets:    make([]uint16, nb*cuckooBucketSize),
		numBuckets: nb,
	}
}

// fingerprintAndIndices derives the (non-zero) fingerprint and the two
// candidate bucket indices for a key. Stable across nodes and across
// marshal round-trips for a given numBuckets.
func (c *CuckooFilter) fingerprintAndIndices(key string) (fp uint16, i1, i2 uint64) {
	h := xxhash.Sum64String(key)
	fp = uint16(h & 0xFFFF)
	if fp == 0 {
		fp = 1 // 0 is the empty sentinel
	}
	mask := c.numBuckets - 1
	i1 = (h >> 32) & mask
	i2 = c.altIndex(i1, fp)
	return fp, i1, i2
}

// altIndex returns the partner bucket of i for fingerprint fp. It is an
// involution: altIndex(altIndex(i, fp), fp) == i, which is what lets a
// kicked fingerprint find its way back.
func (c *CuckooFilter) altIndex(i uint64, fp uint16) uint64 {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], fp)
	return (i ^ xxhash.Sum64(b[:])) & (c.numBuckets - 1)
}

func (c *CuckooFilter) bucket(i uint64) []uint16 {
	start := i * cuckooBucketSize
	return c.buckets[start : start+cuckooBucketSize]
}

func insertIntoBucket(bkt []uint16, fp uint16) bool {
	for i, v := range bkt {
		if v == 0 {
			bkt[i] = fp
			return true
		}
	}
	return false
}

func bucketContains(bkt []uint16, fp uint16) bool {
	return slices.Contains(bkt, fp)
}

// Add inserts key. It is idempotent at the fingerprint level: re-adding a
// key whose fingerprint already sits in a candidate bucket is a no-op.
// Returns ErrCuckooFull if no slot could be freed.
func (c *CuckooFilter) Add(key string) error {
	fp, i1, i2 := c.fingerprintAndIndices(key)
	if bucketContains(c.bucket(i1), fp) || bucketContains(c.bucket(i2), fp) {
		return nil
	}
	if insertIntoBucket(c.bucket(i1), fp) || insertIntoBucket(c.bucket(i2), fp) {
		c.count++
		return nil
	}

	// Both candidate buckets are full — evict a resident and relocate it.
	i := i1
	if c.kickSeed&1 == 1 {
		i = i2
	}
	for range cuckooMaxKicks {
		c.kickSeed++
		bkt := c.bucket(i)
		slot := c.kickSeed % cuckooBucketSize
		fp, bkt[slot] = bkt[slot], fp
		i = c.altIndex(i, fp)
		if insertIntoBucket(c.bucket(i), fp) {
			c.count++
			return nil
		}
	}
	return ErrCuckooFull
}

// MaybeContains reports whether key may be present. False positives are
// possible; false negatives are not (for keys successfully Added).
func (c *CuckooFilter) MaybeContains(key string) bool {
	fp, i1, i2 := c.fingerprintAndIndices(key)
	return bucketContains(c.bucket(i1), fp) || bucketContains(c.bucket(i2), fp)
}

// Count returns the number of live fingerprints.
func (c *CuckooFilter) Count() uint64 { return c.count }

// LoadFactor is the fraction of slots occupied.
func (c *CuckooFilter) LoadFactor() float64 {
	return float64(c.count) / float64(c.numBuckets*cuckooBucketSize)
}

// MarshalBinary serializes the filter: numBuckets (8 bytes, big-endian)
// followed by every slot as a big-endian uint16.
func (c *CuckooFilter) MarshalBinary() ([]byte, error) {
	out := make([]byte, 8+len(c.buckets)*2)
	binary.BigEndian.PutUint64(out[:8], c.numBuckets)
	for i, v := range c.buckets {
		binary.BigEndian.PutUint16(out[8+i*2:], v)
	}
	return out, nil
}

// UnmarshalBinary restores a filter produced by MarshalBinary. The decoded
// filter is query-only in practice (peer filters are never Added to), but
// Add still works should that change.
func (c *CuckooFilter) UnmarshalBinary(data []byte) error {
	if len(data) < 8 {
		return errors.New("cuckoo: short buffer")
	}
	nb := binary.BigEndian.Uint64(data[:8])
	if nb == 0 || nb&(nb-1) != 0 {
		return errors.New("cuckoo: numBuckets not a power of two")
	}
	body := data[8:]
	if uint64(len(body)) != nb*cuckooBucketSize*2 {
		return errors.New("cuckoo: body length mismatch")
	}
	buckets := make([]uint16, nb*cuckooBucketSize)
	var count uint64
	for i := range buckets {
		v := binary.BigEndian.Uint16(body[i*2:])
		buckets[i] = v
		if v != 0 {
			count++
		}
	}
	c.buckets = buckets
	c.numBuckets = nb
	c.count = count
	c.kickSeed = 0
	return nil
}

// cuckooChainChunk is the number of distinct keys the active segment accepts
// before the chain rotates to a fresh one. NewCuckooFilter sizes a segment
// for ~2x this, so the active segment never exceeds ~50% load — far below the
// density where cuckoo insertion can fail. This is what keeps add off the
// (corrupting) ErrCuckooFull path: a failed Add evicts a resident fingerprint
// it then can't replace, silently dropping a previously-added key.
const cuckooChainChunk = 4096

// CuckooChain is a queryable approximate set with NO false negatives, built
// from a sequence of CuckooFilters split into:
//   - active: the single writable segment, always one we allocated and kept
//     below cuckooChainChunk distinct keys, so Add on it never fails.
//   - sealed: query-only segments — filled-up former active segments and any
//     spliced peer segments. Never written to, so a near-full or foreign
//     segment can't be corrupted by an Add.
//
// It never shrinks; a deleted key lingers as a safe false-positive (a needless
// subscribe, never a wrong miss) until the chain is rebuilt.
type CuckooChain struct {
	sealed []*CuckooFilter
	active *CuckooFilter
}

// Add inserts key into the active segment, rotating first if that segment is
// at capacity. Returns true iff a new fingerprint was actually stored (false
// for an idempotent re-add), so callers can skip version bumps when the set
// did not change.
func (cc *CuckooChain) Add(key string) bool {
	if cc.active == nil {
		cc.active = NewCuckooFilter(cuckooChainChunk)
	} else if cc.active.Count() >= cuckooChainChunk {
		cc.sealed = append(cc.sealed, cc.active)
		cc.active = NewCuckooFilter(cuckooChainChunk)
	}
	before := cc.active.Count()
	if err := cc.active.Add(key); err != nil {
		// Unreachable in practice: the active segment is held at ~50% load,
		// far below where Add can fail. If it ever does, seal the (possibly
		// corrupt) segment and add to a fresh one so the key isn't lost, and
		// surface it loudly.
		slog.Error("cuckooChain: active segment full below rotation threshold", "error", err)
		cc.sealed = append(cc.sealed, cc.active)
		cc.active = NewCuckooFilter(cuckooChainChunk)
		_ = cc.active.Add(key)
		return true
	}
	return cc.active.Count() != before
}

// MaybeContains reports whether key may be in the set; false is authoritative.
func (cc *CuckooChain) MaybeContains(key string) bool {
	if cc.active != nil && cc.active.MaybeContains(key) {
		return true
	}
	for _, f := range cc.sealed {
		if f.MaybeContains(key) {
			return true
		}
	}
	return false
}

func (cc *CuckooChain) MarshalBinary() ([]byte, error) {
	n := len(cc.sealed)
	if cc.active != nil {
		n++
	}
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, uint32(n))
	appendOne := func(f *CuckooFilter) error {
		b, err := f.MarshalBinary()
		if err != nil {
			return err
		}
		var lh [4]byte
		binary.BigEndian.PutUint32(lh[:], uint32(len(b)))
		out = append(out, lh[:]...)
		out = append(out, b...)
		return nil
	}
	for _, f := range cc.sealed {
		if err := appendOne(f); err != nil {
			return nil, err
		}
	}
	if cc.active != nil {
		if err := appendOne(cc.active); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// UnmarshalBinary decodes a chain as query-only segments (active stays nil) —
// a decoded peer chain is never written to.
func (cc *CuckooChain) UnmarshalBinary(data []byte) error {
	if len(data) < 4 {
		return errors.New("cuckoo chain: short buffer")
	}
	count := binary.BigEndian.Uint32(data[:4])
	// Each segment costs at least its 4-byte length header, so a count that
	// couldn't fit in the buffer is corrupt — reject it before it drives a
	// giant pre-allocation (the input may come from a peer or the cloud).
	if uint64(count) > uint64(len(data)-4)/4 {
		return errors.New("cuckoo chain: segment count exceeds buffer")
	}
	off := 4
	filters := make([]*CuckooFilter, 0, count)
	for range count {
		if off+4 > len(data) {
			return errors.New("cuckoo chain: truncated length")
		}
		l := int(binary.BigEndian.Uint32(data[off:]))
		off += 4
		if off+l > len(data) {
			return errors.New("cuckoo chain: truncated body")
		}
		var f CuckooFilter
		if err := f.UnmarshalBinary(data[off : off+l]); err != nil {
			return err
		}
		off += l
		filters = append(filters, &f)
	}
	cc.sealed = filters
	cc.active = nil
	return nil
}
