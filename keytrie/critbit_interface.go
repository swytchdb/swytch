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

// KeyIndex is the common interface for key indexing implementations.
type KeyIndex interface {
	// Insert attempts to store a TipSet for the given key using CAS.
	// old is the expected current TipSet (nil for new keys).
	// On success: returns (nil, true).
	// On CAS failure: returns (currentTips, false).
	Insert(key string, old *TipSet, new *TipSet) (*TipSet, bool)

	// Delete removes a key only if its current tips match old (CAS).
	// Returns true if the key was deleted.
	Delete(key string, old *TipSet) bool

	// DeleteAndSnapshot removes a key only if its current tips match old
	// (CAS), returning the previous tip set on success. Returns nil if
	// the key did not exist, was already deleted, or tips changed since
	// old was read.
	DeleteAndSnapshot(key string, old *TipSet) *TipSet

	// Contains checks if a key exists and returns its TipSet.
	// Returns nil if key doesn't exist.
	Contains(key string) *TipSet

	// RemoveTips atomically removes the given refs from a key's TipSet.
	// Safe to call concurrently — uses CAS internally. No-op if the key
	// doesn't exist or the refs aren't present.
	RemoveTips(key string, refs []EffectRef)

	// Size returns the number of keys.
	Size() int64

	// Range iterates over all keys in lexicographic order.
	Range(fn func(key string) bool)

	// RangeFrom iterates over keys in lexicographic order starting after `after`.
	// If after is empty, iterates from the beginning (same as Range).
	RangeFrom(after string, fn func(key string) bool)

	// RangePrefix iterates over all keys with the given prefix.
	RangePrefix(prefix string, fn func(key string) bool)

	// Keys returns all keys.
	Keys() []string

	// MatchPattern returns all keys matching a Redis-style glob pattern.
	MatchPattern(pattern string) []string

	// FirstWithPrefix finds the lexicographically smallest key with the given prefix.
	FirstWithPrefix(prefix string, claim bool) (string, bool, ReleaseClaimFunc)

	// LastWithPrefix finds the lexicographically largest key with the given prefix.
	LastWithPrefix(prefix string, claim bool) (string, bool, ReleaseClaimFunc)

	// NextWithPrefix finds the next key after 'after' with the given prefix.
	NextWithPrefix(prefix, after string, claim bool) (string, bool, ReleaseClaimFunc)

	// PrevWithPrefix finds the previous key before 'before' with the given prefix.
	PrevWithPrefix(prefix, before string, claim bool) (string, bool, ReleaseClaimFunc)

	// TryClaimKey attempts to claim a key for exclusive access.
	TryClaimKey(key string) (exists bool, release ReleaseClaimFunc)

	// GetHeadHint returns the head key hint for a prefix.
	GetHeadHint(prefix string) string

	// SetHeadHint sets the head key hint for a prefix.
	SetHeadHint(prefix string, key string)

	// GetTailHint returns the tail key hint for a prefix.
	GetTailHint(prefix string) string

	// SetTailHint sets the tail key hint for a prefix.
	SetTailHint(prefix string, key string)

	// Snapshot returns a point-in-time copy of the index as a new KeyIndex.
	// TipSets are immutable, so only pointers are copied. Preserves all
	// critbit properties (prefix ranges, ordered iteration).
	Snapshot() KeyIndex

	// Close releases any resources held by the index.
	Close() error
}

// New creates a new critbit tree.
func New() KeyIndex {
	return NewCritbit()
}

// Ensure implementations satisfy the interface.
var _ KeyIndex = (*Critbit)(nil)
