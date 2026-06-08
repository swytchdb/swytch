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

import "slices"

// EffectRef is a (nodeID, offset) pair identifying an effect globally.
type EffectRef = [2]uint64

// TipSet holds an immutable set of effect references representing concurrent branch tips for a key.
// Once created, a TipSet is never modified — all mutations produce a new TipSet (copy-on-write).
type TipSet struct {
	tips []EffectRef // deduplicated, immutable once created
}

// smallTipSetDedup is the size below which NewTipSet deduplicates with a linear
// scan instead of a map. Tip sets are almost always one or two tips, so the
// map allocation is pure overhead on the hot write/ingest path; an O(n²) scan
// over a handful of refs is cheaper than allocating and populating a map.
const smallTipSetDedup = 8

// NewTipSet creates a new TipSet from the given refs, deduplicating them.
func NewTipSet(refs ...EffectRef) *TipSet {
	if len(refs) == 0 {
		return &TipSet{}
	}
	deduped := make([]EffectRef, 0, len(refs))
	if len(refs) <= smallTipSetDedup {
		for _, r := range refs {
			if !slices.Contains(deduped, r) {
				deduped = append(deduped, r)
			}
		}
		return &TipSet{tips: deduped}
	}
	seen := make(map[EffectRef]struct{}, len(refs))
	for _, r := range refs {
		if _, ok := seen[r]; !ok {
			seen[r] = struct{}{}
			deduped = append(deduped, r)
		}
	}
	return &TipSet{tips: deduped}
}

// Tips returns the refs in this tip set.
func (ts *TipSet) Tips() []EffectRef {
	if ts == nil {
		return nil
	}
	return ts.tips
}

// Len returns the number of tips.
func (ts *TipSet) Len() int {
	if ts == nil {
		return 0
	}
	return len(ts.tips)
}

// Contains reports whether the tip set contains the given ref.
func (ts *TipSet) Contains(ref EffectRef) bool {
	if ts == nil {
		return false
	}
	return slices.Contains(ts.tips, ref)
}

// Single returns the single ref if this tip set has exactly one tip, plus a boolean.
func (ts *TipSet) Single() (EffectRef, bool) {
	if ts == nil || len(ts.tips) != 1 {
		return EffectRef{}, false
	}
	return ts.tips[0], true
}

// ContainsAll reports whether the tip set contains all of the given refs.
func (ts *TipSet) ContainsAll(refs []EffectRef) bool {
	if ts == nil {
		return len(refs) == 0
	}
	for _, r := range refs {
		if !ts.Contains(r) {
			return false
		}
	}
	return true
}

// ReleaseClaimFunc is a function that releases a claimed key.
type ReleaseClaimFunc func()
