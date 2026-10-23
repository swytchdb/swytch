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

import "sort"

// ZSetValue stores sorted set members with scores
type ZSetValue struct {
	Members map[string]float64 // member -> score
}

// ZSetEntry represents a member-score pair for sorted results
type ZSetEntry struct {
	Member string
	Score  float64
}

func (z *ZSetValue) ZAdd(members map[string]float64) int {
	added := 0

	for member, score := range members {
		if _, exists := z.Members[member]; !exists {
			added++
		}

		z.Members[member] = score
	}

	return added
}

// ZAddWithOptions adds/updates members with ZADD options
// Returns (added, changed) counts based on options
// Options: nx (only add new), xx (only update), gt (update if greater), lt (update if less), ch (count changed)
func (z *ZSetValue) ZAddWithOptions(member string, score float64, nx, xx, gt, lt bool) (added, changed bool) {
	existing, exists := z.Members[member]

	if nx && exists {
		return false, false // NX: only add if not exists
	}

	if xx && !exists {
		return false, false // XX: only update if exists
	}

	if exists {
		// Check GT/LT conditions
		if gt && score <= existing {
			return false, false
		}

		if lt && score >= existing {
			return false, false
		}

		if score != existing {
			z.Members[member] = score

			return false, true
		}

		return false, false
	}

	z.Members[member] = score

	return true, true
}

// ZRem removes members from the sorted set
// Returns the number of members removed
func (z *ZSetValue) ZRem(members ...string) int {
	removed := 0

	for _, member := range members {
		if _, exists := z.Members[member]; exists {
			delete(z.Members, member)

			removed++
		}
	}

	return removed
}

// ZScore returns the score of a member
func (z *ZSetValue) ZScore(member string) (float64, bool) {
	score, exists := z.Members[member]

	return score, exists
}

// ZCard returns the number of members in the sorted set
func (z *ZSetValue) ZCard() int {
	return len(z.Members)
}

// ZIncrBy increments the score of a member by delta
// Returns the new score
func (z *ZSetValue) ZIncrBy(member string, delta float64) float64 {
	z.Members[member] += delta

	return z.Members[member]
}

// Sorted returns all members sorted by score (ascending), then by member name
func (z *ZSetValue) Sorted() []ZSetEntry {
	entries := make([]ZSetEntry, 0, len(z.Members))
	for member, score := range z.Members {
		entries = append(entries, ZSetEntry{Member: member, Score: score})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Score != entries[j].Score {
			return entries[i].Score < entries[j].Score
		}

		return entries[i].Member < entries[j].Member
	})

	return entries
}

// SortedReverse returns all members sorted by score (descending), then by member name (descending)
func (z *ZSetValue) SortedReverse() []ZSetEntry {
	entries := make([]ZSetEntry, 0, len(z.Members))
	for member, score := range z.Members {
		entries = append(entries, ZSetEntry{Member: member, Score: score})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Score != entries[j].Score {
			return entries[i].Score > entries[j].Score
		}

		return entries[i].Member > entries[j].Member
	})

	return entries
}

// ZRank returns the rank (0-indexed) of a member in ascending order
// Returns -1 if member not found
func (z *ZSetValue) ZRank(member string) int {
	if _, exists := z.Members[member]; !exists {
		return -1
	}

	sorted := z.Sorted()
	for i, entry := range sorted {
		if entry.Member == member {
			return i
		}
	}

	return -1
}

// ZRevRank returns the rank (0-indexed) of a member in descending order
// Returns -1 if member not found
func (z *ZSetValue) ZRevRank(member string) int {
	if _, exists := z.Members[member]; !exists {
		return -1
	}

	sorted := z.SortedReverse()
	for i, entry := range sorted {
		if entry.Member == member {
			return i
		}
	}

	return -1
}

// ZCount counts members with scores between minScore and maxScore, respecting exclusive bounds.
func (z *ZSetValue) ZCount(minScore, maxScore float64, minExclusive, maxExclusive bool) int {
	count := 0

	for _, score := range z.Members {
		if minExclusive {
			if score <= minScore {
				continue
			}
		} else {
			if score < minScore {
				continue
			}
		}

		if maxExclusive {
			if score >= maxScore {
				continue
			}
		} else {
			if score > maxScore {
				continue
			}
		}

		count++
	}

	return count
}

// ZRange returns members in the given rank range (0-indexed, inclusive)
func (z *ZSetValue) ZRange(start, stop int, reverse bool) []ZSetEntry {
	var sorted []ZSetEntry
	if reverse {
		sorted = z.SortedReverse()
	} else {
		sorted = z.Sorted()
	}

	length := len(sorted)
	if length == 0 {
		return nil
	}

	// Handle negative indices
	if start < 0 {
		start = length + start
	}

	if stop < 0 {
		stop = length + stop
	}

	// Clamp to valid range
	if start < 0 {
		start = 0
	}

	if stop >= length {
		stop = length - 1
	}

	if start > stop || start >= length {
		return nil
	}

	return sorted[start : stop+1]
}

// ZRangeByScore returns members with scores in the given range
func (z *ZSetValue) ZRangeByScore(minScore, maxScore float64, minExclusive, maxExclusive, reverse bool, offset, count int) []ZSetEntry {
	var sorted []ZSetEntry
	if reverse {
		sorted = z.SortedReverse()
	} else {
		sorted = z.Sorted()
	}

	result := make([]ZSetEntry, 0)

	for _, entry := range sorted {
		inRange := true

		// Check min bound
		if minExclusive {
			if entry.Score <= minScore {
				inRange = false
			}
		} else {
			if entry.Score < minScore {
				inRange = false
			}
		}

		// Check max bound
		if maxExclusive {
			if entry.Score >= maxScore {
				inRange = false
			}
		} else {
			if entry.Score > maxScore {
				inRange = false
			}
		}

		if inRange {
			result = append(result, entry)
		}
	}

	// Apply offset and count
	// Negative offset returns empty result
	if offset < 0 {
		return nil
	}

	if offset > 0 {
		if offset >= len(result) {
			return nil
		}

		result = result[offset:]
	}

	if count >= 0 && count < len(result) {
		result = result[:count]
	}

	return result
}

// ZPopMin removes and returns the member with the lowest score
func (z *ZSetValue) ZPopMin() (ZSetEntry, bool) {
	if len(z.Members) == 0 {
		return ZSetEntry{}, false
	}

	sorted := z.Sorted()
	entry := sorted[0]
	delete(z.Members, entry.Member)

	return entry, true
}

// ZPopMax removes and returns the member with the highest score
func (z *ZSetValue) ZPopMax() (ZSetEntry, bool) {
	if len(z.Members) == 0 {
		return ZSetEntry{}, false
	}

	sorted := z.SortedReverse()
	entry := sorted[0]
	delete(z.Members, entry.Member)

	return entry, true
}

// ZPopMinN removes and returns up to n members with the lowest scores
func (z *ZSetValue) ZPopMinN(n int) []ZSetEntry {
	if len(z.Members) == 0 || n <= 0 {
		return nil
	}

	sorted := z.Sorted()
	if n > len(sorted) {
		n = len(sorted)
	}

	result := sorted[:n]
	for _, entry := range result {
		delete(z.Members, entry.Member)
	}

	return result
}

// ZPopMaxN removes and returns up to n members with the highest scores
func (z *ZSetValue) ZPopMaxN(n int) []ZSetEntry {
	if len(z.Members) == 0 || n <= 0 {
		return nil
	}

	sorted := z.SortedReverse()
	if n > len(sorted) {
		n = len(sorted)
	}

	result := sorted[:n]
	for _, entry := range result {
		delete(z.Members, entry.Member)
	}

	return result
}
