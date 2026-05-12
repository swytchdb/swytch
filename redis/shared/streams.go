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
	"encoding/binary"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/swytchdb/swytch/keytrie"
)

// checkUint64AddOverflow checks if a + b would overflow uint64.
// Returns true if overflow would occur.
func checkUint64AddOverflow(a, b uint64) bool {
	return a > math.MaxUint64-b
}

// StreamID represents a Redis stream entry ID (milliseconds-sequence)
type StreamID struct {
	Ms  uint64 // Unix timestamp in milliseconds
	Seq uint64 // Sequence number within the millisecond
}

var (
	// MinStreamID is the minimum possible stream ID
	MinStreamID = StreamID{Ms: 0, Seq: 0}
	// MaxStreamID is the maximum possible stream ID
	MaxStreamID = StreamID{Ms: ^uint64(0), Seq: ^uint64(0)}

	ErrInvalidStreamID  = errors.New("invalid stream ID specified as stream command argument")
	ErrStreamIDTooSmall = errors.New("the ID specified in XADD is equal or smaller than the target stream top item")
)

// ParseStreamID parses a stream ID from a string.
// Supported formats:
//   - "*" - auto-generate (only for XADD)
//   - "$" - last entry ID (for XREAD)
//   - ">" - only new entries (for XREADGROUP)
//   - "-" - minimum ID (for XRANGE)
//   - "+" - maximum ID (for XRANGE)
//   - "0" or "0-0" - minimum ID
//   - "1234567890123-0" - full ID
//   - "1234567890123" - partial ID (seq defaults to 0 for start, max for end)
//   - "(1234567890123-0" - exclusive (for XRANGE)
func ParseStreamID(s string) (id StreamID, exclusive bool, isSpecial bool, err error) {
	if len(s) == 0 {
		return StreamID{}, false, false, ErrInvalidStreamID
	}

	// Check for exclusive prefix
	if s[0] == '(' {
		exclusive = true
		s = s[1:]
		if len(s) == 0 {
			return StreamID{}, false, false, ErrInvalidStreamID
		}
	}

	// Handle special IDs
	switch s {
	case "*":
		return StreamID{}, false, true, nil
	case "$":
		return StreamID{}, false, true, nil
	case ">":
		return StreamID{}, false, true, nil
	case "-":
		return MinStreamID, exclusive, true, nil
	case "+":
		return MaxStreamID, exclusive, true, nil
	}

	// Parse ms-seq format
	parts := strings.SplitN(s, "-", 2)
	ms, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return StreamID{}, false, false, ErrInvalidStreamID
	}

	var seq uint64
	if len(parts) == 2 {
		if parts[1] == "*" {
			// Hybrid ID like "1234567890123-*" - auto-generate sequence
			return StreamID{Ms: ms, Seq: 0}, false, true, nil
		}
		seq, err = strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return StreamID{}, false, false, ErrInvalidStreamID
		}
	}
	// If no sequence provided, it's a partial ID (seq=0 for now, caller may adjust)

	return StreamID{Ms: ms, Seq: seq}, exclusive, false, nil
}

// ParseStreamIDForRange parses an ID for XRANGE/XREVRANGE, handling partial IDs appropriately.
// For start IDs, partial IDs get seq=0. For end IDs, partial IDs get seq=MaxUint64.
func ParseStreamIDForRange(s string, isEnd bool) (id StreamID, exclusive bool, err error) {
	if len(s) == 0 {
		return StreamID{}, false, ErrInvalidStreamID
	}

	// Check for exclusive prefix
	if s[0] == '(' {
		exclusive = true
		s = s[1:]
		if len(s) == 0 {
			return StreamID{}, false, ErrInvalidStreamID
		}
	}

	// Handle special IDs
	switch s {
	case "-":
		// Exclusive with - is not allowed
		if exclusive {
			return StreamID{}, false, ErrInvalidStreamID
		}
		return MinStreamID, exclusive, nil
	case "+":
		// Exclusive with + is not allowed
		if exclusive {
			return StreamID{}, false, ErrInvalidStreamID
		}
		return MaxStreamID, exclusive, nil
	}

	// Parse ms-seq format
	parts := strings.SplitN(s, "-", 2)
	ms, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return StreamID{}, false, ErrInvalidStreamID
	}

	var seq uint64
	if len(parts) == 2 {
		seq, err = strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return StreamID{}, false, ErrInvalidStreamID
		}
	} else {
		// Partial ID - set seq based on whether it's start or end
		if isEnd {
			seq = ^uint64(0)
		} else {
			seq = 0
		}
	}

	// Exclusive with 0-0 is not allowed - 0-0 is the minimum possible ID
	// There's nothing before 0-0, so exclusive is meaningless in both positions
	if exclusive && ms == 0 && seq == 0 {
		return StreamID{}, false, ErrInvalidStreamID
	}

	// Exclusive with max ID as START is not allowed - nothing after it
	// (max as END is valid - means exclude max)
	if exclusive && !isEnd && ms == ^uint64(0) && seq == ^uint64(0) {
		return StreamID{}, false, ErrInvalidStreamID
	}

	return StreamID{Ms: ms, Seq: seq}, exclusive, nil
}

// String returns the string representation of the stream ID
func (id StreamID) String() string {
	return strconv.FormatUint(id.Ms, 10) + "-" + strconv.FormatUint(id.Seq, 10)
}

// Key returns a 16-byte big-endian encoding suitable for lexicographic ordering.
// Big-endian ensures that numeric order matches byte order.
func (id StreamID) Key() string {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], id.Ms)
	binary.BigEndian.PutUint64(buf[8:16], id.Seq)
	return string(buf[:])
}

// Compare compares two stream IDs.
// Returns -1 if id < other, 0 if id == other, 1 if id > other.
func (id StreamID) Compare(other StreamID) int {
	if id.Ms < other.Ms {
		return -1
	}
	if id.Ms > other.Ms {
		return 1
	}
	if id.Seq < other.Seq {
		return -1
	}
	if id.Seq > other.Seq {
		return 1
	}
	return 0
}

// IsZero returns true if the stream ID is 0-0
func (id StreamID) IsZero() bool {
	return id.Ms == 0 && id.Seq == 0
}

// StreamEntry represents a single entry in a stream
type StreamEntry struct {
	ID     StreamID
	Fields [][]byte // alternating [key1, val1, key2, val2, ...]
}

// streamEntryOverhead is the memory overhead per stream entry (map entry + StreamEntry struct + ID key)
const streamEntryOverhead = 64

// StreamValue holds all entries and consumer groups for a stream
type StreamValue struct {
	// Entry storage - trie for ordered iteration, map for O(1) lookup
	Index   keytrie.KeyIndex        // ordered index of ID strings (rebuilt on deserialize)
	Entries map[string]*StreamEntry // ID string -> entry data (serialized)

	// Stream metadata
	LastID       StreamID // highest ID ever generated (for auto-ID)
	FirstID      StreamID // first entry ID (updated on trim)
	EntriesAdded int64    // total entries ever added (for XINFO)
	MaxDeletedID StreamID // highest deleted ID (for XSETID)

	// Consumer groups
	Groups map[string]*ConsumerGroup

	// cachedSize tracks the total byte size of all entries for O(1) Size() calls
	// Updated incrementally on add/delete/trim operations to avoid O(n) iteration
	cachedSize int64

	// cachedSerializedSize tracks the serialized byte size for O(1) encodeStreamSize() calls
	// Updated incrementally on add/delete/trim operations to avoid O(n) iteration
	cachedSerializedSize int64
}

// ConsumerGroup represents a consumer group with TCP-style ack tracking
type ConsumerGroup struct {
	Name            string
	LastDeliveredID StreamID // for ">" - next delivery starts after this ID
	EntriesRead     int64    // for lag calculation (XINFO)

	// Pending Entries List (PEL)
	// Memory bounded by gap size, not total volume
	Pending map[StreamID]*PendingEntry

	// Consumers
	Consumers map[string]*Consumer

	// Per-consumer pending limit (0 = unlimited)
	MaxPending int64

	CreatedAt int64 // Unix milliseconds
}

// Consumer represents a consumer within a group
type Consumer struct {
	Name         string
	SeenTime     int64 // last interaction time (Unix ms)
	PendingCount int64 // entries owned by this consumer
	ActiveTime   int64 // last message fetched time
}

// PendingEntry tracks delivery information for Redis compatibility
type PendingEntry struct {
	ID            StreamID
	Consumer      string // owning consumer name
	DeliveryTime  int64  // Unix ms of last delivery
	DeliveryCount int64  // for retry logic / dead-letter detection
}

// NewStreamValue creates a new empty stream
func NewStreamValue() *StreamValue {
	return &StreamValue{
		Index:   keytrie.New(),
		Entries: make(map[string]*StreamEntry),
		Groups:  make(map[string]*ConsumerGroup),
	}
}

// NewConsumerGroup creates a new consumer group
func NewConsumerGroup(name string, lastDeliveredID StreamID) *ConsumerGroup {
	return &ConsumerGroup{
		Name:            name,
		LastDeliveredID: lastDeliveredID,
		EntriesRead:     -1, // -1 means unknown/not yet read
		Pending:         make(map[StreamID]*PendingEntry),
		Consumers:       make(map[string]*Consumer),
		CreatedAt:       time.Now().UnixMilli(),
	}
}

// entrySize calculates the memory size of a stream entry
func entrySize(entry *StreamEntry) int64 {
	size := int64(streamEntryOverhead)
	for _, field := range entry.Fields {
		size += int64(len(field) + 24) // slice header overhead
	}
	return size
}

// entrySerializedSize calculates the serialized size of a stream entry
// This matches the format used in encodeStream: id string len + id + StreamID + fields
func entrySerializedSize(idKey string, entry *StreamEntry) int64 {
	size := int64(4 + len(idKey)) // id string len + id
	size += 16                    // StreamID (Ms + Seq)
	size += 4                     // fields count
	for _, field := range entry.Fields {
		size += int64(4 + len(field)) // field len + field
	}
	return size
}

// ForEach iterates over all entries in order, calling fn for each.
// If fn returns false, iteration stops.
func (s *StreamValue) ForEach(fn func(entry *StreamEntry) bool) {
	if s.Index.Size() == 0 {
		return
	}
	currentKey, found, _ := s.Index.FirstWithPrefix("", false)
	for found {
		if entry, ok := s.Entries[currentKey]; ok {
			if !fn(entry) {
				return
			}
		}
		currentKey, found, _ = s.Index.NextWithPrefix("", currentKey, false)
	}
}

// RefMode specifies how to handle consumer group references during trimming
type RefMode int

const (
	RefModeKeepRef RefMode = iota // Default: keep PEL references for trimmed entries
	RefModeDelRef                 // Delete PEL references when trimming
	RefModeAcked                  // Only trim entries acknowledged by all groups
)

// TrimOptions specifies trimming parameters
type TrimOptions struct {
	MaxLen           int64     // Trim to this length (only used if HasMaxLen is true)
	MinID            *StreamID // Trim entries with ID < MinID (nil = no limit)
	Approx           bool      // Use approximate trimming (~)
	Limit            int64     // Max entries to trim per operation (0 = unlimited)
	NodeSize         int64     // Node size for approximate trimming (default 100)
	RefMode          RefMode   // How to handle consumer group references
	HasMaxLen        bool      // Whether MAXLEN was specified (allows MaxLen=0 to mean "trim to 0")
	HasExplicitLimit bool      // Whether LIMIT was explicitly specified (affects NodeSize rounding)
}

// Trim trims the stream according to the options and returns the count of trimmed entries
func (s *StreamValue) Trim(opts TrimOptions) int64 {
	if s.Index.Size() == 0 {
		return 0
	}

	// Determine effective limit
	// - For approximate trimming without explicit limit: use 100 * NodeSize
	// - For approximate trimming with explicit limit: use the explicit limit
	// - For exact trimming: no limit (use full size)
	limit := opts.Limit
	if limit == 0 {
		if opts.Approx && opts.NodeSize > 0 {
			limit = 100 * opts.NodeSize // Implicit limit for approximate trimming
		} else {
			limit = int64(s.Index.Size()) // No limit for exact trimming
		}
	}

	// Helper to check if an entry can be trimmed based on RefMode
	canTrim := func(id StreamID) bool {
		if opts.RefMode == RefModeAcked && len(s.Groups) > 0 {
			for _, group := range s.Groups {
				// Entry not yet delivered to this group - can't trim with ACKED
				if id.Compare(group.LastDeliveredID) > 0 {
					return false
				}
				// Entry is still pending (not acknowledged) - can't trim with ACKED
				if _, pending := group.Pending[id]; pending {
					return false
				}
			}
		}
		return true
	}

	// Helper to handle RefMode when trimming
	// Note: Trim does NOT update MaxDeletedID - only XDEL does
	handleTrim := func(id StreamID) {
		if opts.RefMode == RefModeDelRef {
			for _, group := range s.Groups {
				if pe, exists := group.Pending[id]; exists {
					if c := group.Consumers[pe.Consumer]; c != nil {
						c.PendingCount--
					}
					delete(group.Pending, id)
				}
			}
		}
	}

	// Collect keys to trim (we can't delete while iterating)
	keysToTrim := make([]string, 0)
	trimCount := int64(0)

	// Trim by MINID first
	if opts.MinID != nil {
		minIDKey := opts.MinID.Key()
		currentKey, found, _ := s.Index.FirstWithPrefix("", false)
		for found && trimCount < limit {
			// Using lexicographic comparison on big-endian keys
			if currentKey >= minIDKey {
				break
			}
			entry := s.Entries[currentKey]
			if entry == nil {
				currentKey, found, _ = s.Index.NextWithPrefix("", currentKey, false)
				continue
			}
			// For ACKED mode, skip entries that can't be trimmed instead of breaking
			if !canTrim(entry.ID) {
				currentKey, found, _ = s.Index.NextWithPrefix("", currentKey, false)
				continue
			}
			handleTrim(entry.ID)
			keysToTrim = append(keysToTrim, currentKey)
			trimCount++
			currentKey, found, _ = s.Index.NextWithPrefix("", currentKey, false)
		}
	}

	// Trim by MAXLEN (only if MAXLEN was specified)
	if opts.HasMaxLen {
		effectiveMaxLen := opts.MaxLen
		if opts.Approx && opts.NodeSize > 0 {
			// Round UP to nearest node boundary
			effectiveMaxLen = ((opts.MaxLen + opts.NodeSize - 1) / opts.NodeSize) * opts.NodeSize
		}

		currentLen := int64(s.Index.Size()) - trimCount
		currentKey, found, _ := s.Index.FirstWithPrefix("", false)

		// Skip entries we already marked for trimming
		for i := int64(0); i < trimCount && found; i++ {
			currentKey, found, _ = s.Index.NextWithPrefix("", currentKey, false)
		}

		// Calculate how many to trim
		toTrim := currentLen - effectiveMaxLen
		if toTrim > 0 {
			// Apply limit
			if toTrim > limit-trimCount {
				toTrim = limit - trimCount
			}

			// For approximate trimming with explicit LIMIT, round down to NodeSize
			// This ensures we only trim whole "nodes" when LIMIT is specified
			if opts.Approx && opts.HasExplicitLimit && opts.NodeSize > 0 {
				toTrim = (toTrim / opts.NodeSize) * opts.NodeSize
			}
		}

		trimmedHere := int64(0)
		for found && trimmedHere < toTrim {
			entry := s.Entries[currentKey]
			if entry == nil {
				currentKey, found, _ = s.Index.NextWithPrefix("", currentKey, false)
				continue
			}
			// For ACKED mode, skip entries that can't be trimmed instead of breaking
			if !canTrim(entry.ID) {
				currentKey, found, _ = s.Index.NextWithPrefix("", currentKey, false)
				continue
			}
			handleTrim(entry.ID)
			keysToTrim = append(keysToTrim, currentKey)
			trimCount++
			trimmedHere++
			currentKey, found, _ = s.Index.NextWithPrefix("", currentKey, false)
		}
	}

	if len(keysToTrim) == 0 {
		return 0
	}

	// Remove trimmed entries and update cached sizes
	for _, key := range keysToTrim {
		if entry, exists := s.Entries[key]; exists {
			s.cachedSize -= entrySize(entry)
			s.cachedSerializedSize -= entrySerializedSize(key, entry)
		}
		s.Index.Delete(key)
		delete(s.Entries, key)
	}

	// Update FirstID
	if s.Index.Size() > 0 {
		if firstKey, found, _ := s.Index.FirstWithPrefix("", false); found {
			if entry, ok := s.Entries[firstKey]; ok {
				s.FirstID = entry.ID
			}
		}
	} else {
		s.FirstID = StreamID{}
	}

	return int64(len(keysToTrim))
}

// GetOrCreateConsumer gets or creates a consumer in the group
func (g *ConsumerGroup) GetOrCreateConsumer(name string) *Consumer {
	if c, exists := g.Consumers[name]; exists {
		c.SeenTime = time.Now().UnixMilli()
		return c
	}

	c := &Consumer{
		Name:     name,
		SeenTime: time.Now().UnixMilli(),
	}
	g.Consumers[name] = c
	return c
}
