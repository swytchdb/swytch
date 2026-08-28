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

package redis

import (
	"errors"
	"sort"
	"strings"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

// sortOptions holds parsed SORT command options
type sortOptions struct {
	byPattern   string   // BY pattern (empty = sort by element value)
	getPatterns []string // GET patterns (can have multiple)
	limit       bool     // Whether LIMIT was specified
	offset      int      // LIMIT offset
	count       int      // LIMIT count
	desc        bool     // DESC (default false = ASC)
	alpha       bool     // ALPHA (lexicographic sort)
	store       string   // STORE destination key (empty = return results)
	noSort      bool     // BY nosort - skip sorting
}

// sortItem represents an element being sorted with its computed weight
type sortItem struct {
	element   []byte  // Original element value
	weight    float64 // Numeric weight for sorting
	strWeight string  // String weight for ALPHA sorting
}

// parseSortOptions parses SORT command options from arguments
func parseSortOptions(args [][]byte) (*sortOptions, error) {
	opts := &sortOptions{
		count: -1, // -1 means no limit
	}

	i := 0
	for i < len(args) {
		opt := string(shared.ToUpper(args[i]))
		switch opt {
		case "BY":
			if i+1 >= len(args) {
				return nil, errors.New("ERR syntax error")
			}
			i++
			opts.byPattern = string(args[i])
			if strings.ToLower(opts.byPattern) == "nosort" {
				opts.noSort = true
				opts.byPattern = ""
			}
		case "GET":
			if i+1 >= len(args) {
				return nil, errors.New("ERR syntax error")
			}
			i++
			opts.getPatterns = append(opts.getPatterns, string(args[i]))
		case "LIMIT":
			if i+2 >= len(args) {
				return nil, errors.New("ERR syntax error")
			}
			offset, ok1 := shared.ParseInt64(args[i+1])
			count, ok2 := shared.ParseInt64(args[i+2])
			if !ok1 || !ok2 {
				return nil, errors.New("ERR value is not an integer or out of range")
			}
			opts.limit = true
			opts.offset = int(offset)
			opts.count = int(count)
			i += 2
		case "ASC":
			opts.desc = false
		case "DESC":
			opts.desc = true
		case "ALPHA":
			opts.alpha = true
		case "STORE":
			if i+1 >= len(args) {
				return nil, errors.New("ERR syntax error")
			}
			i++
			opts.store = string(args[i])
		default:
			return nil, errors.New("ERR syntax error")
		}
		i++
	}

	return opts, nil
}

// substitutePattern replaces * in pattern with element value
// Handles hash field syntax: pattern->field
// Returns: (key, field, isHash)
func substitutePattern(pattern string, element []byte) (key string, field string, isHash bool) {
	// Find * and replace with element
	before, after, ok := strings.Cut(pattern, "*")
	if !ok {
		// No substitution needed, just use pattern as-is
		key = pattern
	} else {
		key = before + string(element) + after
	}

	// Check for hash field syntax (->)
	if before, after, ok := strings.Cut(key, "->"); ok {
		return before, after, true
	}

	return key, "", false
}

// handleSortRO handles the SORT_RO command, rejecting STORE.
func (h *Handler) handleSortRO(cmd *shared.Command, w *shared.Writer, db *shared.Database) {
	for i := 1; i < len(cmd.Args); i++ {
		if strings.EqualFold(string(cmd.Args[i]), "STORE") {
			w.WriteError("ERR syntax error")
			return
		}
	}
	h.handleSort(cmd, w, db)
}

// handleSort handles the SORT command for lists, sets, and sorted sets
func (h *Handler) handleSort(cmd *shared.Command, w *shared.Writer, db *shared.Database) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("sort")
		return
	}

	key := string(cmd.Args[0])

	// Parse options
	opts, err := parseSortOptions(cmd.Args[1:])
	if err != nil {
		w.WriteError(err.Error())
		return
	}

	// Get source snapshot from effects engine
	snap, _, err := cmd.Context.GetSnapshot(key)
	if err != nil {
		w.WriteError(err.Error())
		return
	}
	if snap == nil || snap.Op == pb.EffectOp_REMOVE_OP {
		// Key doesn't exist - return empty result
		if opts.store != "" {
			// Delete destination if it exists, return 0
			if err := h.emitSortDelete(cmd, opts.store); err != nil {
				w.WriteError(err.Error())
				return
			}
			w.WriteInteger(0)
		} else {
			w.WriteArray(0)
		}
		return
	}

	// Extract elements based on collection type
	var elements [][]byte
	switch snap.Collection {
	case pb.CollectionKind_ORDERED:
		// Lists
		for _, elem := range snap.OrderedElements {
			elements = append(elements, elem.Data.Decompress())
		}
	case pb.CollectionKind_KEYED:
		// Sets and sorted sets
		for _, elem := range snap.NetAdds {
			elements = append(elements, elem.Data.Decompress())
		}
	default:
		w.WriteWrongType()
		return
	}

	// Build sort items with weights
	items, err := h.buildSortItems(cmd.Context, db, elements, opts)
	if err != nil {
		w.WriteError(err.Error())
		return
	}

	// Sort unless BY nosort
	if !opts.noSort {
		sortItems(items, opts)
	}

	// Apply LIMIT
	if opts.limit {
		items = applyLimit(items, opts.offset, opts.count)
	}

	// Build results (applies GET patterns if any)
	results, err := h.buildResults(cmd.Context, db, items, opts)
	if err != nil {
		w.WriteError(err.Error())
		return
	}

	// STORE or return
	if opts.store != "" {
		if err := h.storeResults(cmd, opts.store, results); err != nil {
			w.WriteError(err.Error())
			return
		}
		w.WriteInteger(int64(len(results)))
	} else {
		w.WriteArray(len(results))
		for _, r := range results {
			if r == nil {
				w.WriteNullBulkString()
			} else {
				w.WriteBulkString(r)
			}
		}
	}
}

// getExternalValue retrieves a value from an external key, optionally a hash field.
// Reads through the command's Context so filterSnapshot drops TTL-expired
// entries and empty-collection tombstones — direct Engine.GetSnapshot would
// return stale data here, since BY/GET patterns commonly hit ephemeral hash
// fields whose parent key may already be past its TTL.
func (h *Handler) getExternalValue(ectx *effects.Context, db *shared.Database, pattern string, element []byte) ([]byte, bool, error) {
	key, field, isHash := substitutePattern(pattern, element)

	if h.engine == nil || ectx == nil {
		return nil, false, nil
	}
	snap, _, err := ectx.GetSnapshot(key)
	if err != nil {
		return nil, false, err
	}
	if snap == nil {
		return nil, false, nil
	}

	if isHash {
		if snap.Collection != pb.CollectionKind_KEYED {
			return nil, false, nil
		}
		elem, ok := snap.NetAdds[field]
		if !ok {
			return nil, false, nil
		}
		return elem.Data.Decompress(), true, nil
	}

	if snap.Scalar == nil {
		return nil, false, nil
	}
	return snap.Scalar.Decompress(), true, nil
}

// buildSortItems creates sortItems from elements with computed weights
func (h *Handler) buildSortItems(ectx *effects.Context, db *shared.Database, elements [][]byte, opts *sortOptions) ([]sortItem, error) {
	items := make([]sortItem, 0, len(elements))

	for _, elem := range elements {
		item := sortItem{element: elem}

		if opts.byPattern != "" && !opts.noSort {
			// Get weight from external key
			weightData, ok, err := h.getExternalValue(ectx, db, opts.byPattern, elem)
			if err != nil {
				return nil, err
			}
			if ok {
				if opts.alpha {
					item.strWeight = string(weightData)
				} else {
					if w, ok := shared.ParseFloat64(weightData); ok {
						item.weight = w
					}
					// If parsing fails, weight stays 0
				}
			}
			// If key not found, weight stays 0 (or empty string for alpha)
		} else if !opts.noSort {
			// Sort by element value itself
			if opts.alpha {
				item.strWeight = string(elem)
			} else {
				if w, ok := shared.ParseFloat64(elem); ok {
					item.weight = w
				}
				// If parsing fails, weight stays 0
			}
		}

		items = append(items, item)
	}

	return items, nil
}

// sortItems sorts the items based on options
func sortItems(items []sortItem, opts *sortOptions) {
	sort.SliceStable(items, func(i, j int) bool {
		var less bool
		if opts.alpha {
			less = items[i].strWeight < items[j].strWeight
		} else {
			less = items[i].weight < items[j].weight
		}
		if opts.desc {
			return !less
		}
		return less
	})
}

// applyLimit applies LIMIT offset count to items
func applyLimit(items []sortItem, offset, count int) []sortItem {
	if offset >= len(items) {
		return nil
	}
	items = items[offset:]
	if count >= 0 && count < len(items) {
		items = items[:count]
	}
	return items
}

// buildResults builds the result slice, applying GET patterns if specified
func (h *Handler) buildResults(ectx *effects.Context, db *shared.Database, items []sortItem, opts *sortOptions) ([][]byte, error) {
	if len(opts.getPatterns) == 0 {
		// No GET patterns - return elements
		results := make([][]byte, len(items))
		for i, item := range items {
			results[i] = item.element
		}
		return results, nil
	}

	// With GET patterns - each element produces len(getPatterns) results
	results := make([][]byte, 0, len(items)*len(opts.getPatterns))
	for _, item := range items {
		for _, pattern := range opts.getPatterns {
			if pattern == "#" {
				// Special pattern # returns the element itself
				results = append(results, item.element)
			} else {
				val, _, err := h.getExternalValue(ectx, db, pattern, item.element)
				if err != nil {
					return nil, err
				}
				results = append(results, val) // nil if not found
			}
		}
	}
	return results, nil
}

// emitSortDelete emits a delete effect for a key, reading current tips for proper dep chaining.
func (h *Handler) emitSortDelete(cmd *shared.Command, key string) error {
	_, tips, err := cmd.Context.GetSnapshot(key)
	if err != nil {
		return err
	}
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_SCALAR,
		}},
	}, tips)
}

// storeResults stores the results as a list at the destination key via the effects engine.
func (h *Handler) storeResults(cmd *shared.Command, destKey string, results [][]byte) error {
	// Read current tips for the destination key so the first effect depends on them
	_, tips, err := cmd.Context.GetSnapshot(destKey)
	if err != nil {
		return err
	}

	if len(results) == 0 {
		return cmd.Context.Emit(&pb.Effect{
			Key: []byte(destKey),
			Kind: &pb.Effect_Data{Data: &pb.DataEffect{
				Op:         pb.EffectOp_REMOVE_OP,
				Collection: pb.CollectionKind_SCALAR,
			}},
		}, tips)
	}

	// Delete existing key first
	if err := cmd.Context.Emit(&pb.Effect{
		Key: []byte(destKey),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_SCALAR,
		}},
	}, tips); err != nil {
		return err
	}

	// Insert each result element as a list entry
	for _, r := range results {
		value := r
		if value == nil {
			value = []byte{}
		}
		hlc := shared.NextHLC()
		id := shared.EncodeElementID(hlc, shared.EffectsNodeID)
		if err := cmd.Context.Emit(&pb.Effect{
			Key: []byte(destKey),
			Kind: &pb.Effect_Data{Data: &pb.DataEffect{
				Op:         pb.EffectOp_INSERT_OP,
				Merge:      pb.MergeRule_LAST_WRITE_WINS,
				Collection: pb.CollectionKind_ORDERED,
				Placement:  pb.Placement_PLACE_TAIL,
				Id:         id,
				Value:      &pb.DataEffect_Raw{Raw: value},
			}},
		}); err != nil {
			return err
		}
	}

	// Emit list type tag
	return cmd.Context.Emit(&pb.Effect{
		Key:  []byte(destKey),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_LIST}},
	})
}

// registerSortCommands registers SORT and SORT_RO commands.
func (h *Handler) registerSortCommands() {
	r := &h.cmds
	r.Register(shared.CmdSort, &shared.CommandEntry{
		ConnHandler: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, _ *shared.Connection) {
			h.handleSort(cmd, w, db)
		},
		TxnPrepare: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, _ *shared.Connection) (bool, []string, shared.CommandRunner) {
			return true, nil, func() { h.handleSort(cmd, w, db) }
		},
		Keys:  shared.KeysSortCmd,
		Flags: shared.FlagWrite,
	})
	r.Register(shared.CmdSortRO, &shared.CommandEntry{
		ConnHandler: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, _ *shared.Connection) {
			h.handleSortRO(cmd, w, db)
		},
		TxnPrepare: func(cmd *shared.Command, w *shared.Writer, db *shared.Database, _ *shared.Connection) (bool, []string, shared.CommandRunner) {
			return true, nil, func() { h.handleSortRO(cmd, w, db) }
		},
		Keys: shared.KeysFirst,
	})
}
