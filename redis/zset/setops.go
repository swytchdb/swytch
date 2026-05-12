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

package zset

import (
	"fmt"
	"maps"
	"math"
	"sort"
	"strconv"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/redis/shared"
)

// getMembersWithScoresFromSnapshot gets a snapshot and returns member->score map.
// Handles TYPE_ZSET (FloatVal) and TYPE_SET (score=1.0).
func getMembersWithScoresFromSnapshot(cmd *shared.Command, key string) (map[string]float64, bool, bool, error) {
	snap, _, err := cmd.Context.GetSnapshot(key)
	if err != nil {
		return nil, false, false, err
	}
	if snap == nil || snap.Op == pb.EffectOp_REMOVE_OP {
		return nil, false, false, nil
	}
	if snap.Collection == pb.CollectionKind_KEYED || (snap.Collection == pb.CollectionKind_SCALAR && (snap.TypeTag == pb.ValueType_TYPE_ZSET || snap.TypeTag == pb.ValueType_TYPE_SET)) {
		result := make(map[string]float64, len(snap.NetAdds))
		for member, elem := range snap.NetAdds {
			if snap.TypeTag == pb.ValueType_TYPE_SET {
				result[member] = 1.0
			} else {
				result[member] = elem.Data.GetFloatVal()
			}
		}
		return result, true, false, nil
	}
	// Wrong type
	return nil, true, true, nil
}

// parseZStoreArgs parses common arguments for ZUNIONSTORE and ZINTERSTORE
// Returns: keys, weights, aggregate function, error message (empty if ok)
func parseZStoreArgs(args [][]byte, cmdName string) ([]string, []float64, string, string) {
	if len(args) < 2 {
		return nil, nil, "", "wrong number of arguments"
	}

	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return nil, nil, "", "value is not an integer or out of range"
	}
	if numKeys < 1 {
		return nil, nil, "", fmt.Sprintf("at least 1 input key is needed for '%s' command", cmdName)
	}

	if len(args) < 1+numKeys {
		return nil, nil, "", "syntax error"
	}

	keys := make([]string, numKeys)
	for i := range numKeys {
		keys[i] = string(args[1+i])
	}

	weights := make([]float64, numKeys)
	for i := range weights {
		weights[i] = 1.0
	}

	aggregate := "SUM"

	// Parse optional arguments
	i := 1 + numKeys
	for i < len(args) {
		opt := shared.ToUpper(args[i])
		switch string(opt) {
		case "WEIGHTS":
			i++
			for j := range numKeys {
				if i >= len(args) {
					return nil, nil, "", "syntax error"
				}
				w, err := strconv.ParseFloat(string(args[i]), 64)
				if err != nil || math.IsNaN(w) {
					return nil, nil, "", "weight is not a float"
				}
				weights[j] = w
				i++
			}
		case "AGGREGATE":
			i++
			if i >= len(args) {
				return nil, nil, "", "syntax error"
			}
			agg := shared.ToUpper(args[i])
			switch string(agg) {
			case "SUM", "MIN", "MAX":
				aggregate = string(agg)
			default:
				return nil, nil, "", "syntax error"
			}
			i++
		default:
			return nil, nil, "", "syntax error"
		}
	}

	return keys, weights, aggregate, ""
}

func handleZUnionStore(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("zunionstore")
		return
	}

	destKey := string(cmd.Args[0])
	srcKeys, weights, aggregate, errMsg := parseZStoreArgs(cmd.Args[1:], "zunionstore")
	if errMsg != "" {
		w.WriteError("ERR " + errMsg)
		return
	}

	// keys = [dest, src1, src2, ...]
	keys = append([]string{destKey}, srcKeys...)

	valid = true
	runner = func() {
		// Collect all members with their aggregated scores
		result := make(map[string]float64)

		for i, key := range srcKeys {
			members, _, wrongType, err := getMembersWithScoresFromSnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if wrongType {
				w.WriteWrongType()
				return
			}
			if members == nil {
				continue
			}

			for member, score := range members {
				weightedScore := score * weights[i]
				if math.IsNaN(weightedScore) {
					weightedScore = 0
				}
				if existing, ok := result[member]; ok {
					switch aggregate {
					case "SUM":
						sum := existing + weightedScore
						if math.IsNaN(sum) {
							sum = 0
						}
						result[member] = sum
					case "MIN":
						if weightedScore < existing {
							result[member] = weightedScore
						}
					case "MAX":
						if weightedScore > existing {
							result[member] = weightedScore
						}
					}
				} else {
					result[member] = weightedScore
				}
			}
		}

		// Store result
		if len(result) == 0 {
			if err := emitZSetDeleteKey(cmd, destKey); err != nil {
				w.WriteError(err.Error())
				return
			}
			w.WriteInteger(0)
			return
		}

		if err := emitZSetDeleteKey(cmd, destKey); err != nil {
			w.WriteError(err.Error())
			return
		}
		for member, score := range result {
			if err := emitZSetInsert(cmd, destKey, member, score); err != nil {
				w.WriteError(err.Error())
				return
			}
		}
		if err := emitZSetTypeTag(cmd, destKey); err != nil {
			w.WriteError(err.Error())
			return
		}

		w.WriteInteger(int64(len(result)))
	}
	return
}

func handleZInterStore(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("zinterstore")
		return
	}

	destKey := string(cmd.Args[0])
	srcKeys, weights, aggregate, errMsg := parseZStoreArgs(cmd.Args[1:], "zinterstore")
	if errMsg != "" {
		w.WriteError("ERR " + errMsg)
		return
	}

	// keys = [dest, src1, src2, ...]
	keys = append([]string{destKey}, srcKeys...)

	valid = true
	runner = func() {
		// Get first set as base for intersection
		firstMembers, firstExists, wrongType, err := getMembersWithScoresFromSnapshot(cmd, srcKeys[0])
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}
		if !firstExists {
			if err := emitZSetDeleteKey(cmd, destKey); err != nil {
				w.WriteError(err.Error())
				return
			}
			w.WriteInteger(0)
			return
		}

		// Start with members from first set
		result := make(map[string]float64)
		for member, score := range firstMembers {
			result[member] = score * weights[0]
		}

		// Intersect with remaining sets
		for i := 1; i < len(srcKeys); i++ {
			members, exists, wrongType, err := getMembersWithScoresFromSnapshot(cmd, srcKeys[i])
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if wrongType {
				w.WriteWrongType()
				return
			}
			if !exists {
				if err := emitZSetDeleteKey(cmd, destKey); err != nil {
					w.WriteError(err.Error())
					return
				}
				w.WriteInteger(0)
				return
			}

			newResult := make(map[string]float64)
			for member, existingScore := range result {
				if score, ok := members[member]; ok {
					weightedScore := score * weights[i]
					if math.IsNaN(weightedScore) {
						weightedScore = 0
					}
					switch aggregate {
					case "SUM":
						sum := existingScore + weightedScore
						if math.IsNaN(sum) {
							sum = 0
						}
						newResult[member] = sum
					case "MIN":
						if weightedScore < existingScore {
							newResult[member] = weightedScore
						} else {
							newResult[member] = existingScore
						}
					case "MAX":
						if weightedScore > existingScore {
							newResult[member] = weightedScore
						} else {
							newResult[member] = existingScore
						}
					}
				}
			}
			result = newResult

			if len(result) == 0 {
				break
			}
		}

		// Store result
		if len(result) == 0 {
			if err := emitZSetDeleteKey(cmd, destKey); err != nil {
				w.WriteError(err.Error())
				return
			}
			w.WriteInteger(0)
			return
		}

		if err := emitZSetDeleteKey(cmd, destKey); err != nil {
			w.WriteError(err.Error())
			return
		}
		for member, score := range result {
			if err := emitZSetInsert(cmd, destKey, member, score); err != nil {
				w.WriteError(err.Error())
				return
			}
		}
		if err := emitZSetTypeTag(cmd, destKey); err != nil {
			w.WriteError(err.Error())
			return
		}

		w.WriteInteger(int64(len(result)))
	}
	return
}

// parseZSetOpArgs parses arguments for ZUNION, ZINTER, ZDIFF (non-store variants)
// Returns: keys, weights, aggregate function, withScores, error message
func parseZSetOpArgs(args [][]byte, cmdName string) ([]string, []float64, string, bool, string) {
	if len(args) < 2 {
		return nil, nil, "", false, "wrong number of arguments"
	}

	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return nil, nil, "", false, "value is not an integer or out of range"
	}
	if numKeys < 1 {
		return nil, nil, "", false, fmt.Sprintf("at least 1 input key is needed for '%s' command", cmdName)
	}

	if len(args) < 1+numKeys {
		return nil, nil, "", false, "syntax error"
	}

	keys := make([]string, numKeys)
	for i := range numKeys {
		keys[i] = string(args[1+i])
	}

	weights := make([]float64, numKeys)
	for i := range weights {
		weights[i] = 1.0
	}

	aggregate := "SUM"
	withScores := false

	// Parse optional arguments
	i := 1 + numKeys
	for i < len(args) {
		opt := shared.ToUpper(args[i])
		switch string(opt) {
		case "WEIGHTS":
			i++
			for j := range numKeys {
				if i >= len(args) {
					return nil, nil, "", false, "syntax error"
				}
				w, err := strconv.ParseFloat(string(args[i]), 64)
				if err != nil || math.IsNaN(w) {
					return nil, nil, "", false, "weight is not a float"
				}
				weights[j] = w
				i++
			}
		case "AGGREGATE":
			i++
			if i >= len(args) {
				return nil, nil, "", false, "syntax error"
			}
			agg := shared.ToUpper(args[i])
			switch string(agg) {
			case "SUM", "MIN", "MAX":
				aggregate = string(agg)
			default:
				return nil, nil, "", false, "syntax error"
			}
			i++
		case "WITHSCORES":
			withScores = true
			i++
		default:
			return nil, nil, "", false, "syntax error"
		}
	}

	return keys, weights, aggregate, withScores, ""
}

func handleZUnion(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("zunion")
		return
	}

	srcKeys, weights, aggregate, withScores, errMsg := parseZSetOpArgs(cmd.Args, "zunion")
	if errMsg != "" {
		w.WriteError("ERR " + errMsg)
		return
	}

	keys = srcKeys

	valid = true
	runner = func() {
		result := make(map[string]float64)

		for i, key := range srcKeys {
			members, _, wrongType, err := getMembersWithScoresFromSnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if wrongType {
				w.WriteWrongType()
				return
			}
			if members == nil {
				continue
			}

			for member, score := range members {
				weightedScore := score * weights[i]
				if math.IsNaN(weightedScore) {
					weightedScore = 0
				}
				if existing, ok := result[member]; ok {
					switch aggregate {
					case "SUM":
						sum := existing + weightedScore
						if math.IsNaN(sum) {
							sum = 0
						}
						result[member] = sum
					case "MIN":
						if weightedScore < existing {
							result[member] = weightedScore
						}
					case "MAX":
						if weightedScore > existing {
							result[member] = weightedScore
						}
					}
				} else {
					result[member] = weightedScore
				}
			}
		}

		// Sort results by score, then by member
		entries := make([]shared.ZSetEntry, 0, len(result))
		for member, score := range result {
			entries = append(entries, shared.ZSetEntry{Member: member, Score: score})
		}
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].Score != entries[j].Score {
				return entries[i].Score < entries[j].Score
			}
			return entries[i].Member < entries[j].Member
		})

		writeZSetEntries(w, entries, withScores)
	}
	return
}

func handleZInter(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("zinter")
		return
	}

	srcKeys, weights, aggregate, withScores, errMsg := parseZSetOpArgs(cmd.Args, "zinter")
	if errMsg != "" {
		w.WriteError("ERR " + errMsg)
		return
	}

	keys = srcKeys

	if len(srcKeys) == 0 {
		valid = true
		runner = func() {
			w.WriteArray(0)
		}
		return
	}

	valid = true
	runner = func() {
		firstMembers, firstExists, wrongType, err := getMembersWithScoresFromSnapshot(cmd, srcKeys[0])
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}
		if !firstExists {
			w.WriteArray(0)
			return
		}

		result := make(map[string]float64)
		for member, score := range firstMembers {
			result[member] = score * weights[0]
		}

		for i := 1; i < len(srcKeys); i++ {
			members, exists, wrongType, err := getMembersWithScoresFromSnapshot(cmd, srcKeys[i])
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if wrongType {
				w.WriteWrongType()
				return
			}
			if !exists {
				w.WriteArray(0)
				return
			}

			newResult := make(map[string]float64)
			for member, existingScore := range result {
				if score, ok := members[member]; ok {
					weightedScore := score * weights[i]
					if math.IsNaN(weightedScore) {
						weightedScore = 0
					}
					switch aggregate {
					case "SUM":
						sum := existingScore + weightedScore
						if math.IsNaN(sum) {
							sum = 0
						}
						newResult[member] = sum
					case "MIN":
						if weightedScore < existingScore {
							newResult[member] = weightedScore
						} else {
							newResult[member] = existingScore
						}
					case "MAX":
						if weightedScore > existingScore {
							newResult[member] = weightedScore
						} else {
							newResult[member] = existingScore
						}
					}
				}
			}
			result = newResult

			if len(result) == 0 {
				break
			}
		}

		// Sort results
		entries := make([]shared.ZSetEntry, 0, len(result))
		for member, score := range result {
			entries = append(entries, shared.ZSetEntry{Member: member, Score: score})
		}
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].Score != entries[j].Score {
				return entries[i].Score < entries[j].Score
			}
			return entries[i].Member < entries[j].Member
		})

		writeZSetEntries(w, entries, withScores)
	}
	return
}

func handleZInterCard(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("zintercard")
		return
	}

	numKeys, err := strconv.Atoi(string(cmd.Args[0]))
	if err != nil {
		w.WriteError("ERR value is not an integer or out of range")
		return
	}
	if numKeys < 1 {
		w.WriteError("ERR at least 1 input key is needed for 'zintercard' command")
		return
	}

	if len(cmd.Args) < 1+numKeys {
		w.WriteSyntaxError()
		return
	}

	keys = make([]string, numKeys)
	for i := range numKeys {
		keys[i] = string(cmd.Args[1+i])
	}

	limit := 0
	i := 1 + numKeys
	for i < len(cmd.Args) {
		opt := shared.ToUpper(cmd.Args[i])
		if string(opt) == "LIMIT" {
			i++
			if i >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			limit, err = strconv.Atoi(string(cmd.Args[i]))
			if err != nil || limit < 0 {
				w.WriteError("ERR LIMIT must be a non-negative integer")
				return
			}
			i++
		} else {
			w.WriteSyntaxError()
			return
		}
	}

	if len(keys) == 0 {
		valid = true
		runner = func() {
			w.WriteInteger(0)
		}
		return
	}

	valid = true
	runner = func() {
		firstMembers, firstExists, wrongType, err := getMembersWithScoresFromSnapshot(cmd, keys[0])
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}
		if !firstExists {
			w.WriteInteger(0)
			return
		}

		candidates := make(map[string]struct{})
		for member := range firstMembers {
			candidates[member] = struct{}{}
		}

		for i := 1; i < len(keys); i++ {
			members, exists, wrongType, err := getMembersWithScoresFromSnapshot(cmd, keys[i])
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if wrongType {
				w.WriteWrongType()
				return
			}
			if !exists {
				w.WriteInteger(0)
				return
			}

			newCandidates := make(map[string]struct{})
			for member := range candidates {
				if _, ok := members[member]; ok {
					newCandidates[member] = struct{}{}
				}
			}
			candidates = newCandidates

			if len(candidates) == 0 {
				break
			}
		}

		count := len(candidates)
		if limit > 0 && count > limit {
			count = limit
		}

		w.WriteInteger(int64(count))
	}
	return
}

func handleZDiff(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("zdiff")
		return
	}

	numKeys, err := strconv.Atoi(string(cmd.Args[0]))
	if err != nil {
		w.WriteError("ERR value is not an integer or out of range")
		return
	}
	if numKeys < 1 {
		w.WriteError("ERR at least 1 input key is needed for 'zdiff' command")
		return
	}

	if len(cmd.Args) < 1+numKeys {
		w.WriteSyntaxError()
		return
	}

	keys = make([]string, numKeys)
	for i := range numKeys {
		keys[i] = string(cmd.Args[1+i])
	}

	withScores := false
	i := 1 + numKeys
	for i < len(cmd.Args) {
		opt := shared.ToUpper(cmd.Args[i])
		if string(opt) == "WITHSCORES" {
			withScores = true
			i++
		} else {
			w.WriteSyntaxError()
			return
		}
	}

	if len(keys) == 0 {
		valid = true
		runner = func() {
			w.WriteArray(0)
		}
		return
	}

	valid = true
	runner = func() {
		firstMembers, firstExists, wrongType, err := getMembersWithScoresFromSnapshot(cmd, keys[0])
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}
		if !firstExists {
			w.WriteArray(0)
			return
		}

		result := make(map[string]float64)
		maps.Copy(result, firstMembers)

		for i := 1; i < len(keys); i++ {
			members, _, wrongType, err := getMembersWithScoresFromSnapshot(cmd, keys[i])
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if wrongType {
				w.WriteWrongType()
				return
			}
			if members == nil {
				continue
			}

			for member := range members {
				delete(result, member)
			}
		}

		// Sort results
		entries := make([]shared.ZSetEntry, 0, len(result))
		for member, score := range result {
			entries = append(entries, shared.ZSetEntry{Member: member, Score: score})
		}
		sort.Slice(entries, func(i, j int) bool {
			if entries[i].Score != entries[j].Score {
				return entries[i].Score < entries[j].Score
			}
			return entries[i].Member < entries[j].Member
		})

		if withScores {
			w.WriteArray(len(entries) * 2)
			for _, entry := range entries {
				w.WriteBulkStringStr(entry.Member)
				w.WriteScore(entry.Score)
			}
		} else {
			w.WriteArray(len(entries))
			for _, entry := range entries {
				w.WriteBulkStringStr(entry.Member)
			}
		}
	}
	return
}

func handleZDiffStore(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("zdiffstore")
		return
	}

	destKey := string(cmd.Args[0])

	numKeys, err := strconv.Atoi(string(cmd.Args[1]))
	if err != nil {
		w.WriteError("ERR value is not an integer or out of range")
		return
	}
	if numKeys < 1 {
		w.WriteError("ERR at least 1 input key is needed for 'zdiffstore' command")
		return
	}

	if len(cmd.Args) < 2+numKeys {
		w.WriteSyntaxError()
		return
	}

	// ZDIFFSTORE doesn't accept any extra options like WITHSCORES
	if len(cmd.Args) > 2+numKeys {
		w.WriteSyntaxError()
		return
	}

	srcKeys := make([]string, numKeys)
	for i := range numKeys {
		srcKeys[i] = string(cmd.Args[2+i])
	}

	// keys = [dest, src1, src2, ...]
	keys = append([]string{destKey}, srcKeys...)

	valid = true
	runner = func() {
		firstMembers, firstExists, wrongType, err := getMembersWithScoresFromSnapshot(cmd, srcKeys[0])
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}
		if !firstExists {
			if err := emitZSetDeleteKey(cmd, destKey); err != nil {
				w.WriteError(err.Error())
				return
			}
			w.WriteInteger(0)
			return
		}

		result := make(map[string]float64)
		maps.Copy(result, firstMembers)

		for i := 1; i < len(srcKeys); i++ {
			members, _, wrongType, err := getMembersWithScoresFromSnapshot(cmd, srcKeys[i])
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if wrongType {
				w.WriteWrongType()
				return
			}
			if members == nil {
				continue
			}

			for member := range members {
				delete(result, member)
			}
		}

		if len(result) == 0 {
			if err := emitZSetDeleteKey(cmd, destKey); err != nil {
				w.WriteError(err.Error())
				return
			}
			w.WriteInteger(0)
			return
		}

		if err := emitZSetDeleteKey(cmd, destKey); err != nil {
			w.WriteError(err.Error())
			return
		}
		for member, score := range result {
			if err := emitZSetInsert(cmd, destKey, member, score); err != nil {
				w.WriteError(err.Error())
				return
			}
		}
		if err := emitZSetTypeTag(cmd, destKey); err != nil {
			w.WriteError(err.Error())
			return
		}

		w.WriteInteger(int64(len(result)))
	}
	return
}

// handleZRangeStore implements ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]
func handleZRangeStore(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("zrangestore")
		return
	}

	dst := string(cmd.Args[0])
	src := string(cmd.Args[1])
	keys = []string{dst, src}

	// Parse options
	var byScore, byLex, rev bool
	offset, count := 0, -1
	var hasLimit bool

	// Args after dst and src are start/stop
	startArg := cmd.Args[2]
	stopArg := cmd.Args[3]

	// Parse remaining options
	for i := 4; i < len(cmd.Args); i++ {
		opt := shared.ToUpper(cmd.Args[i])
		switch string(opt) {
		case "BYSCORE":
			byScore = true
		case "BYLEX":
			byLex = true
		case "REV":
			rev = true
		case "LIMIT":
			hasLimit = true
			if i+2 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			var err error
			offset, err = strconv.Atoi(string(cmd.Args[i+1]))
			if err != nil {
				w.WriteNotInteger()
				return
			}
			count, err = strconv.Atoi(string(cmd.Args[i+2]))
			if err != nil {
				w.WriteNotInteger()
				return
			}
			i += 2
		default:
			w.WriteSyntaxError()
			return
		}
	}

	// BYSCORE and BYLEX are mutually exclusive
	if byScore && byLex {
		w.WriteSyntaxError()
		return
	}

	// LIMIT requires BYSCORE or BYLEX
	if hasLimit && !byScore && !byLex {
		w.WriteSyntaxError()
		return
	}

	// Pre-parse score/lex bounds during validation
	var scoreMin, scoreMax float64
	var scoreMinEx, scoreMaxEx bool
	var lexMin, lexMax LexBound
	var startIdx, stopIdx int

	if byScore {
		var ok1, ok2 bool
		scoreMin, scoreMinEx, ok1 = ParseScore(startArg)
		scoreMax, scoreMaxEx, ok2 = ParseScore(stopArg)
		if !ok1 || !ok2 {
			w.WriteError("ERR min or max is not a float")
			return
		}
	} else if byLex {
		var ok1, ok2 bool
		lexMin, ok1 = ParseLexBound(startArg)
		lexMax, ok2 = ParseLexBound(stopArg)
		if !ok1 || !ok2 {
			w.WriteError("ERR min or max not valid string range item")
			return
		}
	} else {
		var err error
		startIdx, err = strconv.Atoi(string(startArg))
		if err != nil {
			w.WriteNotInteger()
			return
		}
		stopIdx, err = strconv.Atoi(string(stopArg))
		if err != nil {
			w.WriteNotInteger()
			return
		}
	}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getZSetSnapshot(cmd, src)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			if err := emitZSetDeleteKey(cmd, dst); err != nil {
				w.WriteError(err.Error())
				return
			}
			w.WriteInteger(0)
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		sorted := zsetSortedMembers(snap)
		var entries []shared.ZSetEntry

		if byScore {
			mn, mx := scoreMin, scoreMax
			mnEx, mxEx := scoreMinEx, scoreMaxEx
			if rev {
				mn, mx = mx, mn
				mnEx, mxEx = mxEx, mnEx
			}
			entries = zsetFilterByScore(sorted, mn, mx, mnEx, mxEx, rev, offset, count)
		} else if byLex {
			mn, mx := lexMin, lexMax
			if rev {
				mn, mx = mx, mn
			}
			var source []shared.ZSetEntry
			if rev {
				source = make([]shared.ZSetEntry, len(sorted))
				for i, j := 0, len(sorted)-1; i < len(sorted); i, j = i+1, j-1 {
					source[i] = sorted[j]
				}
			} else {
				source = sorted
			}
			filtered := make([]shared.ZSetEntry, 0)
			for _, entry := range source {
				if InLexRange(entry.Member, mn, mx) {
					filtered = append(filtered, entry)
				}
			}
			if offset < 0 {
				filtered = nil
			} else if offset > 0 {
				if offset >= len(filtered) {
					filtered = nil
				} else {
					filtered = filtered[offset:]
				}
			}
			if count >= 0 && count < len(filtered) {
				filtered = filtered[:count]
			}
			entries = filtered
		} else {
			entries = zsetRangeByIndex(sorted, startIdx, stopIdx, rev)
		}

		if len(entries) == 0 {
			if err := emitZSetDeleteKey(cmd, dst); err != nil {
				w.WriteError(err.Error())
				return
			}
			w.WriteInteger(0)
			return
		}

		// Store entries in destination
		if err := emitZSetDeleteKey(cmd, dst); err != nil {
			w.WriteError(err.Error())
			return
		}
		for _, entry := range entries {
			if err := emitZSetInsert(cmd, dst, entry.Member, entry.Score); err != nil {
				w.WriteError(err.Error())
				return
			}
		}
		if err := emitZSetTypeTag(cmd, dst); err != nil {
			w.WriteError(err.Error())
			return
		}

		w.WriteInteger(int64(len(entries)))
	}
	return
}
