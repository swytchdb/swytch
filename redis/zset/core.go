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
	"math"
	"sort"
	"strconv"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

// getZSetSnapshot returns the snapshot for a zset key.
// Returns (snapshot, tips, exists, wrongType, error).
func getZSetSnapshot(cmd *shared.Command, key string) (snap *pb.ReducedEffect, tips []effects.Tip, exists bool, wrongType bool, err error) {
	snap, tips, err = cmd.Context.GetSnapshot(key)
	if err != nil {
		return nil, nil, false, false, err
	}
	if snap == nil || snap.Op == pb.EffectOp_REMOVE_OP {
		return nil, tips, false, false, nil
	}
	if snap.Collection == pb.CollectionKind_KEYED {
		return snap, tips, true, false, nil
	}
	if snap.Collection == pb.CollectionKind_SCALAR && (snap.TypeTag == pb.ValueType_TYPE_ZSET || snap.TypeTag == pb.ValueType_TYPE_GEO) {
		return snap, tips, true, false, nil
	}
	return nil, tips, true, true, nil
}

func emitZSetInsert(cmd *shared.Command, key string, member string, score float64) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(member),
			Value:      &pb.DataEffect_FloatVal{FloatVal: score},
		}},
	})
}

func emitZSetIncrBy(cmd *shared.Command, key string, member string, delta float64) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_ADDITIVE_FLOAT,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(member),
			Value:      &pb.DataEffect_FloatVal{FloatVal: delta},
		}},
	})
}

func emitZSetRemove(cmd *shared.Command, key string, member string) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(member),
		}},
	})
}

func emitZSetTypeTag(cmd *shared.Command, key string) error {
	return cmd.Context.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_ZSET}},
	})
}

func emitZSetDeleteKey(cmd *shared.Command, key string, tips ...[]effects.Tip) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_SCALAR,
		}},
	}, tips...)
}

func zsetMemberScore(snap *pb.ReducedEffect, member string) (float64, bool) {
	if snap == nil || snap.NetAdds == nil {
		return 0, false
	}
	elem, ok := snap.NetAdds[member]
	if !ok {
		return 0, false
	}
	return elem.Data.GetFloatVal(), true
}

func zsetSortedMembers(snap *pb.ReducedEffect) []shared.ZSetEntry {
	if snap == nil || snap.NetAdds == nil {
		return nil
	}
	entries := make([]shared.ZSetEntry, 0, len(snap.NetAdds))
	for member, elem := range snap.NetAdds {
		entries = append(entries, shared.ZSetEntry{Member: member, Score: elem.Data.GetFloatVal()})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Score != entries[j].Score {
			return entries[i].Score < entries[j].Score
		}
		return entries[i].Member < entries[j].Member
	})
	return entries
}

func handleZAdd(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("zadd")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	// Parse options
	var nx, xx, gt, lt, ch, incr bool
	i := 1
	for i < len(cmd.Args) {
		opt := shared.ToUpper(cmd.Args[i])
		switch string(opt) {
		case "NX":
			nx = true
			i++
		case "XX":
			xx = true
			i++
		case "GT":
			gt = true
			i++
		case "LT":
			lt = true
			i++
		case "CH":
			ch = true
			i++
		case "INCR":
			incr = true
			i++
		default:
			goto parseScoreMembers
		}
	}

parseScoreMembers:
	// Validate conflicting options
	if nx && xx {
		w.WriteError("ERR XX and NX options at the same time are not compatible")
		return
	}
	if gt && lt {
		w.WriteError("ERR GT and LT options at the same time are not compatible")
		return
	}
	if (gt || lt) && nx {
		w.WriteError("ERR GT/LT and NX options at the same time are not compatible")
		return
	}

	// Parse score-member pairs
	remaining := cmd.Args[i:]
	if len(remaining) < 2 || len(remaining)%2 != 0 {
		w.WriteError("ERR syntax error")
		return
	}

	// INCR mode only allows one member
	if incr && len(remaining) != 2 {
		w.WriteError("ERR INCR option supports a single increment-element pair")
		return
	}

	// Pre-validate all scores before emitting any effects (atomicity).
	type scoreMember struct {
		score  float64
		member string
	}
	pairs := make([]scoreMember, 0, len(remaining)/2)
	for j := 0; j < len(remaining); j += 2 {
		score, err := strconv.ParseFloat(string(remaining[j]), 64)
		if err != nil || math.IsNaN(score) {
			w.WriteError("ERR value is not a valid float")
			return
		}
		pairs = append(pairs, scoreMember{score: score, member: string(remaining[j+1])})
	}

	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getZSetSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		// XX requires existing elements - if key doesn't exist, abort
		if xx && !exists {
			if incr {
				w.WriteNullBulkString()
			} else {
				w.WriteInteger(0)
			}
			return
		}

		var addedCount, changedCount int

		for _, pair := range pairs {
			score := pair.score
			member := pair.member

			if incr {
				// INCR mode: increment the score
				existingScore, memberExists := zsetMemberScore(snap, member)
				if nx && memberExists {
					w.WriteNullBulkString()
					return
				}
				if xx && !memberExists {
					w.WriteNullBulkString()
					return
				}
				if memberExists {
					newScore := existingScore + score
					if math.IsNaN(newScore) {
						w.WriteError("ERR resulting score is not a number (NaN)")
						return
					}
					if gt && newScore <= existingScore {
						w.WriteNullBulkString()
						return
					}
					if lt && newScore >= existingScore {
						w.WriteNullBulkString()
						return
					}
					if err := emitZSetIncrBy(cmd, key, member, score); err != nil {
						w.WriteError(err.Error())
						return
					}
					w.WriteScore(newScore)
				} else {
					if err := emitZSetInsert(cmd, key, member, score); err != nil {
						w.WriteError(err.Error())
						return
					}
					w.WriteScore(score)
				}
			} else {
				existingScore, memberExists := zsetMemberScore(snap, member)
				if nx && memberExists {
					continue
				}
				if xx && !memberExists {
					continue
				}
				if memberExists {
					finalScore := score
					if gt && score <= existingScore {
						continue
					}
					if lt && score >= existingScore {
						continue
					}
					if finalScore != existingScore {
						changedCount++
					}
					if err := emitZSetInsert(cmd, key, member, finalScore); err != nil {
						w.WriteError(err.Error())
						return
					}
				} else {
					addedCount++
					if err := emitZSetInsert(cmd, key, member, score); err != nil {
						w.WriteError(err.Error())
						return
					}
				}
			}
		}

		if !exists {
			if err := emitZSetTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		if !incr {
			if ch {
				w.WriteInteger(int64(addedCount + changedCount))
			} else {
				w.WriteInteger(int64(addedCount))
			}
		}
	}
	return
}

func handleZIncrBy(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("zincrby")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	increment, err := strconv.ParseFloat(string(cmd.Args[1]), 64)
	if err != nil || math.IsNaN(increment) {
		w.WriteError("ERR value is not a valid float")
		return
	}
	member := string(cmd.Args[2])

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getZSetSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		existingScore, memberExists := zsetMemberScore(snap, member)
		newScore := existingScore + increment
		if memberExists && math.IsNaN(newScore) {
			w.WriteError("ERR resulting score is not a number (NaN)")
			return
		}

		if memberExists {
			if err := emitZSetIncrBy(cmd, key, member, increment); err != nil {
				w.WriteError(err.Error())
				return
			}
		} else {
			if err := emitZSetInsert(cmd, key, member, increment); err != nil {
				w.WriteError(err.Error())
				return
			}
			newScore = increment
		}

		if !exists {
			if err := emitZSetTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteScore(newScore)
	}
	return
}
