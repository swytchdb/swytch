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

package geo

import (
	"sort"
	"strconv"
	"strings"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

// --- Helper functions (mirror zset/core.go pattern) ---

func getGeoSnapshot(cmd *shared.Command, key string) (snap *pb.ReducedEffect, tips []effects.Tip, exists bool, wrongType bool, err error) {
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
	if snap.Collection == pb.CollectionKind_SCALAR && (snap.TypeTag == pb.ValueType_TYPE_GEO || snap.TypeTag == pb.ValueType_TYPE_ZSET) {
		return snap, tips, true, false, nil
	}
	return nil, tips, true, true, nil
}

func geoMemberScore(snap *pb.ReducedEffect, member string) (float64, bool) {
	if snap == nil || snap.NetAdds == nil {
		return 0, false
	}
	elem, ok := snap.NetAdds[member]
	if !ok {
		return 0, false
	}
	return elem.Data.GetFloatVal(), true
}

func geoSortedEntries(snap *pb.ReducedEffect) []geoEntry {
	if snap == nil || snap.NetAdds == nil {
		return nil
	}
	entries := make([]geoEntry, 0, len(snap.NetAdds))
	for member, elem := range snap.NetAdds {
		hash := uint64(elem.Data.GetFloatVal())
		lon, lat := geoHashDecode(hash)
		entries = append(entries, geoEntry{
			Member:    member,
			Hash:      hash,
			Longitude: lon,
			Latitude:  lat,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Hash != entries[j].Hash {
			return entries[i].Hash < entries[j].Hash
		}
		return entries[i].Member < entries[j].Member
	})
	return entries
}

func emitGeoInsert(cmd *shared.Command, key string, member string, geohash float64) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(member),
			Value:      &pb.DataEffect_FloatVal{FloatVal: geohash},
		}},
	})
}

func emitGeoRemove(cmd *shared.Command, key string, member string) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_KEYED,
			Id:         []byte(member),
		}},
	})
}

func emitGeoTypeTag(cmd *shared.Command, key string) error {
	return cmd.Context.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_GEO}},
	})
}

func emitGeoDeleteKey(cmd *shared.Command, key string, tips ...[]effects.Tip) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_SCALAR,
		}},
	}, tips...)
}

// handleGeoAdd handles the GEOADD command
// GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]
func handleGeoAdd(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("geoadd")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	// Parse options
	var nx, xx, ch bool
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
		case "CH":
			ch = true
			i++
		default:
			goto parseMembers
		}
	}

parseMembers:
	// Validate conflicting options
	if nx && xx {
		w.WriteError("ERR syntax error")
		return
	}

	// Parse longitude, latitude, member triplets
	remaining := cmd.Args[i:]
	if len(remaining) < 3 || len(remaining)%3 != 0 {
		w.WriteError("ERR syntax error")
		return
	}

	// Parse and validate all coordinates first
	type geoMember struct {
		lon, lat float64
		member   string
	}
	members := make([]geoMember, 0, len(remaining)/3)

	for j := 0; j < len(remaining); j += 3 {
		lon, err := strconv.ParseFloat(string(remaining[j]), 64)
		if err != nil {
			w.WriteError("ERR value is not a valid float")
			return
		}
		lat, err := strconv.ParseFloat(string(remaining[j+1]), 64)
		if err != nil {
			w.WriteError("ERR value is not a valid float")
			return
		}

		if !validateCoordinates(lon, lat) {
			w.WriteError("ERR invalid longitude,latitude pair")
			return
		}

		members = append(members, geoMember{
			lon:    lon,
			lat:    lat,
			member: string(remaining[j+2]),
		})
	}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getGeoSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		if !exists && xx {
			w.WriteInteger(0)
			return
		}

		var addedCount, changedCount int
		for _, m := range members {
			hash := geoHashEncode(m.lon, m.lat)
			score := float64(hash)

			existingScore, memberExists := geoMemberScore(snap, m.member)
			if nx && memberExists {
				continue
			}
			if xx && !memberExists {
				continue
			}

			if memberExists {
				if score != existingScore {
					changedCount++
				}
			} else {
				addedCount++
			}

			if err := emitGeoInsert(cmd, key, m.member, score); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		if !exists {
			if err := emitGeoTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		if ch {
			w.WriteInteger(int64(addedCount + changedCount))
		} else {
			w.WriteInteger(int64(addedCount))
		}
	}
	return
}

// handleGeoPos handles the GEOPOS command
// GEOPOS key [member ...]
func handleGeoPos(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("geopos")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getGeoSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		if !exists {
			w.WriteArray(len(cmd.Args) - 1)
			for i := 1; i < len(cmd.Args); i++ {
				w.WriteNullArray()
			}
			return
		}

		w.WriteArray(len(cmd.Args) - 1)
		for i := 1; i < len(cmd.Args); i++ {
			member := string(cmd.Args[i])
			score, found := geoMemberScore(snap, member)
			if !found {
				w.WriteNullArray()
			} else {
				hash := uint64(score)
				lon, lat := geoHashDecode(hash)
				w.WriteArray(2)
				w.WriteBulkStringStr(strconv.FormatFloat(lon, 'f', -1, 64))
				w.WriteBulkStringStr(strconv.FormatFloat(lat, 'f', -1, 64))
			}
		}
	}
	return
}

// handleGeoDist handles the GEODIST command
// GEODIST key member1 member2 [M|KM|FT|MI]
func handleGeoDist(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 || len(cmd.Args) > 4 {
		w.WriteWrongNumArguments("geodist")
		return
	}

	key := string(cmd.Args[0])
	member1 := string(cmd.Args[1])
	member2 := string(cmd.Args[2])
	unit := "m"
	if len(cmd.Args) == 4 {
		unit = strings.ToLower(string(cmd.Args[3]))
		if unit != "m" && unit != "km" && unit != "mi" && unit != "ft" {
			w.WriteError("ERR unsupported unit provided. please use M, KM, FT, MI")
			return
		}
	}

	keys = []string{key}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getGeoSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		if !exists {
			w.WriteNullBulkString()
			return
		}

		score1, found1 := geoMemberScore(snap, member1)
		score2, found2 := geoMemberScore(snap, member2)
		if !found1 || !found2 {
			w.WriteNullBulkString()
			return
		}

		lon1, lat1 := geoHashDecode(uint64(score1))
		lon2, lat2 := geoHashDecode(uint64(score2))

		dist := haversineDistance(lon1, lat1, lon2, lat2)
		dist = convertDistance(dist, unit)

		w.WriteBulkStringStr(strconv.FormatFloat(dist, 'f', 4, 64))
	}
	return
}

// handleGeoHash handles the GEOHASH command
// GEOHASH key [member ...]
func handleGeoHash(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("geohash")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getGeoSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		if !exists {
			w.WriteArray(len(cmd.Args) - 1)
			for i := 1; i < len(cmd.Args); i++ {
				w.WriteNullBulkString()
			}
			return
		}

		w.WriteArray(len(cmd.Args) - 1)
		for i := 1; i < len(cmd.Args); i++ {
			member := string(cmd.Args[i])
			score, found := geoMemberScore(snap, member)
			if !found {
				w.WriteNullBulkString()
			} else {
				hash := uint64(score)
				lon, lat := geoHashDecode(hash)
				stdHash := geoHashEncodeWithRange(lon, lat, -180, 180, -90, 90)
				hashStr := geoHashToString(stdHash)
				w.WriteBulkStringStr(hashStr)
			}
		}
	}
	return
}
