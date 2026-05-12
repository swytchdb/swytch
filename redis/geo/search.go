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

	"github.com/swytchdb/swytch/redis/shared"
)

// handleGeoSearch handles the GEOSEARCH command
// GEOSEARCH key FROMMEMBER member | FROMLONLAT longitude latitude
//
//	BYRADIUS radius M|KM|FT|MI | BYBOX width height M|KM|FT|MI
//	[ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
func handleGeoSearch(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("geosearch")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}

	valid = true
	runner = func() {
		geoSearch(cmd, w, db, "", false)
	}
	return
}

// handleGeoSearchStore handles the GEOSEARCHSTORE command
// GEOSEARCHSTORE destination source FROMMEMBER member | FROMLONLAT longitude latitude
//
//	BYRADIUS radius M|KM|FT|MI | BYBOX width height M|KM|FT|MI
//	[ASC|DESC] [COUNT count [ANY]] [STOREDIST]
func handleGeoSearchStore(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("geosearchstore")
		return
	}

	destKey := string(cmd.Args[0])
	sourceKey := string(cmd.Args[1])
	keys = []string{destKey, sourceKey}

	valid = true
	runner = func() {
		// Create a modified command that looks like GEOSEARCH
		modCmd := &shared.Command{
			Type:    shared.CmdGeoSearch,
			Args:    cmd.Args[1:], // Skip destination key
			Runtime: cmd.Runtime,
			Context: cmd.Context,
		}

		geoSearch(modCmd, w, db, destKey, true)
	}
	return
}

// handleGeoRadius handles the deprecated GEORADIUS command
// GEORADIUS key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]
func handleGeoRadius(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 5 {
		w.WriteWrongNumArguments("georadius")
		return
	}

	sourceKey := string(cmd.Args[0])

	// Extract STORE/STOREDIST options from remaining args (they take a key in GEORADIUS)
	var destKey string
	var storeDist bool
	var filteredOpts [][]byte

	isRO := cmd.Type == shared.CmdGeoRadiusRO

	for i := 5; i < len(cmd.Args); i++ {
		opt := strings.ToUpper(string(cmd.Args[i]))
		switch opt {
		case "STORE":
			if isRO {
				w.WriteError("ERR STORE option not allowed in GEORADIUS_RO")
				return
			}
			if i+1 >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			destKey = string(cmd.Args[i+1])
			i++ // skip the key
		case "STOREDIST":
			if isRO {
				w.WriteError("ERR STOREDIST option not allowed in GEORADIUS_RO")
				return
			}
			if i+1 >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			destKey = string(cmd.Args[i+1])
			storeDist = true
			i++ // skip the key
		default:
			filteredOpts = append(filteredOpts, cmd.Args[i])
		}
	}

	// Build keys list
	if destKey != "" {
		keys = []string{destKey, sourceKey}
	} else {
		keys = []string{sourceKey}
	}

	valid = true
	runner = func() {
		// Build GEOSEARCH args
		newArgs := make([][]byte, 0, len(filteredOpts)+7)
		newArgs = append(newArgs, cmd.Args[0])              // key
		newArgs = append(newArgs, []byte("FROMLONLAT"))     // marker
		newArgs = append(newArgs, cmd.Args[1], cmd.Args[2]) // lon, lat
		newArgs = append(newArgs, []byte("BYRADIUS"))       // marker
		newArgs = append(newArgs, cmd.Args[3], cmd.Args[4]) // radius, unit
		newArgs = append(newArgs, filteredOpts...)          // remaining options (without STORE/STOREDIST)

		if storeDist {
			newArgs = append(newArgs, []byte("STOREDIST"))
		}

		modCmd := &shared.Command{
			Type:    shared.CmdGeoSearch,
			Args:    newArgs,
			Runtime: cmd.Runtime,
			Context: cmd.Context,
		}
		geoSearch(modCmd, w, db, destKey, destKey != "")
	}
	return
}

// handleGeoRadiusByMember handles the deprecated GEORADIUSBYMEMBER command
// GEORADIUSBYMEMBER key member radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]
func handleGeoRadiusByMember(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 4 {
		w.WriteWrongNumArguments("georadiusbymember")
		return
	}

	sourceKey := string(cmd.Args[0])

	// Extract STORE/STOREDIST options from remaining args (they take a key in GEORADIUSBYMEMBER)
	var destKey string
	var storeDist bool
	var filteredOpts [][]byte

	isRO := cmd.Type == shared.CmdGeoRadiusByMemberRO

	for i := 4; i < len(cmd.Args); i++ {
		opt := strings.ToUpper(string(cmd.Args[i]))
		switch opt {
		case "STORE":
			if isRO {
				w.WriteError("ERR STORE option not allowed in GEORADIUSBYMEMBER_RO")
				return
			}
			if i+1 >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			destKey = string(cmd.Args[i+1])
			i++ // skip the key
		case "STOREDIST":
			if isRO {
				w.WriteError("ERR STOREDIST option not allowed in GEORADIUSBYMEMBER_RO")
				return
			}
			if i+1 >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			destKey = string(cmd.Args[i+1])
			storeDist = true
			i++ // skip the key
		default:
			filteredOpts = append(filteredOpts, cmd.Args[i])
		}
	}

	// Build keys list
	if destKey != "" {
		keys = []string{destKey, sourceKey}
	} else {
		keys = []string{sourceKey}
	}

	valid = true
	runner = func() {
		// Build GEOSEARCH args
		newArgs := make([][]byte, 0, len(filteredOpts)+6)
		newArgs = append(newArgs, cmd.Args[0])              // key
		newArgs = append(newArgs, []byte("FROMMEMBER"))     // marker
		newArgs = append(newArgs, cmd.Args[1])              // member
		newArgs = append(newArgs, []byte("BYRADIUS"))       // marker
		newArgs = append(newArgs, cmd.Args[2], cmd.Args[3]) // radius, unit
		newArgs = append(newArgs, filteredOpts...)          // remaining options (without STORE/STOREDIST)

		if storeDist {
			newArgs = append(newArgs, []byte("STOREDIST"))
		}

		modCmd := &shared.Command{
			Type:    shared.CmdGeoSearch,
			Args:    newArgs,
			Runtime: cmd.Runtime,
			Context: cmd.Context,
		}
		geoSearch(modCmd, w, db, destKey, destKey != "")
	}
	return
}

// geoSearch is the core implementation for GEOSEARCH and GEOSEARCHSTORE
func geoSearch(cmd *shared.Command, w *shared.Writer, db *shared.Database, destKey string, isStore bool) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("geosearch")
		return
	}

	key := string(cmd.Args[0])

	// Parse options
	var (
		fromMember        string
		fromLon, fromLat  float64
		hasFromMember     bool
		hasFromLonLat     bool
		byRadius          float64
		byWidth, byHeight float64
		unit              = "m"
		hasRadius         bool
		hasBox            bool
		descending        bool
		doSort            bool // Only sort if ASC or DESC explicitly specified
		count             int
		countAny          bool
		withCoord         bool
		withDist          bool
		withHash          bool
		storeDist         bool
	)

	i := 1
	for i < len(cmd.Args) {
		opt := shared.ToUpper(cmd.Args[i])
		switch string(opt) {
		case "FROMMEMBER":
			if i+1 >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			fromMember = string(cmd.Args[i+1])
			hasFromMember = true
			i += 2
		case "FROMLONLAT":
			if i+2 >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			var err error
			fromLon, err = strconv.ParseFloat(string(cmd.Args[i+1]), 64)
			if err != nil {
				w.WriteError("ERR value is not a valid float")
				return
			}
			fromLat, err = strconv.ParseFloat(string(cmd.Args[i+2]), 64)
			if err != nil {
				w.WriteError("ERR value is not a valid float")
				return
			}
			if !validateCoordinates(fromLon, fromLat) {
				w.WriteError("ERR invalid longitude,latitude pair")
				return
			}
			hasFromLonLat = true
			i += 3
		case "BYRADIUS":
			if i+2 >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			var err error
			byRadius, err = strconv.ParseFloat(string(cmd.Args[i+1]), 64)
			if err != nil || byRadius < 0 {
				w.WriteError("ERR need a non-negative radius")
				return
			}
			unit = strings.ToLower(string(cmd.Args[i+2]))
			if unit != "m" && unit != "km" && unit != "mi" && unit != "ft" {
				w.WriteError("ERR unsupported unit provided. please use M, KM, FT, MI")
				return
			}
			hasRadius = true
			i += 3
		case "BYBOX":
			if i+3 >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			var err error
			byWidth, err = strconv.ParseFloat(string(cmd.Args[i+1]), 64)
			if err != nil || byWidth < 0 {
				w.WriteError("ERR need a non-negative width")
				return
			}
			byHeight, err = strconv.ParseFloat(string(cmd.Args[i+2]), 64)
			if err != nil || byHeight < 0 {
				w.WriteError("ERR need a non-negative height")
				return
			}
			unit = strings.ToLower(string(cmd.Args[i+3]))
			if unit != "m" && unit != "km" && unit != "mi" && unit != "ft" {
				w.WriteError("ERR unsupported unit provided. please use M, KM, FT, MI")
				return
			}
			hasBox = true
			i += 4
		case "ASC":
			descending = false
			doSort = true
			i++
		case "DESC":
			descending = true
			doSort = true
			i++
		case "COUNT":
			if i+1 >= len(cmd.Args) {
				w.WriteError("ERR syntax error")
				return
			}
			var err error
			count64, err := strconv.ParseInt(string(cmd.Args[i+1]), 10, 64)
			if err != nil || count64 <= 0 {
				w.WriteError("ERR COUNT must be > 0")
				return
			}
			count = int(count64)
			i += 2
		case "ANY":
			countAny = true
			i++
		case "WITHCOORD":
			withCoord = true
			i++
		case "WITHDIST":
			withDist = true
			i++
		case "WITHHASH":
			withHash = true
			i++
		case "STOREDIST":
			if !isStore {
				// STOREDIST is only valid for GEOSEARCHSTORE, not GEOSEARCH
				w.WriteError("ERR syntax error")
				return
			}
			storeDist = true
			i++
		default:
			w.WriteError("ERR syntax error")
			return
		}
	}

	// Validate required options
	if !hasFromMember && !hasFromLonLat {
		w.WriteError("ERR exactly one of FROMMEMBER or FROMLONLAT can be specified for GEOSEARCH")
		return
	}
	if hasFromMember && hasFromLonLat {
		w.WriteError("ERR syntax error")
		return
	}
	if !hasRadius && !hasBox {
		w.WriteError("ERR exactly one of BYRADIUS and BYBOX can be specified for GEOSEARCH")
		return
	}
	if hasRadius && hasBox {
		w.WriteError("ERR syntax error")
		return
	}

	// ANY requires COUNT
	if countAny && count == 0 {
		w.WriteError("ERR the ANY option requires the COUNT option")
		return
	}

	// Cannot use WITH* options with STORE
	if isStore && (withCoord || withDist || withHash) {
		w.WriteError("ERR STORE option cannot be combined with WITHCOORD, WITHDIST, or WITHHASH")
		return
	}

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
		if isStore {
			if err := emitGeoDeleteKey(cmd, destKey); err != nil {
				w.WriteError(err.Error())
				return
			}
			w.WriteInteger(0)
		} else {
			w.WriteArray(0)
		}
		return
	}

	// Get center coordinates
	if hasFromMember {
		score, found := geoMemberScore(snap, fromMember)
		if !found {
			w.WriteError("ERR could not decode requested zset member")
			return
		}
		fromLon, fromLat = geoHashDecode(uint64(score))
	}

	// Convert search dimensions to meters
	radiusMeters := convertToMeters(byRadius, unit)
	widthMeters := convertToMeters(byWidth, unit)
	heightMeters := convertToMeters(byHeight, unit)

	// Collect matching entries
	var results []geoEntry

	// Get entries sorted by geohash/score
	sortedEntries := geoSortedEntries(snap)

	for _, entry := range sortedEntries {
		dist := haversineDistance(fromLon, fromLat, entry.Longitude, entry.Latitude)

		var inRange bool
		if hasRadius {
			inRange = dist <= radiusMeters
		} else {
			lonDistance := haversineDistance(fromLon, entry.Latitude, entry.Longitude, entry.Latitude)
			latDistance := haversineDistance(entry.Longitude, fromLat, entry.Longitude, entry.Latitude)
			inRange = latDistance <= heightMeters/2 && lonDistance <= widthMeters/2
		}

		if inRange {
			entry.Distance = dist
			results = append(results, entry)

			if countAny && count > 0 && len(results) >= count {
				break
			}
		}
	}

	// Sort by distance
	shouldSort := doSort || (count > 0 && !countAny)
	if shouldSort {
		if descending {
			sort.Slice(results, func(i, j int) bool {
				return results[i].Distance > results[j].Distance
			})
		} else {
			sort.Slice(results, func(i, j int) bool {
				return results[i].Distance < results[j].Distance
			})
		}
	}

	// Apply count limit
	if count > 0 && len(results) > count {
		results = results[:count]
	}

	// Store results or return them
	if isStore {
		if len(results) > 0 {
			// Delete existing dest key first
			if err := emitGeoDeleteKey(cmd, destKey); err != nil {
				w.WriteError(err.Error())
				return
			}
			for _, entry := range results {
				var score float64
				if storeDist {
					score = convertDistance(entry.Distance, unit)
				} else {
					score = float64(entry.Hash)
				}
				if err := emitGeoInsert(cmd, destKey, entry.Member, score); err != nil {
					w.WriteError(err.Error())
					return
				}
			}
			if err := emitGeoTypeTag(cmd, destKey); err != nil {
				w.WriteError(err.Error())
				return
			}
		} else {
			if err := emitGeoDeleteKey(cmd, destKey); err != nil {
				w.WriteError(err.Error())
				return
			}
		}
		w.WriteInteger(int64(len(results)))
		return
	}

	// Write results
	w.WriteArray(len(results))
	for _, entry := range results {
		// Calculate how many fields to return
		fieldCount := 1 // member name
		if withDist {
			fieldCount++
		}
		if withHash {
			fieldCount++
		}
		if withCoord {
			fieldCount++
		}

		if fieldCount > 1 {
			w.WriteArray(fieldCount)
		}

		w.WriteBulkStringStr(entry.Member)

		if withDist {
			distStr := strconv.FormatFloat(convertDistance(entry.Distance, unit), 'f', 4, 64)
			w.WriteBulkStringStr(distStr)
		}

		if withHash {
			w.WriteInteger(int64(entry.Hash))
		}

		if withCoord {
			w.WriteArray(2)
			w.WriteBulkStringStr(strconv.FormatFloat(entry.Longitude, 'f', -1, 64))
			w.WriteBulkStringStr(strconv.FormatFloat(entry.Latitude, 'f', -1, 64))
		}
	}
}
