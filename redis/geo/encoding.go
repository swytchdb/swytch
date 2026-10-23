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
	"math"
	"strings"
)

// Constants for geohash encoding (Redis uses 52-bit precision)
const (
	GEO_STEP_MAX        = 26           // Maximum geohash step (52 bits / 2)
	GEO_LAT_MIN         = -85.05112878 // Minimum latitude (Web Mercator limit)
	GEO_LAT_MAX         = 85.05112878  // Maximum latitude (Web Mercator limit)
	GEO_LON_MIN         = -180.0       // Minimum longitude
	GEO_LON_MAX         = 180.0        // Maximum longitude
	EARTH_RADIUS_METERS = 6372797.560856
	MERCATOR_MAX        = 20037726.37 // Maximum mercator projection value
	DEG_TO_RAD          = math.Pi / 180.0
	RAD_TO_DEG          = 180.0 / math.Pi
)

// geoEntry represents a location with its member name
type geoEntry struct {
	Member    string
	Hash      uint64
	Longitude float64
	Latitude  float64
	Distance  float64 // Distance from search center (used in searches)
}

// interleave64 interleaves the bits of two 32-bit integers
// lat bits go into even positions (0, 2, 4, ...), lon bits go into odd positions (1, 3, 5, ...)
func interleave64(xlo, ylo uint32) uint64 {
	B := [5]uint64{
		0x5555555555555555,
		0x3333333333333333,
		0x0F0F0F0F0F0F0F0F,
		0x00FF00FF00FF00FF,
		0x0000FFFF0000FFFF,
	}
	S := [5]uint{1, 2, 4, 8, 16}

	x := uint64(xlo)
	y := uint64(ylo)

	x = (x | (x << S[4])) & B[4]
	y = (y | (y << S[4])) & B[4]

	x = (x | (x << S[3])) & B[3]
	y = (y | (y << S[3])) & B[3]

	x = (x | (x << S[2])) & B[2]
	y = (y | (y << S[2])) & B[2]

	x = (x | (x << S[1])) & B[1]
	y = (y | (y << S[1])) & B[1]

	x = (x | (x << S[0])) & B[0]
	y = (y | (y << S[0])) & B[0]

	return x | (y << 1)
}

// deinterleave64 reverses the interleave process
func deinterleave64(interleaved uint64) uint64 {
	B := [6]uint64{
		0x5555555555555555,
		0x3333333333333333,
		0x0F0F0F0F0F0F0F0F,
		0x00FF00FF00FF00FF,
		0x0000FFFF0000FFFF,
		0x00000000FFFFFFFF,
	}
	S := [6]uint{0, 1, 2, 4, 8, 16}

	x := interleaved
	y := interleaved >> 1

	x = (x | (x >> S[0])) & B[0]
	y = (y | (y >> S[0])) & B[0]

	x = (x | (x >> S[1])) & B[1]
	y = (y | (y >> S[1])) & B[1]

	x = (x | (x >> S[2])) & B[2]
	y = (y | (y >> S[2])) & B[2]

	x = (x | (x >> S[3])) & B[3]
	y = (y | (y >> S[3])) & B[3]

	x = (x | (x >> S[4])) & B[4]
	y = (y | (y >> S[4])) & B[4]

	x = (x | (x >> S[5])) & B[5]
	y = (y | (y >> S[5])) & B[5]

	return x | (y << 32)
}

// geoHashEncodeWithRange encodes longitude/latitude to a 52-bit geohash using specified ranges
func geoHashEncodeWithRange(longitude, latitude, lonMin, lonMax, latMin, latMax float64) uint64 {
	// Calculate normalized offsets (0 to 1)
	latOffset := (latitude - latMin) / (latMax - latMin)
	lonOffset := (longitude - lonMin) / (lonMax - lonMin)

	// Convert to 26-bit integers
	latBits := uint32(latOffset * float64(uint64(1)<<GEO_STEP_MAX))
	lonBits := uint32(lonOffset * float64(uint64(1)<<GEO_STEP_MAX))

	// Interleave: lat in even positions, lon in odd positions
	return interleave64(latBits, lonBits)
}

// geoHashEncode encodes longitude/latitude to a 52-bit geohash using Web Mercator limits
func geoHashEncode(longitude, latitude float64) uint64 {
	return geoHashEncodeWithRange(longitude, latitude, GEO_LON_MIN, GEO_LON_MAX, GEO_LAT_MIN, GEO_LAT_MAX)
}

// geoHashDecode decodes a 52-bit geohash to longitude/latitude using Web Mercator limits
func geoHashDecode(hash uint64) (longitude, latitude float64) {
	hashSep := deinterleave64(hash) // result = [LAT in lower 32][LON in upper 32]

	latScale := GEO_LAT_MAX - GEO_LAT_MIN
	lonScale := GEO_LON_MAX - GEO_LON_MIN

	ilato := uint32(hashSep)       // get lat part
	ilono := uint32(hashSep >> 32) // get lon part

	// Calculate center of the decoded area
	latMin := GEO_LAT_MIN + (float64(ilato)/float64(uint64(1)<<GEO_STEP_MAX))*latScale
	latMax := GEO_LAT_MIN + (float64(ilato+1)/float64(uint64(1)<<GEO_STEP_MAX))*latScale
	lonMin := GEO_LON_MIN + (float64(ilono)/float64(uint64(1)<<GEO_STEP_MAX))*lonScale
	lonMax := GEO_LON_MIN + (float64(ilono+1)/float64(uint64(1)<<GEO_STEP_MAX))*lonScale

	longitude = (lonMin + lonMax) / 2
	latitude = (latMin + latMax) / 2

	// Clamp to valid ranges
	if longitude > GEO_LON_MAX {
		longitude = GEO_LON_MAX
	}

	if longitude < GEO_LON_MIN {
		longitude = GEO_LON_MIN
	}

	if latitude > GEO_LAT_MAX {
		latitude = GEO_LAT_MAX
	}

	if latitude < GEO_LAT_MIN {
		latitude = GEO_LAT_MIN
	}

	return
}

// geoHashToString converts a geohash to base32 string (11 characters for 52 bits)
func geoHashToString(hash uint64) string {
	const base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

	var result [11]byte

	for i := range 11 {
		var idx int
		if i == 10 {
			// We have just 52 bits, but the API outputs 11 bytes geohash.
			// For compatibility we assume zero (matching Redis behavior).
			idx = 0
		} else {
			idx = int((hash >> (52 - ((i + 1) * 5))) & 0x1f)
		}

		result[i] = base32[idx]
	}

	return string(result[:])
}

// haversineDistance calculates the distance between two points in meters
func haversineDistance(lon1, lat1, lon2, lat2 float64) float64 {
	// Convert to radians
	lat1Rad := lat1 * DEG_TO_RAD
	lat2Rad := lat2 * DEG_TO_RAD
	dLat := (lat2 - lat1) * DEG_TO_RAD
	dLon := (lon2 - lon1) * DEG_TO_RAD

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1Rad)*math.Cos(lat2Rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return EARTH_RADIUS_METERS * c
}

// convertDistance converts distance to the specified unit
func convertDistance(meters float64, unit string) float64 {
	switch strings.ToLower(unit) {
	case "km":
		return meters / 1000.0
	case "mi":
		return meters / 1609.34
	case "ft":
		return meters * 3.28084
	default: // "m"
		return meters
	}
}

// convertToMeters converts distance from the specified unit to meters
func convertToMeters(dist float64, unit string) float64 {
	switch strings.ToLower(unit) {
	case "km":
		return dist * 1000.0
	case "mi":
		return dist * 1609.34
	case "ft":
		return dist / 3.28084
	default: // "m"
		return dist
	}
}

// validateCoordinates checks if longitude and latitude are valid
func validateCoordinates(longitude, latitude float64) bool {
	return longitude >= GEO_LON_MIN && longitude <= GEO_LON_MAX &&
		latitude >= GEO_LAT_MIN && latitude <= GEO_LAT_MAX
}
