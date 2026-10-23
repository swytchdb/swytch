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
	"testing"
)

func TestGeoHashEncodeDecodRoundtrip(t *testing.T) {
	tests := []struct {
		name string
		lon  float64
		lat  float64
	}{
		{"origin", 0, 0},
		{"positive", 13.361389, 38.115556},
		{"negative lon", -122.4194, 37.7749},
		{"negative both", -73.935242, -33.8688},
		{"max lon", 180, 0},
		{"min lon", -180, 0},
		{"max lat", 0, GEO_LAT_MAX},
		{"min lat", 0, GEO_LAT_MIN},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash := geoHashEncode(tt.lon, tt.lat)
			gotLon, gotLat := geoHashDecode(hash)

			// Geohash precision is about 0.6m at 52 bits, so ~0.00001 degree tolerance
			if math.Abs(gotLon-tt.lon) > 0.0001 {
				t.Errorf("longitude: got %f, want %f (diff %f)", gotLon, tt.lon, math.Abs(gotLon-tt.lon))
			}

			if math.Abs(gotLat-tt.lat) > 0.0001 {
				t.Errorf("latitude: got %f, want %f (diff %f)", gotLat, tt.lat, math.Abs(gotLat-tt.lat))
			}
		})
	}
}

func TestGeoHashToString(t *testing.T) {
	// Encode a known location and verify the string is 11 characters
	hash := geoHashEncodeWithRange(13.361389, 38.115556, -180, 180, -90, 90)

	s := geoHashToString(hash)
	if len(s) != 11 {
		t.Errorf("expected 11-char geohash string, got %d chars: %q", len(s), s)
	}
	// Should only contain valid base32 characters
	for _, c := range s {
		if !strings.ContainsRune("0123456789bcdefghjkmnpqrstuvwxyz", c) {
			t.Errorf("invalid geohash character: %c in %q", c, s)
		}
	}
}

func TestHaversineDistance(t *testing.T) {
	tests := []struct {
		name    string
		lon1    float64
		lat1    float64
		lon2    float64
		lat2    float64
		minDist float64
		maxDist float64
	}{
		{"same point", 0, 0, 0, 0, 0, 0.001},
		{"Rome to Catania", 12.4964, 41.9028, 15.0872, 37.5023, 530000, 540000},
		{"short distance", 13.361389, 38.115556, 15.087269, 37.502669, 166000, 167000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dist := haversineDistance(tt.lon1, tt.lat1, tt.lon2, tt.lat2)
			if dist < tt.minDist || dist > tt.maxDist {
				t.Errorf("distance %f not in range [%f, %f]", dist, tt.minDist, tt.maxDist)
			}
		})
	}
}

func TestConvertDistance(t *testing.T) {
	meters := 1000.0
	tests := []struct {
		unit string
		want float64
	}{
		{"m", 1000.0},
		{"km", 1.0},
		{"mi", 1000.0 / 1609.34},
		{"ft", 1000.0 * 3.28084},
	}

	for _, tt := range tests {
		t.Run(tt.unit, func(t *testing.T) {
			got := convertDistance(meters, tt.unit)
			if math.Abs(got-tt.want) > 0.001 {
				t.Errorf("convertDistance(1000, %q) = %f, want %f", tt.unit, got, tt.want)
			}
		})
	}
}

func TestConvertToMeters(t *testing.T) {
	tests := []struct {
		dist float64
		unit string
		want float64
	}{
		{1000, "m", 1000},
		{1, "km", 1000},
		{1, "mi", 1609.34},
		{3.28084, "ft", 1.0},
	}

	for _, tt := range tests {
		t.Run(tt.unit, func(t *testing.T) {
			got := convertToMeters(tt.dist, tt.unit)
			if math.Abs(got-tt.want) > 0.01 {
				t.Errorf("convertToMeters(%f, %q) = %f, want %f", tt.dist, tt.unit, got, tt.want)
			}
		})
	}
}

func TestValidateCoordinates(t *testing.T) {
	tests := []struct {
		name  string
		lon   float64
		lat   float64
		valid bool
	}{
		{"valid origin", 0, 0, true},
		{"valid positive", 13.361389, 38.115556, true},
		{"valid negative", -122.4194, -33.8688, true},
		{"lon too high", 181, 0, false},
		{"lon too low", -181, 0, false},
		{"lat too high", 0, 86, false},
		{"lat too low", 0, -86, false},
		{"boundary lon max", 180, 0, true},
		{"boundary lon min", -180, 0, true},
		{"boundary lat max", 0, GEO_LAT_MAX, true},
		{"boundary lat min", 0, GEO_LAT_MIN, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := validateCoordinates(tt.lon, tt.lat)
			if got != tt.valid {
				t.Errorf("validateCoordinates(%f, %f) = %v, want %v", tt.lon, tt.lat, got, tt.valid)
			}
		})
	}
}

func TestInterleaveDeinterleaveRoundtrip(t *testing.T) {
	tests := []struct {
		x uint32
		y uint32
	}{
		{0, 0},
		{1, 0},
		{0, 1},
		{0xFFFF, 0xFFFF},
		{12345, 67890},
	}

	for _, tt := range tests {
		interleaved := interleave64(tt.x, tt.y)
		result := deinterleave64(interleaved)
		gotX := uint32(result)

		gotY := uint32(result >> 32)
		if gotX != tt.x || gotY != tt.y {
			t.Errorf("roundtrip(%d, %d): got (%d, %d)", tt.x, tt.y, gotX, gotY)
		}
	}
}
