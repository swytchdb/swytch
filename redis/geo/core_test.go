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
	"bytes"
	"math"
	"strconv"
	"strings"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

func runHandler(handler shared.HandlerFunc, cmd *shared.Command) string {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	cmd.Runtime = eng
	cmd.Context = ctx

	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	valid, _, runner := handler(cmd, w, nil)
	if valid && runner != nil {
		runner()
		_ = ctx.Flush()
	}
	return buf.String()
}

func runHandlerWith(eng *effects.Engine, ctx *effects.Context, handler shared.HandlerFunc, cmd *shared.Command) string {
	cmd.Runtime = eng
	cmd.Context = ctx

	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	valid, _, runner := handler(cmd, w, nil)
	if valid && runner != nil {
		runner()
		_ = ctx.Flush()
	}
	return buf.String()
}

// seedGeoData adds geo members directly via effects for test setup.
func seedGeoData(eng *effects.Engine, ctx *effects.Context, key string, members map[string][2]float64) {
	for member, coords := range members {
		hash := geoHashEncode(coords[0], coords[1])
		_ = ctx.Emit(&pb.Effect{
			Key: []byte(key),
			Kind: &pb.Effect_Data{Data: &pb.DataEffect{
				Op:         pb.EffectOp_INSERT_OP,
				Merge:      pb.MergeRule_LAST_WRITE_WINS,
				Collection: pb.CollectionKind_KEYED,
				Id:         []byte(member),
				Value:      &pb.DataEffect_FloatVal{FloatVal: float64(hash)},
			}},
		})
	}
	_ = ctx.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_GEO}},
	})
	_ = ctx.Flush()
}

func TestGeoAddBasic(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Add a single member
	got := runHandlerWith(eng, ctx, handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{[]byte("mygeo"), []byte("13.361389"), []byte("38.115556"), []byte("Palermo")},
	})
	if got != ":1\r\n" {
		t.Errorf("GEOADD single = %q, want :1", got)
	}

	// Add another member
	got = runHandlerWith(eng, ctx, handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{[]byte("mygeo"), []byte("15.087269"), []byte("37.502669"), []byte("Catania")},
	})
	if got != ":1\r\n" {
		t.Errorf("GEOADD second = %q, want :1", got)
	}

	// Add existing member (no change) -> count should be 0
	got = runHandlerWith(eng, ctx, handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{[]byte("mygeo"), []byte("13.361389"), []byte("38.115556"), []byte("Palermo")},
	})
	if got != ":0\r\n" {
		t.Errorf("GEOADD existing same score = %q, want :0", got)
	}

	// Add multiple members at once
	got = runHandlerWith(eng, ctx, handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{
			[]byte("mygeo2"),
			[]byte("-122.4194"), []byte("37.7749"), []byte("SanFrancisco"),
			[]byte("-73.9857"), []byte("40.7484"), []byte("NewYork"),
		},
	})
	if got != ":2\r\n" {
		t.Errorf("GEOADD multiple = %q, want :2", got)
	}
}

func TestGeoAddNX(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Add initial member
	runHandlerWith(eng, ctx, handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{[]byte("mygeo"), []byte("13.361389"), []byte("38.115556"), []byte("Palermo")},
	})

	// NX: should not update existing
	got := runHandlerWith(eng, ctx, handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{[]byte("mygeo"), []byte("NX"), []byte("15.0"), []byte("37.0"), []byte("Palermo")},
	})
	if got != ":0\r\n" {
		t.Errorf("GEOADD NX existing = %q, want :0", got)
	}

	// NX: should add new member
	got = runHandlerWith(eng, ctx, handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{[]byte("mygeo"), []byte("NX"), []byte("15.087269"), []byte("37.502669"), []byte("Catania")},
	})
	if got != ":1\r\n" {
		t.Errorf("GEOADD NX new = %q, want :1", got)
	}
}

func TestGeoAddXX(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Add initial member
	runHandlerWith(eng, ctx, handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{[]byte("mygeo"), []byte("13.361389"), []byte("38.115556"), []byte("Palermo")},
	})

	// XX: should not add new member
	got := runHandlerWith(eng, ctx, handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{[]byte("mygeo"), []byte("XX"), []byte("15.087269"), []byte("37.502669"), []byte("Catania")},
	})
	if got != ":0\r\n" {
		t.Errorf("GEOADD XX new = %q, want :0", got)
	}

	// XX on non-existent key
	got = runHandlerWith(eng, ctx, handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{[]byte("nonexistent"), []byte("XX"), []byte("15.0"), []byte("37.0"), []byte("member")},
	})
	if got != ":0\r\n" {
		t.Errorf("GEOADD XX nonexistent key = %q, want :0", got)
	}
}

func TestGeoAddCH(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Add initial member
	runHandlerWith(eng, ctx, handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{[]byte("mygeo"), []byte("13.361389"), []byte("38.115556"), []byte("Palermo")},
	})

	// CH: update existing with different coords -> should count as changed
	got := runHandlerWith(eng, ctx, handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{[]byte("mygeo"), []byte("CH"), []byte("15.0"), []byte("37.0"), []byte("Palermo")},
	})
	if got != ":1\r\n" {
		t.Errorf("GEOADD CH changed = %q, want :1", got)
	}

	// CH: add new + update existing
	got = runHandlerWith(eng, ctx, handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{
			[]byte("mygeo"), []byte("CH"),
			[]byte("14.0"), []byte("36.0"), []byte("Palermo"),
			[]byte("15.087269"), []byte("37.502669"), []byte("Catania"),
		},
	})
	if got != ":2\r\n" {
		t.Errorf("GEOADD CH add+change = %q, want :2", got)
	}
}

func TestGeoAddWrongNumArgs(t *testing.T) {
	got := runHandler(handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{[]byte("mygeo")},
	})
	if !strings.Contains(got, "ERR") {
		t.Errorf("expected error, got %q", got)
	}
}

func TestGeoAddInvalidCoordinates(t *testing.T) {
	got := runHandler(handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{[]byte("mygeo"), []byte("200"), []byte("100"), []byte("bad")},
	})
	if !strings.Contains(got, "ERR") {
		t.Errorf("expected error, got %q", got)
	}
}

func TestGeoAddNXXXConflict(t *testing.T) {
	got := runHandler(handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{[]byte("mygeo"), []byte("NX"), []byte("XX"), []byte("13.0"), []byte("38.0"), []byte("m")},
	})
	if !strings.Contains(got, "ERR") {
		t.Errorf("expected error, got %q", got)
	}
}

func TestGeoPosExisting(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{[]byte("mygeo"), []byte("13.361389"), []byte("38.115556"), []byte("Palermo")},
	})

	got := runHandlerWith(eng, ctx, handleGeoPos, &shared.Command{
		Type: shared.CmdGeoPos,
		Args: [][]byte{[]byte("mygeo"), []byte("Palermo")},
	})

	if !strings.Contains(got, "13.36") || !strings.Contains(got, "38.11") {
		t.Errorf("GEOPOS = %q, expected coords near 13.36, 38.11", got)
	}
}

func TestGeoPosMissing(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{[]byte("mygeo"), []byte("13.361389"), []byte("38.115556"), []byte("Palermo")},
	})

	got := runHandlerWith(eng, ctx, handleGeoPos, &shared.Command{
		Type: shared.CmdGeoPos,
		Args: [][]byte{[]byte("mygeo"), []byte("Palermo"), []byte("NonExistent")},
	})

	if !strings.Contains(got, "*2\r\n") {
		t.Errorf("GEOPOS = %q, expected array of 2", got)
	}
	if !strings.Contains(got, "*-1\r\n") {
		t.Errorf("GEOPOS = %q, expected null array for missing member", got)
	}
}

func TestGeoPosNonExistentKey(t *testing.T) {
	got := runHandler(handleGeoPos, &shared.Command{
		Type: shared.CmdGeoPos,
		Args: [][]byte{[]byte("nonexistent"), []byte("member1"), []byte("member2")},
	})
	if got != "*2\r\n*-1\r\n*-1\r\n" {
		t.Errorf("GEOPOS nonexistent key = %q, want array of nulls", got)
	}
}

func TestGeoPosWrongNumArgs(t *testing.T) {
	got := runHandler(handleGeoPos, &shared.Command{
		Type: shared.CmdGeoPos,
		Args: [][]byte{},
	})
	if !strings.Contains(got, "ERR") {
		t.Errorf("expected error, got %q", got)
	}
}

func TestGeoDist(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("13.361389"), []byte("38.115556"), []byte("Palermo"),
			[]byte("15.087269"), []byte("37.502669"), []byte("Catania"),
		},
	})

	// Default unit (meters)
	got := runHandlerWith(eng, ctx, handleGeoDist, &shared.Command{
		Type: shared.CmdGeoDist,
		Args: [][]byte{[]byte("mygeo"), []byte("Palermo"), []byte("Catania")},
	})
	parts := strings.Split(strings.TrimRight(got, "\r\n"), "\r\n")
	if len(parts) >= 2 {
		dist, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			t.Fatalf("failed to parse distance: %v", err)
		}
		if math.Abs(dist-166274) > 1000 {
			t.Errorf("GEODIST distance = %f, expected ~166274m", dist)
		}
	}

	// Kilometers
	got = runHandlerWith(eng, ctx, handleGeoDist, &shared.Command{
		Type: shared.CmdGeoDist,
		Args: [][]byte{[]byte("mygeo"), []byte("Palermo"), []byte("Catania"), []byte("km")},
	})
	parts = strings.Split(strings.TrimRight(got, "\r\n"), "\r\n")
	if len(parts) >= 2 {
		dist, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			t.Fatalf("failed to parse distance: %v", err)
		}
		if math.Abs(dist-166.274) > 1 {
			t.Errorf("GEODIST km = %f, expected ~166.274", dist)
		}
	}
}

func TestGeoDistMissing(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{[]byte("mygeo"), []byte("13.361389"), []byte("38.115556"), []byte("Palermo")},
	})

	got := runHandlerWith(eng, ctx, handleGeoDist, &shared.Command{
		Type: shared.CmdGeoDist,
		Args: [][]byte{[]byte("mygeo"), []byte("Palermo"), []byte("NonExistent")},
	})
	if got != "$-1\r\n" {
		t.Errorf("GEODIST missing member = %q, want null bulk string", got)
	}

	got = runHandlerWith(eng, ctx, handleGeoDist, &shared.Command{
		Type: shared.CmdGeoDist,
		Args: [][]byte{[]byte("nokey"), []byte("a"), []byte("b")},
	})
	if got != "$-1\r\n" {
		t.Errorf("GEODIST nonexistent key = %q, want null bulk string", got)
	}
}

func TestGeoDistWrongNumArgs(t *testing.T) {
	got := runHandler(handleGeoDist, &shared.Command{
		Type: shared.CmdGeoDist,
		Args: [][]byte{[]byte("mygeo"), []byte("a")},
	})
	if !strings.Contains(got, "ERR") {
		t.Errorf("expected error, got %q", got)
	}
}

func TestGeoDistInvalidUnit(t *testing.T) {
	got := runHandler(handleGeoDist, &shared.Command{
		Type: shared.CmdGeoDist,
		Args: [][]byte{[]byte("mygeo"), []byte("a"), []byte("b"), []byte("furlongs")},
	})
	if !strings.Contains(got, "ERR") {
		t.Errorf("expected error, got %q", got)
	}
}

func TestGeoHash(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleGeoAdd, &shared.Command{
		Type: shared.CmdGeoAdd,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("13.361389"), []byte("38.115556"), []byte("Palermo"),
			[]byte("15.087269"), []byte("37.502669"), []byte("Catania"),
		},
	})

	got := runHandlerWith(eng, ctx, handleGeoHash, &shared.Command{
		Type: shared.CmdGeoHash,
		Args: [][]byte{[]byte("mygeo"), []byte("Palermo"), []byte("Catania"), []byte("NonExistent")},
	})

	if !strings.HasPrefix(got, "*3\r\n") {
		t.Errorf("GEOHASH = %q, expected array of 3", got)
	}
	if !strings.Contains(got, "$-1\r\n") {
		t.Errorf("GEOHASH = %q, expected null for missing member", got)
	}
	// Geohash strings should be 11 chars
	lines := strings.Split(got, "\r\n")
	for i, line := range lines {
		if strings.HasPrefix(line, "$11") {
			if i+1 < len(lines) && len(lines[i+1]) != 11 {
				t.Errorf("geohash string length = %d, want 11", len(lines[i+1]))
			}
		}
	}
}

func TestGeoHashNonExistentKey(t *testing.T) {
	got := runHandler(handleGeoHash, &shared.Command{
		Type: shared.CmdGeoHash,
		Args: [][]byte{[]byte("nonexistent"), []byte("member")},
	})
	if !strings.Contains(got, "$-1") {
		t.Errorf("expected null, got %q", got)
	}
}

func TestGeoHashWrongNumArgs(t *testing.T) {
	got := runHandler(handleGeoHash, &shared.Command{
		Type: shared.CmdGeoHash,
		Args: [][]byte{},
	})
	if !strings.Contains(got, "ERR") {
		t.Errorf("expected error, got %q", got)
	}
}

func TestGeoSearchByRadius(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	seedGeoData(eng, ctx, "mygeo", map[string][2]float64{
		"Palermo":  {13.361389, 38.115556},
		"Catania":  {15.087269, 37.502669},
		"Rome":     {12.496366, 41.902782},
		"Florence": {11.255814, 43.769562},
	})

	got := runHandlerWith(eng, ctx, handleGeoSearch, &shared.Command{
		Type: shared.CmdGeoSearch,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("FROMLONLAT"), []byte("13.361389"), []byte("38.115556"),
			[]byte("BYRADIUS"), []byte("200"), []byte("km"),
			[]byte("ASC"),
		},
	})

	if !strings.Contains(got, "Palermo") {
		t.Errorf("GEOSEARCH = %q, expected Palermo", got)
	}
	if !strings.Contains(got, "Catania") {
		t.Errorf("GEOSEARCH = %q, expected Catania", got)
	}
	if strings.Contains(got, "Rome") {
		t.Errorf("GEOSEARCH = %q, did not expect Rome", got)
	}
	if strings.Contains(got, "Florence") {
		t.Errorf("GEOSEARCH = %q, did not expect Florence", got)
	}
}

func TestGeoSearchByBox(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	seedGeoData(eng, ctx, "mygeo", map[string][2]float64{
		"Palermo": {13.361389, 38.115556},
		"Catania": {15.087269, 37.502669},
		"Rome":    {12.496366, 41.902782},
	})

	got := runHandlerWith(eng, ctx, handleGeoSearch, &shared.Command{
		Type: shared.CmdGeoSearch,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("FROMLONLAT"), []byte("13.361389"), []byte("38.115556"),
			[]byte("BYBOX"), []byte("400"), []byte("400"), []byte("km"),
		},
	})

	if !strings.Contains(got, "Palermo") {
		t.Errorf("GEOSEARCH BYBOX = %q, expected Palermo", got)
	}
	if !strings.Contains(got, "Catania") {
		t.Errorf("GEOSEARCH BYBOX = %q, expected Catania", got)
	}
}

func TestGeoSearchCount(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	seedGeoData(eng, ctx, "mygeo", map[string][2]float64{
		"Palermo": {13.361389, 38.115556},
		"Catania": {15.087269, 37.502669},
		"Rome":    {12.496366, 41.902782},
	})

	got := runHandlerWith(eng, ctx, handleGeoSearch, &shared.Command{
		Type: shared.CmdGeoSearch,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("FROMLONLAT"), []byte("13.361389"), []byte("38.115556"),
			[]byte("BYRADIUS"), []byte("1000"), []byte("km"),
			[]byte("ASC"), []byte("COUNT"), []byte("1"),
		},
	})

	if !strings.HasPrefix(got, "*1\r\n") {
		t.Errorf("GEOSEARCH COUNT 1 = %q, expected array of 1", got)
	}
	if !strings.Contains(got, "Palermo") {
		t.Errorf("GEOSEARCH COUNT 1 ASC = %q, expected Palermo", got)
	}
}

func TestGeoSearchDesc(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	seedGeoData(eng, ctx, "mygeo", map[string][2]float64{
		"Palermo": {13.361389, 38.115556},
		"Catania": {15.087269, 37.502669},
		"Rome":    {12.496366, 41.902782},
	})

	got := runHandlerWith(eng, ctx, handleGeoSearch, &shared.Command{
		Type: shared.CmdGeoSearch,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("FROMLONLAT"), []byte("13.361389"), []byte("38.115556"),
			[]byte("BYRADIUS"), []byte("1000"), []byte("km"),
			[]byte("DESC"), []byte("COUNT"), []byte("1"),
		},
	})

	if !strings.HasPrefix(got, "*1\r\n") {
		t.Errorf("GEOSEARCH DESC COUNT 1 = %q, expected array of 1", got)
	}
	if !strings.Contains(got, "Rome") {
		t.Errorf("GEOSEARCH DESC COUNT 1 = %q, expected Rome", got)
	}
}

func TestGeoSearchFromMember(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	seedGeoData(eng, ctx, "mygeo", map[string][2]float64{
		"Palermo": {13.361389, 38.115556},
		"Catania": {15.087269, 37.502669},
	})

	got := runHandlerWith(eng, ctx, handleGeoSearch, &shared.Command{
		Type: shared.CmdGeoSearch,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("FROMMEMBER"), []byte("Palermo"),
			[]byte("BYRADIUS"), []byte("200"), []byte("km"),
			[]byte("ASC"),
		},
	})

	if !strings.Contains(got, "Palermo") || !strings.Contains(got, "Catania") {
		t.Errorf("GEOSEARCH FROMMEMBER = %q, expected both members", got)
	}
}

func TestGeoSearchStore(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	seedGeoData(eng, ctx, "mygeo", map[string][2]float64{
		"Palermo": {13.361389, 38.115556},
		"Catania": {15.087269, 37.502669},
		"Rome":    {12.496366, 41.902782},
	})

	got := runHandlerWith(eng, ctx, handleGeoSearchStore, &shared.Command{
		Type: shared.CmdGeoSearchStore,
		Args: [][]byte{
			[]byte("dest"),
			[]byte("mygeo"),
			[]byte("FROMLONLAT"), []byte("13.361389"), []byte("38.115556"),
			[]byte("BYRADIUS"), []byte("200"), []byte("km"),
		},
	})

	if got != ":2\r\n" {
		t.Errorf("GEOSEARCHSTORE = %q, want :2", got)
	}

	// Verify dest key has data via GEOPOS
	got = runHandlerWith(eng, ctx, handleGeoPos, &shared.Command{
		Type: shared.CmdGeoPos,
		Args: [][]byte{[]byte("dest"), []byte("Palermo")},
	})
	if strings.Contains(got, "*-1\r\n") {
		t.Errorf("GEOSEARCHSTORE dest GEOPOS = %q, expected coords for Palermo", got)
	}
}

func TestGeoSearchStoreStoreDist(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	seedGeoData(eng, ctx, "mygeo", map[string][2]float64{
		"Palermo": {13.361389, 38.115556},
		"Catania": {15.087269, 37.502669},
	})

	got := runHandlerWith(eng, ctx, handleGeoSearchStore, &shared.Command{
		Type: shared.CmdGeoSearchStore,
		Args: [][]byte{
			[]byte("dest"),
			[]byte("mygeo"),
			[]byte("FROMLONLAT"), []byte("13.361389"), []byte("38.115556"),
			[]byte("BYRADIUS"), []byte("200"), []byte("km"),
			[]byte("STOREDIST"),
		},
	})

	if got != ":2\r\n" {
		t.Errorf("GEOSEARCHSTORE STOREDIST = %q, want :2", got)
	}

	snap, _, _, err := eng.GetSnapshot("dest")
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if snap == nil {
		t.Fatal("dest key not found")
	}

	// Palermo distance from itself should be ~0
	palermoScore, ok := geoMemberScore(snap, "Palermo")
	if !ok {
		t.Fatal("Palermo not found in dest")
	}
	if math.Abs(palermoScore) > 1 {
		t.Errorf("Palermo STOREDIST score = %f, expected ~0", palermoScore)
	}

	// Catania distance should be ~166 km
	cataniaScore, ok := geoMemberScore(snap, "Catania")
	if !ok {
		t.Fatal("Catania not found in dest")
	}
	if math.Abs(cataniaScore-166.274) > 1 {
		t.Errorf("Catania STOREDIST score = %f, expected ~166.274 km", cataniaScore)
	}
}

func TestGeoSearchNonExistentKey(t *testing.T) {
	got := runHandler(handleGeoSearch, &shared.Command{
		Type: shared.CmdGeoSearch,
		Args: [][]byte{
			[]byte("nonexistent"),
			[]byte("FROMLONLAT"), []byte("0"), []byte("0"),
			[]byte("BYRADIUS"), []byte("100"), []byte("km"),
		},
	})
	if got != "*0\r\n" {
		t.Errorf("GEOSEARCH nonexistent = %q, want empty array", got)
	}
}

func TestGeoRadiusCompat(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	seedGeoData(eng, ctx, "mygeo", map[string][2]float64{
		"Palermo": {13.361389, 38.115556},
		"Catania": {15.087269, 37.502669},
	})

	got := runHandlerWith(eng, ctx, handleGeoRadius, &shared.Command{
		Type: shared.CmdGeoRadius,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("13.361389"), []byte("38.115556"),
			[]byte("200"), []byte("km"),
			[]byte("ASC"),
		},
	})

	if !strings.Contains(got, "Palermo") || !strings.Contains(got, "Catania") {
		t.Errorf("GEORADIUS = %q, expected both members", got)
	}
}

func TestGeoRadiusByMemberCompat(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	seedGeoData(eng, ctx, "mygeo", map[string][2]float64{
		"Palermo": {13.361389, 38.115556},
		"Catania": {15.087269, 37.502669},
	})

	got := runHandlerWith(eng, ctx, handleGeoRadiusByMember, &shared.Command{
		Type: shared.CmdGeoRadiusByMember,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("Palermo"),
			[]byte("200"), []byte("km"),
			[]byte("ASC"),
		},
	})

	if !strings.Contains(got, "Palermo") || !strings.Contains(got, "Catania") {
		t.Errorf("GEORADIUSBYMEMBER = %q, expected both members", got)
	}
}

func TestGeoWrongType(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// Seed a string key
	_ = ctx.Emit(&pb.Effect{
		Key: []byte("mystring"),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: []byte("hello")},
		}},
	})
	_ = ctx.Emit(&pb.Effect{
		Key:  []byte("mystring"),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_STRING}},
	})
	_ = ctx.Flush()

	tests := []struct {
		name    string
		handler shared.HandlerFunc
		cmd     *shared.Command
	}{
		{"GEOADD", handleGeoAdd, &shared.Command{Type: shared.CmdGeoAdd, Args: [][]byte{[]byte("mystring"), []byte("13.0"), []byte("38.0"), []byte("m")}}},
		{"GEOPOS", handleGeoPos, &shared.Command{Type: shared.CmdGeoPos, Args: [][]byte{[]byte("mystring"), []byte("m")}}},
		{"GEODIST", handleGeoDist, &shared.Command{Type: shared.CmdGeoDist, Args: [][]byte{[]byte("mystring"), []byte("a"), []byte("b")}}},
		{"GEOHASH", handleGeoHash, &shared.Command{Type: shared.CmdGeoHash, Args: [][]byte{[]byte("mystring"), []byte("m")}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := runHandlerWith(eng, ctx, tt.handler, tt.cmd)
			if !strings.Contains(got, "WRONGTYPE") {
				t.Errorf("%s wrong type = %q, expected WRONGTYPE error", tt.name, got)
			}
		})
	}
}

func TestGeoSearchWithOptions(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	seedGeoData(eng, ctx, "mygeo", map[string][2]float64{
		"Palermo": {13.361389, 38.115556},
		"Catania": {15.087269, 37.502669},
	})

	got := runHandlerWith(eng, ctx, handleGeoSearch, &shared.Command{
		Type: shared.CmdGeoSearch,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("FROMLONLAT"), []byte("13.361389"), []byte("38.115556"),
			[]byte("BYRADIUS"), []byte("200"), []byte("km"),
			[]byte("ASC"),
			[]byte("WITHCOORD"), []byte("WITHDIST"), []byte("WITHHASH"),
		},
	})

	if !strings.Contains(got, "13.36") {
		t.Errorf("GEOSEARCH WITHCOORD = %q, expected longitude", got)
	}
	// Palermo distance from itself should be very small (geohash precision means not exactly 0)
	if !strings.Contains(got, "0.000") {
		t.Errorf("GEOSEARCH WITHDIST = %q, expected near-zero distance for Palermo", got)
	}
}
