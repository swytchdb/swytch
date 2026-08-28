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
	"strings"
	"testing"

	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

// addThreeCities seeds Palermo, Catania, and Rome using the effects engine.
func addThreeCities(t *testing.T, eng *effects.Engine, ctx *effects.Context) {
	t.Helper()
	seedGeoData(eng, ctx, "mygeo", map[string][2]float64{
		"Palermo": {13.361389, 38.115556},
		"Catania": {15.087269, 37.502669},
		"Rome":    {12.496366, 41.902782},
	})
}

func TestHandleGeoSearch_ByRadius_FromLonLat(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoSearch, &shared.Command{
		Type: shared.CmdGeoSearch,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("FROMLONLAT"), []byte("13.361389"), []byte("38.115556"),
			[]byte("BYRADIUS"), []byte("200"), []byte("km"),
		},
	})
	if !strings.Contains(got, "Palermo") {
		t.Fatalf("expected Palermo in results, got %q", got)
	}
	if !strings.Contains(got, "Catania") {
		t.Fatalf("expected Catania in results, got %q", got)
	}
	if strings.Contains(got, "Rome") {
		t.Fatalf("did not expect Rome in results, got %q", got)
	}
}

func TestHandleGeoSearch_ByRadius_FromMember(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoSearch, &shared.Command{
		Type: shared.CmdGeoSearch,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("FROMMEMBER"), []byte("Palermo"),
			[]byte("BYRADIUS"), []byte("200"), []byte("km"),
		},
	})
	if !strings.Contains(got, "Palermo") {
		t.Fatalf("expected Palermo in results, got %q", got)
	}
	if !strings.Contains(got, "Catania") {
		t.Fatalf("expected Catania in results, got %q", got)
	}
}

func TestHandleGeoSearch_ByBox(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoSearch, &shared.Command{
		Type: shared.CmdGeoSearch,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("FROMLONLAT"), []byte("13.361389"), []byte("38.115556"),
			[]byte("BYBOX"), []byte("400"), []byte("400"), []byte("km"),
		},
	})
	if !strings.Contains(got, "Palermo") {
		t.Fatalf("expected Palermo in results, got %q", got)
	}
	if !strings.Contains(got, "Catania") {
		t.Fatalf("expected Catania in results, got %q", got)
	}
}

func TestHandleGeoSearch_WithOptions(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoSearch, &shared.Command{
		Type: shared.CmdGeoSearch,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("FROMLONLAT"), []byte("13.361389"), []byte("38.115556"),
			[]byte("BYRADIUS"), []byte("200"), []byte("km"),
			[]byte("WITHCOORD"), []byte("WITHDIST"), []byte("WITHHASH"),
			[]byte("ASC"),
		},
	})
	if !strings.Contains(got, "Palermo") {
		t.Fatalf("expected Palermo, got %q", got)
	}
}

func TestHandleGeoSearch_ASC(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoSearch, &shared.Command{
		Type: shared.CmdGeoSearch,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("FROMLONLAT"), []byte("13.361389"), []byte("38.115556"),
			[]byte("BYRADIUS"), []byte("500"), []byte("km"),
			[]byte("ASC"),
		},
	})
	if !strings.Contains(got, "Palermo") || !strings.Contains(got, "Catania") || !strings.Contains(got, "Rome") {
		t.Fatalf("expected all three cities, got %q", got)
	}
	palermoIdx := strings.Index(got, "Palermo")
	cataniaIdx := strings.Index(got, "Catania")
	romeIdx := strings.Index(got, "Rome")
	if palermoIdx > cataniaIdx || cataniaIdx > romeIdx {
		t.Fatalf("expected ASC order: Palermo < Catania < Rome, got positions %d, %d, %d", palermoIdx, cataniaIdx, romeIdx)
	}
}

func TestHandleGeoSearch_DESC(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoSearch, &shared.Command{
		Type: shared.CmdGeoSearch,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("FROMLONLAT"), []byte("13.361389"), []byte("38.115556"),
			[]byte("BYRADIUS"), []byte("500"), []byte("km"),
			[]byte("DESC"),
		},
	})
	romeIdx := strings.Index(got, "Rome")
	palermoIdx := strings.Index(got, "Palermo")
	if romeIdx > palermoIdx {
		t.Fatalf("expected DESC order: Rome before Palermo, got positions %d, %d", romeIdx, palermoIdx)
	}
}

func TestHandleGeoSearch_Count(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoSearch, &shared.Command{
		Type: shared.CmdGeoSearch,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("FROMLONLAT"), []byte("13.361389"), []byte("38.115556"),
			[]byte("BYRADIUS"), []byte("500"), []byte("km"),
			[]byte("COUNT"), []byte("2"),
		},
	})
	if !strings.HasPrefix(got, "*2\r\n") {
		t.Fatalf("expected array of 2, got %q", got)
	}
}

func TestHandleGeoSearch_CountAny(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoSearch, &shared.Command{
		Type: shared.CmdGeoSearch,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("FROMLONLAT"), []byte("13.361389"), []byte("38.115556"),
			[]byte("BYRADIUS"), []byte("500"), []byte("km"),
			[]byte("COUNT"), []byte("2"), []byte("ANY"),
		},
	})
	if !strings.HasPrefix(got, "*2\r\n") {
		t.Fatalf("expected array of 2, got %q", got)
	}
}

func TestHandleGeoSearch_NonExistentKey(t *testing.T) {
	got := runHandler(handleGeoSearch, &shared.Command{
		Type: shared.CmdGeoSearch,
		Args: [][]byte{
			[]byte("nonexistent"),
			[]byte("FROMLONLAT"), []byte("0"), []byte("0"),
			[]byte("BYRADIUS"), []byte("100"), []byte("km"),
		},
	})
	if !strings.Contains(got, "*0") {
		t.Fatalf("expected empty array, got %q", got)
	}
}

func TestHandleGeoSearch_MissingFromClause(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoSearch, &shared.Command{
		Type: shared.CmdGeoSearch,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("BYRADIUS"), []byte("200"), []byte("km"),
		},
	})
	if !strings.Contains(got, "ERR") {
		t.Fatalf("expected error for missing FROM clause, got %q", got)
	}
}

func TestHandleGeoSearch_MissingByClause(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoSearch, &shared.Command{
		Type: shared.CmdGeoSearch,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("FROMLONLAT"), []byte("0"), []byte("0"),
		},
	})
	if !strings.Contains(got, "ERR") {
		t.Fatalf("expected error for missing BY clause, got %q", got)
	}
}

func TestHandleGeoSearch_AnyWithoutCount(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoSearch, &shared.Command{
		Type: shared.CmdGeoSearch,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("FROMLONLAT"), []byte("0"), []byte("0"),
			[]byte("BYRADIUS"), []byte("100"), []byte("km"),
			[]byte("ANY"),
		},
	})
	if !strings.Contains(got, "ERR") {
		t.Fatalf("expected error for ANY without COUNT, got %q", got)
	}
}

func TestHandleGeoSearch_WrongNumArgs(t *testing.T) {
	got := runHandler(handleGeoSearch, &shared.Command{
		Type: shared.CmdGeoSearch,
		Args: [][]byte{},
	})
	if !strings.Contains(got, "ERR") {
		t.Fatalf("expected error, got %q", got)
	}
}

func TestHandleGeoSearchStore(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoSearchStore, &shared.Command{
		Type: shared.CmdGeoSearchStore,
		Args: [][]byte{
			[]byte("destkey"), []byte("mygeo"),
			[]byte("FROMLONLAT"), []byte("13.361389"), []byte("38.115556"),
			[]byte("BYRADIUS"), []byte("200"), []byte("km"),
		},
	})
	if !strings.Contains(got, ":2") {
		t.Fatalf("expected 2 stored, got %q", got)
	}

	// Verify destination key exists via snapshot
	snap, _, _, err := eng.GetSnapshot("destkey")
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if snap == nil {
		t.Fatal("expected destkey to exist")
	}
}

func TestHandleGeoSearchStore_StoreDist(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoSearchStore, &shared.Command{
		Type: shared.CmdGeoSearchStore,
		Args: [][]byte{
			[]byte("destkey"), []byte("mygeo"),
			[]byte("FROMLONLAT"), []byte("13.361389"), []byte("38.115556"),
			[]byte("BYRADIUS"), []byte("200"), []byte("km"),
			[]byte("STOREDIST"),
		},
	})
	if !strings.Contains(got, ":2") {
		t.Fatalf("expected 2 stored, got %q", got)
	}

	snap, _, _, err := eng.GetSnapshot("destkey")
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if snap == nil {
		t.Fatal("expected destkey to exist")
	}
	score, found := geoMemberScore(snap, "Palermo")
	if !found {
		t.Fatal("expected Palermo in destkey")
	}
	if score > 1.0 {
		t.Fatalf("expected Palermo distance ~0, got %f", score)
	}
}

func TestHandleGeoSearchStore_EmptyResult(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoSearchStore, &shared.Command{
		Type: shared.CmdGeoSearchStore,
		Args: [][]byte{
			[]byte("destkey"), []byte("mygeo"),
			[]byte("FROMLONLAT"), []byte("0"), []byte("0"),
			[]byte("BYRADIUS"), []byte("1"), []byte("m"),
		},
	})
	if !strings.Contains(got, ":0") {
		t.Fatalf("expected 0 stored, got %q", got)
	}
}

func TestHandleGeoSearchStore_WithCoordError(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoSearchStore, &shared.Command{
		Type: shared.CmdGeoSearchStore,
		Args: [][]byte{
			[]byte("destkey"), []byte("mygeo"),
			[]byte("FROMLONLAT"), []byte("0"), []byte("0"),
			[]byte("BYRADIUS"), []byte("500"), []byte("km"),
			[]byte("WITHCOORD"),
		},
	})
	if !strings.Contains(got, "ERR") {
		t.Fatalf("expected error for STORE with WITHCOORD, got %q", got)
	}
}

func TestHandleGeoSearchStore_WrongNumArgs(t *testing.T) {
	got := runHandler(handleGeoSearchStore, &shared.Command{
		Type: shared.CmdGeoSearchStore,
		Args: [][]byte{[]byte("dest")},
	})
	if !strings.Contains(got, "ERR") {
		t.Fatalf("expected error, got %q", got)
	}
}

func TestHandleGeoRadius(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoRadius, &shared.Command{
		Type: shared.CmdGeoRadius,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("13.361389"), []byte("38.115556"),
			[]byte("200"), []byte("km"),
		},
	})
	if !strings.Contains(got, "Palermo") {
		t.Fatalf("expected Palermo in results, got %q", got)
	}
	if !strings.Contains(got, "Catania") {
		t.Fatalf("expected Catania in results, got %q", got)
	}
}

func TestHandleGeoRadius_WithStore(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoRadius, &shared.Command{
		Type: shared.CmdGeoRadius,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("13.361389"), []byte("38.115556"),
			[]byte("200"), []byte("km"),
			[]byte("STORE"), []byte("dest"),
		},
	})
	if !strings.Contains(got, ":2") {
		t.Fatalf("expected 2 stored, got %q", got)
	}
}

func TestHandleGeoRadius_WrongNumArgs(t *testing.T) {
	got := runHandler(handleGeoRadius, &shared.Command{
		Type: shared.CmdGeoRadius,
		Args: [][]byte{[]byte("mygeo"), []byte("0"), []byte("0")},
	})
	if !strings.Contains(got, "ERR") {
		t.Fatalf("expected error, got %q", got)
	}
}

func TestHandleGeoRadiusByMember(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoRadiusByMember, &shared.Command{
		Type: shared.CmdGeoRadiusByMember,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("Palermo"),
			[]byte("200"), []byte("km"),
		},
	})
	if !strings.Contains(got, "Palermo") {
		t.Fatalf("expected Palermo in results, got %q", got)
	}
	if !strings.Contains(got, "Catania") {
		t.Fatalf("expected Catania in results, got %q", got)
	}
}

func TestHandleGeoRadiusByMember_WithStore(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoRadiusByMember, &shared.Command{
		Type: shared.CmdGeoRadiusByMember,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("Palermo"),
			[]byte("200"), []byte("km"),
			[]byte("STOREDIST"), []byte("dest"),
		},
	})
	if !strings.Contains(got, ":2") {
		t.Fatalf("expected 2 stored, got %q", got)
	}
}

func TestHandleGeoRadiusByMember_WrongNumArgs(t *testing.T) {
	got := runHandler(handleGeoRadiusByMember, &shared.Command{
		Type: shared.CmdGeoRadiusByMember,
		Args: [][]byte{[]byte("mygeo"), []byte("member")},
	})
	if !strings.Contains(got, "ERR") {
		t.Fatalf("expected error, got %q", got)
	}
}

func TestHandleGeoRadius_RO_RejectsStore(t *testing.T) {
	got := runHandler(handleGeoRadius, &shared.Command{
		Type: shared.CmdGeoRadiusRO,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("13.361389"), []byte("38.115556"),
			[]byte("200"), []byte("km"),
			[]byte("STORE"), []byte("dest"),
		},
	})
	if !strings.Contains(got, "ERR") || !strings.Contains(got, "GEORADIUS_RO") {
		t.Fatalf("expected RO error, got %q", got)
	}
}

func TestHandleGeoRadius_RO_RejectsStoreDist(t *testing.T) {
	got := runHandler(handleGeoRadius, &shared.Command{
		Type: shared.CmdGeoRadiusRO,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("13.361389"), []byte("38.115556"),
			[]byte("200"), []byte("km"),
			[]byte("STOREDIST"), []byte("dest"),
		},
	})
	if !strings.Contains(got, "ERR") || !strings.Contains(got, "GEORADIUS_RO") {
		t.Fatalf("expected RO error, got %q", got)
	}
}

func TestHandleGeoRadius_RO_AllowsRead(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoRadius, &shared.Command{
		Type: shared.CmdGeoRadiusRO,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("13.361389"), []byte("38.115556"),
			[]byte("200"), []byte("km"),
		},
	})
	if !strings.Contains(got, "Palermo") {
		t.Fatalf("expected Palermo in results, got %q", got)
	}
}

func TestHandleGeoRadiusByMember_RO_RejectsStore(t *testing.T) {
	got := runHandler(handleGeoRadiusByMember, &shared.Command{
		Type: shared.CmdGeoRadiusByMemberRO,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("Palermo"),
			[]byte("200"), []byte("km"),
			[]byte("STORE"), []byte("dest"),
		},
	})
	if !strings.Contains(got, "ERR") || !strings.Contains(got, "GEORADIUSBYMEMBER_RO") {
		t.Fatalf("expected RO error, got %q", got)
	}
}

func TestHandleGeoRadiusByMember_RO_RejectsStoreDist(t *testing.T) {
	got := runHandler(handleGeoRadiusByMember, &shared.Command{
		Type: shared.CmdGeoRadiusByMemberRO,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("Palermo"),
			[]byte("200"), []byte("km"),
			[]byte("STOREDIST"), []byte("dest"),
		},
	})
	if !strings.Contains(got, "ERR") || !strings.Contains(got, "GEORADIUSBYMEMBER_RO") {
		t.Fatalf("expected RO error, got %q", got)
	}
}

func TestHandleGeoRadiusByMember_RO_AllowsRead(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	addThreeCities(t, eng, ctx)

	got := runHandlerWith(eng, ctx, handleGeoRadiusByMember, &shared.Command{
		Type: shared.CmdGeoRadiusByMemberRO,
		Args: [][]byte{
			[]byte("mygeo"),
			[]byte("Palermo"),
			[]byte("200"), []byte("km"),
		},
	})
	if !strings.Contains(got, "Palermo") {
		t.Fatalf("expected Palermo in results, got %q", got)
	}
}
