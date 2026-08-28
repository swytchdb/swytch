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

package set

import (
	"strings"
	"testing"

	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

func seedTestSets(eng *effects.Engine, ctx *effects.Context) {
	// s1 = {a, b, c}
	runHandlerWith(eng, ctx, handleSAdd, &shared.Command{
		Args: [][]byte{[]byte("s1"), []byte("a"), []byte("b"), []byte("c")},
	})
	// s2 = {b, c, d}
	runHandlerWith(eng, ctx, handleSAdd, &shared.Command{
		Args: [][]byte{[]byte("s2"), []byte("b"), []byte("c"), []byte("d")},
	})
}

func TestHandleSInter(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedTestSets(eng, ctx)

	resp := runHandlerWith(eng, ctx, handleSInter, &shared.Command{
		Args: [][]byte{[]byte("s1"), []byte("s2")},
	})
	// Intersection of {a,b,c} and {b,c,d} = {b,c}
	if !strings.Contains(resp, "*2") {
		t.Fatalf("expected array of 2, got %q", resp)
	}
}

func TestHandleSInter_EmptySet(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedTestSets(eng, ctx)

	resp := runHandlerWith(eng, ctx, handleSInter, &shared.Command{
		Args: [][]byte{[]byte("s1"), []byte("nokey")},
	})
	if !strings.Contains(resp, "*0") {
		t.Fatalf("expected empty array, got %q", resp)
	}
}

func TestHandleSInterStore(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedTestSets(eng, ctx)

	resp := runHandlerWith(eng, ctx, handleSInterStore, &shared.Command{
		Args: [][]byte{[]byte("dest"), []byte("s1"), []byte("s2")},
	})
	if resp != ":2\r\n" {
		t.Fatalf("expected :2\\r\\n, got %q", resp)
	}

	// Verify dest cardinality
	resp = runHandlerWith(eng, ctx, handleSCard, &shared.Command{
		Args: [][]byte{[]byte("dest")},
	})
	if resp != ":2\r\n" {
		t.Fatalf("expected dest cardinality 2, got %q", resp)
	}
}

func TestHandleSInterStore_EmptyResult(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedTestSets(eng, ctx)

	resp := runHandlerWith(eng, ctx, handleSInterStore, &shared.Command{
		Args: [][]byte{[]byte("dest"), []byte("s1"), []byte("nokey")},
	})
	if resp != ":0\r\n" {
		t.Fatalf("expected :0\\r\\n, got %q", resp)
	}
}

func TestHandleSInterCard(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedTestSets(eng, ctx)

	resp := runHandlerWith(eng, ctx, handleSInterCard, &shared.Command{
		Args: [][]byte{[]byte("2"), []byte("s1"), []byte("s2")},
	})
	if resp != ":2\r\n" {
		t.Fatalf("expected :2\\r\\n, got %q", resp)
	}
}

func TestHandleSInterCard_WithLimit(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedTestSets(eng, ctx)

	resp := runHandlerWith(eng, ctx, handleSInterCard, &shared.Command{
		Args: [][]byte{[]byte("2"), []byte("s1"), []byte("s2"), []byte("LIMIT"), []byte("1")},
	})
	if resp != ":1\r\n" {
		t.Fatalf("expected :1\\r\\n, got %q", resp)
	}
}

func TestHandleSUnion(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedTestSets(eng, ctx)

	resp := runHandlerWith(eng, ctx, handleSUnion, &shared.Command{
		Args: [][]byte{[]byte("s1"), []byte("s2")},
	})
	// Union of {a,b,c} and {b,c,d} = {a,b,c,d}
	if !strings.Contains(resp, "*4") {
		t.Fatalf("expected array of 4, got %q", resp)
	}
}

func TestHandleSUnion_WithNonExistent(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedTestSets(eng, ctx)

	resp := runHandlerWith(eng, ctx, handleSUnion, &shared.Command{
		Args: [][]byte{[]byte("s1"), []byte("nokey")},
	})
	if !strings.Contains(resp, "*3") {
		t.Fatalf("expected array of 3, got %q", resp)
	}
}

func TestHandleSUnionStore(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedTestSets(eng, ctx)

	resp := runHandlerWith(eng, ctx, handleSUnionStore, &shared.Command{
		Args: [][]byte{[]byte("dest"), []byte("s1"), []byte("s2")},
	})
	if resp != ":4\r\n" {
		t.Fatalf("expected :4\\r\\n, got %q", resp)
	}

	resp = runHandlerWith(eng, ctx, handleSCard, &shared.Command{
		Args: [][]byte{[]byte("dest")},
	})
	if resp != ":4\r\n" {
		t.Fatalf("expected dest cardinality 4, got %q", resp)
	}
}

func TestHandleSDiff(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedTestSets(eng, ctx)

	resp := runHandlerWith(eng, ctx, handleSDiff, &shared.Command{
		Args: [][]byte{[]byte("s1"), []byte("s2")},
	})
	// Diff of {a,b,c} - {b,c,d} = {a}
	if !strings.Contains(resp, "*1") {
		t.Fatalf("expected array of 1, got %q", resp)
	}
}

func TestHandleSDiff_NonExistentFirst(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedTestSets(eng, ctx)

	resp := runHandlerWith(eng, ctx, handleSDiff, &shared.Command{
		Args: [][]byte{[]byte("nokey"), []byte("s1")},
	})
	if !strings.Contains(resp, "*0") {
		t.Fatalf("expected empty array, got %q", resp)
	}
}

func TestHandleSDiffStore(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedTestSets(eng, ctx)

	resp := runHandlerWith(eng, ctx, handleSDiffStore, &shared.Command{
		Args: [][]byte{[]byte("dest"), []byte("s1"), []byte("s2")},
	})
	if resp != ":1\r\n" {
		t.Fatalf("expected :1\\r\\n, got %q", resp)
	}

	resp = runHandlerWith(eng, ctx, handleSCard, &shared.Command{
		Args: [][]byte{[]byte("dest")},
	})
	if resp != ":1\r\n" {
		t.Fatalf("expected dest cardinality 1, got %q", resp)
	}
}

func TestHandleSDiffStore_EmptyResult(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedTestSets(eng, ctx)

	// s1 diff s1 = empty
	resp := runHandlerWith(eng, ctx, handleSDiffStore, &shared.Command{
		Args: [][]byte{[]byte("dest"), []byte("s1"), []byte("s1")},
	})
	if resp != ":0\r\n" {
		t.Fatalf("expected :0\\r\\n, got %q", resp)
	}
}
