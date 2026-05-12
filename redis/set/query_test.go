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

	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

func TestHandleSCard(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleSAdd, &shared.Command{
		Args: [][]byte{[]byte("myset"), []byte("a"), []byte("b"), []byte("c")},
	})

	resp := runHandlerWith(eng, ctx, handleSCard, &shared.Command{
		Args: [][]byte{[]byte("myset")},
	})
	if resp != ":3\r\n" {
		t.Fatalf("expected :3\\r\\n, got %q", resp)
	}
}

func TestHandleSCard_NonExistent(t *testing.T) {
	resp := runHandler(handleSCard, &shared.Command{
		Args: [][]byte{[]byte("nokey")},
	})
	if resp != ":0\r\n" {
		t.Fatalf("expected :0\\r\\n, got %q", resp)
	}
}

func TestHandleSIsMember(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleSAdd, &shared.Command{
		Args: [][]byte{[]byte("myset"), []byte("a"), []byte("b")},
	})

	// Member exists
	resp := runHandlerWith(eng, ctx, handleSIsMember, &shared.Command{
		Args: [][]byte{[]byte("myset"), []byte("a")},
	})
	if resp != ":1\r\n" {
		t.Fatalf("expected :1\\r\\n, got %q", resp)
	}

	// Member does not exist
	resp = runHandlerWith(eng, ctx, handleSIsMember, &shared.Command{
		Args: [][]byte{[]byte("myset"), []byte("z")},
	})
	if resp != ":0\r\n" {
		t.Fatalf("expected :0\\r\\n, got %q", resp)
	}
}

func TestHandleSIsMember_NonExistent(t *testing.T) {
	resp := runHandler(handleSIsMember, &shared.Command{
		Args: [][]byte{[]byte("nokey"), []byte("a")},
	})
	if resp != ":0\r\n" {
		t.Fatalf("expected :0\\r\\n, got %q", resp)
	}
}

func TestHandleSMIsMember(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleSAdd, &shared.Command{
		Args: [][]byte{[]byte("myset"), []byte("a"), []byte("b")},
	})

	resp := runHandlerWith(eng, ctx, handleSMIsMember, &shared.Command{
		Args: [][]byte{[]byte("myset"), []byte("a"), []byte("z"), []byte("b")},
	})
	if !strings.Contains(resp, "*3") {
		t.Fatalf("expected array of 3, got %q", resp)
	}
}

func TestHandleSMIsMember_NonExistent(t *testing.T) {
	resp := runHandler(handleSMIsMember, &shared.Command{
		Args: [][]byte{[]byte("nokey"), []byte("a"), []byte("b")},
	})
	if !strings.Contains(resp, "*2") {
		t.Fatalf("expected array of 2, got %q", resp)
	}
}

func TestHandleSMembers(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleSAdd, &shared.Command{
		Args: [][]byte{[]byte("myset"), []byte("a"), []byte("b")},
	})

	resp := runHandlerWith(eng, ctx, handleSMembers, &shared.Command{
		Args: [][]byte{[]byte("myset")},
	})
	if !strings.Contains(resp, "*2") {
		t.Fatalf("expected array of 2, got %q", resp)
	}
}

func TestHandleSMembers_Empty(t *testing.T) {
	resp := runHandler(handleSMembers, &shared.Command{
		Args: [][]byte{[]byte("nokey")},
	})
	if !strings.Contains(resp, "*0") {
		t.Fatalf("expected empty array, got %q", resp)
	}
}

func TestHandleSRandMember(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleSAdd, &shared.Command{
		Args: [][]byte{[]byte("myset"), []byte("a"), []byte("b"), []byte("c")},
	})

	// Without count - returns single bulk string
	resp := runHandlerWith(eng, ctx, handleSRandMember, &shared.Command{
		Args: [][]byte{[]byte("myset")},
	})
	if !strings.Contains(resp, "$1") {
		t.Fatalf("expected bulk string, got %q", resp)
	}
}

func TestHandleSRandMember_PositiveCount(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleSAdd, &shared.Command{
		Args: [][]byte{[]byte("myset"), []byte("a"), []byte("b"), []byte("c")},
	})

	resp := runHandlerWith(eng, ctx, handleSRandMember, &shared.Command{
		Args: [][]byte{[]byte("myset"), []byte("2")},
	})
	if !strings.Contains(resp, "*2") {
		t.Fatalf("expected array of 2, got %q", resp)
	}
}

func TestHandleSRandMember_NegativeCount(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	runHandlerWith(eng, ctx, handleSAdd, &shared.Command{
		Args: [][]byte{[]byte("myset"), []byte("a")},
	})

	resp := runHandlerWith(eng, ctx, handleSRandMember, &shared.Command{
		Args: [][]byte{[]byte("myset"), []byte("-3")},
	})
	if !strings.Contains(resp, "*3") {
		t.Fatalf("expected array of 3 (with repeats), got %q", resp)
	}
}

func TestHandleSRandMember_NonExistent(t *testing.T) {
	resp := runHandler(handleSRandMember, &shared.Command{
		Args: [][]byte{[]byte("nokey")},
	})
	if !strings.Contains(resp, "$-1") {
		t.Fatalf("expected null bulk string, got %q", resp)
	}
}
