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

package hash

import (
	"strings"
	"testing"

	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

func TestHandleHScan(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1", "f2": "v2", "f3": "v3"})

	cmd := &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("0")}}
	resp := runHandlerWith(eng, ctx, handleHScan, cmd)
	// Should contain field-value pairs
	if !strings.Contains(resp, "f1") || !strings.Contains(resp, "v1") {
		t.Fatalf("expected f1/v1 in scan results, got %q", resp)
	}
}

func TestHandleHScan_NonExistent(t *testing.T) {
	resp := runHandler(handleHScan, &shared.Command{Args: [][]byte{[]byte("nokey"), []byte("0")}})
	// Should return cursor 0 and empty array
	if !strings.Contains(resp, "0") {
		t.Fatalf("expected cursor 0, got %q", resp)
	}
}

func TestHandleHScan_WithMatch(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"name": "alice", "age": "30", "nickname": "ali"})

	cmd := &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("0"), []byte("MATCH"), []byte("n*")}}
	resp := runHandlerWith(eng, ctx, handleHScan, cmd)
	if !strings.Contains(resp, "name") || !strings.Contains(resp, "nickname") {
		t.Fatalf("expected name and nickname in results, got %q", resp)
	}
	if strings.Contains(resp, "age") {
		t.Fatalf("expected age to be filtered out, got %q", resp)
	}
}

func TestHandleHRandField(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1", "f2": "v2", "f3": "v3"})

	// Single random field
	cmd := &shared.Command{Args: [][]byte{[]byte("myhash")}}
	resp := runHandlerWith(eng, ctx, handleHRandField, cmd)
	if !strings.Contains(resp, "f1") && !strings.Contains(resp, "f2") && !strings.Contains(resp, "f3") {
		t.Fatalf("expected one of f1/f2/f3, got %q", resp)
	}
}

func TestHandleHRandField_NonExistent(t *testing.T) {
	resp := runHandler(handleHRandField, &shared.Command{Args: [][]byte{[]byte("nokey")}})
	if !strings.Contains(resp, "$-1") {
		t.Fatalf("expected null, got %q", resp)
	}
}

func TestHandleHRandField_WithCount(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1", "f2": "v2", "f3": "v3"})

	// Positive count - distinct fields
	cmd := &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("2")}}
	resp := runHandlerWith(eng, ctx, handleHRandField, cmd)
	if !strings.Contains(resp, "*2") {
		t.Fatalf("expected array of 2, got %q", resp)
	}
}

func TestHandleHRandField_NegativeCount(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1"})

	// Negative count - allows repeats
	cmd := &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("-3")}}
	resp := runHandlerWith(eng, ctx, handleHRandField, cmd)
	if !strings.Contains(resp, "*3") {
		t.Fatalf("expected array of 3, got %q", resp)
	}
}

func TestHandleHRandField_WrongNumArgs(t *testing.T) {
	resp := runHandler(handleHRandField, &shared.Command{Args: [][]byte{}})
	if !strings.Contains(resp, "ERR") {
		t.Fatalf("expected error, got %q", resp)
	}
}
