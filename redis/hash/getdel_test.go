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

	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

func TestHandleHGetDel(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1", "f2": "v2", "f3": "v3"})

	cmd := &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("FIELDS"), []byte("2"),
		[]byte("f1"), []byte("f2"),
	}}
	resp := runHandlerWith(eng, ctx, handleHGetDel, cmd)
	if !strings.Contains(resp, "v1") || !strings.Contains(resp, "v2") {
		t.Fatalf("expected v1 and v2, got %q", resp)
	}

	// Verify fields were deleted
	cmd = &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("f1")}}
	resp = runHandlerWith(eng, ctx, handleHExists, cmd)
	if !strings.Contains(resp, ":0") {
		t.Fatalf("expected f1 to be deleted, got %q", resp)
	}

	// Verify f3 still exists
	cmd = &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("f3")}}
	resp = runHandlerWith(eng, ctx, handleHExists, cmd)
	if !strings.Contains(resp, ":1") {
		t.Fatalf("expected f3 to still exist, got %q", resp)
	}
}

func TestHandleHGetDel_NonExistent(t *testing.T) {
	resp := runHandler(handleHGetDel, &shared.Command{Args: [][]byte{
		[]byte("nokey"), []byte("FIELDS"), []byte("1"),
		[]byte("f1"),
	}})
	if !strings.Contains(resp, "$-1") {
		t.Fatalf("expected null, got %q", resp)
	}
}

func TestHandleHGetDel_WrongNumArgs(t *testing.T) {
	resp := runHandler(handleHGetDel, &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("FIELDS")}})
	if !strings.Contains(resp, "ERR") {
		t.Fatalf("expected error, got %q", resp)
	}
}

func TestHandleHGetDel_MissingFieldsKeyword(t *testing.T) {
	resp := runHandler(handleHGetDel, &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("NOTFIELDS"), []byte("1"), []byte("f1")}})
	if !strings.Contains(resp, "ERR") {
		t.Fatalf("expected error, got %q", resp)
	}
}
