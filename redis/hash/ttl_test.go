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
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/redis/shared"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// seedHashWithTTL creates a hash and sets per-element TTL via effects
func seedHashWithTTL(eng *effects.Engine, ctx *effects.Context, key string, fields map[string]string, ttlField string, expiresAtMs uint64) {
	seedHash(eng, ctx, key, fields)
	_ = ctx.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{ElementId: []byte(ttlField), ExpiresAt: timestamppb.New(time.UnixMilli(int64(expiresAtMs)))}},
	})
	_ = ctx.Flush()
}

// --- HExpire ---

func TestHandleHExpire(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1", "f2": "v2"})

	cmd := &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("100"),
		[]byte("FIELDS"), []byte("1"), []byte("f1"),
	}}
	resp := runHandlerWith(eng, ctx, handleHExpire, cmd)
	if !strings.Contains(resp, ":1") {
		t.Fatalf("expected 1 (success), got %q", resp)
	}
}

func TestHandleHExpire_NonExistentKey(t *testing.T) {
	resp := runHandler(handleHExpire, &shared.Command{Args: [][]byte{
		[]byte("nokey"), []byte("100"),
		[]byte("FIELDS"), []byte("1"), []byte("f1"),
	}})
	if !strings.Contains(resp, ":-2") {
		t.Fatalf("expected -2, got %q", resp)
	}
}

func TestHandleHExpire_ZeroSeconds(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1"})

	cmd := &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("0"),
		[]byte("FIELDS"), []byte("1"), []byte("f1"),
	}}
	resp := runHandlerWith(eng, ctx, handleHExpire, cmd)
	// Field should be deleted
	if !strings.Contains(resp, ":2") {
		t.Fatalf("expected 2 (deleted), got %q", resp)
	}
}

func TestHandleHExpire_WrongNumArgs(t *testing.T) {
	resp := runHandler(handleHExpire, &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("100")}})
	if !strings.Contains(resp, "ERR") {
		t.Fatalf("expected error, got %q", resp)
	}
}

// --- HPExpire ---

func TestHandleHPExpire(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1"})

	cmd := &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("100000"),
		[]byte("FIELDS"), []byte("1"), []byte("f1"),
	}}
	resp := runHandlerWith(eng, ctx, handleHPExpire, cmd)
	if !strings.Contains(resp, ":1") {
		t.Fatalf("expected 1 (success), got %q", resp)
	}
}

// --- HExpireAt ---

func TestHandleHExpireAt(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1"})

	cmd := &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("9999999999"),
		[]byte("FIELDS"), []byte("1"), []byte("f1"),
	}}
	resp := runHandlerWith(eng, ctx, handleHExpireAt, cmd)
	if !strings.Contains(resp, ":1") {
		t.Fatalf("expected 1 (success), got %q", resp)
	}
}

func TestHandleHExpireAt_PastTime(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1"})

	cmd := &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("1"), // Unix timestamp 1 = in the past
		[]byte("FIELDS"), []byte("1"), []byte("f1"),
	}}
	resp := runHandlerWith(eng, ctx, handleHExpireAt, cmd)
	// Field should be deleted since timestamp is in the past
	if !strings.Contains(resp, ":2") {
		t.Fatalf("expected 2 (deleted), got %q", resp)
	}
}

// --- HPExpireAt ---

func TestHandleHPExpireAt(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1"})

	cmd := &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("9999999999999"),
		[]byte("FIELDS"), []byte("1"), []byte("f1"),
	}}
	resp := runHandlerWith(eng, ctx, handleHPExpireAt, cmd)
	if !strings.Contains(resp, ":1") {
		t.Fatalf("expected 1 (success), got %q", resp)
	}
}

// --- HTTL ---

func TestHandleHTTL(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	// Set a TTL of 60 seconds from now
	expiresAtMs := uint64(9999999999000) // far future
	seedHashWithTTL(eng, ctx, "myhash", map[string]string{"f1": "v1"}, "f1", expiresAtMs)

	cmd := &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("FIELDS"), []byte("1"), []byte("f1"),
	}}
	resp := runHandlerWith(eng, ctx, handleHTTL, cmd)
	// TTL should be positive
	if strings.Contains(resp, ":-1") || strings.Contains(resp, ":-2") {
		t.Fatalf("expected positive TTL, got %q", resp)
	}
}

func TestHandleHTTL_NoTTL(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1"})

	cmd := &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("FIELDS"), []byte("1"), []byte("f1"),
	}}
	resp := runHandlerWith(eng, ctx, handleHTTL, cmd)
	if !strings.Contains(resp, ":-1") {
		t.Fatalf("expected -1 (no TTL), got %q", resp)
	}
}

func TestHandleHTTL_NonExistentKey(t *testing.T) {
	resp := runHandler(handleHTTL, &shared.Command{Args: [][]byte{
		[]byte("nokey"), []byte("FIELDS"), []byte("1"), []byte("f1"),
	}})
	if !strings.Contains(resp, ":-2") {
		t.Fatalf("expected -2, got %q", resp)
	}
}

// --- HPTTL ---

func TestHandleHPTTL(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	expiresAtMs := uint64(9999999999000)
	seedHashWithTTL(eng, ctx, "myhash", map[string]string{"f1": "v1"}, "f1", expiresAtMs)

	cmd := &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("FIELDS"), []byte("1"), []byte("f1"),
	}}
	resp := runHandlerWith(eng, ctx, handleHPTTL, cmd)
	if strings.Contains(resp, ":-1") || strings.Contains(resp, ":-2") {
		t.Fatalf("expected positive TTL in ms, got %q", resp)
	}
}

// --- HExpireTime ---

func TestHandleHExpireTime(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	expiresAtMs := uint64(9999999999000)
	seedHashWithTTL(eng, ctx, "myhash", map[string]string{"f1": "v1"}, "f1", expiresAtMs)

	cmd := &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("FIELDS"), []byte("1"), []byte("f1"),
	}}
	resp := runHandlerWith(eng, ctx, handleHExpireTime, cmd)
	if strings.Contains(resp, ":-1") || strings.Contains(resp, ":-2") {
		t.Fatalf("expected positive expire time, got %q", resp)
	}
}

// --- HPExpireTime ---

func TestHandleHPExpireTime(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	expiresAtMs := uint64(9999999999000)
	seedHashWithTTL(eng, ctx, "myhash", map[string]string{"f1": "v1"}, "f1", expiresAtMs)

	cmd := &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("FIELDS"), []byte("1"), []byte("f1"),
	}}
	resp := runHandlerWith(eng, ctx, handleHPExpireTime, cmd)
	if strings.Contains(resp, ":-1") || strings.Contains(resp, ":-2") {
		t.Fatalf("expected positive expire time in ms, got %q", resp)
	}
}

// --- HPersist ---

func TestHandleHPersist(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	expiresAtMs := uint64(9999999999000)
	seedHashWithTTL(eng, ctx, "myhash", map[string]string{"f1": "v1"}, "f1", expiresAtMs)

	cmd := &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("FIELDS"), []byte("1"), []byte("f1"),
	}}
	resp := runHandlerWith(eng, ctx, handleHPersist, cmd)
	if !strings.Contains(resp, ":1") {
		t.Fatalf("expected 1 (success), got %q", resp)
	}

	// Verify TTL was removed
	cmd = &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("FIELDS"), []byte("1"), []byte("f1"),
	}}
	resp = runHandlerWith(eng, ctx, handleHTTL, cmd)
	if !strings.Contains(resp, ":-1") {
		t.Fatalf("expected -1 (no TTL), got %q", resp)
	}
}

func TestHandleHPersist_NoTTL(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1"})

	cmd := &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("FIELDS"), []byte("1"), []byte("f1"),
	}}
	resp := runHandlerWith(eng, ctx, handleHPersist, cmd)
	// -1 means field exists but has no TTL
	if !strings.Contains(resp, ":-1") {
		t.Fatalf("expected -1 (no TTL to remove), got %q", resp)
	}
}

// --- HSetEx ---

func TestHandleHSetEx(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	cmd := &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("EX"), []byte("100"),
		[]byte("FIELDS"), []byte("2"),
		[]byte("f1"), []byte("v1"),
		[]byte("f2"), []byte("v2"),
	}}
	resp := runHandlerWith(eng, ctx, handleHSetEx, cmd)
	if !strings.Contains(resp, ":1") {
		t.Fatalf("expected 1, got %q", resp)
	}

	// Verify fields were set
	cmd = &shared.Command{Args: [][]byte{[]byte("myhash"), []byte("f1")}}
	resp = runHandlerWith(eng, ctx, handleHGet, cmd)
	if !strings.Contains(resp, "v1") {
		t.Fatalf("expected v1, got %q", resp)
	}
}

func TestHandleHSetEx_FNX(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "existing"})

	// FNX should fail because f1 already exists
	cmd := &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("FNX"),
		[]byte("FIELDS"), []byte("1"),
		[]byte("f1"), []byte("newval"),
	}}
	resp := runHandlerWith(eng, ctx, handleHSetEx, cmd)
	if !strings.Contains(resp, ":0") {
		t.Fatalf("expected 0 (FNX failed), got %q", resp)
	}
}

func TestHandleHSetEx_FXX(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()

	// FXX should fail because hash doesn't exist
	cmd := &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("FXX"),
		[]byte("FIELDS"), []byte("1"),
		[]byte("f1"), []byte("v1"),
	}}
	resp := runHandlerWith(eng, ctx, handleHSetEx, cmd)
	if !strings.Contains(resp, ":0") {
		t.Fatalf("expected 0 (FXX failed), got %q", resp)
	}
}

func TestHandleHSetEx_WrongNumArgs(t *testing.T) {
	resp := runHandler(handleHSetEx, &shared.Command{Args: [][]byte{[]byte("myhash")}})
	if !strings.Contains(resp, "ERR") {
		t.Fatalf("expected error, got %q", resp)
	}
}

// --- HGetEx ---

func TestHandleHGetEx(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	seedHash(eng, ctx, "myhash", map[string]string{"f1": "v1", "f2": "v2"})

	cmd := &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("EX"), []byte("100"),
		[]byte("FIELDS"), []byte("2"),
		[]byte("f1"), []byte("f2"),
	}}
	resp := runHandlerWith(eng, ctx, handleHGetEx, cmd)
	if !strings.Contains(resp, "v1") || !strings.Contains(resp, "v2") {
		t.Fatalf("expected v1 and v2, got %q", resp)
	}
}

func TestHandleHGetEx_Persist(t *testing.T) {
	eng := effects.NewTestEngine()
	ctx := eng.NewContext()
	expiresAtMs := uint64(9999999999000)
	seedHashWithTTL(eng, ctx, "myhash", map[string]string{"f1": "v1"}, "f1", expiresAtMs)

	cmd := &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("PERSIST"),
		[]byte("FIELDS"), []byte("1"), []byte("f1"),
	}}
	resp := runHandlerWith(eng, ctx, handleHGetEx, cmd)
	if !strings.Contains(resp, "v1") {
		t.Fatalf("expected v1, got %q", resp)
	}

	// Verify TTL was removed
	cmd = &shared.Command{Args: [][]byte{
		[]byte("myhash"), []byte("FIELDS"), []byte("1"), []byte("f1"),
	}}
	resp = runHandlerWith(eng, ctx, handleHTTL, cmd)
	if !strings.Contains(resp, ":-1") {
		t.Fatalf("expected -1 (no TTL), got %q", resp)
	}
}

func TestHandleHGetEx_NonExistent(t *testing.T) {
	resp := runHandler(handleHGetEx, &shared.Command{Args: [][]byte{
		[]byte("nokey"),
		[]byte("FIELDS"), []byte("1"), []byte("f1"),
	}})
	if !strings.Contains(resp, "$-1") {
		t.Fatalf("expected null, got %q", resp)
	}
}

func TestHandleHGetEx_WrongNumArgs(t *testing.T) {
	resp := runHandler(handleHGetEx, &shared.Command{Args: [][]byte{[]byte("myhash")}})
	if !strings.Contains(resp, "ERR") {
		t.Fatalf("expected error, got %q", resp)
	}
}

// --- parseHExpireArgs ---

func TestParseHExpireArgs(t *testing.T) {
	// Valid: NX FIELDS 1 f1
	nx, xx, gt, lt, fields, err := parseHExpireArgs([][]byte{
		[]byte("NX"), []byte("FIELDS"), []byte("1"), []byte("f1"),
	}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !nx || xx || gt || lt {
		t.Fatal("expected only NX set")
	}
	if len(fields) != 1 || fields[0] != "f1" {
		t.Fatalf("expected [f1], got %v", fields)
	}

	// Multiple condition flags
	_, _, _, _, _, err = parseHExpireArgs([][]byte{
		[]byte("NX"), []byte("XX"), []byte("FIELDS"), []byte("1"), []byte("f1"),
	}, 0)
	if err == nil {
		t.Fatal("expected error for multiple condition flags")
	}
}

// --- parseHTTLArgs ---

func TestParseHTTLArgs(t *testing.T) {
	// Valid: key FIELDS 2 f1 f2 (args without key)
	fields, err := parseHTTLArgs([][]byte{
		[]byte("key"), []byte("FIELDS"), []byte("2"), []byte("f1"), []byte("f2"),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(fields) != 2 || fields[0] != "f1" || fields[1] != "f2" {
		t.Fatalf("expected [f1 f2], got %v", fields)
	}

	// Missing FIELDS keyword
	_, err = parseHTTLArgs([][]byte{
		[]byte("key"), []byte("NOTFIELDS"), []byte("1"), []byte("f1"),
	})
	if err == nil {
		t.Fatal("expected error for missing FIELDS keyword")
	}
}
