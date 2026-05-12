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

package redis

import (
	"bytes"
	"testing"

	"github.com/swytchdb/swytch/redis/shared"
)

func TestBZPopMinNonBlocking(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := func() *shared.Connection { c := newEffectsConn(h); c.InScript = true; return c }()

	// Seed the sorted set with members: a=1, b=2, c=3
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
		[]byte("bzpopmin_key"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"),
	}}
	h.ExecuteInto(cmd, w, conn)
	if got := buf.String(); got != ":3\r\n" {
		t.Fatalf("ZADD seed got %q, want %q", got, ":3\r\n")
	}

	// BZPOPMIN should pop the minimum element (a with score 1)
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdBZPopMin, Args: [][]byte{[]byte("bzpopmin_key"), []byte("0")}}
	h.ExecuteInto(cmd, w, conn)

	expected := "*3\r\n$12\r\nbzpopmin_key\r\n$1\r\na\r\n$1\r\n1\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BZPOPMIN with data got %q, want %q", got, expected)
	}

	// BZPOPMIN on an empty/non-existent key should return null array
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdBZPopMin, Args: [][]byte{[]byte("bzpopmin_empty"), []byte("0")}}
	h.ExecuteInto(cmd, w, conn)

	expected = "*-1\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BZPOPMIN empty got %q, want %q", got, expected)
	}
}

func TestBZPopMaxNonBlocking(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := func() *shared.Connection { c := newEffectsConn(h); c.InScript = true; return c }()

	// Seed the sorted set with members: x=10, y=20, z=30
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
		[]byte("bzpopmax_key"), []byte("10"), []byte("x"), []byte("20"), []byte("y"), []byte("30"), []byte("z"),
	}}
	h.ExecuteInto(cmd, w, conn)
	if got := buf.String(); got != ":3\r\n" {
		t.Fatalf("ZADD seed got %q, want %q", got, ":3\r\n")
	}

	// BZPOPMAX should pop the maximum element (z with score 30)
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdBZPopMax, Args: [][]byte{[]byte("bzpopmax_key"), []byte("0")}}
	h.ExecuteInto(cmd, w, conn)

	expected := "*3\r\n$12\r\nbzpopmax_key\r\n$1\r\nz\r\n$2\r\n30\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BZPOPMAX with data got %q, want %q", got, expected)
	}

	// BZPOPMAX on an empty/non-existent key should return null array
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdBZPopMax, Args: [][]byte{[]byte("bzpopmax_empty"), []byte("0")}}
	h.ExecuteInto(cmd, w, conn)

	expected = "*-1\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BZPOPMAX empty got %q, want %q", got, expected)
	}
}

func TestBZMPopNonBlocking(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := func() *shared.Connection { c := newEffectsConn(h); c.InScript = true; return c }()

	// Seed the sorted set with members: m1=5, m2=10, m3=15
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
		[]byte("bzmpop_key"), []byte("5"), []byte("m1"), []byte("10"), []byte("m2"), []byte("15"), []byte("m3"),
	}}
	h.ExecuteInto(cmd, w, conn)
	if got := buf.String(); got != ":3\r\n" {
		t.Fatalf("ZADD seed got %q, want %q", got, ":3\r\n")
	}

	// BZMPOP 0 1 bzmpop_key MIN should pop the minimum element (m1 with score 5)
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdBZMPop, Args: [][]byte{
		[]byte("0"), []byte("1"), []byte("bzmpop_key"), []byte("MIN"),
	}}
	h.ExecuteInto(cmd, w, conn)

	// Expected: *2\r\n$10\r\nbzmpop_key\r\n*1\r\n*2\r\n$2\r\nm1\r\n$1\r\n5\r\n
	expected := "*2\r\n$10\r\nbzmpop_key\r\n*1\r\n*2\r\n$2\r\nm1\r\n$1\r\n5\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BZMPOP MIN with data got %q, want %q", got, expected)
	}

	// BZMPOP on an empty/non-existent key should return null array
	buf.Reset()
	cmd = &shared.Command{Type: shared.CmdBZMPop, Args: [][]byte{
		[]byte("0"), []byte("1"), []byte("bzmpop_empty"), []byte("MIN"),
	}}
	h.ExecuteInto(cmd, w, conn)

	expected = "*-1\r\n"
	if got := buf.String(); got != expected {
		t.Errorf("BZMPOP empty got %q, want %q", got, expected)
	}
}
