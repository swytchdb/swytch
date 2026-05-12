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

// seedZSet adds members a-e with scores 1-5 to "myzset" via ZADD.
func seedZSet(t *testing.T, h *Handler, conn *shared.Connection) {
	t.Helper()
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	cmd := &shared.Command{
		Type: shared.CmdZAdd,
		Args: [][]byte{
			[]byte("myzset"),
			[]byte("1"), []byte("a"),
			[]byte("2"), []byte("b"),
			[]byte("3"), []byte("c"),
			[]byte("4"), []byte("d"),
			[]byte("5"), []byte("e"),
		},
	}
	h.ExecuteInto(cmd, w, conn)
	if got := buf.String(); got != ":5\r\n" {
		t.Fatalf("ZADD seed got %q, want %q", got, ":5\r\n")
	}
}

// ---------- ZRANGE ----------

func TestZRange(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)
	seedZSet(t, h, conn)

	t.Run("all by index", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{
			Type: shared.CmdZRange,
			Args: [][]byte{[]byte("myzset"), []byte("0"), []byte("-1")},
		}
		h.ExecuteInto(cmd, w, conn)
		expected := "*5\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("got %q, want %q", got, expected)
		}
	})

	t.Run("subset by index", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{
			Type: shared.CmdZRange,
			Args: [][]byte{[]byte("myzset"), []byte("1"), []byte("2")},
		}
		h.ExecuteInto(cmd, w, conn)
		expected := "*2\r\n$1\r\nb\r\n$1\r\nc\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("got %q, want %q", got, expected)
		}
	})

	t.Run("with WITHSCORES", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{
			Type: shared.CmdZRange,
			Args: [][]byte{[]byte("myzset"), []byte("0"), []byte("-1"), []byte("WITHSCORES")},
		}
		h.ExecuteInto(cmd, w, conn)
		expected := "*10\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\nd\r\n$1\r\n4\r\n$1\r\ne\r\n$1\r\n5\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("got %q, want %q", got, expected)
		}
	})

	t.Run("empty result on nonexistent key", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{
			Type: shared.CmdZRange,
			Args: [][]byte{[]byte("nosuchkey"), []byte("0"), []byte("-1")},
		}
		h.ExecuteInto(cmd, w, conn)
		expected := "*0\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("got %q, want %q", got, expected)
		}
	})
}

// ---------- ZREVRANGE ----------

func TestZRevRange(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)
	seedZSet(t, h, conn)

	t.Run("all reversed", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{
			Type: shared.CmdZRevRange,
			Args: [][]byte{[]byte("myzset"), []byte("0"), []byte("-1")},
		}
		h.ExecuteInto(cmd, w, conn)
		expected := "*5\r\n$1\r\ne\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("got %q, want %q", got, expected)
		}
	})

	t.Run("with WITHSCORES", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{
			Type: shared.CmdZRevRange,
			Args: [][]byte{[]byte("myzset"), []byte("0"), []byte("-1"), []byte("WITHSCORES")},
		}
		h.ExecuteInto(cmd, w, conn)
		expected := "*10\r\n$1\r\ne\r\n$1\r\n5\r\n$1\r\nd\r\n$1\r\n4\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\na\r\n$1\r\n1\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("got %q, want %q", got, expected)
		}
	})
}

// ---------- ZRANGEBYSCORE ----------

func TestZRangeByScore(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)
	seedZSet(t, h, conn)

	t.Run("all with -inf +inf", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{
			Type: shared.CmdZRangeByScore,
			Args: [][]byte{[]byte("myzset"), []byte("-inf"), []byte("+inf")},
		}
		h.ExecuteInto(cmd, w, conn)
		expected := "*5\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("got %q, want %q", got, expected)
		}
	})

	t.Run("score range 1 to 3", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{
			Type: shared.CmdZRangeByScore,
			Args: [][]byte{[]byte("myzset"), []byte("1"), []byte("3")},
		}
		h.ExecuteInto(cmd, w, conn)
		expected := "*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("got %q, want %q", got, expected)
		}
	})

	t.Run("exclusive min", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{
			Type: shared.CmdZRangeByScore,
			Args: [][]byte{[]byte("myzset"), []byte("(1"), []byte("3")},
		}
		h.ExecuteInto(cmd, w, conn)
		// Exclusive 1 means score > 1, so b(2) and c(3) only
		expected := "*2\r\n$1\r\nb\r\n$1\r\nc\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("got %q, want %q", got, expected)
		}
	})

	t.Run("with LIMIT", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{
			Type: shared.CmdZRangeByScore,
			Args: [][]byte{
				[]byte("myzset"), []byte("-inf"), []byte("+inf"),
				[]byte("LIMIT"), []byte("1"), []byte("2"),
			},
		}
		h.ExecuteInto(cmd, w, conn)
		// offset 1, count 2 => b, c
		expected := "*2\r\n$1\r\nb\r\n$1\r\nc\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("got %q, want %q", got, expected)
		}
	})

	t.Run("with WITHSCORES", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{
			Type: shared.CmdZRangeByScore,
			Args: [][]byte{
				[]byte("myzset"), []byte("2"), []byte("4"),
				[]byte("WITHSCORES"),
			},
		}
		h.ExecuteInto(cmd, w, conn)
		// b(2), c(3), d(4) with scores interleaved
		expected := "*6\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\nd\r\n$1\r\n4\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("got %q, want %q", got, expected)
		}
	})
}

// ---------- ZREVRANGEBYSCORE ----------

func TestZRevRangeByScore(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)
	seedZSet(t, h, conn)

	t.Run("all reversed with +inf -inf", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{
			Type: shared.CmdZRevRangeByScore,
			Args: [][]byte{[]byte("myzset"), []byte("+inf"), []byte("-inf")},
		}
		h.ExecuteInto(cmd, w, conn)
		expected := "*5\r\n$1\r\ne\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("got %q, want %q", got, expected)
		}
	})

	t.Run("with LIMIT and WITHSCORES", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{
			Type: shared.CmdZRevRangeByScore,
			Args: [][]byte{
				[]byte("myzset"), []byte("+inf"), []byte("-inf"),
				[]byte("WITHSCORES"),
				[]byte("LIMIT"), []byte("0"), []byte("3"),
			},
		}
		h.ExecuteInto(cmd, w, conn)
		// top 3 in reverse: e(5), d(4), c(3) with scores
		expected := "*6\r\n$1\r\ne\r\n$1\r\n5\r\n$1\r\nd\r\n$1\r\n4\r\n$1\r\nc\r\n$1\r\n3\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("got %q, want %q", got, expected)
		}
	})
}
