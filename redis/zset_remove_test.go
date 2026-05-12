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

func TestZRem(t *testing.T) {
	t.Run("remove single member", func(t *testing.T) {
		h := newHandlerWithEffects()
		defer h.Close()
		conn := newEffectsConn(h)

		// Seed: ZADD zrem1 1 a 2 b 3 c
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zrem1"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}}, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Fatalf("ZADD seed: got %q, want %q", got, ":3\r\n")
		}

		// ZREM zrem1 b
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZRem, Args: [][]byte{[]byte("zrem1"), []byte("b")}}, w, conn)
		if got := buf.String(); got != ":1\r\n" {
			t.Errorf("ZREM single: got %q, want %q", got, ":1\r\n")
		}
	})

	t.Run("remove multiple members", func(t *testing.T) {
		h := newHandlerWithEffects()
		defer h.Close()
		conn := newEffectsConn(h)

		// Seed: ZADD zrem2 1 a 2 b 3 c 4 d
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zrem2"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"), []byte("4"), []byte("d")}}, w, conn)
		if got := buf.String(); got != ":4\r\n" {
			t.Fatalf("ZADD seed: got %q, want %q", got, ":4\r\n")
		}

		// ZREM zrem2 a c d
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZRem, Args: [][]byte{[]byte("zrem2"), []byte("a"), []byte("c"), []byte("d")}}, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Errorf("ZREM multiple: got %q, want %q", got, ":3\r\n")
		}
	})

	t.Run("remove nonexistent member", func(t *testing.T) {
		h := newHandlerWithEffects()
		defer h.Close()
		conn := newEffectsConn(h)

		// Seed: ZADD zrem3 1 a 2 b
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zrem3"), []byte("1"), []byte("a"), []byte("2"), []byte("b")}}, w, conn)
		if got := buf.String(); got != ":2\r\n" {
			t.Fatalf("ZADD seed: got %q, want %q", got, ":2\r\n")
		}

		// ZREM zrem3 nonexistent
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZRem, Args: [][]byte{[]byte("zrem3"), []byte("nonexistent")}}, w, conn)
		if got := buf.String(); got != ":0\r\n" {
			t.Errorf("ZREM nonexistent: got %q, want %q", got, ":0\r\n")
		}
	})

	t.Run("WRONGTYPE error on string key", func(t *testing.T) {
		h := newHandlerWithEffects()
		defer h.Close()
		conn := newEffectsConn(h)

		// SET zremstr value
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdSet, Args: [][]byte{[]byte("zremstr"), []byte("value")}}, w, conn)
		if got := buf.String(); got != "+OK\r\n" {
			t.Fatalf("SET: got %q, want %q", got, "+OK\r\n")
		}

		// ZREM zremstr a
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZRem, Args: [][]byte{[]byte("zremstr"), []byte("a")}}, w, conn)
		expect := "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
		if got := buf.String(); got != expect {
			t.Errorf("ZREM WRONGTYPE: got %q, want %q", got, expect)
		}
	})
}

func TestZRemRangeByScore(t *testing.T) {
	t.Run("remove inclusive range", func(t *testing.T) {
		h := newHandlerWithEffects()
		defer h.Close()
		conn := newEffectsConn(h)

		// Seed: ZADD zrbs1 1 a 2 b 3 c 4 d 5 e
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zrbs1"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"), []byte("4"), []byte("d"), []byte("5"), []byte("e")}}, w, conn)
		if got := buf.String(); got != ":5\r\n" {
			t.Fatalf("ZADD seed: got %q, want %q", got, ":5\r\n")
		}

		// ZREMRANGEBYSCORE zrbs1 2 4 -> removes b, c, d
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZRemRangeByScore, Args: [][]byte{[]byte("zrbs1"), []byte("2"), []byte("4")}}, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Errorf("ZREMRANGEBYSCORE inclusive: got %q, want %q", got, ":3\r\n")
		}

		// Verify ZCARD = 2
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZCard, Args: [][]byte{[]byte("zrbs1")}}, w, conn)
		if got := buf.String(); got != ":2\r\n" {
			t.Errorf("ZCARD after remove: got %q, want %q", got, ":2\r\n")
		}
	})

	t.Run("remove exclusive range", func(t *testing.T) {
		h := newHandlerWithEffects()
		defer h.Close()
		conn := newEffectsConn(h)

		// Seed: ZADD zrbs2 1 a 2 b 3 c 4 d 5 e
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zrbs2"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"), []byte("4"), []byte("d"), []byte("5"), []byte("e")}}, w, conn)
		if got := buf.String(); got != ":5\r\n" {
			t.Fatalf("ZADD seed: got %q, want %q", got, ":5\r\n")
		}

		// ZREMRANGEBYSCORE zrbs2 (1 (5 -> removes b, c, d (scores 2, 3, 4)
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZRemRangeByScore, Args: [][]byte{[]byte("zrbs2"), []byte("(1"), []byte("(5")}}, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Errorf("ZREMRANGEBYSCORE exclusive: got %q, want %q", got, ":3\r\n")
		}

		// Verify ZCARD = 2 (a and e remain)
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZCard, Args: [][]byte{[]byte("zrbs2")}}, w, conn)
		if got := buf.String(); got != ":2\r\n" {
			t.Errorf("ZCARD after exclusive remove: got %q, want %q", got, ":2\r\n")
		}
	})

	t.Run("remove -inf to +inf removes all", func(t *testing.T) {
		h := newHandlerWithEffects()
		defer h.Close()
		conn := newEffectsConn(h)

		// Seed: ZADD zrbs3 1 a 2 b 3 c
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zrbs3"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}}, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Fatalf("ZADD seed: got %q, want %q", got, ":3\r\n")
		}

		// ZREMRANGEBYSCORE zrbs3 -inf +inf -> removes all
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZRemRangeByScore, Args: [][]byte{[]byte("zrbs3"), []byte("-inf"), []byte("+inf")}}, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Errorf("ZREMRANGEBYSCORE -inf +inf: got %q, want %q", got, ":3\r\n")
		}

		// Verify ZCARD = 0
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZCard, Args: [][]byte{[]byte("zrbs3")}}, w, conn)
		if got := buf.String(); got != ":0\r\n" {
			t.Errorf("ZCARD after remove all: got %q, want %q", got, ":0\r\n")
		}
	})
}

func TestZRemRangeByRank(t *testing.T) {
	t.Run("remove by rank", func(t *testing.T) {
		h := newHandlerWithEffects()
		defer h.Close()
		conn := newEffectsConn(h)

		// Seed: ZADD zrbr1 1 a 2 b 3 c 4 d 5 e
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zrbr1"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"), []byte("4"), []byte("d"), []byte("5"), []byte("e")}}, w, conn)
		if got := buf.String(); got != ":5\r\n" {
			t.Fatalf("ZADD seed: got %q, want %q", got, ":5\r\n")
		}

		// ZREMRANGEBYRANK zrbr1 0 1 -> removes first 2 (a, b)
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZRemRangeByRank, Args: [][]byte{[]byte("zrbr1"), []byte("0"), []byte("1")}}, w, conn)
		if got := buf.String(); got != ":2\r\n" {
			t.Errorf("ZREMRANGEBYRANK 0 1: got %q, want %q", got, ":2\r\n")
		}

		// Verify ZCARD = 3
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZCard, Args: [][]byte{[]byte("zrbr1")}}, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Errorf("ZCARD after rank remove: got %q, want %q", got, ":3\r\n")
		}
	})

	t.Run("negative indices", func(t *testing.T) {
		h := newHandlerWithEffects()
		defer h.Close()
		conn := newEffectsConn(h)

		// Seed: ZADD zrbr2 1 a 2 b 3 c 4 d 5 e
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zrbr2"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"), []byte("4"), []byte("d"), []byte("5"), []byte("e")}}, w, conn)
		if got := buf.String(); got != ":5\r\n" {
			t.Fatalf("ZADD seed: got %q, want %q", got, ":5\r\n")
		}

		// ZREMRANGEBYRANK zrbr2 -2 -1 -> removes last 2 (d, e)
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZRemRangeByRank, Args: [][]byte{[]byte("zrbr2"), []byte("-2"), []byte("-1")}}, w, conn)
		if got := buf.String(); got != ":2\r\n" {
			t.Errorf("ZREMRANGEBYRANK -2 -1: got %q, want %q", got, ":2\r\n")
		}

		// Verify ZCARD = 3
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZCard, Args: [][]byte{[]byte("zrbr2")}}, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Errorf("ZCARD after negative rank remove: got %q, want %q", got, ":3\r\n")
		}
	})
}

func TestZRemRangeByLex(t *testing.T) {
	t.Run("inclusive lex range", func(t *testing.T) {
		h := newHandlerWithEffects()
		defer h.Close()
		conn := newEffectsConn(h)

		// Seed with same score: ZADD zrbl1 0 a 0 b 0 c 0 d 0 e
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zrbl1"), []byte("0"), []byte("a"), []byte("0"), []byte("b"), []byte("0"), []byte("c"), []byte("0"), []byte("d"), []byte("0"), []byte("e")}}, w, conn)
		if got := buf.String(); got != ":5\r\n" {
			t.Fatalf("ZADD seed: got %q, want %q", got, ":5\r\n")
		}

		// ZREMRANGEBYLEX zrbl1 [b [d -> removes b, c, d
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZRemRangeByLex, Args: [][]byte{[]byte("zrbl1"), []byte("[b"), []byte("[d")}}, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Errorf("ZREMRANGEBYLEX [b [d: got %q, want %q", got, ":3\r\n")
		}

		// Verify ZCARD = 2 (a and e remain)
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZCard, Args: [][]byte{[]byte("zrbl1")}}, w, conn)
		if got := buf.String(); got != ":2\r\n" {
			t.Errorf("ZCARD after lex remove: got %q, want %q", got, ":2\r\n")
		}
	})

	t.Run("exclusive lex range", func(t *testing.T) {
		h := newHandlerWithEffects()
		defer h.Close()
		conn := newEffectsConn(h)

		// Seed with same score: ZADD zrbl2 0 a 0 b 0 c 0 d 0 e
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zrbl2"), []byte("0"), []byte("a"), []byte("0"), []byte("b"), []byte("0"), []byte("c"), []byte("0"), []byte("d"), []byte("0"), []byte("e")}}, w, conn)
		if got := buf.String(); got != ":5\r\n" {
			t.Fatalf("ZADD seed: got %q, want %q", got, ":5\r\n")
		}

		// ZREMRANGEBYLEX zrbl2 (a (e -> removes b, c, d
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZRemRangeByLex, Args: [][]byte{[]byte("zrbl2"), []byte("(a"), []byte("(e")}}, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Errorf("ZREMRANGEBYLEX (a (e: got %q, want %q", got, ":3\r\n")
		}

		// Verify ZCARD = 2 (a and e remain)
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{Type: shared.CmdZCard, Args: [][]byte{[]byte("zrbl2")}}, w, conn)
		if got := buf.String(); got != ":2\r\n" {
			t.Errorf("ZCARD after exclusive lex remove: got %q, want %q", got, ":2\r\n")
		}
	})
}
