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

func TestZUnionStore(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)

	// Seed zus1: a=1, b=2, c=3
	{
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
			[]byte("zus1"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Fatalf("seed zus1: got %q, want %q", got, ":3\r\n")
		}
	}
	// Seed zus2: b=2, c=3, d=4
	{
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
			[]byte("zus2"), []byte("2"), []byte("b"), []byte("3"), []byte("c"), []byte("4"), []byte("d"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Fatalf("seed zus2: got %q, want %q", got, ":3\r\n")
		}
	}

	t.Run("basic union store with SUM", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZUnionStore, Args: [][]byte{
			[]byte("zusdest1"), []byte("2"), []byte("zus1"), []byte("zus2"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":4\r\n" {
			t.Errorf("ZUNIONSTORE got %q, want %q", got, ":4\r\n")
		}

		// Verify scores: a=1, b=4, d=4, c=6 (sorted by score, then lex)
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		cmd = &shared.Command{Type: shared.CmdZRange, Args: [][]byte{
			[]byte("zusdest1"), []byte("0"), []byte("-1"), []byte("WITHSCORES"),
		}}
		h.ExecuteInto(cmd, w, conn)
		expected := "*8\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n4\r\n$1\r\nd\r\n$1\r\n4\r\n$1\r\nc\r\n$1\r\n6\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("ZRANGE WITHSCORES got %q, want %q", got, expected)
		}
	})

	t.Run("with WEIGHTS", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZUnionStore, Args: [][]byte{
			[]byte("zusdest2"), []byte("2"), []byte("zus1"), []byte("zus2"),
			[]byte("WEIGHTS"), []byte("2"), []byte("1"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":4\r\n" {
			t.Errorf("ZUNIONSTORE WEIGHTS got %q, want %q", got, ":4\r\n")
		}

		// Verify scores: a=1*2=2, b=2*2+2*1=6, c=3*2+3*1=9, d=4*1=4
		// Sorted: a(2), d(4), b(6), c(9)
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		cmd = &shared.Command{Type: shared.CmdZRange, Args: [][]byte{
			[]byte("zusdest2"), []byte("0"), []byte("-1"), []byte("WITHSCORES"),
		}}
		h.ExecuteInto(cmd, w, conn)
		expected := "*8\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nd\r\n$1\r\n4\r\n$1\r\nb\r\n$1\r\n6\r\n$1\r\nc\r\n$1\r\n9\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("ZRANGE WITHSCORES got %q, want %q", got, expected)
		}
	})

	t.Run("with AGGREGATE MIN", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZUnionStore, Args: [][]byte{
			[]byte("zusdest3"), []byte("2"), []byte("zus1"), []byte("zus2"),
			[]byte("AGGREGATE"), []byte("MIN"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":4\r\n" {
			t.Errorf("ZUNIONSTORE AGGREGATE MIN got %q, want %q", got, ":4\r\n")
		}

		// Verify scores: a=1, b=min(2,2)=2, c=min(3,3)=3, d=4
		// Sorted: a(1), b(2), c(3), d(4)
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		cmd = &shared.Command{Type: shared.CmdZRange, Args: [][]byte{
			[]byte("zusdest3"), []byte("0"), []byte("-1"), []byte("WITHSCORES"),
		}}
		h.ExecuteInto(cmd, w, conn)
		expected := "*8\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\nd\r\n$1\r\n4\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("ZRANGE WITHSCORES got %q, want %q", got, expected)
		}
	})
}

func TestZInterStore(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)

	// Seed zis1: a=1, b=2, c=3
	{
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
			[]byte("zis1"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Fatalf("seed zis1: got %q, want %q", got, ":3\r\n")
		}
	}
	// Seed zis2: b=2, c=3, d=4
	{
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
			[]byte("zis2"), []byte("2"), []byte("b"), []byte("3"), []byte("c"), []byte("4"), []byte("d"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Fatalf("seed zis2: got %q, want %q", got, ":3\r\n")
		}
	}

	t.Run("basic intersection store with SUM", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZInterStore, Args: [][]byte{
			[]byte("zisdest"), []byte("2"), []byte("zis1"), []byte("zis2"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":2\r\n" {
			t.Errorf("ZINTERSTORE got %q, want %q", got, ":2\r\n")
		}

		// Verify scores: b=2+2=4, c=3+3=6
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		cmd = &shared.Command{Type: shared.CmdZRange, Args: [][]byte{
			[]byte("zisdest"), []byte("0"), []byte("-1"), []byte("WITHSCORES"),
		}}
		h.ExecuteInto(cmd, w, conn)
		expected := "*4\r\n$1\r\nb\r\n$1\r\n4\r\n$1\r\nc\r\n$1\r\n6\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("ZRANGE WITHSCORES got %q, want %q", got, expected)
		}
	})
}

func TestZUnion(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)

	// Seed zu1: a=1, b=2, c=3
	{
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
			[]byte("zu1"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Fatalf("seed zu1: got %q, want %q", got, ":3\r\n")
		}
	}
	// Seed zu2: b=2, c=3, d=4
	{
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
			[]byte("zu2"), []byte("2"), []byte("b"), []byte("3"), []byte("c"), []byte("4"), []byte("d"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Fatalf("seed zu2: got %q, want %q", got, ":3\r\n")
		}
	}

	t.Run("union with WITHSCORES", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZUnion, Args: [][]byte{
			[]byte("2"), []byte("zu1"), []byte("zu2"), []byte("WITHSCORES"),
		}}
		h.ExecuteInto(cmd, w, conn)
		// Union SUM: a=1, b=4, d=4, c=6 sorted by score then lex
		expected := "*8\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n4\r\n$1\r\nd\r\n$1\r\n4\r\n$1\r\nc\r\n$1\r\n6\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("ZUNION WITHSCORES got %q, want %q", got, expected)
		}
	})
}

func TestZInter(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)

	// Seed zi1: a=1, b=2, c=3
	{
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
			[]byte("zi1"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Fatalf("seed zi1: got %q, want %q", got, ":3\r\n")
		}
	}
	// Seed zi2: b=2, c=3, d=4
	{
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
			[]byte("zi2"), []byte("2"), []byte("b"), []byte("3"), []byte("c"), []byte("4"), []byte("d"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Fatalf("seed zi2: got %q, want %q", got, ":3\r\n")
		}
	}

	t.Run("intersection with WITHSCORES", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZInter, Args: [][]byte{
			[]byte("2"), []byte("zi1"), []byte("zi2"), []byte("WITHSCORES"),
		}}
		h.ExecuteInto(cmd, w, conn)
		// Intersection SUM: b=4, c=6
		expected := "*4\r\n$1\r\nb\r\n$1\r\n4\r\n$1\r\nc\r\n$1\r\n6\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("ZINTER WITHSCORES got %q, want %q", got, expected)
		}
	})
}

func TestZInterCard(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)

	// Seed zic1: a=1, b=2, c=3
	{
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
			[]byte("zic1"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Fatalf("seed zic1: got %q, want %q", got, ":3\r\n")
		}
	}
	// Seed zic2: b=2, c=3, d=4
	{
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
			[]byte("zic2"), []byte("2"), []byte("b"), []byte("3"), []byte("c"), []byte("4"), []byte("d"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Fatalf("seed zic2: got %q, want %q", got, ":3\r\n")
		}
	}

	t.Run("basic intercard", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZInterCard, Args: [][]byte{
			[]byte("2"), []byte("zic1"), []byte("zic2"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":2\r\n" {
			t.Errorf("ZINTERCARD got %q, want %q", got, ":2\r\n")
		}
	})

	t.Run("with LIMIT", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZInterCard, Args: [][]byte{
			[]byte("2"), []byte("zic1"), []byte("zic2"), []byte("LIMIT"), []byte("1"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":1\r\n" {
			t.Errorf("ZINTERCARD LIMIT got %q, want %q", got, ":1\r\n")
		}
	})
}

func TestZDiff(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)

	// Seed zd1: a=1, b=2, c=3
	{
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
			[]byte("zd1"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Fatalf("seed zd1: got %q, want %q", got, ":3\r\n")
		}
	}
	// Seed zd2: b=2, c=3, d=4
	{
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
			[]byte("zd2"), []byte("2"), []byte("b"), []byte("3"), []byte("c"), []byte("4"), []byte("d"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Fatalf("seed zd2: got %q, want %q", got, ":3\r\n")
		}
	}

	t.Run("diff members only", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZDiff, Args: [][]byte{
			[]byte("2"), []byte("zd1"), []byte("zd2"),
		}}
		h.ExecuteInto(cmd, w, conn)
		// zd1 - zd2 = {a}
		expected := "*1\r\n$1\r\na\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("ZDIFF got %q, want %q", got, expected)
		}
	})

	t.Run("diff with WITHSCORES", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZDiff, Args: [][]byte{
			[]byte("2"), []byte("zd1"), []byte("zd2"), []byte("WITHSCORES"),
		}}
		h.ExecuteInto(cmd, w, conn)
		// zd1 - zd2 = {a} with score 1
		expected := "*2\r\n$1\r\na\r\n$1\r\n1\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("ZDIFF WITHSCORES got %q, want %q", got, expected)
		}
	})
}

func TestZDiffStore(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)

	// Seed zds1: a=1, b=2, c=3
	{
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
			[]byte("zds1"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Fatalf("seed zds1: got %q, want %q", got, ":3\r\n")
		}
	}
	// Seed zds2: b=2, c=3, d=4
	{
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
			[]byte("zds2"), []byte("2"), []byte("b"), []byte("3"), []byte("c"), []byte("4"), []byte("d"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Fatalf("seed zds2: got %q, want %q", got, ":3\r\n")
		}
	}

	t.Run("basic diff store", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZDiffStore, Args: [][]byte{
			[]byte("zdsdest"), []byte("2"), []byte("zds1"), []byte("zds2"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":1\r\n" {
			t.Errorf("ZDIFFSTORE got %q, want %q", got, ":1\r\n")
		}
	})
}

func TestZRangeStore(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)

	// Seed zrssrc with 5 members: a=1, b=2, c=3, d=4, e=5
	{
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
			[]byte("zrssrc"),
			[]byte("1"), []byte("a"),
			[]byte("2"), []byte("b"),
			[]byte("3"), []byte("c"),
			[]byte("4"), []byte("d"),
			[]byte("5"), []byte("e"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":5\r\n" {
			t.Fatalf("seed zrssrc: got %q, want %q", got, ":5\r\n")
		}
	}

	t.Run("store first 3 by index", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZRangeStore, Args: [][]byte{
			[]byte("zrsdest"), []byte("zrssrc"), []byte("0"), []byte("2"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":3\r\n" {
			t.Errorf("ZRANGESTORE got %q, want %q", got, ":3\r\n")
		}

		// Verify with ZRANGE
		buf = &bytes.Buffer{}
		w = shared.NewWriter(buf)
		cmd = &shared.Command{Type: shared.CmdZRange, Args: [][]byte{
			[]byte("zrsdest"), []byte("0"), []byte("-1"), []byte("WITHSCORES"),
		}}
		h.ExecuteInto(cmd, w, conn)
		expected := "*6\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\nc\r\n$1\r\n3\r\n"
		if got := buf.String(); got != expected {
			t.Errorf("ZRANGE WITHSCORES got %q, want %q", got, expected)
		}
	})
}
