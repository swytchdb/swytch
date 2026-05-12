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

func TestZLexCount(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)

	// Seed: ZADD zlexcount 0 a 0 b 0 c 0 d 0 e
	{
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
			[]byte("zlexcount"), []byte("0"), []byte("a"),
			[]byte("0"), []byte("b"), []byte("0"), []byte("c"),
			[]byte("0"), []byte("d"), []byte("0"), []byte("e"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":5\r\n" {
			t.Fatalf("seed ZADD: got %q, want %q", got, ":5\r\n")
		}
	}

	tests := []struct {
		name   string
		args   [][]byte
		expect string
	}{
		{
			name:   "count all with - +",
			args:   [][]byte{[]byte("zlexcount"), []byte("-"), []byte("+")},
			expect: ":5\r\n",
		},
		{
			name:   "inclusive range [b [d",
			args:   [][]byte{[]byte("zlexcount"), []byte("[b"), []byte("[d")},
			expect: ":3\r\n",
		},
		{
			name:   "exclusive range (a (e",
			args:   [][]byte{[]byte("zlexcount"), []byte("(a"), []byte("(e")},
			expect: ":3\r\n",
		},
		{
			name:   "mixed inclusive exclusive [b (d",
			args:   [][]byte{[]byte("zlexcount"), []byte("[b"), []byte("(d")},
			expect: ":2\r\n",
		},
		{
			name:   "nonexistent key",
			args:   [][]byte{[]byte("zlexcount_nokey"), []byte("-"), []byte("+")},
			expect: ":0\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			w := shared.NewWriter(buf)
			cmd := &shared.Command{Type: shared.CmdZLexCount, Args: tt.args}
			h.ExecuteInto(cmd, w, conn)
			if got := buf.String(); got != tt.expect {
				t.Errorf("got %q, want %q", got, tt.expect)
			}
		})
	}
}

func TestZRangeByLex(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)

	// Seed: ZADD zrangebylex 0 a 0 b 0 c 0 d 0 e
	{
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
			[]byte("zrangebylex"), []byte("0"), []byte("a"),
			[]byte("0"), []byte("b"), []byte("0"), []byte("c"),
			[]byte("0"), []byte("d"), []byte("0"), []byte("e"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":5\r\n" {
			t.Fatalf("seed ZADD: got %q, want %q", got, ":5\r\n")
		}
	}

	tests := []struct {
		name   string
		args   [][]byte
		expect string
	}{
		{
			name:   "all members with - +",
			args:   [][]byte{[]byte("zrangebylex"), []byte("-"), []byte("+")},
			expect: "*5\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n",
		},
		{
			name:   "inclusive range [b [d",
			args:   [][]byte{[]byte("zrangebylex"), []byte("[b"), []byte("[d")},
			expect: "*3\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n",
		},
		{
			name:   "exclusive min inclusive max (a [c",
			args:   [][]byte{[]byte("zrangebylex"), []byte("(a"), []byte("[c")},
			expect: "*2\r\n$1\r\nb\r\n$1\r\nc\r\n",
		},
		{
			name:   "all with LIMIT 1 2",
			args:   [][]byte{[]byte("zrangebylex"), []byte("-"), []byte("+"), []byte("LIMIT"), []byte("1"), []byte("2")},
			expect: "*2\r\n$1\r\nb\r\n$1\r\nc\r\n",
		},
		{
			name:   "nonexistent key",
			args:   [][]byte{[]byte("zrangebylex_nokey"), []byte("-"), []byte("+")},
			expect: "*0\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			w := shared.NewWriter(buf)
			cmd := &shared.Command{Type: shared.CmdZRangeByLex, Args: tt.args}
			h.ExecuteInto(cmd, w, conn)
			if got := buf.String(); got != tt.expect {
				t.Errorf("got %q, want %q", got, tt.expect)
			}
		})
	}
}

func TestZRevRangeByLex(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)

	// Seed: ZADD zrevrangebylex 0 a 0 b 0 c 0 d 0 e
	{
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		cmd := &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{
			[]byte("zrevrangebylex"), []byte("0"), []byte("a"),
			[]byte("0"), []byte("b"), []byte("0"), []byte("c"),
			[]byte("0"), []byte("d"), []byte("0"), []byte("e"),
		}}
		h.ExecuteInto(cmd, w, conn)
		if got := buf.String(); got != ":5\r\n" {
			t.Fatalf("seed ZADD: got %q, want %q", got, ":5\r\n")
		}
	}

	tests := []struct {
		name   string
		args   [][]byte
		expect string
	}{
		{
			name:   "all members reversed with + -",
			args:   [][]byte{[]byte("zrevrangebylex"), []byte("+"), []byte("-")},
			expect: "*5\r\n$1\r\ne\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n",
		},
		{
			name:   "inclusive range [d [b reversed",
			args:   [][]byte{[]byte("zrevrangebylex"), []byte("[d"), []byte("[b")},
			expect: "*3\r\n$1\r\nd\r\n$1\r\nc\r\n$1\r\nb\r\n",
		},
		{
			name:   "all reversed with LIMIT 0 3",
			args:   [][]byte{[]byte("zrevrangebylex"), []byte("+"), []byte("-"), []byte("LIMIT"), []byte("0"), []byte("3")},
			expect: "*3\r\n$1\r\ne\r\n$1\r\nd\r\n$1\r\nc\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			w := shared.NewWriter(buf)
			cmd := &shared.Command{Type: shared.CmdZRevRangeByLex, Args: tt.args}
			h.ExecuteInto(cmd, w, conn)
			if got := buf.String(); got != tt.expect {
				t.Errorf("got %q, want %q", got, tt.expect)
			}
		})
	}
}
