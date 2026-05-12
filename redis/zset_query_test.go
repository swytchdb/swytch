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
	"slices"
	"strings"
	"testing"

	"github.com/swytchdb/swytch/redis/shared"
)

// seedQueryZSet seeds ZADD myzset 1 a 2 b 3 c (3 members only, for query tests)
func seedQueryZSet(t *testing.T, h *Handler, conn *shared.Connection) {
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
		},
	}
	h.ExecuteInto(cmd, w, conn)
	if got := buf.String(); got != ":3\r\n" {
		t.Fatalf("ZADD seed got %q, want %q", got, ":3\r\n")
	}
}

// seedQueryStringKey creates a string key for WRONGTYPE testing
func seedQueryStringKey(t *testing.T, h *Handler, conn *shared.Connection, key string) {
	t.Helper()
	buf := &bytes.Buffer{}
	w := shared.NewWriter(buf)
	h.ExecuteInto(&shared.Command{
		Type: shared.CmdSet,
		Args: [][]byte{[]byte(key), []byte("value")},
	}, w, conn)
}

func TestZScore(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)

	seedQueryZSet(t, h, conn)
	seedQueryStringKey(t, h, conn, "strkey")

	tests := []struct {
		name   string
		cmd    *shared.Command
		expect string
	}{
		{
			name: "existing member score 1",
			cmd: &shared.Command{
				Type: shared.CmdZScore,
				Args: [][]byte{[]byte("myzset"), []byte("a")},
			},
			expect: "$1\r\n1\r\n",
		},
		{
			name: "existing member score 2",
			cmd: &shared.Command{
				Type: shared.CmdZScore,
				Args: [][]byte{[]byte("myzset"), []byte("b")},
			},
			expect: "$1\r\n2\r\n",
		},
		{
			name: "existing member score 3",
			cmd: &shared.Command{
				Type: shared.CmdZScore,
				Args: [][]byte{[]byte("myzset"), []byte("c")},
			},
			expect: "$1\r\n3\r\n",
		},
		{
			name: "missing member returns null bulk string",
			cmd: &shared.Command{
				Type: shared.CmdZScore,
				Args: [][]byte{[]byte("myzset"), []byte("z")},
			},
			expect: "$-1\r\n",
		},
		{
			name: "nonexistent key returns null bulk string",
			cmd: &shared.Command{
				Type: shared.CmdZScore,
				Args: [][]byte{[]byte("nosuchkey"), []byte("a")},
			},
			expect: "$-1\r\n",
		},
		{
			name: "WRONGTYPE error on string key",
			cmd: &shared.Command{
				Type: shared.CmdZScore,
				Args: [][]byte{[]byte("strkey"), []byte("a")},
			},
			expect: "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			w := shared.NewWriter(buf)
			h.ExecuteInto(tt.cmd, w, conn)
			if got := buf.String(); got != tt.expect {
				t.Errorf("got %q, want %q", got, tt.expect)
			}
		})
	}
}

func TestZMScore(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)

	seedQueryZSet(t, h, conn)
	seedQueryStringKey(t, h, conn, "strkey")

	tests := []struct {
		name   string
		cmd    *shared.Command
		expect string
	}{
		{
			name: "all members exist",
			cmd: &shared.Command{
				Type: shared.CmdZMScore,
				Args: [][]byte{[]byte("myzset"), []byte("a"), []byte("b"), []byte("c")},
			},
			expect: "*3\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n",
		},
		{
			name: "some members missing",
			cmd: &shared.Command{
				Type: shared.CmdZMScore,
				Args: [][]byte{[]byte("myzset"), []byte("a"), []byte("z"), []byte("c")},
			},
			expect: "*3\r\n$1\r\n1\r\n$-1\r\n$1\r\n3\r\n",
		},
		{
			name: "all members missing",
			cmd: &shared.Command{
				Type: shared.CmdZMScore,
				Args: [][]byte{[]byte("myzset"), []byte("x"), []byte("y")},
			},
			expect: "*2\r\n$-1\r\n$-1\r\n",
		},
		{
			name: "nonexistent key returns array of nulls",
			cmd: &shared.Command{
				Type: shared.CmdZMScore,
				Args: [][]byte{[]byte("nosuchkey"), []byte("a"), []byte("b")},
			},
			expect: "*2\r\n$-1\r\n$-1\r\n",
		},
		{
			name: "WRONGTYPE error on string key",
			cmd: &shared.Command{
				Type: shared.CmdZMScore,
				Args: [][]byte{[]byte("strkey"), []byte("a")},
			},
			expect: "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			w := shared.NewWriter(buf)
			h.ExecuteInto(tt.cmd, w, conn)
			if got := buf.String(); got != tt.expect {
				t.Errorf("got %q, want %q", got, tt.expect)
			}
		})
	}
}

func TestZCard(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)

	seedQueryZSet(t, h, conn)
	seedQueryStringKey(t, h, conn, "strkey")

	tests := []struct {
		name   string
		cmd    *shared.Command
		expect string
	}{
		{
			name: "existing key returns cardinality",
			cmd: &shared.Command{
				Type: shared.CmdZCard,
				Args: [][]byte{[]byte("myzset")},
			},
			expect: ":3\r\n",
		},
		{
			name: "nonexistent key returns zero",
			cmd: &shared.Command{
				Type: shared.CmdZCard,
				Args: [][]byte{[]byte("nosuchkey")},
			},
			expect: ":0\r\n",
		},
		{
			name: "WRONGTYPE error on string key",
			cmd: &shared.Command{
				Type: shared.CmdZCard,
				Args: [][]byte{[]byte("strkey")},
			},
			expect: "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			w := shared.NewWriter(buf)
			h.ExecuteInto(tt.cmd, w, conn)
			if got := buf.String(); got != tt.expect {
				t.Errorf("got %q, want %q", got, tt.expect)
			}
		})
	}
}

func TestZRank(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)

	seedQueryZSet(t, h, conn)
	seedQueryStringKey(t, h, conn, "strkey")

	tests := []struct {
		name   string
		cmd    *shared.Command
		expect string
	}{
		{
			name: "rank of lowest score member",
			cmd: &shared.Command{
				Type: shared.CmdZRank,
				Args: [][]byte{[]byte("myzset"), []byte("a")},
			},
			expect: ":0\r\n",
		},
		{
			name: "rank of middle score member",
			cmd: &shared.Command{
				Type: shared.CmdZRank,
				Args: [][]byte{[]byte("myzset"), []byte("b")},
			},
			expect: ":1\r\n",
		},
		{
			name: "rank of highest score member",
			cmd: &shared.Command{
				Type: shared.CmdZRank,
				Args: [][]byte{[]byte("myzset"), []byte("c")},
			},
			expect: ":2\r\n",
		},
		{
			name: "member not found returns null bulk string",
			cmd: &shared.Command{
				Type: shared.CmdZRank,
				Args: [][]byte{[]byte("myzset"), []byte("z")},
			},
			expect: "$-1\r\n",
		},
		{
			name: "nonexistent key returns null bulk string",
			cmd: &shared.Command{
				Type: shared.CmdZRank,
				Args: [][]byte{[]byte("nosuchkey"), []byte("a")},
			},
			expect: "$-1\r\n",
		},
		{
			name: "WRONGTYPE error on string key",
			cmd: &shared.Command{
				Type: shared.CmdZRank,
				Args: [][]byte{[]byte("strkey"), []byte("a")},
			},
			expect: "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			w := shared.NewWriter(buf)
			h.ExecuteInto(tt.cmd, w, conn)
			if got := buf.String(); got != tt.expect {
				t.Errorf("got %q, want %q", got, tt.expect)
			}
		})
	}
}

func TestZRevRank(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)

	seedQueryZSet(t, h, conn)
	seedQueryStringKey(t, h, conn, "strkey")

	tests := []struct {
		name   string
		cmd    *shared.Command
		expect string
	}{
		{
			name: "revrank of highest score member is 0",
			cmd: &shared.Command{
				Type: shared.CmdZRevRank,
				Args: [][]byte{[]byte("myzset"), []byte("c")},
			},
			expect: ":0\r\n",
		},
		{
			name: "revrank of middle score member is 1",
			cmd: &shared.Command{
				Type: shared.CmdZRevRank,
				Args: [][]byte{[]byte("myzset"), []byte("b")},
			},
			expect: ":1\r\n",
		},
		{
			name: "revrank of lowest score member is 2",
			cmd: &shared.Command{
				Type: shared.CmdZRevRank,
				Args: [][]byte{[]byte("myzset"), []byte("a")},
			},
			expect: ":2\r\n",
		},
		{
			name: "member not found returns null bulk string",
			cmd: &shared.Command{
				Type: shared.CmdZRevRank,
				Args: [][]byte{[]byte("myzset"), []byte("z")},
			},
			expect: "$-1\r\n",
		},
		{
			name: "nonexistent key returns null bulk string",
			cmd: &shared.Command{
				Type: shared.CmdZRevRank,
				Args: [][]byte{[]byte("nosuchkey"), []byte("a")},
			},
			expect: "$-1\r\n",
		},
		{
			name: "WRONGTYPE error on string key",
			cmd: &shared.Command{
				Type: shared.CmdZRevRank,
				Args: [][]byte{[]byte("strkey"), []byte("a")},
			},
			expect: "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			w := shared.NewWriter(buf)
			h.ExecuteInto(tt.cmd, w, conn)
			if got := buf.String(); got != tt.expect {
				t.Errorf("got %q, want %q", got, tt.expect)
			}
		})
	}
}

func TestZCount(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)

	seedQueryZSet(t, h, conn)
	seedQueryStringKey(t, h, conn, "strkey")

	tests := []struct {
		name   string
		cmd    *shared.Command
		expect string
	}{
		{
			name: "inclusive range 1 to 3 counts all",
			cmd: &shared.Command{
				Type: shared.CmdZCount,
				Args: [][]byte{[]byte("myzset"), []byte("1"), []byte("3")},
			},
			expect: ":3\r\n",
		},
		{
			name: "inclusive range 1 to 2",
			cmd: &shared.Command{
				Type: shared.CmdZCount,
				Args: [][]byte{[]byte("myzset"), []byte("1"), []byte("2")},
			},
			expect: ":2\r\n",
		},
		{
			name: "exclusive min with ( prefix",
			cmd: &shared.Command{
				Type: shared.CmdZCount,
				Args: [][]byte{[]byte("myzset"), []byte("(1"), []byte("3")},
			},
			expect: ":2\r\n",
		},
		{
			name: "exclusive max with ( prefix",
			cmd: &shared.Command{
				Type: shared.CmdZCount,
				Args: [][]byte{[]byte("myzset"), []byte("1"), []byte("(3")},
			},
			expect: ":2\r\n",
		},
		{
			name: "both exclusive",
			cmd: &shared.Command{
				Type: shared.CmdZCount,
				Args: [][]byte{[]byte("myzset"), []byte("(1"), []byte("(3")},
			},
			expect: ":1\r\n",
		},
		{
			name: "-inf to +inf counts all",
			cmd: &shared.Command{
				Type: shared.CmdZCount,
				Args: [][]byte{[]byte("myzset"), []byte("-inf"), []byte("+inf")},
			},
			expect: ":3\r\n",
		},
		{
			name: "nonexistent key returns zero",
			cmd: &shared.Command{
				Type: shared.CmdZCount,
				Args: [][]byte{[]byte("nosuchkey"), []byte("-inf"), []byte("+inf")},
			},
			expect: ":0\r\n",
		},
		{
			name: "WRONGTYPE error on string key",
			cmd: &shared.Command{
				Type: shared.CmdZCount,
				Args: [][]byte{[]byte("strkey"), []byte("1"), []byte("3")},
			},
			expect: "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			w := shared.NewWriter(buf)
			h.ExecuteInto(tt.cmd, w, conn)
			if got := buf.String(); got != tt.expect {
				t.Errorf("got %q, want %q", got, tt.expect)
			}
		})
	}
}

func TestZRandMember(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)

	seedQueryZSet(t, h, conn)
	seedQueryStringKey(t, h, conn, "strkey")

	t.Run("positive count returns array of distinct members", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{
			Type: shared.CmdZRandMember,
			Args: [][]byte{[]byte("myzset"), []byte("3")},
		}, w, conn)

		got := buf.String()
		// Should be an array of 3 members
		if !strings.HasPrefix(got, "*3\r\n") {
			t.Errorf("expected array of 3 elements, got %q", got)
		}
		// Each element must be one of a, b, c
		for _, member := range []string{"a", "b", "c"} {
			bulkStr := "$1\r\n" + member + "\r\n"
			if !strings.Contains(got, bulkStr) {
				t.Errorf("expected member %q in response, got %q", member, got)
			}
		}
	})

	t.Run("positive count larger than set returns all members", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{
			Type: shared.CmdZRandMember,
			Args: [][]byte{[]byte("myzset"), []byte("10")},
		}, w, conn)

		got := buf.String()
		// Should return all 3 members (capped at set size)
		if !strings.HasPrefix(got, "*3\r\n") {
			t.Errorf("expected array of 3 elements, got %q", got)
		}
	})

	t.Run("count of zero returns empty array", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{
			Type: shared.CmdZRandMember,
			Args: [][]byte{[]byte("myzset"), []byte("0")},
		}, w, conn)

		if got := buf.String(); got != "*0\r\n" {
			t.Errorf("got %q, want %q", got, "*0\r\n")
		}
	})

	t.Run("nonexistent key without count returns null bulk string", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{
			Type: shared.CmdZRandMember,
			Args: [][]byte{[]byte("nosuchkey")},
		}, w, conn)

		if got := buf.String(); got != "$-1\r\n" {
			t.Errorf("got %q, want %q", got, "$-1\r\n")
		}
	})

	t.Run("nonexistent key with count returns empty array", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{
			Type: shared.CmdZRandMember,
			Args: [][]byte{[]byte("nosuchkey"), []byte("3")},
		}, w, conn)

		if got := buf.String(); got != "*0\r\n" {
			t.Errorf("got %q, want %q", got, "*0\r\n")
		}
	})

	t.Run("no count returns single random member", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{
			Type: shared.CmdZRandMember,
			Args: [][]byte{[]byte("myzset")},
		}, w, conn)

		got := buf.String()
		validMembers := []string{
			"$1\r\na\r\n",
			"$1\r\nb\r\n",
			"$1\r\nc\r\n",
		}
		found := slices.Contains(validMembers, got)
		if !found {
			t.Errorf("got %q, want one of %v", got, validMembers)
		}
	})

	t.Run("WRONGTYPE error on string key", func(t *testing.T) {
		buf := &bytes.Buffer{}
		w := shared.NewWriter(buf)
		h.ExecuteInto(&shared.Command{
			Type: shared.CmdZRandMember,
			Args: [][]byte{[]byte("strkey"), []byte("1")},
		}, w, conn)

		expect := "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
		if got := buf.String(); got != expect {
			t.Errorf("got %q, want %q", got, expect)
		}
	})
}
