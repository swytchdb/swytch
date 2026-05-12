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

func TestZAdd(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)

	tests := []struct {
		name string
		cmds []struct {
			cmd    *shared.Command
			expect string
		}
	}{
		{
			name: "basic add 3 members",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zbasic"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}},
					expect: ":3\r\n",
				},
			},
		},
		{
			name: "update existing score returns 0",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zupdate"), []byte("1"), []byte("a"), []byte("2"), []byte("b")}},
					expect: ":2\r\n",
				},
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zupdate"), []byte("5"), []byte("a"), []byte("10"), []byte("b")}},
					expect: ":0\r\n",
				},
			},
		},
		{
			name: "NX flag skip existing add new",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("znx"), []byte("1"), []byte("a")}},
					expect: ":1\r\n",
				},
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("znx"), []byte("NX"), []byte("2"), []byte("a"), []byte("3"), []byte("b")}},
					expect: ":1\r\n", // only "b" added, "a" skipped
				},
			},
		},
		{
			name: "XX flag skip new update existing",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zxx"), []byte("1"), []byte("a")}},
					expect: ":1\r\n",
				},
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zxx"), []byte("XX"), []byte("2"), []byte("a"), []byte("3"), []byte("b")}},
					expect: ":0\r\n", // "a" updated (not added), "b" skipped (doesn't exist)
				},
			},
		},
		{
			name: "GT flag only update if greater",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zgt"), []byte("5"), []byte("a")}},
					expect: ":1\r\n",
				},
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zgt"), []byte("GT"), []byte("CH"), []byte("10"), []byte("a")}},
					expect: ":1\r\n", // 10 > 5, score changed
				},
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zgt"), []byte("GT"), []byte("CH"), []byte("3"), []byte("a")}},
					expect: ":0\r\n", // 3 < 10, not updated
				},
			},
		},
		{
			name: "LT flag only update if less",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zlt"), []byte("5"), []byte("a")}},
					expect: ":1\r\n",
				},
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zlt"), []byte("LT"), []byte("CH"), []byte("3"), []byte("a")}},
					expect: ":1\r\n", // 3 < 5, score changed
				},
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zlt"), []byte("LT"), []byte("CH"), []byte("10"), []byte("a")}},
					expect: ":0\r\n", // 10 > 3, not updated
				},
			},
		},
		{
			name: "CH flag returns changed count",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zch"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}},
					expect: ":3\r\n",
				},
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zch"), []byte("CH"), []byte("10"), []byte("a"), []byte("20"), []byte("b"), []byte("3"), []byte("c")}},
					expect: ":2\r\n", // "a" and "b" changed, "c" same score
				},
			},
		},
		{
			name: "INCR flag increments and returns score",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zincr"), []byte("5"), []byte("a")}},
					expect: ":1\r\n",
				},
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zincr"), []byte("INCR"), []byte("3"), []byte("a")}},
					expect: "$1\r\n8\r\n", // 5 + 3 = 8
				},
			},
		},
		{
			name: "NX and XX conflict",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zconfl1"), []byte("NX"), []byte("XX"), []byte("1"), []byte("a")}},
					expect: "-ERR XX and NX options at the same time are not compatible\r\n",
				},
			},
		},
		{
			name: "GT and LT conflict",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zconfl2"), []byte("GT"), []byte("LT"), []byte("1"), []byte("a")}},
					expect: "-ERR GT and LT options at the same time are not compatible\r\n",
				},
			},
		},
		{
			name: "wrong number of arguments",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{}},
					expect: "-ERR wrong number of arguments for 'zadd' command\r\n",
				},
			},
		},
		{
			name: "WRONGTYPE error on string key",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdSet, Args: [][]byte{[]byte("zstrkey"), []byte("value")}},
					expect: "+OK\r\n",
				},
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zstrkey"), []byte("1"), []byte("a")}},
					expect: "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, c := range tt.cmds {
				buf := &bytes.Buffer{}
				w := shared.NewWriter(buf)
				h.ExecuteInto(c.cmd, w, conn)
				if got := buf.String(); got != c.expect {
					t.Errorf("step %d: got %q, want %q", i, got, c.expect)
				}
			}
		})
	}
}

func TestZIncrBy(t *testing.T) {
	h := newHandlerWithEffects()
	defer h.Close()
	conn := newEffectsConn(h)

	tests := []struct {
		name string
		cmds []struct {
			cmd    *shared.Command
			expect string
		}
	}{
		{
			name: "increment existing member",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zikey"), []byte("5"), []byte("a")}},
					expect: ":1\r\n",
				},
				{
					cmd:    &shared.Command{Type: shared.CmdZIncrBy, Args: [][]byte{[]byte("zikey"), []byte("3"), []byte("a")}},
					expect: "$1\r\n8\r\n", // 5 + 3 = 8
				},
			},
		},
		{
			name: "increment new member creates it",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZIncrBy, Args: [][]byte{[]byte("zinew"), []byte("5"), []byte("newmember")}},
					expect: "$1\r\n5\r\n", // 0 + 5 = 5
				},
			},
		},
		{
			name: "WRONGTYPE error on string key",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdSet, Args: [][]byte{[]byte("zistrkey"), []byte("value")}},
					expect: "+OK\r\n",
				},
				{
					cmd:    &shared.Command{Type: shared.CmdZIncrBy, Args: [][]byte{[]byte("zistrkey"), []byte("1"), []byte("a")}},
					expect: "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, c := range tt.cmds {
				buf := &bytes.Buffer{}
				w := shared.NewWriter(buf)
				h.ExecuteInto(c.cmd, w, conn)
				if got := buf.String(); got != c.expect {
					t.Errorf("step %d: got %q, want %q", i, got, c.expect)
				}
			}
		})
	}
}
