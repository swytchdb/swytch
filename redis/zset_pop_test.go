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

func TestZPopMin(t *testing.T) {
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
			name: "pop single no count arg returns lowest",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zpmin1"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}},
					expect: ":3\r\n",
				},
				{
					cmd:    &shared.Command{Type: shared.CmdZPopMin, Args: [][]byte{[]byte("zpmin1")}},
					expect: "*2\r\n$1\r\na\r\n$1\r\n1\r\n",
				},
			},
		},
		{
			name: "pop with count returns N lowest",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zpmin2"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}},
					expect: ":3\r\n",
				},
				{
					cmd:    &shared.Command{Type: shared.CmdZPopMin, Args: [][]byte{[]byte("zpmin2"), []byte("2")}},
					expect: "*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n",
				},
			},
		},
		{
			name: "pop from empty set",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZPopMin, Args: [][]byte{[]byte("zpmin_empty")}},
					expect: "*0\r\n",
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
					cmd:    &shared.Command{Type: shared.CmdSet, Args: [][]byte{[]byte("zpmin_str"), []byte("value")}},
					expect: "+OK\r\n",
				},
				{
					cmd:    &shared.Command{Type: shared.CmdZPopMin, Args: [][]byte{[]byte("zpmin_str")}},
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

func TestZPopMax(t *testing.T) {
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
			name: "pop single returns highest",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zpmax1"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}},
					expect: ":3\r\n",
				},
				{
					cmd:    &shared.Command{Type: shared.CmdZPopMax, Args: [][]byte{[]byte("zpmax1")}},
					expect: "*2\r\n$1\r\nc\r\n$1\r\n3\r\n",
				},
			},
		},
		{
			name: "pop with count returns N highest",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zpmax2"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}},
					expect: ":3\r\n",
				},
				{
					cmd:    &shared.Command{Type: shared.CmdZPopMax, Args: [][]byte{[]byte("zpmax2"), []byte("2")}},
					expect: "*4\r\n$1\r\nc\r\n$1\r\n3\r\n$1\r\nb\r\n$1\r\n2\r\n",
				},
			},
		},
		{
			name: "pop from empty set",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZPopMax, Args: [][]byte{[]byte("zpmax_empty")}},
					expect: "*0\r\n",
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

func TestZMPop(t *testing.T) {
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
			name: "ZMPOP 1 key MIN returns key name and popped member",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zmpop1"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}},
					expect: ":3\r\n",
				},
				{
					// ZMPOP 1 zmpop1 MIN
					cmd:    &shared.Command{Type: shared.CmdZMPop, Args: [][]byte{[]byte("1"), []byte("zmpop1"), []byte("MIN")}},
					expect: "*2\r\n$6\r\nzmpop1\r\n*1\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n",
				},
			},
		},
		{
			name: "ZMPOP 1 key MAX COUNT 2 returns key and 2 highest",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					cmd:    &shared.Command{Type: shared.CmdZAdd, Args: [][]byte{[]byte("zmpop2"), []byte("1"), []byte("a"), []byte("2"), []byte("b"), []byte("3"), []byte("c")}},
					expect: ":3\r\n",
				},
				{
					// ZMPOP 1 zmpop2 MAX COUNT 2
					cmd:    &shared.Command{Type: shared.CmdZMPop, Args: [][]byte{[]byte("1"), []byte("zmpop2"), []byte("MAX"), []byte("COUNT"), []byte("2")}},
					expect: "*2\r\n$6\r\nzmpop2\r\n*2\r\n*2\r\n$1\r\nc\r\n$1\r\n3\r\n*2\r\n$1\r\nb\r\n$1\r\n2\r\n",
				},
			},
		},
		{
			name: "ZMPOP on nonexistent key returns null array",
			cmds: []struct {
				cmd    *shared.Command
				expect string
			}{
				{
					// ZMPOP 1 nosuchkey MIN
					cmd:    &shared.Command{Type: shared.CmdZMPop, Args: [][]byte{[]byte("1"), []byte("zmpop_nosuchkey"), []byte("MIN")}},
					expect: "*-1\r\n",
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
