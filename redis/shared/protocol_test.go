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

package shared

import (
	"bytes"
	"testing"
)

func TestParserPreservesCommandArgsCapacity(t *testing.T) {
	const iterations = 32
	input := []byte("*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n")
	parser := NewParser(bytes.NewReader(input))
	cmd := GetCommand()
	defer PutCommand(cmd)
	wantCap := cap(cmd.Args)

	for i := range iterations {
		if i > 0 {
			cmd.Reset()
			parser.Reset(bytes.NewReader(input))
		}
		if _, err := parser.ReadCommandInto(cmd); err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if cap(cmd.Args) != wantCap {
			t.Fatalf("iteration %d: Args capacity = %d, want %d", i, cap(cmd.Args), wantCap)
		}
		if cmd.Type != CmdGet || len(cmd.Args) != 1 || string(cmd.Args[0]) != "mykey" {
			t.Fatalf("iteration %d: parsed command = %#v", i, cmd)
		}
	}
}

func TestParserPreservesUnknownCommandName(t *testing.T) {
	input := []byte("*2\r\n$7\r\nMYSTERY\r\n$3\r\narg\r\n")
	parser := NewParser(bytes.NewReader(input))
	cmd := GetCommand()
	defer PutCommand(cmd)

	if _, err := parser.ReadCommandInto(cmd); err != nil {
		t.Fatal(err)
	}
	if cmd.Type != CmdUnknown || string(cmd.RawName) != "MYSTERY" {
		t.Fatalf("unknown command name = %q, type = %v", cmd.RawName, cmd.Type)
	}
	if len(cmd.Args) != 1 || string(cmd.Args[0]) != "arg" {
		t.Fatalf("arguments = %q", cmd.Args)
	}
}
