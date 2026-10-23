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

package scripting

import (
	"testing"

	lua "github.com/yuin/gopher-lua"
)

func TestStructPack(t *testing.T) {
	L := lua.NewState()
	defer L.Close()

	RegisterStructLib(L)

	tests := []struct {
		name     string
		luaCode  string
		expected string
	}{
		{
			name:     "pack unsigned byte",
			luaCode:  `return struct.pack("B", 255)`,
			expected: "\xff",
		},
		{
			name:     "pack signed byte",
			luaCode:  `return struct.pack("b", -1)`,
			expected: "\xff",
		},
		{
			name:     "pack little-endian short",
			luaCode:  `return struct.pack("<H", 0x0102)`,
			expected: "\x02\x01",
		},
		{
			name:     "pack big-endian short",
			luaCode:  `return struct.pack(">H", 0x0102)`,
			expected: "\x01\x02",
		},
		{
			name:     "pack little-endian long",
			luaCode:  `return struct.pack("<L", 0x01020304)`,
			expected: "\x04\x03\x02\x01",
		},
		{
			name:     "pack big-endian long",
			luaCode:  `return struct.pack(">L", 0x01020304)`,
			expected: "\x01\x02\x03\x04",
		},
		{
			name:     "pack zero-terminated string",
			luaCode:  `return struct.pack("s", "hello")`,
			expected: "hello\x00",
		},
		{
			name:     "pack fixed char sequence",
			luaCode:  `return struct.pack("c5", "hi")`,
			expected: "hi\x00\x00\x00",
		},
		{
			name:     "pack multiple values",
			luaCode:  `return struct.pack("<BH", 1, 2)`,
			expected: "\x01\x02\x00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := L.DoString(tt.luaCode); err != nil {
				t.Fatalf("DoString failed: %v", err)
			}

			result := L.Get(-1)
			L.Pop(1)

			if str, ok := result.(lua.LString); ok {
				if string(str) != tt.expected {
					t.Errorf("got %q, want %q", string(str), tt.expected)
				}
			} else {
				t.Errorf("expected LString, got %T", result)
			}
		})
	}
}

func TestStructUnpack(t *testing.T) {
	L := lua.NewState()
	defer L.Close()

	RegisterStructLib(L)

	tests := []struct {
		name     string
		luaCode  string
		expected []any
	}{
		{
			name:     "unpack unsigned byte",
			luaCode:  `return struct.unpack("B", string.char(255))`,
			expected: []any{float64(255), float64(2)},
		},
		{
			name:     "unpack signed byte",
			luaCode:  `return struct.unpack("b", string.char(255))`,
			expected: []any{float64(-1), float64(2)},
		},
		{
			name:     "unpack little-endian short",
			luaCode:  `return struct.unpack("<H", string.char(2, 1))`,
			expected: []any{float64(0x0102), float64(3)},
		},
		{
			name:     "unpack big-endian short",
			luaCode:  `return struct.unpack(">H", string.char(1, 2))`,
			expected: []any{float64(0x0102), float64(3)},
		},
		{
			name:     "unpack little-endian long",
			luaCode:  `return struct.unpack("<L", string.char(4, 3, 2, 1))`,
			expected: []any{float64(0x01020304), float64(5)},
		},
		{
			name:     "unpack zero-terminated string",
			luaCode:  `return struct.unpack("s", "hello" .. string.char(0) .. "world")`,
			expected: []any{"hello", float64(7)},
		},
		{
			name:     "unpack fixed char sequence",
			luaCode:  `return struct.unpack("c5", "hello")`,
			expected: []any{"hello", float64(6)},
		},
		{
			name:     "unpack with starting position",
			luaCode:  `return struct.unpack("B", string.char(1, 2, 3), 2)`,
			expected: []any{float64(2), float64(3)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := L.DoString(tt.luaCode); err != nil {
				t.Fatalf("DoString failed: %v", err)
			}
			// Get all returned values
			top := L.GetTop()
			if top != len(tt.expected) {
				L.SetTop(0)
				t.Fatalf("got %d return values, want %d", top, len(tt.expected))
			}

			for i := 1; i <= top; i++ {
				result := L.Get(i)
				switch expected := tt.expected[i-1].(type) {
				case float64:
					if num, ok := result.(lua.LNumber); ok {
						if float64(num) != expected {
							t.Errorf("value %d: got %v, want %v", i, float64(num), expected)
						}
					} else {
						t.Errorf("value %d: expected LNumber, got %T", i, result)
					}
				case string:
					if str, ok := result.(lua.LString); ok {
						if string(str) != expected {
							t.Errorf("value %d: got %q, want %q", i, string(str), expected)
						}
					} else {
						t.Errorf("value %d: expected LString, got %T", i, result)
					}
				}
			}

			L.SetTop(0)
		})
	}
}

func TestStructSize(t *testing.T) {
	L := lua.NewState()
	defer L.Close()

	RegisterStructLib(L)

	tests := []struct {
		name     string
		luaCode  string
		expected int
	}{
		{
			name:     "size of byte",
			luaCode:  `return struct.size("B")`,
			expected: 1,
		},
		{
			name:     "size of short",
			luaCode:  `return struct.size("H")`,
			expected: 2,
		},
		{
			name:     "size of long",
			luaCode:  `return struct.size("L")`,
			expected: 4,
		},
		{
			name:     "size of multiple",
			luaCode:  `return struct.size("BHL")`,
			expected: 7,
		},
		{
			name:     "size of fixed char",
			luaCode:  `return struct.size("c10")`,
			expected: 10,
		},
		{
			name:     "size of i4",
			luaCode:  `return struct.size("i4")`,
			expected: 4,
		},
		{
			name:     "size of double",
			luaCode:  `return struct.size("d")`,
			expected: 8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := L.DoString(tt.luaCode); err != nil {
				t.Fatalf("DoString failed: %v", err)
			}

			result := L.Get(-1)
			L.Pop(1)

			if num, ok := result.(lua.LNumber); ok {
				if int(num) != tt.expected {
					t.Errorf("got %d, want %d", int(num), tt.expected)
				}
			} else {
				t.Errorf("expected LNumber, got %T", result)
			}
		})
	}
}

func TestStructRoundTrip(t *testing.T) {
	L := lua.NewState()
	defer L.Close()

	RegisterStructLib(L)

	// Test that pack followed by unpack returns the original values
	luaCode := `
		local packed = struct.pack("<BHLs", 42, 1000, 100000, "test")
		local a, b, c, d, pos = struct.unpack("<BHLs", packed)
		return a, b, c, d
	`
	if err := L.DoString(luaCode); err != nil {
		t.Fatalf("DoString failed: %v", err)
	}

	expected := []float64{42, 1000, 100000}

	for i := 1; i <= 3; i++ {
		result := L.Get(i)
		if num, ok := result.(lua.LNumber); ok {
			if float64(num) != expected[i-1] {
				t.Errorf("value %d: got %v, want %v", i, float64(num), expected[i-1])
			}
		}
	}

	result := L.Get(4)
	if str, ok := result.(lua.LString); ok {
		if string(str) != "test" {
			t.Errorf("string value: got %q, want %q", string(str), "test")
		}
	}
}

func TestStructInVM(t *testing.T) {
	// Test that struct is available in the VM pool
	pool := NewLuaVMPool(1)
	defer pool.Close()

	L := pool.Get()
	defer pool.Put(L)

	// The struct library should already be registered
	luaCode := `return struct.pack("B", 42)`
	if err := L.DoString(luaCode); err != nil {
		t.Fatalf("struct not available in VM pool: %v", err)
	}

	result := L.Get(-1)
	L.Pop(1)

	if str, ok := result.(lua.LString); ok {
		if string(str) != "*" {
			t.Errorf("got %q, want %q", string(str), "*")
		}
	}
}
