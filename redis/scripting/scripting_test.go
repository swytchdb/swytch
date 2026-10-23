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
	"bytes"
	"testing"
)

func TestScriptRegistry(t *testing.T) {
	r := NewScriptRegistry()

	// Test Load and Get
	script := []byte("return 1")

	sha := r.Load(script)
	if sha == "" {
		t.Error("Load returned empty SHA")
	}

	// SHA should be 40 hex characters
	if len(sha) != 40 {
		t.Errorf("SHA length should be 40, got %d", len(sha))
	}

	// Get should return the script
	got, ok := r.Get(sha)
	if !ok {
		t.Error("Get returned false for loaded script")
	}

	if !bytes.Equal(got, script) {
		t.Error("Get returned different script")
	}

	// Get for unknown SHA should return false
	_, ok = r.Get("0000000000000000000000000000000000000000")
	if ok {
		t.Error("Get returned true for unknown SHA")
	}

	// Test Exists
	results := r.Exists([]string{sha, "0000000000000000000000000000000000000000"})
	if !results[0] {
		t.Error("Exists returned false for loaded script")
	}

	if results[1] {
		t.Error("Exists returned true for unknown script")
	}

	// Test Flush
	r.Flush()

	_, ok = r.Get(sha)
	if ok {
		t.Error("Get returned true after Flush")
	}
}

func TestLuaVMPool(t *testing.T) {
	pool := NewLuaVMPool(2)
	defer pool.Close()

	// Get a VM
	L1 := pool.Get()
	if L1 == nil {
		t.Fatal("Get returned nil VM")
	}

	// Get another VM
	L2 := pool.Get()
	if L2 == nil {
		t.Fatal("Get returned nil VM")
	}

	// Put them back
	pool.Put(L1)
	pool.Put(L2)

	// Get should return one of them
	L3 := pool.Get()
	if L3 == nil {
		t.Fatal("Get returned nil VM after Put")
	}

	pool.Put(L3)
}

func TestFunctionRegistry(t *testing.T) {
	t.Run("NewFunctionRegistry", func(t *testing.T) {
		r := NewFunctionRegistry()
		if r == nil {
			t.Fatal("NewFunctionRegistry returned nil")
		}

		if r.Count() != 0 {
			t.Errorf("expected 0 functions, got %d", r.Count())
		}

		if r.LibraryCount() != 0 {
			t.Errorf("expected 0 libraries, got %d", r.LibraryCount())
		}
	})

	t.Run("Load and Get", func(t *testing.T) {
		r := NewFunctionRegistry()
		lib := &Library{
			Name:   "mylib",
			Engine: "lua",
			Source: "#!lua name=mylib\nredis.register_function('hello', function() return 'world' end)",
			Functions: map[string]*FunctionDef{
				"hello": {Name: "hello"},
			},
		}

		err := r.Load(lib, false)
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}

		if r.Count() != 1 {
			t.Errorf("expected 1 function, got %d", r.Count())
		}

		if r.LibraryCount() != 1 {
			t.Errorf("expected 1 library, got %d", r.LibraryCount())
		}

		funcDef, lib2 := r.GetFunction("hello")
		if funcDef == nil || lib2 == nil {
			t.Fatal("GetFunction returned nil")
		}

		if funcDef.Name != "hello" {
			t.Errorf("expected function name 'hello', got '%s'", funcDef.Name)
		}

		if lib2.Name != "mylib" {
			t.Errorf("expected library name 'mylib', got '%s'", lib2.Name)
		}
	})

	t.Run("Load duplicate library", func(t *testing.T) {
		r := NewFunctionRegistry()
		lib := &Library{
			Name:   "mylib",
			Engine: "lua",
			Functions: map[string]*FunctionDef{
				"hello": {Name: "hello"},
			},
		}

		err := r.Load(lib, false)
		if err != nil {
			t.Fatalf("first Load failed: %v", err)
		}

		// Try loading again without replace
		err = r.Load(lib, false)
		if err == nil {
			t.Error("expected error for duplicate library")
		}

		// Now try with replace
		err = r.Load(lib, true)
		if err != nil {
			t.Fatalf("Load with replace failed: %v", err)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		r := NewFunctionRegistry()
		lib := &Library{
			Name:   "mylib",
			Engine: "lua",
			Functions: map[string]*FunctionDef{
				"hello": {Name: "hello"},
			},
		}

		if err := r.Load(lib, false); err != nil {
			t.Fatal(err)
		}

		if r.LibraryCount() != 1 {
			t.Errorf("expected 1 library, got %d", r.LibraryCount())
		}

		deleted := r.Delete("mylib")
		if !deleted {
			t.Error("Delete returned false")
		}

		if r.LibraryCount() != 0 {
			t.Errorf("expected 0 libraries after delete, got %d", r.LibraryCount())
		}

		if r.Count() != 0 {
			t.Errorf("expected 0 functions after delete, got %d", r.Count())
		}

		// Try deleting again
		deleted = r.Delete("mylib")
		if deleted {
			t.Error("Delete of non-existent library returned true")
		}
	})

	t.Run("Flush", func(t *testing.T) {
		r := NewFunctionRegistry()

		for i := range 5 {
			lib := &Library{
				Name:   "lib" + string(rune('a'+i)),
				Engine: "lua",
				Functions: map[string]*FunctionDef{
					"func" + string(rune('a'+i)): {Name: "func" + string(rune('a'+i))},
				},
			}
			if err := r.Load(lib, false); err != nil {
				t.Fatal(err)
			}
		}

		if r.LibraryCount() != 5 {
			t.Errorf("expected 5 libraries, got %d", r.LibraryCount())
		}

		r.Flush()

		if r.LibraryCount() != 0 {
			t.Errorf("expected 0 libraries after flush, got %d", r.LibraryCount())
		}

		if r.Count() != 0 {
			t.Errorf("expected 0 functions after flush, got %d", r.Count())
		}
	})

	t.Run("List with pattern", func(t *testing.T) {
		r := NewFunctionRegistry()

		for _, name := range []string{"mylib", "mylib2", "otherlib"} {
			lib := &Library{
				Name:   name,
				Engine: "lua",
				Functions: map[string]*FunctionDef{
					"func_" + name: {Name: "func_" + name},
				},
			}
			if err := r.Load(lib, false); err != nil {
				t.Fatal(err)
			}
		}

		// List all
		libs := r.List("")
		if len(libs) != 3 {
			t.Errorf("expected 3 libraries, got %d", len(libs))
		}

		// List with pattern
		libs = r.List("mylib*")
		if len(libs) != 2 {
			t.Errorf("expected 2 libraries matching 'mylib*', got %d", len(libs))
		}

		// List exact match
		libs = r.List("otherlib")
		if len(libs) != 1 {
			t.Errorf("expected 1 library matching 'otherlib', got %d", len(libs))
		}
	})
}

func TestParseShebang(t *testing.T) {
	tests := []struct {
		name       string
		source     string
		wantEngine string
		wantLib    string
		wantErr    bool
	}{
		{
			name:       "valid shebang",
			source:     "#!lua name=mylib\nlocal x = 1",
			wantEngine: "lua",
			wantLib:    "mylib",
			wantErr:    false,
		},
		{
			name:       "valid shebang with extra whitespace",
			source:     "#!lua   name=mylib  \nlocal x = 1",
			wantEngine: "lua",
			wantLib:    "mylib",
			wantErr:    false,
		},
		{
			name:    "missing shebang",
			source:  "local x = 1",
			wantErr: true,
		},
		{
			name:    "missing engine",
			source:  "#! name=mylib\nlocal x = 1",
			wantErr: true,
		},
		{
			name:    "missing library name",
			source:  "#!lua\nlocal x = 1",
			wantErr: true,
		},
		{
			name:    "unsupported engine",
			source:  "#!python name=mylib\nprint('hello')",
			wantErr: true,
		},
		{
			name:    "empty source",
			source:  "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine, libName, err := parseShebang(tt.source)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if engine != tt.wantEngine {
				t.Errorf("expected engine '%s', got '%s'", tt.wantEngine, engine)
			}

			if libName != tt.wantLib {
				t.Errorf("expected libName '%s', got '%s'", tt.wantLib, libName)
			}
		})
	}
}
