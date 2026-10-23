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
	"fmt"

	"github.com/puzpuzpuz/xsync/v4"
)

// FunctionDef represents a registered Lua function within a library.
type FunctionDef struct {
	Name        string   // Function name
	Description string   // Optional description
	Flags       []string // Function flags (e.g., "no-writes", "allow-stale")
}

// Library represents a loaded Lua library containing one or more functions.
type Library struct {
	Name      string                  // Library name (from shebang)
	Engine    string                  // Engine type (always "lua")
	Source    string                  // Original Lua source code
	Functions map[string]*FunctionDef // function name -> FunctionDef
}

// FunctionRegistry provides thread-safe storage for Lua function libraries.
type FunctionRegistry struct {
	mu        xsync.RBMutex
	libraries map[string]*Library // libname -> Library
	funcIndex map[string]string   // funcname -> libname (fast lookup)
}

// NewFunctionRegistry creates a new function registry.
func NewFunctionRegistry() *FunctionRegistry {
	return &FunctionRegistry{
		libraries: make(map[string]*Library),
		funcIndex: make(map[string]string),
	}
}

// Load adds or replaces a library in the registry.
// Returns an error if the library already exists and replace is false,
// or if any function name conflicts with another library.
func (r *FunctionRegistry) Load(lib *Library, replace bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if library exists
	existingLib, exists := r.libraries[lib.Name]
	if exists && !replace {
		return fmt.Errorf("Library '%s' already exists", lib.Name)
	}

	// Check for function name conflicts with other libraries
	for funcName := range lib.Functions {
		if existingLibName, exists := r.funcIndex[funcName]; exists {
			if existingLibName != lib.Name {
				return fmt.Errorf("Function '%s' already exists in library '%s'", funcName, existingLibName) //nolint:staticcheck // Redis protocol error message
			}
		}
	}

	// If replacing, remove old function index entries
	if existingLib != nil {
		for funcName := range existingLib.Functions {
			delete(r.funcIndex, funcName)
		}
	}

	// Store the library
	r.libraries[lib.Name] = lib

	// Index all functions
	for funcName := range lib.Functions {
		r.funcIndex[funcName] = lib.Name
	}

	return nil
}

// Delete removes a library by name.
// Returns true if the library was found and deleted.
func (r *FunctionRegistry) Delete(libName string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	lib, exists := r.libraries[libName]
	if !exists {
		return false
	}

	// Remove function index entries
	for funcName := range lib.Functions {
		delete(r.funcIndex, funcName)
	}

	// Remove the library
	delete(r.libraries, libName)

	return true
}

// Flush removes all libraries.
func (r *FunctionRegistry) Flush() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.libraries = make(map[string]*Library)
	r.funcIndex = make(map[string]string)
}

// GetFunction returns a function definition and its library source by function name.
// Returns nil if the function doesn't exist.
func (r *FunctionRegistry) GetFunction(funcName string) (*FunctionDef, *Library) {
	token := r.mu.RLock()
	defer r.mu.RUnlock(token)

	libName, exists := r.funcIndex[funcName]
	if !exists {
		return nil, nil
	}

	lib := r.libraries[libName]
	if lib == nil {
		return nil, nil
	}

	funcDef := lib.Functions[funcName]

	return funcDef, lib
}

// GetLibrary returns a library by name.
func (r *FunctionRegistry) GetLibrary(libName string) *Library {
	token := r.mu.RLock()
	defer r.mu.RUnlock(token)

	return r.libraries[libName]
}

// List returns all libraries, optionally filtered by a pattern.
// If pattern is empty, all libraries are returned.
func (r *FunctionRegistry) List(pattern string) []*Library {
	token := r.mu.RLock()
	defer r.mu.RUnlock(token)

	var result []*Library

	for _, lib := range r.libraries {
		if pattern == "" || matchGlobPattern(pattern, lib.Name) {
			result = append(result, lib)
		}
	}

	return result
}

// Count returns the total number of registered functions across all libraries.
func (r *FunctionRegistry) Count() int {
	token := r.mu.RLock()
	defer r.mu.RUnlock(token)

	return len(r.funcIndex)
}

// LibraryCount returns the number of loaded libraries.
func (r *FunctionRegistry) LibraryCount() int {
	token := r.mu.RLock()
	defer r.mu.RUnlock(token)

	return len(r.libraries)
}

// matchGlobPattern performs simple glob pattern matching.
// Supports * for any sequence of characters.
func matchGlobPattern(pattern, name string) bool {
	if pattern == "*" {
		return true
	}

	if pattern == name {
		return true
	}
	// Simple prefix matching for patterns like "foo*"
	if len(pattern) > 1 && pattern[len(pattern)-1] == '*' {
		prefix := pattern[:len(pattern)-1]

		return len(name) >= len(prefix) && name[:len(prefix)] == prefix
	}

	return false
}
