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
	"crypto/sha1"
	"encoding/hex"

	"github.com/puzpuzpuz/xsync/v4"
)

// ScriptRegistry provides thread-safe storage for Lua scripts indexed by SHA1 hash.
type ScriptRegistry struct {
	mu      xsync.RBMutex
	scripts map[string][]byte // SHA1 -> script source
}

// NewScriptRegistry creates a new script registry.
func NewScriptRegistry() *ScriptRegistry {
	return &ScriptRegistry{
		scripts: make(map[string][]byte),
	}
}

// Load adds a script to the registry and returns its SHA1 hash.
func (r *ScriptRegistry) Load(script []byte) string {
	sha := computeSHA1(script)

	r.mu.Lock()
	defer r.mu.Unlock()

	// Store a copy of the script
	scriptCopy := make([]byte, len(script))
	copy(scriptCopy, script)
	r.scripts[sha] = scriptCopy

	return sha
}

// Get retrieves a script by its SHA1 hash.
func (r *ScriptRegistry) Get(sha1Hash string) ([]byte, bool) {
	token := r.mu.RLock()
	defer r.mu.RUnlock(token)

	script, ok := r.scripts[sha1Hash]

	return script, ok
}

// Exists checks if scripts exist in the registry.
// Returns a slice of booleans corresponding to each SHA1 hash.
func (r *ScriptRegistry) Exists(sha1Hashes []string) []bool {
	token := r.mu.RLock()
	defer r.mu.RUnlock(token)

	results := make([]bool, len(sha1Hashes))
	for i, sha := range sha1Hashes {
		_, results[i] = r.scripts[sha]
	}

	return results
}

// Flush removes all scripts from the registry.
func (r *ScriptRegistry) Flush() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.scripts = make(map[string][]byte)
}

// Count returns the number of scripts in the registry.
func (r *ScriptRegistry) Count() int {
	token := r.mu.RLock()
	defer r.mu.RUnlock(token)

	return len(r.scripts)
}

// computeSHA1 computes the SHA1 hash of data and returns it as a lowercase hex string.
func computeSHA1(data []byte) string {
	h := sha1.New()
	h.Write(data)

	return hex.EncodeToString(h.Sum(nil))
}
