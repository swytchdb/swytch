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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/swytchdb/swytch/cache"
	"github.com/swytchdb/swytch/keytrie"
)

// DefaultNumDatabases is the default number of Redis databases
const DefaultNumDatabases = 16

// KeyEvent represents the type of change that occurred to a key
type KeyEvent int

const (
	KeyEventSet    KeyEvent = iota // Key was created or updated
	KeyEventDelete                 // Key was deleted
)

// KeyChangeCallback is called when a key is modified
// dbIndex: the database index
// key: the key that changed
// event: the type of change
// value: the new value (nil for delete)
type KeyChangeCallback func(dbIndex int, key string, event KeyEvent)

// dbKey creates a composite key from database index and key
// Format: "db:key" where db is the database index
func dbKey(dbIndex int, key string) string {
	// Fast path for db 0 (most common)
	if dbIndex == 0 {
		return key
	}
	// Use strconv for efficiency
	return strconv.Itoa(dbIndex) + ":" + key
}

// userKey extracts the user-facing key from an internal key
func userKey(dbIndex int, internalKey string) string {
	// Fast path for db 0 (most common) - no prefix
	if dbIndex == 0 {
		return internalKey
	}
	// Strip "N:" prefix
	colonIdx := strings.Index(internalKey, ":")
	if colonIdx > 0 {
		return internalKey[colonIdx+1:]
	}
	return internalKey
}

// Database represents a single Redis database (logical view into shared cache)
type Database struct {
	index         int
	flushAt       atomic.Int64       // Unix nano timestamp - items created before this are considered flushed
	expirationMgr *ExpirationManager // For scheduling short-TTL keys
	onChange      KeyChangeCallback  // Called when a key is set or deleted
	manager       *DatabaseManager   // Back-reference to owning manager
}

// newDatabaseWithKeys creates a new database view into a shared cache with an optional shared keytrie.
// If sharedKeys is provided, it is used for KEYS/SCAN support (used with TieredCache).
// If sharedKeys is nil, a new keytrie is created (used with CloxCache).
func newDatabaseWithKeys(index int, sharedKeys keytrie.KeyIndex, expMgr *ExpirationManager, onChange KeyChangeCallback, dm *DatabaseManager) *Database {
	keys := sharedKeys
	if keys == nil {
		keys = keytrie.New() // Lock-free trie for performance
	}
	return &Database{
		index:         index,
		expirationMgr: expMgr,
		onChange:      onChange,
		manager:       dm,
	}
}

// Manager returns the owning DatabaseManager.
func (db *Database) Manager() *DatabaseManager {
	return db.manager
}

// GetLock returns a striped lock for the given key.
func (db *Database) GetLock(key string) *sync.Mutex {
	return db.manager.GetLock(key)
}

// MakeKey creates the internal key for this database
func (db *Database) MakeKey(key string) string {
	return dbKey(db.index, key)
}

// EvictCacheOnly removes a key from the cache without touching the keytrie.
// Used for cache invalidation when the key should be reconstructed from effects
// on next access. Does NOT fire onChange callback.
func (db *Database) EvictCacheOnly(key string) bool {
	return false
}

// Stats returns cache statistics
func (db *Database) Stats() (hits, misses, evictions uint64) {
	return 0, 0, 0
}

// AdaptiveStats returns per-shard adaptive statistics
func (db *Database) AdaptiveStats() []cache.AdaptiveStats {
	return nil
}

// ItemCount returns the raw item count (may include expired items)
func (db *Database) ItemCount() int {
	return 0
}

// Evictions returns the total eviction count
func (db *Database) Evictions() uint64 {
	return 0
}

// Close is a no-op for individual databases (cache is shared)
func (db *Database) Close() {
	// No-op - cache is shared and closed by DatabaseManager
}

// DatabaseManager manages multiple databases with a single shared cache
type DatabaseManager struct {
	databases     []*Database
	numDBs        int
	expirationMgr *ExpirationManager
	onChange      KeyChangeCallback // Called when any key changes in any database

	// Striped locks for atomic operations
	locks [4096]sync.Mutex

	// Subscriptions manages blocking command wake signals
	Subscriptions *SubscriptionManager[struct{}]
}

// GetLock returns a striped lock for the given key using FNV-1a hashing.
func (dm *DatabaseManager) GetLock(key string) *sync.Mutex {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return &dm.locks[hash&4095]
}

// DatabaseManagerConfig holds configuration for DatabaseManager
type DatabaseManagerConfig struct {
	NumDBs        int
	TotalCapacity int
	MemoryLimit   int64 // Memory limit in bytes (0 = use count-based eviction only)
}

// NewDatabaseManagerWithConfig creates a new database manager with the given configuration
func NewDatabaseManagerWithConfig(cfg DatabaseManagerConfig) *DatabaseManager {
	if cfg.NumDBs <= 0 {
		cfg.NumDBs = DefaultNumDatabases
	}

	// Create keytries for each database upfront
	// These will be populated during rebuild for TieredCache
	keyTries := make([]keytrie.KeyIndex, cfg.NumDBs)
	for i := 0; i < cfg.NumDBs; i++ {
		keyTries[i] = keytrie.New()
	}

	dm := &DatabaseManager{
		databases:     make([]*Database, cfg.NumDBs),
		numDBs:        cfg.NumDBs,
		Subscriptions: NewSubscriptionManager[struct{}](),
	}

	// Create database views into shared cache
	// Each database gets its own keytrie (populated during rebuild for TieredCache)
	for i := 0; i < cfg.NumDBs; i++ {
		dm.databases[i] = newDatabaseWithKeys(i, keyTries[i], nil, func(dbIndex int, key string, event KeyEvent) {
			if dm.onChange != nil {
				dm.onChange(dbIndex, key, event)
			}
		}, dm)
	}

	return dm
}

// SetOnChange registers a callback to be called when any key changes
func (dm *DatabaseManager) SetOnChange(cb KeyChangeCallback) {
	dm.onChange = cb
}

// GetDB returns the database at the given index
func (dm *DatabaseManager) GetDB(index int) *Database {
	if index < 0 || index >= dm.numDBs {
		return nil
	}
	return dm.databases[index]
}

// NumDatabases returns the number of databases
func (dm *DatabaseManager) NumDatabases() int {
	return dm.numDBs
}

// SwapDB atomically swaps two databases
// The swap is metadata-only - no data is moved in the cache since keys are prefixed with db.index
func (dm *DatabaseManager) SwapDB(db1, db2 int) error {
	if db1 < 0 || db1 >= dm.numDBs || db2 < 0 || db2 >= dm.numDBs {
		return fmt.Errorf("ERR DB index is out of range")
	}
	if db1 == db2 {
		return nil // no-op
	}
	// Swap pointers - each Database keeps its index field, so keys remain correctly prefixed
	dm.databases[db1], dm.databases[db2] = dm.databases[db2], dm.databases[db1]
	return nil
}

// TotalItemCount returns the raw item count (shared cache)
func (dm *DatabaseManager) TotalItemCount() int {
	return 0
}

// TotalEvictions returns total evictions (shared cache)
func (dm *DatabaseManager) TotalEvictions() uint64 {
	return 0
}

// GetCacheBytes returns current memory usage in bytes
func (dm *DatabaseManager) GetCacheBytes() int64 {
	return 0
}

// GetMemoryLimit returns the configured memory limit in bytes (0 = unlimited)
func (dm *DatabaseManager) GetMemoryLimit() int64 {
	return 0
}

// Close releases resources for the shared cache
func (dm *DatabaseManager) Close() {
	// Stop active expiration first
	if dm.expirationMgr != nil {
		dm.expirationMgr.Stop()
	}
}
