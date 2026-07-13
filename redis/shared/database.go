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

// Database is the handle threaded through command handlers. Its only
// remaining job is reaching the blocking-command subscription registry;
// actual state lives in the effects engine.
type Database struct {
	manager *DatabaseManager
}

// Manager returns the owning DatabaseManager.
func (db *Database) Manager() *DatabaseManager {
	return db.manager
}

// DatabaseManager owns the Database and the blocking-command wake registry.
type DatabaseManager struct {
	db *Database

	// Subscriptions manages blocking command wake signals. Waiters register
	// locally; wake signals arrive through the engine's OnKeyDataAdded /
	// OnKeyDeleted / OnFlushAll callbacks, which fire for both local flushes
	// and remote effect arrivals.
	Subscriptions *SubscriptionManager[struct{}]
}

// NewDatabaseManager creates a new database manager.
func NewDatabaseManager() *DatabaseManager {
	dm := &DatabaseManager{
		Subscriptions: NewSubscriptionManager[struct{}](),
	}
	dm.db = &Database{manager: dm}
	return dm
}

// DB returns the database.
func (dm *DatabaseManager) DB() *Database {
	return dm.db
}
