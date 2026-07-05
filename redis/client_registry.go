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
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/swytchdb/swytch/redis/shared"
)

// ClientInfo holds per-client state for CLIENT LIST reporting
type ClientInfo struct {
	ID         int64
	Addr       string
	Name       string
	CreatedAt  time.Time
	LastActive time.Time
	DB         int
	Flags      string
	LastCmd    atomic.Pointer[string]
	Conn       *shared.Connection
}

// ClientRegistry tracks all active client connections
type ClientRegistry struct {
	mu      xsync.RBMutex
	clients map[int64]*ClientInfo
	nextID  atomic.Int64
	byConn  map[*shared.Connection]*ClientInfo
}

// NewClientRegistry creates a new client registry
func NewClientRegistry() *ClientRegistry {
	return &ClientRegistry{
		clients: make(map[int64]*ClientInfo),
		byConn:  make(map[*shared.Connection]*ClientInfo),
	}
}

// Register adds a new client connection and returns its ClientInfo
func (r *ClientRegistry) Register(conn *shared.Connection) *ClientInfo {
	id := r.nextID.Add(1)
	now := time.Now()

	info := &ClientInfo{
		ID:         id,
		Addr:       conn.RemoteAddr,
		CreatedAt:  now,
		LastActive: now,
		DB:         conn.SelectedDB,
		Conn:       conn,
	}

	r.mu.Lock()
	r.clients[id] = info
	r.byConn[conn] = info
	r.mu.Unlock()

	return info
}

// Unregister removes a client connection
func (r *ClientRegistry) Unregister(conn *shared.Connection) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if info, ok := r.byConn[conn]; ok {
		delete(r.clients, info.ID)
		delete(r.byConn, conn)
	}
}

// GetByConn returns the ClientInfo for a connection
func (r *ClientRegistry) GetByConn(conn *shared.Connection) *ClientInfo {
	token := r.mu.RLock()
	defer r.mu.RUnlock(token)
	return r.byConn[conn]
}

// RecordCommand updates last-command tracking for CLIENT LIST. It stores an
// interned name pointer, so the per-command path performs no allocation, no
// registry lookup, and no locking.
func (info *ClientInfo) RecordCommand(t shared.CommandType) {
	info.LastCmd.Store(t.StringPtr())
	info.LastActive = time.Now()
}

// buildFlags builds the flags string for a client info entry
func buildFlags(info *ClientInfo) string {
	flags := "N"
	if info.Conn != nil && info.Conn.IsBlocked() {
		flags = "b"
	}
	if info.Conn != nil {
		if info.Conn.InTransaction {
			if flags == "N" {
				flags = "x"
			} else {
				flags += "x"
			}
		}
		if info.Conn.TxnAborted {
			if flags == "N" {
				flags = "d"
			} else {
				flags += "d"
			}
		}
	}
	return flags
}

// FormatClientList returns the CLIENT LIST output string
func (r *ClientRegistry) FormatClientList() string {
	token := r.mu.RLock()
	defer r.mu.RUnlock(token)

	var sb strings.Builder
	now := time.Now()

	for _, info := range r.clients {
		age := int64(now.Sub(info.CreatedAt).Seconds())
		idle := int64(now.Sub(info.LastActive).Seconds())

		flags := buildFlags(info)

		// Get last command
		lastCmd := "NULL"
		if cmd := info.LastCmd.Load(); cmd != nil {
			lastCmd = *cmd
		}

		// Get client name
		name := info.Name
		if info.Conn != nil && info.Conn.ClientName != "" {
			name = info.Conn.ClientName
		}

		// Get current DB
		db := info.DB
		if info.Conn != nil {
			db = info.Conn.SelectedDB
		}

		// Number of queued commands in MULTI
		multi := -1
		watch := 0
		if info.Conn != nil && info.Conn.InTransaction {
			multi = len(info.Conn.QueuedCmds)
			watch = len(info.Conn.WatchedKeys)
		}

		fmt.Fprintf(&sb, "id=%d addr=%s fd=%d name=%s age=%d idle=%d flags=%s db=%d sub=0 psub=0 multi=%d watch=%d qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=%s\r\n",
			info.ID, info.Addr, 0, name, age, idle, flags, db, multi, watch, lastCmd)
	}

	return sb.String()
}

// Count returns the number of registered clients
func (r *ClientRegistry) Count() int {
	token := r.mu.RLock()
	defer r.mu.RUnlock(token)
	return len(r.clients)
}

// CountBlocked returns the number of blocked clients
func (r *ClientRegistry) CountBlocked() int {
	token := r.mu.RLock()
	defer r.mu.RUnlock(token)

	count := 0
	for _, info := range r.clients {
		if info.Conn != nil && info.Conn.IsBlocked() {
			count++
		}
	}
	return count
}

// formatClientInfo returns a single client info line
func (r *ClientRegistry) formatClientInfo(info *ClientInfo) string {
	now := time.Now()
	age := int64(now.Sub(info.CreatedAt).Seconds())
	idle := int64(now.Sub(info.LastActive).Seconds())

	flags := buildFlags(info)

	// Get last command
	lastCmd := "NULL"
	if cmd := info.LastCmd.Load(); cmd != nil {
		lastCmd = *cmd
	}

	// Get client name
	name := info.Name
	if info.Conn != nil && info.Conn.ClientName != "" {
		name = info.Conn.ClientName
	}

	// Get current DB
	db := info.DB
	if info.Conn != nil {
		db = info.Conn.SelectedDB
	}

	// Number of queued commands in MULTI
	multi := -1
	watch := 0
	if info.Conn != nil && info.Conn.InTransaction {
		multi = len(info.Conn.QueuedCmds)
		watch = len(info.Conn.WatchedKeys)
	}

	return fmt.Sprintf("id=%d addr=%s fd=%d name=%s age=%d idle=%d flags=%s db=%d sub=0 psub=0 multi=%d watch=%d qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=%s\r\n",
		info.ID, info.Addr, 0, name, age, idle, flags, db, multi, watch, lastCmd)
}

// GetByID returns the ClientInfo for a client ID
func (r *ClientRegistry) GetByID(id int64) *ClientInfo {
	token := r.mu.RLock()
	defer r.mu.RUnlock(token)
	return r.clients[id]
}

// Unblock unblocks a blocked client by ID. Returns true if the client was unblocked.
// If withError is true, the client receives an error message instead of nil.
func (r *ClientRegistry) Unblock(clientID int64, withError bool) bool {
	token := r.mu.RLock()
	info := r.clients[clientID]
	r.mu.RUnlock(token)

	if info == nil || info.Conn == nil {
		return false
	}

	return info.Conn.Unblock(withError)
}
