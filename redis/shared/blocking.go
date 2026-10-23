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
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/zeebo/xxh3"
)

var ErrNoChannels = errors.New("register called with zero channels")

/*
USAGE PATTERNS

=== Pattern 1: Wake-Signal-Only (for BLPOP/BRPOP where store is source of truth) ===

Use SubscriptionManager[struct{}] for wake signals only.

PRODUCER (LPUSH/RPUSH):

	store.Push(key, value)
	sm.Notify([]byte(key))  // wake oldest waiter on this key

CONSUMER (BLPOP/BRPOP):

	reg, _ := sm.Register(ctx, keys...)
	defer reg.Cancel()

	for {
		if k, v, ok := tryPop(keys...); ok {
			return k, v, nil
		}
		if _, err := reg.Wait(); err != nil {
			return nil, nil, err
		}
	}

Key behavior:
- Waiter stays in queue until Cancel() is called
- Multiple Notify() calls to same key wake the same oldest waiter
- Only Cancel() removes the waiter from all queues
*/

// Waiter represents a subscriber waiting for a wake signal
type Waiter[T any] struct {
	ch             chan struct{} // signals wake (buffered 1)
	alive          atomic.Bool   // false = canceled, skip in notify
	channels       [][]byte      // the channels this waiter is registered on
	sm             *SubscriptionManager[T]
	unblockOnNoKey bool // true if client wants to be unblocked when key is deleted
}

// Topic holds the waiting subscribers for a single key
type Topic[T any] struct {
	mu      sync.Mutex
	waiters []*Waiter[T] // FIFO queue of waiters
}

// notifyOldest signals the oldest live waiter without removing it.
// Returns true if a waiter was signaled.
func (t *Topic[T]) notifyOldest() bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, w := range t.waiters {
		if w != nil && w.alive.Load() {
			// Non-blocking send (channel is buffered 1)
			select {
			case w.ch <- struct{}{}:
			default:
				// Already has a pending signal, that's fine
			}

			return true
		}
	}

	return false
}

// remove removes a specific waiter from this topic
func (t *Topic[T]) remove(w *Waiter[T]) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i, waiter := range t.waiters {
		if waiter == w {
			// Remove by setting to nil; compaction happens lazily
			t.waiters[i] = nil

			return
		}
	}
}

// SubscriptionManager manages subscriptions for blocking commands
type SubscriptionManager[T any] struct {
	mu     xsync.RBMutex
	topics map[uint64]*Topic[T]
}

// NewSubscriptionManager creates a new subscription manager
func NewSubscriptionManager[T any]() *SubscriptionManager[T] {
	return &SubscriptionManager[T]{topics: make(map[uint64]*Topic[T])}
}

func (m *SubscriptionManager[T]) getTopic(hash uint64) *Topic[T] {
	token := m.mu.RLock()
	t := m.topics[hash]
	m.mu.RUnlock(token)

	if t != nil {
		return t
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if t = m.topics[hash]; t == nil {
		t = &Topic[T]{waiters: make([]*Waiter[T], 0, 4)}
		m.topics[hash] = t
	}

	return t
}

// Notify wakes the oldest waiting subscriber for this channel.
// Waiters remain in the queue until they call Cancel().
// Returns true if a waiter was signaled, false if no waiters.
func (m *SubscriptionManager[T]) Notify(channel []byte) bool {
	hash := xxh3.Hash(channel)

	token := m.mu.RLock()
	t := m.topics[hash]
	m.mu.RUnlock(token)

	if t == nil {
		return false
	}

	return t.notifyOldest()
}

// Enqueue is an alias for Notify (for compatibility)
func (m *SubscriptionManager[T]) Enqueue(channel []byte, _ T) bool {
	return m.Notify(channel)
}

// NotifyAllWaiters wakes ALL waiters for a specific channel.
// Used for operations like DEL, XGROUP DESTROY that invalidate the key/group.
func (m *SubscriptionManager[T]) NotifyAllWaiters(channel []byte) {
	hash := xxh3.Hash(channel)

	token := m.mu.RLock()
	t := m.topics[hash]
	m.mu.RUnlock(token)

	if t == nil {
		return
	}

	t.notifyAll()
}

// NotifyAll wakes ALL waiters across all topics.
// Used for operations like SWAPDB that may affect any blocked client.
func (m *SubscriptionManager[T]) NotifyAll() {
	token := m.mu.RLock()
	defer m.mu.RUnlock(token)

	for _, t := range m.topics {
		t.notifyAll()
	}
}

// BlockingKeyCount returns the number of keys that have at least one active waiter.
func (m *SubscriptionManager[T]) BlockingKeyCount() int64 {
	token := m.mu.RLock()
	defer m.mu.RUnlock(token)

	var count int64

	for _, t := range m.topics {
		if t.hasActiveWaiters() {
			count++
		}
	}

	return count
}

// BlockingKeyCounts returns the total number of blocking keys and the number of
// blocking keys where at least one client wants to be unblocked on key deletion (nokey).
func (m *SubscriptionManager[T]) BlockingKeyCounts() (total int64, nokey int64) {
	token := m.mu.RLock()
	defer m.mu.RUnlock(token)

	// Track unique keys and whether any waiter on that key has unblockOnNoKey set.
	// Skip internal subkeys (containing \x01 separator) — they are used for
	// internal notifications (e.g., XGROUP DESTROY) and should not be visible
	// to users through INFO.
	seenKeys := make(map[uint64]bool) // hash -> hasNoKeyWaiter

	for _, t := range m.topics {
		t.mu.Lock()
		for _, w := range t.waiters {
			if w != nil && w.alive.Load() {
				for _, ch := range w.channels {
					if bytes.ContainsRune(ch, '\x01') {
						continue
					}

					hash := xxh3.Hash(ch)
					if hasNoKey, seen := seenKeys[hash]; seen {
						// Already seen this key, update if this waiter has nokey flag
						if w.unblockOnNoKey && !hasNoKey {
							seenKeys[hash] = true
						}
					} else {
						seenKeys[hash] = w.unblockOnNoKey
					}
				}
			}
		}
		t.mu.Unlock()
	}

	total = int64(len(seenKeys))
	for _, hasNoKeyWaiter := range seenKeys {
		if hasNoKeyWaiter {
			nokey++
		}
	}

	return total, nokey
}

// notifyAll signals all live waiters in this topic
func (t *Topic[T]) notifyAll() {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, w := range t.waiters {
		if w != nil && w.alive.Load() {
			select {
			case w.ch <- struct{}{}:
			default:
			}
		}
	}
}

// hasActiveWaiters returns true if this topic has at least one active waiter
func (t *Topic[T]) hasActiveWaiters() bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, w := range t.waiters {
		if w != nil && w.alive.Load() {
			return true
		}
	}

	return false
}

// Registration represents a pending subscription
type Registration[T any] struct {
	sm       *SubscriptionManager[T]
	ctx      context.Context
	waiter   *Waiter[T]
	canceled atomic.Bool
}

// Register creates a waiter for the given channels without blocking.
// Call Cancel() when done, or use defer reg.Cancel().
func (m *SubscriptionManager[T]) Register(ctx context.Context, channels ...[]byte) (*Registration[T], error) {
	return m.RegisterWithOptions(ctx, false, channels...)
}

// RegisterWithNoKey creates a waiter that should be unblocked when the key is deleted.
// This is used for XREADGROUP with ">" where the client wants to know if the stream is deleted.
func (m *SubscriptionManager[T]) RegisterWithNoKey(ctx context.Context, channels ...[]byte) (*Registration[T], error) {
	return m.RegisterWithOptions(ctx, true, channels...)
}

// RegisterWithOptions creates a waiter with configurable options.
// unblockOnNoKey: if true, the client wants to be unblocked when the key is deleted.
func (m *SubscriptionManager[T]) RegisterWithOptions(ctx context.Context, unblockOnNoKey bool, channels ...[]byte) (*Registration[T], error) {
	if len(channels) == 0 {
		return nil, ErrNoChannels
	}

	w := &Waiter[T]{
		ch:             make(chan struct{}, 1),
		channels:       channels,
		sm:             m,
		unblockOnNoKey: unblockOnNoKey,
	}
	w.alive.Store(true)

	// Register waiter on all channels
	for _, ch := range channels {
		hash := xxh3.Hash(ch)
		topic := m.getTopic(hash)

		topic.mu.Lock()
		topic.waiters = append(topic.waiters, w)
		topic.mu.Unlock()
	}

	return &Registration[T]{
		sm:     m,
		ctx:    ctx,
		waiter: w,
	}, nil
}

// IsOldestWaiter checks if this waiter is first in line for the given channel.
func (r *Registration[T]) IsOldestWaiter(channel []byte) bool {
	hash := xxh3.Hash(channel)

	token := r.sm.mu.RLock()
	t := r.sm.topics[hash]
	r.sm.mu.RUnlock(token)

	if t == nil {
		return true
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	for _, w := range t.waiters {
		if w != nil && w.alive.Load() {
			return w == r.waiter
		}
	}

	return true
}

// Cancel marks the waiter as dead and removes it from all topics.
// Safe to call multiple times. Always call this (use defer).
func (r *Registration[T]) Cancel() {
	if r.canceled.Swap(true) {
		return
	}

	// Mark as dead so Notify skips this waiter
	r.waiter.alive.Store(false)

	// Remove from all topics
	for _, ch := range r.waiter.channels {
		hash := xxh3.Hash(ch)

		token := r.sm.mu.RLock()
		t := r.sm.topics[hash]
		r.sm.mu.RUnlock(token)

		if t != nil {
			t.remove(r.waiter)
		}
	}
}

// CancelAndRenotify cancels this registration and wakes the next waiter
// on each channel. Use this when the waiter could not consume the data
// (e.g. WRONGTYPE on destination) so the next blocked client gets a chance.
func (r *Registration[T]) CancelAndRenotify() {
	r.Cancel()

	for _, ch := range r.waiter.channels {
		r.sm.Notify(ch)
	}
}

// Wait blocks until a wake signal is received or ctx is canceled.
// Returns the channel that was signaled (nil if canceled) and error.
// Can be called multiple times - waiter remains subscribed until Cancel().
func (r *Registration[T]) Wait() ([]byte, error) {
	if r.canceled.Load() {
		return nil, context.Canceled
	}

	select {
	case <-r.waiter.ch:
		// Woken up - waiter is still in the queue for next wake
		return nil, nil
	case <-r.ctx.Done():
		r.Cancel()

		return nil, r.ctx.Err()
	}
}

// NotifyChan returns the channel that receives wake signals.
// Use this when you need to select on multiple channels along with the wake signal.
func (r *Registration[T]) NotifyChan() <-chan struct{} {
	return r.waiter.ch
}
