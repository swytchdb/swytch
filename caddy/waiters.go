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

package caddy

import "sync"

// waiters is a per-key registry of goroutines blocked on lock release or
// refresh events. wake() is invoked from the engine's OnKeyDataAdded /
// OnKeyDeleted callbacks for any key that falls in the lock subspace.
//
// Channels are buffered (cap 1) and closed by whichever of wake or
// unsubscribe sees them while the entry is still in the map. Both
// methods hold the mutex across both the slice mutation AND the close,
// so the channel is only ever closed once — no recover() / panic-
// swallowing needed.
type waiters struct {
	mu sync.Mutex
	m  map[string][]chan struct{}
}

func newWaiters() *waiters {
	return &waiters{m: make(map[string][]chan struct{})}
}

// subscribe registers a new waiter for the key and returns its channel.
// The channel is closed by the next wake() on the key, or by
// unsubscribe if the caller's wait is cancelled first.
func (w *waiters) subscribe(key string) chan struct{} {
	ch := make(chan struct{}, 1)
	w.mu.Lock()
	w.m[key] = append(w.m[key], ch)
	w.mu.Unlock()
	return ch
}

// unsubscribe removes ch from the key's waiter slice and closes it.
// If wake() already removed-and-closed it, the channel is no longer in
// the slice and unsubscribe is a no-op — we hold the mutex across both
// the search and the close so the two paths can't race.
func (w *waiters) unsubscribe(key string, ch chan struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()
	slice := w.m[key]
	for i, c := range slice {
		if c == ch {
			w.m[key] = append(slice[:i], slice[i+1:]...)
			if len(w.m[key]) == 0 {
				delete(w.m, key)
			}
			close(ch)
			return
		}
	}
	// Not in the slice — wake() already removed and closed it.
}

// wake closes every channel registered for the key and drops the slice.
// Safe to call when no waiters exist. Holds the mutex across the close
// loop so concurrent unsubscribe calls observe an empty slice and skip
// the close.
func (w *waiters) wake(key string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	slice := w.m[key]
	delete(w.m, key)
	for _, ch := range slice {
		close(ch)
	}
}
