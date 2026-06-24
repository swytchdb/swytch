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

package keytrie

import _ "unsafe" // for go:linkname

// nanotime is the runtime's monotonic clock (CLOCK_MONOTONIC), read via the
// VDSO on Linux — no syscall, no shared cache line.
//
//go:linkname nanotime runtime.nanotime
func nanotime() int64

// monotonic stamps a leaf's lastAccess for the LRU tiebreak. It replaces the
// old trie-global atomic access counter, whose every-read increment was the
// read path's worst contention point: a single cache line taken exclusive on
// the writing core and invalidated on every other core, once per access. The
// monotonic clock has no shared state, so concurrent reads no longer serialize
// on it. Ties (two stamps within the same nanosecond) are harmless — lastAccess
// is only an ordering hint among equal-frequency leaves, never a correctness
// input, and a just-written key still stamps a strictly later time than older
// ones, so read-your-writes sparing holds.
func monotonic() uint64 { return uint64(nanotime()) }
