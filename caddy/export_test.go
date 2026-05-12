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

import (
	"github.com/swytchdb/swytch/beacon"
	"github.com/swytchdb/swytch/effects"
)

// resetRuntimeForTest tears the singleton down so a test can reinitialise
// it with different config.
func resetRuntimeForTest() {
	runtimeMu.Lock()
	defer runtimeMu.Unlock()
	if currentRuntime != nil {
		_ = currentRuntime.rt.Stop()
		currentRuntime = nil
	}
}

// installTestRuntime injects an already-constructed engine as the
// process-wide runtime. Tests use this to drive the storage layer
// without standing up a full beacon (no cluster port bind, no DNS).
func installTestRuntime(eng *effects.Engine, keyPrefix string) {
	runtimeMu.Lock()
	defer runtimeMu.Unlock()
	sr := &sharedRuntime{
		rt:        &beacon.Runtime{Engine: eng},
		waiters:   newWaiters(),
		keyPrefix: keyPrefix,
		refs:      1,
	}
	wireLockWakeups(eng, sr)
	currentRuntime = sr
}
