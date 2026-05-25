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

package cluster

import (
	"sync/atomic"
	"testing"
	"time"
)

// TestPeerLifecycleHook_NoDeadlock verifies that lifecycle callbacks
// fire OUTSIDE pm.mu. If they fire under the lock, a hook that
// re-enters PeerManager via any RLock path (e.g. PeerIDs(), or the
// QUIC transport's connFunc on the real send path) will self-
// deadlock since xsync.RBMutex does not permit recursive read while
// the same goroutine holds the write lock.
//
// The hook here calls pm.PeerIDs() which takes pm.mu.RLock() — the
// same RLock that the QUIC connFunc takes when SendOneWayTo flows
// through. If the production code fires onPeerAdded/onPeerRemoved
// while pm.mu is W-locked, this test will hang and fail via the
// test timeout.
func TestPeerLifecycleHook_NoDeadlock(t *testing.T) {
	handler := &recordingHandler{}
	logReader := &mockLogReader{data: []byte("test")}

	cfg := &ClusterConfig{
		NodeID: 0,
		Nodes: []NodeConfig{
			{ID: 0, Address: "127.0.0.1:0", Region: "test"},
		},
	}
	pm, err := NewPeerManager(cfg, handler, logReader)
	if err != nil {
		t.Fatalf("create pm: %v", err)
	}
	tlsCfg := generateTestTLSConfig(t)
	pm.serverTLS = tlsCfg
	pm.clientTLS = tlsCfg

	var addedCalls atomic.Int32
	var removedCalls atomic.Int32

	pm.SetPeerLifecycleHooks(
		func(id NodeId) {
			addedCalls.Add(1)
			// Re-enter PeerManager via an RLock path. With the bug
			// (hook fires under pm.mu.Lock()) this hangs forever.
			_ = pm.PeerIDs()
		},
		func(id NodeId) {
			removedCalls.Add(1)
			_ = pm.PeerIDs()
		},
	)

	if err := pm.Start(t.Context()); err != nil {
		t.Fatalf("start: %v", err)
	}
	// NOTE: no defer pm.Stop() — if the bug is present, the leaked
	// goroutine still holds pm.mu and Stop would block forever. We
	// only clean up on the success path. Leaked goroutines on the
	// fail path are torn down when the test binary exits.

	runUnderTimeout := func(name string, fn func()) {
		done := make(chan struct{})
		go func() {
			fn()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s deadlocked: lifecycle hook fired under pm.mu", name)
		}
	}

	runUnderTimeout("UpdateTopology(add)", func() {
		pm.UpdateTopology(&ClusterConfig{
			NodeID: 0,
			Nodes: []NodeConfig{
				{ID: 0, Address: pm.ListenAddr(), Region: "test"},
				{ID: 1, Address: "127.0.0.1:12345", Region: "test"},
			},
		})
	})
	if got := addedCalls.Load(); got < 1 {
		t.Errorf("onPeerAdded should have fired at least once, got %d", got)
	}

	runUnderTimeout("UpdateTopology(remove)", func() {
		pm.UpdateTopology(&ClusterConfig{
			NodeID: 0,
			Nodes:  []NodeConfig{{ID: 0, Address: pm.ListenAddr(), Region: "test"}},
		})
	})
	if got := removedCalls.Load(); got < 1 {
		t.Errorf("onPeerRemoved should have fired at least once, got %d", got)
	}

	pm.Stop()
}
