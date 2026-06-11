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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	pb "github.com/swytchdb/swytch/cluster/proto"
)

// TestReplicate_HeldThroughSendFailure_GivenUpWhenPeerDies is the regression
// guard for the orphaned-snapshot bug: a same-region send that fails only
// because the peer's connection isn't up yet (connFunc returns nil →
// ErrPeerUnavailable) must NOT be dropped. The old code rejected the future
// with ErrNoPeers the instant every send failed, so the effect committed
// locally with zero replicas; when a later effect depended on it the cluster
// could never reconstruct the key. The fixed Replicator holds the request and
// retransmits it until the peer ACKs or leaves the alive+symmetric set.
//
// With a connFunc that always returns nil there is never an ACK, so this test
// drives the two endpoints of that contract: (1) the future stays pending
// through the failed initial send and a retransmit while the peer is alive,
// and (2) it only rejects once the peer is declared dead.
func TestReplicate_HeldThroughSendFailure_GivenUpWhenPeerDies(t *testing.T) {
	const self = NodeId(1)
	const peer = NodeId(2)

	health := NewPeerHealthTable(ClockConfig{})
	ph := health.GetOrCreate(peer)
	ph.alive.Store(true)
	ph.symmetric.Store(true) // a valid replication target...

	var sendAttempts atomic.Int32
	connFunc := func(NodeId) *quic.Conn {
		sendAttempts.Add(1)
		return nil // ...whose connection isn't up yet → Send returns ErrPeerUnavailable
	}
	transport := NewQUICNotifyTransport(self, nil, connFunc, nil)

	r := NewReplicator(self, "test", health, transport, &recordingHandler{}, 30*time.Second)
	r.AddPeer(peer, "test")

	notify := &pb.OffsetNotify{
		Origin: &pb.EffectRef{NodeId: uint64(self), Offset: 1},
		Key:    []byte("k"),
	}
	future := r.Replicate(notify, []byte("data"))

	// The initial send failed, but the peer is alive: the request must be held,
	// not dropped. Old behavior rejected here with ErrNoPeers.
	if attempts := sendAttempts.Load(); attempts != 1 {
		t.Fatalf("expected 1 initial send attempt, got %d", attempts)
	}
	select {
	case <-future.Done():
		t.Fatalf("future resolved on send failure to a live peer — request was dropped, not held: %v", future.Err())
	default:
	}

	// Force the retransmit cadence (lastSentAt is stamped "now" by Replicate)
	// and sweep: while the peer is alive the held request is retransmitted, not
	// abandoned.
	r.tracker.pending.Range(func(_ uint64, tr *trackedRequest) bool {
		tr.lastSentAt.Store(0)
		return true
	})
	r.sweep()
	if attempts := sendAttempts.Load(); attempts != 2 {
		t.Fatalf("expected a retransmit while peer alive (2 attempts), got %d", attempts)
	}
	select {
	case <-future.Done():
		t.Fatalf("future resolved while peer still alive: %v", future.Err())
	default:
	}

	// Peer declared dead (no heartbeat within timeout): give up and reject so a
	// blocking SafeMode flush unblocks and commits locally as last-node-standing.
	ph.alive.Store(false)
	r.sweep()

	select {
	case <-future.Done():
	case <-time.After(time.Second):
		t.Fatal("future did not reject after peer declared dead")
	}
	if err := future.Err(); !errors.Is(err, ErrNoPeers) {
		t.Fatalf("expected ErrNoPeers after peer died, got: %v", err)
	}

	pendingCount := 0
	r.tracker.pending.Range(func(uint64, *trackedRequest) bool {
		pendingCount++
		return true
	})
	if pendingCount != 0 {
		t.Fatalf("given-up request was not removed from the pending tracker: %d left", pendingCount)
	}
}
