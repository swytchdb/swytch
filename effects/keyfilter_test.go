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

package effects

import (
	"errors"
	"testing"

	pb "github.com/swytchdb/swytch/cluster/proto"
)

// linkedBroadcaster delivers a writer engine's broadcasts straight into a
// reader engine's HandleRemote — the in-process analogue of cluster delivery.
// Enough to exercise cluster-key-filter propagation end-to-end without the UDP
// transport, so the macro behavior is testable in milliseconds (not Jepsen).
type linkedBroadcaster struct {
	mockBroadcaster
	peer *Engine
}

func (b *linkedBroadcaster) ReplicateTo(notify *pb.OffsetNotify, data []byte, target pb.NodeID) ([]*pb.NackNotify, error) {
	_, _ = b.mockBroadcaster.ReplicateTo(notify, data, target)
	if b.peer != nil {
		return b.peer.HandleRemote(notify)
	}
	return nil, nil
}

func (b *linkedBroadcaster) BroadcastWithData(notify *pb.OffsetNotify, data []byte) {
	b.mockBroadcaster.BroadcastWithData(notify, data)
	if b.peer != nil {
		if _, err := b.peer.HandleRemote(notify); err != nil {
			panic("linkedBroadcaster: peer HandleRemote: " + err.Error())
		}
	}
}

// TestPeerWriteVisibleToReader is the macro regression for the hit->miss bug:
// when a writer commits a brand-new key, the reader must learn that a peer
// holds it (so a read-only GET subscribes-and-hits instead of free-missing).
// Before the fix, the writer's SubscriptionEffect hit the reader's authority
// gate and returned before peerFilterAdd, so the reader never learned the key.
func TestPeerWriteVisibleToReader(t *testing.T) {
	writer := newTestEngine(nil)
	writer.nodeID = 1
	reader := newTestEngine(nil)
	reader.nodeID = 2

	bc := &linkedBroadcaster{peer: reader}
	bc.peerIDs = []pb.NodeID{2}
	bc.allRegionPeersReachable = true
	writer.broadcaster = bc

	// A real write: read-before-write subscribes (broadcasting a
	// SubscriptionEffect to the reader), then the data effect is broadcast too.
	ctx := writer.NewContext()
	if _, _, err := ctx.GetSnapshot("k"); err != nil {
		t.Fatalf("writer GetSnapshot: %v", err)
	}
	if err := ctx.Emit(dataEffect("k")); err != nil {
		t.Fatalf("writer Emit: %v", err)
	}
	if err := ctx.Flush(); err != nil {
		t.Fatalf("writer Flush: %v", err)
	}

	if !reader.clusterMaybeHasKey("k") {
		t.Fatal("reader free-misses a key the writer committed: hit->miss regression")
	}
	// A key nobody wrote stays a (correct) free miss.
	if reader.clusterMaybeHasKey("never-written") {
		t.Fatal("reader reports a key no peer ever wrote")
	}
}

// TestReadOnlyMissAnnouncesAsync is the core win: a read of a key no peer
// holds returns nil without a blocking subscribe round-trip (no ReplicateTo),
// but it must still announce the subscription fire-and-forget so future peer
// writes route to us — never skip it. The subscription is installed locally.
func TestReadOnlyMissAnnouncesAsync(t *testing.T) {
	bc := &mockBroadcaster{peerIDs: []pb.NodeID{2, 3}, allRegionPeersReachable: true}
	e := newTestEngine(bc)

	ctx := e.NewReadOnlyContext()
	result, _, err := ctx.GetSnapshot("never-written")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil snapshot for absent key, got %v", result)
	}
	if len(bc.replicateToPeers) != 0 {
		t.Fatalf("read-only miss must not block on a subscribe round-trip, but ReplicateTo was called for %v", bc.replicateToPeers)
	}
	if e.index.Contains("never-written") == nil {
		t.Fatal("the async announce must install our own subscription in the index")
	}
	// The announce is fire-and-forget (go), so observe it through the lock.
	waitForBroadcast(t, bc)
}

// TestReadWriteMissAnnouncesAsync confirms the rule is uniform: a normal
// (read-write) context also announces async on a miss — fire-and-forget, no
// blocking ReplicateTo — rather than the old synchronous bootstrap.
func TestReadWriteMissAnnouncesAsync(t *testing.T) {
	bc := &mockBroadcaster{peerIDs: []pb.NodeID{2, 3}, allRegionPeersReachable: true}
	e := newTestEngine(bc)

	ctx := e.NewContext()
	if _, _, err := ctx.GetSnapshot("never-written"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(bc.replicateToPeers) != 0 {
		t.Fatalf("read-write miss on a nowhere-key must not block on a subscribe round-trip, got %v", bc.replicateToPeers)
	}
	// The announce is fire-and-forget (go), so observe it through the lock.
	waitForBroadcast(t, bc)
}

// TestReadOnlyHitSubscribes: once a peer has announced a key (filter hit), a
// read-only read falls through to the real subscribe + reconstruct path.
func TestReadOnlyHitSubscribes(t *testing.T) {
	bc := &mockBroadcaster{peerIDs: []pb.NodeID{2}, allRegionPeersReachable: true}
	e := newTestEngine(bc)

	// Peer 2 announces "k" (as it would via an inbound SubscriptionEffect).
	e.peerFilterAdd(pb.NodeID(2), "k")
	if !e.clusterMaybeHasKey("k") {
		t.Fatal("clusterMaybeHasKey should report the announced key")
	}

	ctx := e.NewReadOnlyContext()
	if _, _, err := ctx.GetSnapshot("k"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(bc.replicateToPeers) == 0 {
		t.Fatal("a filter hit must fall through to a real subscribe")
	}
}

// TestReadOnlyMinorityDoesNotFreeMiss: a SafeMode node not in the majority
// partition cannot trust a filter-miss; it must attempt a subscribe (which
// then fails with ErrRegionPartitioned) rather than serve a (possibly stale)
// nil.
func TestReadOnlyMinorityDoesNotFreeMiss(t *testing.T) {
	bc := &mockBroadcaster{peerIDs: []pb.NodeID{2, 3}, allRegionPeersReachable: false}
	e := newTestEngine(bc)
	e.safety.Store(&safetyMap{defaultMode: SafeMode})

	ctx := e.NewReadOnlyContext()
	_, _, err := ctx.GetSnapshot("absent")
	if !errors.Is(err, ErrRegionPartitioned) {
		t.Fatalf("minority read of absent key must not free-miss; want ErrRegionPartitioned, got %v", err)
	}
}

// TestReadOnlyMinorityUnsafeReadsThrough: UnsafeMode has no majority to gate
// on — a minority node's read of an absent key subscribes without blocking
// and serves its local view (nil here) instead of erroring.
func TestReadOnlyMinorityUnsafeReadsThrough(t *testing.T) {
	bc := &mockBroadcaster{peerIDs: []pb.NodeID{2, 3}, allRegionPeersReachable: false}
	e := newTestEngine(bc)

	ctx := e.NewReadOnlyContext()
	r, _, err := ctx.GetSnapshot("absent")
	if err != nil {
		t.Fatalf("unsafe minority read must not error, got %v", err)
	}
	if r != nil {
		t.Fatalf("expected nil for an absent key, got %v", r)
	}
}

// TestOwnFilterPopulatedOnWrite: writing data adds the key to the own filter,
// which then serializes with a bumped version for shipping on NACKs.
func TestOwnFilterPopulatedOnWrite(t *testing.T) {
	e := newTestEngine(nil)

	ctx := e.NewContext()
	if err := ctx.Emit(dataEffect("written")); err != nil {
		t.Fatal(err)
	}
	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	data, ver := e.ownFilterSnapshot()
	if len(data) == 0 || ver == 0 {
		t.Fatalf("own filter should be populated after a write: bytes=%d ver=%d", len(data), ver)
	}
	var chain CuckooChain
	if err := chain.UnmarshalBinary(data); err != nil {
		t.Fatalf("own filter did not round-trip: %v", err)
	}
	if !chain.MaybeContains("written") {
		t.Fatal("own filter should contain the written key")
	}
}

// TestPeerFilterBulkPropagation: a node decodes a peer's bulk filter (as
// delivered on a NACK) and thereafter answers clusterMaybeHasKey for the
// peer's keys — the join/bootstrap path.
func TestPeerFilterBulkPropagation(t *testing.T) {
	// Peer builds its own filter by writing keys.
	peer := newTestEngine(nil)
	pctx := peer.NewContext()
	for _, k := range []string{"alpha", "beta", "gamma"} {
		if err := pctx.Emit(dataEffect(k)); err != nil {
			t.Fatal(err)
		}
		if err := pctx.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	data, ver := peer.ownFilterSnapshot()

	// Local node receives that bulk filter (as on a membership NACK).
	local := newTestEngine(nil)
	local.cachePeerFilter(pb.NodeID(2), data, ver)

	for _, k := range []string{"alpha", "beta", "gamma"} {
		if !local.clusterMaybeHasKey(k) {
			t.Fatalf("local should see peer key %q after bulk transfer", k)
		}
	}
	if local.clusterMaybeHasKey("delta-never-written") {
		t.Fatal("unexpected false positive for a never-written key")
	}

	// A stale (older-version) bulk must not be re-applied.
	local.cachePeerFilter(pb.NodeID(2), data, ver-1)
}

// TestPeerFilterBulkReplacePreservesRealtime: a newer bulk replaces the old
// one (no unbounded duplication), but real-time additions made since are not
// lost.
func TestPeerFilterBulkReplacePreservesRealtime(t *testing.T) {
	peer := newTestEngine(nil)
	pctx := peer.NewContext()
	if err := pctx.Emit(dataEffect("bulk-key")); err != nil {
		t.Fatal(err)
	}
	if err := pctx.Flush(); err != nil {
		t.Fatal(err)
	}
	data, ver := peer.ownFilterSnapshot()

	local := newTestEngine(nil)
	local.cachePeerFilter(pb.NodeID(2), data, ver)
	// A real-time announcement arrives for a key not yet in any bulk.
	local.peerFilterAdd(pb.NodeID(2), "realtime-key")

	// A newer bulk arrives (still only contains bulk-key). It must replace the
	// old bulk without dropping the real-time key.
	local.cachePeerFilter(pb.NodeID(2), data, ver+10)

	if !local.clusterMaybeHasKey("bulk-key") {
		t.Fatal("bulk key lost after bulk replace")
	}
	if !local.clusterMaybeHasKey("realtime-key") {
		t.Fatal("real-time key lost after bulk replace")
	}
}

// TestEnrichedNackCarriesFilter: the NACK for a system key (e.g. membership)
// advertises this node's own key filter so subscribers can bulk-cache it.
// NACKs for ordinary keys do not, to bound bandwidth.
func TestEnrichedNackCarriesFilter(t *testing.T) {
	e := newTestEngine(&mockBroadcaster{})
	ctx := e.NewContext()
	if err := ctx.Emit(dataEffect("advertised")); err != nil {
		t.Fatal(err)
	}
	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	// Ordinary-key NACK: no filter (bandwidth).
	if plain := e.buildEnrichedNack("advertised", nil, nil); len(plain.NodeKeyFilter) != 0 {
		t.Fatal("ordinary-key NACK must not carry the filter")
	}

	// System-key NACK: carries the bulk filter.
	nack := e.buildEnrichedNack("__swytch:members", nil, nil)
	if len(nack.NodeKeyFilter) == 0 || nack.FilterVersion == 0 {
		t.Fatalf("system-key NACK should carry the own filter: bytes=%d ver=%d",
			len(nack.NodeKeyFilter), nack.FilterVersion)
	}
	var chain CuckooChain
	if err := chain.UnmarshalBinary(nack.NodeKeyFilter); err != nil {
		t.Fatalf("NACK filter did not decode: %v", err)
	}
	if !chain.MaybeContains("advertised") {
		t.Fatal("NACK filter should contain the advertised key")
	}
}
