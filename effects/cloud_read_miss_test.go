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
	"context"
	"errors"
	"testing"

	pb "github.com/swytchdb/swytch/cluster/proto"
)

// fakeCloudReader is a CloudReader stub: it returns a fixed tip frontier (and
// advisory closure) per key and counts how many times it was consulted, so a
// test can assert that once a key is rehydrated the cluster takes over and
// Cloud is not asked again.
type fakeCloudReader struct {
	tips    map[string][]Tip
	closure map[string][]Tip
	err     error
	calls   int
}

func (f *fakeCloudReader) CloudTips(_ context.Context, key string) ([]Tip, []Tip, error) {
	f.calls++
	if f.err != nil {
		return nil, nil, f.err
	}
	return f.tips[key], f.closure[key], nil
}

// TestReadMissRehydratesFromCloud is the tiered-storage core: a read of a key
// the cluster holds nothing for, but Cloud does, must pull Cloud's frontier,
// install it, and return the value instead of free-missing. After that the key
// lives in the local index and the cluster owns it — Cloud is never consulted
// again for it.
func TestReadMissRehydratesFromCloud(t *testing.T) {
	const key = "evicted"
	// The single leaf effect Cloud still holds for the key (writer long gone).
	tip, wire := makeReachableEffect(t, key, pb.NodeID(7), 1)

	bc := &selectiveBroadcaster{
		mockBroadcaster: mockBroadcaster{
			allRegionPeersReachable: true, // in the majority partition
			peerIDs:                 []pb.NodeID{7},
		},
		fetchable: map[Tip][]byte{tip: wire}, // the CDN/peer serves the blob
	}
	e := newTestEngine(bc)
	// The closure names the same ref, exercising the parallel prefetch ahead of
	// the walk (the walk then finds the blob already cached).
	fake := &fakeCloudReader{
		tips:    map[string][]Tip{key: {tip}},
		closure: map[string][]Tip{key: {tip}},
	}
	e.SetCloudReader(fake)

	// No peer announces the key and we hold nothing: without the Cloud backstop
	// this would free-miss.
	if e.clusterMaybeHasKey(key) {
		t.Fatal("precondition: no peer should announce the evicted key")
	}

	ctx := e.NewReadOnlyContext()
	result, _, err := ctx.GetSnapshot(key)
	if err != nil {
		t.Fatalf("read of Cloud-resident key errored: %v", err)
	}
	if result == nil {
		t.Fatal("read free-missed a key Cloud still holds; expected a rehydrated value")
	}
	if fake.calls != 1 {
		t.Fatalf("expected exactly one Cloud consult on the miss, got %d", fake.calls)
	}

	// The cluster now owns the key: it's installed in the index.
	if e.index.Contains(key) == nil {
		t.Fatal("rehydrated key was not installed into the local index")
	}

	// A second read is served locally — Cloud must not be consulted again.
	if _, _, err := ctx.GetSnapshot(key); err != nil {
		t.Fatalf("second read errored: %v", err)
	}
	if fake.calls != 1 {
		t.Fatalf("Cloud was re-consulted after rehydrate (%d calls); the cluster should have taken over", fake.calls)
	}
}

// TestReadMissCloudEmptyStillMisses: when Cloud also holds nothing for the key,
// the read is a genuine miss — the backstop returns nil and does not fabricate a
// value. The complete "holds nothing" answer makes the nil authoritative:
// repeated reads of the (still-nil) key must not consult Cloud again — the
// chain is subscription-only from here and reduces to nil on every read, and
// without the consult-once marker each of those reads would pay a WAN
// round-trip.
func TestReadMissCloudEmptyStillMisses(t *testing.T) {
	bc := &selectiveBroadcaster{
		mockBroadcaster: mockBroadcaster{allRegionPeersReachable: true},
	}
	e := newTestEngine(bc)
	fake := &fakeCloudReader{tips: map[string][]Tip{}} // Cloud holds nothing
	e.SetCloudReader(fake)

	ctx := e.NewReadOnlyContext()
	result, _, err := ctx.GetSnapshot("never-written")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatal("expected a miss for a key neither the cluster nor Cloud holds")
	}
	if fake.calls != 1 {
		t.Fatalf("expected one Cloud consult on the miss, got %d", fake.calls)
	}

	for range 3 {
		if _, _, err := ctx.GetSnapshot("never-written"); err != nil {
			t.Fatalf("repeat read errored: %v", err)
		}
	}
	if fake.calls != 1 {
		t.Fatalf("authoritative nil was re-consulted (%d calls); one complete Cloud answer must stick for the leaf's residency", fake.calls)
	}
}

// TestReadMissCloudErrorFailsRead: a transport failure is not an answer — the
// read must fail with ErrCloudUnavailable, never fabricate a miss (a nil for a
// key Cloud may hold is indistinguishable from data loss). The leaf must not
// be marked consulted, so the next read retries the backstop (Cloud coming
// back mid-outage must still rehydrate the key).
func TestReadMissCloudErrorFailsRead(t *testing.T) {
	bc := &selectiveBroadcaster{
		mockBroadcaster: mockBroadcaster{allRegionPeersReachable: true},
	}
	e := newTestEngine(bc)
	fake := &fakeCloudReader{err: context.DeadlineExceeded}
	e.SetCloudReader(fake)

	ctx := e.NewReadOnlyContext()
	if _, _, err := ctx.GetSnapshot("unreachable"); !errors.Is(err, ErrCloudUnavailable) {
		t.Fatalf("read must fail with ErrCloudUnavailable on Cloud error, got: %v", err)
	}
	if _, _, err := ctx.GetSnapshot("unreachable"); !errors.Is(err, ErrCloudUnavailable) {
		t.Fatalf("second read must also fail while Cloud is down, got: %v", err)
	}
	if fake.calls != 2 {
		t.Fatalf("expected the failed consult to retry on the next read, got %d calls", fake.calls)
	}
}

// TestReadMissPartialFrontierFailsRead: Cloud names a frontier we can only
// partially fetch. Serving the reachable subset would be a stale value posing
// as the whole state, so the read must fail — and nothing may be installed,
// or the NEXT read would reconstruct the partial state locally and skip the
// consult entirely.
func TestReadMissPartialFrontierFailsRead(t *testing.T) {
	const key = "evicted"
	reachable, wire := makeReachableEffect(t, key, pb.NodeID(7), 1)
	unreachable := Tip{9, 4} // blob never fetchable

	bc := &selectiveBroadcaster{
		mockBroadcaster: mockBroadcaster{
			allRegionPeersReachable: true,
			peerIDs:                 []pb.NodeID{7},
		},
		fetchable: map[Tip][]byte{reachable: wire},
	}
	e := newTestEngine(bc)
	fake := &fakeCloudReader{
		tips: map[string][]Tip{key: {reachable, unreachable}},
	}
	e.SetCloudReader(fake)

	ctx := e.NewReadOnlyContext()
	for i := range 2 {
		result, _, err := ctx.GetSnapshot(key)
		if !errors.Is(err, ErrCloudUnavailable) {
			t.Fatalf("read %d: want ErrCloudUnavailable for a partial frontier, got result=%v err=%v",
				i+1, result, err)
		}
	}
	if fake.calls != 2 {
		t.Fatalf("expected each failed read to re-consult (nothing installed), got %d calls", fake.calls)
	}
}

// TestReadMissNoCloudFreeMisses: with no Cloud configured the read-only free-miss
// fast path is preserved — the read returns nil without any Cloud consult (Cloud
// is optional, and standalone must keep its zero-cost miss).
func TestReadMissNoCloudFreeMisses(t *testing.T) {
	bc := &mockBroadcaster{allRegionPeersReachable: true, peerIDs: []pb.NodeID{2, 3}}
	e := newTestEngine(bc) // cloudReader stays nil

	ctx := e.NewReadOnlyContext()
	result, _, err := ctx.GetSnapshot("never-written")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Fatal("expected a free-miss with no Cloud configured")
	}
	// The async subscription announce still fires (never skipped).
	waitForBroadcast(t, bc)
}
