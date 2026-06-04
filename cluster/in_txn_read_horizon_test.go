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
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
)

// gatedPairedBus is a two-node in-process replication shim that wires two
// effects.Engine instances together without QUIC. It satisfies
// effects.Broadcaster on a per-engine basis (gatedBroadcaster below).
//
// The bus offers two timing controls relevant to the in-txn-read /
// HorizonSet race:
//
//  1. holdSnapshotsTo(nodeID, true) — buffers verdict-carrying snapshot
//     effects destined for `nodeID` in a holding pen. Without the
//     verdict snapshot, the destination's HorizonSet keeps the
//     originator's bind invisible (the 5s fallback timer is the only
//     other promotion signal — well outside the test window).
//
//  2. releaseHeldSnapshots(nodeID) — flushes the buffered snapshots
//     through to the destination's HandleRemote, which triggers
//     applySnapshotVerdicts → HorizonSet.MakeVisible.
//
// The bus does NOT model fetch/peer-disconnect/anti-entropy. The bug we
// reproduce is on the in-bind reconstruct path; fancier transport
// modelling isn't load-bearing.
type gatedPairedBus struct {
	mu       sync.Mutex
	engines  map[pb.NodeID]*effects.Engine
	hold     map[pb.NodeID]bool           // hold snapshot deliveries to this peer
	heldSnap map[pb.NodeID][]heldSnapshot // buffered (notify, data) per target
	t        *testing.T
}

type heldSnapshot struct {
	notify *pb.OffsetNotify
	data   []byte
}

func newGatedPairedBus(t *testing.T) *gatedPairedBus {
	t.Helper()
	return &gatedPairedBus{
		engines:  make(map[pb.NodeID]*effects.Engine),
		hold:     make(map[pb.NodeID]bool),
		heldSnap: make(map[pb.NodeID][]heldSnapshot),
		t:        t,
	}
}

func (b *gatedPairedBus) registerEngine(e *effects.Engine) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.engines[e.NodeID()] = e
}

func (b *gatedPairedBus) holdSnapshotsTo(target pb.NodeID, hold bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.hold[target] = hold
}

// releaseHeldSnapshots drains any buffered snapshot notifies to target
// (in arrival order) and stops holding new ones.
func (b *gatedPairedBus) releaseHeldSnapshots(target pb.NodeID) {
	b.mu.Lock()
	held := b.heldSnap[target]
	b.heldSnap[target] = nil
	b.hold[target] = false
	dest := b.engines[target]
	b.mu.Unlock()
	if dest == nil {
		return
	}
	for _, h := range held {
		if _, err := dest.HandleRemote(h.notify); err != nil {
			slog.Warn("gatedPairedBus: HandleRemote on released snapshot failed",
				"target", target, "error", err)
		}
	}
}

// deliver routes a notify to `target`. Snapshot effects with a non-empty
// TxnVerdicts map are subject to the hold gate; everything else goes
// straight through.
func (b *gatedPairedBus) deliver(target pb.NodeID, notify *pb.OffsetNotify, wireData []byte) ([]*pb.NackNotify, error) {
	b.mu.Lock()
	dest, ok := b.engines[target]
	if !ok {
		b.mu.Unlock()
		return nil, fmt.Errorf("gatedPairedBus: unknown target %v", target)
	}

	// Parse the effect to decide whether it's a held snapshot. We have to
	// peek into the wire payload — notify.EffectData carries the
	// [4-byte LE keyLen][key][protoData] tuple BuildOffsetNotify produced.
	if shouldHold(notify, b.hold[target]) {
		b.heldSnap[target] = append(b.heldSnap[target], heldSnapshot{notify: notify, data: wireData})
		slog.Debug("gatedPairedBus: holding snapshot delivery",
			"target", target, "origin", notify.Origin)
		b.mu.Unlock()
		return nil, nil
	}
	b.mu.Unlock()

	return dest.HandleRemote(notify)
}

// shouldHold decides whether a notify is a verdict-carrying snapshot
// the bus is configured to hold for this target. Bus mu must be held.
func shouldHold(notify *pb.OffsetNotify, holding bool) bool {
	if !holding {
		return false
	}
	if notify.EffectData == nil {
		return false
	}
	// Parse wire format.
	if len(notify.EffectData) < 4 {
		return false
	}
	keyLen := int(notify.EffectData[0]) |
		int(notify.EffectData[1])<<8 |
		int(notify.EffectData[2])<<16 |
		int(notify.EffectData[3])<<24
	if keyLen < 0 || 4+keyLen > len(notify.EffectData) {
		return false
	}
	protoData := notify.EffectData[4+keyLen:]
	eff := &pb.Effect{}
	if err := effects.UnmarshalEffect(protoData, eff); err != nil {
		return false
	}
	snap := eff.GetSnapshot()
	if snap == nil {
		return false
	}
	return len(snap.TxnVerdicts) > 0
}

// gatedBroadcaster is the per-engine view of the bus that satisfies the
// effects.Broadcaster interface.
type gatedBroadcaster struct {
	bus   *gatedPairedBus
	self  pb.NodeID
	peer  pb.NodeID
	peers []pb.NodeID
}

func newGatedBroadcaster(bus *gatedPairedBus, self, peer pb.NodeID) *gatedBroadcaster {
	return &gatedBroadcaster{
		bus:   bus,
		self:  self,
		peer:  peer,
		peers: []pb.NodeID{peer},
	}
}

func (g *gatedBroadcaster) Broadcast(notify *pb.OffsetNotify) {
	_, _ = g.bus.deliver(g.peer, notify, notify.EffectData)
}

func (g *gatedBroadcaster) BroadcastWithData(notify *pb.OffsetNotify, _ []byte) {
	// Notify already embeds EffectData via BuildOffsetNotify; the
	// explicit data argument is the unwrapped proto bytes (a duplicate
	// the peer can reconstruct from notify.EffectData). Routing through
	// the wire-format payload keeps the receive path identical to the
	// production transport.
	_, _ = g.bus.deliver(g.peer, notify, notify.EffectData)
}

func (g *gatedBroadcaster) Replicate(notify *pb.OffsetNotify, _ []byte) error {
	_, err := g.bus.deliver(g.peer, notify, notify.EffectData)
	return err
}

func (g *gatedBroadcaster) ReplicateTo(notify *pb.OffsetNotify, _ []byte, target pb.NodeID) ([]*pb.NackNotify, error) {
	return g.bus.deliver(target, notify, notify.EffectData)
}

func (g *gatedBroadcaster) SendNack(_ *pb.NackNotify, _ pb.NodeID) {
	// No-op: the in-process bus delivers NACKs synchronously as the
	// ReplicateTo return value. An async SendNack path isn't exercised
	// here.
}

func (g *gatedBroadcaster) FetchFromAny(ref *pb.EffectRef) ([]byte, error) {
	g.bus.mu.Lock()
	peer, ok := g.bus.engines[g.peer]
	g.bus.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("gatedBroadcaster: peer %v not registered", g.peer)
	}
	cache := peer.EffectCache()
	if cache == nil {
		return nil, fmt.Errorf("gatedBroadcaster: peer %v has no effect cache", g.peer)
	}
	eff, ok := cache.Get(effects.Tip{ref.NodeId, ref.Offset})
	if !ok {
		return nil, fmt.Errorf("gatedBroadcaster: peer %v missing effect %+v", g.peer, ref)
	}
	data, err := effects.MarshalEffect(eff)
	if err != nil {
		return nil, err
	}
	// Wire format: [4-byte LE keyLen][key][protoData]
	keyBytes := eff.Key
	wire := make([]byte, 4+len(keyBytes)+len(data))
	wire[0] = byte(len(keyBytes))
	wire[1] = byte(len(keyBytes) >> 8)
	wire[2] = byte(len(keyBytes) >> 16)
	wire[3] = byte(len(keyBytes) >> 24)
	copy(wire[4:4+len(keyBytes)], keyBytes)
	copy(wire[4+len(keyBytes):], data)
	return wire, nil
}

func (g *gatedBroadcaster) Fetch(ref *pb.EffectRef) ([]byte, error) {
	return g.FetchFromAny(ref)
}

func (g *gatedBroadcaster) PeerIDs() []pb.NodeID          { return g.peers }
func (g *gatedBroadcaster) AllRegionPeersReachable() bool { return true }
func (g *gatedBroadcaster) InMajorityPartition() bool     { return true }
func (g *gatedBroadcaster) ForwardTransaction(_ context.Context, _ pb.NodeID, _ *pb.ForwardedTransaction) (*pb.ForwardedResponse, error) {
	return nil, fmt.Errorf("gatedBroadcaster: ForwardTransaction not implemented")
}

// ---------- the test ----------

// extractOrderedIDs returns the element IDs of an ordered ReducedEffect in
// reduction order. A nil reduced or non-ordered result yields nil.
func extractOrderedIDs(r *pb.ReducedEffect) []string {
	if r == nil {
		return nil
	}
	if r.Collection != pb.CollectionKind_ORDERED {
		return nil
	}
	ids := make([]string, 0, len(r.OrderedElements))
	for _, el := range r.OrderedElements {
		if el == nil || el.Data == nil {
			continue
		}
		ids = append(ids, string(el.Data.Id))
	}
	return ids
}

// isPrefix reports whether `prefix` is a prefix (element-wise) of `full`.
func isPrefix(prefix, full []string) bool {
	if len(prefix) > len(full) {
		return false
	}
	for i, s := range prefix {
		if full[i] != s {
			return false
		}
	}
	return true
}

func orderedTailInsert(id string, value []byte) *pb.DataEffect {
	return &pb.DataEffect{
		Op:         pb.EffectOp_INSERT_OP,
		Merge:      pb.MergeRule_LAST_WRITE_WINS,
		Collection: pb.CollectionKind_ORDERED,
		Placement:  pb.Placement_PLACE_TAIL,
		Id:         []byte(id),
		Value:      &pb.DataEffect_Raw{Raw: value},
	}
}

// TestInTxnReadObservesInconsistentSnapshotAcrossHorizon reproduces the
// :incompatible-order hypothesis from Jepsen run 26373595271. The bug is
// observer-side: a Context.GetSnapshot called inside an in-flight txn
// returns a value that excludes an in-ancestry bind sitting in HorizonSet
// (invisible), and that value reaches the client. After horizon promotion,
// a fresh read on the same node includes the previously-skipped bind,
// producing two reads on the same key that are not prefix-comparable.
//
// Scenario (mirrors the Jepsen el-0 anchor: 107, 211, 553):
//
//  1. Engines A (writer of XA) and B (writer of XB) are wired via a
//     gated in-process bus. Both subscribe to key K. A baseline element
//     "base" is committed first so K has tips on both nodes.
//
//  2. A starts a txn that appends element XA and flushes. The bind is
//     replicated to B; B's handleRemoteBind installs it in HorizonSet
//     (invisible) and schedules MakeVisible for 5s (crash fallback). A
//     receives an empty NACK list back, commits, and emits a verdict
//     snapshot via BroadcastWithData. The bus HOLDS the verdict snapshot
//     so it never reaches B during the test window — A's bind therefore
//     stays invisible on B.
//
//  3. On B, a local Context txn begins. B's GetSnapshot on K reconstructs
//     from K's tips; XA's bind is reached on the walk but skipped as
//     invisible. The reduced state contains "base" and B's own XB only.
//     This is the in-bind read whose value the test captures.
//
//  4. B flushes its txn. (A is the only peer, so the bind broadcasts to
//     A and round-trips back; the post-txn engine state on B has both
//     B's bind tip and XA's bind in horizon — still invisible.)
//
//  5. The bus releases A's verdict snapshot to B. applySnapshotVerdicts
//     fires HorizonSet.MakeVisible for A's txn. XA becomes visible.
//
//  6. A fresh GetSnapshot on B reconstructs K. XA's bind is now visible
//     and contributes "XA" to the ordered state. The fresh read returns
//     [base, XA, XB] (the exact order depends on fork-choice hash on
//     base, but the SET of IDs grows from {base, XB} to {base, XA, XB}).
//
//  7. Assertion: the two reads on K must be prefix-comparable. The
//     in-bind read returned {base, XB}; the post-promotion read returned
//     {base, XA, XB}. As lists, the in-bind read is not a prefix of the
//     post-promotion read because XB sits where XA sits in the longer
//     list — Elle's :incompatible-order anomaly.
//
// Failure mode: the assertion fires because the in-bind read returned a
// list whose last element (XB) is NOT the last element of the
// post-promotion read. The fix surface is for in-txn reads to wait for
// in-ancestry HorizonSet binds to resolve before returning a value.
func TestInTxnReadObservesInconsistentSnapshotAcrossHorizon(t *testing.T) {
	const key = "el-0"
	const idBase = "base"
	const idA = "XA"
	const idB = "XB"

	bus := newGatedPairedBus(t)

	// Both engines get a real broadcaster, which auto-inits horizon.
	engA := effects.NewEngine(effects.EngineConfig{
		NodeID: 1001, DefaultMode: effects.UnsafeMode, // skip majority-partition gating
	})
	engB := effects.NewEngine(effects.EngineConfig{
		NodeID: 1002, DefaultMode: effects.UnsafeMode,
	})

	// Hook SetBroadcaster so the engines learn about each other via the
	// shim. Order matters slightly: registerEngine before SetBroadcaster
	// so any racy bootstrap path can resolve immediately.
	bus.registerEngine(engA)
	bus.registerEngine(engB)
	engA.SetBroadcaster(newGatedBroadcaster(bus, engA.NodeID(), engB.NodeID()))
	engB.SetBroadcaster(newGatedBroadcaster(bus, engB.NodeID(), engA.NodeID()))

	defer func() {
		_ = engA.Close()
		_ = engB.Close()
	}()

	// Step 1: baseline append of "base" via A, committed cluster-wide.
	// This guarantees both engines see a non-empty tip set on K via the
	// normal bootstrap + broadcast paths.
	ctxA := engA.NewContext()
	if err := ctxA.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Data{Data: orderedTailInsert(idBase, []byte("v_base"))},
	}); err != nil {
		t.Fatalf("baseline emit on A: %v", err)
	}
	if err := ctxA.Flush(); err != nil {
		t.Fatalf("baseline flush on A: %v", err)
	}

	// Wait briefly for the baseline broadcast to settle on B (B was the
	// peer in A's broadcaster but A's flush is non-transactional, so the
	// data effect went through BroadcastWithData which is synchronous in
	// our bus).
	var baseIDs []string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r, tips, _, err := engB.GetSnapshot(key)
		if err == nil && len(tips) > 0 {
			baseIDs = extractOrderedIDs(r)
			if len(baseIDs) > 0 {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("B baseline read of %s: %v", key, baseIDs)
	if len(baseIDs) == 0 {
		t.Fatalf("setup: B should see at least the baseline element after A's broadcast")
	}

	// Step 2: arm the snapshot-hold gate so A's eventual verdict
	// snapshot doesn't promote A's bind out of horizon on B.
	bus.holdSnapshotsTo(engB.NodeID(), true)

	// A starts a txn that appends idA. A's flushTx will replicate the
	// bind to B (via the bus, synchronous), B's HandleRemote installs
	// the bind in HorizonSet (invisible) and returns NACK/ACK. A then
	// commits and broadcasts the verdict snapshot — held by the bus.
	ctxA2 := engA.NewContext()
	ctxA2.BeginTx()
	if err := ctxA2.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Data{Data: orderedTailInsert(idA, []byte("v_XA"))},
	}); err != nil {
		t.Fatalf("A txn emit: %v", err)
	}
	if err := ctxA2.Flush(); err != nil {
		t.Fatalf("A txn flush: %v", err)
	}

	// At this point: A committed locally; the verdict snapshot is held
	// in the bus's heldSnap for B. A's bind is in B's HorizonSet,
	// invisible. The 5s horizon-fallback timer hasn't fired yet.

	// Sanity: B's index has tips on K but A's bind contribution is
	// invisible. We read GetSnapshot just to log the visible content.
	preTxR, preTxTips, _, _ := engB.GetSnapshot(key)
	t.Logf("B's view of %s after A's txn (A's bind held in horizon): ids=%v tips=%v",
		key, extractOrderedIDs(preTxR), preTxTips)

	// Step 3-4: B runs its own txn. Under the fix, the in-bind read on K
	// walks the DAG, sees A's bind invisible in HorizonSet, and blocks on
	// the waiter mutex. Release A's held verdict snapshot from a goroutine
	// so HandleRemote → applySnapshotVerdicts → HorizonSet.MakeVisible
	// fires while B's read is blocked. The read then restarts reconstruct
	// with A's bind visible and includes XA in the result.
	go func() {
		time.Sleep(50 * time.Millisecond)
		bus.releaseHeldSnapshots(engB.NodeID())
	}()

	ctxB := engB.NewContext()
	ctxB.BeginTx()
	if err := ctxB.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Data{Data: orderedTailInsert(idB, []byte("v_XB"))},
	}); err != nil {
		t.Fatalf("B txn emit: %v", err)
	}
	inBindReduced, _, err := ctxB.GetSnapshot(key)
	if err != nil {
		t.Fatalf("B in-bind GetSnapshot: %v", err)
	}
	inBindIDs := extractOrderedIDs(inBindReduced)
	t.Logf("B in-bind read of %s returned ordered IDs: %v", key, inBindIDs)

	// Flush B's txn. The bind broadcasts to A through the bus.
	flushBErr := ctxB.Flush()
	t.Logf("B flush result: %v", flushBErr)

	// Give the engine a moment to process the snapshot arrival and
	// evict the cache so the next GetSnapshot reconstructs.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r, _, _, err := engB.GetSnapshot(key)
		if err == nil {
			ids := extractOrderedIDs(r)
			containsXA := slices.Contains(ids, idA)
			if containsXA {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Step 6: post-promotion read on B.
	postReduced, _, _, err := engB.GetSnapshot(key)
	if err != nil {
		t.Fatalf("B post-promotion GetSnapshot: %v", err)
	}
	postIDs := extractOrderedIDs(postReduced)
	t.Logf("B post-promotion read of %s returned ordered IDs: %v", key, postIDs)

	// Step 7: the two reads must be prefix-comparable. With the bug, the
	// in-bind read returned [base, XB] (XA was skipped) and the
	// post-promotion read returns [base, XA, XB] — XA was slotted in
	// before XB by topo order, so [base, XB] is NOT a prefix of
	// [base, XA, XB]. This is exactly the :incompatible-order anomaly
	// in the Jepsen run.
	if isPrefix(inBindIDs, postIDs) || isPrefix(postIDs, inBindIDs) {
		t.Logf("reads were prefix-comparable: in-bind=%v post=%v", inBindIDs, postIDs)
		return
	}

	t.Fatalf(":incompatible-order: B's in-bind read %v is not a prefix of the post-promotion read %v (and vice versa). "+
		"The in-bind read excluded XA because A's bind sat in HorizonSet (invisible) on B during the read; "+
		"the post-promotion reconstruct included XA after the verdict snapshot fired MakeVisible. "+
		"The fix surface: in-txn reads must wait for in-ancestry HorizonSet binds to resolve before returning. "+
		"This mirrors the Jepsen run 26373595271 :r el-0 [107 553] vs [107 211] anomaly recorded in /tmp/26373595271/latest/results.edn.",
		inBindIDs, postIDs)
}
