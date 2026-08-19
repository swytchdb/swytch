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
	"bytes"
	"fmt"
	"testing"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// holedFetchBroadcaster answers every fetch with the CDN-404 sentinel: any
// offset not already in the engine's cache is a provable durable hole.
type holedFetchBroadcaster struct {
	mockBroadcaster
}

func (b *holedFetchBroadcaster) FetchFromAny(ref *pb.EffectRef, _ FetchHint) ([]byte, error) {
	return nil, fmt.Errorf("cdn fetch %v: %w", r(ref), ErrCDNBlobMissing)
}

// TestRepairCloudFrontierSupersedesUnreachableAncestry: repairing over a
// frontier tip whose bytes this engine can never fetch must leave the key
// fully readable — the walk stops at the state-carrying repair snapshot (LCA)
// without fetching the superseded ancestry — and the snapshot's deps must name
// the foreign frontier, since that dep-reference is what makes the cloud
// consume the poisoned tips out of its tips record.
func TestRepairCloudFrontierSupersedesUnreachableAncestry(t *testing.T) {
	e := newTxnTestEngine(nil)
	defer func() { _ = e.Close() }()

	ctx := e.NewContext()
	if err := ctx.Emit(scalarSet("k", "local")); err != nil {
		t.Fatal(err)
	}
	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	// A tip from a node this engine has never seen: nothing local, no peers,
	// no cloud — any walk that descends beneath the snapshot fails loudly.
	foreign := Tip{12345, 7}
	if err := e.RepairCloudFrontier("k", nil, []Tip{foreign}); err != nil {
		t.Fatalf("repair: %v", err)
	}

	ts := e.index.Contains("k")
	if ts == nil || len(ts.Tips()) != 1 {
		t.Fatalf("post-repair tips = %v, want exactly the tip above the snapshot", ts.Tips())
	}
	top, err := e.getEffect("k", ts.Tips()[0])
	if err != nil {
		t.Fatal(err)
	}
	if top.GetNoop() == nil {
		t.Fatalf("frontier tip is %T, want the Noop above the snapshot", top.Kind)
	}
	if len(top.Deps) != 1 {
		t.Fatalf("noop deps = %v, want exactly the snapshot", top.Deps)
	}
	snap, err := e.getEffect("k", r(top.Deps[0]))
	if err != nil {
		t.Fatal(err)
	}
	if snap.GetSnapshot() == nil || snap.GetSnapshot().State == nil {
		t.Fatalf("effect beneath the tip is not a state-carrying snapshot: %v", snap)
	}
	var namesForeign bool
	for _, dep := range snap.Deps {
		if r(dep) == foreign {
			namesForeign = true
			break
		}
	}
	if !namesForeign {
		t.Fatalf("snapshot deps %v do not name the superseded frontier tip %v", snap.Deps, foreign)
	}

	// The read proves the LCA stop: reconstruct never fetches the foreign
	// ancestry (which would error) and still sees the pre-repair local state.
	result, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatalf("post-repair read: %v", err)
	}
	if result == nil || result.Scalar == nil || string(result.Scalar.GetRaw()) != "local" {
		t.Fatalf("post-repair state = %v, want scalar %q", result, "local")
	}
}

// TestRepairCloudFrontierNamesSelfAsSubscriber: the repair snapshot is a
// state-carrying LCA, so a peer bootstrapping the repaired key stops there and
// never sees the SubscriptionEffects beneath it. The stamped subscriber set is
// the only thing that crosses that boundary, and it must name the repairing
// node — it is the one holding the state peers need to route writes to.
func TestRepairCloudFrontierNamesSelfAsSubscriber(t *testing.T) {
	e := newTxnTestEngine(&holedFetchBroadcaster{})
	defer func() { _ = e.Close() }()

	if err := e.RepairCloudFrontier("k", nil, []Tip{{12345, 7}}); err != nil {
		t.Fatalf("repair: %v", err)
	}

	snap := requireRepairedFrontier(t, e, "k")
	subs := snap.GetSnapshot().State.GetSubscribers()
	if !subs[uint64(e.nodeID)] {
		t.Fatalf("snapshot subscribers = %v, want the repairing node %d named", subs, e.nodeID)
	}
	if _, ok := e.subscriptions.Load("k"); !ok {
		t.Fatal("repair did not subscribe the engine to the repaired key")
	}
}

// requireRepairedFrontier asserts the key's frontier is exactly one Noop above
// a state-carrying snapshot and returns the snapshot effect.
func requireRepairedFrontier(t *testing.T, e *Engine, key string) *pb.Effect {
	t.Helper()
	ts := e.index.Contains(key)
	if ts == nil || len(ts.Tips()) != 1 {
		t.Fatalf("post-repair tips = %v, want exactly the tip above the snapshot", ts.Tips())
	}
	top, err := e.getEffect(key, ts.Tips()[0])
	if err != nil {
		t.Fatal(err)
	}
	if top.GetNoop() == nil {
		t.Fatalf("frontier tip is %T, want the Noop above the snapshot", top.Kind)
	}
	if len(top.Deps) != 1 {
		t.Fatalf("noop deps = %v, want exactly the snapshot", top.Deps)
	}
	snap, err := e.getEffect(key, r(top.Deps[0]))
	if err != nil {
		t.Fatal(err)
	}
	if snap.GetSnapshot() == nil || snap.GetSnapshot().State == nil {
		t.Fatalf("effect beneath the tip is not a state-carrying snapshot: %v", snap)
	}
	return snap
}

func requireDep(t *testing.T, eff *pb.Effect, want Tip) {
	t.Helper()
	for _, dep := range eff.Deps {
		if r(dep) == want {
			return
		}
	}
	t.Fatalf("deps %v do not name tip %v", eff.Deps, want)
}

// TestInstallCloudTipsRepairsHoledFrontier: a cloud frontier tip whose walk
// fails with the CDN-404 sentinel must not fail the install forever — the
// install supersedes it with a repair snapshot carrying the local state, so
// reads and reconciles of the key proceed.
func TestInstallCloudTipsRepairsHoledFrontier(t *testing.T) {
	e := newTxnTestEngine(&holedFetchBroadcaster{})
	defer func() { _ = e.Close() }()

	ctx := e.NewContext()
	if err := ctx.Emit(scalarSet("k", "local")); err != nil {
		t.Fatal(err)
	}
	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}

	holedTip := Tip{12345, 7}
	if err := e.InstallCloudTips("k", []Tip{holedTip}, nil); err != nil {
		t.Fatalf("install over holed frontier: %v", err)
	}

	snap := requireRepairedFrontier(t, e, "k")
	requireDep(t, snap, holedTip)

	result, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatalf("post-repair read: %v", err)
	}
	if result == nil || result.Scalar == nil || string(result.Scalar.GetRaw()) != "local" {
		t.Fatalf("post-repair state = %v, want scalar %q", result, "local")
	}
}

// TestInstallCloudTipsPreservesReadableSiblings: when only part of the cloud
// frontier sits above a hole, the readable tips' state must survive — reduced
// into the repair snapshot and dep-named — while the holed tips are
// superseded.
func TestInstallCloudTipsPreservesReadableSiblings(t *testing.T) {
	e := newTxnTestEngine(&holedFetchBroadcaster{})
	defer func() { _ = e.Close() }()

	hlc := timestamppb.New(time.Now())
	readableTip := Tip{1111, 1}
	readableEff := &pb.Effect{
		Key:            []byte("k"),
		Hlc:            hlc,
		ForkChoiceHash: ComputeForkChoiceHash(1111, hlc),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: []byte("cloud")},
		}},
	}
	holedTip := Tip{12345, 7}

	sidecar := []CloudEffect{{Tip: readableTip, Eff: readableEff, ProtoLen: proto.Size(readableEff)}}
	if err := e.InstallCloudTips("k", []Tip{readableTip, holedTip}, sidecar); err != nil {
		t.Fatalf("install over partially holed frontier: %v", err)
	}

	snap := requireRepairedFrontier(t, e, "k")
	requireDep(t, snap, holedTip)
	requireDep(t, snap, readableTip)

	result, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatalf("post-repair read: %v", err)
	}
	if result == nil || result.Scalar == nil || string(result.Scalar.GetRaw()) != "cloud" {
		t.Fatalf("post-repair state = %v, want the readable sibling's scalar %q", result, "cloud")
	}
}

func TestRepairCloudTipsPreservesReadableSiblings(t *testing.T) {
	e := newTxnTestEngine(&holedFetchBroadcaster{})
	defer func() { _ = e.Close() }()

	hlc := timestamppb.New(time.Now())
	readableTip := Tip{2222, 1}
	readableEff := &pb.Effect{
		Key:            []byte("k"),
		Hlc:            hlc,
		ForkChoiceHash: ComputeForkChoiceHash(2222, hlc),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: []byte("preserved")},
		}},
	}
	holedTip := Tip{54321, 7}
	sidecar := []CloudEffect{{Tip: readableTip, Eff: readableEff, ProtoLen: proto.Size(readableEff)}}

	repaired, err := e.RepairCloudTips("k", []Tip{readableTip, holedTip}, sidecar)
	if err != nil {
		t.Fatalf("repair cloud tips: %v", err)
	}
	if !repaired {
		t.Fatal("holed frontier was not repaired")
	}
	snap := requireRepairedFrontier(t, e, "k")
	requireDep(t, snap, readableTip)
	requireDep(t, snap, holedTip)

	result, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatalf("post-repair read: %v", err)
	}
	if result == nil || result.Scalar == nil || string(result.Scalar.GetRaw()) != "preserved" {
		t.Fatalf("post-repair state = %v, want readable sibling state %q", result, "preserved")
	}
}

func TestRepairCloudTipsLeavesHealedFrontierUninstalled(t *testing.T) {
	e := newTxnTestEngine(&holedFetchBroadcaster{})
	defer func() { _ = e.Close() }()

	hlc := timestamppb.New(time.Now())
	tip := Tip{3333, 1}
	eff := &pb.Effect{
		Key:            []byte("k"),
		Hlc:            hlc,
		ForkChoiceHash: ComputeForkChoiceHash(3333, hlc),
		Kind:           &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	}
	sidecar := []CloudEffect{{Tip: tip, Eff: eff, ProtoLen: proto.Size(eff)}}

	repaired, err := e.RepairCloudTips("k", []Tip{tip}, sidecar)
	if err != nil {
		t.Fatalf("classify healed frontier: %v", err)
	}
	if repaired {
		t.Fatal("healthy frontier was unnecessarily repaired")
	}
	if got := e.index.Contains("k"); got != nil {
		t.Fatalf("healthy discovery frontier was installed before peer bootstrap: %v", got.Tips())
	}
}

func TestRepairCloudTipsConsumesSubscriptionMetadataWithoutDefeatingSnapshotLCA(t *testing.T) {
	e := newTxnTestEngine(&holedFetchBroadcaster{})
	defer func() { _ = e.Close() }()

	hlc := timestamppb.New(time.Now())
	foldedHole := Tip{4444, 1}
	snapshotTip := Tip{4444, 2}
	snapshotEff := &pb.Effect{
		Key:            []byte("k"),
		Hlc:            hlc,
		ForkChoiceHash: bytes.Repeat([]byte{0xff}, 32),
		Deps:           []*pb.EffectRef{toPbRef(foldedHole)},
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
			Collection: pb.CollectionKind_SCALAR,
			State: &pb.ReducedEffect{
				Collection: pb.CollectionKind_SCALAR,
				Scalar: &pb.DataEffect{
					Op:         pb.EffectOp_INSERT_OP,
					Merge:      pb.MergeRule_LAST_WRITE_WINS,
					Collection: pb.CollectionKind_SCALAR,
					Value:      &pb.DataEffect_Raw{Raw: []byte("snapshot-state")},
				},
			},
		}},
	}
	subTip := Tip{5555, 1}
	subEff := &pb.Effect{
		Key:            []byte("k"),
		Hlc:            hlc,
		ForkChoiceHash: make([]byte, 32),
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
			SubscriberNodeId: 5555,
		}},
	}
	triggerHole := Tip{6666, 1}
	sidecar := []CloudEffect{
		{Tip: snapshotTip, Eff: snapshotEff, ProtoLen: proto.Size(snapshotEff)},
		{Tip: subTip, Eff: subEff, ProtoLen: proto.Size(subEff)},
	}

	repaired, err := e.RepairCloudTips("k", []Tip{snapshotTip, subTip, triggerHole}, sidecar)
	if err != nil {
		t.Fatalf("repair mixed membership frontier: %v", err)
	}
	if !repaired {
		t.Fatal("mixed frontier was not repaired")
	}
	snap := requireRepairedFrontier(t, e, "k")
	requireDep(t, snap, snapshotTip)
	requireDep(t, snap, subTip)
	requireDep(t, snap, triggerHole)

	result, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatalf("post-repair read: %v", err)
	}
	if result == nil || result.Scalar == nil || string(result.Scalar.GetRaw()) != "snapshot-state" {
		t.Fatalf("post-repair state = %v, want folded snapshot state", result)
	}
}

// TestRepairCloudFrontierKeepsForkChoiceWinnerAcrossSiblingRepairs: two nodes
// racing the repair each mint a sibling snapshot above the same hole. Each
// chain reads alone (its own LCA stop holds) but the joint walk descends
// beneath both snapshots into the hole. A subsequent repair must converge the
// key on the fork-choice winner's state instead of failing forever.
func TestRepairCloudFrontierKeepsForkChoiceWinnerAcrossSiblingRepairs(t *testing.T) {
	e := newTxnTestEngine(&holedFetchBroadcaster{})
	defer func() { _ = e.Close() }()

	// Our own repair chain: local state "one" superseding an unfetchable tip.
	ctx := e.NewContext()
	if err := ctx.Emit(scalarSet("k", "one")); err != nil {
		t.Fatal(err)
	}
	if err := ctx.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := e.RepairCloudFrontier("k", nil, []Tip{{55555, 3}}); err != nil {
		t.Fatalf("first repair: %v", err)
	}

	// A sibling repair chain as another node would have minted it: snapshot
	// carrying "two" above the same kind of hole, Noop on top. Crafted with an
	// all-FF fork-choice hash so it deterministically loses to our chain.
	hlc := timestamppb.New(time.Now())
	snapTip := Tip{7777, 1}
	siblingSnap := &pb.Effect{
		Key:            []byte("k"),
		Hlc:            hlc,
		ForkChoiceHash: ComputeForkChoiceHash(7777, hlc),
		Deps:           []*pb.EffectRef{toPbRef(Tip{55555, 3})},
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
			Collection: pb.CollectionKind_SCALAR,
			State: &pb.ReducedEffect{
				Op:         pb.EffectOp_INSERT_OP,
				Merge:      pb.MergeRule_LAST_WRITE_WINS,
				Collection: pb.CollectionKind_SCALAR,
				Scalar: &pb.DataEffect{
					Op:         pb.EffectOp_INSERT_OP,
					Merge:      pb.MergeRule_LAST_WRITE_WINS,
					Collection: pb.CollectionKind_SCALAR,
					Value:      &pb.DataEffect_Raw{Raw: []byte("two")},
				},
			},
		}},
	}
	noopTip := Tip{7777, 2}
	siblingNoop := &pb.Effect{
		Key:            []byte("k"),
		Hlc:            hlc,
		ForkChoiceHash: bytes.Repeat([]byte{0xff}, 32),
		Deps:           []*pb.EffectRef{toPbRef(snapTip)},
		Kind:           &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	}
	e.putIngested(snapTip, siblingSnap, proto.Size(siblingSnap))
	e.putIngested(noopTip, siblingNoop, proto.Size(siblingNoop))
	e.installTips("k", []Tip{noopTip})

	// Sanity: the sibling branches defeat each other's LCA stop, so the joint
	// read cannot reduce (reconstructLocal swallows the walk error into nil).
	broken, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatalf("pre-repair read: %v", err)
	}
	if broken != nil {
		t.Fatalf("pre-repair read = %v, want nil from the unreadable sibling join", broken)
	}

	if err := e.RepairCloudFrontier("k", nil, nil); err != nil {
		t.Fatalf("converging repair: %v", err)
	}

	requireRepairedFrontier(t, e, "k")
	result, _, _, err := e.GetSnapshot("k")
	if err != nil {
		t.Fatalf("post-repair read: %v", err)
	}
	if result == nil || result.Scalar == nil || string(result.Scalar.GetRaw()) != "one" {
		t.Fatalf("post-repair state = %v, want fork-choice winner %q", result, "one")
	}
}
