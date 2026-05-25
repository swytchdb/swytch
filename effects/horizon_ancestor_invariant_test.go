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
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	clox "github.com/swytchdb/swytch/cache"
	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/crdt"
	"github.com/swytchdb/swytch/keytrie"
)

// TestReconstructIncludesDescendantBindWithInvisibleAncestor reproduces the
// Jepsen :incompatible-order anomaly from run 20260522T150925.499Z on key el-4.
//
// Hypothesis: HorizonSet's per-bind invisibility filter in reconstruct decides
// each bind independently and ignores the DAG-ancestor invariant. When a
// visible descendant bind B has an invisible ancestor bind A in its causal
// history (B's data chain transitively deps A's bind effect), reconstruct
// emits B's effects while silently dropping A's — yielding a state that
// reflects a "future" without its "past".
//
// Timeline rows this test models (artifact:
// /tmp/26295647551/swytch-elle-causal/20260522T150925.499Z/):
//
//	12.320471Z  10.0.2.141  HorizonSet.Add (origin)   writer-66 bind [7642733957214361410, 24]
//	12.321518Z  10.0.0.85   HorizonSet.Add (remote)   writer-66 invisible
//	12.329862Z  10.0.0.7    reconstruct done count=7  writer-69 sees writer-66 bind in tips
//	12.330433Z  10.0.0.7    HorizonSet.Add (origin)   writer-69 bind [7642733957363584996, 24]
//	12.331682Z  10.0.0.85   HorizonSet.Add (remote)   writer-69 invisible
//	12.467210Z  10.0.0.7    flushTx committed          writer-69 origin done
//	12.467906Z  10.0.0.85   HorizonSet.MakeVisible    writer-69 visible on reader
//	12.500723Z  10.0.0.85   losersOnKey verdict       binds_seen only writer-69
//	12.502999Z  10.0.0.85   reconstruct: skip bind (invisible) writer-66
//	12.503005Z  10.0.0.85   reconstruct: include bind  writer-69
//	12.683904Z  10.0.0.85   HorizonSet.MakeVisible    writer-66 visible (anomaly returned)
//
// DAG ancestor evidence (dag.walk log @ 12.501138988Z on 10.0.0.85): writer-69's
// pre-tx chain [7642733957363584996, 18] has dep [7642733957214361410, 24]
// — writer-66's bind offset.
//
// Structure built here (key K, two binds, only one tip):
//
//	root (non-tx data on K)
//	  ↑
//	txA_write (tx-marked, txn-A, RPUSH "66")
//	  ↑
//	bindA  (consumed=[root], newTip=txA_write, txn-A)  ← INVISIBLE in horizon
//	  ↑
//	preB (non-tx Noop on K, deps=[bindA])              ← writer-69's pre-tx chain
//	  ↑
//	txB_write (tx-marked, txn-B, deps=[preB], RPUSH "69")
//	  ↑
//	bindB  (consumed=[preB], newTip=txB_write, txn-B)  ← VISIBLE
//
// Reading K with tip=bindB walks bindB → txB_write → preB → bindA → txA_write
// → root. With HEAD's per-bind filter, bindA is skipped, so txA_write's "66"
// is elided; only bindB's "69" appears. The correct outcome is either
// (a) both "66" and "69" in the list (honoring the DAG ancestor), or (b)
// refusing to include bindB (because its ancestor isn't yet visible).
func TestReconstructIncludesDescendantBindWithInvisibleAncestor(t *testing.T) {
	log := newSnapshotLog()
	e := newSnapshotEngineWithHorizon(log)

	const key = "el-4"

	// root: non-tx RPUSH "base" — establishes K's initial tip
	root := log.putEffect(&pb.Effect{
		Key:            []byte(key),
		Hlc:            sTs(10),
		NodeId:         1,
		ForkChoiceHash: ComputeForkChoiceHash(1, sTs(10)),
		Kind:           &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "base", []byte("base"))},
	})

	// writer-66 (txn-A): tx-marked RPUSH "66", then bind
	txAWrite := log.putEffect(&pb.Effect{
		Key:            []byte(key),
		Hlc:            sTs(20),
		NodeId:         2,
		TxnId:          "txn-A",
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(20)),
		Deps:           []*pb.EffectRef{toPbRef(root)},
		Kind:           &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "writer-66", []byte("66"))},
	})
	bindA := log.putEffect(&pb.Effect{
		Key:            []byte(key),
		Hlc:            sTs(25),
		NodeId:         2,
		TxnId:          "txn-A",
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(25)),
		Deps:           []*pb.EffectRef{toPbRef(txAWrite)},
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			Keys: []*pb.TransactionalBindEffect_KeyBind{{
				Key:          []byte(key),
				ConsumedTips: []*pb.EffectRef{toPbRef(root)},
				NewTip:       toPbRef(txAWrite),
			}},
			OriginatorNodeId: 2,
			TxnHlc:           sTs(20),
		}},
	})

	// writer-69's pre-tx tip: a non-tx Noop on K whose deps include bindA.
	// This mirrors the production dag.walk encoding showing writer-69's
	// pre-tx chain [7642733957363584996, 18] depending on writer-66's bind.
	preB := log.putEffect(&pb.Effect{
		Key:            []byte(key),
		Hlc:            sTs(30),
		NodeId:         3,
		ForkChoiceHash: ComputeForkChoiceHash(3, sTs(30)),
		Deps:           []*pb.EffectRef{toPbRef(bindA)},
		Kind:           &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	})

	// writer-69 (txn-B): tx-marked RPUSH "69", then bind. ConsumedTips
	// references preB (the tip writer-69 observed at txn start).
	txBWrite := log.putEffect(&pb.Effect{
		Key:            []byte(key),
		Hlc:            sTs(40),
		NodeId:         3,
		TxnId:          "txn-B",
		ForkChoiceHash: ComputeForkChoiceHash(3, sTs(40)),
		Deps:           []*pb.EffectRef{toPbRef(preB)},
		Kind:           &pb.Effect_Data{Data: orderedInsert(pb.Placement_PLACE_TAIL, "writer-69", []byte("69"))},
	})
	bindB := log.putEffect(&pb.Effect{
		Key:            []byte(key),
		Hlc:            sTs(45),
		NodeId:         3,
		TxnId:          "txn-B",
		ForkChoiceHash: ComputeForkChoiceHash(3, sTs(45)),
		Deps:           []*pb.EffectRef{toPbRef(txBWrite)},
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			Keys: []*pb.TransactionalBindEffect_KeyBind{{
				Key:          []byte(key),
				ConsumedTips: []*pb.EffectRef{toPbRef(preB)},
				NewTip:       toPbRef(txBWrite),
			}},
			OriginatorNodeId: 3,
			TxnHlc:           sTs(40),
		}},
	})

	// Install bindA invisible in the HorizonSet — never call MakeVisible.
	// Matches row "12.321518Z 10.0.0.85 HorizonSet.Add (remote) writer-66
	// invisible" — A is in horizon on the reader and stays there past
	// the moment we read.
	e.horizon.Add("txn-A", bindA,
		bindA_pbBind(key, []Tip{root}, txAWrite))
	if !e.horizon.IsInvisible("txn-A") {
		t.Fatal("setup: txn-A should be invisible after Add")
	}

	// Install bindB invisible, then immediately MakeVisible — matches
	// "12.331682Z HorizonSet.Add (remote) writer-69 invisible" followed
	// by "12.467906Z HorizonSet.MakeVisible writer-69 visible on reader".
	e.horizon.Add("txn-B", bindB,
		bindA_pbBind(key, []Tip{preB}, txBWrite))
	e.horizon.MakeVisible("txn-B")
	if e.horizon.IsInvisible("txn-B") {
		t.Fatal("setup: txn-B should be visible after MakeVisible")
	}
	if !e.horizon.IsInvisible("txn-A") {
		t.Fatal("setup: txn-A should still be invisible after txn-B was made visible")
	}

	// Read K with tip=bindB — corresponds to row 12.502999Z reconstruct on
	// 10.0.0.85 reading el-4 at the moment writer-69 is visible but
	// writer-66 still isn't.
	result, _, err := e.reconstruct(key, []Tip{bindB}, "", false)
	if err != nil {
		t.Fatalf("reconstruct error: %v", err)
	}
	got := orderedValues(result)
	sort.Strings(got)

	hasBase := false
	has66 := false
	has69 := false
	for _, v := range got {
		switch v {
		case "base":
			hasBase = true
		case "66":
			has66 = true
		case "69":
			has69 = true
		}
	}

	// Correct outcomes:
	//   (a) Honor both binds: result contains "66" AND "69" (and "base")
	//   (b) Refuse to include the descendant: result excludes "69" too
	//       (refuses to emit bindB because its DAG ancestor is invisible)
	//
	// Wrong outcome (the bug, observed at 12.502999Z on 10.0.0.85):
	//   result contains "69" but NOT "66" — bindB's effects surfaced
	//   while its DAG ancestor bindA was silently elided.
	if has69 && !has66 {
		t.Fatalf("DAG-ancestor invariant violated: reconstruct emitted "+
			"descendant bind B's value %q while its INVISIBLE DAG-ancestor "+
			"bind A's value %q was silently elided. "+
			"Expected either both values (honor both binds) or neither (refuse B "+
			"because its ancestor is invisible). got=%v base=%v 66=%v 69=%v",
			"69", "66", got, hasBase, has66, has69)
	}
}

// bindA_pbBind builds a TransactionalBindEffect for HorizonSet.Add.
// (Name disambiguates from the existing testBind helper in horizon_test.go.)
func bindA_pbBind(key string, consumed []Tip, newTip Tip) *pb.TransactionalBindEffect {
	refs := make([]*pb.EffectRef, len(consumed))
	for i, t := range consumed {
		refs[i] = toPbRef(t)
	}
	return &pb.TransactionalBindEffect{
		TxnHlc:           sTs(100),
		OriginatorNodeId: 1,
		Keys: []*pb.TransactionalBindEffect_KeyBind{{
			Key:          []byte(key),
			ConsumedTips: refs,
			NewTip:       toPbRef(newTip),
		}},
	}
}

// newSnapshotEngineWithHorizon mirrors newSnapshotEngine but also attaches a
// HorizonSet (using fake timers so visibility decisions are explicit, not
// timer-driven).
func newSnapshotEngineWithHorizon(log *snapshotLog) *Engine {
	var ec *clox.CloxCache[Tip, *pb.Effect]
	if log != nil {
		ec = log.effectCache
	} else {
		ec = clox.NewCloxCache[Tip, *pb.Effect](clox.ConfigFromMemorySize(1024 * 1024))
	}
	e := &Engine{
		index:             keytrie.New(),
		effectCache:       ec,
		nodeID:            1,
		clock:             crdt.NewHLC(),
		subscriptions:     xsync.NewMap[string, *subscriptionState](),
		peerSubscribers:   xsync.NewMap[string, *xsync.Map[pb.NodeID, struct{}]](),
		pendingTxns:       xsync.NewMap[Tip, *pendingTxn](),
		pendingTxTips:     xsync.NewMap[Tip, []Tip](),
		txAbortCounts:     xsync.NewMap[string, *atomic.Int32](),
		pendingBootstraps: xsync.NewMap[string, *bootstrapCollector](),
		unsubInFlight:     xsync.NewMap[string, struct{}](),
		spokenBinds:       clox.NewCloxCache[Tip, struct{}](clox.ConfigFromCapacity(256)),
	}
	e.safety.Store(&safetyMap{defaultMode: UnsafeMode})
	e.horizon = newHorizonSet(e)
	// Replace timer creation with a no-op so invisible binds stay invisible
	// for the duration of the test (no fire from time.AfterFunc).
	e.horizon.afterFunc = func(d time.Duration, f func()) *time.Timer {
		_ = f
		_ = d
		return time.NewTimer(time.Hour)
	}
	return e
}
