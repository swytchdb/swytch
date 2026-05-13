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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	clox "github.com/swytchdb/swytch/cache"
	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/crdt"
	"github.com/swytchdb/swytch/keytrie"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newHorizonTestEngine() *Engine {
	e := &Engine{
		effectCache:       clox.NewCloxCache[Tip, *pb.Effect](clox.ConfigFromMemorySize(1024 * 1024)),
		index:             keytrie.New(),
		nodeID:            1,
		clock:             crdt.NewHLC(),
		subscriptions:     xsync.NewMap[string, *subscriptionState](),
		pendingTxns:       xsync.NewMap[Tip, *pendingTxn](),
		pendingTxTips:     xsync.NewMap[Tip, []Tip](),
		txAbortCounts:     xsync.NewMap[string, *atomic.Int32](),
		pendingBootstraps: xsync.NewMap[string, *bootstrapCollector](),
		unsubInFlight:     xsync.NewMap[string, struct{}](),
		voidedBinds:       clox.NewCloxCache[string, struct{}](clox.ConfigFromCapacity(256)),
	}
	e.safety.Store(&safetyMap{defaultMode: UnsafeMode})
	e.horizon = newHorizonSet(e, 500*time.Millisecond)
	return e
}

// installFakeTimers replaces timer creation with a controllable fire function.
func installFakeTimers(h *HorizonSet) func() {
	var mu sync.Mutex
	var fns []func()

	h.afterFunc = func(d time.Duration, f func()) *time.Timer {
		mu.Lock()
		fns = append(fns, f)
		mu.Unlock()
		return time.NewTimer(time.Hour) // never fires
	}

	return func() {
		mu.Lock()
		defer mu.Unlock()
		for _, f := range fns {
			f()
		}
		fns = nil
	}
}

func TestHorizonSet_StandaloneEngineNilHorizon(t *testing.T) {
	e := &Engine{
		effectCache:       clox.NewCloxCache[Tip, *pb.Effect](clox.ConfigFromMemorySize(1024 * 1024)),
		index:             keytrie.New(),
		nodeID:            1,
		clock:             crdt.NewHLC(),
		subscriptions:     xsync.NewMap[string, *subscriptionState](),
		pendingTxns:       xsync.NewMap[Tip, *pendingTxn](),
		pendingTxTips:     xsync.NewMap[Tip, []Tip](),
		txAbortCounts:     xsync.NewMap[string, *atomic.Int32](),
		pendingBootstraps: xsync.NewMap[string, *bootstrapCollector](),
		unsubInFlight:     xsync.NewMap[string, struct{}](),
	}
	e.safety.Store(&safetyMap{defaultMode: UnsafeMode})
	if e.horizon != nil {
		t.Fatal("standalone engine should have nil horizon")
	}
}

func testBind(consumedTips []*pb.EffectRef, newTip *pb.EffectRef) *pb.TransactionalBindEffect {
	return &pb.TransactionalBindEffect{
		TxnHlc:           timestamppb.New(time.Unix(0, 100)),
		OriginatorNodeId: 1,
		Keys: []*pb.TransactionalBindEffect_KeyBind{
			{Key: []byte("k1"), ConsumedTips: consumedTips, NewTip: newTip},
		},
	}
}

func TestHorizonSet_AddMakesInvisible(t *testing.T) {
	e := newHorizonTestEngine()
	fireTimer := installFakeTimers(e.horizon)
	_ = fireTimer

	bind := testBind(
		[]*pb.EffectRef{{NodeId: 1, Offset: 10}},
		&pb.EffectRef{NodeId: 1, Offset: 20},
	)

	e.horizon.Add("tx1", Tip{1, 30}, bind, nil)

	if !e.horizon.IsInvisible("tx1") {
		t.Fatal("tx1 should be invisible after Add")
	}
}

func TestHorizonSet_MakeVisibleRemovesEntry(t *testing.T) {
	e := newHorizonTestEngine()
	fireTimer := installFakeTimers(e.horizon)
	_ = fireTimer

	e.pendingTxTips.Store(Tip{1, 20}, []Tip{{1, 10}})

	bind := testBind(
		[]*pb.EffectRef{{NodeId: 1, Offset: 10}},
		&pb.EffectRef{NodeId: 1, Offset: 20},
	)

	e.horizon.Add("tx1", Tip{1, 30}, bind, nil)
	e.horizon.MakeVisible("tx1")

	if e.horizon.IsInvisible("tx1") {
		t.Fatal("tx1 should be visible after MakeVisible")
	}

	if _, ok := e.pendingTxTips.Load(Tip{1, 20}); ok {
		t.Fatal("pendingTxTips should be cleaned after MakeVisible")
	}
}

func TestHorizonSet_TimerFiresMakesVisible(t *testing.T) {
	e := newHorizonTestEngine()
	fireTimer := installFakeTimers(e.horizon)

	e.pendingTxTips.Store(Tip{1, 20}, []Tip{{1, 10}})

	bind := testBind(
		[]*pb.EffectRef{{NodeId: 1, Offset: 10}},
		&pb.EffectRef{NodeId: 1, Offset: 20},
	)

	e.horizon.Add("tx1", Tip{1, 30}, bind, nil)

	if !e.horizon.IsInvisible("tx1") {
		t.Fatal("tx1 should be invisible before timer fires")
	}

	fireTimer()

	if e.horizon.IsInvisible("tx1") {
		t.Fatal("tx1 should be visible after timer fires")
	}

	if _, ok := e.pendingTxTips.Load(Tip{1, 20}); ok {
		t.Fatal("pendingTxTips should be cleaned after timer fires")
	}
}
