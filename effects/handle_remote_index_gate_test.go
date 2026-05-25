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
	"slices"
	"testing"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"google.golang.org/protobuf/proto"
)

// Rule 1 (docs/reconstruction.md): HandleRemote must never update the
// index for a key the local node is not subscribed to. The bind's bytes
// can still land in the effectCache (a HandleRemote arrival is often
// needed for canonical-key work on a different key that's subscribed
// locally), but the per-key index mutation is gated on subscription.

// TestHandleRemote_Rule1_UnsubscribedCanonical_NoIndexUpdate covers the
// case from run 25907365258: a bind arrives whose canonical key (eff.Key)
// is NOT subscribed locally, but an additional touched key IS. The
// additional-key index gets the bind; the canonical does not.
func TestHandleRemote_Rule1_UnsubscribedCanonical_NoIndexUpdate(t *testing.T) {
	e := newTestEngineForEphemeral(t)

	// Subscribe only to "el-1". The bind's canonical key will be "el-0",
	// which we never see locally — we only participate in el-1.
	e.subscriptions.Store("el-1", &subscriptionState{ready: make(chan struct{})})

	bindOff := Tip{2, 4000}
	bind := &pb.TransactionalBindEffect{
		TxnHlc:           sTs(50),
		OriginatorNodeId: 2,
		Keys: []*pb.TransactionalBindEffect_KeyBind{
			{Key: []byte("el-0"), NewTip: &pb.EffectRef{NodeId: 2, Offset: 3900}},
			{Key: []byte("el-1"), NewTip: &pb.EffectRef{NodeId: 2, Offset: 3950}},
		},
	}
	eff := &pb.Effect{
		Key:            []byte("el-0"),
		Hlc:            sTs(50),
		NodeId:         2,
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(50)),
		TxnId:          "2:50:1",
		Kind:           &pb.Effect_TxnBind{TxnBind: bind},
	}
	data, err := proto.Marshal(eff)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	notify := BuildOffsetNotify(2, bindOff, eff, data, nil)

	if _, err := e.HandleRemote(notify); err != nil {
		t.Fatalf("HandleRemote: %v", err)
	}

	// el-1: we are subscribed, the bind must be indexed.
	if tips := e.index.Contains("el-1"); tips == nil {
		t.Fatal("el-1: expected bind to be indexed under subscribed key")
	} else {
		found := slices.Contains(tips.Tips(), bindOff)
		if !found {
			t.Errorf("el-1: bind tip %v not in index tips %v", bindOff, tips.Tips())
		}
	}

	// el-0: we are NOT subscribed, the index must be untouched.
	if tips := e.index.Contains("el-0"); tips != nil {
		t.Errorf("el-0: index must not be created for unsubscribed key, got tips %v", tips.Tips())
	}

	// The bind's bytes are still in the effect cache — Rule 1 separates
	// cache acceptance from index participation.
	if e.effectCache != nil {
		if _, ok := e.effectCache.Get(bindOff, 0); !ok {
			t.Errorf("effect cache: bind %v should be cached even when canonical key is unsubscribed", bindOff)
		}
	}
}

// TestHandleRemote_Rule1_UnsubscribedAdditional_NoIndexUpdate is the
// symmetric case: subscribed to the canonical key, the additional key
// is the unsubscribed one.
func TestHandleRemote_Rule1_UnsubscribedAdditional_NoIndexUpdate(t *testing.T) {
	e := newTestEngineForEphemeral(t)

	// Subscribe only to "el-0" (canonical). The additional key "el-1"
	// is touched by the bind but not subscribed here.
	e.subscriptions.Store("el-0", &subscriptionState{ready: make(chan struct{})})

	bindOff := Tip{2, 4000}
	bind := &pb.TransactionalBindEffect{
		TxnHlc:           sTs(60),
		OriginatorNodeId: 2,
		Keys: []*pb.TransactionalBindEffect_KeyBind{
			{Key: []byte("el-0"), NewTip: &pb.EffectRef{NodeId: 2, Offset: 3900}},
			{Key: []byte("el-1"), NewTip: &pb.EffectRef{NodeId: 2, Offset: 3950}},
		},
	}
	eff := &pb.Effect{
		Key:            []byte("el-0"),
		Hlc:            sTs(60),
		NodeId:         2,
		ForkChoiceHash: ComputeForkChoiceHash(2, sTs(60)),
		TxnId:          "2:60:1",
		Kind:           &pb.Effect_TxnBind{TxnBind: bind},
	}
	data, err := proto.Marshal(eff)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	notify := BuildOffsetNotify(2, bindOff, eff, data, nil)

	if _, err := e.HandleRemote(notify); err != nil {
		t.Fatalf("HandleRemote: %v", err)
	}

	// el-0: subscribed, indexed.
	if tips := e.index.Contains("el-0"); tips == nil {
		t.Fatal("el-0: expected bind to be indexed under subscribed canonical key")
	}

	// el-1: not subscribed, must NOT be indexed.
	if tips := e.index.Contains("el-1"); tips != nil {
		t.Errorf("el-1: index must not be created for unsubscribed additional key, got tips %v", tips.Tips())
	}
}
