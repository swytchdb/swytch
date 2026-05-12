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
	"testing"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestConcurrentRowWritesSharedBase_WeWin: when our
// ForkChoiceHash is lower than theirs, we should commit. The test
// sets up the same structure but with a their-hash that's
// guaranteed higher than any real hash we produce (all-ones).
func TestConcurrentRowWritesSharedBase_WeWin(t *testing.T) {
	bc := &txnMockBroadcaster{}
	e := newTxnTestEngine(bc)

	subCtx := e.NewContext()
	if err := subCtx.Emit(&pb.Effect{
		Key: []byte("k"),
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
			SubscriberNodeId: 99,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := subCtx.Flush(); err != nil {
		t.Fatal(err)
	}

	setupCtx := e.NewContext()
	if err := setupCtx.Emit(scalarSet("k", "base")); err != nil {
		t.Fatal(err)
	}
	if err := setupCtx.Flush(); err != nil {
		t.Fatal(err)
	}
	setupTips := e.index.Contains("k")
	var sharedBase []*pb.EffectRef
	for _, tp := range setupTips.Tips() {
		sharedBase = append(sharedBase, toPbRef(tp))
	}

	theirTxnID := "them-tx"
	theirRWOffset := Tip{5, 699}
	theirHLC := timestamppb.New(time.Unix(0, 10))
	theirRW := &pb.Effect{
		Key:            []byte("k"),
		Hlc:            theirHLC,
		NodeId:         5,
		TxnId:          theirTxnID,
		ForkChoiceHash: ComputeForkChoiceHash(5, theirHLC),
		Kind: &pb.Effect_RowWrite{RowWrite: &pb.RowWriteEffect{
			Kind:    pb.RowWriteEffect_INSERT,
			Columns: []*pb.TypedValue{{Kind: pb.TypedValue_INT, IntVal: 42}},
		}},
	}
	e.effectCache.Put(theirRWOffset, theirRW)

	theirBindOffset := Tip{5, 700}
	theirBindHLC := timestamppb.New(time.Unix(0, 11))
	// All-ones hash: always higher than any real hash. They lose.
	theirHash := make([]byte, 32)
	for i := range theirHash {
		theirHash[i] = 0xff
	}
	theirBind := &pb.Effect{
		Key:            []byte("k"),
		Hlc:            theirBindHLC,
		NodeId:         5,
		TxnId:          theirTxnID,
		ForkChoiceHash: theirHash,
		Kind: &pb.Effect_TxnBind{TxnBind: &pb.TransactionalBindEffect{
			TxnHlc:           theirBindHLC,
			OriginatorNodeId: 5,
			Keys: []*pb.TransactionalBindEffect_KeyBind{
				{
					Key:          []byte("k"),
					ConsumedTips: sharedBase,
					NewTip:       toPbRef(theirRWOffset),
				},
			},
		}},
	}
	e.effectCache.Put(theirBindOffset, theirBind)

	bc.replicateToResults = map[pb.NodeID]*pb.NackNotify{
		99: {
			Key: []byte("k"),
			TipDetails: []*pb.NackTipDetail{
				{
					Ref:                toPbRef(theirBindOffset),
					Hlc:                theirBindHLC,
					IsBind:             true,
					IsTransactional:    true,
					BindHlc:            theirBindHLC,
					BindNodeId:         5,
					BindForkChoiceHash: theirHash,
					BindConsumedTips: []*pb.KeyConsumedTips{
						{Key: []byte("k"), ConsumedTips: sharedBase},
					},
				},
			},
		},
	}

	ctx := e.NewContext()
	ctx.BeginTx()
	if err := ctx.Emit(&pb.Effect{
		Key: []byte("k"),
		Kind: &pb.Effect_RowWrite{RowWrite: &pb.RowWriteEffect{
			Kind:    pb.RowWriteEffect_INSERT,
			Columns: []*pb.TypedValue{{Kind: pb.TypedValue_INT, IntVal: 7}},
		}},
	}); err != nil {
		t.Fatal(err)
	}

	if err := ctx.Flush(); err != nil {
		t.Fatalf("our hash wins fork-choice, should commit; got %v", err)
	}
}
