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

func tTs(nanos int64) *timestamppb.Timestamp {
	return timestamppb.New(time.Unix(0, nanos))
}

// testIsRealConflict is a shim for tests that predate the
// Engine-method signature. It spins up a minimal Engine so
// isRealConflict's predicate-refinement path can no-op out (it
// requires ptxn.txnID + a fetchable competing effect, neither of
// which these structural tests set up). When those two preconditions
// are absent, predicate refinement is bypassed and the test's
// shared-base + tie-break expectations match the pre-refinement
// behaviour.
func testIsRealConflict(t *testing.T, ptxn *pendingTxn, key string, detail *pb.NackTipDetail) bool {
	t.Helper()
	e := &Engine{}
	return e.isRealConflict(ptxn, key, detail)
}

func newTestPendingTxn(txnHLCNanos int64, keys ...pendingTxnKey) *pendingTxn {
	txnHLC := time.Unix(0, txnHLCNanos)
	return &pendingTxn{
		txnHLC:         txnHLC,
		originNode:     1,
		forkChoiceHash: ComputeForkChoiceHash(1, timestamppb.New(txnHLC)),
		bindOffset:     Tip{1, 9999},
		keys:           keys,
		done:           make(chan struct{}),
	}
}

func TestIsRealConflict_MetaEffect_NotConflict(t *testing.T) {
	ptxn := newTestPendingTxn(100, pendingTxnKey{
		key:        "k",
		newTip:     Tip{1, 500},
		collection: pb.CollectionKind_SCALAR,
	})

	detail := &pb.NackTipDetail{
		Ref:             &pb.EffectRef{NodeId: 1, Offset: 600},
		Hlc:             tTs(110),
		IsData:          false,
		IsBind:          false,
		IsTransactional: false,
	}

	if testIsRealConflict(t, ptxn, "k", detail) {
		t.Fatal("MetaEffect should not be a real conflict")
	}
}

func TestIsRealConflict_SameElementID_Conflict(t *testing.T) {
	ptxn := newTestPendingTxn(100, pendingTxnKey{
		key:        "k",
		newTip:     Tip{1, 500},
		collection: pb.CollectionKind_KEYED,
		elementIDs: [][]byte{[]byte("field1")},
	})

	detail := &pb.NackTipDetail{
		Ref:        &pb.EffectRef{NodeId: 1, Offset: 600},
		Hlc:        tTs(110),
		IsData:     true,
		Collection: pb.CollectionKind_KEYED,
		ElementId:  []byte("field1"),
	}

	if !testIsRealConflict(t, ptxn, "k", detail) {
		t.Fatal("same element ID should be a real conflict")
	}
}

func TestIsRealConflict_DifferentElementID_NotConflict(t *testing.T) {
	ptxn := newTestPendingTxn(100, pendingTxnKey{
		key:        "k",
		newTip:     Tip{1, 500},
		collection: pb.CollectionKind_KEYED,
		elementIDs: [][]byte{[]byte("field1")},
	})

	detail := &pb.NackTipDetail{
		Ref:        &pb.EffectRef{NodeId: 1, Offset: 600},
		Hlc:        tTs(110),
		IsData:     true,
		Collection: pb.CollectionKind_KEYED,
		ElementId:  []byte("field2"),
	}

	if testIsRealConflict(t, ptxn, "k", detail) {
		t.Fatal("different element ID should not be a real conflict")
	}
}

func TestIsRealConflict_NoopKey_AnyWriteConflicts(t *testing.T) {
	ptxn := newTestPendingTxn(100, pendingTxnKey{
		key:        "k",
		newTip:     Tip{1, 500},
		readOnly:   true,
		collection: pb.CollectionKind_SCALAR,
	})

	detail := &pb.NackTipDetail{
		Ref:        &pb.EffectRef{NodeId: 1, Offset: 600},
		Hlc:        tTs(110),
		IsData:     true,
		Collection: pb.CollectionKind_SCALAR,
	}

	if !testIsRealConflict(t, ptxn, "k", detail) {
		t.Fatal("any write on a read-only key should be a real conflict (phantom write)")
	}
}

func TestIsRealConflict_DependentEffect_NotConflict(t *testing.T) {
	ptxn := newTestPendingTxn(100, pendingTxnKey{
		key:        "k",
		newTip:     Tip{1, 500},
		collection: pb.CollectionKind_SCALAR,
	})

	// The competing effect depends on our tx tip → sequential, not concurrent
	detail := &pb.NackTipDetail{
		Ref:        &pb.EffectRef{NodeId: 1, Offset: 600},
		Hlc:        tTs(110),
		IsData:     true,
		Collection: pb.CollectionKind_SCALAR,
		Deps:       []*pb.EffectRef{{NodeId: 1, Offset: 500}}, // depends on our newTip
	}

	if testIsRealConflict(t, ptxn, "k", detail) {
		t.Fatal("effect that depends on our tx should not be a real conflict")
	}
}

// A NACK carrying a competing bind from another txn is resolved by
// ForkChoiceHash, the same arbiter reconstruct uses. We abort iff their
// hash is lower than ours; otherwise we win and continue.
func TestIsRealConflict_CompetingBind_TheirHashLower_WeAbort(t *testing.T) {
	ptxn := newTestPendingTxn(100, pendingTxnKey{
		key:          "k",
		newTip:       Tip{1, 500},
		consumedTips: []Tip{{1, 300}},
		collection:   pb.CollectionKind_SCALAR,
	})
	// Find a (nodeID, hlc) pair whose hash sorts below ours.
	theirNode := pb.NodeID(2)
	theirHash := ComputeForkChoiceHash(theirNode, tTs(100))
	for !ForkChoiceLess(theirHash, ptxn.forkChoiceHash) {
		theirNode++
		theirHash = ComputeForkChoiceHash(theirNode, tTs(100))
	}
	detail := &pb.NackTipDetail{
		Ref:                &pb.EffectRef{NodeId: 1, Offset: 600},
		Hlc:                tTs(100),
		IsBind:             true,
		BindHlc:            tTs(100),
		BindNodeId:         uint64(theirNode),
		BindForkChoiceHash: theirHash,
		BindConsumedTips: []*pb.KeyConsumedTips{
			{Key: []byte("k"), ConsumedTips: []*pb.EffectRef{{NodeId: 1, Offset: 400}}},
		},
	}
	if !testIsRealConflict(t, ptxn, "k", detail) {
		t.Fatal("their lower hash → we must abort")
	}
}

func TestIsRealConflict_CompetingBind_OurHashLower_WeWin(t *testing.T) {
	ptxn := newTestPendingTxn(100, pendingTxnKey{
		key:          "k",
		newTip:       Tip{1, 500},
		consumedTips: []Tip{{1, 300}},
		collection:   pb.CollectionKind_SCALAR,
	})
	// Find a (nodeID, hlc) pair whose hash sorts above ours.
	theirNode := pb.NodeID(2)
	theirHash := ComputeForkChoiceHash(theirNode, tTs(200))
	for ForkChoiceLess(theirHash, ptxn.forkChoiceHash) {
		theirNode++
		theirHash = ComputeForkChoiceHash(theirNode, tTs(200))
	}
	detail := &pb.NackTipDetail{
		Ref:                &pb.EffectRef{NodeId: 1, Offset: 600},
		Hlc:                tTs(200),
		IsBind:             true,
		BindHlc:            tTs(200),
		BindNodeId:         uint64(theirNode),
		BindForkChoiceHash: theirHash,
		BindConsumedTips: []*pb.KeyConsumedTips{
			{Key: []byte("k"), ConsumedTips: []*pb.EffectRef{{NodeId: 1, Offset: 400}}},
		},
	}
	if testIsRealConflict(t, ptxn, "k", detail) {
		t.Fatal("our lower hash → we must win and continue")
	}
}

func TestIsRealConflict_NonTxCompetitor_Conflict(t *testing.T) {
	ptxn := newTestPendingTxn(100, pendingTxnKey{
		key:        "k",
		newTip:     Tip{1, 500},
		collection: pb.CollectionKind_SCALAR,
	})

	// Non-transactional data effect on same key
	detail := &pb.NackTipDetail{
		Ref:             &pb.EffectRef{NodeId: 1, Offset: 600},
		Hlc:             tTs(110),
		IsData:          true,
		IsTransactional: false,
		Collection:      pb.CollectionKind_SCALAR,
	}

	if !testIsRealConflict(t, ptxn, "k", detail) {
		t.Fatal("non-tx data write on same scalar key should be a real conflict")
	}
}

func TestIsRealConflict_InProgressTx_NotConflict(t *testing.T) {
	ptxn := newTestPendingTxn(100, pendingTxnKey{
		key:        "k",
		newTip:     Tip{1, 500},
		collection: pb.CollectionKind_SCALAR,
	})

	// Transactional effect without a bind → in-progress, not competing yet
	detail := &pb.NackTipDetail{
		Ref:             &pb.EffectRef{NodeId: 1, Offset: 600},
		Hlc:             tTs(110),
		IsData:          true,
		IsTransactional: true,
		IsBind:          false,
		Collection:      pb.CollectionKind_SCALAR,
	}

	if testIsRealConflict(t, ptxn, "k", detail) {
		t.Fatal("in-progress tx effect without bind should not be a real conflict")
	}
}

func TestCommitAbortPendingTxn(t *testing.T) {
	ptxn := newTestPendingTxn(100)

	// Commit should succeed
	if !commitPendingTxn(ptxn) {
		t.Fatal("first commit should succeed")
	}

	// Channel should be closed
	select {
	case <-ptxn.done:
	default:
		t.Fatal("done channel should be closed after commit")
	}

	if ptxn.state.Load() != txnStateCommitted {
		t.Fatal("state should be committed")
	}

	// Second commit should fail (already committed)
	if commitPendingTxn(ptxn) {
		t.Fatal("second commit should fail")
	}

	// Abort should also fail
	if abortPendingTxn(ptxn) {
		t.Fatal("abort after commit should fail")
	}
}

func TestAbortThenCommit(t *testing.T) {
	ptxn := newTestPendingTxn(100)

	if !abortPendingTxn(ptxn) {
		t.Fatal("first abort should succeed")
	}

	select {
	case <-ptxn.done:
	default:
		t.Fatal("done channel should be closed after abort")
	}

	if ptxn.state.Load() != txnStateAborted {
		t.Fatal("state should be aborted")
	}

	if commitPendingTxn(ptxn) {
		t.Fatal("commit after abort should fail")
	}
}

func TestIsRealConflict_SubscriptionEffect_NotConflict(t *testing.T) {
	ptxn := newTestPendingTxn(100, pendingTxnKey{
		key:        "k",
		newTip:     Tip{1, 500},
		collection: pb.CollectionKind_SCALAR,
	})

	// SubscriptionEffect: not data, not bind
	detail := &pb.NackTipDetail{
		Ref:    &pb.EffectRef{NodeId: 1, Offset: 600},
		Hlc:    tTs(110),
		IsData: false,
		IsBind: false,
	}

	if testIsRealConflict(t, ptxn, "k", detail) {
		t.Fatal("SubscriptionEffect should not be a real conflict")
	}
}

func TestIsRealConflict_DependsOnBindOffset_NotConflict(t *testing.T) {
	ptxn := newTestPendingTxn(100, pendingTxnKey{
		key:        "k",
		newTip:     Tip{1, 500},
		collection: pb.CollectionKind_SCALAR,
	})

	// Effect that depends on our bind offset → sequential
	detail := &pb.NackTipDetail{
		Ref:        &pb.EffectRef{NodeId: 1, Offset: 600},
		Hlc:        tTs(110),
		IsData:     true,
		Collection: pb.CollectionKind_SCALAR,
		Deps:       []*pb.EffectRef{{NodeId: 1, Offset: 9999}}, // depends on our bindOffset
	}

	if testIsRealConflict(t, ptxn, "k", detail) {
		t.Fatal("effect depending on our bind should not conflict")
	}
}
