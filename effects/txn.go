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
	"errors"
	"sync/atomic"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ErrTxnAborted is returned by Flush when a transaction loses FWW or
// encounters a real conflict that cannot be resolved.
var ErrTxnAborted = errors.New("transaction aborted")

// ErrRegionPartitioned is returned by Flush in SafeMode when not all
// same-region peers are reachable.
var ErrRegionPartitioned = errors.New("region partitioned: not all same-region peers are reachable")

// ErrBootstrapIncomplete is returned by ensureSubscribed when the bootstrap
// could not fetch the full causal chain because some peers are unreachable.
// A background retry continues until the chain is complete.
var ErrBootstrapIncomplete = errors.New("bootstrap incomplete: some peers unreachable")

// ErrAuthorityDropped signals that HandleRemote rejected the inbound
// effect because this node has no authority over the key. The
// transport layer interprets this as "do not respond" — neither ACK
// nor NACK is sent — so the sender's tracked replication times out
// rather than counting us as a successful first-ACK replica. The
// sender's other peers (those with authority) still get the chance
// to accept the write.
var ErrAuthorityDropped = errors.New("authority dropped: not subscribed to this key")

// DefaultSerializationThreshold is the number of consecutive aborts on a
// key before escalating to serialized coordination.
const DefaultSerializationThreshold = 3

// txnState constants for pendingTxn.state.
const (
	txnStatePending   uint32 = 0
	txnStateCommitted uint32 = 1
	txnStateAborted   uint32 = 2
)

// pendingTxn tracks a transaction awaiting NACK resolution.
type pendingTxn struct {
	txnID      string // transaction ID (matches TxnId on emitted effects)
	txnHLC     time.Time
	originNode pb.NodeID
	bindOffset Tip             // offset of the TransactionalBindEffect
	keys       []pendingTxnKey // read-only after creation
	state      atomic.Uint32   // txnState*
	done       chan struct{}   // closed on decision
}

type pendingTxnKey struct {
	key          string
	consumedTips []Tip
	newTip       Tip
	readOnly     bool              // only NoopEffect on this key (phantom write detection)
	collection   pb.CollectionKind // collection kind
	elementIDs   [][]byte          // element IDs our tx touches on this key
}

// isRealConflict determines whether a NACK Tip represents a real conflict
// with our pending transaction on the given key.
//
// NACK itself only says "peer's tips diverged from your bind's
// ConsumedTips". Whether that divergence is a real conflict is a
// deterministic function of (our effects, their effects, predicate
// rules) — the same function evaluateBindForkChoice runs on every
// node. All call sites must reach the same answer so the client's
// abort/commit signal matches the cluster's view of the bind.
func (e *Engine) isRealConflict(ptxn *pendingTxn, key string, detail *pb.NackTipDetail) bool {
	// Effects whose deps include any of our tx offsets are sequential, not concurrent
	for _, dep := range detail.Deps {
		if r(dep) == ptxn.bindOffset {
			return false
		}
		for _, pk := range ptxn.keys {
			if r(dep) == pk.newTip {
				return false
			}
		}
	}

	// Commutative effects: never conflict
	if !detail.IsData && !detail.IsBind {
		// MetaEffect, SubscriptionEffect, SerializationEffect, NoopEffect
		return false
	}

	// Competing bind: only a conflict if they share a causal base on
	// any overlapping key. Without shared consumed tips, the binds are
	// independent and coexist — not competing.
	if detail.IsBind {
		sharedKey, shared := nackBindSharedBaseKey(ptxn, detail)
		if !shared {
			return false // different causal bases, not competing
		}
		// Predicate refinement: the peer will reach the same
		// verdict via evaluateBindForkChoice when it processes the
		// competing bind; we must get there too. Walk both sides'
		// obs/rw on the shared key (the bind offset in detail.Ref
		// is a valid entry point — allDeps inside the walker
		// follows the bind's per-key NewTips). If either side
		// lacks evidence, fall back to shared-base + tie-break.
		theirBindOffset := r(detail.Ref)
		if theirEff, err := e.getEffect(theirBindOffset); err == nil {
			theirTxnID := theirEff.TxnId
			if ptxn.txnID != "" && theirTxnID != "" {
				ourStart := collectOurBindTips(ptxn, sharedKey)
				conflict, bothHadEvidence := e.hasPredicateConflict(
					ptxn.txnID, theirTxnID, sharedKey,
					ourStart,
					[]Tip{theirBindOffset})
				if bothHadEvidence && !conflict {
					return false // predicates don't intersect; not a real conflict
				}
			}
		}
		ourHash := ComputeForkChoiceHash(ptxn.originNode, timestamppb.New(ptxn.txnHLC))
		if ForkChoiceLess(detail.BindForkChoiceHash, ourHash) {
			// Their hash is lower → they win, we lose
			return true
		}
		// Our hash is lower → we win
		return false
	}

	// Transactional effect without a bind: in-progress, not yet competing
	if detail.IsTransactional && !detail.IsBind {
		return false
	}

	// Non-transactional DataEffect on same key
	if detail.IsData {
		pk := findPendingKey(ptxn, key)
		if pk == nil {
			return false
		}
		return affectsSameData(pk, detail)
	}

	return false
}

// collectOurBindTips returns the local tx's start tips for the walk
// on a specific key — our bind offset plus our NewTip on that key.
func collectOurBindTips(ptxn *pendingTxn, key string) []Tip {
	tips := []Tip{ptxn.bindOffset}
	for i := range ptxn.keys {
		if ptxn.keys[i].key == key {
			tips = append(tips, ptxn.keys[i].newTip)
			break
		}
	}
	return tips
}

// nackBindSharedBaseKey returns the first key on which the NACK'd
// bind shares a consumed tip with our pending tx, plus a boolean for
// whether any shared base exists at all. Used by isRealConflict to
// scope the predicate walk to the specific key where overlap lives.
func nackBindSharedBaseKey(ptxn *pendingTxn, detail *pb.NackTipDetail) (string, bool) {
	for _, kct := range detail.BindConsumedTips {
		detailKey := string(kct.Key)
		pk := findPendingKey(ptxn, detailKey)
		if pk == nil {
			continue
		}
		ourSet := make(map[Tip]bool, len(pk.consumedTips))
		for _, ct := range pk.consumedTips {
			ourSet[ct] = true
		}
		for _, ct := range kct.ConsumedTips {
			if ourSet[r(ct)] {
				return detailKey, true
			}
		}
	}
	return "", false
}

// affectsSameData checks if a competing Tip affects the same data as our
// transaction's key entry.
func affectsSameData(pk *pendingTxnKey, detail *pb.NackTipDetail) bool {
	// If our key is read-only (NoopEffect), any SCALAR write is a phantom write conflict
	if pk.readOnly {
		if detail.Collection == pb.CollectionKind_SCALAR {
			return true
		}
		// For KEYED/ORDERED, a noop read doesn't conflict with writes to
		// different elements, but we conservatively treat it as conflict
		// because we can't distinguish which elements were "read".
		return true
	}

	// SCALAR: always conflicts (single value per key)
	if pk.collection == pb.CollectionKind_SCALAR || detail.Collection == pb.CollectionKind_SCALAR {
		return true
	}

	// ORDERED: any concurrent insert conflicts — position depends on seeing
	// all prior inserts, so concurrent inserts create incompatible orderings.
	if pk.collection == pb.CollectionKind_ORDERED || detail.Collection == pb.CollectionKind_ORDERED {
		return true
	}

	// KEYED: compare element IDs — concurrent writes to different fields are fine
	if len(detail.ElementId) == 0 {
		return true // no element ID → assume conflict
	}
	for _, eid := range pk.elementIDs {
		if bytes.Equal(eid, detail.ElementId) {
			return true
		}
	}
	return false
}

// nackBindSharesBase checks if a competing bind in a NACK shares a consumed
// Tip with our pending transaction on any overlapping key.
func nackBindSharesBase(ptxn *pendingTxn, detail *pb.NackTipDetail) bool {
	for _, kct := range detail.BindConsumedTips {
		detailKey := string(kct.Key)
		pk := findPendingKey(ptxn, detailKey)
		if pk == nil {
			continue // our tx doesn't touch this key
		}
		// Check if any of our consumed tips overlap with theirs
		ourSet := make(map[Tip]bool, len(pk.consumedTips))
		for _, ct := range pk.consumedTips {
			ourSet[ct] = true
		}
		for _, ct := range kct.ConsumedTips {
			if ourSet[r(ct)] {
				return true
			}
		}
	}
	return false
}

// findPendingKey finds the pendingTxnKey for a given key name.
func findPendingKey(ptxn *pendingTxn, key string) *pendingTxnKey {
	for i := range ptxn.keys {
		if ptxn.keys[i].key == key {
			return &ptxn.keys[i]
		}
	}
	return nil
}

// commitPendingTxn CAS-sets the state to committed and closes the done channel.
func commitPendingTxn(ptxn *pendingTxn) bool {
	if ptxn.state.CompareAndSwap(txnStatePending, txnStateCommitted) {
		close(ptxn.done)
		return true
	}
	return false
}

// abortPendingTxn CAS-sets the state to aborted and closes the done channel.
func abortPendingTxn(ptxn *pendingTxn) bool {
	if ptxn.state.CompareAndSwap(txnStatePending, txnStateAborted) {
		close(ptxn.done)
		return true
	}
	return false
}
