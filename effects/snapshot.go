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
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// filterSnapshot applies post-reconstruction filtering: key-level expiry,
// element-level expiry for KEYED collections, and auto-delete of empty
// collections (Redis semantics: empty lists/sets/hashes/zsets don't exist).
func filterSnapshot(result *pb.ReducedEffect) *pb.ReducedEffect {
	if result == nil {
		return nil
	}
	// Key-level expiry
	if result.ExpiresAt != nil && time.Now().After(result.ExpiresAt.AsTime()) {
		return nil
	}
	// Filter expired elements from KEYED collections
	if len(result.NetAdds) > 0 {
		now := time.Now()
		for k, elem := range result.NetAdds {
			if elem.ExpiresAt != nil && now.After(elem.ExpiresAt.AsTime()) {
				delete(result.NetAdds, k)
			}
		}
	}
	// Empty keyed collection = non-existent (Redis auto-delete)
	if result.Collection == pb.CollectionKind_KEYED &&
		len(result.NetAdds) == 0 && result.Scalar == nil {
		return nil
	}
	// Empty ordered collection = non-existent (Redis auto-delete)
	// Exception: streams with a TypeTag should remain visible even when empty
	// (e.g., a stream with all entries trimmed). Lists auto-delete when empty.
	if result.Collection == pb.CollectionKind_ORDERED &&
		len(result.OrderedElements) == 0 && result.Scalar == nil &&
		result.TypeTag != pb.ValueType_TYPE_STREAM {
		return nil
	}
	// Empty snapshot — either a metadata-only effect (subscription /
	// serialization, Op=UNKNOWN_OP) or a SCALAR DEL tombstone produced
	// by ReduceChain (Op=REMOVE_OP with no data fields, kept around for
	// causal continuity). Both are non-existent from the data perspective.
	if result.Scalar == nil && len(result.NetAdds) == 0 && len(result.OrderedElements) == 0 &&
		(result.Op == pb.EffectOp_UNKNOWN_OP || result.Op == pb.EffectOp_REMOVE_OP) {
		return nil
	}
	return result
}

// GetSnapshot returns the current materialized state of a key and the
// tip offsets the snapshot was derived from. Callers that perform
// read-modify-write (SETBIT, INCR, etc.) must pass the returned tips
// to Emit so the first effect depends on the tips the snapshot was
// actually computed from, not whatever the index contains at Emit time.
//
// Cache hit returns immediately. On miss, walks the causal DAG from
// the index tip set and reconstructs via ReduceBranch + canonical merge.
func (e *Engine) GetSnapshot(key string) (*pb.ReducedEffect, []Tip, int, error) {
	if err := e.ensureSubscribed(key); err != nil {
		return nil, nil, 0, err
	}

	// Read index tips once — these are the tips the returned snapshot
	// corresponds to.  We capture them before the cache check so that
	// a concurrent HandleRemote that evicts the cache AND updates the
	// index between our reads is correctly handled: we either get a
	// cache hit with the pre-update tips (correct), or a cache miss
	// followed by reconstruction against the new tips (also correct).
	indexTips := e.index.Contains(key)
	var snapshotTips []Tip
	if indexTips != nil {
		snapshotTips = indexTips.Tips()
	}

	// Fast path: cache hit
	if e.cache != nil {
		if r, ok := e.cache.Get(key); ok {
			r = filterSnapshot(r)
			if r == nil {
				slog.Debug("GetSnapshot: expired/empty", "key", key)
				e.cache.Evict(key)
				return nil, nil, 0, nil
			}
			slog.Debug("GetSnapshot: cache hit", "key", key)
			return r, snapshotTips, 0, nil
		}
	}

	// Check index for tips
	if len(snapshotTips) == 0 {
		slog.Debug("GetSnapshot: no tips", "key", key)
		return nil, nil, 0, nil
	}

	// Visibility fence: exclude in-progress tx tips, substitute pre-tx deps
	tipOffsets := e.resolveTipDeps(snapshotTips)
	if len(tipOffsets) == 0 {
		return nil, nil, 0, nil
	}

	// Walk DAG and reconstruct
	slog.Debug("GetSnapshot: cache miss, reconstructing", "key", key, "tips", tipOffsets)
	result, chainLen, err := e.reconstruct(key, tipOffsets)
	if err != nil {
		slog.Debug("GetSnapshot: reconstruction incomplete, returning empty", "key", key, "error", err)
		return nil, nil, 0, nil
	}

	// Sync serialization state from reconstruction result before filtering
	// strips metadata-only snapshots. This populates the in-memory map so
	// the handler can do fast lookups without a full snapshot.
	e.updateSerializationState(key, result)

	if result != nil && e.cache != nil {
		e.cache.Put(key, result)
	}

	// Targeted filter: surface a metadata-only ReducedEffect as nil so
	// callers don't see a "key exists" signal driven by the originator's
	// own SubscriptionEffect (installed by ensureSubscribed during this
	// call) or by a REMOVE_OP tombstone. Expiry, element-level expiry,
	// and empty-collection auto-delete are presentation concerns and stay
	// in filterSnapshot for the Context layer — engine.GetSnapshot must
	// return raw causal state for those cases per the test
	// TestGetSnapshot_ExpiredReconstructedStillReturnsState. When we
	// surface nil here, also zero chainLen so the caller's compaction
	// path doesn't dereference a nil result.
	if result != nil &&
		result.Scalar == nil && len(result.NetAdds) == 0 && len(result.OrderedElements) == 0 &&
		(result.Op == pb.EffectOp_UNKNOWN_OP || result.Op == pb.EffectOp_REMOVE_OP) {
		return nil, snapshotTips, 0, nil
	}
	return result, snapshotTips, chainLen, nil
}

// ensureSubscribed records local subscription and emits a SubscriptionEffect.
// On first access, broadcasts to ALL nodes and waits for NACKs with Tip sets
// so we can fetch remote state before GetSnapshot proceeds.
//
// Returns ErrRegionPartitioned if the node is in a minority partition and
// cannot announce the subscription cluster-wide. Per the whitepaper (§3.3),
// a node must be subscribed to a key before any read or write.
func (e *Engine) ensureSubscribed(key string) error {
	state := &subscriptionState{ready: make(chan struct{})}
	if existing, loaded := e.subscriptions.LoadOrStore(key, state); loaded {
		if existing.incomplete.Load() {
			return ErrBootstrapIncomplete
		}
		<-existing.ready
		// Re-check after wait: ready may have been closed by a failure
		// path (markFailed) rather than a successful bootstrap. The
		// closeOnce + Store-before-close in markFailed ensures this
		// load sees the up-to-date value.
		if existing.incomplete.Load() {
			return ErrBootstrapIncomplete
		}
		return nil
	}
	slog.Debug("ensureSubscribed: bootstrapping", "key", key)
	succeeded := false
	defer func() {
		if succeeded {
			state.markReady()
		}
	}()

	hlc := timestamppb.New(e.clock.Now())
	eff := &pb.Effect{
		Key:            []byte(key),
		Hlc:            hlc,
		NodeId:         uint64(e.nodeID),
		ForkChoiceHash: ComputeForkChoiceHash(e.nodeID, hlc),
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
			SubscriberNodeId: uint64(e.nodeID),
		}},
	}
	data, err := MarshalEffect(eff)
	if err != nil {
		// Marshal failure is a code bug; remove state so a fresh call
		// retries from scratch and concurrent waiters re-bootstrap.
		e.subscriptions.Delete(key)
		state.markFailed()
		return err
	}
	offset := e.nextOffset()

	if e.effectCache != nil {
		e.effectCache.Put(offset, eff)
	}
	e.updateIndex(key, nil, offset)

	if e.broadcaster == nil {
		succeeded = true
		return nil
	}

	notify := BuildOffsetNotify(e.nodeID, offset, eff, data, nil)

	// Register bootstrap collector before broadcasting so NACKs aren't missed
	collector := &bootstrapCollector{
		nacks: make(chan *pb.NackNotify, 64),
	}
	e.pendingBootstraps.Store(key, collector)
	defer e.pendingBootstraps.Delete(key)

	// Send to each peer individually and wait for ACKs. Retry if no peers
	// responded (e.g. noise sessions not yet established after restart).
	peerIDs := e.broadcaster.PeerIDs()
	if len(peerIDs) == 0 {
		succeeded = true
		return nil
	}

	var allTipOffsets []Tip
	bootstrapDeadline := time.After(30 * time.Second)

	for attempt := 0; ; attempt++ {
		var mu sync.Mutex
		var successCount sync.WaitGroup
		var ackCount atomic.Int32
		for _, pid := range peerIDs {
			successCount.Add(1)
			go func(pid pb.NodeID) {
				defer successCount.Done()
				nacks, err := e.broadcaster.ReplicateTo(notify, notify.EffectData, pid)
				if err != nil {
					return
				}
				ackCount.Add(1)
				// Collect NACKs returned synchronously from ReplicateTo.
				// HandleRemote on the peer generates these inline and
				// returns them as the ReplicateTo response.
				mu.Lock()
				for _, nack := range nacks {
					for _, tp := range nack.Tips {
						allTipOffsets = append(allTipOffsets, r(tp))
					}
				}
				mu.Unlock()
			}(pid)
		}
		successCount.Wait()

		expected := int(ackCount.Load())
		if expected == 0 {
			// No peers responded — noise sessions likely not ready yet. Retry.
			select {
			case <-bootstrapDeadline:
				slog.Warn("subscription bootstrap: no peers responded after retries",
					"key", key, "attempts", attempt+1)
				goto done
			case <-time.After(500 * time.Millisecond):
				slog.Debug("subscription bootstrap: retrying, no peers ACK'd",
					"key", key, "attempt", attempt+1)
				continue
			}
		}
		break
	}
done:

	// Minority partition check: if we couldn't reach a majority of peers,
	// the subscription wasn't announced cluster-wide. Transactions on
	// this key must not proceed. Delete the state so a later call
	// re-bootstraps once the partition resolves; mark failed so any
	// concurrent waiters re-check incomplete after their <-ready and
	// also surface ErrBootstrapIncomplete.
	if !e.broadcaster.InMajorityPartition() {
		e.subscriptions.Delete(key)
		state.markFailed()
		return ErrRegionPartitioned
	}

	// Second round: re-probe all peers to collect tips that arrived during
	// the first round. When multiple nodes subscribe concurrently, the
	// first round only gets each peer's pre-subscription tips. By now,
	// peers have received our subscription AND may have emitted their own.
	// A second probe collects those updated tips so all nodes converge on
	// the same tip set — critical for fork-choice conflict detection
	// (bindsShareBase requires shared consumed tips).
	{
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, pid := range peerIDs {
			wg.Add(1)
			go func(pid pb.NodeID) {
				defer wg.Done()
				nacks, err := e.broadcaster.ReplicateTo(notify, notify.EffectData, pid)
				if err != nil {
					return
				}
				mu.Lock()
				for _, nack := range nacks {
					for _, tp := range nack.Tips {
						allTipOffsets = append(allTipOffsets, r(tp))
					}
				}
				mu.Unlock()
			}(pid)
		}
		wg.Wait()
	}

	if len(allTipOffsets) == 0 {
		succeeded = true
		return nil
	}

	// Deduplicate
	seen := make(map[Tip]struct{}, len(allTipOffsets))
	unique := make([]Tip, 0, len(allTipOffsets))
	for _, off := range allTipOffsets {
		if _, ok := seen[off]; !ok {
			seen[off] = struct{}{}
			unique = append(unique, off)
		}
	}

	installed, skipped := e.walkAndInstall(key, unique)

	if installed > 0 {
		if skipped > 0 {
			slog.Debug("ensureSubscribed: bootstrap completed with partial reachability",
				"key", key, "installed", installed, "skipped", skipped)
		}
		succeeded = true
		return nil
	}

	// No tips reachable but peers did NACK with state. Could be a
	// real partition or peers all stuck on the same unfetchable
	// chain; retry in the background. The NACK filter in
	// buildEnrichedNack prevents propagating the broken state
	// outward while incomplete. Leaves state in place (incomplete=true)
	// so retryBootstrap can install tips and markReady when reachable.
	slog.Debug("ensureSubscribed: incomplete bootstrap, retrying in background",
		"key", key, "unreachable_tips", skipped)
	state.incomplete.Store(true)
	go e.retryBootstrap(key, state, unique)
	return ErrBootstrapIncomplete
}

// walkAndInstall validates each tip with a single-tip walk; tips
// whose causal chain resolves get installed in the index, others are
// skipped. Returns counts of (installed, skipped) — the invariant
// preserved is "a tip in the index implies the bytes are
// reachable." Walks share work through effectCache, so common
// ancestors are paid for once.
func (e *Engine) walkAndInstall(key string, tips []Tip) (installed, skipped int) {
	for _, tipOff := range tips {
		rd := newDag(e, key, "")
		if walkErr := rd.walk([]Tip{tipOff}, func(*pb.Effect) error { return nil }); walkErr != nil {
			slog.Debug("walkAndInstall: skipping unreachable tip",
				"key", key, "tip", tipOff, "error", walkErr)
			skipped++
			continue
		}
		e.updateIndex(key, nil, tipOff)
		installed++
	}
	return
}

// retryBootstrap re-attempts an incomplete bootstrap on a fixed
// cadence until at least one of the original NACK'd tips becomes
// reachable or the engine closes. On engine shutdown, fails the
// subscription state so any caller parked on <-state.ready unblocks
// and re-checks incomplete (returning ErrBootstrapIncomplete) rather
// than waiting until process exit.
func (e *Engine) retryBootstrap(key string, state *subscriptionState, nackTips []Tip) {
	for !e.closed.Load() {
		time.Sleep(500 * time.Millisecond)

		if e.closed.Load() {
			break
		}

		installed, _ := e.walkAndInstall(key, nackTips)
		if installed == 0 {
			slog.Debug("retryBootstrap: still incomplete",
				"key", key, "tips", len(nackTips))
			continue
		}

		slog.Debug("retryBootstrap: complete",
			"key", key, "installed", installed, "of", len(nackTips))
		state.incomplete.Store(false)
		state.markReady()
		return
	}

	slog.Debug("retryBootstrap: aborted by engine shutdown", "key", key)
	e.subscriptions.Delete(key)
	state.markFailed()
}

// reconstruct rebuilds the materialized state for a key from its effect DAG.
//
// Walks the effect graph from tips to the nearest snapshot using the dag
// walker, which produces a causally ordered sequence with fork-choice
// ordering baked in. Transactional effects are skipped; bind envelopes
// are collected and resolved via fork-choice after the main walk.
//
// Returns the reduced result and the number of effects walked.
func (e *Engine) reconstruct(key string, tips []Tip, currentTxID ...string) (*pb.ReducedEffect, int, error) {
	var txID string
	if len(currentTxID) > 0 {
		txID = currentTxID[0]
	}
	slog.Debug("reconstruct: start", "key", key, "tips", tips, "txn_id", txID)

	// A bind is atomic across every key it touches. To determine who
	// won/lost we cannot just look at the key we're reconstructing —
	// a bind that lost on key L (because another bind beat it there
	// via reachability) must be skipped on every key in its keyset.
	// We compute the bind-key closure starting from `key` (which also
	// transitively subscribes to every key in the closure), then run
	// per-key fork choice on each closure key, seeded with the bind
	// NewTips discovered during closure expansion so the walk reaches
	// every relevant bind even across intervening snapshots.
	expandKeys, bindsByKey, err := e.bindKeyClosure(key)
	if err != nil {
		return nil, 0, err
	}
	atomicallyLost := make(map[string]struct{})
	crossKeyWalked := 0
	for k := range expandKeys {
		losers, walked := e.losersOnKey(k, bindsByKey[k])
		if k != key {
			crossKeyWalked += walked
		}
		for txnID := range losers {
			atomicallyLost[txnID] = struct{}{}
		}
	}
	for txnID := range atomicallyLost {
		e.voidedBinds.Put(txnID, struct{}{})
	}
	if len(expandKeys) > 1 || len(atomicallyLost) > 0 {
		closure := make([]string, 0, len(expandKeys))
		for k := range expandKeys {
			closure = append(closure, k)
		}
		lost := make([]string, 0, len(atomicallyLost))
		for txn := range atomicallyLost {
			lost = append(lost, txn)
		}
		slog.Debug("reconstruct: cross-key closure",
			"key", key,
			"closure", closure,
			"atomically_lost", lost,
			"cross_key_walked", crossKeyWalked)
	}

	d := newDag(e, key, txID)

	var result *pb.ReducedEffect
	var subRootEffects []*pb.Effect
	count := 0

	var isInvisible func(string) bool
	if e.horizon != nil {
		isInvisible = e.horizon.IsInvisible
	}

	err = d.walk(tips, func(eff *pb.Effect) error {
		count++

		if snap := eff.GetSnapshot(); snap != nil && snap.State != nil {
			result = cloneReduced(snap.State)
			return nil
		}

		if eff.GetSubscription() != nil && len(eff.Deps) == 0 {
			subRootEffects = append(subRootEffects, eff)
			return nil
		}

		if bind := eff.GetTxnBind(); bind != nil {
			if isInvisible != nil && isInvisible(eff.TxnId) {
				slog.Debug("reconstruct: skip bind (invisible)", "key", key, "txn", eff.TxnId)
				return nil
			}
			if _, voided := e.voidedBinds.Get(eff.TxnId, 0); voided {
				slog.Debug("reconstruct: skip bind (voided)", "key", key, "txn", eff.TxnId)
				return nil
			}
			if _, lost := atomicallyLost[eff.TxnId]; lost {
				slog.Debug("reconstruct: skip bind (atomically lost)", "key", key, "txn", eff.TxnId)
				return nil
			}
			slog.Debug("reconstruct: include bind", "key", key, "txn", eff.TxnId)
			bindEffects, fetchErr := e.collectBindEffects(eff, key)
			if fetchErr != nil {
				return fetchErr
			}
			result = ReduceChain(result, bindEffects)
			return nil
		}

		result = ReduceChain(result, []*pb.Effect{eff})
		return nil
	})
	if err != nil {
		return nil, 0, err
	}

	if count == 0 {
		return nil, 0, nil
	}

	if len(subRootEffects) > 0 {
		result = ReduceChain(result, subRootEffects)
	}

	// Prune stale tips: any input tip that was collected but is an
	// ancestor (referenced as a dep by another collected node) can be
	// removed from the index. Unreadable tips are preserved — they may
	// be temporarily unavailable. Only prune when at least one real
	// DAG tip is committed (present in the current index) to avoid
	// removing tips superseded only by an in-flight transaction.
	if len(tips) > 1 {
		inputSet := make(map[Tip]bool, len(tips))
		for _, t := range tips {
			inputSet[t] = true
		}
		hasChild := make(map[Tip]bool, len(tips))
		for _, eff := range d.nodes {
			for _, dep := range eff.Deps {
				dt := r(dep)
				if inputSet[dt] {
					hasChild[dt] = true
				}
			}
		}
		realTips := make(map[Tip]bool, len(tips))
		for _, t := range tips {
			if _, ok := d.nodes[t]; ok && !hasChild[t] {
				realTips[t] = true
			}
		}
		if len(realTips) < len(tips) {
			currentIndex := e.index.Contains(key)
			if currentIndex != nil {
				indexSet := make(map[Tip]bool)
				for _, t := range currentIndex.Tips() {
					indexSet[t] = true
				}
				hasCommitted := false
				for t := range realTips {
					if indexSet[t] {
						hasCommitted = true
						break
					}
				}
				if hasCommitted {
					var stale []Tip
					for _, t := range tips {
						if realTips[t] {
							continue
						}
						if _, ok := d.nodes[t]; ok {
							stale = append(stale, t)
						}
					}
					if len(stale) > 0 {
						slog.Debug("reconstruct: pruning stale tips", "key", key, "stale", stale)
						e.index.RemoveTips(key, stale)
					}
				}
			}
		}
	}

	slog.Debug("reconstruct: done", "key", key, "txn_id", txID,
		"count", count, "cross_key_walked", crossKeyWalked,
		"result_nil", result == nil)
	return result, count + crossKeyWalked, nil
}

// forkChoiceBindEntry holds bind metadata for fork-choice resolution.
// Used by the commit path (evaluateBindForkChoice).
type forkChoiceBindEntry struct {
	offset Tip
	hash   []byte
	txnID  string
	keys   map[string][]Tip // key → ConsumedTips
	tips   map[string]Tip   // key → NewTip
}

// bindsShareBase returns true if two binds have overlapping keys with at
// least one shared ConsumedTip on those keys.
func bindsShareBase(a, b *forkChoiceBindEntry) bool {
	for key, aBase := range a.keys {
		bBase, ok := b.keys[key]
		if !ok {
			continue
		}
		aSet := make(map[Tip]bool, len(aBase))
		for _, off := range aBase {
			aSet[off] = true
		}
		for _, off := range bBase {
			if aSet[off] {
				return true
			}
		}
	}
	return false
}

// bindKeyClosure widens outward from startKey along bind cross-key links
// until a fixed point. A bind is atomic across every key it touches, so
// determining whether a bind we encounter on startKey was a winner
// requires per-key verdicts on every key it touches — and transitively
// on every key reachable from those binds.
//
// As each new key K' enters the closure, ensureSubscribed(K') is called
// synchronously. Without it, the local index has no tipset for K' and
// losersOnKey can't see any competitors there — the bind would silently
// surface as a winner. Failing the read on bootstrap error is correct:
// we can't reconstruct K without authoritative state on K'.
//
// bindsByKey[K'] is the set of bind NewTip[K'] offsets discovered during
// the closure walk. Callers pass these as extra starting tips to
// losersOnKey(K') so the per-key walk reaches those binds even when an
// intervening snapshot on K' would otherwise truncate the walk before
// them.
func (e *Engine) bindKeyClosure(startKey string) (map[string]struct{}, map[string][]Tip, error) {
	keys := map[string]struct{}{startKey: {}}
	bindsByKey := make(map[string][]Tip)
	seenBindTip := make(map[string]map[Tip]struct{})
	addBindTip := func(k string, t Tip) {
		s, ok := seenBindTip[k]
		if !ok {
			s = make(map[Tip]struct{})
			seenBindTip[k] = s
		}
		if _, dup := s[t]; dup {
			return
		}
		s[t] = struct{}{}
		bindsByKey[k] = append(bindsByKey[k], t)
	}
	queue := []string{startKey}
	for len(queue) > 0 {
		k := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if err := e.ensureSubscribed(k); err != nil {
			return nil, nil, err
		}
		tipSet := e.index.Contains(k)
		var startTips []Tip
		if tipSet != nil {
			startTips = append(startTips, tipSet.Tips()...)
		}
		startTips = append(startTips, bindsByKey[k]...)
		if len(startTips) == 0 {
			continue
		}
		// Tolerant traversal: dag.walk errors on the first unfetchable
		// dep, which would truncate closure expansion. For closure
		// discovery we only need to find binds reachable from tips,
		// not a complete DAG — skip missing refs and keep going.
		visited := make(map[Tip]bool)
		stack := append([]Tip(nil), startTips...)
		for len(stack) > 0 {
			t := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if visited[t] {
				continue
			}
			visited[t] = true
			eff, err := e.getEffect(t)
			if err != nil {
				continue
			}
			bind := eff.GetTxnBind()
			if bind != nil {
				for _, kb := range bind.Keys {
					other := string(kb.Key)
					if other != k {
						// Cross-key NewTip: seed losersOnKey's walk on
						// `other` so it reaches this bind even when the
						// only path goes through the current key.
						// Same-key NewTip is already reachable from
						// `other`'s own tipset; recording it here just
						// piles redundant starting tips into long
						// single-key chains and explodes the pairwise
						// reach check.
						addBindTip(other, r(kb.NewTip))
					}
					if _, ok := keys[other]; !ok {
						keys[other] = struct{}{}
						queue = append(queue, other)
					}
				}
				// Bind dep: same-key NewTip.
				for _, kb := range bind.Keys {
					if string(kb.Key) == k {
						stack = append(stack, r(kb.NewTip))
						break
					}
				}
				continue
			}
			if eff.GetSnapshot() != nil {
				continue // snapshot is the walk's lower bound
			}
			for _, dep := range eff.Deps {
				stack = append(stack, r(dep))
			}
		}
	}
	return keys, bindsByKey, nil
}

// losersOnKey walks one key's DAG and returns the txnIDs of binds that
// lost the per-key reachability test there. A bind loses when its
// NewTip on `key` does not reach the NewTip of a competitor and the
// competitor has the lower ForkChoiceHash and predicate refinement
// does not prove the writes disjoint. Pairwise hash comparison is the
// arbiter — walk order is not a fork-choice oracle.
//
// extraTips augments the starting set with NewTip[key] offsets of
// binds the caller discovered on other closure keys. Without them, a
// snapshot on `key` between the current tipset and a bind B would
// truncate dag.walk before B and silently miss B's competitors here.
// Returns the loser set and the count of effects walked, which the
// caller folds into reconstruct's chain length so the existing
// compaction trigger fires when cross-key walks accumulate.
func (e *Engine) losersOnKey(key string, extraTips []Tip) (map[string]struct{}, int) {
	losers := make(map[string]struct{})
	tipSet := e.index.Contains(key)
	if tipSet == nil && len(extraTips) == 0 {
		return losers, 0
	}
	var startTips []Tip
	if tipSet != nil {
		startTips = append(startTips, tipSet.Tips()...)
	}
	if len(extraTips) > 0 {
		seen := make(map[Tip]struct{}, len(startTips))
		for _, t := range startTips {
			seen[t] = struct{}{}
		}
		for _, t := range extraTips {
			if _, ok := seen[t]; ok {
				continue
			}
			seen[t] = struct{}{}
			startTips = append(startTips, t)
		}
	}
	if len(startTips) == 0 {
		return losers, 0
	}
	d := newDag(e, key, "")

	// Collect every bind visible on this key. We can't decide winners
	// during the walk — dag.walk's topo order isn't a hash-order, so
	// the first bind we see isn't necessarily the lowest-hash one.
	// Fork choice is over the hashes, period; we run it pairwise after
	// the walk.
	type seenBind struct {
		txnID  string
		newTip Tip
		hash   []byte
	}
	var binds []seenBind

	reaches := func(from, target Tip) bool {
		if from == target {
			return true
		}
		visited := make(map[Tip]bool)
		stack := []Tip{from}
		for len(stack) > 0 {
			cur := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if visited[cur] {
				continue
			}
			visited[cur] = true
			nodeEff, ok := d.nodes[cur]
			if !ok {
				continue
			}
			for _, dep := range nodeEff.Deps {
				dt := r(dep)
				if dt == target {
					return true
				}
				if !visited[dt] {
					stack = append(stack, dt)
				}
			}
		}
		return false
	}

	var isInvisible func(string) bool
	if e.horizon != nil {
		isInvisible = e.horizon.IsInvisible
	}

	walked := 0
	err := d.walk(startTips, func(eff *pb.Effect) error {
		walked++
		bind := eff.GetTxnBind()
		if bind == nil {
			return nil
		}
		if isInvisible != nil && isInvisible(eff.TxnId) {
			return nil
		}
		if _, voided := e.voidedBinds.Get(eff.TxnId, 0); voided {
			return nil
		}
		var newTip Tip
		for _, kb := range bind.Keys {
			if string(kb.Key) == key {
				newTip = r(kb.NewTip)
				break
			}
		}
		var zero Tip
		if newTip == zero {
			return nil
		}
		binds = append(binds, seenBind{
			txnID:  eff.TxnId,
			newTip: newTip,
			hash:   eff.ForkChoiceHash,
		})
		return nil
	})

	// Pairwise fork choice. A bind B loses iff there exists another
	// bind B' on this key such that:
	//   1. neither reaches the other (concurrent siblings on K), AND
	//   2. B' has the lower ForkChoiceHash, AND
	//   3. predicate refinement doesn't prove the writes disjoint.
	for i := range binds {
		b := &binds[i]
		for j := range binds {
			if i == j {
				continue
			}
			other := &binds[j]
			if reaches(b.newTip, other.newTip) || reaches(other.newTip, b.newTip) {
				continue // serializable, not competing
			}
			if !ForkChoiceLess(other.hash, b.hash) {
				continue // they don't beat us; check next
			}
			// They beat us by hash. Predicate refinement is the only out.
			if other.txnID != "" && b.txnID != "" {
				conflict, bothHadEvidence := e.hasPredicateConflict(
					other.txnID, b.txnID, key,
					[]Tip{other.newTip}, []Tip{b.newTip})
				if bothHadEvidence && !conflict {
					continue // disjoint predicates, coexist
				}
			}
			losers[b.txnID] = struct{}{}
			break
		}
	}
	if err != nil {
		slog.Debug("losersOnKey: walk failed", "key", key, "error", err)
	}
	if len(binds) > 0 {
		seen := make([]string, 0, len(binds))
		for _, b := range binds {
			hashPrefix := ""
			if len(b.hash) >= 4 {
				hashPrefix = fmt.Sprintf("%x", b.hash[:4])
			}
			seen = append(seen, fmt.Sprintf("%s@%v#%s", b.txnID, b.newTip, hashPrefix))
		}
		loserList := make([]string, 0, len(losers))
		for txn := range losers {
			loserList = append(loserList, txn)
		}
		slog.Debug("losersOnKey: verdict",
			"key", key,
			"binds_seen", seen,
			"losers", loserList)
	}
	return losers, walked
}

// on the given key. Walks from the bind's NewTip following deps until it
// reaches the ConsumedTips (the pre-transaction state), collecting only
// effects that belong to the same transaction.
func (e *Engine) collectBindEffects(bindEff *pb.Effect, key string) ([]*pb.Effect, error) {
	bind := bindEff.GetTxnBind()
	txnID := bindEff.TxnId

	var newTip Tip
	consumed := make(map[Tip]bool)
	for _, kb := range bind.Keys {
		if string(kb.Key) == key {
			newTip = r(kb.NewTip)
			for _, ct := range fromPbRefs(kb.ConsumedTips) {
				consumed[ct] = true
			}
			break
		}
	}

	var zero Tip
	if newTip == zero {
		return nil, nil
	}

	// Walk from NewTip back to ConsumedTips, collecting txn effects
	var effects []*pb.Effect
	visited := make(map[Tip]bool)
	stack := []Tip{newTip}
	for len(stack) > 0 {
		t := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if visited[t] || consumed[t] || t == zero {
			continue
		}
		visited[t] = true

		eff, err := e.getEffect(t)
		if err != nil {
			return nil, err
		}
		if eff.TxnId != txnID {
			continue
		}
		effects = append(effects, eff)
		for _, dep := range eff.Deps {
			stack = append(stack, r(dep))
		}
	}

	// Reverse: we walked tip→root, need root→tip for ReduceChain
	for i, j := 0, len(effects)-1; i < j; i, j = i+1, j-1 {
		effects[i], effects[j] = effects[j], effects[i]
	}
	return effects, nil
}

// getEffect returns the deserialized Effect at the given offset, fetching
// from the effect cache or remote peers as needed.
func (e *Engine) getEffect(offset Tip) (*pb.Effect, error) {
	// Check deserialized effect cache
	if e.effectCache != nil {
		if cached, ok := e.effectCache.Get(offset, 0); ok {
			return cached, nil
		}
	}

	// Not in cache — try to fetch from remote
	if e.broadcaster == nil {
		return nil, fmt.Errorf("getEffect: offset %d not cached and no broadcaster", offset)
	}
	fetchedData, fetchErr := e.broadcaster.FetchFromAny(toPbRef(offset))
	if fetchErr != nil {
		return nil, fmt.Errorf("getEffect: offset %d fetch failed: %w", offset, fetchErr)
	}
	if storeErr := e.storeWireData(offset, fetchedData); storeErr != nil {
		return nil, fmt.Errorf("getEffect: failed to store fetched offset %d: %w", offset, storeErr)
	}
	// storeWireData puts it in effectCache; try again
	if e.effectCache != nil {
		if cached, ok := e.effectCache.Get(offset, 0); ok {
			return cached, nil
		}
	}
	// Fallback: parse wire data directly
	eff, parseErr := parseWireEffect(fetchedData)
	if parseErr != nil {
		return nil, fmt.Errorf("getEffect: offset %d parse failed: %w", offset, parseErr)
	}
	return eff, nil
}

// parseWireEffect parses an effect from wire format bytes:
// [4-byte LE keyLen][key][protoData]
func parseWireEffect(wireData []byte) (*pb.Effect, error) {
	if len(wireData) <= 4 {
		return nil, fmt.Errorf("wire data too short: %d bytes", len(wireData))
	}
	keyLen := binary.LittleEndian.Uint32(wireData[:4])
	var protoData []byte
	if keyLen > 0 && uint32(len(wireData)) >= 4+keyLen {
		protoData = wireData[4+keyLen:]
	} else {
		protoData = wireData
	}
	eff := &pb.Effect{}
	if err := UnmarshalEffect(protoData, eff); err != nil {
		return nil, err
	}
	return eff, nil
}
