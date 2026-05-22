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
	"encoding/binary"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/keytrie"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// verdictEntry holds a snapshot's verdict for a txnID; snapshotTip is the
// offset of the snapshot that supplied it, used for debug log provenance.
type verdictEntry struct {
	verdict     pb.Verdict
	snapshotTip Tip
}

// verdictMap accumulates snapshot-supplied verdicts during a walk so binds
// already adjudicated can skip pairwise fork choice.
type verdictMap map[string]verdictEntry

// harvest folds a snapshot's TxnVerdicts into the map. First-seen wins;
// disagreement is logged but not arbitrated (HLC is author-supplied and
// not a trustworthy tiebreaker).
func (v verdictMap) harvest(snapshotTip Tip, snap *pb.SnapshotEffect) {
	if snap == nil {
		return
	}
	for txnID, newVerdict := range snap.TxnVerdicts {
		if newVerdict == pb.Verdict_VERDICT_UNSPECIFIED {
			continue
		}
		existing, ok := v[txnID]
		if !ok {
			v[txnID] = verdictEntry{verdict: newVerdict, snapshotTip: snapshotTip}
			continue
		}
		if existing.verdict == newVerdict {
			continue
		}
		slog.Warn("snapshot verdict disagreement",
			"txn_id", txnID,
			"existing_verdict", existing.verdict,
			"existing_snapshot", existing.snapshotTip,
			"new_verdict", newVerdict,
			"new_snapshot", snapshotTip)
	}
}

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
	// reconstruct may return a nil result with a non-zero chainLen when the
	// walk visits a verdict-only snapshot at the LCA, an all-lost bind set,
	// or a chain of NoopEffects. The caller's compaction branch keys on
	// chainLen and dereferences result; zero chainLen here so it doesn't.
	if result == nil {
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
				// A non-empty NACK list means the peer was authoritative on
				// this key — i.e., the peer is subscribed. Record it so
				// flushTx can address bind broadcast directly without
				// re-deriving from the DAG.
				if len(nacks) > 0 {
					e.addPeerSubscriber(key, pid)
				}
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
				if len(nacks) > 0 {
					e.addPeerSubscriber(key, pid)
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

	// Bind discovery + verdict harvest only need to walk back to the
	// caller's pre-tx view. For a tx-context reconstruct the engine's
	// per-tx snapshot supplies per-key cutoff tips; outside a tx, no
	// cutoff (walk is bounded only by snapshot LCA in dag.walk).
	cutoff := e.txCutoff(txID)

	expandKeys, bindsByKey, snapshotVerdicts, err := e.bindKeyClosure(key, cutoff)
	if err != nil {
		return nil, 0, err
	}
	atomicallyLost := make(map[string]struct{})
	for txnID, entry := range snapshotVerdicts {
		if entry.verdict == pb.Verdict_LOST {
			atomicallyLost[txnID] = struct{}{}
		}
	}
	crossKeyWalked := 0
	for k := range expandKeys {
		losers, walked := e.losersOnKey(k, bindsByKey[k], snapshotVerdicts, cutoff)
		if k != key {
			crossKeyWalked += walked
		}
		for txnID := range losers {
			atomicallyLost[txnID] = struct{}{}
		}
	}
	if slog.Default().Enabled(context.Background(), slog.LevelDebug) &&
		(len(expandKeys) > 1 || len(atomicallyLost) > 0 || len(snapshotVerdicts) > 0) {
		closure := make([]string, 0, len(expandKeys))
		for k := range expandKeys {
			closure = append(closure, k)
		}
		lost := make([]string, 0, len(atomicallyLost))
		for txn := range atomicallyLost {
			lost = append(lost, txn)
		}
		verdictSrcs := make([]string, 0, len(snapshotVerdicts))
		for txn, entry := range snapshotVerdicts {
			verdictSrcs = append(verdictSrcs, fmt.Sprintf("%s=%s@%v", txn, entry.verdict.String(), entry.snapshotTip))
		}
		slog.Debug("reconstruct: cross-key closure",
			"key", key,
			"closure", closure,
			"atomically_lost", lost,
			"snapshot_verdicts", verdictSrcs,
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
			if _, lost := atomicallyLost[eff.TxnId]; lost {
				if entry, fromSnap := snapshotVerdicts[eff.TxnId]; fromSnap && entry.verdict == pb.Verdict_LOST {
					slog.Debug("reconstruct: skip bind (snapshot verdict LOST)",
						"key", key, "txn", eff.TxnId,
						"snapshot", entry.snapshotTip)
				} else {
					slog.Debug("reconstruct: skip bind (atomically lost)", "key", key, "txn", eff.TxnId)
				}
				return nil
			}
			if entry, fromSnap := snapshotVerdicts[eff.TxnId]; fromSnap && entry.verdict == pb.Verdict_WON {
				slog.Debug("reconstruct: include bind (snapshot verdict WON)",
					"key", key, "txn", eff.TxnId,
					"snapshot", entry.snapshotTip)
			} else {
				slog.Debug("reconstruct: include bind", "key", key, "txn", eff.TxnId)
			}
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
func (e *Engine) bindKeyClosure(startKey string, txCutoff keytrie.KeyIndex) (map[string]struct{}, map[string][]Tip, verdictMap, error) {
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
	verdicts := make(verdictMap)
	queue := []string{startKey}
	for len(queue) > 0 {
		k := queue[len(queue)-1]
		queue = queue[:len(queue)-1]
		if err := e.ensureSubscribed(k); err != nil {
			return nil, nil, nil, err
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
			// Pre-tx cutoff: if this tip is in the caller's tx snapshot
			// view for this effect's key, anything below it is already
			// in our pre-tx state and can't compete. Don't descend.
			if txCutoff != nil {
				if ts := txCutoff.Contains(string(eff.Key)); ts != nil && ts.Contains(t) {
					continue
				}
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
			if snap := eff.GetSnapshot(); snap != nil {
				verdicts.harvest(t, snap)
				if snap.PrevSnapshot != nil {
					stack = append(stack, r(snap.PrevSnapshot))
				}
				for _, dep := range eff.Deps {
					stack = append(stack, r(dep))
				}
				continue
			}
			for _, dep := range eff.Deps {
				stack = append(stack, r(dep))
			}
		}
	}
	return keys, bindsByKey, verdicts, nil
}

// losersOnKey returns the txnIDs of binds on `key` that lost fork choice,
// plus the count of effects walked. Binds with a LOST verdict in any
// reachable snapshot (or in snapshotVerdicts from the caller) bypass the
// pairwise hash comparison; binds with a WON verdict skip the pass
// entirely. The unadjudicated tail still runs pairwise hash + predicate
// refinement. extraTips augments the starting set with NewTip[key] offsets
// the caller harvested on other closure keys.
func (e *Engine) losersOnKey(key string, extraTips []Tip, snapshotVerdicts verdictMap, txCutoff keytrie.KeyIndex) (map[string]struct{}, int) {
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

	var isInvisible func(string) bool
	if e.horizon != nil {
		isInvisible = e.horizon.IsInvisible
	}

	// Local verdict map, seeded from any verdicts the caller harvested via
	// bindKeyClosure on other closure keys. Extended as we encounter
	// snapshots during our own walk on this key.
	verdicts := make(verdictMap, len(snapshotVerdicts))
	maps.Copy(verdicts, snapshotVerdicts)

	type seenBind struct {
		txnID      string
		bindOffset Tip // bind anchor offset (the TransactionalBindEffect's own offset)
		newTip     Tip
		hash       []byte
		consumed   []Tip // ConsumedTips on this key
	}
	var binds []seenBind

	visited := make(map[Tip]bool)
	stack := append([]Tip(nil), startTips...)
	walked := 0
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
		if txCutoff != nil {
			if ts := txCutoff.Contains(string(eff.Key)); ts != nil && ts.Contains(t) {
				continue
			}
		}
		walked++

		if snap := eff.GetSnapshot(); snap != nil {
			verdicts.harvest(t, snap)
			if snap.PrevSnapshot != nil {
				stack = append(stack, r(snap.PrevSnapshot))
			}
			for _, dep := range eff.Deps {
				stack = append(stack, r(dep))
			}
			continue
		}

		if bind := eff.GetTxnBind(); bind != nil {
			if isInvisible == nil || !isInvisible(eff.TxnId) {
				var newTip Tip
				var consumed []Tip
				for _, kb := range bind.Keys {
					if string(kb.Key) == key {
						newTip = r(kb.NewTip)
						consumed = fromPbRefs(kb.ConsumedTips)
						break
					}
				}
				var zero Tip
				if newTip != zero {
					binds = append(binds, seenBind{
						txnID:      eff.TxnId,
						bindOffset: t,
						newTip:     newTip,
						hash:       eff.ForkChoiceHash,
						consumed:   consumed,
					})
				}
			}
			for _, kb := range bind.Keys {
				if string(kb.Key) == key {
					stack = append(stack, r(kb.NewTip))
					break
				}
			}
			continue
		}

		for _, dep := range eff.Deps {
			stack = append(stack, r(dep))
		}
	}

	type bindWithSource struct {
		seenBind
		verdictSrc Tip
	}
	var unadjudicated []seenBind
	verdictAdjudicated := make(map[string]bindWithSource)
	for _, b := range binds {
		if entry, ok := verdicts[b.txnID]; ok {
			if entry.verdict == pb.Verdict_LOST {
				losers[b.txnID] = struct{}{}
			}
			verdictAdjudicated[b.txnID] = bindWithSource{seenBind: b, verdictSrc: entry.snapshotTip}
			continue
		}
		unadjudicated = append(unadjudicated, b)
	}

	// Chain detection: if every unadjudicated bind's ConsumedTips on this
	// key contains another unadjudicated bind's anchor offset, the binds
	// form a total order via consumed-bind edges (each bind explicitly
	// built on its predecessor's committed bind). No two can be
	// DAG-concurrent on this key, so no losers — skip the O(N²) pairwise
	// and ancestor work entirely. Common in sequential single-writer
	// workloads (XADD, LPUSH, INCR), where this is the difference
	// between O(N²) and O(N).
	if len(unadjudicated) > 1 {
		bindOffsetToIdx := make(map[Tip]int, len(unadjudicated))
		for i := range unadjudicated {
			bindOffsetToIdx[unadjudicated[i].bindOffset] = i
		}
		rootBinds := 0
		chained := true
		for i := range unadjudicated {
			consumesSibling := false
			for _, ct := range unadjudicated[i].consumed {
				if _, ok := bindOffsetToIdx[ct]; ok {
					consumesSibling = true
					break
				}
			}
			if !consumesSibling {
				rootBinds++
				if rootBinds > 1 {
					chained = false
					break
				}
			}
		}
		if chained {
			return losers, walked
		}
	}

	// Ancestor closures computed lazily on first reach query. Cached so
	// each bind's parent DAG is walked at most once across all pairs.
	// Fetches via e.getEffect (CloxCache-backed) instead of holding a
	// separate nodes map, so the discovery walk doesn't pay for an
	// extra map slot per visited effect when chain-detect short-circuits.
	ancestorsOf := make(map[Tip]map[Tip]struct{})
	buildAncestors := func(start Tip) map[Tip]struct{} {
		if cached, ok := ancestorsOf[start]; ok {
			return cached
		}
		seen := make(map[Tip]struct{})
		seen[start] = struct{}{}
		stack := []Tip{start}
		for len(stack) > 0 {
			cur := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			eff, err := e.getEffect(cur)
			if err != nil {
				continue
			}
			for _, dep := range eff.Deps {
				dt := r(dep)
				if _, dup := seen[dt]; dup {
					continue
				}
				seen[dt] = struct{}{}
				stack = append(stack, dt)
			}
		}
		ancestorsOf[start] = seen
		return seen
	}
	reaches := func(from, target Tip) bool {
		if from == target {
			return true
		}
		_, found := buildAncestors(from)[target]
		return found
	}

	// Pairwise fork choice over the unadjudicated tail.
	for i := range unadjudicated {
		b := &unadjudicated[i]
		for j := range unadjudicated {
			if i == j {
				continue
			}
			other := &unadjudicated[j]
			if reaches(b.newTip, other.newTip) || reaches(other.newTip, b.newTip) {
				continue
			}
			if !ForkChoiceLess(other.hash, b.hash) {
				continue
			}
			if other.txnID != "" && b.txnID != "" {
				conflict, bothHadEvidence := e.hasPredicateConflict(
					other.txnID, b.txnID, key,
					[]Tip{other.newTip}, []Tip{b.newTip})
				if bothHadEvidence && !conflict {
					continue
				}
			}
			losers[b.txnID] = struct{}{}
			break
		}
	}

	if len(binds) > 0 && slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		seen := make([]string, 0, len(binds))
		for _, b := range binds {
			hashPrefix := ""
			if len(b.hash) >= 4 {
				hashPrefix = fmt.Sprintf("%x", b.hash[:4])
			}
			suffix := ""
			if adj, ok := verdictAdjudicated[b.txnID]; ok {
				v := verdicts[b.txnID].verdict
				suffix = fmt.Sprintf(" verdict=%s src=%v", v.String(), adj.verdictSrc)
			}
			seen = append(seen, fmt.Sprintf("%s@%v#%s%s", b.txnID, b.newTip, hashPrefix, suffix))
		}
		loserList := make([]string, 0, len(losers))
		for txn := range losers {
			loserList = append(loserList, txn)
		}
		slog.Debug("losersOnKey: verdict",
			"key", key,
			"binds_seen", seen,
			"losers", loserList,
			"snapshot_adjudicated", len(verdictAdjudicated),
			"pairwise_candidates", len(unadjudicated))
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
