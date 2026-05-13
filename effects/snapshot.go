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
		// Bootstrap still running with unreachable effects — return error
		// immediately so the client retries rather than blocking.
		if existing.incomplete.Load() {
			return ErrBootstrapIncomplete
		}
		// Already subscribed or bootstrapping — wait for completion
		<-existing.ready
		return nil
	}
	slog.Debug("ensureSubscribed: bootstrapping", "key", key)
	bootstrapComplete := true
	defer func() {
		if bootstrapComplete {
			close(state.ready)
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
		return err
	}
	offset := e.nextOffset()

	if e.broadcaster == nil {
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
	// this key must not proceed.
	if !e.broadcaster.InMajorityPartition() {
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

	// Per-tip validation: install a tip only if we can fully walk its
	// causal chain. Installing tips before validation was the root
	// cause of "ghost tip" pollution — a NACK from a peer would seed
	// our index with a tip whose bytes were unreachable, and we'd
	// then advertise that ghost tip in our own NACKs to future
	// joiners.
	//
	// Walks share work through effectCache: once a deep ancestor is
	// fetched, subsequent walks hit it locally.
	installed := 0
	var unreachable []Tip
	for _, tipOff := range unique {
		rd := newDag(e, key, "")
		if walkErr := rd.walk([]Tip{tipOff}, func(*pb.Effect) error { return nil }); walkErr != nil {
			slog.Debug("ensureSubscribed: skipping unreachable tip",
				"key", key, "tip", tipOff, "error", walkErr)
			unreachable = append(unreachable, tipOff)
			continue
		}
		e.updateIndex(key, nil, tipOff)
		installed++
	}

	if installed > 0 {
		// At least one tip from this bootstrap is fetchable — we have
		// authority for the reachable subset. Unreachable tips are
		// ghosts; leaving them out of the index is the desired
		// behavior (they'll never reach the cluster again via us).
		if len(unreachable) > 0 {
			slog.Debug("ensureSubscribed: bootstrap completed with partial reachability",
				"key", key, "installed", installed, "skipped", len(unreachable))
		}
		return nil
	}

	// No tips reachable, but peers did NACK with state. This is a
	// real cluster issue (full partition, every peer is in the same
	// ghost-tip trap, etc.). Mark incomplete and let retryBootstrap
	// keep probing in the background. The NACK filter in
	// buildEnrichedNack ensures we never propagate the broken state
	// outward even while incomplete.
	slog.Debug("ensureSubscribed: incomplete bootstrap, retrying in background",
		"key", key, "unreachable_tips", len(unreachable))
	state.incomplete.Store(true)
	bootstrapComplete = false
	go e.retryBootstrap(key, state, unique)
	return ErrBootstrapIncomplete
}

// retryBootstrap periodically re-checks whether any of the original
// NACK'd tips are now reachable. ensureSubscribed leaves the index
// empty on full-bootstrap-failure, so retry both validates and
// installs: per-tip walks identify which tips have become
// fetchable (via anti-entropy fill-in, peer recovery, etc.), and we
// install those before marking state ready.
//
// Uses the original NACK'd tips, not the current index tips — the
// index reflects only what we already accepted, while nackTips is
// what peers reported at bootstrap time.
func (e *Engine) retryBootstrap(key string, state *subscriptionState, nackTips []Tip) {
	for !e.closed.Load() {
		time.Sleep(500 * time.Millisecond)

		installed := 0
		for _, tipOff := range nackTips {
			rd := newDag(e, key, "")
			if walkErr := rd.walk([]Tip{tipOff}, func(*pb.Effect) error { return nil }); walkErr != nil {
				continue
			}
			e.updateIndex(key, nil, tipOff)
			installed++
		}

		if installed == 0 {
			slog.Debug("retryBootstrap: still incomplete",
				"key", key, "tips", len(nackTips))
			continue
		}

		slog.Debug("retryBootstrap: complete",
			"key", key, "installed", installed, "of", len(nackTips))
		state.incomplete.Store(false)
		close(state.ready)
		return
	}
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

	d := newDag(e, key, txID)

	var result *pb.ReducedEffect
	var subRootEffects []*pb.Effect
	count := 0

	type wonBind struct {
		txnID string
		tips  []Tip // start tips for predicate collection
	}
	wonTips := make(map[Tip]wonBind)

	var isInvisible func(string) bool
	if e.horizon != nil {
		isInvisible = e.horizon.IsInvisible
	}

	err := d.walk(tips, func(eff *pb.Effect) error {
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
				return nil
			}
			if _, voided := e.voidedBinds.Get(eff.TxnId, 0); voided {
				return nil
			}
			// Check if this bind competes with an already-won bind.
			// Topo order visits lower fork-choice hash first, so the
			// first bind for a given set of consumed tips is the winner —
			// unless predicate refinement shows non-overlapping predicates,
			// in which case both coexist (matching commit-time semantics).
			for _, kb := range bind.Keys {
				if string(kb.Key) == key {
					consumed := fromPbRefs(kb.ConsumedTips)
					newTip := r(kb.NewTip)
					for _, ct := range consumed {
						if prev, competing := wonTips[ct]; competing {
							if e != nil {
								conflict, bothHadEvidence := e.hasPredicateConflict(
									prev.txnID, eff.TxnId, key,
									prev.tips, []Tip{newTip})
								if bothHadEvidence && !conflict {
									continue
								}
							}
							return nil
						}
					}
					wb := wonBind{txnID: eff.TxnId, tips: []Tip{newTip}}
					for _, ct := range consumed {
						wonTips[ct] = wb
					}
					break
				}
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
		"count", count, "result_nil", result == nil)
	return result, count, nil
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

// collectBindEffects fetches the transactional effects for a winning bind
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
