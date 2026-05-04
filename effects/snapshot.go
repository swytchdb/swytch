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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/swytchdb/cache/cluster/proto"
	"google.golang.org/protobuf/proto"
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
	// Metadata-only snapshot (subscription/serialization effects only, no actual data)
	// should be treated as non-existent from the data perspective.
	if result.Scalar == nil && len(result.NetAdds) == 0 && len(result.OrderedElements) == 0 &&
		result.Op == pb.EffectOp_UNKNOWN_OP {
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
		slog.Debug("GetSnapshot: reconstruction failed", "key", key, "error", err)
		return nil, nil, 0, err
	}

	// Sync serialization state from reconstruction result before filtering
	// strips metadata-only snapshots. This populates the in-memory map so
	// the handler can do fast lookups without a full snapshot.
	e.updateSerializationState(key, result)

	// Apply expiry + empty-collection filtering
	result = filterSnapshot(result)

	// Cache the result
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
	data, err := proto.Marshal(eff)
	if err != nil {
		return err
	}
	offset := e.nextOffset()

	if e.broadcaster == nil {
		return nil
	}

	notify := buildOffsetNotify(e.nodeID, offset, eff, data, nil)

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

	// Update local index with remote tips — add alongside existing, don't consume
	for _, tipOff := range unique {
		e.updateIndex(key, nil, tipOff)
	}

	// Verify the full causal chain is reachable. collectReachableNodes
	// calls getEffect for each node (which fetches from peers). If any
	// effect is unreachable, visited > collected — the DAG is incomplete
	// and reads would return wrong results.
	cr, _ := e.collectReachableNodes(key, unique)
	if len(cr.visited) > len(cr.nodes) {
		slog.Debug("ensureSubscribed: incomplete bootstrap, retrying in background",
			"key", key, "visited", len(cr.visited), "collected", len(cr.nodes))
		state.incomplete.Store(true)
		bootstrapComplete = false
		go e.retryBootstrap(key, state, unique)
		return ErrBootstrapIncomplete
	}
	return nil
}

// retryBootstrap periodically re-checks whether the full causal chain for a
// key is reachable. Uses the original NACK'd tips from the initial bootstrap
// (what peers reported as their state), not the current index tips — the
// index may only contain the node's own subscription effects.
func (e *Engine) retryBootstrap(key string, state *subscriptionState, nackTips []Tip) {
	for !e.closed.Load() {
		time.Sleep(500 * time.Millisecond)

		cr, err := e.collectReachableNodes(key, nackTips)
		if err != nil || len(cr.visited) > len(cr.nodes) {
			slog.Debug("retryBootstrap: still incomplete",
				"key", key, "visited", len(cr.visited), "collected", len(cr.nodes))
			continue
		}

		slog.Debug("retryBootstrap: complete", "key", key)
		state.incomplete.Store(false)
		close(state.ready)
		return
	}
}

// reconstruct rebuilds the materialized state for a key from its effect DAG.
// Uses a DAG-based algorithm ported from the types package:
//  1. Collect all reachable effects (expanding snapshots to their deps)
//  2. Build DAG, resolve via resolveFull (recursive fork decomposition)
//
// Returns the reduced result and the number of effects walked.
func (e *Engine) reconstruct(key string, tips []Tip, currentTxID ...string) (*pb.ReducedEffect, int, error) {
	cr, err := e.collectReachableNodes(key, tips)
	if err != nil {
		return nil, 0, err
	}
	if len(cr.nodes) == 0 {
		return nil, 0, nil
	}

	// Filter tentative effects: transactional effects are only visible
	// when their Bind is confirmed (present in the DAG). Per whitepaper
	// §4.1: "Write effects are tentative until the Bind is confirmed.
	// Other nodes skip them when reconstructing current state."
	var txID string
	if len(currentTxID) > 0 {
		txID = currentTxID[0]
	}
	slog.Debug("reconstruct: start", "key", key, "tips", tips, "txn_id", txID)
	preFilterCount := len(cr.nodes)
	var isInvisible func(string) bool
	if e.horizon != nil {
		isInvisible = e.horizon.IsInvisible
	}
	isVoided := func(txnID string) bool {
		_, ok := e.voidedBinds.Load(txnID)
		return ok
	}
	nodes := filterTentativeEffectsWithEngine(e, cr.nodes, txID, key, isInvisible, isVoided)
	if len(nodes) == 0 {
		return nil, 0, nil
	}

	dag := BuildDAG(nodes)
	slog.Debug("DAG", "encoded", encodeDAG(dag), "key", key, "txn_id", txID)
	result := resolveFull(dag)

	// Prune stale tips: any index tip that was successfully read and
	// is an ancestor (not a real DAG tip) gets removed. This keeps the
	// tip set from growing unboundedly as HandleRemote adds offsets.
	//
	// CRITICAL — two preservation rules:
	//   1. Do NOT prune tips that were unreadable. They may be
	//      temporarily unavailable (e.g. bind effects not yet
	//      fetchable) and removing them loses committed transaction
	//      visibility.
	//   2. Only prune when the superseding DAG tips are themselves
	//      committed (present in the current index). Callers inside a
	//      transaction pass their own pending lastOffset as a recon
	//      tip; walking from it marks the committed index tip as an
	//      ancestor. If we prune that ancestor now, and the caller's
	//      transaction later aborts, the index loses a tip that was
	//      never actually superseded — leaving subsequent reads with
	//      no visibility into the table's schema or data. Pruning only
	//      against committed-tip supersessions keeps the index
	//      consistent regardless of whether any in-flight tx
	//      eventually commits or aborts.
	dagTips := dag.Tips()
	if len(dagTips) < len(tips) && len(nodes) > 0 {
		currentIndex := e.index.Contains(key)
		indexSet := map[Tip]bool{}
		if currentIndex != nil {
			for _, t := range currentIndex.Tips() {
				indexSet[t] = true
			}
		}
		committedDagTips := false
		for _, t := range dagTips {
			if indexSet[t.Offset] {
				committedDagTips = true
				break
			}
		}
		if committedDagTips {
			realTips := make(map[Tip]bool, len(dagTips))
			for _, t := range dagTips {
				realTips[t.Offset] = true
			}
			collected := make(map[Tip]bool, len(cr.nodes))
			for _, n := range cr.nodes {
				collected[n.Offset] = true
			}
			var stale []Tip
			for _, t := range tips {
				if realTips[t] {
					continue // it's a real DAG tip, keep it
				}
				if collected[t] {
					stale = append(stale, t)
				}
			}
			if len(stale) > 0 {
				slog.Debug("reconstruct: pruning stale tips", "key", key, "stale", stale)
				e.index.RemoveTips(key, stale)
			}
		}
	}

	slog.Debug("reconstruct: done", "key", key, "txn_id", txID,
		"pre_filter_count", preFilterCount, "post_filter_count", len(nodes),
		"result_nil", result == nil)
	return result, len(nodes), nil
}

// encodeDAG produces a compact string encoding of the DAG.
// Offsets are mapped to sequential IDs (0,1,2...) for readability.
// Format: "N<count>;T<tip_ids>;R<root_ids>;[id:kind:val:deps;...]"
// kind: D=data, S=snapshot, N=noop, M=meta, U=subscription, B=bind, X=serialization, ?=unknown
// val: int value for data/snapshot, 0 otherwise
// deps: comma-separated IDs (within DAG only)
func encodeDAG(dag *DAG) string {
	// Assign sequential IDs
	idMap := make(map[Tip]int)
	id := 0
	// Sort by offset for deterministic output
	offsets := make([]Tip, 0, len(dag.byOffset))
	for off := range dag.byOffset {
		offsets = append(offsets, off)
	}
	//slices.Sort(offsets)
	for _, off := range offsets {
		idMap[off] = id
		id++
	}

	var b strings.Builder
	fmt.Fprintf(&b, "N%d;", len(dag.byOffset))

	// Tips
	b.WriteString("T")
	for i, t := range dag.Tips() {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%d", idMap[t.Offset])
	}
	b.WriteByte(';')

	// Roots
	b.WriteString("R")
	for i, r := range dag.Roots() {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%d", idMap[r.Offset])
	}
	b.WriteByte(';')

	// Nodes
	for _, off := range offsets {
		n := dag.byOffset[off]
		nid := idMap[off]

		kind := byte('?')
		val := int64(0)
		switch {
		case n.Effect.GetData() != nil:
			kind = 'D'
			val = n.Effect.GetData().GetIntVal()
		case n.Effect.GetSnapshot() != nil:
			kind = 'S'
			if s := n.Effect.GetSnapshot().State; s != nil && s.Scalar != nil {
				val = s.Scalar.GetIntVal()
			}
		case n.Effect.GetNoop() != nil:
			kind = 'N'
		case n.Effect.GetMeta() != nil:
			kind = 'M'
		case n.Effect.GetSubscription() != nil:
			kind = 'U'
		case n.Effect.GetTxnBind() != nil:
			kind = 'B'
		case n.Effect.GetSerialization() != nil:
			kind = 'X'
		}

		fmt.Fprintf(&b, "%d:%c:%d:", nid, kind, val)
		first := true
		for _, dep := range n.Effect.Deps {
			if did, ok := idMap[r(dep)]; ok {
				if !first {
					b.WriteByte(',')
				}
				fmt.Fprintf(&b, "%d", did)
				first = false
			}
		}
		b.WriteByte(';')
	}

	return b.String()
}

// resolveFull resolves an arbitrary DAG into a single ReducedEffect.
//
// Algorithm:
//   - Single Tip, no forks: linear reduction via topological sort + ReduceBranch
//   - Single Tip with forks: peel merge tips iteratively (in-place, O(N) total),
//     then resolve the exposed fork structure and chain the peeled tips back on.
//   - Multiple tips, no common ancestor: partition into connected components,
//     resolve each, merge results.
//   - Multiple tips with common ancestor (fork):
//     1. Compute fork state = resolveFull(DAG restricted to fork's ancestors)
//     2. Compute branch delta = resolveFull(DAG restricted to non-ancestors)
//     3. Result = composeSequential(fork state, branch delta)
//
// The DAG is modified in place (tips are peeled destructively). Callers must
// not reuse the DAG after this call.
func resolveFull(dag *DAG) *pb.ReducedEffect {
	return resolveFullInner(dag, true)
}

func resolveFullInner(dag *DAG, useSnapshotSeed bool) *pb.ReducedEffect {
	// Phase 1: iteratively peel single-tip merge nodes in place.
	// This replaces the old recursive peel-one-rebuild-everything loop
	// which was O(N²) in allocations.
	var peeledEffects []*pb.Effect

	for {
		tips := dag.Tips()
		if len(tips) != 1 {
			break // multi-tip: handle with fork decomposition below
		}

		if !dag.hasFork() {
			// Linear chain — reduce directly, then chain peeled tips on top.
			// If the root is a snapshot and we're at top level, use its
			// State as seed. Inside fork decomposition, snapshots on
			// individual branches would double-count the shared prefix.
			sorted := dag.TopologicalSort()
			effects := sortedNodesToEffects(sorted)
			var seed *pb.ReducedEffect
			if useSnapshotSeed && len(effects) > 0 {
				if snap := effects[0].GetSnapshot(); snap != nil && snap.State != nil {
					seed = snap.State
					effects = effects[1:]
				}
			}
			result := ReduceChain(seed, effects)
			if len(peeledEffects) > 0 {
				result = ReduceChain(result, peeledEffects)
			}
			slog.Debug("resolveFull: linear",
				"nodes", len(dag.byOffset),
				"peeled", len(peeledEffects))
			return result
		}

		// Single tip with forks: peel this merge tip in place.
		// Prepend so peeledEffects stays in causal (inner-to-outer) order.
		tip := tips[0]
		slog.Debug("resolveFull: peeling merge tip",
			"tip", tip.Offset, "nodes", len(dag.byOffset))
		peeledEffects = append([]*pb.Effect{tip.Effect}, peeledEffects...)
		dag.removeTip(tip)

		if len(dag.byOffset) == 0 {
			return ReduceBranch(peeledEffects)
		}
	}

	// Phase 2: resolve multi-tip DAG (fork decomposition)
	tips := dag.Tips()
	if len(tips) == 0 {
		if len(peeledEffects) > 0 {
			return ReduceBranch(peeledEffects)
		}
		return nil
	}

	result := resolveMultiTip(dag)

	// Chain peeled tips back on
	if len(peeledEffects) > 0 {
		result = ReduceChain(result, peeledEffects)
	}
	return result
}

// resolveMultiTip handles DAGs with multiple tips via fork decomposition.
// Uses Restrict for sub-DAG views instead of copying nodes into new DAGs.
func resolveMultiTip(dag *DAG) *pb.ReducedEffect {
	tips := dag.Tips()
	if len(tips) == 0 {
		return nil
	}
	if len(tips) == 1 {
		// Recursed into a single-tip sub-DAG — resolve normally
		return resolveFullInner(dag, false)
	}

	fork := dag.FindLCA(tips)
	if fork == nil {
		// No global common ancestor. Partition into connected components.
		components := dag.ConnectedComponents()
		if len(components) <= 1 {
			sorted := dag.TopologicalSort()
			return ReduceBranch(sortedNodesToEffects(sorted))
		}
		var componentResults []*pb.ReducedEffect
		for _, comp := range components {
			compResult := resolveFullInner(BuildDAG(comp), false)
			if compResult != nil {
				componentResults = append(componentResults, compResult)
			}
		}
		return MergeN(componentResults)
	}

	// Fork state: restrict to ancestors of fork (including fork itself)
	forkAncestors := make(map[Tip]bool)
	dag.collectAncestors(fork, forkAncestors)

	slog.Debug("resolveFull: fork split",
		"fork", fork.Offset,
		"fork_count", len(forkAncestors),
		"total", len(dag.byOffset))
	forkState := resolveFullInner(dag.Restrict(forkAncestors), false)

	// Branch delta: restrict to nodes NOT in fork's ancestors
	branchOffsets := make(map[Tip]bool, len(dag.byOffset)-len(forkAncestors))
	for offset := range dag.byOffset {
		if !forkAncestors[offset] {
			branchOffsets[offset] = true
		}
	}
	if len(branchOffsets) == 0 {
		return forkState
	}
	slog.Debug("resolveFull: branch",
		"branch_count", len(branchOffsets))
	branchDelta := resolveFullInner(dag.Restrict(branchOffsets), false)

	forkVal := int64(0)
	if forkState != nil && forkState.Scalar != nil {
		forkVal = forkState.Scalar.GetIntVal()
	}
	branchVal := int64(0)
	if branchDelta != nil && branchDelta.Scalar != nil {
		branchVal = branchDelta.Scalar.GetIntVal()
	}
	result := composeSequential(forkState, branchDelta)
	resultVal := int64(0)
	if result != nil && result.Scalar != nil {
		resultVal = result.Scalar.GetIntVal()
	}
	slog.Debug("resolveFull: compose",
		"fork_val", forkVal,
		"branch_val", branchVal,
		"result", resultVal)

	return result
}

// filterTentativeEffects removes transactional effects whose Bind either
// doesn't exist or lost fork-choice against a competing Bind.
// Per whitepaper §4.1: tentative effects are skipped during reconstruction.
// Per §4.2-4.3: competing Binds (overlapping keys, shared causal base)
// are resolved by fork-choice — lowest hash wins, losers' effects are
// tentative and invisible.
//
// The engine pointer is used for phase-2 predicate refinement
// (hasPredicateConflict). May be nil in test harnesses that want to
// exercise the pre-refinement shared-base behaviour.
func filterTentativeEffectsWithEngine(e *Engine, nodes []*DAGNode, currentTxID string, key string, isInvisible func(string) bool, isVoided ...func(string) bool) []*DAGNode {
	inputCount := len(nodes)

	// Phase 1: collect all binds and build a consumed-tips set per bind per key
	// Skip binds that are invisible (horizon wait) or voided (lost fork-choice).
	var voidedFn func(string) bool
	if len(isVoided) > 0 {
		voidedFn = isVoided[0]
	}
	var binds []forkChoiceBindEntry
	for _, n := range nodes {
		if b := n.Effect.GetTxnBind(); b != nil {
			if isInvisible != nil && isInvisible(n.Effect.TxnId) {
				continue // bind still in horizon wait
			}
			if voidedFn != nil && voidedFn(n.Effect.TxnId) {
				continue // bind lost fork-choice or SSI-invalidated
			}
			keys := make(map[string][]Tip, len(b.Keys))
			tips := make(map[string]Tip, len(b.Keys))
			for _, kb := range b.Keys {
				keys[string(kb.Key)] = fromPbRefs(kb.ConsumedTips)
				tips[string(kb.Key)] = r(kb.NewTip)
			}
			binds = append(binds, forkChoiceBindEntry{
				offset: n.Offset,
				hash:   n.Effect.ForkChoiceHash,
				txnID:  n.Effect.TxnId,
				keys:   keys,
				tips:   tips,
			})
		}
	}
	slog.Debug("filterTentative: phase1 bind collection", "key", key,
		"bind_count", len(binds), "total_nodes", inputCount)

	// TEMP: dump the pre-filter DAG for the no-binds case so we can
	// see whether the bind exists in the reachable walk but is being
	// filtered, or genuinely isn't reachable from the current tips.
	if len(nodes) > 0 && len(binds) == 0 {
		preDAG := BuildDAG(nodes)
		slog.Debug("filterTentative: pre-filter DAG (no-binds)",
			"key", key, "encoded", encodeDAG(preDAG))
	}

	// Phase 2: resolve fork-choice between competing binds.
	// Two binds conflict if they share a consumed tip on any
	// overlapping key AND their obs/row-write evidence on that key
	// actually intersects (predicate refinement). Same rule as
	// evaluateBindForkChoice / isRealConflict — every path must
	// reach the same deterministic verdict, so the snapshot view on
	// read matches the fork-choice decision at commit.
	// Among competitors, lowest hash wins — same tiebreak.
	lost := make(map[Tip]bool)
	for i := range binds {
		if lost[binds[i].offset] {
			continue
		}
		for j := i + 1; j < len(binds); j++ {
			if lost[binds[j].offset] {
				continue
			}
			if !bindsShareBase(&binds[i], &binds[j]) {
				continue
			}
			if e != nil {
				sharedKey, sharedExists := sharedBaseKey(&binds[i], &binds[j])
				if sharedExists {
					conflict, bothHadEvidence := e.hasPredicateConflict(
						binds[i].txnID, binds[j].txnID, sharedKey,
						[]Tip{binds[i].tips[sharedKey], binds[i].offset},
						[]Tip{binds[j].tips[sharedKey], binds[j].offset})
					if bothHadEvidence && !conflict {
						continue
					}
				}
			}
			if ForkChoiceLess(binds[i].hash, binds[j].hash) {
				lost[binds[j].offset] = true
			} else {
				lost[binds[i].offset] = true
			}
		}
	}
	if len(lost) > 0 {
		lostOffsets := make([]Tip, 0, len(lost))
		for off := range lost {
			lostOffsets = append(lostOffsets, off)
		}
		slog.Debug("filterTentative: phase2 fork-choice losers", "key", key,
			"lost_bind_offsets", lostOffsets)
	}

	// Build offset index early — used by Phase 2.5 and Phase 3.
	byOffset := make(map[Tip]*DAGNode, len(nodes))
	for _, n := range nodes {
		byOffset[n.Offset] = n
	}

	var zero Tip

	// Phase 2.5: SSI validation — a winning bind is invalidated if a
	// concurrent non-transactional data effect exists on this key.
	// "Concurrent" = not an ancestor of the bind's consumed tips AND
	// not a descendant of the bind (doesn't depend on it).
	for i := range binds {
		if lost[binds[i].offset] {
			continue
		}
		consumedTips, ok := binds[i].keys[key]
		if !ok {
			continue
		}

		// Ancestor set of consumed tips: everything the transaction saw.
		ancestors := make(map[Tip]bool)
		stack := make([]Tip, len(consumedTips))
		copy(stack, consumedTips)
		for len(stack) > 0 {
			off := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if ancestors[off] || off == zero {
				continue
			}
			ancestors[off] = true
			if n := byOffset[off]; n != nil {
				stack = append(stack, fromPbRefs(n.Effect.Deps)...)
			}
		}

		bindOffset := binds[i].offset
		bindNewTip := binds[i].tips[key]

		for _, n := range nodes {
			if n.Effect.TxnId != "" {
				continue // transactional — handled by bind fork-choice
			}
			if n.Effect.GetData() == nil {
				continue // not a data effect (subscription, meta, noop)
			}
			if ancestors[n.Offset] {
				continue // transaction saw this effect
			}
			// Check if this effect is a descendant of the bind
			// (transitively depends on the bind or its NewTip).
			if dependsOn(byOffset, n.Offset, bindOffset, bindNewTip) {
				continue // sequential, written after bind — not concurrent
			}
			// Concurrent non-transactional data effect → SSI conflict
			slog.Debug("filterTentative: phase2.5 SSI invalidation",
				"key", key,
				"bind_offset", bindOffset,
				"concurrent_offset", n.Offset)
			lost[binds[i].offset] = true
			break
		}
	}

	// Phase 3: confirm effects from winning binds only.
	// Walk backward from each bind's NewTips, confirming only effects
	// that belong to the SAME transaction (same TxnId). Cross-transaction
	// deps must NOT be confirmed — the other transaction's bind may not
	// exist yet (its effects are still tentative).
	confirmed := make(map[Tip]bool)
	for _, bi := range binds {
		if lost[bi.offset] {
			continue
		}
		bindNode := byOffset[bi.offset]
		if bindNode == nil {
			continue
		}
		bindTxnId := bindNode.Effect.TxnId

		confirmed[bi.offset] = true
		for _, tip := range bi.tips {
			confirmed[tip] = true
		}

		// Walk deps from confirmed tips, but only confirm effects
		// from the same transaction as this bind.
		walkStack := make([]Tip, 0, len(bi.tips)+1)
		walkStack = append(walkStack, bi.offset)
		for _, tip := range bi.tips {
			walkStack = append(walkStack, tip)
		}
		for len(walkStack) > 0 {
			off := walkStack[len(walkStack)-1]
			walkStack = walkStack[:len(walkStack)-1]
			n := byOffset[off]
			if n == nil || n.Effect.TxnId == "" {
				continue
			}
			for _, dep := range n.Effect.Deps {
				if !confirmed[r(dep)] {
					if dn := byOffset[r(dep)]; dn != nil && dn.Effect.TxnId == bindTxnId {
						confirmed[r(dep)] = true
						walkStack = append(walkStack, r(dep))
					}
				}
			}
		}
	}
	slog.Debug("filterTentative: phase3 chain confirmation", "key", key,
		"confirmed_count", len(confirmed))

	// Phase 4: filter
	filtered := make([]*DAGNode, 0, len(nodes))
	for _, n := range nodes {
		if n.Effect.TxnId == "" || confirmed[n.Offset] || n.Effect.TxnId == currentTxID {
			filtered = append(filtered, n)
		}
	}
	slog.Debug("filterTentative: phase4 filter", "key", key,
		"input_nodes", inputCount, "output_nodes", len(filtered),
		"filtered_out", inputCount-len(filtered))
	return filtered
}

// forkChoiceBindEntry holds bind metadata for fork-choice resolution.
type forkChoiceBindEntry struct {
	offset Tip
	hash   []byte
	txnID  string           // the bind's TxnId, for predicate refinement
	keys   map[string][]Tip // key → ConsumedTips
	tips   map[string]Tip   // key → NewTip
}

// filterTentativeEffects is the engine-less compatibility wrapper
// for tests and callers that don't need predicate refinement. Phase
// 2 falls back to pre-refinement shared-base + tiebreak behaviour.
func filterTentativeEffects(nodes []*DAGNode, currentTxID string, key string, isInvisible func(string) bool, isVoided ...func(string) bool) []*DAGNode {
	return filterTentativeEffectsWithEngine(nil, nodes, currentTxID, key, isInvisible, isVoided...)
}

// sharedBaseKey returns the first key on which a and b share a
// ConsumedTip, along with a presence flag. Matches bindsShareBase's
// scan order.
func sharedBaseKey(a, b *forkChoiceBindEntry) (string, bool) {
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
				return key, true
			}
		}
	}
	return "", false
}

// dependsOn returns true if the node at `from` transitively depends on any
// of the target offsets. Used to check if a non-transactional effect is a
// descendant of a bind (sequential, not concurrent).
func dependsOn(byOffset map[Tip]*DAGNode, from Tip, targets ...Tip) bool {
	targetSet := make(map[Tip]bool, len(targets))
	var zero Tip
	for _, t := range targets {
		if t != zero {
			targetSet[t] = true
		}
	}
	visited := make(map[Tip]bool)
	stack := []Tip{from}
	for len(stack) > 0 {
		off := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if targetSet[off] {
			return true
		}
		if visited[off] || off == zero {
			continue
		}
		visited[off] = true
		if n := byOffset[off]; n != nil {
			stack = append(stack, fromPbRefs(n.Effect.Deps)...)
		}
	}
	return false
}

// bindsShareBase returns true if two binds have overlapping keys with at
// least one shared consumed Tip (§4.2: same causal base on overlapping key).
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

// sortedNodesToEffects extracts pb.Effect pointers from a topologically sorted slice.
func sortedNodesToEffects(sorted []AnnotatedNode) []*pb.Effect {
	effects := make([]*pb.Effect, len(sorted))
	for i, an := range sorted {
		effects[i] = an.Node.Effect
	}
	return effects
}

// collectReachableNodes walks backward from all tips, collecting every
// reachable effect as a DAGNode for the given key. For linear chains
// (single Tip), the walk stops at the first snapshot with valid State.
// For forked DAGs (multiple tips), snapshots are walked through since
// branch-local snapshots would cause double-counting during merge.
//
// For bind effects, only follows the KeyBind.NewTip for the specified key
// to avoid pulling in effects from unrelated keys.
//
// collectResult holds the output of collectReachableNodes.
type collectResult struct {
	nodes   []*DAGNode
	visited map[Tip]bool // all offsets visited (including unreadable ones)
}

func (e *Engine) collectReachableNodes(key string, tips []Tip) (*collectResult, error) {
	// Subscription effects are isolated nodes (no deps, no descendants)
	// that don't participate in the data chain. Exclude them when
	// determining linearity so the snapshot stop optimization fires
	// for keys with many subscribers but a single data chain.
	dataTips := 0
	for _, tip := range tips {
		if eff, err := e.getEffect(tip); err == nil && eff.GetSubscription() == nil {
			dataTips++
		}
	}
	linear := dataTips <= 1
	visited := make(map[Tip]bool)
	var nodes []*DAGNode
	stack := make([]Tip, len(tips))
	copy(stack, tips)
	var zero Tip

	for len(stack) > 0 {
		offset := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if offset == zero || visited[offset] {
			continue
		}
		visited[offset] = true

		eff, err := e.getEffect(offset)
		if err != nil {
			slog.Debug("collectReachableNodes: skipping unreadable effect",
				"offset", offset, "error", err)
			continue
		}

		nodes = append(nodes, &DAGNode{Offset: offset, Effect: eff})

		// Linear chain: stop at first valid snapshot — its State
		// already materializes everything behind it.
		if linear {
			if snap := eff.GetSnapshot(); snap != nil && snap.State != nil {
				continue
			}
		}

		// Bind effects carry deps to all keys' last effects for causal
		// ordering, but we must NOT follow those cross-key deps during
		// per-key reconstruction. Instead, only follow the KeyBind.NewTip
		// for the key being reconstructed.
		if bind := eff.GetTxnBind(); bind != nil {
			for _, kb := range bind.Keys {
				if string(kb.Key) == key {
					stack = append(stack, r(kb.NewTip))
				}
			}
		} else {
			stack = append(stack, fromPbRefs(eff.Deps)...)
		}
	}

	slog.Debug("collectReachableNodes: done", "key", key, "tips", tips,
		"visited", len(visited), "nodes_collected", len(nodes))
	return &collectResult{nodes: nodes, visited: visited}, nil
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
	if err := proto.Unmarshal(protoData, eff); err != nil {
		return nil, err
	}
	return eff, nil
}
