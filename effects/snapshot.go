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
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// errHorizonRetryNeeded is an internal sentinel returned by reconstruct's
// iterate callback when it encountered a bind in HorizonSet and the caller
// requested waitForHorizon. The outer reconstruct loop catches it, the
// wait has already completed inline, and the walk restarts so losersOnKey,
// snapshotVerdicts, and dependsOnInvisible are recomputed against the new
// post-resolution state.
var errHorizonRetryNeeded = errors.New("reconstruct: horizon ancestor resolved, retry")

// leafState is the per-key payload attached to each critbit leaf (the trie's
// T). It carries the cached subdag for the key so reads reuse the prepared
// structure instead of rebuilding it. It is created once per key and mutated
// in place; later stages add eviction metadata (freq, last-access) alongside
// the subdag, which is why this is a struct rather than a bare subdag pointer.
type leafState struct {
	subdag atomic.Pointer[cachedDag]
	// reduced memoizes the fully-materialized read result for a specific tip
	// set, so a repeated client read of an unchanged key skips fork-choice
	// adjudication and ReduceChain entirely (only the subdag structure cache
	// existed before). reconstruct(key, tips) is a pure function of the tip set
	// — the reachable DAG is immutable — so a tip change yields a different key
	// and misses; see GetSnapshot for the store/serve invariant.
	reduced atomic.Pointer[reducedResult]

	// cloudConsulted marks that the Cloud read-miss backstop has received one
	// complete answer for this key during this leaf's residency. It turns a
	// nil reduction from "not known — ask Cloud" into "authoritatively
	// nothing": while the leaf lives we are subscribed and replication keeps
	// the local view current, so one consult per residency is enough. Eviction
	// reclaims the leaf and the marker with it, re-arming the rehydrate.
	cloudConsulted atomic.Bool

	// cloudPending marks that a Cloud consult for this key found a durable
	// hole (ErrCDNBlobMissing) it handed to the cluster's reconcile loop via
	// MarkPending, rather than a complete answer. It is mutually exclusive
	// with cloudConsulted for a given consult: unlike cloudConsulted it does
	// NOT make a nil reduction authoritative — Cloud provably holds ancestry
	// this node could not fetch, so GetSnapshot must keep returning
	// ErrCloudUnavailable instead of a free miss. What it does share with
	// cloudConsulted is suppressing repeat WAN GetTips round-trips for the
	// rest of this leaf's residency: the reconcile loop owns retrying the
	// hole on Cloud's own schedule, and re-polling here on every read of a
	// hot pending key would just pay the same round-trip for the same
	// not-yet-healed answer. Eviction reclaims the leaf and the marker with
	// it, re-arming the consult.
	cloudPending atomic.Bool

	// cloudMu single-flights the Cloud consult for this leaf: concurrent
	// missers of the same key queue here instead of each paying the WAN
	// GetTips round-trip, then re-check cloudConsulted/cloudPending and serve
	// the winner's install. A failed consult leaves both markers false, so
	// the next miss retries.
	cloudMu sync.Mutex

	// subscribers is the set of peer node IDs subscribed to this key, used by
	// flushTx to address bind broadcast without re-deriving it from the DAG
	// (reconstruct's Subscribers accumulator can return empty for legitimate
	// reasons — verdict-only snapshot at LCA, all-lost bind set, NoopEffect
	// chain). It lives on the leaf so it is reclaimed automatically when the key
	// is evicted; a departed peer's stale id is filtered against current
	// membership at broadcast time (collectSubscribers), so no eager scrub on
	// peer removal is needed. Guarded by subMu; lazily allocated.
	subMu       sync.Mutex
	subscribers map[pb.NodeID]struct{}
}

// reducedResult is a cached materialized read keyed on the exact tip set it was
// derived from. The result is immutable once published. Read paths must use
// copy-on-write before changing it (filterSnapshot and compaction do this).
//
// result aliases the value byte slices of the effects it was reduced from
// (cloneData/cloneReduced share Raw slices rather than copying). Those effects
// are in the active window and kept resident by dep-refcounting, so the memo
// needs no refcount of its own.
type reducedResult struct {
	tips     []Tip
	result   *pb.ReducedEffect
	chainLen int
}

// cachedDag is the reusable output of dag.prepare for a specific tip set: the
// fork-choice-sorted topological order and the LCA. It is immutable once
// published; a tip change produces a new one. Fork-choice adjudication
// (losersOnKey / bindKeyClosure) and ReduceChain still run fresh on every read
// against this structure — only the BFS+topo build is cached, so the answer
// remains derived from the DAG.
//
// It deliberately holds no *pb.Effect pointers: the vertex pool is the only
// owner of effect objects, and topoOrder doubles as the pin set — publishSubdag
// increfs every tip before the structure becomes reachable, so a cache hit can
// always re-materialize the node map from the pool. Holding pointers here
// instead used to let a subdag outlive its nodes' pool residency (an incref
// that raced reclaim silently no-op'd), accumulating gigabytes of heap-live
// effects invisible to vertex_count and unreachable by eviction.
type cachedDag struct {
	tips      []Tip
	topoOrder []Tip
	lcaTip    Tip
}

// evictedSubdag is the terminal sentinel releaseChainRefs installs on a
// leafState whose refs it has released. A dropped leaf can never accept
// another subdag: a racing read's publish would pin vertices on a leaf no
// release walk will ever visit again — an unevictable leak. publishSubdag
// refuses to displace it.
var evictedSubdag = &cachedDag{}

// tipsEqual reports whether two tip sets are equal as sets. Tip sets are
// small (usually one tip), so the quadratic membership check is cheap and
// avoids spurious cache misses from ordering differences.
func tipsEqual(a, b []Tip) bool {
	if len(a) != len(b) {
		return false
	}
	for _, x := range a {
		found := slices.Contains(b, x)
		if !found {
			return false
		}
	}
	return true
}

// prepareDag populates d with the DAG structure for (key, tips), reusing the
// leaf's cached subdag when it matches tips and rebuilding + publishing it
// otherwise. Falls back to a transient build when the key has no leaf (not in
// the index) — nothing to cache against.
//
// A cache hit re-materializes d.nodes from the vertex pool: the subdag's pin
// (one ref per topoOrder tip, taken in publishSubdag) guarantees residency, so
// a miss means the pin was released underneath us — a concurrent eviction of
// this key between our subdag.Load and the pool read. That read races the same
// window a fresh walk does, so fall through to the full build and let it
// resolve (or surface the eviction) the same way.
func (e *Engine) prepareDag(d *dag, key string, tips []Tip) error {
	if e.index == nil {
		return d.prepare(tips)
	}
	ls, _ := e.index.LoadOrStoreData(key, &leafState{})
	if ls == nil {
		return d.prepare(tips)
	}
	if cs := ls.subdag.Load(); cs != nil && cs != evictedSubdag && tipsEqual(cs.tips, tips) {
		if nodes, ok := e.materializeSubdag(cs); ok {
			d.nodes = nodes
			d.topoOrder = cs.topoOrder
			d.lcaTip = cs.lcaTip
			return nil
		}
	}
	if err := d.prepare(tips); err != nil {
		return err
	}
	e.publishSubdag(ls, d, tips)
	return nil
}

// materializeSubdag rebuilds the node map for a cached subdag from the vertex
// pool. Every tip is pool-resident by the pin invariant; ok=false means the
// pin was concurrently released (racing eviction) and the caller must rebuild.
func (e *Engine) materializeSubdag(cs *cachedDag) (map[Tip]*pb.Effect, bool) {
	if e.effectCache == nil {
		return nil, false
	}
	nodes := make(map[Tip]*pb.Effect, len(cs.topoOrder))
	for _, tip := range cs.topoOrder {
		eff, ok := e.effectCache.Get(tip)
		if !ok {
			return nil, false
		}
		nodes[tip] = eff
	}
	return nodes, true
}

// publishSubdag installs d's prepared structure as the leaf's cached subdag.
// The subdag's pin is what keeps the key's readable window resident: one
// pool reference per topoOrder tip, taken BEFORE the structure becomes
// reachable. Order matters — pin, publish, then release the displaced pin:
//
//   - An incref that fails (vertex missing or claimed by reclaim) means an
//     eviction of this key raced the build. Roll back and don't publish;
//     serving the transient d is still correct for this read, and the next
//     read rebuilds against the post-eviction index. Publishing anyway would
//     retain pool-dead effects off the pool's books — the exact off-books
//     residency the pin protocol exists to prevent.
//   - A losing CAS builder rolls back its own pin; the winner owns the leaf's
//     refs. The winner releases the displaced subdag's pin after the swap
//     (nodes present in both hold two refs for that instant, never zero).
//
// A tip that falls below a freshly-converged snapshot LCA simply isn't in the
// new topoOrder: releasing the old pin drops its read claim while its creation
// ref (held to eviction) keeps it resident for unconverged forks.
func (e *Engine) publishSubdag(ls *leafState, d *dag, tips []Tip) {
	if e.effectCache != nil {
		for i, tip := range d.topoOrder {
			if !e.effectCache.incref(tip) {
				for _, taken := range d.topoOrder[:i] {
					e.effectCache.decref(taken)
				}
				return
			}
		}
	}
	cs := &cachedDag{
		tips:      append([]Tip(nil), tips...),
		topoOrder: d.topoOrder,
		lcaTip:    d.lcaTip,
	}
	old := ls.subdag.Load()
	if old == evictedSubdag || !ls.subdag.CompareAndSwap(old, cs) {
		// Leaf evicted, or another builder won: roll back our pin. In the
		// eviction case releaseChainRefs already swept this leafState — a
		// publish here would pin vertices no release walk will ever visit.
		if e.effectCache != nil {
			for _, tip := range d.topoOrder {
				e.effectCache.decref(tip)
			}
		}
		return
	}
	if e.effectCache != nil && old != nil {
		for _, tip := range old.topoOrder {
			e.effectCache.decref(tip)
		}
	}
}

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

// isJSONType reports whether the type tag marks a RedisJSON node (key root or a
// nested object/array). JSON nodes are exempt from Redis empty-collection
// auto-delete — an empty {} or [] is a real value.
func isJSONType(t pb.ValueType) bool {
	switch t {
	case pb.ValueType_TYPE_JSON, pb.ValueType_TYPE_JSON_OBJECT, pb.ValueType_TYPE_JSON_ARRAY:
		return true
	}
	return false
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
	// JSON nodes manage their own emptiness: an empty object/array (or a
	// meta-only root) is a real value, not a Redis empty-collection auto-delete.
	// (Cf. the TYPE_STREAM exception for empty streams below.)
	if isJSONType(result.TypeTag) {
		return result
	}
	// Filter expired elements from KEYED collections
	if len(result.NetAdds) > 0 {
		now := time.Now()
		cloned := false
		for k, elem := range result.NetAdds {
			if elem.ExpiresAt != nil && now.After(elem.ExpiresAt.AsTime()) {
				if !cloned {
					// A reduced memo may own result. Clone only on the rare
					// expired-element path; ordinary reads remain allocation-free
					// here and never mutate the published snapshot.
					result = cloneReduced(result)
					cloned = true
				}
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
// The returned ReducedEffect is an immutable snapshot and may be shared by
// subsequent reads; callers that need to change it must clone it first.
//
// Cache hit returns immediately. On miss, walks the causal DAG from
// the index tip set and reconstructs via ReduceBranch + canonical merge.
func (e *Engine) GetSnapshot(key string) (*pb.ReducedEffect, []Tip, int, error) {
	// Free miss: nothing local (no tips, no subscription record) and no peer
	// filter admits the key — there is no state to bootstrap and no writer to
	// hear from, so announcing a subscription would be pure DAG noise (on
	// miss-heavy workloads the mint + self-install churn dominated memory).
	// The read is answered from the filters; a stale filter costs one
	// premature miss, the documented filter contract, and every read
	// re-evaluates. Authority is claimed exactly when it is exercised: a
	// write subscribes in Emit, and the Cloud backstop below subscribes when
	// a hydrate installs state.
	if !e.freeMissEligible(key) {
		if err := e.ensureSubscribed(key); err != nil {
			return nil, nil, 0, err
		}
	}

	result, tips, chainLen := e.reconstructLocal(key)

	// Tiered storage: a nil result can mean three things and only two warrant
	// a Cloud consult. "Not-known nil" — no local history, or the key was
	// evicted — is the rehydrate case: ask Cloud for the frontier and install
	// whatever portion of it is locally walkable (blobs pulled via
	// FetchFromAny → CDN), so reconstructing again returns the value and the
	// cluster owns the key thereafter. "Authoritative nil" — we hold the
	// chain and it reduces to nothing (DEL tombstone, emptied collection) —
	// must not re-consult: such keys stay filter-positive in Cloud forever,
	// and without the distinction every GET of them pays a WAN GetTips
	// round-trip. "Holed nil" — Cloud's frontier hit a durable
	// ErrCDNBlobMissing and nothing readable resolved locally — must not be
	// reported as a miss either, since Cloud provably holds ancestry this
	// node could not fetch; it returns ErrCloudUnavailable instead, and only
	// Cloud's own reconcile loop (via MarkPending) resolves it, on its own
	// schedule.
	//
	// A data-bearing tip marks the nil authoritative; subscription tips don't
	// count (a subscribed reader's own SubscriptionEffect sits in the index
	// as a tip), and a free-missed key has no tips at all — vacuously
	// subscription-only. The leaf's cloudConsulted and cloudPending markers
	// are mutually exclusive per consult and both cap further WAN round-trips
	// for the rest of this leaf's residency — one distinguishes "nil is
	// authoritative" from "nil is a retryable hole" — and eviction reclaims
	// both with the leaf, re-arming the consult. Gated on the majority
	// partition: a minority node can't trust its cluster view
	// (ensureSubscribed already errored out above for it). UnsafeMode has no
	// majority to trust — it rehydrates on whatever view it has.
	if result == nil && e.cloudReader != nil && e.subscriptionOnlyTips(tips) &&
		(e.inMajorityPartition() || e.modeForKey(key) == UnsafeMode) {
		ls, _ := e.index.LoadOrStoreData(key, &leafState{})
		if ls == nil || (!ls.cloudConsulted.Load() && !ls.cloudPending.Load()) {
			if ls != nil {
				ls.cloudMu.Lock()
				defer ls.cloudMu.Unlock()
			}
			if ls == nil || (!ls.cloudConsulted.Load() && !ls.cloudPending.Load()) {
				installed, consulted, hydrateErr := e.hydrateFromCloud(key)
				if hydrateErr != nil {
					// Cloud may hold this key and we couldn't get a complete
					// answer: the read fails (client retries) rather than
					// fabricating a miss for data that exists.
					return nil, nil, 0, hydrateErr
				}
				if installed {
					// The hydrate made this node a holder of the key: subscribe
					// (idempotent when the read already did) so peer writes route
					// to us and the returned tips include our SubscriptionEffect
					// for RMW deps.
					if err := e.ensureSubscribed(key); err != nil {
						return nil, nil, 0, err
					}
					result, tips, chainLen = e.reconstructLocal(key)
				}
				if ls == nil {
					// The hydrate may have just created the leaf.
					ls, _ = e.index.LoadOrStoreData(key, &leafState{})
				}
				if ls != nil {
					if consulted {
						ls.cloudConsulted.Store(true)
					} else {
						// hydrateFromCloud reports !consulted with a nil error
						// exactly when it found a durable hole and handed it
						// to MarkPending — never for a transport failure,
						// which surfaced as hydrateErr above.
						ls.cloudPending.Store(true)
					}
				}
			} else {
				// Lost the single-flight race: a concurrent reader already
				// resolved (or found pending) this leaf's Cloud consult while
				// we waited on cloudMu. Serve its result.
				result, tips, chainLen = e.reconstructLocal(key)
			}
		}
		if result == nil && ls != nil && ls.cloudPending.Load() && !ls.cloudConsulted.Load() {
			return nil, nil, 0, fmt.Errorf("%w: frontier for %q partially holed, reconcile pending", ErrCloudUnavailable, key)
		}
	}

	return result, tips, chainLen, nil
}

// awaitSubscription resolves an ensureSubscribed call that found an existing
// subscription record. UnsafeMode proceeds on whatever state is local (the
// background retry keeps healing unreachable tips); SafeMode waits for the
// bootstrap and re-checks after the wait, because ready may have been closed
// by a failure path (markFailed) rather than a successful bootstrap — the
// closeOnce + Store-before-close in markFailed ensures the re-load sees the
// up-to-date value.
func (e *Engine) awaitSubscription(existing *subscriptionState, unsafe bool) error {
	if existing.incomplete.Load() {
		if unsafe {
			return nil
		}
		return ErrBootstrapIncomplete
	}
	<-existing.ready
	if existing.incomplete.Load() {
		if unsafe {
			return nil
		}
		return ErrBootstrapIncomplete
	}
	return nil
}

// freeMissEligible reports whether a read of key can be answered "absent"
// without announcing a subscription: this node holds nothing (no subscription
// record, no index tips), no peer's key filter admits the key, and the
// cluster view is trustworthy (majority, or UnsafeMode where there is no
// majority to trust — a SafeMode minority falls through so ensureSubscribed
// surfaces ErrRegionPartitioned instead of fabricating a miss). No key is
// special-cased: whatever a key is for, holding nothing of it and hearing no
// claim on it means there is nothing to announce — a later write (or a peer's
// claim) puts it on the subscribe path like any other key.
func (e *Engine) freeMissEligible(key string) bool {
	if _, ok := e.subscriptions.Load(key); ok {
		return false // already subscribed; ensureSubscribed is a cheap no-op
	}
	if e.index.Contains(key) != nil {
		return false // we hold state (peer arrivals, backfill): formalize authority
	}
	if e.clusterMaybeHasKey(key) {
		return false // a peer may hold it: bootstrap required
	}
	if e.cloudReader != nil && e.cloudReader.MayHold(key) {
		// Cloud may hold it: the consult path below needs the subscription's
		// leaf to anchor the cloudConsulted marker that caps WAN round-trips.
		return false
	}
	return e.inMajorityPartition() || e.modeForKey(key) == UnsafeMode
}

// subscriptionOnlyTips reports whether tips carry no data history: every tip
// resolves to a SubscriptionEffect (vacuously true for an empty set). A tip
// whose effect is not cached is treated as data — reconstruction just walked
// these tips, so a cache miss here means the leaf is being torn down and the
// next read re-evaluates from scratch anyway.
func (e *Engine) subscriptionOnlyTips(tips []Tip) bool {
	for _, t := range tips {
		eff, ok := e.effectCache.Get(t)
		if !ok || eff.GetSubscription() == nil {
			return false
		}
	}
	return true
}

// reconstructLocal builds a key's materialized state from the tips already in
// the local index (installed by subscription bootstrap, peer arrivals, or a
// Cloud rehydrate). It never reaches outside the node for the frontier, so a nil
// result means the cluster — as this node currently sees it — holds no committed
// value. Split out of GetSnapshot so the Cloud read-miss backstop can retry it
// after installing Cloud's frontier.
func (e *Engine) reconstructLocal(key string) (*pb.ReducedEffect, []Tip, int) {
	// Read index tips once — these are the tips the returned snapshot
	// corresponds to.
	indexTips := e.index.Contains(key)
	var snapshotTips []Tip
	if indexTips != nil {
		snapshotTips = indexTips.Tips()
	}

	// Check index for tips
	if len(snapshotTips) == 0 {
		if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
			slog.Debug("GetSnapshot: no tips", "key", key)
		}
		return nil, nil, 0
	}

	// Visibility fence: exclude in-progress tx tips, substitute pre-tx deps
	tipOffsets := e.resolveTipDeps(snapshotTips)
	if len(tipOffsets) == 0 {
		return nil, nil, 0
	}

	// Reduced-result memo: a client read (waitForHorizon=true) always waits out
	// any in-horizon ancestor before completing, so a completed result is the
	// committed answer for these exact tips, and reconstruct(key, tips) is a
	// pure function of the tip set (the reachable DAG is immutable). Cache it in
	// the leaf keyed on the tip set — a later write/arrival changes the tips,
	// yielding a different key that misses. The cached result is immutable;
	// filtering and compaction use copy-on-write before changing it. The memo is
	// bypassed while any txn is mid-horizon: a conservative guard that keeps the
	// cache out of the in-flight-visibility window entirely.
	horizonClear := e.horizon == nil || e.horizon.Empty()
	ls := e.index.LoadData(key)
	if ls == nil {
		ls, _ = e.index.LoadOrStoreData(key, &leafState{})
	}

	var result *pb.ReducedEffect
	var chainLen int
	served := false
	if horizonClear && ls != nil {
		if rr := ls.reduced.Load(); rr != nil && tipsEqual(rr.tips, tipOffsets) {
			result, chainLen, served = rr.result, rr.chainLen, true
		}
	}

	if !served {
		// Walk DAG and reconstruct. waitForHorizon=true: this is a client-facing
		// read, so the walk must block on any in-ancestry HorizonSet bind to
		// avoid returning a value that excludes a bind which will be present
		// in the next reconstruct from the same tips (the Elle :incompatible-order
		// shape from Jepsen run 26373595271).
		if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
			slog.Debug("GetSnapshot: reconstructing", "key", key, "tips", tipOffsets)
		}
		var err error
		result, chainLen, err = e.reconstruct(key, tipOffsets, "", true)
		if err != nil {
			slog.Debug("GetSnapshot: reconstruction incomplete, returning empty", "key", key, "error", err)
			return nil, nil, 0
		}
		if horizonClear && ls != nil {
			// The reduced memo is a pure read cache. Its result aliases the value
			// bytes of effects in the active window, but those are kept resident by
			// dep-refcounting (the chain from the live tip down stays pinned), so
			// the memo needs no refcount of its own.
			ls.reduced.Store(&reducedResult{
				tips:     append([]Tip(nil), tipOffsets...),
				result:   result,
				chainLen: chainLen,
			})
		}
	}

	// Sync serialization state from reconstruction result before filtering
	// strips metadata-only snapshots. This populates the in-memory map so
	// the handler can do fast lookups without a full snapshot.
	e.updateSerializationState(key, result)

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
	if result != nil && !isJSONType(result.TypeTag) &&
		result.Scalar == nil && len(result.NetAdds) == 0 && len(result.OrderedElements) == 0 &&
		(result.Op == pb.EffectOp_UNKNOWN_OP || result.Op == pb.EffectOp_REMOVE_OP) {
		return nil, snapshotTips, 0
	}
	// reconstruct may return a nil result with a non-zero chainLen when the
	// walk visits a verdict-only snapshot at the LCA, an all-lost bind set,
	// or a chain of NoopEffects. The caller's compaction branch keys on
	// chainLen and dereferences result; zero chainLen here so it doesn't.
	if result == nil {
		return nil, snapshotTips, 0
	}
	return result, snapshotTips, chainLen
}

// ensureSubscribed records local subscription and emits a SubscriptionEffect.
// On first access, broadcasts to ALL nodes and waits for NACKs with Tip sets
// so we can fetch remote state before GetSnapshot proceeds — UNLESS no peer
// holds the key (authoritative in the majority partition), in which case it
// announces the subscription fire-and-forget and returns without blocking
// (there is no remote state to fetch). Used by reads and non-tx writes.
//
// Returns ErrRegionPartitioned if the node is in a minority partition and
// cannot announce the subscription cluster-wide. Per the whitepaper (§3.3),
// a node must be subscribed to a key before any read or write.
//
// In UnsafeMode neither error is possible: there is no majority to gate on.
// Unreachable peers are skipped without retry, the bootstrap proceeds with
// whatever peers respond, and the branches merge later via anti-entropy.
func (e *Engine) ensureSubscribed(key string) error {
	return e.ensureSubscribedMode(key, true)
}

// ensureSubscribedBlocking forces the synchronous bootstrap even when no peer
// currently holds the key. The transactional commit path uses this: its
// fork-choice base-sharing depends on the round-2 tip collection completing
// before the bind is emitted, so it must never take the async-announce path.
func (e *Engine) ensureSubscribedBlocking(key string) error {
	return e.ensureSubscribedMode(key, false)
}

func (e *Engine) ensureSubscribedMode(key string, allowAsync bool) error {
	unsafe := e.modeForKey(key) == UnsafeMode
	// Allocation-free fast path: Emit calls this on every effect, so the
	// already-subscribed case must not pay the state+channel allocation the
	// LoadOrStore below needs.
	if existing, ok := e.subscriptions.Load(key); ok {
		return e.awaitSubscription(existing, unsafe)
	}
	// Callers on the Redis hot path may borrow key from a pooled command
	// buffer. A new subscription outlives that command, so own the map/trie key
	// before publishing any state. The already-subscribed path above remains
	// allocation-free.
	ownedKey := strings.Clone(key)
	state := &subscriptionState{ready: make(chan struct{})}
	if existing, loaded := e.subscriptions.LoadOrStore(ownedKey, state); loaded {
		return e.awaitSubscription(existing, unsafe)
	}
	if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		slog.Debug("ensureSubscribed: bootstrapping", "key", ownedKey)
	}
	succeeded := false
	defer func() {
		if succeeded {
			state.markReady()
		}
	}()

	hlc := timestamppb.New(e.clock.Now())
	eff := &pb.Effect{
		Key:            []byte(ownedKey),
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
		e.subscriptions.Delete(ownedKey)
		state.markFailed()
		return err
	}
	offset := e.nextOffset()

	if e.effectCache != nil {
		e.effectCache.PutSized(offset, eff, len(data))
	}
	e.updateIndex(ownedKey, nil, offset)
	e.fireLocalEffect(offset, eff)

	if e.broadcaster == nil {
		succeeded = true
		return nil
	}

	notify := BuildOffsetNotify(e.nodeID, offset, eff, data, nil)

	// Async-announce fast path: when no peer in the cluster holds this key
	// (authoritative while we're in the majority partition) there is no prior
	// state to bootstrap — the blocking rounds below would collect nothing. We
	// still MUST announce the subscription so future peer writes route to us
	// (skipping it splits brain), but we can fire-and-forget: QUIC delivers
	// reliably on a live connection, and peer-recovery re-announce
	// (pubsub_router.OnPeerAdded) heals any peer whose connection was down.
	// Never skip the announce; only skip the wait. The local install above is
	// synchronous (so a subsequent Emit dep-references our SubscriptionEffect),
	// but the network send is off the critical path: fire it in the background
	// so the calling SET/GET returns immediately. A connected peer whose bulk
	// filter hasn't arrived yet forces clusterMaybeHasKey true, so a fresh
	// node always takes the blocking bootstrap below.
	if allowAsync &&
		(e.inMajorityPartition() || unsafe) && !e.clusterMaybeHasKey(ownedKey) {
		go e.broadcaster.BroadcastWithData(notify, notify.EffectData)
		succeeded = true
		return nil
	}

	// Register bootstrap collector before broadcasting so NACKs aren't missed
	collector := &bootstrapCollector{
		nacks: make(chan *pb.NackNotify, 64),
	}
	e.pendingBootstraps.Store(ownedKey, collector)
	defer e.pendingBootstraps.Delete(ownedKey)

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
					e.addPeerSubscriber(ownedKey, pid)
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
			// UnsafeMode: unreachable peers never block. Proceed with local
			// state; the subscription announce reaches recovering peers via
			// the peer-recovery re-announce, and branches merge later.
			if unsafe {
				succeeded = true
				return nil
			}
			// No peers responded — noise sessions likely not ready yet. Retry.
			select {
			case <-bootstrapDeadline:
				slog.Warn("subscription bootstrap: no peers responded after retries",
					"key", ownedKey, "attempts", attempt+1)
				goto done
			case <-time.After(500 * time.Millisecond):
				slog.Debug("subscription bootstrap: retrying, no peers ACK'd",
					"key", ownedKey, "attempt", attempt+1)
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
	// also surface ErrBootstrapIncomplete. UnsafeMode skips the gate:
	// there is no majority to protect, only branches to merge.
	if !unsafe && !e.broadcaster.InMajorityPartition() {
		e.subscriptions.Delete(ownedKey)
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
					e.addPeerSubscriber(ownedKey, pid)
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

	installed, skipped := e.walkAndInstall(ownedKey, unique)

	if installed > 0 {
		if skipped > 0 {
			slog.Debug("ensureSubscribed: bootstrap completed with partial reachability",
				"key", ownedKey, "installed", installed, "skipped", skipped)
		}
		// Recover subscribers that have been compacted into snapshots. The
		// NACK-response seeding above only learns peers that are live and
		// authoritative right now; a peer subscribed before the last compaction
		// (or one currently partitioned away) survives only in snapshot state.
		e.seedSubscribersFromDag(ownedKey)
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
		"key", ownedKey, "unreachable_tips", skipped)
	state.incomplete.Store(true)
	go e.retryBootstrap(ownedKey, state, unique)
	if unsafe {
		return nil
	}
	return ErrBootstrapIncomplete
}

// seedSubscribersFromDag derives the net subscriber set for key from its DAG
// and merges it into leafState, augmenting the NACK-response seeding done
// during the bootstrap probe. This recovers subscribers compacted into
// snapshots whose raw SubscriptionEffects are no longer in the chain.
//
// Snapshots' State.Subscribers provide the base — each already folds the
// sub/unsub events below it. Raw SubscriptionEffects above the snapshot
// frontier apply in causal (topo) order. An unsubscribe wins over a later
// snapshot-union that still lists the peer (the "minus interim unsubscribes"
// rule): a tombstone blocks re-adding it, and a subsequent re-subscribe clears
// the tombstone. Erring toward exclusion is the safe direction — a missing
// subscriber is repaired by live notifications, whereas resurrecting an
// unsubscribed peer would re-establish it as a holder and defeat eviction.
// Verdict snapshots (State == nil) carry no subscribers and are skipped.
func (e *Engine) seedSubscribersFromDag(key string) {
	cs := e.index.Contains(key)
	if cs == nil {
		return
	}
	tips := cs.Tips()
	if len(tips) == 0 {
		return
	}
	d := newDag(e, key, "")
	if err := d.prepare(tips); err != nil {
		slog.Debug("seedSubscribersFromDag: prepare failed", "key", key, "error", err)
		return
	}

	net := make(map[pb.NodeID]struct{})
	tombstoned := make(map[pb.NodeID]struct{})
	for _, t := range d.topoOrder {
		eff := d.nodes[t]
		if snap := eff.GetSnapshot(); snap != nil {
			if snap.State != nil {
				for id, ok := range snap.State.Subscribers {
					if !ok {
						continue
					}
					p := pb.NodeID(id)
					if _, dead := tombstoned[p]; dead {
						continue
					}
					net[p] = struct{}{}
				}
			}
			continue
		}
		if sub := eff.GetSubscription(); sub != nil {
			p := pb.NodeID(sub.SubscriberNodeId)
			if sub.Unsubscribe {
				delete(net, p)
				tombstoned[p] = struct{}{}
			} else {
				net[p] = struct{}{}
				delete(tombstoned, p)
			}
		}
	}

	for id := range net {
		e.addPeerSubscriber(key, id)
	}
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
	const maxRetries = 10
	for attempt := range maxRetries {
		if e.closed.Load() {
			break
		}
		time.Sleep(500 * time.Millisecond)
		if e.closed.Load() {
			break
		}

		installed, _ := e.walkAndInstall(key, nackTips)
		if installed > 0 {
			slog.Debug("retryBootstrap: complete",
				"key", key, "installed", installed, "of", len(nackTips))
			state.incomplete.Store(false)
			state.markReady()
			return
		}
		slog.Debug("retryBootstrap: still incomplete",
			"key", key, "tips", len(nackTips), "attempt", attempt+1)
	}

	slog.Warn("retryBootstrap: giving up, clearing subscription for re-bootstrap",
		"key", key, "tips", len(nackTips))
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
func (e *Engine) reconstruct(key string, tips []Tip, txID string, waitForHorizon bool) (*pb.ReducedEffect, int, error) {
	slog.Debug("reconstruct: start", "key", key, "tips", tips, "txn_id", txID, "wait_for_horizon", waitForHorizon)

	var (
		result           *pb.ReducedEffect
		subRootEffects   []*pb.Effect
		count            int
		crossKeyWalked   int
		d                *dag
		err              error
		expandKeys       map[string]struct{}
		bindsByKey       map[string][]Tip
		snapshotVerdicts verdictMap
		atomicallyLost   map[string]struct{}
	)

	// result aliases a cached snapshot's State whenever the walk adopts one;
	// it must be cloned exactly once before any mutation, after which the
	// accumulation below owns it and reduces in place (reduceChainOwned).
	// Cloning per visited node instead is quadratic in chain length.
	resultFromSnapshot := false
	ensureResultOwned := func() {
		if resultFromSnapshot {
			result = cloneReduced(result)
			resultFromSnapshot = false
		}
	}

	// Retry loop: every time iterate encounters an in-horizon bind on the
	// ancestry of tips AND waitForHorizon is true, the wait completes
	// inline (entry.waiters.RLock blocks until MakeVisible or Abort
	// releases the write lock) and the callback returns errHorizonRetryNeeded.
	// We restart with fresh bindKeyClosure / losersOnKey / dependsOnInvisible
	// state — the resolved bind's verdict is now visible to fork-choice,
	// and its descendants in the walk no longer trigger the "descends from
	// invisible ancestor" skip.
	//
	// Bounded by the number of distinct in-horizon binds on the ancestry
	// (each iteration resolves at least one). Each iteration's wait is
	// bounded by horizonFallbackWait (5s) at worst. Internal callers pass
	// waitForHorizon=false and get the existing single-pass behavior.
	for {
		result = nil
		subRootEffects = nil
		count = 0
		crossKeyWalked = 0
		resultFromSnapshot = false

		expandKeys, bindsByKey, snapshotVerdicts, err = e.bindKeyClosure(key)
		if err != nil {
			return nil, 0, err
		}
		atomicallyLost = make(map[string]struct{})
		for txnID, entry := range snapshotVerdicts {
			if entry.verdict == pb.Verdict_LOST {
				atomicallyLost[txnID] = struct{}{}
			}
		}
		for k := range expandKeys {
			losers, walked := e.losersOnKey(k, bindsByKey[k], snapshotVerdicts)
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

		d = newDag(e, key, txID)
		if err := e.prepareDag(d, key, tips); err != nil {
			return nil, 0, err
		}

		// An empty horizon means nothing is mid-wait, so no walked bind can be
		// invisible — skip the precompute (2 map allocs + a full topoOrder scan)
		// and the per-bind checks below entirely.
		var isInvisible func(string) bool
		if e.horizon != nil && !e.horizon.Empty() {
			isInvisible = e.horizon.IsInvisible
		}

		// Propagate HorizonSet invisibility along DAG dep edges. A visible
		// bind whose causal chain reaches an invisible bind cannot be
		// honored: its writes describe a state that's only consistent if
		// the ancestor's pending verdict resolves WON. Until that verdict
		// lands, including the descendant while skipping the ancestor would
		// surface a "future without its past" (Elle :incompatible-order).
		// Walk d.topoOrder (parents before children, includes tx-data tips
		// iterate filters from the processor stream) and mark every tip
		// that transitively depends on an invisible bind.
		//
		// When waitForHorizon is true, the iterate callback below will
		// block on the invisible ancestor (parents visited first in topo
		// order) and signal retry; on the next iteration this precompute
		// runs against the post-resolution state, so the marked set will
		// be empty by the time we reach a descendant that would have been
		// skipped here.
		var dependsOnInvisible map[Tip]struct{}
		if isInvisible != nil {
			invisibleBindTips := make(map[Tip]struct{})
			dependsOnInvisible = make(map[Tip]struct{})
			for _, t := range d.topoOrder {
				eff := d.nodes[t]
				var refs []*pb.EffectRef
				if bind := eff.GetTxnBind(); bind != nil {
					for _, kb := range bind.Keys {
						if string(kb.Key) == key {
							refs = []*pb.EffectRef{kb.NewTip}
							break
						}
					}
				} else {
					refs = eff.GetDeps()
				}
				for _, ref := range refs {
					dt := r(ref)
					if _, ok := invisibleBindTips[dt]; ok {
						dependsOnInvisible[t] = struct{}{}
						break
					}
					if _, ok := dependsOnInvisible[dt]; ok {
						dependsOnInvisible[t] = struct{}{}
						break
					}
				}
				if bind := eff.GetTxnBind(); bind != nil && isInvisible(eff.TxnId) {
					invisibleBindTips[t] = struct{}{}
				}
			}
		}

		err = d.iterate(func(tip Tip, eff *pb.Effect) error {
			count++

			if snap := eff.GetSnapshot(); snap != nil && snap.State != nil {
				result = snap.State
				resultFromSnapshot = true
				return nil
			}

			if eff.GetSubscription() != nil && len(eff.Deps) == 0 {
				subRootEffects = append(subRootEffects, eff)
				return nil
			}

			if bind := eff.GetTxnBind(); bind != nil {
				if isInvisible != nil && isInvisible(eff.TxnId) {
					if waitForHorizon && e.horizon != nil {
						// Block on the entry's waiter mutex. RLock returns
						// once MakeVisible or Abort releases the write lock
						// taken at horizon.Add. Release the read token,
						// then signal the outer loop to restart with fresh
						// fork-choice + dependsOnInvisible state.
						slog.Debug("reconstruct: waiting on in-horizon ancestor",
							"key", key, "txn", eff.TxnId)
						if w := e.horizon.WaitForClear(eff.TxnId); w != nil {
							w.Release()
						}
						return errHorizonRetryNeeded
					}
					slog.Debug("reconstruct: skip bind (invisible)", "key", key, "txn", eff.TxnId)
					return nil
				}
				if dependsOnInvisible != nil {
					if _, ok := dependsOnInvisible[tip]; ok {
						slog.Debug("reconstruct: skip bind (descends from invisible ancestor)",
							"key", key, "txn", eff.TxnId)
						return nil
					}
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
				ensureResultOwned()
				bindEffects, fetchErr := e.collectBindEffects(eff, key)
				if fetchErr != nil {
					return fetchErr
				}
				result = reduceChainOwned(result, bindEffects)
				return nil
			}

			ensureResultOwned()
			result = reduceChainOwned(result, []*pb.Effect{eff})
			return nil
		})
		if errors.Is(err, errHorizonRetryNeeded) {
			continue
		}
		if err != nil {
			return nil, 0, err
		}
		break
	}

	if count == 0 {
		return nil, 0, nil
	}

	if len(subRootEffects) > 0 {
		ensureResultOwned()
		result = reduceChainOwned(result, subRootEffects)
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

	// A walk that adopted a snapshot with no reduction after it would return
	// the pooled effect's State itself; callers hand the result to
	// filterSnapshot, which writes fields. No-op when reductions already own
	// the result, so the single-clone invariant holds.
	ensureResultOwned()

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
func (e *Engine) bindKeyClosure(startKey string) (map[string]struct{}, map[string][]Tip, verdictMap, error) {
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
			eff, err := e.getEffect(k, t)
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
			if snap := eff.GetSnapshot(); snap != nil {
				verdicts.harvest(t, snap)
				if snap.PrevSnapshot != nil {
					stack = append(stack, r(snap.PrevSnapshot))
				}
				if snap.State == nil {
					for _, dep := range eff.Deps {
						stack = append(stack, r(dep))
					}
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
func (e *Engine) losersOnKey(key string, extraTips []Tip, snapshotVerdicts verdictMap) (map[string]struct{}, int) {
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
	if e.horizon != nil && !e.horizon.Empty() {
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
		eff, err := e.getEffect(key, t)
		if err != nil {
			continue
		}
		walked++

		if snap := eff.GetSnapshot(); snap != nil {
			verdicts.harvest(t, snap)
			if snap.PrevSnapshot != nil {
				stack = append(stack, r(snap.PrevSnapshot))
			}
			if snap.State == nil {
				for _, dep := range eff.Deps {
					stack = append(stack, r(dep))
				}
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
			eff, err := e.getEffect(key, cur)
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

		eff, err := e.getEffect(key, t)
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
// from the effect cache, peers, or the cloud CDN as needed. key is the key
// the walk runs on — it picks the fetch source order via fetchHint — and may
// be "" when the caller has no single-key context (cross-key bind
// adjudication), which defaults to peers-first.
func (e *Engine) getEffect(key string, offset Tip) (*pb.Effect, error) {
	// Check deserialized effect cache
	if e.effectCache != nil {
		if cached, ok := e.effectCache.Get(offset); ok {
			return cached, nil
		}
	}

	// Not in cache — try to fetch from remote
	if e.broadcaster == nil {
		return nil, fmt.Errorf("getEffect: offset %d not cached and no broadcaster", offset)
	}
	fetchedData, fetchErr := e.broadcaster.FetchFromAny(toPbRef(offset), e.fetchHint(key))
	if fetchErr != nil {
		return nil, fmt.Errorf("getEffect: offset %d fetch failed: %w", offset, fetchErr)
	}
	if storeErr := e.storeWireData(offset, fetchedData); storeErr != nil {
		return nil, fmt.Errorf("getEffect: failed to store fetched offset %d: %w", offset, storeErr)
	}
	// storeWireData puts it in effectCache; try again
	if e.effectCache != nil {
		if cached, ok := e.effectCache.Get(offset); ok {
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
