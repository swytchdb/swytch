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
	"log/slog"

	pb "github.com/swytchdb/swytch/cluster/proto"
)

// peerKeyFilter is this node's view of which keys a single peer holds, from
// two sources kept separate:
//   - bulk: the peer's own filter, delivered at connection establishment.
//     Replaced wholesale when a newer version arrives (bulkVer guards against
//     stale copies). The peer's filter is monotonic, so a newer bulk is a
//     superset of the old — replacing (not appending) avoids unbounded
//     duplication.
//   - realtime: append-only additions from inbound SubscriptionEffects, kept
//     across bulk replacements so a key announced since the last bulk is not
//     lost.
//
// Both only ever add — a stale entry is a safe false-positive (a needless
// subscribe, never a wrong miss).
//
// A connected peer whose bulk filter hasn't arrived yet is presumed to hold
// EVERYTHING: absence of knowledge must read as "must ask", never "definitely
// absent" — the inverse default fabricates authoritative misses for keys the
// peer actually holds. counted marks peers the connection layer has announced
// via NotePeerConnected; unfilteredPeers tracks how many of those still lack a
// bulk filter, so clusterMaybeHasKey can answer the conservative default
// without iterating peers on the read hot path.
type peerKeyFilter struct {
	bulk     CuckooChain
	bulkVer  uint64
	hasBulk  bool // a bulk filter has been applied; empty and version-0 are both legitimate
	realtime CuckooChain
	counted  bool
}

func (pf *peerKeyFilter) maybeContains(key string) bool {
	return pf.bulk.MaybeContains(key) || pf.realtime.MaybeContains(key)
}

// ownFilterAdd records that this node now holds data for key. Called from the
// data-added notification sites.
func (e *Engine) ownFilterAdd(key string) {
	e.keyFilterMu.Lock()
	if e.ownKeyFilter.Add(key) {
		// Only bump the version when the set actually changed, so the
		// marshaled-bytes cache isn't invalidated by idempotent re-adds.
		e.ownFilterVer++
	}
	e.keyFilterMu.Unlock()
}

// OwnKeyFilterSnapshot returns the serialized own filter and its version,
// re-marshaling only when the filter changed since the last call. The cluster
// layer sends this to a peer at connection establishment.
func (e *Engine) OwnKeyFilterSnapshot() ([]byte, uint64) {
	e.keyFilterMu.Lock()
	defer e.keyFilterMu.Unlock()
	if e.ownFilterBytes == nil || e.ownFilterBytesVer != e.ownFilterVer {
		b, err := e.ownKeyFilter.MarshalBinary()
		if err != nil {
			slog.Error("OwnKeyFilterSnapshot: marshal failed", "error", err)
			return nil, 0
		}
		e.ownFilterBytes = b
		e.ownFilterBytesVer = e.ownFilterVer
	}
	return e.ownFilterBytes, e.ownFilterVer
}

// peerFilterAdd records, in real time, that peer announced interest in key
// (via a SubscriptionEffect). This is the load-bearing freshness signal: it
// fires synchronously with the majority-gated subscription broadcast, so a
// committed write's key is visible to every majority peer before the write acks.
func (e *Engine) peerFilterAdd(peer pb.NodeID, key string) {
	if peer == e.nodeID {
		return
	}
	e.keyFilterMu.Lock()
	e.getOrCreatePeerFilter(peer).realtime.Add(key)
	e.keyFilterMu.Unlock()
}

// getOrCreatePeerFilter returns the filter entry for peer, creating it if
// absent. Caller must hold keyFilterMu.
func (e *Engine) getOrCreatePeerFilter(peer pb.NodeID) *peerKeyFilter {
	if e.peerKeyFilters == nil {
		e.peerKeyFilters = make(map[pb.NodeID]*peerKeyFilter)
	}
	pf := e.peerKeyFilters[peer]
	if pf == nil {
		pf = &peerKeyFilter{}
		e.peerKeyFilters[peer] = pf
	}
	return pf
}

// NotePeerConnected marks peer as a live cluster member whose key filter we
// may not have yet. Until its bulk filter arrives, the peer is presumed to
// hold every key (clusterMaybeHasKey answers true), so reads bootstrap for
// real instead of fabricating misses. Called by the cluster layer when a peer
// connection is registered.
func (e *Engine) NotePeerConnected(peer pb.NodeID) {
	if peer == e.nodeID {
		return
	}
	e.keyFilterMu.Lock()
	pf := e.getOrCreatePeerFilter(peer)
	if !pf.counted {
		pf.counted = true
		if !pf.hasBulk {
			e.unfilteredPeers++
		}
	}
	e.keyFilterMu.Unlock()
}

// CachePeerFilter splices a peer's bulk filter into our view of that peer, if
// it's newer than what we've already applied. Delivered by the cluster layer
// at connection establishment; the arrival releases the peer from the
// "presumed to hold everything" default.
func (e *Engine) CachePeerFilter(peer pb.NodeID, data []byte, version uint64) {
	if peer == e.nodeID {
		return
	}
	// An empty filter is legitimate: a fresh node holds nothing, and saying
	// so is exactly what releases it from the presumed-everything default.
	var decoded CuckooChain
	if err := decoded.UnmarshalBinary(data); err != nil {
		slog.Warn("CachePeerFilter: undecodable filter from peer", "peer", peer, "error", err)
		return
	}
	e.keyFilterMu.Lock()
	pf := e.getOrCreatePeerFilter(peer)
	if !pf.hasBulk || version > pf.bulkVer {
		// Replace, don't append: the peer's filter is monotonic, so a newer
		// bulk is a superset of the old. Real-time adds live in pf.realtime
		// and are untouched.
		if pf.counted && !pf.hasBulk {
			e.unfilteredPeers--
		}
		pf.bulk = decoded
		pf.bulkVer = version
		pf.hasBulk = true
	}
	e.keyFilterMu.Unlock()
}

// removePeerKeyFilter drops a departed peer's filter. Deliberately forgets
// everything: if the peer reconnects it re-enters through NotePeerConnected
// as "presumed to hold everything" until a fresh bulk arrives — its key set
// may have grown while we weren't listening, and treating the stale filter
// as authority would fabricate misses for the new keys.
func (e *Engine) removePeerKeyFilter(peer pb.NodeID) {
	e.keyFilterMu.Lock()
	if pf := e.peerKeyFilters[peer]; pf != nil {
		if pf.counted && !pf.hasBulk {
			e.unfilteredPeers--
		}
		delete(e.peerKeyFilters, peer)
	}
	e.keyFilterMu.Unlock()
}

// inMajorityPartition reports whether read-misses can be trusted. A node not
// in the majority may be missing recent writes, so it must fall back to a real
// subscribe rather than serve a filter-miss. Standalone (no broadcaster) is
// the whole cluster, so its local view is authoritative.
func (e *Engine) inMajorityPartition() bool {
	if e.broadcaster == nil {
		return true
	}
	return e.broadcaster.InMajorityPartition()
}

// clusterMaybeHasKey reports whether any peer may hold data for key. For a
// node in the majority partition a false return is authoritative: every
// connected peer has delivered its filter and none admits the key, so it has
// no committed, causally-prior value, and a read may return a miss without
// subscribing. A connected peer whose filter hasn't arrived yet forces true —
// the peer holds everything until it tells us otherwise. False-positives are
// safe — they fall through to a real subscribe + reconstruct.
func (e *Engine) clusterMaybeHasKey(key string) bool {
	e.keyFilterMu.RLock()
	defer e.keyFilterMu.RUnlock()
	if e.unfilteredPeers > 0 {
		return true
	}
	for _, pf := range e.peerKeyFilters {
		if pf.maybeContains(key) {
			return true
		}
	}
	return false
}

// fetchHint orders FetchFromAny's sources for a fetch on key: a key some
// peer's filter claims is served peers-first (the cluster holds it — the CDN
// would be a WAN detour and, in bulk, a request storm the edge blocks); a key
// no peer claims is cloud state being rehydrated, CDN-first. Unknown key
// (cross-key bind adjudication) defaults to peers — those refs arrive via
// NACK chains a peer provably holds.
func (e *Engine) fetchHint(key string) FetchHint {
	if key == "" || e.clusterMaybeHasKey(key) {
		return PreferPeers
	}
	return PreferCDN
}
