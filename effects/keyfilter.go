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
//   - bulk: the peer's own filter, delivered on a NACK. Replaced wholesale
//     when a newer version arrives (bulkVer guards against stale copies). The
//     peer's filter is monotonic, so a newer bulk is a superset of the old —
//     replacing (not appending) avoids unbounded duplication.
//   - realtime: append-only additions from inbound SubscriptionEffects, kept
//     across bulk replacements so a key announced since the last bulk is not
//     lost.
//
// Both only ever add — a stale entry is a safe false-positive (a needless
// subscribe, never a wrong miss).
type peerKeyFilter struct {
	bulk     CuckooChain
	bulkVer  uint64
	realtime CuckooChain
}

func (pf *peerKeyFilter) maybeContains(key string) bool {
	return pf.bulk.MaybeContains(key) || pf.realtime.MaybeContains(key)
}

// ownFilterAdd records that this node now holds data for key. Called from the
// data-added notification sites. System keys are skipped: they are not user
// data and are never served via the read-only fast path.
func (e *Engine) ownFilterAdd(key string) {
	if isSystemKey([]byte(key)) {
		return
	}
	e.keyFilterMu.Lock()
	if e.ownKeyFilter.Add(key) {
		// Only bump the version when the set actually changed, so the
		// marshaled-bytes cache isn't invalidated by idempotent re-adds.
		e.ownFilterVer++
	}
	e.keyFilterMu.Unlock()
}

// ownFilterSnapshot returns the serialized own filter and its version,
// re-marshaling only when the filter changed since the last call.
func (e *Engine) ownFilterSnapshot() ([]byte, uint64) {
	e.keyFilterMu.Lock()
	defer e.keyFilterMu.Unlock()
	if e.ownFilterBytes == nil || e.ownFilterBytesVer != e.ownFilterVer {
		b, err := e.ownKeyFilter.MarshalBinary()
		if err != nil {
			slog.Error("ownFilterSnapshot: marshal failed", "error", err)
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
	if peer == e.nodeID || isSystemKey([]byte(key)) {
		return
	}
	e.keyFilterMu.Lock()
	if e.peerKeyFilters == nil {
		e.peerKeyFilters = make(map[pb.NodeID]*peerKeyFilter)
	}
	pf := e.peerKeyFilters[peer]
	if pf == nil {
		pf = &peerKeyFilter{}
		e.peerKeyFilters[peer] = pf
	}
	pf.realtime.Add(key)
	e.keyFilterMu.Unlock()
}

// cachePeerFilter splices a peer's bulk filter (received on a NACK) into our
// view of that peer, if it's newer than what we've already applied. This is
// the bulk-bootstrap channel — a joining node learns every peer's existing
// keys from the membership-key NACKs.
func (e *Engine) cachePeerFilter(peer pb.NodeID, data []byte, version uint64) {
	if peer == e.nodeID || len(data) == 0 {
		return
	}
	var decoded CuckooChain
	if err := decoded.UnmarshalBinary(data); err != nil {
		slog.Warn("cachePeerFilter: undecodable filter from peer", "peer", peer, "error", err)
		return
	}
	e.keyFilterMu.Lock()
	if e.peerKeyFilters == nil {
		e.peerKeyFilters = make(map[pb.NodeID]*peerKeyFilter)
	}
	pf := e.peerKeyFilters[peer]
	if pf == nil {
		pf = &peerKeyFilter{}
		e.peerKeyFilters[peer] = pf
	}
	if version > pf.bulkVer {
		// Replace, don't append: the peer's filter is monotonic, so a newer
		// bulk is a superset of the old. Real-time adds live in pf.realtime
		// and are untouched.
		pf.bulk = decoded
		pf.bulkVer = version
	}
	e.keyFilterMu.Unlock()
}

// removePeerKeyFilter drops a departed peer's filter so its keys stop forcing
// needless subscribes. Eviction is an optimization only — a retained filter
// is safe.
func (e *Engine) removePeerKeyFilter(peer pb.NodeID) {
	e.keyFilterMu.Lock()
	delete(e.peerKeyFilters, peer)
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
// node in the majority partition a false return is authoritative: no peer
// ever announced the key, so it has no committed, causally-prior value, and a
// read may return a miss without subscribing. False-positives are safe — they
// fall through to a real subscribe + reconstruct.
func (e *Engine) clusterMaybeHasKey(key string) bool {
	e.keyFilterMu.RLock()
	defer e.keyFilterMu.RUnlock()
	for _, pf := range e.peerKeyFilters {
		if pf.maybeContains(key) {
			return true
		}
	}
	return false
}
