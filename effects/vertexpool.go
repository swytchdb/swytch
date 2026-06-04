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
	"math"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"google.golang.org/protobuf/proto"

	clox "github.com/swytchdb/swytch/cache"
	pb "github.com/swytchdb/swytch/cluster/proto"
)

// vertexOverhead approximates the per-entry bookkeeping cost (the Tip key,
// the vertex struct, and the map slot) added to the marshalled effect size
// for byte accounting.
const vertexOverhead = 48

// vertex wraps an immutable effect resident in the VertexPool. size caches
// the entry's accounted bytes so eviction can decrement the total without
// re-marshalling. refs counts how many index tips and cached subdags include
// this vertex (key membership); reclaimUnreferenced frees a vertex only once
// refs hits zero.
type vertex struct {
	eff  *pb.Effect
	size int64
	refs atomic.Int32
}

// VertexPool is the engine's deserialized effect store: a map from Tip to
// its effect, replacing the per-offset CloxCache. Presence in the pool means
// "resident locally"; absence means getEffect must fetch the bytes from a
// peer. The pool is dumb storage with byte accounting — it has no eviction
// policy of its own. Eviction is driven by the engine memory governor, which
// is the only component that knows a key's active DAG path and can therefore
// choose safe victims.
//
// Per-key refcounting (incref/decref) lets eviction drop whole keys and
// reclaim only the vertices no other key still needs; a vertex is referenced
// by every index tip and cached subdag that includes it.
type VertexPool struct {
	m     *xsync.Map[Tip, *vertex]
	bytes atomic.Int64

	hits      atomic.Uint64
	misses    atomic.Uint64
	evictions atomic.Uint64
}

func newVertexPool() *VertexPool {
	return &VertexPool{m: xsync.NewMap[Tip, *vertex]()}
}

// effectSize returns the accounted byte cost of holding eff.
func effectSize(eff *pb.Effect) int64 {
	if eff == nil {
		return vertexOverhead
	}
	return int64(proto.Size(eff)) + vertexOverhead
}

// Get returns the effect at tip.
func (p *VertexPool) Get(tip Tip) (*pb.Effect, bool) {
	v, ok := p.m.Load(tip)
	if !ok {
		p.misses.Add(1)
		return nil, false
	}
	p.hits.Add(1)
	return v.eff, true
}

// Put stores eff at tip on first insertion. A re-Put of an already-resident
// tip keeps the existing vertex so its accumulated refcount survives: effects
// are immutable (a Tip is written once), so the payload and byte cost are
// identical and there is nothing to update. Replacing the vertex would reset
// refs to 0, letting reclaimUnreferenced free an effect the index still holds
// as a frontier tip — a premature free surfacing as a missing read.
func (p *VertexPool) Put(tip Tip, eff *pb.Effect) {
	nv := &vertex{eff: eff, size: effectSize(eff)}
	if _, loaded := p.m.LoadOrStore(tip, nv); !loaded {
		p.bytes.Add(nv.size)
	}
}

// delete removes tip from the pool, decrements the byte total, and counts an
// eviction. Returns the bytes freed, or 0 if tip was absent.
func (p *VertexPool) delete(tip Tip) int64 {
	prev, loaded := p.m.LoadAndDelete(tip)
	if !loaded {
		return 0
	}
	p.bytes.Add(-prev.size)
	p.evictions.Add(1)
	return prev.size
}

// rangeVertices iterates every resident vertex. The callback must not call
// the pool's mutating methods on the same tip mid-iteration other than
// delete, which xsync.Map tolerates.
func (p *VertexPool) rangeVertices(fn func(tip Tip, v *vertex) bool) {
	p.m.Range(fn)
}

// incref bumps the key-membership refcount of the vertex at tip, if resident.
// A no-op for a non-resident tip (e.g. an effect served from the wire-parse
// fallback that never entered the pool) — refcounting only governs pooled
// vertices.
func (p *VertexPool) incref(tip Tip) {
	if v, ok := p.m.Load(tip); ok {
		v.refs.Add(1)
	}
}

// decref lowers the key-membership refcount of the vertex at tip, if resident.
func (p *VertexPool) decref(tip Tip) {
	if v, ok := p.m.Load(tip); ok {
		v.refs.Add(-1)
	}
}

// Bytes returns the accounted memory held by the pool.
func (p *VertexPool) Bytes() int64 { return p.bytes.Load() }

// EntryCount returns the number of resident effects.
func (p *VertexPool) EntryCount() int { return p.m.Size() }

// Stats returns cumulative hit, miss, and eviction counts.
func (p *VertexPool) Stats() (hits, misses, evictions uint64) {
	return p.hits.Load(), p.misses.Load(), p.evictions.Load()
}

// startMemoryGovernor launches the background loop that keeps process RSS near
// targetBytes by sweeping below-LCA effects out of the vertex pool. It also
// sets GOMEMLIMIT (if unset) so the GC cooperates with the budget, mirroring
// the prior CloxCache enforcement. targetBytes <= 0 disables enforcement.
//
// This is the interim memory-pressure mechanism. It is looser than the old
// per-Put CloxCache eviction (it reacts on a 1s tick rather than inline),
// because choosing a safe victim now requires the active-path walk, which is
// too expensive for the ingest hot path. The tight, per-key bound returns
// with the sharded eviction policy in a later stage.
func (e *Engine) startMemoryGovernor(targetBytes int64) {
	if targetBytes <= 0 {
		return
	}
	if cur := debug.SetMemoryLimit(-1); cur == math.MaxInt64 {
		// Reserve 5% of the target for non-heap memory (stacks, mmap, etc.).
		debug.SetMemoryLimit(targetBytes * 95 / 100)
		slog.Debug("vertex pool: set GOMEMLIMIT", "limit", clox.FormatMemory(uint64(targetBytes*95/100)))
	}
	e.memGovStop = make(chan struct{})
	e.memGovWg.Add(1)
	go e.memoryGovernorLoop(targetBytes)
}

func (e *Engine) memoryGovernorLoop(targetBytes int64) {
	defer e.memGovWg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-e.memGovStop:
			return
		case <-ticker.C:
			if int64(clox.GetProcessRSS()) > targetBytes {
				e.relieveMemoryPressure(targetBytes)
			}
		}
	}
}

// maxEvictionsPerTick caps how many cold keys the governor evicts in one tick
// so a single pass can't stall on a huge eviction storm (each eviction also
// broadcasts an unsubscribe). Pressure persisting past the cap is handled on
// the next tick.
const maxEvictionsPerTick = 256

// relieveMemoryPressure runs the two-axis reclaim under memory pressure: first
// drop refs-0 vertices (below-LCA history and orphans — cheap, no eviction),
// then, if the pool's own footprint still exceeds the target, evict cold keys
// via the bounded sweep until under target or the per-tick cap is hit. Each
// eviction releases the key's refs synchronously, so a final reclaim frees the
// now-unreferenced vertices.
func (e *Engine) relieveMemoryPressure(targetBytes int64) {
	e.reclaimUnreferenced()
	evicted := 0
	for e.effectCache.Bytes() > targetBytes && evicted < maxEvictionsPerTick {
		if !e.index.EvictBounded(0) {
			break // nothing evictable this sweep
		}
		evicted++
	}
	if evicted > 0 {
		e.reclaimUnreferenced()
		slog.Debug("vertex pool: evicted cold keys under pressure",
			"evicted", evicted, "remaining_bytes", e.effectCache.Bytes())
	}
}

// reclaimUnreferenced drops every pool vertex whose key-membership refcount is
// zero: it is held by no current index tip and no cached subdag, so it is
// below some LCA or an orphan and safe to evict — reconstruct never needs it
// and getEffect re-fetches on the rare miss. This replaces the activeSet walk:
// the maintained refcount is the membership oracle, so reclamation is a single
// pool pass with no per-key DAG traversal. System-key effects are pinned
// defensively (their tips keep them referenced anyway). Returns bytes freed.
func (e *Engine) reclaimUnreferenced() int64 {
	if e.effectCache == nil {
		return 0
	}
	var reclaimed int64
	e.effectCache.rangeVertices(func(tip Tip, v *vertex) bool {
		if v.refs.Load() > 0 {
			return true
		}
		if v.eff != nil && isSystemKey(v.eff.Key) {
			return true
		}
		reclaimed += e.effectCache.delete(tip)
		return true
	})
	if reclaimed > 0 {
		slog.Debug("vertex pool: reclaimed unreferenced effects",
			"reclaimed", reclaimed, "remaining_bytes", e.effectCache.Bytes())
	}
	return reclaimed
}
