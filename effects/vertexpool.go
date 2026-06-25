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
	"fmt"
	"log/slog"
	"math"
	"runtime/debug"
	"runtime/metrics"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"google.golang.org/protobuf/proto"

	clox "github.com/swytchdb/swytch/cache"
	pb "github.com/swytchdb/swytch/cluster/proto"
)

// vertexOverhead approximates the per-entry bookkeeping cost added to the
// marshalled effect size for byte accounting. The bare structs alone are ~48B
// (vertex ~24 + Tip key 16 + map value pointer 8), but that ignores heap
// size-class rounding on the vertex allocation, the xsync.Map bucket/hash
// overhead per entry, and the in-memory *pb.Effect struct cost (Hlc, Deps/
// ForkChoiceHash slice headers, the Kind interface) that proto.Size does not
// capture — dominant for small effects. 96 is a conservative estimate of the
// total; over-counting makes the governor evict slightly early, which is the
// safe direction (under-counting lets RSS overshoot the target).
const vertexOverhead = 96

// vertex wraps an immutable effect resident in the VertexPool. size is a
// per-vertex byte estimate (protoLen + vertexOverhead) kept only for reclaim
// telemetry — it is no longer summed into a pool footprint (Bytes() reads the
// runtime's live heap instead). refs counts how many DAGs reference this vertex:
// the tipsets that reach it (one per occupying key, set at PutSized) plus the
// cached read-subdags that adopted it (publishSubdag). Each tipset ref is
// released by its key's eviction walk; each subdag ref by the subdag's
// drop/replace or its leaf's eviction. reclaimUnreferenced frees a vertex only
// once refs hits zero.
type vertex struct {
	eff  *pb.Effect
	size int64
	refs atomic.Int32
}

// VertexPool is the engine's deserialized effect store: a map from Tip to
// its effect, replacing the per-offset CloxCache. Presence in the pool means
// "resident locally"; absence means getEffect must fetch the bytes from a
// peer. The pool is dumb storage — it has no eviction policy of its own.
// Eviction is driven by the engine memory governor, which is the only component
// that knows a key's active DAG path and can therefore choose safe victims.
//
// The pool does NOT meter its own footprint: a protoLen-based sum undercounted
// each decoded effect graph ~3x and was blind to the off-pool per-key state
// (subdags, reduced memos, subscriptions) that grows alongside it. Bytes()
// reports the runtime's exact live heap instead — see Bytes.
//
// DAG-reference counting lets eviction drop whole keys and reclaim only the
// vertices no other key or read still needs; a vertex is referenced by every
// tipset that reaches it and every cached subdag that adopted it.
type VertexPool struct {
	m *xsync.Map[Tip, *vertex]

	// reclaimQueue holds tips whose refcount just transitioned to zero — the
	// candidates the governor's per-tick drainReclaim frees without ever scanning
	// the whole map. This is the sole reclaim mechanism: there is no full-map
	// rangeVertices fallback, at any pressure level, because every path to refs==0
	// enqueues here — the two decrement sites (decref, decrefChainNode) and
	// born-at-zero cache puts (PutSizedCache) — so the queue is by construction the
	// complete set of reclaimable vertices. Completeness is enforced by
	// TestRefcount_EvictAllReclaimsPool (evict every key, then the pool must be
	// empty); a future decrement site that forgets to enqueue fails that test
	// rather than silently leaking. The single governor goroutine consumes;
	// reclaimPending counts enqueued-but-undrained entries so the consumer dequeues
	// exactly the available set (UMPSCQueue.Dequeue blocks on empty, has no Len),
	// mirroring releaseQueue/releasePending. A stale or duplicate entry is harmless:
	// drainReclaim's CAS(0→tombstone) fails on a re-referenced vertex and delete
	// no-ops on an absent one. Allocated lazily (reclaimQ) on the first transition.
	reclaimQueue   atomic.Pointer[xsync.UMPSCQueue[Tip]]
	reclaimPending atomic.Int64

	hits   atomic.Uint64
	misses atomic.Uint64
	// reclaimed counts vertices freed by reclaimUnreferenced (refs-0 below-LCA
	// history and orphans) — pure storage churn, not cache eviction. delete()
	// is its only increment site. coldEvictions counts keys the index's bounded
	// sweep actually evicted under memory pressure (the real "evicted_keys"),
	// bumped once per victim in onLeafEvicted. These were conflated before: the
	// old single "evictions" counter only ever saw reclaim churn, so cold-key
	// eviction was invisible in telemetry.
	reclaimed     atomic.Uint64
	coldEvictions atomic.Uint64
}

func newVertexPool() *VertexPool {
	return &VertexPool{m: xsync.NewMap[Tip, *vertex]()}
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

// Put stores eff at tip, computing its accounted size from the proto. Callers
// that already hold the serialized length — a marshalled buffer on emit, the
// wire protoData on ingest — should use PutSized to skip the proto.Size walk.
//
// A re-Put of an already-resident tip keeps the existing vertex so its
// accumulated refcount survives: effects are immutable (a Tip is written once),
// so the payload and byte cost are identical. Replacing the vertex would reset
// refs to 0, letting reclaimUnreferenced free an effect the index still holds
// as a frontier tip — a premature free surfacing as a missing read. The
// resident fast-path also skips the proto.Size walk on the common re-delivery.
func (p *VertexPool) Put(tip Tip, eff *pb.Effect) {
	if _, ok := p.m.Load(tip); ok {
		return
	}
	p.PutSized(tip, eff, proto.Size(eff))
}

// PutSized stores eff at tip using a caller-supplied serialized length, avoiding
// the proto.Size walk Put would do. protoLen is the marshalled effect size (the
// length of MarshalEffect's output, or the wire protoData); vertexOverhead is
// added on top. Re-Put semantics match Put: an already-resident tip is kept.
//
// A new vertex is born with its creation refcount equal to the number of tipsets
// it occupies — its "walkable from a tipset" references, one per reaching tip.
// An ordinary effect is the tip of its single key, so it starts at 1; a bind
// commits a whole keyset and is the tip of each key, so it starts at len(Keys).
// This pins the vertex (interior chain nodes included, local or backfilled from a
// peer) until each occupying key is evicted, where the eviction reachable-walk
// decrefs it once per key (releaseChainRefs). The count is stored before the
// vertex is published via LoadOrStore, so reclaimUnreferenced never observes a
// fresh vertex at 0 (a birth-race free). Read adoption layers further refs on top
// in publishSubdag.
//
// If the tip is already resident as a cache entry (PutSizedCache left it at
// refs==0 because we did not yet serve its key), this owned Put promotes it by
// adding the creation ref it lacked, so reclaim won't drop a vertex we now own.
// The CAS makes that race-safe: it loses to reclaim's 0→tombstone claim (the
// vertex is being freed; we refetch on the next miss) and to a concurrent read's
// publishSubdag incref (refs>0; left un-promoted, resolved by a later refetch),
// and it never double-counts an already-owned re-put (refs>0 → CAS fails → keep
// the accumulated count).
func (p *VertexPool) PutSized(tip Tip, eff *pb.Effect, protoLen int) {
	tc := tipCount(eff)
	nv := &vertex{eff: eff, size: int64(protoLen) + vertexOverhead}
	nv.refs.Store(tc)
	if v, loaded := p.m.LoadOrStore(tip, nv); loaded {
		v.refs.CompareAndSwap(0, tc) // promote a cache-only entry to owned, once
	}
}

// tipCount is the number of index tipsets a freshly-emitted effect occupies: one
// per key it is the tip of. A bind commits every key in its keyset, so it is the
// tip of each; any other effect is the tip of its single key.
func tipCount(eff *pb.Effect) int32 {
	if bind := eff.GetTxnBind(); bind != nil {
		if n := len(bind.Keys); n > 0 {
			return int32(n)
		}
	}
	return 1
}

// PutSizedCache stores a fetched effect on a key we do NOT serve as a reclaimable
// cache entry: refs start at 0, so it carries no creation ref and reclaim frees
// it the moment no subdag adopts it. These are cross-key bind adjudication
// fetches (and remote effects on unsubscribed keys): a read that walks them holds
// them via the reconstruct dag for its duration, and they are refetchable from a
// peer, so they must not pin memory the way an owned effect's creation ref does.
// An already-resident owned tip is kept (LoadOrStore), never downgraded. protoLen
// semantics match PutSized.
//
// A freshly-stored entry is born at refs==0 and will never see a refs→0
// transition unless first adopted (publishSubdag) and then dropped, so it is
// enqueued as a reclaim candidate at birth — without this an un-adopted cache
// fetch would never be reclaimed (there is no full-map scan to catch it).
func (p *VertexPool) PutSizedCache(tip Tip, eff *pb.Effect, protoLen int) {
	nv := &vertex{eff: eff, size: int64(protoLen) + vertexOverhead}
	if _, loaded := p.m.LoadOrStore(tip, nv); !loaded {
		p.enqueueReclaim(tip)
	}
}

// delete removes tip from the pool and counts a reclaim. It does NOT touch the
// effect's deps: a vertex is kept alive by the tipsets and DAGs that reach it
// (its own creation/adoption refs), not by its parents, so freeing a vertex
// never cascades to its deps — a shared dep stays as long as another reacher
// still references it (which is why the reclaim queue, not a re-scan, is what
// catches each newly-zero vertex). Returns the per-vertex size estimate freed
// (telemetry only), or 0 if tip was absent. Its only caller is claimAndDelete.
func (p *VertexPool) delete(tip Tip) int64 {
	prev, loaded := p.m.LoadAndDelete(tip)
	if !loaded {
		return 0
	}
	p.reclaimed.Add(1)
	return prev.size
}

// decrefChainNode releases one creation reference on tip and returns its effect
// so the eviction reachable-walk (releaseChainRefs) can follow its deps. ok is
// false when tip is not resident — already reclaimed or never fetched — so there
// is nothing below it to release. It decrefs only when refs>0: a served key's
// chain can include cache nodes (refs==0, no creation ref) fetched for cross-key
// adjudication, and those carry no creation ref to release — the subdag-adoption
// decref already handled any DAG ref they held. A single map load serves both the
// decref and the dep follow.
func (p *VertexPool) decrefChainNode(tip Tip) (*pb.Effect, bool) {
	v, ok := p.m.Load(tip)
	if !ok {
		return nil, false
	}
	for {
		n := v.refs.Load()
		if n <= 0 {
			break // cache node (no creation ref) or already released — nothing to do
		}
		if v.refs.CompareAndSwap(n, n-1) {
			if n-1 == 0 {
				p.enqueueReclaim(tip) // last ref released — reclaim candidate
			}
			break
		}
	}
	return v.eff, true
}

// recordColdEviction counts one key evicted by the index's bounded sweep under
// memory pressure. Called once per victim from onLeafEvicted — the cold-key
// eviction the old conflated counter never saw.
func (p *VertexPool) recordColdEviction() { p.coldEvictions.Add(1) }

// vertexTombstone marks a vertex reclaimUnreferenced has claimed for deletion.
// It is deeply negative so the refcount can never climb back to a live (>=0)
// value through ordinary increfs, and so incref can detect the claim and refuse
// to resurrect it. Reclaim CAS-claims 0 -> vertexTombstone, then deletes.
const vertexTombstone = math.MinInt32 / 2

// incref bumps the key-membership refcount of the vertex at tip, if resident.
// A no-op for a non-resident tip (e.g. an effect served from the wire-parse
// fallback that never entered the pool) — refcounting only governs pooled
// vertices.
//
// It must not resurrect a vertex reclaimUnreferenced has already claimed for
// deletion (refs < 0): the claim-then-delete is atomic with this CAS, so a
// concurrent reader that walks a just-claimed tip simply treats it as absent
// (the effect is still reachable via getEffect / a re-fetch). Without this
// guard, a read could incref between reclaim's refs==0 check and its delete,
// leaving the pool to free a vertex the read still references — a vertex that
// stays resident off the pool's books (Bytes() drifts below true memory).
func (p *VertexPool) incref(tip Tip) {
	if v, ok := p.m.Load(tip); ok {
		for {
			r := v.refs.Load()
			if r < 0 {
				return // claimed for reclamation; do not resurrect
			}
			if v.refs.CompareAndSwap(r, r+1) {
				return
			}
		}
	}
}

// decref lowers the key-membership refcount of the vertex at tip, if resident.
// A refcount that goes negative means a decref had no matching incref — a
// refcount-protocol bug that would let reclaimUnreferenced free a vertex the
// index or a cache still references (a missing read), or strand it forever
// (a leak). It is never correct, so it panics rather than silently underflowing.
func (p *VertexPool) decref(tip Tip) {
	if v, ok := p.m.Load(tip); ok {
		n := v.refs.Add(-1)
		if n < 0 {
			panic(fmt.Sprintf("vertex refcount underflow: tip=%v refs=%d (decref without matching incref)", tip, n))
		}
		if n == 0 {
			p.enqueueReclaim(tip) // last ref released — reclaim candidate
		}
	}
}

// Bytes returns the process's live heap as marked by the last GC
// (/gc/heap/live:bytes). This is the exact, total memory signal the governor
// triggers and sizes eviction on, and it counts everything that grows per key —
// decoded effect graphs in the pool, off-pool subdags/reduced memos/subscription
// state, the trie — not just the pool's slice. A protoLen-based pool sum
// undercounted each decoded graph ~3x and was blind to the off-pool half, so
// triggering on it let the heap reach 10GB while the pool read 3GB.
//
// It deliberately uses /gc/heap/live (live objects only) rather than HeapAlloc
// (/memory/classes/heap/objects), which is live + not-yet-collected garbage and
// sawtooths up to the GC goal (~2x live at GOGC=100). Triggering on HeapAlloc
// fired eviction on the garbage peaks while the true working set was under
// target. /gc/heap/live updates once per GC cycle — stable between cycles and,
// under a churning workload, fresh well within the governor's 1s tick. metrics.Read
// is cheap and non-STW.
func (p *VertexPool) Bytes() int64 {
	s := []metrics.Sample{{Name: "/gc/heap/live:bytes"}}
	metrics.Read(s)
	return int64(s[0].Value.Uint64())
}

// EntryCount returns the number of resident effects.
func (p *VertexPool) EntryCount() int { return p.m.Size() }

// Stats returns cumulative hit, miss, and reclaim counts. The third value is
// reclaimUnreferenced churn (vertices freed), not cache eviction — use
// ColdEvictions for keys dropped under memory pressure.
func (p *VertexPool) Stats() (hits, misses, reclaimed uint64) {
	return p.hits.Load(), p.misses.Load(), p.reclaimed.Load()
}

// ColdEvictions returns the number of keys the index's bounded sweep evicted
// under memory pressure — the real "evicted_keys", distinct from reclaim churn.
func (p *VertexPool) ColdEvictions() uint64 { return p.coldEvictions.Load() }

// Reclaimed returns the number of vertices freed by reclaimUnreferenced
// (below-LCA history and orphans) — storage churn, not cache eviction.
func (p *VertexPool) Reclaimed() uint64 { return p.reclaimed.Load() }

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
	// Eviction is only ever driven by this governor, so install the hooks here
	// rather than unconditionally: the bounded sweep pins system keys (never a
	// victim) and hands each evicted cold key's tips + leaf payload to
	// onLeafEvicted (which releases refs and unsubscribes). Installing the hooks
	// also activates the trie's per-read eviction bookkeeping, so a node without
	// a memory target pays none of that read-path cost.
	e.index.SetEvictHooks(
		func(key string) bool { return !isSystemKey([]byte(key)) },
		e.onLeafEvicted,
		// Live over-target signal: the governor sets evictBudget > 0 each tick it
		// measures live heap above target, and writers drain it back to 0. This is
		// the keytrie analogue of CloxCache's instantaneous entryCount >= capacity,
		// so a graduation is counted only while there is genuinely room to reclaim.
		func() bool { return e.evictBudget.Load() > 0 },
	)
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
			// 1. Drain any budget the write path didn't (idle / low write rate), so
			//    a quiet node still converges to target. Each eviction only drops the
			//    victim's tip and queues its ref-release walk (releaseQueue) — the
			//    walk runs in step 2, off the latency path.
			e.drainEvictBudget(maxDrainPerTick)
			// 2. Reclaim, entirely off the candidate queue — NO full-map scan, at any
			//    pressure level. Run the deferred ref-release walks for every key
			//    evicted since the last tick (inline back-pressure evictions plus the
			//    drain above), then free the vertices those walks and concurrent reads
			//    drove to refs==0. Every path to refs==0 enqueues (decref,
			//    decrefChainNode, born-at-zero PutSizedCache), so the queue is the
			//    complete set of reclaimable vertices — a rangeVertices sweep would
			//    only re-confirm what the queue already freed while pinning a core and
			//    stalling reads/writes. Reclaiming before measuring keeps Bytes() on
			//    the true working set (below-LCA history / orphans the GC still marks
			//    live are freed here first).
			e.drainPendingReleases()
			e.effectCache.drainReclaim()
			// 3. Re-measure and set the deficit for the write path to drain over the
			//    next second as back-pressure. If we are still over target after
			//    draining every reclaimable vertex, the live working set genuinely
			//    exceeds the budget: evict cold keys. Their chain release re-enqueues
			//    their vertices, which the next tick's drainReclaim frees — so sustained
			//    pressure converges through the queue, never through a full scan.
			//    Sized as overage / true per-key footprint (live heap / live keys).
			live := e.effectCache.Bytes()
			over := live - targetBytes
			if over <= 0 {
				e.evictBudget.Store(0)
				continue
			}
			keys := e.index.Size()
			if keys == 0 {
				e.evictBudget.Store(0)
				continue
			}
			avgPerKey := max(live/keys, 64)
			e.evictBudget.Store(min(over/avgPerKey, maxDrainPerTick))
		}
	}
}

// evictBatchSize is how many keys one EvictBatch call harvests per sweep when the
// governor drains in bulk. Inline write-path drains use a much smaller cap.
const evictBatchSize = 4096

// inlineEvictCap bounds how many keys a single write may evict while draining the
// budget, so back-pressure stays a steady per-insert tax rather than an
// occasional multi-thousand-key latency spike on one unlucky write.
const inlineEvictCap = 8

// maxDrainPerTick caps the deficit the governor sets (and bulk-drains) per tick so
// a runaway overage can't try to evict the whole cache at once; the remainder is
// re-measured and drained on following ticks.
const maxDrainPerTick = 1 << 18 // 262144

// backpressure is the write-path hook: an insert over the memory target pays for
// a small bounded slice of eviction before returning, so insertion throughput is
// clamped to eviction throughput — you cannot add faster than you make room. A
// cheap atomic-load no-op in the common under-target case (budget == 0).
func (e *Engine) backpressure() {
	if e.evictBudget.Load() > 0 {
		e.drainEvictBudget(inlineEvictCap)
	}
}

// drainEvictBudget evicts up to maxKeys keys, decrementing the shared evictBudget
// the governor set from the live-heap overage. It is the single drain path for
// both the governor (idle case, large maxKeys) and the write path (back-pressure,
// inlineEvictCap). Concurrent writers share the budget via the atomic, so a burst
// spreads the eviction work across the inserts causing it — insertion cannot
// outrun eviction because every insert over budget does the evicting itself.
//
// Stops early when the budget is exhausted (at/under target) or when a sweep finds
// nothing evictable (all pinned / empty window this pass); the persistent clock
// hand means the next call resumes elsewhere in the arena.
func (e *Engine) drainEvictBudget(maxKeys int) {
	for maxKeys > 0 {
		b := e.evictBudget.Load()
		if b <= 0 {
			return
		}
		want := int(min(b, int64(maxKeys), evictBatchSize))
		n := e.index.EvictBatch(want)
		if n == 0 {
			return // nothing evictable right now; leave the budget for next time
		}
		e.evictBudget.Add(-int64(n))
		maxKeys -= n
	}
	// Note: no reclaim here. EvictBatch already released each dropped leaf's tips,
	// subdag, reduced memo, and subscription synchronously (via the refDelta hook +
	// onLeafEvicted); the now-refs-0 pool vertices are enqueued as reclaim
	// candidates and freed by the governor's per-tick drainReclaim. A full pool
	// scan must never run on the inline write-path drain.
}

// drainPendingReleases runs the deferred creation-ref chain walks
// (releaseChainRefs) for keys cold-evicted since the last call. Single-consumer:
// only the governor goroutine calls it — the memoryGovernorLoop steady-state path
// and reclaimUnreferenced. It dequeues exactly releasePending entries — the count
// enqueued so far — so the blocking UMPSCQueue.Dequeue never waits on an empty
// queue; entries enqueued concurrently after the Swap are counted into the next
// drain.
func (e *Engine) drainPendingReleases() {
	n := e.releasePending.Swap(0)
	if n == 0 {
		return
	}
	q := e.releaseQueue.Load() // non-nil: pending>0 means onLeafEvicted ran releaseQ
	for ; n > 0; n-- {
		ek := q.Dequeue()
		e.releaseChainRefs(ek.key, ek.tips, ek.ls)
	}
}

// reclaimQ returns the reclaim-candidate queue, allocating it on first use. The
// CAS lets concurrent first-producers converge on a single queue; the loser's is
// dropped (GC'd) and it reloads the winner's. Mirrors Engine.releaseQ.
func (p *VertexPool) reclaimQ() *xsync.UMPSCQueue[Tip] {
	if q := p.reclaimQueue.Load(); q != nil {
		return q
	}
	q := xsync.NewUMPSCQueue[Tip]()
	if p.reclaimQueue.CompareAndSwap(nil, q) {
		return q
	}
	return p.reclaimQueue.Load()
}

// enqueueReclaim records a tip whose refcount just reached zero (or a born-at-zero
// cache put) as a reclaim candidate. Enqueue precedes the pending increment so the
// consumer's Swap-then-dequeue-n never observes a count for an item not yet in the
// queue — the releaseQueue/releasePending invariant.
func (p *VertexPool) enqueueReclaim(tip Tip) {
	p.reclaimQ().Enqueue(tip)
	p.reclaimPending.Add(1)
}

// claimAndDelete frees the vertex at tip iff it is still a reclaimable refs==0
// non-system vertex, returning the bytes freed (0 if it survived). The
// CAS(0→vertexTombstone) is the membership oracle: it fails if a concurrent incref
// or owned re-Put bumped refs above 0 since the candidate was enqueued/observed —
// in which case the vertex is referenced again and must survive. incref treats a
// successful claim as "absent" and refuses to resurrect it, so delete after the
// claim is safe. Without the CAS this check-then-delete races incref and frees a
// referenced vertex. System keys are pinned defensively. Shared by the queue drain
// and the full scan.
func (p *VertexPool) claimAndDelete(tip Tip, v *vertex) int64 {
	if v.eff != nil && isSystemKey(v.eff.Key) {
		return 0
	}
	if !v.refs.CompareAndSwap(0, vertexTombstone) {
		return 0
	}
	return p.delete(tip)
}

// drainReclaim frees every queued reclaim candidate, returning the bytes freed.
// This is the whole reclaim mechanism — the governor calls it every tick at any
// pressure level (and reclaimUnreferenced drives it for tests): O(transitions
// since the last tick), touching only refs==0 candidates, with no full-map scan
// and no sustained per-bucket lock, so it never pins a core or stalls the request
// path the way the old unconditional rangeVertices sweep did. Single-consumer
// (the governor goroutine): the Swap-then-dequeue is not safe to run from two
// goroutines at once. A stale candidate (already reclaimed, re-referenced, or
// re-put as owned) is a cheap no-op via claimAndDelete. It dequeues exactly
// reclaimPending entries — the count enqueued so far — so the blocking
// UMPSCQueue.Dequeue never waits on an empty queue; entries enqueued concurrently
// after the Swap are counted into the next drain.
func (p *VertexPool) drainReclaim() int64 {
	n := p.reclaimPending.Swap(0)
	if n == 0 {
		return 0
	}
	q := p.reclaimQueue.Load() // non-nil: pending>0 means a producer allocated it
	var total int64
	for ; n > 0; n-- {
		tip := q.Dequeue()
		if v, ok := p.m.Load(tip); ok {
			total += p.claimAndDelete(tip, v)
		}
	}
	return total
}

// reclaimUnreferenced runs the deferred creation-ref chain walks for keys
// cold-evicted since the last call, then frees every pool vertex now at refcount
// zero by draining the candidate queue — the same path the governor runs per
// tick, with no full-map scan. A vertex held by no tipset that reaches it and no
// cached subdag that adopted it is a released chain node, a reclaimable cache
// fetch, or a true orphan, and safe to free (getEffect re-fetches on the rare
// miss); every path to refs==0 enqueues it (decref, decrefChainNode, born-at-zero
// PutSizedCache), so the queue is the complete set of reclaimable vertices.
//
// This is the deterministic "reclaim everything queued now" entry the tests
// drive. Like the governor's drainReclaim it is single-consumer: a test must not
// call it concurrently with a running governor (no test does — the governor is
// only started when a memory target is configured, which the reclaim tests do
// not set).
func (e *Engine) reclaimUnreferenced() int64 {
	if e.effectCache == nil {
		return 0
	}
	e.drainPendingReleases()
	total := e.effectCache.drainReclaim()
	if total > 0 {
		slog.Debug("vertex pool: reclaimed unreferenced effects",
			"est_bytes_freed", total, "live_heap", e.effectCache.Bytes())
	}
	return total
}
