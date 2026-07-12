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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	clox "github.com/swytchdb/swytch/cache"
	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/crdt"
	"github.com/swytchdb/swytch/keytrie"
	"github.com/swytchdb/swytch/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// systemKeyPrefix is the reserved namespace for cluster-operational
// keys (membership, pubsub routing, etc.). The ONLY thing this prefix
// governs is cache pinning (see vertexpool.go): system-key vertices are
// never reclaim or eviction victims, because losing the membership chain
// under memory pressure would sever the node from the cluster. Everywhere
// else — filters, authority gates, bootstrap, flush — system keys are
// ordinary keys; per-key prefix scans on hot paths are how isSystemKey
// once ate half a CPU.
var systemKeyPrefix = []byte("__swytch:")

func isSystemKey(key []byte) bool {
	return bytes.HasPrefix(key, systemKeyPrefix)
}

// isServed reports whether this node is authoritative for key — subscribed to it
// or a system key. Effects on served keys are pooled "owned" (PutSized seeds a
// creation ref so a never-read key's chain survives reclaim until eviction);
// effects on un-served keys are pooled as reclaimable cache (PutSizedCache, no
// creation ref) because we fetch them only to read through (cross-key bind
// adjudication) and can refetch from a peer. Routing ingest/fetch through this
// keeps the invariant "an owned, creation-ref'd vertex is reachable from a tip we
// serve", so reclaim never strands an un-served fetch as a permanent leak.
func (e *Engine) isServed(key []byte) bool {
	_, sub := e.subscriptions.Load(string(key))
	return sub
}

// putIngested pools a fetched/ingested effect, choosing owned vs cache residency
// by whether we serve its key. The single routing point for every ingest path
// (HandleRemote, backfill, NACK causal-chain prefetch, getEffect's storeWireData).
func (e *Engine) putIngested(tip Tip, eff *pb.Effect, protoLen int) {
	if e.effectCache == nil {
		return
	}
	if e.isServed(eff.Key) {
		e.effectCache.PutSized(tip, eff, protoLen)
	} else {
		e.effectCache.PutSizedCache(tip, eff, protoLen)
	}
}

// bootstrapCollector collects NACKs during subscription bootstrapping.
// ensureSubscribed registers one per key and waits for NACKs from peers.
type bootstrapCollector struct {
	nacks chan *pb.NackNotify
}

// Engine is the central coordinator for the causal effect log.
// Lock-free: the log uses CAS, the index manages its own concurrency,
// and safety config is swapped atomically.
type Engine struct {
	// index is the per-key tip frontier. Its leaf payload is *leafState,
	// which carries the cached subdag (and, later, eviction metadata) for
	// each key — one structure, the trie, owns both the tips and the
	// derived read cache.
	index       *keytrie.Critbit[leafState]
	broadcaster Broadcaster // nil for standalone
	cloudReader CloudReader // nil when no cloud is configured (standalone/dev)

	nodeID pb.NodeID
	clock  *crdt.HLC

	// In-memory offset generator (no durable log)
	nextOff atomic.Uint64

	subscriptions *xsync.Map[string, *subscriptionState]

	safety atomic.Pointer[safetyMap]

	// Transaction state
	pendingTxns   *xsync.Map[keytrie.EffectRef, *pendingTxn]
	pendingTxTips *xsync.Map[keytrie.EffectRef, []Tip]
	txAbortCounts *xsync.Map[string, *atomic.Int32]

	// Adaptive serialization state (§5)
	rttProvider        PeerRTTProvider
	serializationState *xsync.Map[string, *keySerializationState]

	// Subscription bootstrapping: key → *bootstrapCollector
	pendingBootstraps *xsync.Map[string, *bootstrapCollector]

	// Per-node cluster key filters powering free read-misses. ownKeyFilter
	// holds the keys this node has data for; the cluster layer ships it to
	// each peer at connection establishment so peers can answer a read-miss
	// without subscribing. peerKeyFilters caches each peer's keys — bulk from
	// the connection handshake, real-time from inbound SubscriptionEffects
	// (see cuckoo.go and keyfilter.go). unfilteredPeers counts connected
	// peers whose bulk filter hasn't arrived: until it does, the peer is
	// presumed to hold every key. Guarded by keyFilterMu (plain map + mutex,
	// never sync.Map).
	keyFilterMu       sync.RWMutex
	ownKeyFilter      CuckooChain
	ownFilterVer      uint64
	ownFilterBytes    []byte // cached marshal of ownKeyFilter at ownFilterBytesVer
	ownFilterBytesVer uint64
	peerKeyFilters    map[pb.NodeID]*peerKeyFilter
	unfilteredPeers   int

	// Deserialized effect store — effects are immutable once written.
	// Replaces the per-offset CloxCache; eviction is driven by the engine
	// memory governor (see startMemoryGovernor), not the pool itself.
	effectCache *VertexPool

	// Memory governor lifecycle (RSS-driven below-LCA sweeps).
	memGovStop chan struct{}
	memGovWg   sync.WaitGroup

	// memTarget is the resolved byte budget the governor holds the pool under
	// (0 = unbounded). Exposed via MemoryTarget for heartbeat telemetry.
	memTarget int64

	// compressValues makes Emit store data values zstd-compressed inside the
	// effect (--compress). Write-side only: reading inflates unconditionally,
	// driven by the per-effect flag, never by this setting.
	compressValues bool

	// evictBudget is the number of keys the governor wants evicted, computed each
	// tick from the live-heap overage. The write path drains it inline (see
	// drainEvictBudget) so insertion is back-pressured by eviction — you cannot
	// add faster than you make room. The governor drains any leftover for the
	// idle/low-write case. Zero means at/under target: writes evict nothing.
	evictBudget atomic.Int64

	// releaseItems defers cold-evicted keys' creation-ref chain walk
	// (releaseChainRefs) off the eviction hot path. EvictBatch drops the victim's
	// tip synchronously (the act that bounds memory) and enqueues the key here;
	// the expensive resident-chain walk that releases its refs is bookkeeping the
	// governor drains at the top of each reclaimUnreferenced, then frees the
	// now-refs-0 vertices in the same pass — so memory frees on the same tick it
	// would have, with the walk off the write/read latency path. Many write-path
	// drains enqueue, the single governor consumes.
	//
	// This is deliberately a mutex+slice pair and NOT xsync's UMPSCQueue: that
	// queue never zeroes a dequeued slot and pools consumed segments fully
	// populated, so every evictedKey it ever carried — and the leafState → subdag
	// → effect-payload chain each one pins — stayed reachable for GC cycles after
	// being "drained". Under an eviction storm that phantom live set reached
	// gigabytes and locked the governor permanently over target. The buffers here
	// are explicitly cleared after every drain (see drainPendingReleases).
	releaseMu    sync.Mutex
	releaseItems []evictedKey
	releaseSpare []evictedKey

	closed atomic.Bool

	// Transaction ID counter
	txnIDCounter atomic.Uint64

	// Striped locks for per-key atomic operations
	locks [4096]sync.Mutex

	// FlushGeneration is incremented on each FlushIndex call.
	// Used by WATCH to detect FLUSHALL/FLUSHDB between WATCH and EXEC.
	flushGeneration atomic.Uint64

	// Notification callbacks — fired after effects are durable
	OnKeyDataAdded func(key string) // wake oldest waiter (data inserted)
	OnKeyDeleted   func(key string) // wake all waiters (key removed)
	OnFlushAll     func()           // wake all waiters across all keys

	// OnLocalEffect fires once for every effect this node mints and persists —
	// data/meta/bind via Context, verdict snapshots, subscriptions,
	// unsubscribes. This is the originator-uploads-its-own-writes hook for
	// cloud durability: exactly the locally-authored DAG, never a peer's
	// effects and never wire-only probes (discovery subscriptions, ephemeral
	// pub/sub, PUBLISH payloads — those bypass the mint path entirely). Called
	// synchronously on the emit path; implementations must not block.
	localEffectMu sync.RWMutex
	OnLocalEffect func(offset Tip, eff *pb.Effect)

	// Ephemeral pub/sub callbacks — fired from HandleRemote on
	// receive of wire-only effects that are never stored or indexed.
	// OnPubSubMessage delivers an inbound PUBLISH to local subscribers.
	OnPubSubMessage func(channel, payload []byte)
	// OnEphemeralSubscribe records / removes a remote peer's interest
	// in a routing key. unsubscribe=false means register, true means
	// drop. The routing key is the raw Effect.Key bytes — encoding is
	// the cluster router's concern, not the engine's.
	OnEphemeralSubscribe func(subscriberNodeID uint64, routingKey []byte, unsubscribe bool)

	// Horizon wait for bind visibility
	horizon *HorizonSet

	// ConsumedTips of remote binds we have ACKed/NACKed. flushTx's
	// pre-flight aborts when a new local bind's pre-tx tip is in here:
	// emitting from that fork point would commit DAG-concurrent with the
	// remote bind we already took a position on. Cache eviction is safe
	// because the stale-tip / new-foreign-tip checks in flushTx catch
	// the same divergence structurally.
	spokenBinds *clox.CloxCache[Tip, struct{}]

	// Anti-entropy
	antiEntropyStop chan struct{}
	antiEntropyWg   sync.WaitGroup

	// Debounced reconvergence — coalesces multiple peer recovery events
	// into a single background anti-entropy pass.
	reconvergeTrigger chan struct{}
}

// subscriptionState tracks a subscription's readiness.
// The ready channel is closed once bootstrapping completes.
type subscriptionState struct {
	ready      chan struct{}
	incomplete atomic.Bool // true if bootstrap saw unreachable effects
	closeOnce  sync.Once   // guards close(ready)
}

// markReady unblocks any waiters on this subscription state. Safe to
// call multiple times; subsequent calls are no-ops.
func (s *subscriptionState) markReady() {
	s.closeOnce.Do(func() { close(s.ready) })
}

// markFailed flips the state to incomplete and unblocks waiters. They
// re-check incomplete after the wait and return ErrBootstrapIncomplete
// rather than incorrectly interpreting a closed ready channel as
// success.
func (s *subscriptionState) markFailed() {
	s.incomplete.Store(true)
	s.markReady()
}

// NewTestEngine creates a minimal Engine for use in tests outside this package.
func NewTestEngine() *Engine {
	return NewEngine(EngineConfig{
		NodeID: 1,
	})
}

// KeyCount returns the number of keys in the index.
func (e *Engine) KeyCount() int64 {
	return e.index.Size()
}

// ScanKeys iterates over keys matching the glob pattern, starting after `after`.
// Pass empty string for `after` to start from the beginning.
// Return false from fn to stop iteration.
func (e *Engine) ScanKeys(after string, pattern string, fn func(key string) bool) {
	ranger := e.index.RangeFrom
	if after == "" {
		ranger = func(_ string, fn func(string) bool) { e.index.Range(fn) }
	}
	if pattern == "*" {
		ranger(after, fn)
		return
	}
	ranger(after, func(key string) bool {
		if keytrie.MatchGlob(key, pattern) {
			return fn(key)
		}
		return true
	})
}

// MatchKeys returns all keys matching the glob pattern.
// Uses a point-in-time snapshot for consistency.
func (e *Engine) MatchKeys(pattern string) []string {
	snap := e.index.Snapshot()
	return snap.MatchPattern(pattern)
}

// swytchEpoch is Jan 7, 2026 00:00:00 UTC.
var swytchEpoch = time.Date(2026, 1, 7, 0, 0, 0, 0, time.UTC)

// nextOffset returns a globally unique (nodeID, seq) Tip for the next effect.
func (e *Engine) nextOffset() Tip {
	seq := e.nextOff.Add(1)
	return Tip{uint64(e.nodeID), seq}
}

// FlushKey is the special key used to signal a full index wipe (FLUSHDB/FLUSHALL).
// Lives under the __swytch: namespace so it's pinned in the cache and can't
// collide with a user-chosen key.
const FlushKey = "__swytch:flush"

// FlushIndex deletes all keys from the index and evicts all cache entries.
func (e *Engine) FlushIndex() {
	e.flushGeneration.Add(1)
	slog.Info("FlushIndex: wiping all keys from index")
	keys := e.index.Keys()
	for _, key := range keys {
		tips := e.index.Contains(key)
		if tips != nil {
			// DeleteAndSnapshot bypasses the eviction notify, so this is the only
			// refcount-release signal for flush (see releaseChainRefs).
			ls, _ := e.index.LoadOrStoreData(key, &leafState{})
			e.releaseChainRefs(key, tips.Tips(), ls)
		}
		e.index.Delete(key, tips)
	}
}

// GetLock returns a striped lock for the given key using FNV-1a hashing.
func (e *Engine) GetLock(key string) *sync.Mutex {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	return &e.locks[hash&4095]
}

type safetyMap struct {
	defaultMode SafetyMode
	rules       []KeyRangeRule
}

func (sm *safetyMap) modeForKey(key string) SafetyMode {
	for _, rule := range sm.rules {
		if keytrie.MatchGlob(key, rule.Pattern) {
			return rule.Mode
		}
	}
	return sm.defaultMode
}

// NewEngine creates a new Engine from the given configuration.
func NewEngine(cfg EngineConfig) *Engine {
	e := &Engine{
		index:              keytrie.NewCritbit[leafState](),
		broadcaster:        cfg.Broadcaster,
		nodeID:             pb.NewNodeID(),
		clock:              crdt.NewHLC(),
		rttProvider:        cfg.RTTProvider,
		serializationState: initSerializationState(),
		subscriptions:      xsync.NewMap[string, *subscriptionState](),
		pendingTxns:        xsync.NewMap[keytrie.EffectRef, *pendingTxn](),
		pendingTxTips:      xsync.NewMap[keytrie.EffectRef, []Tip](),
		txAbortCounts:      xsync.NewMap[string, *atomic.Int32](),
		pendingBootstraps:  xsync.NewMap[string, *bootstrapCollector](),
		peerKeyFilters:     make(map[pb.NodeID]*peerKeyFilter),
		effectCache:        newVertexPool(),
		spokenBinds:        clox.NewCloxCache[Tip, struct{}](clox.ConfigFromCapacity(8192)),
		compressValues:     cfg.CompressValues,
	}

	// Creation refs are seeded at PutSized (one per tipset a vertex occupies) and
	// released by the key-removal walk (releaseChainRefs) on eviction/flush; read
	// adoption refs are maintained in publishSubdag. No per-transition trie hook
	// is needed — a superseded tip stays reachable and must keep its ref, so the
	// only refcount signals are creation, adoption, and whole-key removal.

	// Memory governor: keep process RSS near the target. Under pressure it
	// first reclaims refs-0 vertices (below-LCA / orphans), then evicts cold
	// keys whose active state still exceeds the budget. Percent-based uses a
	// fraction of available memory; absolute uses the exact byte budget from
	// --maxmemory. The governor also sets GOMEMLIMIT so the GC cooperates.
	var memTarget int64
	if cfg.MemoryLimitPercent > 0 && cfg.MemoryLimitPercent <= 1.0 {
		memTarget = int64(float64(clox.GetAvailableMemory()) * cfg.MemoryLimitPercent)
	} else if cfg.MemoryLimit > 0 {
		memTarget = cfg.MemoryLimit
	}
	e.memTarget = memTarget
	e.startMemoryGovernor(memTarget)

	// System keys ("__swytch:*": membership, flush, ...) are commutative
	// protocol metadata and must never be quorum-gated — a node has to be able
	// to register itself into a minority partition (you can't reach a majority
	// until you've joined, and can't join if joining needs a majority). Pin it
	// as the first rule so first-match-wins makes it an invariant no config can
	// override; the anchored pattern won't match user keys like "_foo".
	rules := append([]KeyRangeRule{{Pattern: "__swytch:*", Mode: UnsafeMode}}, cfg.KeyRangeRules...)
	e.safety.Store(&safetyMap{
		defaultMode: cfg.DefaultMode,
		rules:       rules,
	})

	if cfg.Broadcaster != nil {
		e.horizon = newHorizonSet(e)
	}

	return e
}

func (e *Engine) NodeID() pb.NodeID {
	return e.nodeID
}

// PinKey adds a dynamic do-not-evict hold on key; UnpinKey releases it. The
// cloud outbox is the caller: a key with un-acked uploads must stay findable —
// evicting it frees almost nothing (the outbox pins the effect bytes) while
// unsubscribing makes the committed data invisible to the whole cluster until
// the ack lands. Reports whether a live leaf carried the operation.
func (e *Engine) PinKey(key string) bool   { return e.index.Pin(key) }
func (e *Engine) UnpinKey(key string) bool { return e.index.Unpin(key) }

// makeTip constructs a full Tip from a local offset.
func (e *Engine) makeTip(offset uint64) Tip {
	return Tip{uint64(e.nodeID), offset}
}

// generateTxnID returns a globally unique transaction ID.
// Format: "nodeID:hlc:seq" — unique across reboots (HLC provides time component).
func (e *Engine) generateTxnID() string {
	seq := e.txnIDCounter.Add(1)
	return fmt.Sprintf("%d:%d:%d", e.nodeID, e.clock.Now().UnixNano(), seq)
}

// EffectCache returns the engine's deserialized effect cache for use by
// the fetch handler (serves effects from cache when the log is unavailable).
func (e *Engine) EffectCache() *VertexPool {
	return e.effectCache
}

// AverageK exposes the index's current eviction threshold for telemetry.
// k now lives on the critbit index (the vertex pool has no eviction policy of
// its own), so heartbeat stats read it from here rather than the effect cache.
func (e *Engine) AverageK() float64 {
	return e.index.EvictK()
}

// EvictStats exposes the index's adaptive-eviction internals for telemetry.
func (e *Engine) EvictStats() keytrie.EvictStats {
	return e.index.EvictStats()
}

// ArenaBytes returns the critbit index's slot-array footprint (the trie
// skeleton), distinct from the vertex pool's effect bytes (EffectCache().Bytes()).
// Exposed for telemetry so the two memory consumers can be compared directly.
func (e *Engine) ArenaBytes() int64 {
	if e.index == nil {
		return 0
	}
	return e.index.ArenaBytes()
}

// VertexCount returns the number of effects resident in the vertex pool.
func (e *Engine) VertexCount() int {
	if e.effectCache == nil {
		return 0
	}
	return e.effectCache.EntryCount()
}

// MemoryTarget returns the byte budget the governor holds the vertex pool
// under (0 = unbounded, no --maxmemory configured). The pool is byte-bounded
// rather than slot-bounded, so this is its capacity for heartbeat telemetry.
func (e *Engine) MemoryTarget() int64 {
	return e.memTarget
}

// addPeerSubscriber records that peer is subscribed to key. Idempotent.
// Called from ensureSubscribed (peers that NACK back during bootstrap)
// and HandleRemote (foreign SubscriptionEffect arrival). The subscriber set
// lives on the key's leaf; if the key has no leaf (never read/written or
// already evicted), there is nothing to subscribe to and the call is a no-op.
func (e *Engine) addPeerSubscriber(key string, peer pb.NodeID) {
	if peer == e.nodeID {
		return
	}
	ls, _ := e.index.LoadOrStoreData(key, &leafState{})
	if ls == nil {
		return
	}
	ls.subMu.Lock()
	if ls.subscribers == nil {
		ls.subscribers = make(map[pb.NodeID]struct{})
	}
	ls.subscribers[peer] = struct{}{}
	ls.subMu.Unlock()
}

// removePeerSubscriber removes peer from the subscriber set for key.
// Called from HandleRemote on a foreign unsubscribe SubscriptionEffect.
func (e *Engine) removePeerSubscriber(key string, peer pb.NodeID) {
	ls, _ := e.index.LoadOrStoreData(key, &leafState{})
	if ls == nil {
		return
	}
	ls.subMu.Lock()
	delete(ls.subscribers, peer)
	ls.subMu.Unlock()
}

// snapshotSubscribers returns the key's current subscriber set in the proto
// map form, for stamping into a compaction SnapshotEffect's reduced state so
// the set survives chain compaction and is available to a bootstrapping peer.
// Returns nil when there are no subscribers. Sourced from the authoritative
// leafState set — no DAG walk.
func (e *Engine) snapshotSubscribers(key string) map[uint64]bool {
	ls, _ := e.index.LoadOrStoreData(key, &leafState{})
	if ls == nil {
		return nil
	}
	ls.subMu.Lock()
	defer ls.subMu.Unlock()
	if len(ls.subscribers) == 0 {
		return nil
	}
	m := make(map[uint64]bool, len(ls.subscribers))
	for id := range ls.subscribers {
		m[uint64(id)] = true
	}
	return m
}

// PeerSubscribers returns the current subscriber set for key. The returned
// slice is a snapshot; callers may iterate without holding any lock. It is
// unfiltered by membership — flushTx's collectSubscribers applies the
// current-membership filter at broadcast time.
func (e *Engine) PeerSubscribers(key string) []pb.NodeID {
	ls, _ := e.index.LoadOrStoreData(key, &leafState{})
	if ls == nil {
		return nil
	}
	ls.subMu.Lock()
	defer ls.subMu.Unlock()
	if len(ls.subscribers) == 0 {
		return nil
	}
	result := make([]pb.NodeID, 0, len(ls.subscribers))
	for id := range ls.subscribers {
		result = append(result, id)
	}
	return result
}

// DropPeer releases per-peer cached state when a peer permanently leaves the
// cluster. Wired into PeerManager.SetPeerLifecycleHooks on the onRemoved hook
// (which fires only on genuine membership removal, not transient
// unreachability). Subscriber sets are NOT scrubbed here: they live in per-key
// leafState (reclaimed on leaf eviction) and are filtered against current
// membership at broadcast time, so a departed peer's id simply falls out of
// collectSubscribers. The key filter, by contrast, is real per-peer cached
// data that cannot be lazily re-derived, so it must be dropped explicitly.
func (e *Engine) DropPeer(peer pb.NodeID) {
	e.removePeerKeyFilter(peer)
}

// SetBroadcaster sets the broadcaster for replicating effects to peers.
// Must be called before any Emit/Flush if cluster mode is desired.
//
// Also lazy-inits the horizon set if not already present: NewEngine
// gates horizon on cfg.Broadcaster != nil, but the bootstrap path in
// beacon/runtime.go constructs the engine first (with a nil
// Broadcaster) and wires the PeerManager as broadcaster afterwards
// (chicken-and-egg: PeerManager needs engine to build the effect
// handler). Without this lazy init, horizon stays nil for the life
// of the engine and bind-arrival visibility isn't deferred — peers
// see aborting-txn effects before fork-choice has settled, which
// surfaces as Elle :incompatible-order on cross-node reads.
func (e *Engine) SetBroadcaster(b Broadcaster) {
	e.broadcaster = b
	if e.horizon == nil && b != nil {
		e.horizon = newHorizonSet(e)
	}
}

// SetRTTProvider sets the RTT provider for adaptive serialization leader selection.
func (e *Engine) SetRTTProvider(p PeerRTTProvider) {
	e.rttProvider = p
}

// SetCloudReader installs the Cloud read backstop consulted on a read-miss. Nil
// leaves the engine free-missing as before (standalone / no cloud).
func (e *Engine) SetCloudReader(r CloudReader) {
	e.cloudReader = r
}

// cloudTipsBackstop caps a read-miss's Cloud GetTips round-trip. Liveness is
// keepalive's job — the cloud client pings both directions, so a dead or
// half-open connection errors out of the RPC on its own. This exists only for
// the case keepalive cannot see: a wedged-but-alive stream (healthy
// connection, stuck handler). It is deliberately generous because a tight
// deadline here treats slow as down — a cold dataplane rebuilding its store
// legitimately answers in seconds, and failing the read at 5s manufactured
// ErrCloudUnavailable errors for reads that were about to succeed.
const cloudTipsBackstop = 60 * time.Second

// ErrCloudUnavailable marks a read that could not be answered because the
// Cloud consult failed or returned a frontier we could not fully fetch. It
// must surface to the client as an error, never as a miss: Cloud provably
// holds data for the key, so answering "no such key" would be
// indistinguishable from data loss.
var ErrCloudUnavailable = errors.New("cloud unavailable")

// hydrateFromCloud is the tiered-storage rehydrate on a read-miss: it asks Cloud
// for key's tip frontier and installs it into the index (pulling blobs via
// FetchFromAny → CDN), so a subsequent reconstruct finds the key locally and
// the cluster owns it thereafter. installed reports whether the frontier
// landed in the index; consulted reports whether Cloud gave a complete
// answer — including "holds nothing" — so GetSnapshot can mark the leaf as
// authoritative and stop re-consulting for a key whose state reduces to nil.
//
// A transport error, a timeout, or a partially-unreachable frontier returns
// ErrCloudUnavailable — the read fails rather than fabricating a miss (or,
// worse, a subset of the frontier posing as the whole state). The install is
// all-or-nothing for the same reason: installing the reachable subset would
// let the NEXT read reconstruct partial state locally and skip the consult
// entirely. The leaf stays unconsulted on failure, so a later read retries.
func (e *Engine) hydrateFromCloud(key string) (installed, consulted bool, err error) {
	if e.cloudReader == nil {
		return false, false, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), cloudTipsBackstop)
	defer cancel()
	tips, sidecar, err := e.cloudReader.CloudTips(ctx, key)
	if err != nil {
		slog.Warn("cloud read-miss backstop: get tips failed; failing read", "key", key, "error", err)
		return false, false, fmt.Errorf("%w: get tips for %q: %w", ErrCloudUnavailable, key, err)
	}
	if len(tips) == 0 {
		return false, true, nil // Cloud holds nothing either: an authoritative miss.
	}
	if err := e.InstallCloudTips(key, tips, sidecar); err != nil {
		return false, false, err
	}
	return true, true, nil
}

// InstallCloudTips validates and installs a Cloud-provided tip frontier for
// key, merging it with whatever the index already holds (the DAG reduces the
// union; a tip that is an ancestor of an existing tip is redundant but
// harmless — the next write consumes and collapses it). Besides the read-miss
// rehydrate it serves the cloud-sync reconcile path: a read served from the
// outbox during a Cloud outage installs the missed Cloud frontier here once
// Cloud answers again.
//
// The install is all-or-nothing: the whole frontier is walk-validated before
// any tip lands in the index. Installing the reachable subset would let a
// subsequent read reconstruct partial state as if it were the whole answer.
// Unlike retryBootstrap's walkAndInstall, which is deliberately progressive.
func (e *Engine) InstallCloudTips(key string, tips []Tip, sidecar []CloudEffect) error {
	// Warm the cache with the closure effects GetTips delivered inline, so the
	// walk below runs locally instead of one WAN fetch per dep (n+1). Same
	// owned-vs-cache routing as every other ingest path; stragglers the
	// sidecar didn't carry are pulled by the walk itself.
	if e.effectCache != nil {
		for _, ce := range sidecar {
			if _, ok := e.effectCache.Get(ce.Tip); ok {
				continue
			}
			e.putIngested(ce.Tip, ce.Eff, ce.ProtoLen)
		}
	}
	for _, tip := range tips {
		rd := newDag(e, key, "")
		if walkErr := rd.walk([]Tip{tip}, func(*pb.Effect) error { return nil }); walkErr != nil {
			slog.Warn("cloud tip install: tip unreachable; installing nothing",
				"key", key, "tip", tip, "error", walkErr)
			return fmt.Errorf("%w: tip %v unreachable for %q: %w",
				ErrCloudUnavailable, tip, key, walkErr)
		}
	}
	e.installTips(key, tips)
	return nil
}

// fireLocalEffect invokes OnLocalEffect for a freshly-minted persisted effect.
func (e *Engine) fireLocalEffect(offset Tip, eff *pb.Effect) {
	e.localEffectMu.RLock()
	defer e.localEffectMu.RUnlock()
	if e.OnLocalEffect != nil {
		e.OnLocalEffect(offset, eff)
	}
}

// SetOnLocalEffect replaces the local-mint hook. It waits for callbacks already
// in flight, which gives lifecycle owners a clean boundary before draining or
// tearing down the hook's resources.
func (e *Engine) SetOnLocalEffect(hook func(offset Tip, eff *pb.Effect)) {
	e.localEffectMu.Lock()
	e.OnLocalEffect = hook
	e.localEffectMu.Unlock()
}

// UpdateSafetyRules atomically replaces the key-range safety configuration.
func (e *Engine) UpdateSafetyRules(defaultMode SafetyMode, rules []KeyRangeRule) {
	e.safety.Store(&safetyMap{
		defaultMode: defaultMode,
		rules:       rules,
	})
}

func (e *Engine) modeForKey(key string) SafetyMode {
	return e.safety.Load().modeForKey(key)
}

type Tip = keytrie.EffectRef

// r converts a protobuf EffectRef to an internal Tip.
func r(ref *pb.EffectRef) Tip {
	return Tip{ref.NodeId, ref.Offset}
}

// toPbRef converts an internal Tip to a protobuf EffectRef.
func toPbRef(t Tip) *pb.EffectRef {
	return &pb.EffectRef{NodeId: t[0], Offset: t[1]}
}

// toPbRefs converts a slice of tips to protobuf EffectRefs.
func toPbRefs(tips []Tip) []*pb.EffectRef {
	refs := make([]*pb.EffectRef, len(tips))
	for i, t := range tips {
		refs[i] = toPbRef(t)
	}
	return refs
}

// fromPbRefs converts protobuf EffectRefs to internal tips.
func fromPbRefs(refs []*pb.EffectRef) []Tip {
	tips := make([]Tip, len(refs))
	for i, ref := range refs {
		tips[i] = r(ref)
	}
	return tips
}

// resolveTipDeps walks each tip back through pendingTxTips until it
// lands on a committed ancestor (a bind or a non-tx effect). SSI
// requires that every dep we hand a new effect references state that
// has already been committed somewhere in the cluster — never a tip
// belonging to a foreign in-flight transaction. Single-level
// substitution would leave us pointing at another tx-tipped offset of
// the same in-flight txn, which is what we're trying to avoid.
func (e *Engine) resolveTipDeps(tips []Tip) []Tip {
	seen := make(map[Tip]struct{}, len(tips))
	var resolved []Tip
	changed := false
	var walk func(Tip)
	walk = func(tp Tip) {
		if _, dup := seen[tp]; dup {
			return
		}
		seen[tp] = struct{}{}
		deps, ok := e.pendingTxTips.Load(tp)
		if !ok {
			resolved = append(resolved, tp)
			return
		}
		changed = true
		if len(deps) == 0 {
			// First write on this key; no committed predecessor exists.
			// Keep the tx tip — the new emit has nothing else to anchor
			// on, and reconstruct's tx-skip filtering handles it.
			resolved = append(resolved, tp)
			return
		}
		for _, d := range deps {
			walk(d)
		}
	}
	for _, tp := range tips {
		walk(tp)
	}
	if !changed {
		return tips
	}
	return resolved
}

// emitSnapshot writes a verdict-carrying SnapshotEffect on `key`, chained
// via prev_snapshot to the latest existing snapshot tip. Deps are the
// current tips with pending-tx tips substituted; the index is updated
// and the effect is broadcast.
func (e *Engine) emitSnapshot(key string, verdicts map[string]pb.Verdict) error {
	hlc := timestamppb.New(e.clock.Now())
	eff := &pb.Effect{
		Key:            []byte(key),
		Hlc:            hlc,
		NodeId:         uint64(e.nodeID),
		ForkChoiceHash: ComputeForkChoiceHash(e.nodeID, hlc),
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
			TxnVerdicts: verdicts,
		}},
	}

	currentSet := e.index.Contains(key)
	if currentSet != nil {
		tips := currentSet.Tips()
		for _, t := range tips {
			cached, err := e.getEffect(key, t)
			if err != nil {
				continue
			}
			if cached.GetSnapshot() != nil {
				eff.GetSnapshot().PrevSnapshot = toPbRef(t)
				break
			}
		}
		eff.Deps = toPbRefs(e.resolveTipDeps(tips))
	}

	data, err := MarshalEffect(eff)
	if err != nil {
		return err
	}
	offset := e.nextOffset()
	if e.effectCache != nil {
		e.effectCache.PutSized(offset, eff, len(data))
	}
	e.updateIndex(key, currentSet, offset)
	e.fireLocalEffect(offset, eff)

	if e.broadcaster != nil {
		notify := BuildOffsetNotify(e.nodeID, offset, eff, data, context.Background())
		e.broadcaster.BroadcastWithData(notify, data)
	}

	slog.Debug("emitSnapshot",
		"key", key,
		"offset", offset,
		"verdicts", len(verdicts),
		"prev_snapshot", eff.GetSnapshot().PrevSnapshot)
	return nil
}

// applySnapshotVerdicts promotes every txn named in a SnapshotEffect's
// verdict map out of horizon. The verdict snapshot is emitted by the
// originator only after commitPendingTxn returns (every subscriber
// responded, isRealConflict cleared), so its arrival is strictly stronger
// evidence than the 1×RTT timer that any concurrent competitor has had
// its chance. MakeVisible is idempotent — txns not in horizon are a
// no-op, and the timer-driven path remains as a crash fallback.
func (e *Engine) applySnapshotVerdicts(eff *pb.Effect) {
	if e.horizon == nil {
		return
	}
	snap := eff.GetSnapshot()
	if snap == nil || len(snap.TxnVerdicts) == 0 {
		return
	}
	for txnID := range snap.TxnVerdicts {
		e.horizon.MakeVisible(txnID)
	}
}

// lookupSnapshotVerdict walks the snapshot chain on `key` for `txnID`'s
// adjudication outcome. Used by arrival-time fork-choice sites to skip a
// competing bind a prior snapshot already locked in as LOST.
func (e *Engine) lookupSnapshotVerdict(key, txnID string) (pb.Verdict, bool) {
	tipSet := e.index.Contains(key)
	if tipSet == nil {
		return pb.Verdict_VERDICT_UNSPECIFIED, false
	}
	visited := make(map[Tip]bool)
	stack := append([]Tip(nil), tipSet.Tips()...)
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
		if snap := eff.GetSnapshot(); snap != nil {
			if v, ok := snap.TxnVerdicts[txnID]; ok && v != pb.Verdict_VERDICT_UNSPECIFIED {
				return v, true
			}
			if snap.PrevSnapshot != nil {
				stack = append(stack, r(snap.PrevSnapshot))
			}
			for _, dep := range eff.Deps {
				stack = append(stack, r(dep))
			}
			continue
		}
		if bind := eff.GetTxnBind(); bind != nil {
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
	return pb.Verdict_VERDICT_UNSPECIFIED, false
}

// HandleRemote processes a remote effect notification: stores the effect
// in the log, updates the index, and returns NACKs if deps don't match tips.
//
// EffectData may be in wire format [4-byte LE keyLen][key][protoData] or
// raw proto bytes. Both are handled transparently.
//
// Returns all NACKs generated (one per diverged key) so the caller can
// send them synchronously as the ReplicateTo response.
func (e *Engine) HandleRemote(notify *pb.OffsetNotify) ([]*pb.NackNotify, error) {
	if notify == nil || notify.Origin == nil {
		return nil, nil
	}

	if tracing.Enabled() {
		remoteCtx := tracing.ExtractFromBytes(notify.GetTraceContext())
		_, handleSpan := tracing.Tracer().Start(remoteCtx, "effects.handle_remote",
			trace.WithAttributes(
				attribute.String("effect.key", string(notify.GetKey())),
				attribute.Int64("effect.offset", int64(notify.GetOrigin().GetOffset())),
				attribute.Int("effect.node_id", int(notify.GetOrigin().GetNodeId())),
			))
		defer handleSpan.End()
	}

	slog.Debug("HandleRemote: received",
		"offset", notify.Origin.Offset,
		"node", notify.Origin.NodeId,
		"key", notify.Key)

	effectData := notify.EffectData
	if len(effectData) == 0 {
		// Need to fetch from the originator
		if e.broadcaster == nil {
			return nil, nil
		}
		slog.Debug("HandleRemote: fetching missing effect data", "offset", notify.Origin)
		var err error
		effectData, err = e.broadcaster.FetchFromAny(notify.Origin, e.fetchHint(string(notify.Key)))
		if err != nil {
			return nil, err
		}
	}

	// Parse wire format: [4-byte LE keyLen][key][protoData]
	// If it doesn't look like wire format, treat as raw proto bytes.
	protoData := effectData
	if len(effectData) > 4 {
		keyLen := binary.LittleEndian.Uint32(effectData[:4])
		if keyLen > 0 && uint32(len(effectData)) >= 4+keyLen {
			protoData = effectData[4+keyLen:]
		}
	}

	// Deserialize to inspect
	eff := &pb.Effect{}
	if err := UnmarshalEffect(protoData, eff); err != nil {
		return nil, err
	}

	// Discovery probes (anti-entropy) are ephemeral wire-only messages —
	// handle them before fork_choice_hash validation and log storage since
	// they are never persisted and only need to trigger a NACK response.
	if sub := eff.GetSubscription(); sub != nil && sub.Discovery {
		key := string(eff.Key)
		slog.Debug("HandleRemote: discovery probe, sending NACK only",
			"key", key, "from_node", eff.NodeId)
		if e.broadcaster != nil {
			initialTips := e.index.Contains(key)
			var tipOffsets []keytrie.EffectRef
			if initialTips != nil {
				tipOffsets = initialTips.Tips()
			}
			nack := e.buildEnrichedNack(key, notify.Origin, tipOffsets)
			e.broadcaster.SendNack(nack, pb.NodeID(notify.Origin.NodeId))
		}
		return nil, nil
	}

	// Ephemeral pub/sub subscription announce — wire-only, never stored.
	// Receiver records the announcing peer's interest in a per-peer table
	// owned by the cluster router. No NACK, no index, no log.
	//
	// Identity is taken from notify.Origin.NodeId (the engine-wide origin
	// field) rather than the payload's redundant SubscriberNodeId. Neither
	// is authenticated against the QUIC peer ID at this boundary — the
	// engine trusts payload origin everywhere — but using Origin keeps
	// one source of truth and prevents a malformed payload from claiming
	// a different subscriber than its own origin.
	if sub := eff.GetSubscription(); sub != nil && sub.Ephemeral {
		if e.OnEphemeralSubscribe != nil {
			e.OnEphemeralSubscribe(notify.Origin.NodeId, eff.Key, sub.Unsubscribe)
		}
		return nil, nil
	}

	// Ephemeral pub/sub message — wire-only PUBLISH payload, delivered
	// directly to local subscribers. Never stored, never indexed, never
	// re-broadcast.
	if msg := eff.GetPubsubMessage(); msg != nil {
		if e.OnPubSubMessage != nil {
			e.OnPubSubMessage(msg.Channel, msg.Payload)
		}
		return nil, nil
	}

	// Populate the cluster key filter from the originating peer BEFORE the
	// authority gate below. That gate early-returns for keys we don't yet
	// track — which are exactly the keys a read-only handler needs the filter
	// to know about (a peer's brand-new key). Recording the originator's
	// effect on K here is a safe over-approximation: worst case a needless
	// subscribe, never a wrong free-miss. peerFilterAdd skips self.
	if origin := pb.NodeID(eff.NodeId); origin != e.nodeID {
		if bind := eff.GetTxnBind(); bind != nil {
			for _, kb := range bind.Keys {
				e.peerFilterAdd(origin, string(kb.Key))
			}
		} else if eff.GetData() != nil {
			e.peerFilterAdd(origin, string(eff.Key))
		} else if sub := eff.GetSubscription(); sub != nil && !sub.Unsubscribe {
			e.peerFilterAdd(origin, string(eff.Key))
		}
	}

	// Authority gate: accept the effect only if we already track the
	// key locally — either subscribed to it, or the index already has
	// tips (a previous local write installed them).
	//
	// SubscriptionEffects are exempt and respond with an empty ACK so
	// ensureSubscribed isn't deadlocked when nobody yet has authority
	// for a brand-new key. Flush-all is likewise exempt: it's a cluster
	// control message — nobody subscribes to FlushKey, and a flush must
	// reach every node regardless (the handler below wipes the index).
	//
	// TxnBinds touch multiple keys; collectSubscribers replicates them
	// to peers subscribed to any touched key, so the gate must accept
	// the bind when authority holds for any key in bind.Keys (not just
	// eff.Key, the canonical first key).
	{
		key := string(eff.Key)
		_, subscribed := e.subscriptions.Load(key)
		authoritative := subscribed || e.index.Contains(key) != nil

		if !authoritative {
			if bind := eff.GetTxnBind(); bind != nil {
				for _, kb := range bind.Keys {
					kbKey := string(kb.Key)
					if _, sub := e.subscriptions.Load(kbKey); sub || e.index.Contains(kbKey) != nil {
						authoritative = true
						break
					}
				}
			}
		}
		if !authoritative && key == FlushKey {
			authoritative = true
		}

		if !authoritative {
			if sub := eff.GetSubscription(); sub != nil {
				slog.Debug("HandleRemote: empty bootstrap response for no-authority subscription",
					"key", key, "offset", notify.Origin, "unsubscribe", sub.Unsubscribe)
				return nil, nil
			}
			slog.Debug("HandleRemote: dropping notify for key with no local authority",
				"key", key, "offset", notify.Origin)
			return []*pb.NackNotify{{Key: eff.Key, NotSubscribed: true}}, nil
		}
	}

	// Validate fork_choice_hash: must be present and correct on all effects.
	// This hash is the global tiebreaker for ALL effect ordering (merges,
	// winner selection, DAG sort, FWW). Rejecting missing or incorrect
	// hashes prevents a malicious peer from gaming deterministic ordering.
	expected := ComputeForkChoiceHash(pb.NodeID(eff.NodeId), eff.Hlc)
	if !bytes.Equal(eff.ForkChoiceHash, expected) {
		return nil, fmt.Errorf("fork_choice_hash missing or mismatch for offset %v", notify.Origin)
	}

	// Cache deserialized effect — owned if we serve its key, reclaimable cache
	// (no creation ref) if we only need it to read through (e.g. cross-key).
	e.putIngested(r(notify.Origin), eff, len(protoData))

	// Verdict-snapshot arrival ends horizon wait for every txn it adjudicates.
	e.applySnapshotVerdicts(eff)

	// Handle flush-all: wipe the entire index
	if string(eff.Key) == FlushKey {
		if data := eff.GetData(); data != nil && data.Op == pb.EffectOp_REMOVE_OP {
			slog.Info("HandleRemote: received flush-all from peer", "node", notify.Origin.NodeId)
			e.FlushIndex()
			if e.OnFlushAll != nil {
				e.OnFlushAll()
			}
			return nil, nil
		}
	}

	// Track in-progress transactional tips
	if eff.TxnId != "" && eff.GetTxnBind() == nil {
		slog.Debug("HandleRemote: tracking pending tx tip", "offset", notify.Origin, "deps", eff.Deps)
		e.pendingTxTips.Store(r(notify.Origin), fromPbRefs(eff.Deps))
	}

	// Handle bind: remove entries from pendingTxTips (or defer via horizon)
	if bind := eff.GetTxnBind(); bind != nil {
		slog.Debug("HandleRemote: processing remote bind", "offset", notify.Origin)
		e.handleRemoteBind(bind, r(notify.Origin), eff.TxnId)
	}

	key := string(eff.Key)

	slog.Debug("HandleRemote: updating index",
		"key", key, "offset", notify.Origin.Offset,
		"deps", eff.Deps)

	initialTips := e.index.Contains(key)

	_, canonicalIndexable := e.subscriptions.Load(key)

	// Consume deps: any dep that is a current tip becomes an ancestor.
	deps := eff.Deps
	if bind := eff.GetTxnBind(); bind != nil {
		for _, kb := range bind.Keys {
			if string(kb.Key) == key {
				deps = []*pb.EffectRef{kb.NewTip}
				break
			}
		}
	}
	if canonicalIndexable {
		// Consume deps and add the new tip in a single index transition.
		e.updateIndex(key, keytrie.NewTipSet(fromPbRefs(deps)...), r(notify.Origin))
		// Remote inserts pay their own eviction debt: a subscribed node that runs
		// no local commands never reaches Flush's back-pressure, so without this its
		// pool would grow unbounded on replication traffic and dump the whole debt
		// on the next read. Owned ingest (we serve this key) makes room as it lands.
		e.backpressure()
	}

	if bind := eff.GetTxnBind(); bind != nil {
		for _, kb := range bind.Keys {
			kbKey := string(kb.Key)
			if kbKey == key { // canonical handled above
				continue
			}
			if _, ok := e.subscriptions.Load(kbKey); !ok {
				continue
			}
			preTips := e.index.Contains(kbKey)
			var preOffsets []Tip
			if preTips != nil {
				preOffsets = preTips.Tips()
			}
			slog.Debug("HandleRemote: indexing bind for additional key (before)",
				"key", kbKey, "bind_offset", notify.Origin,
				"pre_tips", preOffsets)
			e.updateIndex(kbKey, nil, r(notify.Origin))
			postTips := e.index.Contains(kbKey)
			var postOffsets []Tip
			if postTips != nil {
				postOffsets = postTips.Tips()
			}
			slog.Debug("HandleRemote: indexing bind for additional key (after)",
				"key", kbKey, "bind_offset", notify.Origin,
				"post_tips", postOffsets)
		}
	}

	// Collect NACKs to return synchronously (replaces async SendNack calls).
	var nacks []*pb.NackNotify

	// Check if deps match tips → NACK if diverged.
	// For bind effects, check EACH key's ConsumedTips against that key's
	// current tips. A bind has no eff.Deps but carries per-key causal bases.
	if bind := eff.GetTxnBind(); bind != nil {
		for _, kb := range bind.Keys {
			kbKey := string(kb.Key)
			kbTips := e.index.Contains(kbKey)
			if kbTips == nil {
				continue
			}
			// Check for divergence: are there current tips that the bind
			// doesn't know about? The bind knows about its ConsumedTips
			// and its own effects (NewTip + the bind offset itself).
			// Any other tip means a concurrent write happened — NACK.
			known := make(map[Tip]bool, len(kb.ConsumedTips)+2)
			for _, ct := range kb.ConsumedTips {
				known[r(ct)] = true
			}
			known[r(kb.NewTip)] = true
			known[r(notify.Origin)] = true // the bind itself
			diverged := false
			for _, tip := range kbTips.Tips() {
				if !known[tip] {
					diverged = true
					break
				}
			}
			if diverged {
				slog.Debug("HandleRemote: bind has unknown tips, NACK",
					"key", kbKey, "consumed_tips", kb.ConsumedTips,
					"new_tip", kb.NewTip,
					"current_tips", kbTips.Tips())
				nacks = append(nacks, e.buildEnrichedNack(kbKey, notify.Origin, kbTips.Tips()))
			}
		}
	} else if initialTips != nil && len(eff.Deps) > 0 {
		preTips := initialTips.Tips()
		if !depsMatchTips(fromPbRefs(eff.Deps), preTips) {
			slog.Debug("HandleRemote: deps diverged, NACK",
				"key", key, "deps", eff.Deps, "tips", preTips)
			postTips := e.index.Contains(key)
			var postTipOffsets []Tip
			if postTips != nil {
				postTipOffsets = postTips.Tips()
			}
			nacks = append(nacks, e.buildEnrichedNack(key, notify.Origin, postTipOffsets))
		}
	}

	// Subscription bootstrapping: when a remote SubscriptionEffect arrives,
	// always NACK back with our current tips so the subscriber can fetch state.
	// Send pre-update tips (excluding the subscription itself).
	//
	// Also maintain the in-memory peerSubscribers map so flushTx doesn't
	// have to re-derive the subscriber set from the DAG (where reconstruct
	// can return Subscribers-less results for legitimate reasons).
	if sub := eff.GetSubscription(); sub != nil && eff.NodeId != uint64(e.nodeID) {
		if sub.Unsubscribe {
			e.removePeerSubscriber(key, pb.NodeID(eff.NodeId))
		} else {
			e.addPeerSubscriber(key, pb.NodeID(eff.NodeId))
			// The cluster key filter is populated for all inbound effects in a
			// single block above, before the authority gate — see there.
		}
		slog.Debug("HandleRemote: remote subscription, bootstrap NACK",
			"key", key, "from_node", eff.NodeId, "unsubscribe", sub.Unsubscribe)
		var tipOffsets []Tip
		if initialTips != nil {
			tipOffsets = initialTips.Tips()
		}
		nacks = append(nacks, e.buildEnrichedNack(key, notify.Origin, tipOffsets))
	}

	// Fire notification callbacks for remote effects
	if eff.TxnId != "" && eff.GetTxnBind() == nil {
		// In-progress transactional effect — skip notification until bind
	} else if bind := eff.GetTxnBind(); bind != nil {
		if e.horizon != nil {
			// Callbacks deferred to MakeVisible
		} else {
			// Bind effect: wake waiters for each bound key (spurious wakeups OK)
			for _, kb := range bind.Keys {
				e.ownFilterAdd(string(kb.Key))
				if e.OnKeyDataAdded != nil {
					e.OnKeyDataAdded(string(kb.Key))
				}
			}
		}
	} else if data := eff.GetData(); data != nil {
		// Non-transactional data effect
		switch data.Op {
		case pb.EffectOp_INSERT_OP:
			e.ownFilterAdd(key)
			if e.OnKeyDataAdded != nil {
				e.OnKeyDataAdded(key)
			}
		case pb.EffectOp_REMOVE_OP:
			if len(data.Id) == 0 {
				if e.OnKeyDeleted != nil {
					e.OnKeyDeleted(key)
				}
			} else if data.Collection == pb.CollectionKind_ORDERED || data.Collection == pb.CollectionKind_KEYED {
				// Element remove on ordered/keyed collection — chain-wake
				e.ownFilterAdd(key)
				if e.OnKeyDataAdded != nil {
					e.OnKeyDataAdded(key)
				}
			}
		}
	}

	return nacks, nil
}

// handleBackfill ingests an effect we've learned about indirectly — via a
// NACK's tip details, a recursive dep fetch, or anti-entropy. It does the
// index work HandleRemote does (cache, pendingTxTips, canonical update,
// cross-key bind indexing) but skips the horizon-wait entry: the bind has
// already been adjudicated at its originator and is past its wait at the
// peer that named it to us. Routing it through horizon would re-enter
// HorizonSet.Add for an already-visible txn, which is non-idempotent and
// makes the bind invisible again until the next timer fires.
func (e *Engine) handleBackfill(notify *pb.OffsetNotify) error {
	if notify == nil || notify.Origin == nil {
		return nil
	}

	effectData := notify.EffectData
	var eff *pb.Effect
	fromCache := false
	if len(effectData) == 0 {
		// Already resident? Skip the network FetchFromAny — this runs inside
		// the synchronous NACK-ingest loop that gates commit/abort, and the
		// effect is immutable so the cached bytes are identical. Backfill's
		// index/bind/pendingTxTips tail is idempotent, so re-running it on the
		// cached effect is safe.
		if e.effectCache != nil {
			if cached, ok := e.effectCache.Get(r(notify.Origin)); ok {
				eff, fromCache = cached, true
			}
		}
		if !fromCache {
			if e.broadcaster == nil {
				return nil
			}
			var err error
			effectData, err = e.broadcaster.FetchFromAny(notify.Origin, e.fetchHint(string(notify.Key)))
			if err != nil {
				return err
			}
		}
	}

	var protoData []byte
	if !fromCache {
		protoData = effectData
		if len(effectData) > 4 {
			keyLen := binary.LittleEndian.Uint32(effectData[:4])
			if keyLen > 0 && uint32(len(effectData)) >= 4+keyLen {
				protoData = effectData[4+keyLen:]
			}
		}
		eff = &pb.Effect{}
		if err := UnmarshalEffect(protoData, eff); err != nil {
			return err
		}
	}

	{
		key := string(eff.Key)
		_, subscribed := e.subscriptions.Load(key)
		authoritative := subscribed || e.index.Contains(key) != nil
		if !authoritative {
			if bind := eff.GetTxnBind(); bind != nil {
				for _, kb := range bind.Keys {
					kbKey := string(kb.Key)
					if _, sub := e.subscriptions.Load(kbKey); sub || e.index.Contains(kbKey) != nil {
						authoritative = true
						break
					}
				}
			}
		}
		if !authoritative {
			return nil
		}
	}

	expected := ComputeForkChoiceHash(pb.NodeID(eff.NodeId), eff.Hlc)
	if !bytes.Equal(eff.ForkChoiceHash, expected) {
		return fmt.Errorf("fork_choice_hash missing or mismatch for offset %v", notify.Origin)
	}

	if !fromCache {
		e.putIngested(r(notify.Origin), eff, len(protoData))
	}

	// Verdict-snapshot arrival ends horizon wait for every txn it adjudicates.
	e.applySnapshotVerdicts(eff)

	if eff.TxnId != "" && eff.GetTxnBind() == nil {
		e.pendingTxTips.Store(r(notify.Origin), fromPbRefs(eff.Deps))
	}

	if bind := eff.GetTxnBind(); bind != nil {
		for _, kb := range bind.Keys {
			e.pendingTxTips.Delete(r(kb.NewTip))
		}
	}

	key := string(eff.Key)
	_, canonicalIndexable := e.subscriptions.Load(key)

	deps := eff.Deps
	if bind := eff.GetTxnBind(); bind != nil {
		for _, kb := range bind.Keys {
			if string(kb.Key) == key {
				deps = []*pb.EffectRef{kb.NewTip}
				break
			}
		}
	}
	if canonicalIndexable {
		// Single index transition (consume deps + add arrival) so the tip
		// refcount stays balanced — see HandleRemote.
		e.updateIndex(key, keytrie.NewTipSet(fromPbRefs(deps)...), r(notify.Origin))
	}

	if bind := eff.GetTxnBind(); bind != nil {
		for _, kb := range bind.Keys {
			kbKey := string(kb.Key)
			if kbKey == key {
				continue
			}
			if _, ok := e.subscriptions.Load(kbKey); !ok {
				continue
			}
			e.updateIndex(kbKey, nil, r(notify.Origin))
		}
	}

	return nil
}

// handleRemoteBind processes a TransactionalBindEffect received remotely.
func (e *Engine) handleRemoteBind(bind *pb.TransactionalBindEffect, bindOffset Tip, txnID string) {
	if e.horizon != nil {
		// Remote-arrival: hold the bind invisible until the originator's
		// verdict snapshot arrives (applySnapshotVerdicts in the ingestion
		// path), with a crash-fallback timer as backstop. Without this
		// wait, a reader sees the bind, then later a competitor arrives
		// and reconstruct picks a different winner — the earlier read
		// becomes a retroactive lie.
		e.horizon.Add(txnID, bindOffset, bind)
		e.horizon.ScheduleMakeVisible(txnID, e.horizon.computeHorizonWait())
	} else {
		// Standalone: remove bound key tips from pendingTxTips immediately
		for _, kb := range bind.Keys {
			e.pendingTxTips.Delete(r(kb.NewTip))
		}
	}

	// Process abort_deps: remove those offsets and abort any local pending txn
	for _, abortedOffset := range bind.AbortDeps {
		aborted := r(abortedOffset)
		e.pendingTxTips.Delete(aborted)

		// Check if any local pending txn has this offset → abort it
		e.pendingTxns.Range(func(_ Tip, ptxn *pendingTxn) bool {
			for _, pk := range ptxn.keys {
				if pk.newTip == aborted {
					abortPendingTxn(ptxn)
					return false // stop ranging
				}
			}
			return true
		})
	}

	// Cache every consumed tip this bind forked from so flushTx's
	// pre-flight will abort a future local emit that forks from the
	// same point.
	if e.spokenBinds != nil {
		for _, kb := range bind.Keys {
			for _, ct := range kb.ConsumedTips {
				e.spokenBinds.Put(r(ct), struct{}{})
			}
		}
	}
}

// checkCompetingBinds walks the DAG fragment for each key the bind touches
// and returns the txnID of any bind whose NewTip on that key is not a
// structural ancestor of our ConsumedTips — i.e., a bind we know about
// that's space-like concurrent with our forthcoming bind. Returns "" if
// no such bind exists.
//
// Every bind in the DAG fragment counts: invisible (in-horizon), visible,
// and prior LOST adjudications alike. By the time flushTx reaches this
// check we've already ACK'd or NACK'd every bind we've ingested, and that
// response is the cluster-visible promise that we won't emit a structural
// competitor. Re-deriving "could I commit anyway?" from a partial view
// (horizon-only, losers-only, or index-tips-only) would let us
// retroactively change our mind — exactly what the "once you've spoken"
// invariant forbids.
//
// The DAG walk is the source of truth because the per-key index can lose
// in-horizon bind tips: emitSnapshot for an unrelated txn consumes all
// current tips and replaces them with its own snapshot tip, so an
// invisible bind that arrived between WATCH and EXEC can vanish from the
// index even though it's still alive in the DAG (reachable as a dep of
// the snapshot that consumed it).
//
// Predicate refinement: two binds whose row-write evidence proves disjoint
// element IDs on a KEYED collection coexist without aborting either.
func (e *Engine) checkCompetingBinds(bind *pb.TransactionalBindEffect, txnID string) string {
	for _, kb := range bind.Keys {
		k := string(kb.Key)
		ourNewTip := r(kb.NewTip)
		consumedTips := make([]Tip, 0, len(kb.ConsumedTips))
		for _, ct := range kb.ConsumedTips {
			consumedTips = append(consumedTips, r(ct))
		}

		tipSet := e.index.Contains(k)
		if tipSet == nil {
			continue
		}

		// Ancestor closure of our consumed tips on this key. A bind whose
		// NewTip on `k` lands in this set is structurally in our past.
		ourAncestors := make(map[Tip]struct{}, len(consumedTips)*8)
		ancestorStack := make([]Tip, 0, len(consumedTips))
		for _, t := range consumedTips {
			if _, dup := ourAncestors[t]; dup {
				continue
			}
			ourAncestors[t] = struct{}{}
			ancestorStack = append(ancestorStack, t)
		}
		for len(ancestorStack) > 0 {
			cur := ancestorStack[len(ancestorStack)-1]
			ancestorStack = ancestorStack[:len(ancestorStack)-1]
			eff, err := e.getEffect(k, cur)
			if err != nil {
				continue
			}
			for _, dep := range eff.Deps {
				dt := r(dep)
				if _, dup := ourAncestors[dt]; dup {
					continue
				}
				ourAncestors[dt] = struct{}{}
				ancestorStack = append(ancestorStack, dt)
			}
		}

		visited := make(map[Tip]bool)
		walkStack := append([]Tip(nil), tipSet.Tips()...)
		for len(walkStack) > 0 {
			t := walkStack[len(walkStack)-1]
			walkStack = walkStack[:len(walkStack)-1]
			if visited[t] {
				continue
			}
			visited[t] = true
			eff, err := e.getEffect(k, t)
			if err != nil {
				continue
			}

			if otherBind := eff.GetTxnBind(); otherBind != nil {
				for _, okb := range otherBind.Keys {
					if string(okb.Key) != k {
						continue
					}
					theirNewTip := r(okb.NewTip)
					if _, inAncestors := ourAncestors[theirNewTip]; inAncestors {
						walkStack = append(walkStack, theirNewTip)
						break
					}
					if txnID != "" && eff.TxnId != "" {
						conflict, bothHadEvidence := e.hasPredicateConflict(
							txnID, eff.TxnId, k,
							[]Tip{ourNewTip},
							[]Tip{theirNewTip, t})
						if bothHadEvidence && !conflict {
							walkStack = append(walkStack, theirNewTip)
							break
						}
					}
					return eff.TxnId
				}
				continue
			}

			if snap := eff.GetSnapshot(); snap != nil && snap.PrevSnapshot != nil {
				walkStack = append(walkStack, r(snap.PrevSnapshot))
			}
			for _, dep := range eff.Deps {
				walkStack = append(walkStack, r(dep))
			}
		}
	}
	return ""
}

// evaluateBindForkChoice returns true if the bind would lose at the
// originator's commit point against any existing competitor on its keys
// (hash-based fork choice with predicate-refinement and shared-base
// gating) or against a concurrent non-tx data effect (SSI). flushTx
// uses this to abort before emitting.
func (e *Engine) evaluateBindForkChoice(bind *pb.TransactionalBindEffect, bindOffset Tip, bindHash []byte, txnID string) bool {
	newEntry := &forkChoiceBindEntry{
		offset: bindOffset,
		hash:   bindHash,
		txnID:  txnID,
		keys:   make(map[string][]Tip, len(bind.Keys)),
		tips:   make(map[string]Tip, len(bind.Keys)),
	}
	for _, kb := range bind.Keys {
		k := string(kb.Key)
		newEntry.keys[k] = fromPbRefs(kb.ConsumedTips)
		newEntry.tips[k] = r(kb.NewTip)
	}

	for _, kb := range bind.Keys {
		k := string(kb.Key)
		tips := e.index.Contains(k)
		if tips == nil {
			continue
		}

		for _, tipOff := range tips.Tips() {
			if tipOff == bindOffset {
				continue
			}
			eff, err := e.getEffect(k, tipOff)
			if err != nil {
				continue
			}

			// Competing bind: fork-choice determines winner
			if otherBind := eff.GetTxnBind(); otherBind != nil {
				if eff.TxnId == txnID {
					continue // same transaction
				}
				// Already adjudicated as LOST by a snapshot? skip
				if v, ok := e.lookupSnapshotVerdict(k, eff.TxnId); ok && v == pb.Verdict_LOST {
					continue
				}

				otherEntry := &forkChoiceBindEntry{
					offset: tipOff,
					hash:   eff.ForkChoiceHash,
					txnID:  eff.TxnId,
					keys:   make(map[string][]Tip, len(otherBind.Keys)),
				}
				theirNewTips := make(map[string]Tip, len(otherBind.Keys))
				for _, okb := range otherBind.Keys {
					otherEntry.keys[string(okb.Key)] = fromPbRefs(okb.ConsumedTips)
					theirNewTips[string(okb.Key)] = r(okb.NewTip)
				}

				if bindsShareBase(newEntry, otherEntry) {
					// Predicate refinement: suppress the fork-choice
					// when both txs carry obs/row-write evidence on
					// this key and their predicates don't intersect
					// the other's writes. Fallback to shared-base
					// conflict when either side lacks evidence
					// (schema-only mutations, non-SQL binds, etc.).
					conflict, bothHadEvidence := e.hasPredicateConflict(
						txnID, eff.TxnId, k,
						[]Tip{newEntry.tips[k], bindOffset},
						[]Tip{theirNewTips[k], tipOff})
					if bothHadEvidence && !conflict {
						continue
					}
					if !ForkChoiceLess(newEntry.hash, otherEntry.hash) {
						return true
					}
				}
			}

			// Non-transactional data effect: SSI invalidation
			if eff.TxnId == "" && eff.GetData() != nil {
				// Check if this effect is concurrent with our bind on this key:
				// not in the ancestor set of our consumed tips for this key.
				consumed := fromPbRefs(kb.ConsumedTips)
				isAncestor := slices.Contains(consumed, tipOff)
				if !isAncestor {
					// Not a consumed tip — could be concurrent.
					// Check if it's a descendant of our bind (depends on us).
					// If it depends on our NewTip or bindOffset, it's sequential.
					isDescendant := false
					// Walk backward from the non-tx effect to see if it reaches our bind
					visited := make(map[Tip]bool)
					stack := []Tip{tipOff}
					zero := Tip{0, 0}
					for len(stack) > 0 {
						off := stack[len(stack)-1]
						stack = stack[:len(stack)-1]
						if off == bindOffset || off == r(kb.NewTip) {
							isDescendant = true
							break
						}
						if visited[off] || off == zero {
							continue
						}
						visited[off] = true
						depEff, err := e.getEffect(k, off)
						if err != nil {
							continue
						}
						stack = append(stack, fromPbRefs(depEff.Deps)...)
					}
					if !isDescendant {
						return true
					}
				}
			}
		}
	}
	return false
}

// HandleNack processes a NACK from a remote peer.
func (e *Engine) HandleNack(nack *pb.NackNotify) error {
	if nack == nil {
		return nil
	}

	slog.Debug("HandleNack: received", "key", nack.Key,
		"conflicting_offset", nack.Conflicting.GetOffset(),
		"tip_count", len(nack.Tips))

	// Forward to bootstrap collector if subscription bootstrapping is in progress
	if bc, ok := e.pendingBootstraps.Load(string(nack.Key)); ok {
		select {
		case bc.nacks <- nack:
		default: // channel full, drop
		}
	}

	// Pull every tip the peer mentions into our local DAG before deciding
	// anything else. The NACK is informational; we must consume it.
	e.ingestNackTips(nack)

	// Check if this NACK is for a transactional key
	var matchedTxn *pendingTxn
	e.pendingTxns.Range(func(_ Tip, ptxn *pendingTxn) bool {
		for _, pk := range ptxn.keys {
			if pk.key == string(nack.Key) {
				matchedTxn = ptxn
				return false
			}
		}
		return true
	})

	if matchedTxn == nil {
		slog.Debug("HandleNack: non-transactional, ignoring", "key", nack.Key)
		return nil
	}

	slog.Debug("HandleNack: transactional conflict check", "key", nack.Key, "bind_offset", matchedTxn.bindOffset)
	// Transactional NACK: smart conflict detection across all keys in the tx
	for _, detail := range nack.TipDetails {
		if e.isRealConflict(matchedTxn, string(nack.Key), detail) {
			abortPendingTxn(matchedTxn)
			return nil
		}
	}

	return nil
}

// buildEnrichedNack constructs a NackNotify advertising the subset
// of `tips` for which this node holds the bytes locally. A NACK is
// an authority claim that the receiver will walk, so tips whose
// bytes we don't hold are dropped — advertising them would propagate
// an unresolvable reference.
//
// Lookup is local-only: a remote fetch fallback would synchronously
// block the receive path with network I/O for every tip in every
// NACK.
func (e *Engine) buildEnrichedNack(key string, conflicting *pb.EffectRef, tips []keytrie.EffectRef) *pb.NackNotify {
	nack := &pb.NackNotify{
		Key:         []byte(key),
		Conflicting: conflicting,
	}

	if e.effectCache == nil {
		return nack
	}

	for _, tp := range tips {
		eff, ok := e.effectCache.Get(tp)
		if !ok {
			continue
		}

		nack.Tips = append(nack.Tips, toPbRef(tp))

		detail := &pb.NackTipDetail{
			Ref:             toPbRef(tp),
			Hlc:             eff.Hlc,
			IsTransactional: eff.TxnId != "",
			Deps:            eff.Deps,
		}
		if data := eff.GetData(); data != nil {
			detail.IsData = true
			detail.Collection = data.Collection
			detail.ElementId = data.Id
			detail.Op = data.Op
		}
		if bind := eff.GetTxnBind(); bind != nil {
			detail.IsBind = true
			detail.BindHlc = bind.TxnHlc
			detail.BindNodeId = bind.OriginatorNodeId
			detail.BindForkChoiceHash = ComputeForkChoiceHash(pb.NodeID(bind.OriginatorNodeId), bind.TxnHlc)
			for _, kb := range bind.Keys {
				detail.BindConsumedTips = append(detail.BindConsumedTips, &pb.KeyConsumedTips{
					Key:          kb.Key,
					ConsumedTips: kb.ConsumedTips,
				})
			}
		}
		nack.TipDetails = append(nack.TipDetails, detail)
	}

	nack.CausalChain = e.collectCausalChain(key, tips)

	return nack
}

// collectCausalChain walks the local DAG from tips to the LCA snapshot
// (BFS, same stop condition as dag.bfs) and returns every effect ref in
// the active path. The receiver can bulk-fetch all of them instead of
// discovering the chain one hop at a time.
func (e *Engine) collectCausalChain(key string, tips []keytrie.EffectRef) []*pb.EffectRef {
	if e.effectCache == nil || len(tips) == 0 {
		return nil
	}
	var zero Tip
	visited := make(map[Tip]bool, len(tips)*4)
	queue := make([]Tip, 0, len(tips))
	var chain []*pb.EffectRef

	for _, tp := range tips {
		if tp == zero || visited[tp] {
			continue
		}
		visited[tp] = true
		queue = append(queue, tp)
		chain = append(chain, toPbRef(tp))
	}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		eff, ok := e.effectCache.Get(cur)
		if !ok {
			continue
		}

		if snap := eff.GetSnapshot(); snap != nil && snap.State != nil && len(queue) == 0 {
			break
		}

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
			if dt == zero || visited[dt] {
				continue
			}
			visited[dt] = true
			queue = append(queue, dt)
			chain = append(chain, ref)
		}
	}

	return chain
}

// ingestNackTips fetches and processes effects referenced in a NACK's
// tip details so the local DAG catches up with the peer's view.
//
// When the NACK includes a CausalChain (the sender pre-walked its DAG
// from tips to LCA), all refs are fetched in parallel — no sequential
// dep discovery needed. Falls back to BFS dep-walking for old peers
// that don't populate the chain.
func (e *Engine) ingestNackTips(nack *pb.NackNotify) {
	if nack == nil || e.broadcaster == nil {
		return
	}

	if len(nack.CausalChain) > 0 {
		go e.ingestCausalChain(nack)
	}

	var zero Tip
	visited := make(map[Tip]bool)
	queue := make([]Tip, 0, len(nack.TipDetails))
	for _, d := range nack.TipDetails {
		if d == nil || d.Ref == nil {
			continue
		}
		queue = append(queue, r(d.Ref))
	}
	for len(queue) > 0 {
		off := queue[0]
		queue = queue[1:]
		if off == zero || visited[off] {
			continue
		}
		visited[off] = true
		notify := &pb.OffsetNotify{
			Origin: toPbRef(off),
			Key:    nack.Key,
		}
		if err := e.handleBackfill(notify); err != nil {
			slog.Debug("ingestNackTips: handleBackfill failed",
				"ref", off, "error", err)
			continue
		}
		if e.effectCache != nil {
			if cached, ok := e.effectCache.Get(off); ok {
				if snap := cached.GetSnapshot(); snap != nil && snap.State != nil && len(queue) == 0 {
					break
				}
				stack := fromPbRefs(cached.Deps)
				queue = append(queue, stack...)
			}
		}
	}
}

// ingestCausalChain bulk-fetches every ref in the NACK's pre-computed
// causal chain in parallel and caches the deserialized effects. Does
// NOT install tips — the synchronous ingestNackTips / walkAndInstall
// path handles that.
func (e *Engine) ingestCausalChain(nack *pb.NackNotify) {
	e.prefetchEffects(fromPbRefs(nack.CausalChain))
}

// prefetchEffects fetches every listed ref not already resident in parallel
// and caches the deserialized effects, so a subsequent dag.bfs finds
// everything locally instead of doing sequential FetchFromAny calls. Purely a
// cache warmer: it installs no tips, and a failed fetch is skipped — the walk
// that follows fetches stragglers itself.
func (e *Engine) prefetchEffects(refs []Tip) {
	if e.effectCache == nil || len(refs) == 0 {
		return
	}

	var pending int
	type fetchResult struct {
		ref  Tip
		data []byte
	}
	results := make(chan fetchResult, len(refs))

	// Bound the fan-out: a causal chain can name thousands of refs, and one
	// in-flight fetch per ref would dogpile peers/CDN. results is buffered to
	// len(refs), so a worker never blocks sending and always frees its slot.
	const prefetchParallelism = 16
	sem := semaphore.NewWeighted(prefetchParallelism)
	for _, off := range refs {
		if _, ok := e.effectCache.Get(off); ok {
			continue
		}
		if err := sem.Acquire(context.Background(), 1); err != nil {
			// Unreachable with a Background context; stop dispatching rather
			// than fetch unbounded.
			slog.Error("prefetchEffects: semaphore acquire failed", "error", err)
			break
		}
		pending++
		go func(ref *pb.EffectRef) {
			defer sem.Release(1)
			data, err := e.broadcaster.FetchFromAny(ref, PreferPeers)
			if err != nil {
				results <- fetchResult{}
				return
			}
			results <- fetchResult{ref: r(ref), data: data}
		}(toPbRef(off))
	}

	for range pending {
		res := <-results
		var zero Tip
		if res.ref == zero || len(res.data) == 0 {
			continue
		}
		protoData := res.data
		if len(res.data) > 4 {
			keyLen := binary.LittleEndian.Uint32(res.data[:4])
			if keyLen > 0 && uint32(len(res.data)) >= 4+keyLen {
				protoData = res.data[4+keyLen:]
			}
		}
		eff := &pb.Effect{}
		if err := UnmarshalEffect(protoData, eff); err != nil {
			continue
		}
		e.putIngested(res.ref, eff, len(protoData))
	}
}

// onLeafEvicted is the eviction-notify callback the index invokes after its
// bounded sweep soft-deletes a victim key's leaf (the cold-key teardown). This is
// the sole refcount-release site for cold eviction: the trie hook was retired
// (creation refs live at PutSized, adoption refs at publishSubdag), and unlike
// that hook this callback always carries the key and tips even when the leaf was
// never read (nil leafState) — the case the diff-based hook could not tell from a
// supersede. It releases the key's references, then unsubscribes cluster-wide so
// peers reduce us out of the subscriber set. Future reads re-subscribe and
// bootstrap fresh. The leaf is already soft-deleted by the sweep.
// evictedKey is a cold-evicted key queued for deferred creation-ref release; see
// Engine.releaseItems.
type evictedKey struct {
	key  string
	tips []Tip
	ls   *leafState
}

func (e *Engine) onLeafEvicted(key string, tips []Tip, ls *leafState) {
	if e.closed.Load() {
		return
	}

	// Defer the creation-ref chain walk to the governor: EvictBatch already
	// dropped the tip (the act that frees space), and the walk is bookkeeping that
	// must not sit on the write/read latency path that triggered this eviction.
	// The governor drains releaseItems at the top of reclaimUnreferenced, so the
	// vertices still free on the same tick.
	e.releaseMu.Lock()
	e.releaseItems = append(e.releaseItems, evictedKey{key: key, tips: tips, ls: ls})
	e.releaseMu.Unlock()

	// Count the real cold-key eviction here (once per victim) — the bounded
	// sweep chose this key under memory pressure. Distinct from the vertex
	// reclaim churn delete() counts.
	e.effectCache.recordColdEviction()

	// Async: the unsubscribe broadcast is network I/O and must not run under
	// the sweep lock. Future reads re-subscribe and bootstrap fresh.
	go e.broadcastUnsubscribe(key, tips)
}

// releaseChainRefs releases every refcount a key held once its tips are removed
// for good — cold eviction or flush. The cached subdag's nodes each hold one
// read-adoption ref (publishSubdag), released first. Then the key's full
// reachable resident chain is walked from tips, scoped to this key (at a bind
// follow only the bind's NewTip for this key, else follow deps), decref'ing each
// visited vertex's creation ref exactly once via a visited set — so a shared bind
// or diamond dep is released once per key, never per parent. Unlike a read's bfs
// this walk does NOT stop at a snapshot LCA: it follows a snapshot's own deps so
// below-snapshot history is released too. The whole key is gone, so releasing
// below-LCA nodes is unconditionally safe here — no unconverged fork can still
// need them. decrefChainNode no-ops on absent tips, so a partially-resident
// chain is safe.
func (e *Engine) releaseChainRefs(key string, tips []Tip, ls *leafState) {
	if e.effectCache == nil {
		return
	}
	// Terminal swap: after this, publishSubdag refuses to install into this
	// leafState, so no racing read can pin vertices on a leaf whose release
	// walk has already run. The sentinel also makes a double release (evict +
	// flush) a no-op.
	if ls != nil {
		if cs := ls.subdag.Swap(evictedSubdag); cs != nil && cs != evictedSubdag {
			for _, tip := range cs.topoOrder {
				e.effectCache.decref(tip)
			}
		}
	}
	visited := make(map[Tip]struct{}, len(tips)*8)
	stack := append([]Tip(nil), tips...)
	for len(stack) > 0 {
		t := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, seen := visited[t]; seen {
			continue
		}
		visited[t] = struct{}{}
		eff, ok := e.effectCache.decrefChainNode(t)
		if !ok {
			continue // not resident — nothing below to release
		}
		if bind := eff.GetTxnBind(); bind != nil {
			for _, kb := range bind.Keys {
				if string(kb.Key) == key {
					stack = append(stack, r(kb.NewTip))
					break
				}
			}
			continue
		}
		for _, dep := range eff.GetDeps() {
			stack = append(stack, r(dep))
		}
	}
}

// broadcastUnsubscribe announces that this node has given up authority on key
// by emitting an unsubscribe SubscriptionEffect as a proper DAG element (Deps
// consume the dropped tips so peers reduce us out of the subscribers map),
// then drops the local subscription.
func (e *Engine) broadcastUnsubscribe(key string, tips []Tip) {
	if e.closed.Load() {
		return
	}
	if e.broadcaster != nil {
		hlc := timestamppb.New(e.clock.Now())
		offset := e.nextOffset()
		deps := e.resolveTipDeps(tips)
		unsub := &pb.Effect{
			Key:            []byte(key),
			Hlc:            hlc,
			NodeId:         uint64(e.nodeID),
			Deps:           toPbRefs(deps),
			ForkChoiceHash: ComputeForkChoiceHash(e.nodeID, hlc),
			Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
				SubscriberNodeId: uint64(e.nodeID),
				Unsubscribe:      true,
			}},
		}
		if data, err := MarshalEffect(unsub); err == nil {
			// OnLocalEffect's contract is that the vertex is resident and owned
			// when the hook runs (the cloud outbox increfs it and panics on a
			// miss). This hand-rolled emit must install like rawEmit does. No
			// leaf will ever release the creation ref — the key is evicted and
			// unsubscribed — so it is dropped right after the hook: whoever
			// increfed there (or nobody) owns the vertex from here.
			if e.effectCache != nil {
				e.effectCache.PutSized(offset, unsub, len(data))
			}
			e.fireLocalEffect(offset, unsub)
			if e.effectCache != nil {
				e.effectCache.Decref(offset)
			}
			notify := BuildOffsetNotify(e.nodeID, offset, unsub, data, nil)
			e.broadcaster.BroadcastWithData(notify, notify.EffectData)
		} else {
			slog.Error("broadcastUnsubscribe: marshal failed", "key", key, "error", err)
		}
	}
	e.subscriptions.Delete(key)
	slog.Debug("onLeafEvicted: evicted cold key", "key", key)
}

// Close performs graceful shutdown of the engine and its background components.
func (e *Engine) Close() error {
	e.closed.Store(true)
	if e.antiEntropyStop != nil {
		close(e.antiEntropyStop)
		e.antiEntropyWg.Wait()
	}
	if e.memGovStop != nil {
		close(e.memGovStop)
		e.memGovWg.Wait()
	}
	if e.spokenBinds != nil {
		e.spokenBinds.Close()
	}
	return nil
}

// ReconvergeAllKeys triggers a debounced background anti-entropy pass
// to fetch any effects missed during the partition. Multiple calls
// within the debounce window are coalesced into a single pass.
// This replaces the previous approach of deleting all subscriptions,
// which caused a thundering herd of blocking re-bootstraps on the
// read path and cascading peer timeouts.
func (e *Engine) ReconvergeAllKeys() {
	if e.reconvergeTrigger == nil {
		return
	}
	// Non-blocking send — if a trigger is already pending, skip.
	select {
	case e.reconvergeTrigger <- struct{}{}:
	default:
	}
}

// StartAntiEntropy launches a background goroutine that periodically
// exchanges tips with peers and fetches missing effect chains.
// This ensures effects missed during partitions are eventually discovered
// without polluting the log with redundant subscription effects.
// Also starts the reconvergence debounce loop that coalesces peer
// recovery events into background anti-entropy passes.
func (e *Engine) StartAntiEntropy(interval time.Duration) {
	if e.broadcaster == nil {
		return
	}
	e.antiEntropyStop = make(chan struct{})
	e.reconvergeTrigger = make(chan struct{}, 1)
	e.antiEntropyWg.Add(1)
	// Periodic anti-entropy disabled: the reconvergence-on-peer-recovery
	// path handles partition recovery, and normal replication NACKs handle
	// divergence. The periodic sweep over all keys generates excessive
	// traffic and contributes to ACK timeout flapping.
	go e.reconvergeLoop()
	e.startSerializationRelease()
}

// reconvergeLoop waits for peer recovery triggers, debounces them over
// a 2-second window, then runs a single anti-entropy pass. This prevents
// cascading timeouts caused by the old approach of resetting all
// subscriptions on every peer recovery.
func (e *Engine) reconvergeLoop() {
	defer e.antiEntropyWg.Done()
	for {
		select {
		case <-e.antiEntropyStop:
			return
		case <-e.reconvergeTrigger:
		}

		// Debounce: wait 2s for additional recovery events to coalesce.
		debounce := time.NewTimer(2 * time.Second)
		draining := true
		for draining {
			select {
			case <-e.antiEntropyStop:
				debounce.Stop()
				return
			case <-e.reconvergeTrigger:
				// Another peer recovered — restart the debounce window.
				debounce.Reset(2 * time.Second)
			case <-debounce.C:
				draining = false
			}
		}

		if e.closed.Load() {
			return
		}
		slog.Info("reconverge: running anti-entropy after peer recovery")
		e.runAntiEntropy()
	}
}

func (e *Engine) antiEntropyLoop(interval time.Duration) {
	defer e.antiEntropyWg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-e.antiEntropyStop:
			return
		case <-ticker.C:
			if e.closed.Load() {
				return
			}
			e.runAntiEntropy()
		}
	}
}

// runAntiEntropy picks a random peer, asks for its tips on all keys
// we know about, and fetches any chains we're missing.
func (e *Engine) runAntiEntropy() {
	peers := e.broadcaster.PeerIDs()
	if len(peers) == 0 {
		return
	}
	keys := e.index.Keys()
	if len(keys) == 0 {
		return
	}

	for _, key := range keys {
		localTips := e.index.Contains(key)
		if localTips == nil {
			continue
		}

		// Send a subscription effect to trigger NACKs with remote tips.
		// Use the existing ensureSubscribed mechanism but only if not
		// already subscribed — this avoids log pollution.
		// Instead, directly send a lightweight probe and collect tips.
		e.probeAndFetchKey(key)
	}
}

// probeAndFetchKey sends a subscription probe to all peers for a single key,
// collects their tips via NACKs, and fetches any missing effect chains.
// The probe is NOT persisted to the log — it's a lightweight wire-only message.
func (e *Engine) probeAndFetchKey(key string) {
	hlc := timestamppb.Now()
	eff := &pb.Effect{
		Key:            []byte(key),
		Hlc:            hlc,
		NodeId:         uint64(e.nodeID),
		ForkChoiceHash: ComputeForkChoiceHash(e.nodeID, hlc),
		Kind: &pb.Effect_Subscription{Subscription: &pb.SubscriptionEffect{
			SubscriberNodeId: uint64(e.nodeID),
			Discovery:        true,
		}},
	}
	data, err := MarshalEffect(eff)
	if err != nil {
		return
	}

	notify := BuildOffsetNotify(e.nodeID, Tip{0, uint64(hlc.Seconds)}, eff, data, nil)

	collector := &bootstrapCollector{
		nacks: make(chan *pb.NackNotify, 64),
	}
	e.pendingBootstraps.Store(key, collector)
	defer e.pendingBootstraps.Delete(key)

	var alivePeers []pb.NodeID
	if e.rttProvider != nil {
		alivePeers = e.rttProvider.AlivePeerIDs()
	} else {
		alivePeers = e.broadcaster.PeerIDs()
	}
	if len(alivePeers) == 0 {
		return
	}

	var ackCount atomic.Int32
	var wg sync.WaitGroup
	for _, pid := range alivePeers {
		wg.Add(1)
		go func(pid pb.NodeID) {
			defer wg.Done()
			if _, err := e.broadcaster.ReplicateTo(notify, notify.EffectData, pid); err == nil {
				ackCount.Add(1)
			}
		}(pid)
	}
	wg.Wait()

	expected := int(ackCount.Load())
	if expected == 0 {
		return
	}

	// Collect tips from NACKs
	var allTipOffsets []Tip
	nackDeadline := time.After(5 * time.Second)
	received := 0
	for received < expected {
		select {
		case nack := <-collector.nacks:
			received++
			for _, tp := range nack.Tips {
				allTipOffsets = append(allTipOffsets, r(tp))
			}
		case <-nackDeadline:
			received = expected
		}
	}

	if len(allTipOffsets) == 0 {
		return
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

	// Filter out tips we already have locally
	localTips := e.index.Contains(key)
	var missing []Tip
	for _, off := range unique {
		if localTips == nil || !localTips.Contains(off) {
			missing = append(missing, off)
		}
	}
	if len(missing) == 0 {
		return
	}

	slog.Debug("anti-entropy: found missing tips", "key", key, "missing", len(missing))

	// Fetch full chain from missing tips and route each effect through
	// handleBackfill so the cross-key bind indexing runs. HandleRemote
	// would re-enter HorizonSet for already-visible binds, making them
	// invisible again until the next 1×RTT timer fires; backfilled effects
	// have already been adjudicated at their originator.
	fetched := make(map[Tip]bool)
	fetchStack := make([]Tip, len(missing))
	copy(fetchStack, missing)
	zero := Tip{0, 0}
	for len(fetchStack) > 0 {
		off := fetchStack[len(fetchStack)-1]
		fetchStack = fetchStack[:len(fetchStack)-1]
		if off == zero || fetched[off] {
			continue
		}
		fetched[off] = true

		if e.effectCache != nil {
			if cached, ok := e.effectCache.Get(off); ok {
				fetchStack = append(fetchStack, fromPbRefs(cached.Deps)...)
				continue
			}
		}

		fetchedData, fetchErr := e.broadcaster.FetchFromAny(toPbRef(off), e.fetchHint(key))
		if fetchErr != nil {
			continue
		}
		fetchedEff, parseErr := parseWireEffect(fetchedData)
		if parseErr != nil {
			continue
		}
		notify := &pb.OffsetNotify{
			Origin:     toPbRef(off),
			Key:        fetchedEff.Key,
			EffectData: fetchedData,
		}
		if herr := e.handleBackfill(notify); herr != nil {
			slog.Debug("anti-entropy: handleBackfill failed",
				"ref", off, "error", herr)
			continue
		}
		fetchStack = append(fetchStack, fromPbRefs(fetchedEff.Deps)...)
	}

	slog.Debug("anti-entropy: synced", "key", key, "fetched", len(fetched), "new_tips", len(missing))
}

// storeWireData parses wire-format bytes and caches the deserialized effect.
// Wire format: [4-byte LE keyLen][key][protoData].
func (e *Engine) storeWireData(offset Tip, wireData []byte) error {
	if len(wireData) <= 4 {
		return fmt.Errorf("wire data too short: %d bytes", len(wireData))
	}
	keyLen := binary.LittleEndian.Uint32(wireData[:4])
	if keyLen == 0 || uint32(len(wireData)) < 4+keyLen {
		return fmt.Errorf("invalid wire data format")
	}
	protoData := wireData[4+keyLen:]
	eff := &pb.Effect{}
	if err := UnmarshalEffect(protoData, eff); err != nil {
		return err
	}
	e.putIngested(offset, eff, len(protoData))
	return nil
}

// depsMatchTips checks if the effect's deps are a subset of current tips.
func depsMatchTips(deps []Tip, tips []Tip) bool {
	if len(tips) == 0 {
		return len(deps) == 0
	}
	tipSet := make(map[Tip]struct{}, len(tips))
	for _, t := range tips {
		tipSet[t] = struct{}{}
	}
	for _, d := range deps {
		if _, ok := tipSet[d]; !ok {
			return false
		}
	}
	return true
}

// incrementAbortCount increments the abort counter for a key and returns
// true if the serialization threshold has been exceeded.
func (e *Engine) incrementAbortCount(key string) bool {
	counter, _ := e.txAbortCounts.LoadOrStore(key, &atomic.Int32{})
	count := counter.Add(1)
	return int(count) >= DefaultSerializationThreshold
}

// resetAbortCount resets the abort counter for a key on successful commit.
func (e *Engine) resetAbortCount(key string) {
	if counter, ok := e.txAbortCounts.Load(key); ok {
		counter.Store(0)
	}
}
