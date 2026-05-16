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
	"encoding/binary"
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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// systemKeyPrefix is the reserved namespace for cluster-operational
// keys (membership, pubsub routing, etc.). Effects on these keys are
// pinned in the effect cache and bypass the inbound authority gate
// because they're load-bearing for protocol operation, not user data.
var systemKeyPrefix = []byte("__swytch:")

func isSystemKey(key []byte) bool {
	return bytes.HasPrefix(key, systemKeyPrefix)
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
	index       keytrie.KeyIndex
	cache       StateCache  // nil disables caching
	broadcaster Broadcaster // nil for standalone

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

	// Per-key dedupe for handleEviction. Multiple effects on the
	// same key can evict in close succession; only one goroutine
	// should run the broadcast + teardown for any given key.
	unsubInFlight *xsync.Map[string, struct{}]

	// Deserialized effect cache — effects are immutable once written
	effectCache *clox.CloxCache[keytrie.EffectRef, *pb.Effect]

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

	// Voided binds — txnIDs whose binds lost fork-choice or were
	// SSI-invalidated. Checked by filterTentativeEffects to skip
	// these binds without per-read DAG walks.
	// Backed by a low-capacity CloxCache to bound memory; eviction
	// is safe because reconstruct's wonTips map independently
	// resolves competing binds.
	voidedBinds *clox.CloxCache[string, struct{}]

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
		Index:  keytrie.New(),
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
// Lives under the __swytch: namespace so isSystemKey recognizes it as
// cluster-operational and the authority gate bypasses it on inbound,
// and so it can't collide with a user-chosen key.
const FlushKey = "__swytch:flush"

// FlushIndex deletes all keys from the index and evicts all cache entries.
func (e *Engine) FlushIndex() {
	e.flushGeneration.Add(1)
	slog.Info("FlushIndex: wiping all keys from index")
	keys := e.index.Keys()
	for _, key := range keys {
		e.index.Delete(key)
		if e.cache != nil {
			e.cache.Evict(key)
		}
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
	cache := cfg.Cache

	e := &Engine{
		index:              cfg.Index,
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
		unsubInFlight:      xsync.NewMap[string, struct{}](),
		effectCache: clox.NewCloxCache[keytrie.EffectRef, *pb.Effect](func() clox.Config {
			c := clox.ConfigFromMemorySize(effectCacheSize(cfg.MemoryLimit))
			c.CollectStats = true
			return c
		}()),
		voidedBinds: clox.NewCloxCache[string, struct{}](clox.ConfigFromCapacity(8192)),
	}
	e.effectCache.SetSizeFunc(func(_ keytrie.EffectRef, v *pb.Effect) int64 {
		return 16 + int64(proto.Size(v)) // 16 = sizeof(EffectRef=[2]uint64)
	})
	// Pin cluster-operational keys (any "__swytch:" prefix). Membership,
	// pubsub channel routing, pubsub pattern routing, and any future
	// system-owned key shares this namespace. Eviction of one of these
	// effects silently breaks cluster invariants (index claims a tip
	// we no longer hold bytes for), so the cache must skip them
	// during victim selection.
	e.effectCache.SetEvictDecider(func(_ keytrie.EffectRef, eff *pb.Effect) bool {
		if eff == nil {
			return true
		}
		return !isSystemKey(eff.Key)
	})
	// After-eviction hook: the cache lost the bytes for an effect, so
	// the local index can no longer honestly claim authority over its
	// tip. Drop the key + unsubscribe cluster-wide. Dispatched on a
	// fresh goroutine because evictNotify runs under shard.mu and
	// handleEviction takes other locks + does a network broadcast.
	e.effectCache.SetEvictNotify(func(ref keytrie.EffectRef, eff *pb.Effect) {
		go e.handleEviction(ref, eff)
	})
	e.cache = cache

	// Percent-based memory limit: delegate to cloxcache's live
	// enforcer rather than locking a static byte budget. The
	// enforcer re-evaluates available memory each tick so cgroup /
	// system changes propagate to cache capacity automatically.
	if cfg.MemoryLimitPercent > 0 && cfg.MemoryLimitPercent <= 1.0 {
		const avgEffectBytes = 512 // conservative; loop self-corrects from actual bytes
		e.effectCache.EnforceMemoryTarget(cfg.MemoryLimitPercent, avgEffectBytes)
	}

	e.safety.Store(&safetyMap{
		defaultMode: cfg.DefaultMode,
		rules:       cfg.KeyRangeRules,
	})

	if cfg.Broadcaster != nil {
		e.horizon = newHorizonSet(e)
	}

	return e
}

// effectCacheSize returns the byte budget for the deserialized effect cache.
func effectCacheSize(memoryLimit int64) uint64 {
	if memoryLimit <= 0 {
		return 10 * 1024 * 1024 // 10MB default
	}
	return uint64(memoryLimit)
}

func (e *Engine) NodeID() pb.NodeID {
	return e.nodeID
}

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
func (e *Engine) EffectCache() *clox.CloxCache[Tip, *pb.Effect] {
	return e.effectCache
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

	// Create a child span from the remote trace context so it appears
	// under replication.receive in the originator's trace tree.
	remoteCtx := tracing.ExtractFromBytes(notify.GetTraceContext())
	_, handleSpan := tracing.Tracer().Start(remoteCtx, "effects.handle_remote",
		trace.WithAttributes(
			attribute.String("effect.key", string(notify.GetKey())),
			attribute.Int64("effect.offset", int64(notify.GetOrigin().GetOffset())),
			attribute.Int("effect.node_id", int(notify.GetOrigin().GetNodeId())),
		))
	defer handleSpan.End()

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
		effectData, err = e.broadcaster.FetchFromAny(notify.Origin)
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

	// Authority gate: accept the effect only if we already track the
	// key locally — either subscribed to it, or the index already has
	// tips (a previous local write installed them). System keys are
	// exempt because cluster bootstrap depends on them arriving before
	// any subscription exists.
	//
	// SubscriptionEffects are exempt and respond with an empty ACK so
	// ensureSubscribed isn't deadlocked when nobody yet has authority
	// for a brand-new key.
	//
	// TxnBinds touch multiple keys; collectSubscribers replicates them
	// to peers subscribed to any touched key, so the gate must accept
	// the bind when authority holds for any key in bind.Keys (not just
	// eff.Key, the canonical first key).
	if !isSystemKey(eff.Key) {
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
			if sub := eff.GetSubscription(); sub != nil {
				slog.Debug("HandleRemote: empty bootstrap response for no-authority subscription",
					"key", key, "offset", notify.Origin, "unsubscribe", sub.Unsubscribe)
				return nil, nil
			}
			slog.Info("HandleRemote: dropping notify for key with no local authority",
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

	// Cache deserialized effect
	if e.effectCache != nil {
		e.effectCache.Put(r(notify.Origin), eff)
	}

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

	_, subscribedCanonical := e.subscriptions.Load(key)
	canonicalIndexable := subscribedCanonical || isSystemKey(eff.Key)

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
		if len(deps) > 0 {
			e.index.RemoveTips(key, fromPbRefs(deps))
		}
		e.updateIndex(key, nil, r(notify.Origin))
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
	if eff.GetSubscription() != nil && eff.NodeId != uint64(e.nodeID) {
		slog.Debug("HandleRemote: remote subscription, bootstrap NACK",
			"key", key, "from_node", eff.NodeId)
		var tipOffsets []Tip
		if initialTips != nil {
			tipOffsets = initialTips.Tips()
		}
		nacks = append(nacks, e.buildEnrichedNack(key, notify.Origin, tipOffsets))
	}

	// Invalidate cache — but NOT for bind effects when horizon is active
	// (deferred to horizon timer / MakeVisible)
	if eff.GetTxnBind() != nil && e.horizon != nil {
		// Cache eviction deferred to MakeVisible
	} else if e.cache != nil {
		e.cache.Evict(key)
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
				if e.OnKeyDataAdded != nil {
					e.OnKeyDataAdded(string(kb.Key))
				}
			}
		}
	} else if data := eff.GetData(); data != nil {
		// Non-transactional data effect
		switch data.Op {
		case pb.EffectOp_INSERT_OP:
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
				if e.OnKeyDataAdded != nil {
					e.OnKeyDataAdded(key)
				}
			}
		}
	}

	return nacks, nil
}

// handleRemoteBind processes a TransactionalBindEffect received remotely.
func (e *Engine) handleRemoteBind(bind *pb.TransactionalBindEffect, bindOffset Tip, txnID string) {
	if e.horizon != nil {
		// Remote-arrival: hold the bind invisible for ~1×RTT so any
		// concurrently-broadcast competing bind has time to arrive at us
		// before reads can observe this one. Without this wait, a reader
		// sees the bind, then later a competitor arrives and reconstruct
		// picks a different winner — the earlier read becomes a
		// retroactive lie. The RTT is measured per-peer; we wait for the
		// slowest currently-alive peer.
		e.horizon.Add(txnID, bindOffset, bind)
		var peers []pb.NodeID
		if e.broadcaster != nil {
			peers = e.broadcaster.PeerIDs()
		}
		e.horizon.ScheduleMakeVisible(txnID, e.horizon.computeHorizonWait(peers))
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
}

// checkCompetingBinds checks if any bind in the local index shares a causal
// base with our transaction. A bind in the index — whether visible or still
// in horizon wait — is a real competitor. If one exists, our transaction
// is wrong by construction (stale snapshot) — return the competing txnID.
func (e *Engine) checkCompetingBinds(bind *pb.TransactionalBindEffect, txnID string) string {
	for _, kb := range bind.Keys {
		k := string(kb.Key)
		tips := e.index.Contains(k)
		if tips == nil {
			continue
		}
		for _, tipOff := range tips.Tips() {
			eff, err := e.getEffect(tipOff)
			if err != nil {
				continue
			}
			otherBind := eff.GetTxnBind()
			if otherBind == nil {
				continue
			}
			// Skip voided binds (already lost fork-choice)
			if _, voided := e.voidedBinds.Get(eff.TxnId, 0); voided {
				continue
			}
			// Check if they share a consumed tip on any overlapping key.
			shared := false
			var theirNewTip Tip
			for _, okb := range otherBind.Keys {
				if string(okb.Key) != k {
					continue
				}
				ourSet := make(map[Tip]bool, len(kb.ConsumedTips))
				for _, ct := range kb.ConsumedTips {
					ourSet[r(ct)] = true
				}
				for _, ct := range okb.ConsumedTips {
					if ourSet[r(ct)] {
						shared = true
						theirNewTip = r(okb.NewTip)
						break
					}
				}
				if shared {
					break
				}
			}
			if !shared {
				continue
			}
			// Predicate refinement: shared base alone is too coarse
			// when both txs carry observation/row-write evidence on
			// the key. If neither side's observations match the
			// other's writes, the txs are genuinely disjoint — skip
			// the abort. Falls back to the conservative shared-base
			// conflict when either side lacks evidence (e.g. a bind
			// that only mutated schema metadata).
			conflict, bothHadEvidence := e.hasPredicateConflict(
				txnID, eff.TxnId, k,
				[]Tip{r(kb.NewTip)},
				[]Tip{theirNewTip, tipOff})
			if bothHadEvidence && !conflict {
				continue
			}
			return eff.TxnId // competing bind found
		}
	}
	return "" // no competing bind
}

// evaluateBindForkChoice returns true if the bind would lose at the
// originator's commit point against any existing competitor on its keys
// (hash-based fork choice with predicate-refinement and shared-base
// gating) or against a concurrent non-tx data effect (SSI). flushTx
// uses this to abort before emitting. The check is local-arrival-time
// only — it does NOT write to voidedBinds, because arrival-time
// visibility is partial (multi-step competitor flips, unsubscribed
// cross-keys), and a wrong-but-trusted voidedBinds entry would cause
// reconstruct to skip a winner. reconstruct is the canonical writer.
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
			eff, err := e.getEffect(tipOff)
			if err != nil {
				continue
			}

			// Competing bind: fork-choice determines winner
			if otherBind := eff.GetTxnBind(); otherBind != nil {
				if eff.TxnId == txnID {
					continue // same transaction
				}
				// Already voided? skip
				if _, voided := e.voidedBinds.Get(eff.TxnId, 0); voided {
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
						depEff, err := e.getEffect(off)
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
		eff, ok := e.effectCache.Get(tp, 0)
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

	return nack
}

// ingestNackTips fetches and processes every effect referenced in a NACK's
// tip details so the local DAG catches up with the peer's view. A NACK
// is the cluster telling us "you missed these tips"; ignoring the payload
// leaves us permanently behind and the cluster never converges.
//
// Walks each tip's dep chain (BFS) and pulls any effect we don't already
// have via FetchFromAny + HandleRemote. Idempotent — effects we already
// hold are skipped.
func (e *Engine) ingestNackTips(nack *pb.NackNotify) {
	if nack == nil || e.broadcaster == nil {
		return
	}
	var zero Tip
	visited := make(map[Tip]bool)
	var stack []Tip
	for _, d := range nack.TipDetails {
		if d == nil || d.Ref == nil {
			continue
		}
		stack = append(stack, r(d.Ref))
	}
	for len(stack) > 0 {
		off := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if off == zero || visited[off] {
			continue
		}
		visited[off] = true
		notify := &pb.OffsetNotify{
			Origin: toPbRef(off),
			Key:    nack.Key,
		}
		if _, err := e.HandleRemote(notify); err != nil {
			slog.Debug("ingestNackTips: HandleRemote failed",
				"ref", off, "error", err)
			continue
		}
		if e.effectCache != nil {
			if cached, ok := e.effectCache.Get(off, 0); ok {
				stack = append(stack, fromPbRefs(cached.Deps)...)
			}
		}
	}
}

// handleEviction restores the "tip ⇒ fetchable bytes" invariant when
// the cache loses the bytes behind a tip. Emits an unsubscribe
// SubscriptionEffect as a proper DAG element — real offset from
// nextOffset, Deps consuming the current tips so peers reduce us out
// of the subscribers map — then drops local state. Future reads
// re-subscribe and bootstrap fresh.
//
// The broadcast goes out BEFORE local teardown so this node remains
// authoritative for any still-cached effects on the key (other tips
// in the same chain) during the round-trip. We deliberately do NOT
// add the unsub to our own effectCache; we're giving up authority,
// not retaining its trail.
//
// Per-key idempotency via unsubInFlight: multiple effects on the
// same key can evict in close succession, but only one goroutine
// runs the broadcast + teardown. Invoked on a fresh goroutine
// because evictNotify runs under cache shard.mu.
func (e *Engine) handleEviction(evictedRef Tip, evictedEffect *pb.Effect) {
	if e.closed.Load() || evictedEffect == nil {
		return
	}
	if isSystemKey(evictedEffect.Key) {
		// evictDecider should have pinned these; reaching here means
		// the pin invariant was bypassed.
		slog.Warn("handleEviction: system key evicted; pin invariant violated",
			"key", string(evictedEffect.Key), "ref", evictedRef)
		return
	}

	key := string(evictedEffect.Key)

	if _, claimed := e.unsubInFlight.LoadOrStore(key, struct{}{}); claimed {
		return
	}
	defer e.unsubInFlight.Delete(key)

	// Atomically take the deletion: any concurrent updateIndex with a
	// stale non-nil old will fail its tips.CAS once tips is cleared,
	// and the snapshot we broadcast matches the tips we actually drop.
	tipSet := e.index.DeleteAndSnapshot(key)
	if tipSet == nil {
		// Already torn down or never installed.
		e.subscriptions.Delete(key)
		return
	}

	if e.broadcaster != nil {
		hlc := timestamppb.New(e.clock.Now())
		offset := e.nextOffset()
		// Substitute pre-tx deps for any in-progress txn tips so the
		// unsub doesn't reference uncommitted state.
		deps := e.resolveTipDeps(tipSet.Tips())
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
			notify := BuildOffsetNotify(e.nodeID, offset, unsub, data, nil)
			e.broadcaster.BroadcastWithData(notify, notify.EffectData)
		} else {
			slog.Error("handleEviction: marshal unsubscribe failed", "key", key, "error", err)
		}
	}

	e.subscriptions.Delete(key)

	slog.Debug("handleEviction: dropped key and unsubscribed",
		"key", key, "ref", evictedRef)
}

// Close performs graceful shutdown of the engine and its background components.
func (e *Engine) Close() error {
	e.closed.Store(true)
	if e.antiEntropyStop != nil {
		close(e.antiEntropyStop)
		e.antiEntropyWg.Wait()
	}
	if e.voidedBinds != nil {
		e.voidedBinds.Close()
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
	// HandleRemote so the normal bind/tx-tip/horizon machinery runs.
	// Direct updateIndex would slip binds into the index without ever
	// passing through handleRemoteBind — they'd skip HorizonSet entirely
	// and become visible to reads before the 1×RTT competing-bind wait
	// could possibly do its job.
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
			if cached, ok := e.effectCache.Get(off, 0); ok {
				fetchStack = append(fetchStack, fromPbRefs(cached.Deps)...)
				continue
			}
		}

		fetchedData, fetchErr := e.broadcaster.FetchFromAny(toPbRef(off))
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
		if _, herr := e.HandleRemote(notify); herr != nil {
			slog.Debug("anti-entropy: HandleRemote failed",
				"ref", off, "error", herr)
			continue
		}
		fetchStack = append(fetchStack, fromPbRefs(fetchedEff.Deps)...)
	}

	if e.cache != nil {
		e.cache.Evict(key)
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
	if e.effectCache != nil {
		e.effectCache.Put(offset, eff)
	}
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
