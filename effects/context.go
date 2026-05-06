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
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	pb "github.com/swytchdb/cache/cluster/proto"
	"github.com/swytchdb/cache/keytrie"
	"github.com/swytchdb/cache/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// watchedKeyState records the state at WATCH time so that BeginTx and
// flushTx can detect modifications via the causal structure.
type watchedKeyState struct {
	noopOffset Tip             // offset of the WATCH-time NOOP in the log
	tips       *keytrie.TipSet // TipSet pointer AFTER the WATCH NOOP was flushed
	hadData    bool            // key had actual data (non-nil filterSnapshot) at WATCH time
	flushGen   uint64          // engine.flushGeneration at WATCH time
}

// Context tracks per-key state for dep chaining (Emit) and deferred
// index update + broadcast (Flush). One Context per command invocation.
type Context struct {
	engine      *Engine
	traceCtx    context.Context // carries the active OTel span for this command
	inTx        bool
	txnID       string
	keys        map[string]*contextKey
	watchedKeys map[string]*watchedKeyState // keys registered via Watch with WATCH-time state
	txSnapshot  keytrie.KeyIndex            // frozen index snapshot for SSI reads (nil outside MULTI/EXEC)
}

type contextKey struct {
	initialTips *keytrie.TipSet    // tips read from index on first Emit for this key
	lastOffset  Tip                // most recently written effect (nodeID + offset)
	notifies    []*pb.OffsetNotify // all notifies in emit order — every effect must be broadcast for durability

	// Transaction metadata (only populated when inTx)
	readOnly   bool              // only NoopEffects on this key
	watched    bool              // true if this key was added via WATCH (SSI fork detection in flushTx)
	collection pb.CollectionKind // collection kind
	elementIDs [][]byte          // element IDs touched
	hasData    bool              // at least one DataEffect emitted

	// Notification intent — tracks whether this key should wake blocked clients
	shouldNotifyData   bool // an INSERT_OP was the last data-modifying op on this key
	shouldNotifyDelete bool // a full-key REMOVE was the last data-modifying op
}

// ContextSavepoint is an opaque snapshot of a Context's pending
// per-key state. Restore it (via Context.RestoreSavepoint) to
// discard every Emit made after the snapshot was taken; already-
// written log offsets become orphans (never indexed), same as
// Abort. Used by higher-level SQL SAVEPOINT semantics.
//
// Savepoints do NOT snapshot the engine's committed index — the
// engine's state is unchanged until Flush, so reverting the
// Context's pending keys is sufficient to roll back.
type ContextSavepoint struct {
	keys map[string]*contextKey
}

// TakeSavepoint captures a deep-enough copy of the Context's pending
// key state that RestoreSavepoint can revert every Emit issued
// between Take and Restore. Cheap: each contextKey is a small struct
// with slice fields that we clone by reference (the underlying
// values — notifies, elementIDs — are immutable once added).
func (c *Context) TakeSavepoint() *ContextSavepoint {
	snap := &ContextSavepoint{
		keys: make(map[string]*contextKey, len(c.keys)),
	}
	for k, ck := range c.keys {
		cloned := *ck
		// Clone mutable slices so later Emits on the Context don't
		// extend the saved slices in place.
		if len(ck.notifies) > 0 {
			cloned.notifies = make([]*pb.OffsetNotify, len(ck.notifies))
			copy(cloned.notifies, ck.notifies)
		}
		if len(ck.elementIDs) > 0 {
			cloned.elementIDs = make([][]byte, len(ck.elementIDs))
			copy(cloned.elementIDs, ck.elementIDs)
		}
		snap.keys[k] = &cloned
	}
	return snap
}

// RestoreSavepoint replaces the Context's pending-key state with the
// snapshot. Any Emit issued after TakeSavepoint is discarded from
// the Context; its log offsets remain in the engine's log but never
// reach the index (identical to what happens to all effects on
// Abort).
func (c *Context) RestoreSavepoint(sp *ContextSavepoint) {
	if sp == nil {
		return
	}
	// Clone the snapshot's keys map so subsequent Emits don't mutate
	// it (in case the caller retains the snapshot for nested
	// rollbacks).
	restored := make(map[string]*contextKey, len(sp.keys))
	for k, ck := range sp.keys {
		cloned := *ck
		if len(ck.notifies) > 0 {
			cloned.notifies = make([]*pb.OffsetNotify, len(ck.notifies))
			copy(cloned.notifies, ck.notifies)
		}
		if len(ck.elementIDs) > 0 {
			cloned.elementIDs = make([][]byte, len(ck.elementIDs))
			copy(cloned.elementIDs, ck.elementIDs)
		}
		restored[k] = &cloned
	}
	c.keys = restored
}

// NewContext creates a new write context bound to the engine.
func (e *Engine) NewContext() *Context {
	return &Context{
		engine: e,
		keys:   make(map[string]*contextKey),
	}
}

// PendingKeys returns the names of all keys with pending effects in
// this Context. Callers use this to acquire per-key locks before
// calling Flush — Flush's fork-choice critical section races with
// any other Flush on the same key, and the handler layer (redis
// handler, sql handler) is where that serialisation belongs.
func (c *Context) PendingKeys() []string {
	if c == nil || len(c.keys) == 0 {
		return nil
	}
	out := make([]string, 0, len(c.keys))
	for k := range c.keys {
		out = append(out, k)
	}
	return out
}

// SetTraceCtx sets the OTel trace context for this command.
func (c *Context) SetTraceCtx(ctx context.Context) {
	c.traceCtx = ctx
}

// TraceCtx returns the OTel trace context, or context.Background() if none set.
func (c *Context) TraceCtx() context.Context {
	if c.traceCtx != nil {
		return c.traceCtx
	}
	return context.Background()
}

// GetSnapshot returns the current materialized state of a key, including
// any unflushed effects from this context. Within MULTI/EXEC, earlier
// commands' effects are in the log but not yet in the index; this method
// reconstructs from the log so commands within the same transaction can
// see each other's writes.
//
// When a txSnapshot is active (MULTI/EXEC with SSI), reads for keys not
// yet in the context use the snapshot instead of the live index, and a
// NOOP is emitted to record the read in the causal structure.
func (c *Context) GetSnapshot(key string) (*pb.ReducedEffect, []Tip, error) {
	if c == nil {
		return nil, nil, nil
	}
	_, snapSpan := tracing.Tracer().Start(c.TraceCtx(), "effects.get_snapshot",
		trace.WithAttributes(attribute.String("effect.key", key)))
	defer snapSpan.End()
	ck, hasPending := c.keys[key]
	if !hasPending {
		// SSI: if we have a snapshot, read from it instead of the live index
		if c.txSnapshot != nil {
			return c.getSnapshotFromTx(key)
		}
		// No unflushed effects for this key — delegate to committed state
		result, tips, chainLen, err := c.engine.GetSnapshot(key)
		if err != nil {
			return nil, nil, err
		}
		// Compact long chains by emitting a snapshot into this context
		if chainLen >= 20+rand.IntN(31) && result != nil {
			slog.Debug("compaction: emitting snapshot",
				"key", key,
				"chainLen", chainLen,
				"tips", tips)
			c.Emit(&pb.Effect{
				Key: []byte(key),
				Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
					Collection: result.Collection,
					State:      result,
				}},
			}, tips)
		}
		return result, tips, nil
	}

	// Reconstruct from lastOffset merged with index tips. When an SSI
	// snapshot is active (MULTI/EXEC), use the snapshot to preserve
	// isolation — concurrent writes must not be visible. Outside SSI,
	// use the live index so concurrent writes (e.g. XGROUP DESTROY
	// while a blocking XREADGROUP holds a compaction snapshot) are visible.
	myTip := ck.lastOffset
	reconTips := []Tip{myTip}
	var indexed *keytrie.TipSet
	if c.txSnapshot != nil {
		indexed = c.txSnapshot.Contains(key)
	} else {
		indexed = c.engine.index.Contains(key)
	}
	if indexed != nil {
		for _, t := range indexed.Tips() {
			if t != myTip {
				reconTips = append(reconTips, t)
			}
		}
	}
	slog.Debug("Context.GetSnapshot: hasPending reconstruct", "key", key,
		"txn_id", c.txnID, "recon_tips", reconTips)
	result, _, err := c.engine.reconstruct(key, reconTips, c.txnID)
	if err != nil {
		return nil, nil, err
	}

	result = filterSnapshot(result)

	// Return initialTips — these are the committed tips from before our writes.
	var tips []Tip
	if ck.initialTips != nil {
		tips = ck.initialTips.Tips()
	}
	return result, tips, nil
}

// getSnapshotFromTx reads a key from the SSI snapshot and emits a NOOP
// to record the read in the causal log for the Bind's read set.
func (c *Context) getSnapshotFromTx(key string) (*pb.ReducedEffect, []Tip, error) {
	slog.Debug("getSnapshotFromTx: entry", "key", key, "txn_id", c.txnID)
	tips := c.txSnapshot.Contains(key)
	if tips == nil {
		return nil, nil, nil // key didn't exist at snapshot time
	}
	snapshotTips := tips.Tips()
	tipOffsets := c.engine.resolveTipDeps(snapshotTips)
	if len(tipOffsets) == 0 {
		return nil, nil, nil
	}
	result, _, err := c.engine.reconstruct(key, tipOffsets, c.txnID)
	if err != nil {
		return nil, nil, err
	}
	result = filterSnapshot(result)

	// Emit a NOOP to record this read in the causal structure.
	// The Bind will include this key in its read set.
	c.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	}, snapshotTips)

	return result, snapshotTips, nil
}

// Watch records a key for optimistic locking by emitting a NoopEffect
// immediately and flushing it. This records the observation in the
// causal log so all nodes can verify whether the key was modified
// between WATCH and EXEC. The NOOP offset and post-flush TipSet pointer
// are stored for comparison at BeginTx time.
func (c *Context) Watch(key string) {
	if c.watchedKeys == nil {
		c.watchedKeys = make(map[string]*watchedKeyState)
	}

	// Check if key has actual data BEFORE emitting the NOOP
	snap, _, _, _ := c.engine.GetSnapshot(key)
	hadData := snap != nil

	// Emit NOOP to record the observation in the causal log
	c.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	})

	// Capture the NOOP offset before Flush clears c.keys
	noopOffset := c.keys[key].lastOffset

	// Flush immediately so the NOOP is durable and in the index.
	// ExecuteInto's post-handler Flush will be a no-op (c.keys empty).
	c.Flush()

	// Capture the post-flush TipSet pointer (immutable — pointer identity = equality)
	tips := c.engine.index.Contains(key)
	c.watchedKeys[key] = &watchedKeyState{
		noopOffset: noopOffset,
		tips:       tips,
		hadData:    hadData,
		flushGen:   c.engine.flushGeneration.Load(),
	}
}

// ClearWatches removes all watched keys without affecting transaction state.
func (c *Context) ClearWatches() {
	clear(c.watchedKeys)
}

// BeginTx marks subsequent effects as transactional.
// This is called by read-modify-write commands (INCR, LPUSH, etc.) for
// atomicity AND by handleExec for MULTI/EXEC. Watch processing is NOT
// done here — use CheckWatches after BeginTx for EXEC.
func (c *Context) BeginTx() {
	if c.inTx {
		return // nested transaction inherits outer
	}
	c.inTx = true
	c.txnID = c.engine.generateTxnID()
}

// CheckWatches validates WATCH observations and emits transactional NOOPs
// for the Bind's read set. Must be called AFTER BeginTx (so NOOPs get
// IsTransactional=true). Only called from handleExec.
//
// Returns false if any watched key was modified — the caller should abort
// the transaction and return a null array to the client.
func (c *Context) CheckWatches() bool {
	// Capture index snapshot for SSI — all reads within the transaction
	// will use this snapshot instead of the live index.
	c.txSnapshot = c.engine.index.Snapshot()

	if len(c.watchedKeys) == 0 {
		return true
	}

	// Validate watched keys — same check a subscriber would perform
	currentFlushGen := c.engine.flushGeneration.Load()
	for key, ws := range c.watchedKeys {
		currentTips := c.engine.index.Contains(key)
		if ws.tips == currentTips {
			continue // TipSet pointer identical — no change
		}
		// Pointer changed. Handle FLUSHALL on non-existent key:
		// Redis says FLUSHALL does NOT dirty watches on keys that didn't exist.
		if ws.flushGen != currentFlushGen && !ws.hadData && currentTips == nil {
			continue // key didn't exist at WATCH time, still doesn't after flush
		}
		// Key was modified — abort
		clear(c.watchedKeys)
		c.inTx = false
		c.txSnapshot = nil
		return false
	}

	// Emit transactional NOOPs using WATCH NOOP offsets as snapshotTips.
	// This forces deps on the WATCH-time state, creating a fork if the
	// key was modified (the fork is structurally detectable by any node).
	for key, ws := range c.watchedKeys {
		c.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, []Tip{ws.noopOffset})
		// Mark as watched for SSI fork detection in flushTx
		if ck, ok := c.keys[key]; ok {
			ck.watched = true
		}
	}
	clear(c.watchedKeys)
	return true
}

// Abort discards pending index updates and broadcasts. Effects already
// written to the log remain durable but invisible (index not updated).
func (c *Context) Abort() {
	clear(c.keys)
	c.inTx = false
	c.txnID = ""
	c.txSnapshot = nil
}

// Emit writes a single effect to the log. The context tracks per-key
// state so that consecutive effects on the same key form a dep chain.
// The index is NOT updated until Flush.
//
// For read-modify-write commands, pass the tip offsets returned by
// GetSnapshot as snapshotTips so that the first effect depends on the
// tips the handler actually read, not whatever the index contains now.
// Pure writes (SET, LPUSH) omit snapshotTips and Emit reads the index.
func (c *Context) Emit(eff *pb.Effect, snapshotTips ...[]Tip) error {
	key := string(eff.Key)

	_, emitSpan := tracing.Tracer().Start(c.TraceCtx(), "effects.emit",
		trace.WithAttributes(attribute.String("effect.key", key)))
	defer emitSpan.End()

	// Fill causality
	eff.Hlc = timestamppb.New(c.engine.clock.Now())
	eff.NodeId = uint64(c.engine.nodeID)
	eff.ForkChoiceHash = ComputeForkChoiceHash(c.engine.nodeID, eff.Hlc)
	if c.inTx {
		eff.TxnId = c.txnID
	}

	// Fill deps
	ck, exists := c.keys[key]
	if !exists {
		var tips *keytrie.TipSet
		if len(snapshotTips) > 0 && snapshotTips[0] != nil {
			tips = keytrie.NewTipSet(snapshotTips[0]...)
		} else if c.txSnapshot != nil {
			tips = c.txSnapshot.Contains(key) // SSI: use snapshot, not live index
		} else {
			tips = c.engine.index.Contains(key)
		}
		ck = &contextKey{initialTips: tips, readOnly: c.inTx}
		c.keys[key] = ck
		if tips != nil {
			tipOffsets := tips.Tips()
			// Exclude in-progress tx tips: substitute pre-tx deps
			eff.Deps = toPbRefs(c.engine.resolveTipDeps(tipOffsets))
		}
	} else {
		eff.Deps = []*pb.EffectRef{toPbRef(ck.lastOffset)}
	}

	// Track notification intent from DataEffect ops
	if data := eff.GetData(); data != nil {
		switch data.Op {
		case pb.EffectOp_INSERT_OP:
			ck.shouldNotifyData = true
			ck.shouldNotifyDelete = false
		case pb.EffectOp_REMOVE_OP:
			if len(data.Id) == 0 {
				// Full key delete
				ck.shouldNotifyData = false
				ck.shouldNotifyDelete = true
			} else if data.Collection == pb.CollectionKind_ORDERED || data.Collection == pb.CollectionKind_KEYED {
				// Element remove on ordered/keyed collection — chain-wake
				// so the next blocked client gets a chance to pop
				ck.shouldNotifyData = true
			}
		}
	}

	// Track tx metadata
	if c.inTx {
		if data := eff.GetData(); data != nil {
			ck.readOnly = false
			ck.hasData = true
			ck.collection = data.Collection
			if len(data.Id) > 0 {
				ck.elementIDs = append(ck.elementIDs, data.Id)
			}
		}
		// A RowWriteEffect is semantic evidence of a row-level write
		// on this key — flip readOnly so SSI's "any concurrent write
		// aborts" doesn't fire on write-carrying txs. The refined
		// predicate check in checkCompetingBinds / evaluateBindForkChoice
		// handles conflict detection for these txs.
		if eff.GetRowWrite() != nil {
			ck.readOnly = false
		}
	}

	offset, notify, err := c.rawEmit(eff)
	if err != nil {
		// Clean up the contextKey if we just created it and it has no prior effects
		if len(ck.notifies) == 0 {
			delete(c.keys, key)
		}
		return err
	}

	// Update contextKey
	ck.lastOffset = offset
	ck.notifies = append(ck.notifies, notify)

	return nil
}

// rawEmit is the core write path: serialize, write to log, cache, and
// build notify. The caller is responsible for setting all fields on the
// effect (Key, Hlc, NodeId, ForkChoiceHash, TxnId, Deps, Kind).
// Returns the log offset and the constructed OffsetNotify.
func (c *Context) rawEmit(eff *pb.Effect) (Tip, *pb.OffsetNotify, error) {
	key := string(eff.Key)

	data, err := proto.Marshal(eff)
	if err != nil {
		return Tip{}, nil, err
	}

	offset := c.engine.nextOffset()

	slog.Debug("Emit: wrote effect",
		"key", key, "offset", offset, "deps", eff.Deps, "tx", eff.TxnId != "")

	if c.engine.effectCache != nil {
		c.engine.effectCache.Put(offset, proto.Clone(eff).(*pb.Effect))
	}

	notify := buildOffsetNotify(c.engine.nodeID, offset, eff, data, c.TraceCtx())
	return offset, notify, nil
}

// Flush updates the index for all touched keys and broadcasts all
// notifications per key for durability, then resets the context for reuse.
func (c *Context) Flush() error {
	_, flushSpan := tracing.Tracer().Start(c.TraceCtx(), "effects.flush",
		trace.WithAttributes(attribute.Int("flush.keys", len(c.keys))))
	defer flushSpan.End()

	if !c.inTx {
		return c.flushNonTx()
	}
	return c.flushTx()
}

// flushNonTx is the original Flush body for non-transactional writes.
func (c *Context) flushNonTx() error {
	slog.Debug("Flush: non-tx", "keys", len(c.keys))

	for key, ck := range c.keys {
		// Handle flush-all: wipe index before broadcasting
		if key == FlushKey {
			slog.Info("Flush: flush-all effect, wiping index")
			c.engine.FlushIndex()
			// Broadcast to peers but skip index update (already wiped)
			if c.engine.broadcaster != nil {
				for _, n := range ck.notifies {
					c.engine.broadcaster.BroadcastWithData(n, n.EffectData)
				}
			}
			// Fire OnFlushAll callback
			if c.engine.OnFlushAll != nil {
				c.engine.OnFlushAll()
			}
			delete(c.keys, key)
			continue
		}

		mode := c.engine.modeForKey(key)

		func() {
			_, idxSpan := tracing.Tracer().Start(c.TraceCtx(), "flush.update_index",
				trace.WithAttributes(attribute.String("effect.key", key)))
			defer idxSpan.End()
			slog.Debug("Flush: updating index", "key", key, "offset", ck.lastOffset)
			c.engine.updateIndex(key, ck.initialTips, ck.lastOffset)
		}()

		// Tip-count trigger: emit serialization request when tips exceed threshold
		// and no leader is already active for this key.
		func() {
			_, serSpan := tracing.Tracer().Start(c.TraceCtx(), "flush.serialization_check",
				trace.WithAttributes(attribute.String("effect.key", key)))
			defer serSpan.End()
			if c.engine.CheckSerializationLeader(key) == nil {
				if tips := c.engine.index.Contains(key); tips != nil && len(tips.Tips()) > tipSerializationThreshold {
					slog.Info("adaptive serialization: tip count exceeded threshold",
						"key", key, "tips", len(tips.Tips()), "threshold", tipSerializationThreshold)
					c.emitSerializationEffect(key)
				}
			}
		}()

		if c.engine.broadcaster != nil {
			bcastCtx, bcastSpan := tracing.Tracer().Start(c.TraceCtx(), "flush.broadcast",
				trace.WithAttributes(
					attribute.String("effect.key", key),
					attribute.Int("flush.mode", int(mode)),
					attribute.Int("flush.notifies", len(ck.notifies)),
				))
			// Re-stamp trace context on notifies so remote spans parent to this broadcast
			bcastTrace := tracing.InjectIntoBytes(bcastCtx)
			for _, n := range ck.notifies {
				n.TraceContext = bcastTrace
			}
			if mode == SafeMode {
				// Pre-check already verified all peers are reachable.
				// Index is updated, so the write is committed — replicate
				// but don't fail the client if replication errors out.
				for i, n := range ck.notifies {
					if i < len(ck.notifies)-1 {
						c.engine.broadcaster.BroadcastWithData(n, n.EffectData)
					} else {
						if err := c.engine.broadcaster.Replicate(n, n.EffectData); err != nil {
							// Solo cluster (no peers yet) is not worth warning
							// on every write — demote to debug.
							if len(c.engine.broadcaster.PeerIDs()) == 0 {
								slog.Debug("SafeMode replication skipped: no peers", "key", key, "error", err)
							} else {
								slog.Warn("SafeMode replication failed after commit", "key", key, "error", err)
							}
						}
					}
				}
			} else {
				for _, n := range ck.notifies {
					c.engine.broadcaster.BroadcastWithData(n, n.EffectData)
				}
			}
			bcastSpan.End()
		}

		func() {
			_, evictSpan := tracing.Tracer().Start(c.TraceCtx(), "flush.cache_evict",
				trace.WithAttributes(attribute.String("effect.key", key)))
			defer evictSpan.End()
			// Evict snapshot cache so next read sees the new state
			if c.engine.cache != nil {
				c.engine.cache.Evict(key)
			}
		}()

		// Fire notification callbacks after effect is durable
		func() {
			_, cbSpan := tracing.Tracer().Start(c.TraceCtx(), "flush.notify_callbacks",
				trace.WithAttributes(attribute.String("effect.key", key)))
			defer cbSpan.End()
			if ck.shouldNotifyData && c.engine.OnKeyDataAdded != nil {
				c.engine.OnKeyDataAdded(key)
			}
			if ck.shouldNotifyDelete && c.engine.OnKeyDeleted != nil {
				c.engine.OnKeyDeleted(key)
			}
		}()

		delete(c.keys, key)
	}

	return nil
}

// flushTx implements the transactional commit protocol.
func (c *Context) flushTx() error {
	slog.Debug("Flush: transactional commit", "keys", len(c.keys))
	c.inTx = false
	hadSnapshot := c.txSnapshot != nil
	c.txSnapshot = nil
	if len(c.keys) == 0 {
		return nil
	}

	// SafeMode pre-check: transactions block when in a minority partition.
	// Commutative (non-tx) writes are allowed — they merge on reconnect.
	if c.engine.broadcaster != nil {
		for key := range c.keys {
			if c.engine.modeForKey(key) == SafeMode {
				if !c.engine.broadcaster.InMajorityPartition() {
					c.reset()
					return ErrRegionPartitioned
				}
				break // one check is enough — reachability is per-region, not per-key
			}
		}
	}

	// Step 0.5: Ensure subscription for every key in the transaction.
	// Per whitepaper §3.3, a node must subscribe before any read or write.
	// If we're in a minority partition, ensureSubscribed returns an error
	// and the transaction must abort.
	for key := range c.keys {
		if err := c.engine.ensureSubscribed(key); err != nil {
			c.reset()
			return ErrRegionPartitioned
		}
	}

	// Step 0.75: Pre-flight staleness check. Protocol invariant: once
	// this node ACKs (or NACKs) a remote bind on key K, it must not
	// emit its own bind consuming any tip that remote bind consumed.
	// The per-key striped lock held by the caller serialises this
	// flush against HandleRemote, so any remote bind on one of our
	// keys has either been indexed already or hasn't been observed
	// at all. If any of our initialTips is no longer a current tip,
	// a remote bind has already consumed it locally — we're past
	// the ACK/NACK commit point and the invariant forbids us from
	// emitting. Abort without running fork-choice; that arbiter is
	// reserved for truly concurrent competitors that neither side
	// has observed yet.
	for key, ck := range c.keys {
		if ck.initialTips == nil {
			continue // first write to a brand-new key
		}
		initial := ck.initialTips.Tips()
		if len(initial) == 0 {
			continue
		}
		current := c.engine.index.Contains(key)
		currentSet := make(map[Tip]bool)
		if current != nil {
			for _, t := range current.Tips() {
				currentSet[t] = true
			}
		}
		for _, t := range initial {
			if !currentSet[t] {
				slog.Debug("flushTx: stale consumed tip, aborting before emission",
					"key", key, "stale_tip", t, "current_tips", current)
				for _, ck3 := range c.keys {
					c.engine.pendingTxTips.Delete(ck3.lastOffset)
				}
				c.reset()
				return ErrTxnAborted
			}
		}
	}

	// Step 1: Update index + broadcast individual effects per key
	for key, ck := range c.keys {
		c.engine.updateIndex(key, ck.initialTips, ck.lastOffset)

		// Register as pending tx tip
		var preTxDeps []Tip
		if ck.initialTips != nil {
			preTxDeps = ck.initialTips.Tips()
		}
		c.engine.pendingTxTips.Store(ck.lastOffset, preTxDeps)

		if c.engine.broadcaster != nil {
			for _, n := range ck.notifies {
				c.engine.broadcaster.BroadcastWithData(n, n.EffectData)
			}
		}
	}

	// Step 1.5: SSI fork validation.
	// When a snapshot is active, ALL keys use snapshot deps. Any fork means
	// a concurrent write happened since the snapshot — SSI conflict.
	// Without a snapshot, only check readOnly keys (WATCH NOOPs).
	for key, ck := range c.keys {
		if !hadSnapshot && !ck.readOnly {
			continue
		}
		tips := c.engine.index.Contains(key)
		if tips == nil {
			continue
		}
		myTip := ck.lastOffset
		for _, t := range tips.Tips() {
			if t == myTip {
				continue // our own tip
			}
			isOurs := false
			for _, ck2 := range c.keys {
				if ck2.lastOffset == t {
					isOurs = true
					break
				}
			}
			if !isOurs {
				// Concurrent effect on watched key → SSI conflict
				slog.Debug("SSI conflict: concurrent write on watched key",
					"tip", t, "our_tip", myTip)
				for _, ck3 := range c.keys {
					c.engine.pendingTxTips.Delete(ck3.lastOffset)
				}
				c.reset()
				return ErrTxnAborted
			}
		}
	}

	// Step 2: Collect competing in-progress tx offsets for abort_deps
	var abortDeps []*pb.EffectRef
	for key := range c.keys {
		tips := c.engine.index.Contains(key)
		if tips == nil {
			continue
		}
		for _, t := range tips.Tips() {
			if _, isPending := c.engine.pendingTxTips.Load(t); isPending {
				// Check it's not our own tip
				isOurs := false
				for _, ck := range c.keys {
					if ck.lastOffset == t {
						isOurs = true
						break
					}
				}
				if !isOurs {
					abortDeps = append(abortDeps, toPbRef(t))
				}
			}
		}
	}

	// Step 3: Build TransactionalBindEffect
	txnHLC := c.engine.clock.Now()
	hlcTs := timestamppb.New(txnHLC)
	bind := &pb.TransactionalBindEffect{
		TxnHlc:           timestamppb.New(txnHLC),
		OriginatorNodeId: uint64(c.engine.nodeID),
		AbortDeps:        abortDeps,
	}

	for key, ck := range c.keys {
		var consumedTips []*pb.EffectRef
		if ck.initialTips != nil {
			consumedTips = toPbRefs(ck.initialTips.Tips())
		}
		kb := &pb.TransactionalBindEffect_KeyBind{
			Key:          []byte(key),
			ConsumedTips: consumedTips,
			NewTip:       toPbRef(ck.lastOffset),
		}
		bind.Keys = append(bind.Keys, kb)
	}

	// Bind deps: the last effect on each key in the transaction.
	var bindDeps []*pb.EffectRef
	for _, ck := range c.keys {
		bindDeps = append(bindDeps, toPbRef(ck.lastOffset))
	}

	// Pre-emission check: a bind that completed its horizon wait is a
	// committed transaction. If a visible competing bind shares our causal
	// base on any key, our snapshot is stale — abort without emitting.
	if conflict := c.engine.checkCompetingBinds(bind, c.txnID); conflict != "" {
		slog.Debug("flushTx: competing bind found, aborting before emission",
			"competing_txn", conflict)
		for key, ck3 := range c.keys {
			if c.engine.horizon == nil {
				c.engine.pendingTxTips.Delete(ck3.lastOffset)
			}
			// Check serialization escalation (only if no leader already active)
			if c.engine.CheckSerializationLeader(key) == nil && c.engine.incrementAbortCount(key) {
				slog.Info("adaptive serialization: abort threshold exceeded",
					"key", key, "threshold", DefaultSerializationThreshold)
				c.emitSerializationEffect(key)
			}
		}
		c.reset()
		return ErrTxnAborted
	}

	bindEff := &pb.Effect{
		Key:            bind.Keys[0].Key, // use first key as canonical
		Hlc:            hlcTs,
		NodeId:         uint64(c.engine.nodeID),
		ForkChoiceHash: ComputeForkChoiceHash(c.engine.nodeID, hlcTs),
		TxnId:          c.txnID,
		Deps:           bindDeps,
		Kind:           &pb.Effect_TxnBind{TxnBind: bind},
	}

	// Use rawEmit: the bind bypasses Emit's dep/context tracking because
	// it must not update any contextKey's lastOffset (which would corrupt
	// pending txn tracking). rawEmit handles log write, effect cache, and
	// notify construction.
	bindOffset, bindNotify, err := c.rawEmit(bindEff)
	if err != nil {
		return err
	}

	// Index bind under ALL keys it touches, consuming the DATA tip that
	// Step 1 indexed (ck.lastOffset). This replaces the tentative DATA
	// tip with the BIND, preventing tip accumulation across transactions.
	// Concurrent tips from other writers are preserved (not consumed).
	for _, kb := range bind.Keys {
		key := string(kb.Key)
		ck := c.keys[key]
		c.engine.updateIndex(key, keytrie.NewTipSet(ck.lastOffset), bindOffset)
	}

	// Evaluate fork-choice against existing binds on all keys.
	c.engine.evaluateBindForkChoice(bind, bindOffset, bindEff.ForkChoiceHash, c.txnID)

	// If a concurrent bind that was emitted between our
	// checkCompetingBinds pre-flight and our bind's indexing has
	// won fork-choice over us, we're in voidedBinds now — abort
	// instead of falsely reporting a commit. Without this check,
	// two truly-concurrent commits (same ConsumedTips, races past
	// checkCompetingBinds) would both commit locally while only one
	// is actually valid per the deterministic fork-choice rule.
	if _, voided := c.engine.voidedBinds.Load(c.txnID); voided {
		slog.Debug("flushTx: voided by concurrent bind, aborting",
			"bind_offset", bindOffset, "txn", c.txnID)
		for key, ck := range c.keys {
			_ = key
			c.engine.pendingTxTips.Delete(ck.lastOffset)
		}
		c.reset()
		return ErrTxnAborted
	}

	// Step 4: Create pending txn for NACK tracking
	ptxn := &pendingTxn{
		txnID:      c.txnID,
		txnHLC:     txnHLC,
		originNode: c.engine.nodeID,
		bindOffset: bindOffset,
		done:       make(chan struct{}),
	}
	for key, ck := range c.keys {
		var consumedTips []Tip
		if ck.initialTips != nil {
			consumedTips = ck.initialTips.Tips()
		}
		ptxn.keys = append(ptxn.keys, pendingTxnKey{
			key:          key,
			consumedTips: consumedTips,
			newTip:       ck.lastOffset,
			readOnly:     ck.readOnly,
			collection:   ck.collection,
			elementIDs:   ck.elementIDs,
		})
	}
	c.engine.pendingTxns.Store(bindOffset, ptxn)

	// Step 5: Gather subscribers and replicate bind

	if c.engine.broadcaster != nil {
		subscribers := c.collectSubscribers()
		slog.Debug("flushTx: replicating bind",
			"bind_offset", bindOffset,
			"subscribers", len(subscribers))
		if len(subscribers) == 0 {
			// Single-node fast path: commit immediately. No
			// horizon.Add needed — the bind has no peers to wait
			// on. Adding it would have made our own in-flight
			// bind invisible to the collectSubscribers reconstruct
			// above, causing the stale-tip pruning to yank the
			// bind offset out of the index before we ever got a
			// chance to commit it.
			slog.Debug("flushTx: no subscribers, committing immediately",
				"bind_offset", bindOffset)
			commitPendingTxn(ptxn)
		} else {
			// Peers exist — keep the bind invisible while we wait
			// for ACK/NACK. MakeVisible is called once the wait
			// resolves (below). Horizon timer is RTT-scaled over
			// the subscriber set — it only needs to be long enough
			// that a concurrent competing bind from any of these
			// peers would have reached us.
			if c.engine.horizon != nil {
				c.engine.horizon.Add(c.txnID, bindOffset, bind, subscribers)
			}
			// Replicate to all subscribers concurrently, collect responses.
			// Track successes — we need a majority to commit.
			// Network errors to individual peers don't abort unless
			// we lose majority.
			type replicaResult struct {
				subID pb.NodeID
				nacks []*pb.NackNotify
				err   error
			}

			results := make([]replicaResult, len(subscribers))
			var wg sync.WaitGroup
			wg.Add(len(subscribers))
			for i, subID := range subscribers {
				go func(idx int, target pb.NodeID) {
					defer wg.Done()
					slog.Debug("flushTx: ReplicateTo",
						"bind_offset", bindOffset,
						"target", target)
					nacks, err := c.engine.broadcaster.ReplicateTo(bindNotify, bindNotify.EffectData, target)
					results[idx] = replicaResult{subID: target, nacks: nacks, err: err}
				}(i, subID)
			}
			wg.Wait()

			ackCount := 0
			for _, res := range results {
				if res.err != nil {
					slog.Warn("flushTx: ReplicateTo failed",
						"bind_offset", bindOffset,
						"target", res.subID,
						"error", res.err)
					continue
				}
				ackCount++
				for _, nack := range res.nacks {
					slog.Debug("flushTx: received NACK",
						"bind_offset", bindOffset,
						"target", res.subID,
						"nack_key", string(nack.Key),
						"tip_count", len(nack.TipDetails))
					for _, detail := range nack.TipDetails {
						if c.engine.isRealConflict(ptxn, string(nack.Key), detail) {
							slog.Debug("flushTx: real conflict detected, aborting",
								"bind_offset", bindOffset,
								"nack_key", string(nack.Key),
								"detail_offset", detail.Ref,
								"detail_is_bind", detail.IsBind,
								"detail_is_data", detail.IsData,
								"detail_is_tx", detail.IsTransactional)
							abortPendingTxn(ptxn)
							break
						}
					}
					if ptxn.state.Load() == txnStateAborted {
						break
					}
				}
				if ptxn.state.Load() == txnStateAborted {
					break
				}
			}

			if ptxn.state.Load() != txnStateAborted {
				// +1 for ourselves — we always have our own bind
				totalNodes := len(subscribers) + 1
				reachable := ackCount + 1
				if reachable > totalNodes/2 {
					if commitPendingTxn(ptxn) {
						slog.Debug("flushTx: majority responded, committed",
							"bind_offset", bindOffset,
							"acks", ackCount,
							"subscribers", len(subscribers))
					}
				} else {
					// The Bind is already broadcast and stored on peers.
					// Aborting locally would leave tentative effects
					// visible elsewhere (G1a). Commit and let fork-choice
					// resolve any competing Binds on reconnect.
					// The replication layer already marked unreachable
					// peers dead, so subsequent pre-checks will block.
					slog.Warn("flushTx: minority partition post-broadcast, committing holographically",
						"bind_offset", bindOffset,
						"acks", ackCount,
						"subscribers", len(subscribers))
					commitPendingTxn(ptxn)
				}
				// We've heard back from every peer we waited on (wg.Wait
				// completed above) and decided to commit. The bind is
				// no longer tentative from our perspective — it IS the
				// commit decision we're about to tell the client about.
				// Make it visible now so the client's immediate next
				// read sees its own write. Leaving it horizon-invisible
				// here (the old all-ACK-no-NACK gate) causes lost-update
				// anomalies: client hears COMMIT, next read returns
				// stale state because horizon timer hasn't fired.
				if c.engine.horizon != nil {
					c.engine.horizon.MakeVisible(c.txnID)
				}
			}
		}
	} else {
		// Standalone: commit immediately
		commitPendingTxn(ptxn)
	}

	// Step 6: Process result
	state := ptxn.state.Load()
	c.engine.pendingTxns.Delete(bindOffset)

	if state == txnStateAborted {
		slog.Debug("Flush: transaction aborted", "bind_offset", bindOffset)
		// Clean up pending tx tips — but NOT when horizon is active
		// (effects stay invisible, timer cleans up)
		if c.engine.horizon == nil {
			for _, pk := range ptxn.keys {
				c.engine.pendingTxTips.Delete(pk.newTip)
			}
		}

		// Check serialization escalation (only if no leader already active)
		for _, pk := range ptxn.keys {
			if c.engine.CheckSerializationLeader(pk.key) == nil && c.engine.incrementAbortCount(pk.key) {
				slog.Info("adaptive serialization: abort threshold exceeded",
					"key", pk.key, "threshold", DefaultSerializationThreshold)
				c.emitSerializationEffect(pk.key)
			}
		}

		c.reset()
		return ErrTxnAborted
	}

	slog.Debug("Flush: transaction committed", "bind_offset", bindOffset)
	// Committed: clean up pending tx tips, reset abort counts, evict cache
	// When horizon is active, MakeVisible (fast-path or timer) handles
	// pendingTxTips cleanup and cache eviction.
	for _, pk := range ptxn.keys {
		if c.engine.horizon == nil {
			c.engine.pendingTxTips.Delete(pk.newTip)
		}
		c.engine.resetAbortCount(pk.key)
		if c.engine.horizon == nil && c.engine.cache != nil {
			c.engine.cache.Evict(pk.key)
		}
		// Tip-count trigger: emit serialization request when tips exceed
		// threshold and no leader is already active for this key.
		if c.engine.CheckSerializationLeader(pk.key) == nil {
			if tips := c.engine.index.Contains(pk.key); tips != nil && len(tips.Tips()) > tipSerializationThreshold {
				slog.Info("adaptive serialization: tip count exceeded threshold (tx)",
					"key", pk.key, "tips", len(tips.Tips()), "threshold", tipSerializationThreshold)
				c.emitSerializationEffect(pk.key)
			}
		}
	}

	// Fire notification callbacks after successful commit
	for key, ck := range c.keys {
		if ck.shouldNotifyData && c.engine.OnKeyDataAdded != nil {
			c.engine.OnKeyDataAdded(key)
		}
		if ck.shouldNotifyDelete && c.engine.OnKeyDeleted != nil {
			c.engine.OnKeyDeleted(key)
		}
	}

	c.reset()
	return nil
}

// collectSubscribers gathers unique subscriber node IDs across all touched keys.
// Uses reconstruct directly instead of GetSnapshot to avoid filterSnapshot
// stripping metadata-only snapshots (which contain subscription info).
func (c *Context) collectSubscribers() []pb.NodeID {
	seen := make(map[pb.NodeID]struct{})
	for key := range c.keys {
		tips := c.engine.index.Contains(key)
		if tips == nil {
			continue
		}
		tipOffsets := c.engine.resolveTipDeps(tips.Tips())
		if len(tipOffsets) == 0 {
			continue
		}
		r, _, err := c.engine.reconstruct(key, tipOffsets)
		if err != nil || r == nil {
			continue
		}
		for subID := range r.Subscribers {
			if subID != uint64(c.engine.nodeID) {
				seen[pb.NodeID(subID)] = struct{}{}
			}
		}
	}

	result := make([]pb.NodeID, 0, len(seen))
	for id := range seen {
		result = append(result, id)
	}
	return result
}

// emitSerializationEffect emits a serialization request for a key without
// a transaction so it becomes visible immediately. Under heavy contention
// the transactional path would abort or remain invisible behind pending
// tx tips, causing concurrent GetSnapshot calls to clear the leader.
// The leader is selected using RTT measurements when available.
func (c *Context) emitSerializationEffect(key string) {
	if !adaptiveSerializationEnabled {
		return
	}
	leader := c.engine.selectSerializationLeader(key)

	slog.Info("adaptive serialization: requesting leader",
		"key", key, "leader", leader, "self", c.engine.nodeID)

	ctx := c.engine.NewContext()
	if err := ctx.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Serialization{Serialization: &pb.SerializationEffect{
			LeaderNodeId: uint64(leader),
		}},
	}); err != nil {
		slog.Error("failed to emit serialization effect", "key", key, "error", err)
		return
	}
	if err := ctx.Flush(); err != nil {
		slog.Debug("serialization effect flush failed",
			"key", key, "error", err)
	}
}

// reset clears the context for reuse.
func (c *Context) reset() {
	clear(c.keys)
	c.inTx = false
	c.txnID = ""
	c.txSnapshot = nil
}

// updateIndex performs a CAS loop to merge the new offset into the
// index, preserving any tips added by concurrent writers.
// After a successful CAS, records the transition in the Tip recovery log.
func (e *Engine) updateIndex(key string, initialTips *keytrie.TipSet, lastOffset Tip) {
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			slog.Debug("updateIndex: CAS retry", "key", key, "attempt", attempt)
		}
		current := e.index.Contains(key)
		var offsets []Tip
		if current != nil {
			for _, tp := range current.Tips() {
				if initialTips == nil || !initialTips.Contains(tp) {
					offsets = append(offsets, tp) // keep tips we didn't consume
				}
			}
		}
		offsets = append(offsets, lastOffset)
		newTips := keytrie.NewTipSet(offsets...)
		if _, ok := e.index.Insert(key, current, newTips); ok {
			return
		}
	}
}

// buildOffsetNotify constructs a single-effect notification.
// EffectData is wire format: [4-byte LE keyLen][key][protoData].
// traceCtx is optional; when non-nil, OTel trace context is injected into the notification.
func buildOffsetNotify(nodeID pb.NodeID, offset Tip, eff *pb.Effect, data []byte, traceCtx context.Context) *pb.OffsetNotify {
	// Wire format: [4-byte LE keyLen][key][protoData]
	keyBytes := eff.Key
	wireData := make([]byte, 4+len(keyBytes)+len(data))
	binary.LittleEndian.PutUint32(wireData[:4], uint32(len(keyBytes)))
	copy(wireData[4:4+len(keyBytes)], keyBytes)
	copy(wireData[4+len(keyBytes):], data)

	notify := &pb.OffsetNotify{
		Origin:     toPbRef(offset),
		Hlc:        eff.Hlc,
		Key:        eff.Key,
		Deps:       eff.Deps,
		EffectData: wireData,
		SendTime:   uint64(time.Now().UnixNano()),
	}

	if traceCtx != nil {
		notify.TraceContext = tracing.InjectIntoBytes(traceCtx)
	}

	return notify
}
