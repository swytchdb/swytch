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
	"log/slog"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/keytrie"
	"github.com/swytchdb/swytch/tracing"
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
		if chainLen >= 20+rand.IntN(31) {
			slog.Debug("compaction: emitting snapshot",
				"key", key,
				"chainLen", chainLen,
				"tips", tips)
			if err := c.Emit(&pb.Effect{
				Key: []byte(key),
				Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
					Collection: result.Collection,
					State:      result,
				}},
			}, tips); err != nil {
				slog.Error("compaction: snapshot emit failed",
					"key", key,
					"chainLen", chainLen,
					"error", err)
			}
		}
		return filterSnapshot(result), tips, nil
	}

	// Reconstruct from lastOffset merged with index tips. When an SSI
	// snapshot is active (MULTI/EXEC), use the snapshot to preserve
	// isolation — concurrent writes must not be visible. Outside SSI,
	// use the live index so concurrent writes (e.g. XGROUP DESTROY
	// while a blocking XREADGROUP holds a compaction snapshot) are visible.
	myTip := ck.lastOffset
	reconTips := []Tip{myTip}
	var baseTips []Tip
	if c.txSnapshot != nil {
		if ts := c.txSnapshot.Contains(key); ts != nil {
			// Match getSnapshotFromTx: resolve pending-tx tips to their
			// pre-tx deps so a later read in the same txn walks the same
			// committed-as-of-snapshot DAG as the first read did.
			baseTips = c.engine.resolveTipDeps(ts.Tips())
		}
	} else if ts := c.engine.index.Contains(key); ts != nil {
		baseTips = ts.Tips()
	}
	for _, t := range baseTips {
		if t != myTip {
			reconTips = append(reconTips, t)
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
	if err := c.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	}, snapshotTips); err != nil {
		slog.Error("getSnapshotFromTx: NOOP emit failed", "key", key, "error", err)
		return nil, nil, err
	}

	return result, snapshotTips, nil
}

// Watch records a key for optimistic locking by emitting a NoopEffect
// immediately and flushing it. This records the observation in the
// causal log so all nodes can verify whether the key was modified
// between WATCH and EXEC. The NOOP offset and post-flush TipSet pointer
// are stored for comparison at BeginTx time.
//
// The noop is tx-marked with the upcoming transaction's id (generated
// lazily here). This is what makes write-skew detection work across
// nodes: a competing write on a watched key becomes a structural fork
// sibling of the watching tx's bind, surfaced by reconstruct's
// pairwise fork-choice on the key. Without the TxnId, the noop is an
// anonymous committed effect that other writers can chain off,
// producing false sequential relationships (the run-25890153673 G1a).
func (c *Context) Watch(key string) {
	if c.watchedKeys == nil {
		c.watchedKeys = make(map[string]*watchedKeyState)
	}

	// Lazily materialize the txn id so the WATCH noop is tx-marked.
	// The upcoming BeginTx (at MULTI/EXEC) inherits this id.
	if c.txnID == "" {
		c.txnID = c.engine.generateTxnID()
	}

	// Check if key has actual data BEFORE emitting the NOOP
	snap, _, _, _ := c.engine.GetSnapshot(key)
	hadData := snap != nil

	// Emit NOOP to record the observation in the causal log. We set
	// TxnId explicitly because Emit only stamps it inside an active
	// MULTI (c.inTx); WATCH is pre-MULTI but the noop still belongs
	// to the upcoming tx.
	if err := c.Emit(&pb.Effect{
		Key:   []byte(key),
		TxnId: c.txnID,
		Kind:  &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	}); err != nil {
		slog.Error("Watch: NOOP emit failed", "key", key, "error", err)
		return
	}

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

// ClearWatches removes all watched keys. Called from UNWATCH, DISCARD,
// and EXEC's pre-BeginTx abort paths — all cases where the upcoming
// transaction is being discarded. Also drops the lazily-generated
// txnID so the next WATCH/MULTI starts fresh; if we're already inside
// MULTI/EXEC (c.inTx) the id is load-bearing for in-flight effects
// and must be preserved.
func (c *Context) ClearWatches() {
	clear(c.watchedKeys)
	if !c.inTx {
		c.txnID = ""
	}
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
	// A prior WATCH may have already materialized our txn id; reuse it
	// so the WATCH noop and the MULTI/EXEC effects share one tx.
	if c.txnID == "" {
		c.txnID = c.engine.generateTxnID()
	}
	// SSI: snapshot the index at txn start so every read/emit within
	// the txn sees a consistent committed state.
	if c.txSnapshot == nil {
		c.txSnapshot = c.engine.index.Snapshot()
	}
}

// CheckWatches validates WATCH observations and emits transactional NOOPs
// for the Bind's read set. Must be called AFTER BeginTx (so NOOPs get
// IsTransactional=true). Only called from handleExec.
//
// Returns false if any watched key was modified — the caller should abort
// the transaction and return a null array to the client.
func (c *Context) CheckWatches() bool {
	// BeginTx already captured the SSI snapshot at txn start. Cover the
	// case where CheckWatches is called without a prior BeginTx (no
	// transactional emits before EXEC).
	if c.txSnapshot == nil {
		c.txSnapshot = c.engine.index.Snapshot()
	}

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
		c.txnID = ""
		c.txSnapshot = nil
		return false
	}

	// Emit transactional NOOPs using WATCH NOOP offsets as snapshotTips.
	// This forces deps on the WATCH-time state, creating a fork if the
	// key was modified (the fork is structurally detectable by any node).
	for key, ws := range c.watchedKeys {
		if err := c.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, []Tip{ws.noopOffset}); err != nil {
			slog.Error("CheckWatches: transactional NOOP emit failed", "key", key, "error", err)
			clear(c.watchedKeys)
			c.inTx = false
			c.txnID = ""
			c.txSnapshot = nil
			return false
		}
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
	clear(c.watchedKeys)
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

	data, err := MarshalEffect(eff)
	if err != nil {
		return Tip{}, nil, err
	}

	offset := c.engine.nextOffset()

	slog.Debug("Emit: wrote effect",
		"key", key, "offset", offset, "deps", eff.Deps, "tx", eff.TxnId != "")

	if c.engine.effectCache != nil {
		c.engine.effectCache.Put(offset, proto.Clone(eff).(*pb.Effect))
	}

	notify := BuildOffsetNotify(c.engine.nodeID, offset, eff, data, c.TraceCtx())
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

	// Protocol invariant: once this node has observed a remote bind on
	// key K, it must not emit its own bind that's DAG-concurrent with
	// the remote bind. Two ways that can happen, both caught here:
	//  (a) the remote bind consumed one of our initialTips — our tip
	//      is gone from current.
	//  (b) the remote bind landed as a new tip after we captured
	//      initialTips — current has a tip our initialTips never saw.
	// In (b), our emit won't dep-reference the remote bind so the two
	// commit DAG-concurrent. Abort here rather than let fork-choice
	// surface the inconsistency as G1a / incompatible-order later.
	for key, ck := range c.keys {
		if ck.initialTips == nil {
			continue // first write to a brand-new key
		}
		initial := ck.initialTips.Tips()
		if len(initial) == 0 {
			continue
		}
		initialSet := make(map[Tip]bool, len(initial))
		for _, t := range initial {
			initialSet[t] = true
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
		for t := range currentSet {
			if !initialSet[t] {
				slog.Debug("flushTx: concurrent remote bind landed, aborting before emission",
					"key", key, "new_tip", t, "initial_tips", initial)
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
	// Sort bind.Keys by key bytes so the canonical key (bind.Keys[0])
	// and every observer's iteration order are deterministic — c.keys
	// is a Go map, raw iteration is random. Any predicate or behavior
	// that branches on eff.Key (or on bind.Keys ordering) needs this
	// to be stable across runs.
	sort.Slice(bind.Keys, func(i, j int) bool {
		return bytes.Compare(bind.Keys[i].Key, bind.Keys[j].Key) < 0
	})

	// Bind deps: the last effect on each key in the transaction.
	// Walk bind.Keys (already sorted) so eff.Deps is deterministic too.
	bindDeps := make([]*pb.EffectRef, 0, len(bind.Keys))
	for _, kb := range bind.Keys {
		bindDeps = append(bindDeps, kb.NewTip)
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
	// Returns true if our bind lost — abort immediately without
	// re-reading voidedBinds (the cache write is still done for
	// cross-transaction visibility in reconstruct/checkCompetingBinds).
	if c.engine.evaluateBindForkChoice(bind, bindOffset, bindEff.ForkChoiceHash, c.txnID) {
		slog.Debug("flushTx: voided by concurrent bind, aborting",
			"bind_offset", bindOffset, "txn", c.txnID)
		for _, ck := range c.keys {
			c.engine.pendingTxTips.Delete(ck.lastOffset)
		}
		c.reset()
		return ErrTxnAborted
	}

	// Step 4: Create pending txn for NACK tracking
	ptxn := &pendingTxn{
		txnID:          c.txnID,
		txnHLC:         txnHLC,
		originNode:     c.engine.nodeID,
		forkChoiceHash: ComputeForkChoiceHash(c.engine.nodeID, hlcTs),
		bindOffset:     bindOffset,
		done:           make(chan struct{}),
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
			// Peers exist — bind stays invisible until we finish processing
			// peer responses and explicitly call MakeVisible (commit) or
			// Abort (abort) below. The decision must happen AFTER the NACK
			// loop, otherwise the bind can briefly become visible while a
			// concurrent reader picks up its effects before isRealConflict
			// has had a chance to fire.
			if c.engine.horizon != nil {
				c.engine.horizon.Add(c.txnID, bindOffset, bind)
			}
			// Replicate to all subscribers concurrently, collect responses.
			// Per the TLA+ spec (ExactlyOnce.CommitPop): commit requires
			// every subscriber to respond (ACK or NACK). A peer that ACK'd
			// has committed via the "once you've spoken" invariant to NACK
			// any subsequent competing bind on this key. A peer we couldn't
			// reach hasn't made that commitment, so we cannot rely on it
			// to enforce AtMostOneCommit.
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

			responseCount := 0
			for _, res := range results {
				if res.err != nil {
					slog.Warn("flushTx: ReplicateTo failed",
						"bind_offset", bindOffset,
						"target", res.subID,
						"error", res.err)
					continue
				}
				responseCount++
				for _, nack := range res.nacks {
					slog.Debug("flushTx: received NACK",
						"bind_offset", bindOffset,
						"target", res.subID,
						"nack_key", string(nack.Key),
						"tip_count", len(nack.TipDetails))
					// Pull every tip the peer mentions into our DAG
					// before deciding conflict — the NACK is informational
					// and ignoring it would let the cluster diverge.
					c.engine.ingestNackTips(nack)
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
				if responseCount < len(subscribers) {
					slog.Warn("flushTx: not all subscribers responded, aborting",
						"bind_offset", bindOffset,
						"responses", responseCount,
						"subscribers", len(subscribers))
					abortPendingTxn(ptxn)
				} else if commitPendingTxn(ptxn) {
					slog.Debug("flushTx: all subscribers responded, committed",
						"bind_offset", bindOffset,
						"subscribers", len(subscribers))
				}
			}

			// Drive visibility from here, AFTER the NACK loop has had a
			// chance to set txnStateAborted via isRealConflict. The bind
			// must not become visible before this decision is made.
			if c.engine.horizon != nil {
				if ptxn.state.Load() == txnStateAborted {
					c.engine.horizon.Abort(c.txnID)
				} else {
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

// BuildOffsetNotify constructs a single-effect notification.
// EffectData is wire format: [4-byte LE keyLen][key][protoData].
// traceCtx is optional; when non-nil, OTel trace context is injected into the notification.
func BuildOffsetNotify(nodeID pb.NodeID, offset Tip, eff *pb.Effect, data []byte, traceCtx context.Context) *pb.OffsetNotify {
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
