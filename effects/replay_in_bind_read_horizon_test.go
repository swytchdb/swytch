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
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	clox "github.com/swytchdb/swytch/cache"
	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/crdt"
	"github.com/swytchdb/swytch/keytrie"
	"google.golang.org/protobuf/proto"
)

// Constants captured from the Jepsen run 26373595271 swytch.log files at
// /tmp/26373595271/latest/. Node IDs come from each node's "effects engine
// initialized" line; txn IDs come from the bind anchors named in the
// recorded reconstruct/HorizonSet log lines.
const (
	replay26373595271NodeN103 = 7643578538326529083
	replay26373595271NodeN117 = 7643578539064450548
	replay26373595271NodeN3   = 7643578540917157424

	// The 107 bind on n_117 (process 1's append 0 107).
	replay26373595271Txn107 = "7643578539064450548:1779659316683817690:6"
	// The 211 bind on n_3 (process 2's append 0 211) — the R bind whose
	// in-horizon state during n_103's in-bind reconstruct produced the
	// :incompatible-order anomaly.
	replay26373595271Txn211 = "7643578540917157424:1779659316806205225:3"
)

// TestReplayRun26373595271InBindReadDiverges asserts the read-side
// monotonicity property: a reconstruct that visits a bind held in
// HorizonSet must NOT return a result that's strictly "smaller" (missing
// element IDs) than a reconstruct from the same logical tip set after
// that bind is promoted via MakeVisible. Per the project model
// (CLAUDE.md, "HorizonSet's wait IS load-bearing"), reads must only ever
// observe post-fork-choice state. An in-bind read that returns a
// "future without its past" — the descendant snapshot tip n_117:50 is
// included while its ancestor bind n_3:27 is skipped — surfaces as
// Elle's :incompatible-order anomaly: the in-bind reply [107 553] and
// the post-promotion reply [107 211 553] are not prefix-comparable.
//
// Recorded artifact: Jepsen run 26373595271.
//
//	/tmp/26373595271/latest/ANALYSIS.md
//	/tmp/26373595271/latest/10.0.0.103/swytch.log lines 1601..1715
//	/tmp/26373595271/latest/10.0.0.3/swytch.log lines 1396..1755
//
// Anchor events on n_103 (NodeID 7643578538326529083) reproduced here:
//
//  1. 21:48:36.822097 — bind R = n_3:27 (txn …:3, "211" append) arrives
//     via HandleRemote with bind.Keys = {el-0, el-2}. Lands in
//     HorizonSet (invisible) and in el-0 / el-2 tip indexes.
//
//  2. 21:48:36.960384 — n_117:50 verdict snapshot (state-carrying in the
//     real run, verdict-only here — see "Divergences from the timeline"
//     in the test return message) lands. Its deps include n_3:27.
//     HandleRemote's dep-remove step (effect.go ~line 885) pulls n_3:27
//     out of el-0's current tip set. R is now reachable only as a DAG
//     ancestor via n_117:50's dep edge, NOT as a current tip. R is
//     still invisible in HorizonSet because n_117:50's TxnVerdicts only
//     names the 107 bind (n_117 hadn't yet adjudicated R at that wall
//     clock).
//
//  3. 21:48:37.138071 — n_103 begins txn T (id …:5). T emits a tx-marked
//     data effect on el-0 (= n_103:40 in the recorded log, value 553)
//     then calls GetSnapshot for el-0 — this is the in-bind read whose
//     return value [107 553] the client ultimately receives via [:r 0 …]
//     in the EXEC reply at 21:48:37.902.
//
//  4. 21:48:37.232993 — n_3 commits R locally and broadcasts its verdict
//     snapshot; n_103 receives it and calls HorizonSet.MakeVisible for
//     R. Any reconstruct on el-0 from the same physical tip set will
//     now include R.
//
// Assertion: the set of KEYED element IDs returned by the in-bind read
// must be a superset (modulo T's own pending write 553) of the set
// returned by a fresh reconstruct from {R, current-index-tips} after
// MakeVisible. The recorded run violates this property — the in-bind
// read is missing 211 — and so does this replay.
//
// Synthesis note: the log records offsets, deps, txnIDs, and per-bind
// ConsumedTips/NewTip but NOT the DataEffect payloads. We synthesize
// KEYED-collection inserts with element IDs equal to the recorded
// numeric values ("107", "211", "553"). The reduced state's NetAdds
// map is a set whose membership we assert on directly.
func TestReplayRun26373595271InBindReadDiverges(t *testing.T) {
	if os.Getenv("REPLAY_DEBUG") != "" {
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	}

	e := newInBindReadReplayEngine(t)
	markInBindReadSubscribed(e, "el-0")
	markInBindReadSubscribed(e, "el-2")

	// --- Step 1: replay the n_117:49 ("107") bind as a remote arrival,
	// then explicitly MakeVisible. In the recorded run this matched
	// 21:48:36.685..821 (data emits + bind) and 21:48:36.960384 (the
	// HorizonSet.MakeVisible on n_103). The bind anchor offset is taken
	// verbatim from ANALYSIS.md row 3 of the "Append 107" subsection.
	bind107Off := Tip{uint64(replay26373595271NodeN117), 49}
	dataN117_43Off := Tip{uint64(replay26373595271NodeN117), 43}
	dataN117_43 := &pb.Effect{
		Key:            []byte("el-0"),
		Hlc:            sTs(1_000_100),
		NodeId:         uint64(replay26373595271NodeN117),
		ForkChoiceHash: ComputeForkChoiceHash(replay26373595271NodeN117, sTs(1_000_100)),
		TxnId:          replay26373595271Txn107,
		Kind:           &pb.Effect_Data{Data: keyedInsertRaw("107", []byte("v107"))},
	}
	e.effectCache.Put(dataN117_43Off, dataN117_43)
	bind107 := &pb.TransactionalBindEffect{
		TxnHlc:           sTs(1_000_200),
		OriginatorNodeId: uint64(replay26373595271NodeN117),
		Keys: []*pb.TransactionalBindEffect_KeyBind{
			{Key: []byte("el-0"), NewTip: toPbRef(dataN117_43Off)},
		},
	}
	bind107Eff := &pb.Effect{
		Key:            []byte("el-0"),
		Hlc:            sTs(1_000_200),
		NodeId:         uint64(replay26373595271NodeN117),
		ForkChoiceHash: ComputeForkChoiceHash(replay26373595271NodeN117, sTs(1_000_200)),
		TxnId:          replay26373595271Txn107,
		Deps:           []*pb.EffectRef{toPbRef(dataN117_43Off)},
		Kind:           &pb.Effect_TxnBind{TxnBind: bind107},
	}
	pushInBindEffect(t, e, replay26373595271NodeN117, 49, bind107Eff)
	e.horizon.MakeVisible(replay26373595271Txn107)

	// --- Step 2: replay R = n_3:27 (the "211" bind) arrival. R has
	// bind.Keys = {el-0, el-2} with NewTip on el-0 = n_3:26, NewTip on
	// el-2 = n_3:24, ConsumedTips on el-0 = [n_3:21], ConsumedTips on
	// el-2 = [n_3:20]. All offsets / refs verbatim from the ANALYSIS.md
	// DAG section. The recorded run logs this as
	// "21:48:36.821669, n_3, Emit, el-0, [n_3,27]" then n_3 broadcasts;
	// n_103 receives at 21:48:36.822080 ("HandleRemote: processing remote
	// bind").
	bindROff := Tip{uint64(replay26373595271NodeN3), 27}
	r3_21 := &pb.EffectRef{NodeId: uint64(replay26373595271NodeN3), Offset: 21}
	r3_26 := &pb.EffectRef{NodeId: uint64(replay26373595271NodeN3), Offset: 26}
	r3_20 := &pb.EffectRef{NodeId: uint64(replay26373595271NodeN3), Offset: 20}
	r3_24 := &pb.EffectRef{NodeId: uint64(replay26373595271NodeN3), Offset: 24}

	dataR_el0 := &pb.Effect{
		Key:            []byte("el-0"),
		Hlc:            sTs(2_000_026),
		NodeId:         uint64(replay26373595271NodeN3),
		ForkChoiceHash: ComputeForkChoiceHash(replay26373595271NodeN3, sTs(2_000_026)),
		TxnId:          replay26373595271Txn211,
		Kind:           &pb.Effect_Data{Data: keyedInsertRaw("211", []byte("v211"))},
	}
	e.effectCache.Put(Tip{uint64(replay26373595271NodeN3), 26}, dataR_el0)
	dataR_el2 := &pb.Effect{
		Key:            []byte("el-2"),
		Hlc:            sTs(2_000_024),
		NodeId:         uint64(replay26373595271NodeN3),
		ForkChoiceHash: ComputeForkChoiceHash(replay26373595271NodeN3, sTs(2_000_024)),
		TxnId:          replay26373595271Txn211,
		Kind:           &pb.Effect_Data{Data: keyedInsertRaw("210", []byte("v210"))},
	}
	e.effectCache.Put(Tip{uint64(replay26373595271NodeN3), 24}, dataR_el2)

	bindR := &pb.TransactionalBindEffect{
		TxnHlc:           sTs(2_000_100),
		OriginatorNodeId: uint64(replay26373595271NodeN3),
		Keys: []*pb.TransactionalBindEffect_KeyBind{
			{Key: []byte("el-0"), ConsumedTips: []*pb.EffectRef{r3_21}, NewTip: r3_26},
			{Key: []byte("el-2"), ConsumedTips: []*pb.EffectRef{r3_20}, NewTip: r3_24},
		},
	}
	bindREff := &pb.Effect{
		Key:            []byte("el-0"),
		Hlc:            sTs(2_000_100),
		NodeId:         uint64(replay26373595271NodeN3),
		ForkChoiceHash: ComputeForkChoiceHash(replay26373595271NodeN3, sTs(2_000_100)),
		TxnId:          replay26373595271Txn211,
		Deps:           []*pb.EffectRef{r3_26, r3_24},
		Kind:           &pb.Effect_TxnBind{TxnBind: bindR},
	}
	pushInBindEffect(t, e, replay26373595271NodeN3, 27, bindREff)

	if !e.horizon.IsInvisible(replay26373595271Txn211) {
		t.Fatalf("setup: R (n_3:27) must be invisible immediately after HandleRemote")
	}

	// --- Step 3: replay n_117:50 — a verdict-only snapshot whose deps
	// include n_3:27. HandleRemote's dep-remove step strips n_3:27 out
	// of el-0's tip index. R is still in HorizonSet because n_117:50's
	// TxnVerdicts only names the 107 bind; applySnapshotVerdicts does not
	// promote R.
	//
	// Divergence from the recorded run: the real n_117:50 was
	// state-carrying. A state-carrying LCA snapshot trims its DAG
	// ancestors out of reconstruct's walk, which would make R structurally
	// invisible regardless of HorizonSet. The bug we want to reproduce
	// is read-side horizon skipping; using a verdict-only snapshot
	// preserves R's structural reachability so the reconstruct visits R
	// and applies the horizon-invisible check there. This is faithful to
	// the bug class but not to the literal recorded payload — documented
	// in "Divergences from the timeline" in the return message.
	snapN117_50 := &pb.Effect{
		Key:            []byte("el-0"),
		Hlc:            sTs(1_500_000),
		NodeId:         uint64(replay26373595271NodeN117),
		ForkChoiceHash: ComputeForkChoiceHash(replay26373595271NodeN117, sTs(1_500_000)),
		Deps: []*pb.EffectRef{
			toPbRef(bind107Off),
			toPbRef(bindROff),
		},
		Kind: &pb.Effect_Snapshot{Snapshot: &pb.SnapshotEffect{
			Collection: pb.CollectionKind_KEYED,
			State:      nil,
			TxnVerdicts: map[string]pb.Verdict{
				replay26373595271Txn107: pb.Verdict_WON,
			},
		}},
	}
	pushInBindEffect(t, e, replay26373595271NodeN117, 50, snapN117_50)

	// Confirm the structural setup the hypothesis depends on:
	tipsAfterSnap := e.index.Contains("el-0")
	if tipsAfterSnap == nil {
		t.Fatalf("setup: el-0 must have tips after snapshot arrival")
	}
	for _, tp := range tipsAfterSnap.Tips() {
		if tp == bindROff {
			t.Fatalf("setup: R (n_3:27) must NOT be a current tip after n_117:50 consumed it as a dep (got tips=%v)", tipsAfterSnap.Tips())
		}
	}
	if !e.horizon.IsInvisible(replay26373595271Txn211) {
		t.Fatalf("setup: R must still be invisible after snapshot (no verdict for R)")
	}
	t.Logf("post-snapshot el-0 tips = %v; R is reachable only as DAG ancestor via n_117:50 deps; R IsInvisible=%v",
		tipsAfterSnap.Tips(), e.horizon.IsInvisible(replay26373595271Txn211))

	// --- Step 4: capture the tip set the in-bind read will operate
	// against. This is also the tip set the post-promotion read will use,
	// so both reads see the same logical DAG cut.
	preTxTips := append([]Tip(nil), tipsAfterSnap.Tips()...)

	// --- Step 5: open a Context, begin a transaction, emit a tx-marked
	// data effect on el-0 with value 553 (modelling n_103:40 in the
	// recorded log), then call Context.GetSnapshot for el-0.
	//
	// Under the fix, GetSnapshot's reconstruct walk hits R on the ancestry,
	// sees IsInvisible(R)=true, and blocks on the entry's waiter mutex.
	// Step 6 fires MakeVisible from a goroutine to release the wait. After
	// the wait returns, reconstruct restarts with R visible and includes
	// it in the result. The committed-element view of the in-bind read
	// then matches a post-promotion read from the same tip set.
	go func() {
		// Small delay so the read goroutine has time to enter WaitForClear.
		// 50ms is generous; the RLock acquire is ~microseconds.
		time.Sleep(50 * time.Millisecond)
		e.horizon.MakeVisible(replay26373595271Txn211)
	}()

	ctx := e.NewContext()
	ctx.BeginTx()
	if err := ctx.Emit(&pb.Effect{
		Key:  []byte("el-0"),
		Kind: &pb.Effect_Data{Data: keyedInsertRaw("553", []byte("v553"))},
	}); err != nil {
		t.Fatalf("Emit local 553: %v", err)
	}
	inBindReduced, _, err := ctx.GetSnapshot("el-0")
	if err != nil {
		t.Fatalf("in-bind GetSnapshot: %v", err)
	}
	inBindIDs := inBindReadIDSet(inBindReduced)
	t.Logf("in-bind read of el-0 (txn %s in flight, 553 pending) returned IDs=%v",
		ctx.txnID, sortedKeys(inBindIDs))

	if e.horizon.IsInvisible(replay26373595271Txn211) {
		t.Fatalf("MakeVisible should have cleared R from invisible set during the wait")
	}

	// --- Step 6: do a fresh reconstruct from the SAME logical tip set
	// preTxTips (the index state BEFORE T's emits). With R promoted, this
	// is the read every other client / process would do post-promotion.
	postReduced, _, err := e.reconstruct("el-0", preTxTips, "", false)
	if err != nil {
		t.Fatalf("post-promotion reconstruct: %v", err)
	}
	postIDs := inBindReadIDSet(postReduced)
	t.Logf("post-promotion reconstruct of el-0 (same tips=%v as in-bind read) returned IDs=%v",
		preTxTips, sortedKeys(postIDs))

	// --- The assertion. The in-bind read additionally carries T's
	// pending write 553, so we subtract 553 before comparing. After the
	// fix, the in-bind read waited for R's promotion and now includes R's
	// element — every committed element the post-promotion read sees must
	// be in the in-bind committed view. The recorded anomaly (211 ∈ postIDs
	// but 211 ∉ inBindIDs) is gone iff the wait fired.
	pendingIDs := map[string]struct{}{"553": {}}
	inBindCommitted := subtractSet(inBindIDs, pendingIDs)

	missing := elementsInOnlyOneSet(postIDs, inBindCommitted)
	if len(missing) > 0 {
		t.Fatalf("in-bind read returned a result MISSING element IDs that are present "+
			"in a same-tips reconstruct after MakeVisible: missing=%v "+
			"(in-bind committed=%v, post-promotion=%v). "+
			"The waitForHorizon path in Context.GetSnapshot should have blocked the "+
			"read on R's HorizonSet entry until the goroutine's MakeVisible fired, "+
			"and reconstruct should have restarted with R visible. "+
			"R: txn %s at %v.",
			sortedKeys(missing),
			sortedKeys(inBindCommitted), sortedKeys(postIDs),
			replay26373595271Txn211, bindROff)
	}
}

// --- helpers (kept in this file to avoid coupling with the existing
// replay test's helpers, which are slightly differently scoped) ---

// newInBindReadReplayEngine builds a minimally-wired Engine matching
// n_103's NodeID (7643578538326529083) with horizon installed and fake
// timers, so the 5s crash-fallback doesn't promote R out from under the
// test.
func newInBindReadReplayEngine(t *testing.T) *Engine {
	t.Helper()
	e := &Engine{
		effectCache:       clox.NewCloxCache[Tip, *pb.Effect](clox.ConfigFromMemorySize(1024 * 1024)),
		index:             keytrie.New(),
		broadcaster:       &mockBroadcaster{},
		nodeID:            replay26373595271NodeN103,
		clock:             crdt.NewHLC(),
		subscriptions:     xsync.NewMap[string, *subscriptionState](),
		pendingTxns:       xsync.NewMap[Tip, *pendingTxn](),
		pendingTxTips:     xsync.NewMap[Tip, []Tip](),
		txAbortCounts:     xsync.NewMap[string, *atomic.Int32](),
		pendingBootstraps: xsync.NewMap[string, *bootstrapCollector](),
		peerSubscribers:   xsync.NewMap[string, *xsync.Map[pb.NodeID, struct{}]](),
		unsubInFlight:     xsync.NewMap[string, struct{}](),
		spokenBinds:       clox.NewCloxCache[Tip, struct{}](clox.ConfigFromCapacity(256)),
		txSnapshots:       xsync.NewMap[string, keytrie.KeyIndex](),
	}
	e.safety.Store(&safetyMap{defaultMode: UnsafeMode})
	e.horizon = newHorizonSet(e)
	installFakeTimers(e.horizon)
	return e
}

// markInBindReadSubscribed installs a ready subscription so that the
// authority gate in HandleRemote accepts inbound effects for the key.
func markInBindReadSubscribed(e *Engine, key string) {
	st := &subscriptionState{ready: make(chan struct{})}
	close(st.ready)
	e.subscriptions.Store(key, st)
}

// pushInBindEffect routes a synthetic remote effect through HandleRemote
// so it lands in the cache, the index, and (for binds) HorizonSet — the
// same path a real network arrival would take.
func pushInBindEffect(t *testing.T, e *Engine, originNode pb.NodeID, off uint64, eff *pb.Effect) {
	t.Helper()
	data, err := proto.Marshal(eff)
	if err != nil {
		t.Fatalf("marshal effect: %v", err)
	}
	notify := BuildOffsetNotify(originNode, Tip{uint64(originNode), off}, eff, data, nil)
	if _, err := e.HandleRemote(notify); err != nil {
		t.Fatalf("HandleRemote: %v", err)
	}
}

// inBindReadIDSet pulls the KEYED-collection NetAdds element-ID set out
// of a ReducedEffect as a Go set.
func inBindReadIDSet(r *pb.ReducedEffect) map[string]struct{} {
	out := make(map[string]struct{})
	if r == nil || r.NetAdds == nil {
		return out
	}
	for id := range r.NetAdds {
		out[id] = struct{}{}
	}
	return out
}

// subtractSet returns the elements of a not in b.
func subtractSet(a, b map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(a))
	for k := range a {
		if _, ok := b[k]; !ok {
			out[k] = struct{}{}
		}
	}
	return out
}

// elementsInOnlyOneSet returns elements in want that are missing from got.
func elementsInOnlyOneSet(want, got map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{})
	for k := range want {
		if _, ok := got[k]; !ok {
			out[k] = struct{}{}
		}
	}
	return out
}

// sortedKeys returns the keys of m as a stable slice (for log readability).
func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	// Manual sort to avoid pulling in a new import; lexicographic is fine.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
