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
	"sort"
	"testing"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/keytrie"
)

// TestBindConsumedTipsMatchEmittedDeps isolates the deps-vs-ConsumedTips
// parity asymmetry that produced the G1a / dirty-update in Jepsen run
// 27070432424 (element 31947, key el-1).
//
// The invariant under test: for a committing transaction, the bind's
// ConsumedTips on a key must name the SAME post-substitution causal base
// that the emitted data effect's Deps on that key name. A committed txn
// must never list in its ConsumedTips a tip that its own Deps refused to
// depend on.
//
// In Context.Emit (context.go ~565) the emitted data effect's Deps go
// through resolveTipDeps — a foreign IN-FLIGHT tx-marked tip is replaced
// by its pre-tx committed base. But initialTips is stored RAW (the live
// index tips), and the bind's ConsumedTips is built raw from initialTips
// (context.go ~986). So when a foreign in-flight tx tip is present in the
// captured tip set, Deps EXCLUDE it (substituted to base) while
// ConsumedTips INCLUDE it. That asymmetry is the leak: a committed txn's
// ConsumedTips can name another (possibly-aborting) txn's in-progress
// effect — exactly how A:491 / 31947 (an in-flight effect of a txn that
// later aborted) became part of committed bind B:527's el-1 ConsumedTips.
//
// Models these (timestamp, node, event) triples from ANALYSIS.md:
//   (18:59:09.382483, .92/A, Emit el-1 append value 31947 -> A:491, tx:true)   = foreign in-flight tx tip
//   (18:59:09.383360, .217/B, HandleRemote ingests A:491 as a live tip)         = foreign tip live in B's index + pendingTxTips
//   (18:59:09.410815, .217/B, Emit competing BIND anchor B:527, tx:true)        = the committing txn whose ConsumedTips we inspect
//   (B:527 el-1 ConsumedTips = [B:522, A:491])                                   = raw initialTips, includes the foreign in-flight tip
//
// Here node 42 plays B (.217, the committing originator). Tip{7,100} plays
// B's own committed base (B:522). Tip{8,200} plays the FOREIGN in-flight
// tx tip A:491 (the failed op-801 append carrying 31947).
func TestBindConsumedTipsMatchEmittedDeps(t *testing.T) {
	const key = "el-1"

	bc := &mockBroadcaster{}
	e := newTestEngine(bc)

	// Subscribe locally to el-1 so HandleRemote treats this node as
	// authoritative and indexes the foreign in-flight effect (the inbound
	// authority gate, effect.go ~875, only indexes for subscribed keys).
	// markReady so flushTx's ensureSubscribed sees a completed bootstrap
	// and doesn't block waiting on the ready channel.
	subState := &subscriptionState{ready: make(chan struct{})}
	subState.markReady()
	e.subscriptions.Store(key, subState)

	base := Tip{7, 100}    // committed base on el-1 (plays B:522)
	foreign := Tip{8, 200} // foreign in-flight tx tip on el-1 (plays A:491 / 31947)

	// (1) Committed base effect on el-1: in index + cache. No TxnId — fully
	// committed, the legitimate causal base.
	baseEff := &pb.Effect{
		Key:            []byte(key),
		Hlc:            sTs(100),
		NodeId:         base[0],
		ForkChoiceHash: ComputeForkChoiceHash(pb.NodeID(base[0]), sTs(100)),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_ORDERED,
			Value:      &pb.DataEffect_Raw{Raw: []byte("base")},
		}},
	}
	e.effectCache.Put(base, baseEff)
	e.index.Insert(key, nil, keytrie.NewTipSet(base))

	// (2) Foreign IN-FLIGHT tx-marked effect on el-1: TxnId set, NO bind,
	// deps = [base]. Ingest via HandleRemote so it becomes a live index tip
	// AND lands in pendingTxTips (effect.go ~856-858). This is the
	// asymmetric "started before the other committed" case — the foreign
	// txn's data write before its bind anchor (which, in the run, never
	// committed).
	foreignEff := &pb.Effect{
		Key:            []byte(key),
		Hlc:            sTs(200),
		NodeId:         foreign[0],
		ForkChoiceHash: ComputeForkChoiceHash(pb.NodeID(foreign[0]), sTs(200)),
		TxnId:          "foreign-inflight-txn-68",
		Deps:           toPbRefs([]Tip{base}),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_ORDERED,
			Value:      &pb.DataEffect_Raw{Raw: []byte("31947")},
		}},
	}
	foreignData, err := MarshalEffect(foreignEff)
	if err != nil {
		t.Fatalf("MarshalEffect(foreign): %v", err)
	}
	foreignNotify := BuildOffsetNotify(
		pb.NodeID(foreign[0]), foreign, foreignEff, foreignData, nil,
	)
	if _, err := e.HandleRemote(foreignNotify); err != nil {
		t.Fatalf("HandleRemote(foreign in-flight tip): %v", err)
	}

	// Verify the foreign tip is BOTH a live index tip AND in pendingTxTips.
	idxTips := e.index.Contains(key)
	if idxTips == nil {
		t.Fatal("el-1 has no index tips after ingesting foreign in-flight effect")
	}
	if !idxTips.Contains(foreign) {
		t.Fatalf("foreign in-flight tip %v is not a live index tip; tips=%v", foreign, idxTips.Tips())
	}
	if _, ok := e.pendingTxTips.Load(foreign); !ok {
		t.Fatalf("foreign in-flight tip %v not registered in pendingTxTips", foreign)
	}

	// (4) Run a real committing transaction on el-1 with NO subscribers, so
	// flushTx takes the immediate-commit fast path (no NACK, no abort). At
	// Emit time the live index tips are {base, foreign}; Emit captures them
	// RAW into initialTips and runs resolveTipDeps for eff.Deps.
	ctx := e.NewContext()
	ctx.BeginTx()
	if err := ctx.Emit(dataEffect(key)); err != nil {
		t.Fatalf("Emit data effect: %v", err)
	}
	txnID := ctx.txnID // Flush's reset() clears ctx.txnID — capture it now
	if err := ctx.Flush(); err != nil {
		t.Fatalf("expected commit, got: %v", err)
	}

	// Locate the committed BIND on el-1 and the DATA effect it anchors.
	committedTips := e.index.Contains(key)
	if committedTips == nil {
		t.Fatal("el-1 has no tips after commit")
	}
	var (
		bindEff *pb.Effect
		dataEff *pb.Effect
	)
	for _, off := range committedTips.Tips() {
		eff, ok := e.effectCache.Get(off)
		if !ok {
			continue
		}
		if b := eff.GetTxnBind(); b != nil && eff.TxnId == txnID {
			bindEff = eff
			// The bind's NewTip on el-1 points to our committed data effect.
			for _, kb := range b.Keys {
				if string(kb.Key) != key {
					continue
				}
				if d, ok := e.effectCache.Get(r(kb.NewTip)); ok {
					dataEff = d
				}
			}
		}
	}
	if bindEff == nil {
		t.Fatal("did not find our committed bind on el-1")
	}
	if dataEff == nil {
		t.Fatal("did not find the data effect anchored by our bind's el-1 NewTip")
	}

	// Extract the data effect's resolved Deps on el-1.
	depSet := offsetSet(fromPbRefs(dataEff.Deps))

	// Extract the bind's ConsumedTips on el-1.
	var consumed []Tip
	for _, kb := range bindEff.GetTxnBind().Keys {
		if string(kb.Key) == key {
			consumed = fromPbRefs(kb.ConsumedTips)
		}
	}
	consumedSet := offsetSet(consumed)

	t.Logf("data effect Deps[%s]      = %v", key, sortedTips(depSet))
	t.Logf("bind ConsumedTips[%s]     = %v", key, sortedTips(consumedSet))
	t.Logf("base (committed)          = %v", base)
	t.Logf("foreign (in-flight tx)    = %v", foreign)

	// (5) PARITY ASSERTION: the bind's ConsumedTips on el-1 must equal the
	// data effect's resolved Deps on el-1. Both should be the SUBSTITUTED
	// committed base {base}, NOT the foreign in-flight tip.
	if !sameTipSet(depSet, consumedSet) {
		t.Fatalf("deps/ConsumedTips disagree on %s: Deps=%v but ConsumedTips=%v; "+
			"expected parity (both the substituted committed base %v, not the "+
			"foreign in-flight tip %v). A committed txn must never name in its "+
			"ConsumedTips a tip its own Deps refused to depend on.",
			key, sortedTips(depSet), sortedTips(consumedSet), base, foreign)
	}

	// Belt-and-suspenders: the foreign in-flight tip must not appear in
	// ConsumedTips at all — that is precisely the A:491 leak.
	if consumedSet[foreign] {
		t.Fatalf("committed bind ConsumedTips[%s] names the foreign in-flight "+
			"tx tip %v — this is the aborted-txn effect leak (G1a). ConsumedTips=%v",
			key, foreign, sortedTips(consumedSet))
	}
}

// --- small set helpers (test-local) ---

func offsetSet(tips []Tip) map[Tip]bool {
	m := make(map[Tip]bool, len(tips))
	for _, t := range tips {
		m[t] = true
	}
	return m
}

func sameTipSet(a, b map[Tip]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for t := range a {
		if !b[t] {
			return false
		}
	}
	return true
}

func sortedTips(m map[Tip]bool) []Tip {
	out := make([]Tip, 0, len(m))
	for t := range m {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i][0] != out[j][0] {
			return out[i][0] < out[j][0]
		}
		return out[i][1] < out[j][1]
	})
	return out
}
