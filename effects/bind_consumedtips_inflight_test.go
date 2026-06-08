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
	"testing"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/keytrie"
)

// TestBindConsumedTipsSubstitutesForeignInflightTip reproduces the
// ConsumedTips half of Jepsen run 27070432424's :G1a / :dirty-update.
//
// Mechanism under test (CLAUDE.md "resolveTipDeps substitutes pending tx
// tips"): a transaction captures its ConsumedTips RAW from the index tips
// at Emit time (context.go:985-986, `consumedTips = toPbRefs(
// ck.initialTips.Tips())`), while the matching eff.Deps for the same key
// are run through resolveTipDeps (context.go:565). When a FOREIGN in-flight
// tx-marked effect (TxnId != "", no bind yet — "inert state in the DAG"
// per CLAUDE.md) is the live index tip at Emit time, the committing
// transaction records that foreign in-flight effect in its bind's
// ConsumedTips. eff.Deps, by contrast, correctly substitutes it for its
// pre-tx predecessor. If the originating txn later aborts, its in-progress
// write has leaked into the committing txn's committed causal base.
//
// Timeline rows modeled (ANALYSIS.md "Timeline" / "DAG"):
//   - (18:59:09.382483, 10.0.2.92, "Emit: wrote effect" el-1 value 31947,
//     offset A:491, tx:true, no bind) — the foreign in-flight append. Here:
//     foreignInflight.
//   - (18:59:09.383360, 10.0.0.217, "HandleRemote: updating index" el-1
//     offset 491) — A:491 ingested at the competing node while still
//     in-flight. Here: e.HandleRemote(foreignInflight).
//   - (18:59:09.410815, 10.0.0.217, "Emit: wrote effect" competing bind
//     anchor B:527, bind el-1 ConsumedTips = [B:522, A:491]) — the
//     committed bind records the foreign in-flight tip A:491 in its el-1
//     ConsumedTips. Here: the bind this test captures.
//   - (18:59:09.412389, 10.0.2.92, "flushTx: voided by concurrent bind,
//     aborting" txn ...68) — A:491's originating txn aborts, so A:491 is a
//     dirty value that B:527 has committed atop.
//
// The assertion: the committed bind's ConsumedTips on the key must NOT
// contain the foreign in-flight tip. The correct value is `base` (the
// pre-tx committed predecessor), exactly what resolveTipDeps substitutes
// and exactly what the emitted data effect's Deps already resolved to.
func TestBindConsumedTipsSubstitutesForeignInflightTip(t *testing.T) {
	const key = "el-1"
	const foreignNode = pb.NodeID(7) // the op-801 origin (.92) analog

	bc := &txnMockBroadcaster{}
	e := newTxnTestEngine(bc) // nodeID 42 (the competing .217 analog)

	// Mark the key already subscribed + ready so (a) HandleRemote treats us
	// as authoritative and indexes the foreign tx-marked effect, and (b)
	// flushTx's ensureSubscribed is a no-op (no SubscriptionEffect emitted
	// mid-flush that would perturb the tip set).
	subState := &subscriptionState{ready: make(chan struct{})}
	close(subState.ready)
	e.subscriptions.Store(key, subState)

	// Step 2: committed base on the key — a plain, non-tx data effect in
	// both index and effectCache. This is what a correct ConsumedTips/deps
	// SHOULD reference.
	base := Tip{99, 400}
	baseEff := &pb.Effect{
		Key:            []byte(key),
		Hlc:            sTs(400),
		NodeId:         99,
		ForkChoiceHash: ComputeForkChoiceHash(99, sTs(400)),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_ORDERED,
			Value:      &pb.DataEffect_Raw{Raw: []byte("90")},
		}},
	}
	e.effectCache.Put(base, baseEff)
	e.index.Insert(key, nil, keytrie.NewTipSet(base))

	// Step 3: inject the FOREIGN in-flight tx-marked effect on the same key:
	// TxnId != "", no bind, depping on base. Ingest via HandleRemote so it
	// lands through the real path (cache + index + pendingTxTips).
	foreignInflight := Tip{uint64(foreignNode), 491}
	foreignEff := &pb.Effect{
		Key:            []byte(key),
		Hlc:            sTs(491),
		NodeId:         uint64(foreignNode),
		ForkChoiceHash: ComputeForkChoiceHash(foreignNode, sTs(491)),
		TxnId:          "7648358837138979750:1780772349384662707:68", // op-801's txn
		Deps:           []*pb.EffectRef{toPbRef(base)},
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_ORDERED,
			Value:      &pb.DataEffect_Raw{Raw: []byte("31947")}, // the dirty element
		}},
	}
	foreignData, err := MarshalEffect(foreignEff)
	if err != nil {
		t.Fatal(err)
	}
	notify := BuildOffsetNotify(foreignNode, foreignInflight, foreignEff, foreignData, nil)
	if _, err := e.HandleRemote(notify); err != nil {
		t.Fatal(err)
	}

	// Confirm the foreign in-flight effect is (a) a live index tip and (b)
	// tracked in pendingTxTips with base as its pre-tx dep.
	idx := e.index.Contains(key)
	if idx == nil || !idx.Contains(foreignInflight) {
		t.Fatalf("foreign in-flight tip %v is not a live index tip; tips=%v",
			foreignInflight, idx)
	}
	preTxDeps, ok := e.pendingTxTips.Load(foreignInflight)
	if !ok {
		t.Fatalf("foreign in-flight tip %v not tracked in pendingTxTips", foreignInflight)
	}
	if len(preTxDeps) != 1 || preTxDeps[0] != base {
		t.Fatalf("pendingTxTips[%v] = %v; want pre-tx dep [%v]",
			foreignInflight, preTxDeps, base)
	}

	// Step 4: run a local transaction that writes the same key, then commit.
	// No subscribers → flushTx commits immediately and emits the bind.
	ctx := e.NewContext()
	ctx.BeginTx()
	if err := ctx.Emit(dataEffect(key)); err != nil {
		t.Fatal(err)
	}
	if err := ctx.flushTx(); err != nil {
		t.Fatalf("local txn did not commit: %v", err)
	}

	// Step 5: locate the emitted bind. After commit it is the index tip on
	// the key whose effect carries a TxnBind.
	bind := findBindOnKey(t, e, key)

	// Inspect its ConsumedTips for the key.
	var consumed []Tip
	for _, kb := range bind.Keys {
		if string(kb.Key) == key {
			consumed = fromPbRefs(kb.ConsumedTips)
			break
		}
	}

	// The data effect's Deps for the same key are the resolved truth: a
	// correct ConsumedTips must equal them (base, not the foreign in-flight
	// tip).
	for _, c := range consumed {
		if c == foreignInflight {
			t.Fatalf("bind.ConsumedTips[%s] = %v; contains foreign in-flight tip %v "+
				"(expected [%v] — the pre-tx committed predecessor, as resolveTipDeps "+
				"substitutes for eff.Deps). The committing txn recorded an aborting "+
				"peer's in-progress write as part of its committed causal base (G1a).",
				key, consumed, foreignInflight, base)
		}
	}

	// Positive form of the same assertion: ConsumedTips should be exactly
	// [base].
	if len(consumed) != 1 || consumed[0] != base {
		t.Fatalf("bind.ConsumedTips[%s] = %v; want [%v] (the resolved pre-tx base)",
			key, consumed, base)
	}
}

// findBindOnKey returns the TransactionalBindEffect that is a live index
// tip on key, failing the test if none is present.
func findBindOnKey(t *testing.T, e *Engine, key string) *pb.TransactionalBindEffect {
	t.Helper()
	tips := e.index.Contains(key)
	if tips == nil {
		t.Fatalf("no index entry for key %q after commit", key)
	}
	for _, off := range tips.Tips() {
		eff, ok := e.effectCache.Get(off)
		if !ok {
			continue
		}
		if bind := eff.GetTxnBind(); bind != nil {
			return bind
		}
	}
	t.Fatalf("no TxnBind among index tips for key %q: %v", key, tips.Tips())
	return nil
}
