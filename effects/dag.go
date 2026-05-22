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
	"fmt"
	"log/slog"
	"sort"
	"strings"

	pb "github.com/swytchdb/swytch/cluster/proto"
)

// allDeps copies the effect's Deps and appends any KeyBind.NewTip offsets
// for bind effects. The returned slice is always a fresh allocation when
// bind tips are present to avoid racing with concurrent callers.
func allDeps(eff *pb.Effect) []*pb.EffectRef {
	bind := eff.GetTxnBind()
	if bind == nil || len(bind.Keys) == 0 {
		return eff.Deps
	}
	deps := make([]*pb.EffectRef, len(eff.Deps), len(eff.Deps)+len(bind.Keys))
	copy(deps, eff.Deps)
	for _, kb := range bind.Keys {
		deps = append(deps, kb.NewTip)
	}
	return deps
}

type dag struct {
	engine      *Engine
	key         string
	activeTxnID string
	visited     map[Tip]bool
	nodes       map[Tip]*pb.Effect
	lcaTip      Tip
	// topoOrder is the post-order DFS sequence of every tip reached
	// during phase 2, including tx-data effects that iterate filters
	// out of the processor stream. Callers that need to propagate
	// per-tip state along dependency edges (e.g. HorizonSet
	// invisibility propagation in reconstruct) read this between
	// prepare and iterate.
	topoOrder []Tip
}

func newDag(engine *Engine, key string, activeTxnID string) *dag {
	return &dag{engine: engine, key: key, activeTxnID: activeTxnID}
}

// walk performs a two-phase traversal of the effect graph:
//
//  1. BFS from tips following deps, tracking path convergence. Stops at the
//     LCA snapshot (first snapshot where all paths have converged). Collects
//     all nodes between tips and the LCA.
//  2. Topological replay (DFS post-order) from tips within the collected set,
//     ordered by fork choice hash at each fork. Calls processor in causal
//     order (LCA snapshot → tips).
//
// Transactional effects (non-empty TxnId, not a bind) are skipped from the
// processor output but their deps are still followed. Bind effects appear
// in the sequence as envelopes.
//
// walk is a convenience wrapper over prepare + iterate; callers that need
// to inspect d.topoOrder between BFS and the processor loop should call
// prepare and iterate explicitly.
func (d *dag) walk(tips []Tip, processor func(*pb.Effect) error) error {
	if err := d.prepare(tips); err != nil {
		return err
	}
	return d.iterate(func(_ Tip, eff *pb.Effect) error {
		return processor(eff)
	})
}

// prepare runs BFS + LCA trim and builds d.topoOrder. Safe to read
// d.nodes and d.topoOrder after this returns successfully.
func (d *dag) prepare(tips []Tip) error {
	d.visited = make(map[Tip]bool, len(tips)*8)
	d.nodes = make(map[Tip]*pb.Effect, len(tips)*8)

	if err := d.bfs(tips, &d.lcaTip); err != nil {
		return err
	}

	if len(d.nodes) == 0 {
		return nil
	}

	// Phase 1.5: trim nodes already folded into the LCA snapshot. BFS may
	// have walked into the snapshot's dep chain (because the snapshot was
	// dequeued with a non-empty queue, so it wasn't terminal at that point).
	// Those nodes are already represented in the snapshot's reduced state;
	// leaving them in d.nodes makes ReduceChain process the same data
	// effects twice — duplicates in list reads, double-counts in scalars.
	d.trimAncestorsOfLCA()

	if slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		slog.Debug("dag.walk", "key", d.key, "nodes", len(d.nodes),
			"lca", d.lcaTip, "encoded", d.encode(tips))
	}

	// Phase 2: topo-order via DFS post-order within collected nodes.
	d.topoOrder = make([]Tip, 0, len(d.nodes))
	topoVisited := make(map[Tip]bool, len(d.nodes))

	type pair struct {
		tip Tip
		eff *pb.Effect
	}
	sortedTips := make([]pair, 0, len(tips))
	for _, t := range tips {
		if eff, ok := d.nodes[t]; ok {
			sortedTips = append(sortedTips, pair{t, eff})
		}
	}
	if len(sortedTips) > 1 {
		sort.Slice(sortedTips, func(i, j int) bool {
			return ForkChoiceLess(sortedTips[i].eff.ForkChoiceHash, sortedTips[j].eff.ForkChoiceHash)
		})
	}

	for _, p := range sortedTips {
		d.topoCollect(p.tip, topoVisited)
	}
	return nil
}

// iterate calls processor for each emitted tip in d.topoOrder, filtering
// out tx-data effects (TxnId != "", not a bind, not the active txn) and
// non-LCA snapshots. Requires prepare to have run.
func (d *dag) iterate(processor func(Tip, *pb.Effect) error) error {
	for _, t := range d.topoOrder {
		eff := d.nodes[t]
		if d.shouldEmit(t, eff) {
			if err := processor(t, eff); err != nil {
				return err
			}
		}
	}
	return nil
}

// shouldEmit returns true iff the effect at tip t should be passed to
// the processor. Non-LCA snapshots and tx-data from non-active txns
// participate in dependency walking but are not emitted.
func (d *dag) shouldEmit(t Tip, eff *pb.Effect) bool {
	if eff.GetSnapshot() != nil {
		return t == d.lcaTip
	}
	if eff.TxnId != "" && eff.GetTxnBind() == nil && eff.TxnId != d.activeTxnID {
		return false
	}
	return true
}

// bfs explores the graph from tips following deps. A snapshot is the LCA
// when either: (1) a dep we follow is already visited and is a snapshot
// (paths converged), or (2) we dequeue a snapshot with an empty queue
// (no concurrent paths). All visited nodes are stored in d.nodes.
func (d *dag) bfs(tips []Tip, lcaTip *Tip) error {
	queue := make([]Tip, 0, len(tips)*2)

	for _, t := range tips {
		eff, err := d.engine.getEffect(t)
		if err != nil {
			return err
		}
		d.visited[t] = true
		d.nodes[t] = eff
		queue = append(queue, t)
	}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		eff := d.nodes[cur]

		if eff.GetSnapshot() != nil && len(queue) == 0 {
			*lcaTip = cur
			return nil
		}

		var refs []*pb.EffectRef
		if bind := eff.GetTxnBind(); bind != nil {
			for _, kb := range bind.Keys {
				if string(kb.Key) == d.key {
					refs = []*pb.EffectRef{kb.NewTip}
					break
				}
			}
		} else {
			refs = eff.GetDeps()
		}

		for _, ref := range refs {
			dt := r(ref)
			if d.visited[dt] {
				continue
			}
			depEff, err := d.engine.getEffect(dt)
			if err != nil {
				return err
			}
			d.visited[dt] = true
			d.nodes[dt] = depEff
			queue = append(queue, dt)
		}
	}
	return nil
}

// trimAncestorsOfLCA removes from d.nodes every node in the transitive
// dep-ancestry of the LCA snapshot. Those nodes' state is already folded
// into the snapshot's reduced effect; including them in ReduceChain would
// double-count their data effects (the symptom is duplicate elements in
// a list-append read).
//
// Only nodes already in d.nodes are removed — we don't fetch additional
// effects from the engine. The bfs that populated d.nodes is the only
// source of folded-ancestor entries we need to clear.
func (d *dag) trimAncestorsOfLCA() {
	var zero Tip
	if d.lcaTip == zero {
		return
	}
	lcaEff, ok := d.nodes[d.lcaTip]
	if !ok || lcaEff.GetSnapshot() == nil {
		return
	}
	folded := make(map[Tip]struct{})
	stack := make([]Tip, 0, len(lcaEff.Deps))
	for _, dep := range lcaEff.Deps {
		stack = append(stack, r(dep))
	}
	for len(stack) > 0 {
		cur := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, seen := folded[cur]; seen {
			continue
		}
		folded[cur] = struct{}{}
		eff, ok := d.nodes[cur]
		if !ok {
			continue
		}
		var refs []*pb.EffectRef
		if bind := eff.GetTxnBind(); bind != nil {
			for _, kb := range bind.Keys {
				if string(kb.Key) == d.key {
					refs = []*pb.EffectRef{kb.NewTip}
					break
				}
			}
		} else {
			refs = eff.Deps
		}
		for _, dep := range refs {
			stack = append(stack, r(dep))
		}
	}
	for t := range folded {
		delete(d.nodes, t)
		delete(d.visited, t)
	}
}

// topoCollect does a post-order DFS within the collected nodes, recording
// every visited tip into d.topoOrder. At forks, deps are visited in fork
// choice hash order. Filtering (tx-data effects, non-LCA snapshots) is
// applied by iterate, not here — d.topoOrder is the complete causal order
// of visited tips so callers can propagate per-tip state along dep edges.
func (d *dag) topoCollect(t Tip, visited map[Tip]bool) {
	if visited[t] {
		return
	}
	visited[t] = true

	eff, ok := d.nodes[t]
	if !ok {
		return
	}

	// LCA snapshot: record and don't descend past it
	if eff.GetSnapshot() != nil && t == d.lcaTip {
		d.topoOrder = append(d.topoOrder, t)
		return
	}

	// Follow deps within collected set
	var refs []*pb.EffectRef
	if bind := eff.GetTxnBind(); bind != nil {
		for _, kb := range bind.Keys {
			if string(kb.Key) == d.key {
				refs = []*pb.EffectRef{kb.NewTip}
				break
			}
		}
	} else {
		refs = eff.GetDeps()
	}

	if len(refs) > 0 {
		type pair struct {
			tip Tip
			eff *pb.Effect
		}
		deps := make([]pair, 0, len(refs))
		for _, ref := range refs {
			dt := r(ref)
			if de, ok := d.nodes[dt]; ok {
				deps = append(deps, pair{dt, de})
			}
		}
		if len(deps) > 1 {
			sort.Slice(deps, func(i, j int) bool {
				return ForkChoiceLess(deps[i].eff.ForkChoiceHash, deps[j].eff.ForkChoiceHash)
			})
		}
		for _, dep := range deps {
			d.topoCollect(dep.tip, visited)
		}
	}

	d.topoOrder = append(d.topoOrder, t)
}

// encode produces a compact string encoding of the collected dag nodes.
// Format: "N<count>;L<lca_node>:<lca_off>;T<tip_node>:<tip_off>,...;[<node>:<off>:kind:val:dep_node:dep_off,...;...]"
// Effects are identified by their full (nodeID, offset) so log readers can
// directly check whether a specific emitted offset shows up — and with what
// kind — without reverse-mapping position indexes.
func (d *dag) encode(tips []Tip) string {
	offsets := make([]Tip, 0, len(d.nodes))
	for off := range d.nodes {
		offsets = append(offsets, off)
	}
	sort.Slice(offsets, func(i, j int) bool {
		if offsets[i][0] != offsets[j][0] {
			return offsets[i][0] < offsets[j][0]
		}
		return offsets[i][1] < offsets[j][1]
	})

	var b strings.Builder
	fmt.Fprintf(&b, "N%d;", len(d.nodes))

	if d.lcaTip != (Tip{}) {
		fmt.Fprintf(&b, "L%d:%d;", d.lcaTip[0], d.lcaTip[1])
	}

	b.WriteString("T")
	first := true
	for _, t := range tips {
		if _, ok := d.nodes[t]; ok {
			if !first {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "%d:%d", t[0], t[1])
			first = false
		}
	}
	b.WriteByte(';')

	for _, off := range offsets {
		eff := d.nodes[off]

		kind := byte('?')
		val := int64(0)
		switch {
		case eff.GetData() != nil:
			kind = 'D'
			val = eff.GetData().GetIntVal()
		case eff.GetSnapshot() != nil:
			kind = 'S'
			if s := eff.GetSnapshot().State; s != nil && s.Scalar != nil {
				val = s.Scalar.GetIntVal()
			}
		case eff.GetNoop() != nil:
			kind = 'N'
		case eff.GetMeta() != nil:
			kind = 'M'
		case eff.GetSubscription() != nil:
			kind = 'U'
		case eff.GetTxnBind() != nil:
			kind = 'B'
		case eff.GetSerialization() != nil:
			kind = 'X'
		}

		fmt.Fprintf(&b, "%d:%d:%c:%d:", off[0], off[1], kind, val)
		depFirst := true
		for _, dep := range eff.Deps {
			dt := r(dep)
			if _, ok := d.nodes[dt]; ok {
				if !depFirst {
					b.WriteByte(',')
				}
				fmt.Fprintf(&b, "%d:%d", dt[0], dt[1])
				depFirst = false
			}
		}
		b.WriteByte(';')
	}

	return b.String()
}
