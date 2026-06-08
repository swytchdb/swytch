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
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	pb "github.com/swytchdb/swytch/cluster/proto"
)

// horizonEntry tracks a single Bind that is in its horizon wait period.
type horizonEntry struct {
	group      *horizonGroup
	bindOffset Tip
	txnID      string
	keyNewTips map[string]Tip // key → data effect offset (for pendingTxTips cleanup)
	waiters    xsync.RBMutex
}

// horizonGroup tracks a group of competing Binds that share at least one
// consumed Tip on an overlapping key. Visibility is driven externally:
// flushTx calls MakeVisible / Abort on the originator side; handleRemoteBind
// schedules MakeVisible via ScheduleMakeVisible on the remote-arrival side
// (1×RTT timer).
type horizonGroup struct {
	mu       sync.Mutex
	entries  map[string]*horizonEntry // txnID → entry
	bindKeys map[string][]Tip         // key → consumed tips (for overlap detection)
	allKeys  map[string]struct{}      // union of all keys in group
	timer    *time.Timer              // scheduled MakeVisible (remote-arrival path)
	visible  bool
}

// HorizonSet tracks Binds in their horizon wait period. Effects from
// invisible Binds are excluded from reconstruction until the horizon
// completes.
type HorizonSet struct {
	entries *xsync.Map[string, *horizonEntry] // txnID → entry (fast lookup)
	mu      sync.Mutex                        // protects group creation/merge
	groups  []*horizonGroup                   // all active groups
	engine  *Engine

	// Test hook: if non-nil, called instead of time.AfterFunc.
	afterFunc func(d time.Duration, f func()) *time.Timer
}

func newHorizonSet(engine *Engine) *HorizonSet {
	return &HorizonSet{
		entries: xsync.NewMap[string, *horizonEntry](),
		engine:  engine,
	}
}

// horizonFallbackWait is the crash-fallback duration used when the
// originator's verdict-snapshot never arrives (e.g., originator died
// between bind broadcast and snapshot emit). The primary signal is the
// snapshot itself via applySnapshotVerdicts; this timer only fires when
// that signal is lost. Generous on purpose — racing the snapshot risks
// premature visibility, holding longer just delays the rare crash case.
const horizonFallbackWait = 5 * time.Second

// computeHorizonWait returns the duration the remote-arrival path should
// hold a bind invisible before making it visible, when no verdict snapshot
// arrives. Returns horizonFallbackWait — long enough that the originator's
// snapshot almost always wins the race and shortcuts the wait, but short
// enough that a crashed originator doesn't hang reads indefinitely.
func (h *HorizonSet) computeHorizonWait() time.Duration {
	return horizonFallbackWait
}

// Add registers a Bind in the invisible set. The bind stays invisible
// until MakeVisible, Abort, or a timer scheduled via ScheduleMakeVisible
// fires. If the bind's keys overlap with an existing group's consumed tips,
// it joins that group; otherwise a new group is created.
func (h *HorizonSet) Add(txnID string, bindOffset Tip, bind *pb.TransactionalBindEffect) {
	consumedByKey := make(map[string][]Tip, len(bind.Keys))
	newTips := make(map[string]Tip, len(bind.Keys))
	allKeys := make(map[string]struct{}, len(bind.Keys))
	for _, kb := range bind.Keys {
		k := string(kb.Key)
		consumedByKey[k] = fromPbRefs(kb.ConsumedTips)
		newTips[k] = r(kb.NewTip)
		allKeys[k] = struct{}{}
	}

	entry := &horizonEntry{
		bindOffset: bindOffset,
		txnID:      txnID,
		keyNewTips: newTips,
		waiters:    xsync.RBMutex{},
	}
	// immediately prevent concurrent access to this entry
	entry.waiters.Lock()

	h.mu.Lock()
	newBind := &forkChoiceBindEntry{keys: consumedByKey}
	var overlapping []*horizonGroup
	for _, g := range h.groups {
		if g.visible {
			continue
		}
		g.mu.Lock()
		if groupOverlaps(g, newBind) {
			overlapping = append(overlapping, g)
		}
		g.mu.Unlock()
	}

	var targetGroup *horizonGroup
	if len(overlapping) == 0 {
		targetGroup = &horizonGroup{
			entries:  make(map[string]*horizonEntry),
			bindKeys: make(map[string][]Tip),
			allKeys:  make(map[string]struct{}),
		}
		h.groups = append(h.groups, targetGroup)
	} else if len(overlapping) == 1 {
		targetGroup = overlapping[0]
	} else {
		targetGroup = overlapping[0]
		targetGroup.mu.Lock()
		for _, other := range overlapping[1:] {
			other.mu.Lock()
			for tid, e := range other.entries {
				e.group = targetGroup
				targetGroup.entries[tid] = e
			}
			for k, tips := range other.bindKeys {
				targetGroup.bindKeys[k] = append(targetGroup.bindKeys[k], tips...)
			}
			for k := range other.allKeys {
				targetGroup.allKeys[k] = struct{}{}
			}
			if other.timer != nil {
				other.timer.Stop()
				other.timer = nil
			}
			other.visible = true // consumed
			other.mu.Unlock()
		}
		targetGroup.mu.Unlock()
		h.groups = filterGroups(h.groups)
	}

	targetGroup.mu.Lock()
	entry.group = targetGroup
	targetGroup.entries[txnID] = entry
	for k, tips := range consumedByKey {
		targetGroup.bindKeys[k] = append(targetGroup.bindKeys[k], tips...)
	}
	for k := range allKeys {
		targetGroup.allKeys[k] = struct{}{}
	}
	targetGroup.mu.Unlock()
	h.mu.Unlock()

	h.entries.Store(txnID, entry)

	slog.Debug("HorizonSet.Add", "txnID", txnID, "bindOffset", bindOffset,
		"group_size", len(targetGroup.entries))
}

// HorizonWait is a handle returned by WaitForClear. It carries the
// horizonEntry pointer and the RToken so Release can RUnlock the same
// mutex even after the entry has been removed from h.entries by
// MakeVisible or Abort.
type HorizonWait struct {
	entry *horizonEntry
	tok   *xsync.RToken
}

// WaitForClear blocks until the bind for txnID has been resolved
// (MakeVisible or Abort releases the entry's waiter lock). Returns nil if
// the bind is not in the invisible set — the caller may proceed without
// waiting. Returns a *HorizonWait the caller must Release when done.
func (h *HorizonSet) WaitForClear(txnID string) *HorizonWait {
	if entry, ok := h.entries.Load(txnID); ok {
		return &HorizonWait{entry: entry, tok: entry.waiters.RLock()}
	}
	return nil
}

// Release returns the read-lock token to the horizonEntry. Safe on nil.
func (w *HorizonWait) Release() {
	if w == nil {
		return
	}
	w.entry.waiters.RUnlock(w.tok)
}

// ScheduleMakeVisible schedules MakeVisible(txnID) to fire after `wait`.
// Used by handleRemoteBind as a crash-fallback for the remote-arrival
// wait — the primary signal is the originator's verdict-snapshot
// arriving via applySnapshotVerdicts, which short-circuits this timer.
// The timer only fires when the originator died between broadcasting
// the bind and broadcasting the verdict; we keep it generous (~5s) to
// avoid racing the snapshot under normal conditions. Resets any
// existing timer on the group; a later-joining bind extends the wait.
func (h *HorizonSet) ScheduleMakeVisible(txnID string, wait time.Duration) {
	entry, ok := h.entries.Load(txnID)
	if !ok {
		return
	}
	group := entry.group
	group.mu.Lock()
	if group.visible {
		group.mu.Unlock()
		return
	}
	if group.timer != nil {
		group.timer.Stop()
	}
	fn := func() { h.MakeVisible(txnID) }
	if h.afterFunc != nil {
		group.timer = h.afterFunc(wait, fn)
	} else {
		group.timer = time.AfterFunc(wait, fn)
	}
	group.mu.Unlock()
}

// MakeVisible promotes an entire group: removes all txnIDs from the invisible
// set, cleans up pendingTxTips, evicts cache, and fires OnKeyDataAdded callbacks.
func (h *HorizonSet) MakeVisible(txnID string) {
	entry, ok := h.entries.Load(txnID)
	if !ok {
		return
	}

	group := entry.group
	group.mu.Lock()
	if group.visible {
		group.mu.Unlock()
		return
	}
	group.visible = true
	if group.timer != nil {
		group.timer.Stop()
		group.timer = nil
	}
	entries := make([]*horizonEntry, 0, len(group.entries))
	for _, e := range group.entries {
		entries = append(entries, e)
	}
	group.mu.Unlock()

	allKeys := make(map[string]struct{})
	for _, e := range entries {
		if et, ok := h.entries.Load(e.txnID); ok {
			et.waiters.Unlock()
		}

		h.entries.Delete(e.txnID)
		for k, newTip := range e.keyNewTips {
			h.engine.pendingTxTips.Delete(newTip)
			allKeys[k] = struct{}{}
		}
	}

	for k := range allKeys {
		h.engine.ownFilterAdd(k)
		if h.engine.OnKeyDataAdded != nil {
			h.engine.OnKeyDataAdded(k)
		}
	}

	h.mu.Lock()
	h.groups = filterGroups(h.groups)
	h.mu.Unlock()

	slog.Debug("HorizonSet.MakeVisible", "txnID", txnID,
		"promoted_count", len(entries), "keys", len(allKeys))
}

// Abort removes a single entry from the horizon set without promoting it.
// Used by flushTx when the originator decides to abort after NACK
// processing. The bind effect is still in the local DAG and at peers;
// reconstruct's cross-key reachability is what skips it on read.
//
// Other entries in the same group are unaffected — they continue waiting
// on their own visibility trigger (timer or explicit MakeVisible).
func (h *HorizonSet) Abort(txnID string) {
	entry, ok := h.entries.Load(txnID)
	if !ok {
		return
	}
	h.entries.Delete(txnID)
	entry.waiters.Unlock()

	group := entry.group
	group.mu.Lock()
	delete(group.entries, txnID)
	for _, newTip := range entry.keyNewTips {
		h.engine.pendingTxTips.Delete(newTip)
	}
	isEmpty := len(group.entries) == 0
	if isEmpty {
		if group.timer != nil {
			group.timer.Stop()
			group.timer = nil
		}
		group.visible = true // marker for filterGroups
	}
	group.mu.Unlock()

	if isEmpty {
		h.mu.Lock()
		h.groups = filterGroups(h.groups)
		h.mu.Unlock()
	}

	slog.Debug("HorizonSet.Abort", "txnID", txnID)
}

// IsInvisible returns true if the given txnID is in the invisible set.
func (h *HorizonSet) IsInvisible(txnID string) bool {
	_, ok := h.entries.Load(txnID)
	return ok
}

// Empty reports whether no txn is currently held invisible. When true,
// reconstruct can skip its per-read invisibility precompute: any bind
// reachable from a read's captured tips had its horizon entry added during
// ingest (before the index update that made it visible to the read), so an
// empty horizon at reconstruct setup proves no walked bind is invisible.
func (h *HorizonSet) Empty() bool {
	return h.entries.Size() == 0
}

// groupOverlaps checks if a horizon group has any overlapping key with shared
// consumed tips against a new bind entry. Caller must hold g.mu.
func groupOverlaps(g *horizonGroup, newBind *forkChoiceBindEntry) bool {
	for key, newBase := range newBind.keys {
		existingBase, ok := g.bindKeys[key]
		if !ok {
			continue
		}
		existingSet := make(map[Tip]bool, len(existingBase))
		for _, off := range existingBase {
			existingSet[off] = true
		}
		for _, off := range newBase {
			if existingSet[off] {
				return true
			}
		}
	}
	return false
}

// filterGroups removes groups that are marked as visible.
func filterGroups(groups []*horizonGroup) []*horizonGroup {
	n := 0
	for _, g := range groups {
		if !g.visible {
			groups[n] = g
			n++
		}
	}
	for i := n; i < len(groups); i++ {
		groups[i] = nil
	}
	return groups[:n]
}
