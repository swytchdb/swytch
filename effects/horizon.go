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

	"github.com/puzpuzpuz/xsync/v4"
	pb "github.com/swytchdb/swytch/cluster/proto"
)

// horizonEntry tracks a single Bind that is in its horizon wait period.
type horizonEntry struct {
	group      *horizonGroup
	bindOffset Tip
	txnID      string
	keyNewTips map[string]Tip // key → data effect offset (for pendingTxTips cleanup)
}

// horizonGroup tracks a group of competing Binds that share at least one
// consumed Tip on an overlapping key. The group is promoted when waitCount
// reaches zero — i.e. every peer the originator was waiting on has replied
// (ACK or NACK). A remote-arrival bind contributes 0 to the count.
type horizonGroup struct {
	mu        sync.Mutex
	entries   map[string]*horizonEntry // txnID → entry
	bindKeys  map[string][]Tip         // key → consumed tips (for overlap detection)
	allKeys   map[string]struct{}      // union of all keys in group
	waitCount int                      // peer responses still pending
	visible   bool
}

// HorizonSet tracks Binds in their horizon wait period. Effects from
// invisible Binds are excluded from reconstruction until the horizon
// completes (all expected peer responses received via Response).
type HorizonSet struct {
	entries *xsync.Map[string, *horizonEntry] // txnID → entry (fast lookup)
	mu      sync.Mutex                        // protects group creation/merge
	groups  []*horizonGroup                   // all active groups
	engine  *Engine
}

func newHorizonSet(engine *Engine) *HorizonSet {
	return &HorizonSet{
		entries: xsync.NewMap[string, *horizonEntry](),
		engine:  engine,
	}
}

// Add registers a Bind in the invisible set. If the Bind's keys overlap
// with an existing group (shared consumed tips), the Bind joins that group.
//
// peerCount is the number of ACK/NACK responses we will wait for before
// this bind becomes visible. The originator passes the size of its replicate
// fan-out; a remote-arrival bind passes 0 (it becomes visible the moment
// it's added, unless it merges into a group that's still waiting for its
// originator's responses).
func (h *HorizonSet) Add(txnID string, bindOffset Tip, bind *pb.TransactionalBindEffect, peerCount int) {
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
	}

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
			targetGroup.waitCount += other.waitCount
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
	targetGroup.waitCount += peerCount
	shouldFire := targetGroup.waitCount <= 0
	targetGroup.mu.Unlock()
	h.mu.Unlock()

	h.entries.Store(txnID, entry)

	slog.Debug("HorizonSet.Add", "txnID", txnID, "bindOffset", bindOffset,
		"group_size", len(targetGroup.entries), "wait_count", targetGroup.waitCount)

	if shouldFire {
		h.MakeVisible(txnID)
	}
}

// Response records that one peer the originator was waiting on has replied
// (ACK or NACK). When the group's waitCount reaches zero, MakeVisible fires.
func (h *HorizonSet) Response(txnID string) {
	entry, ok := h.entries.Load(txnID)
	if !ok {
		return
	}
	group := entry.group
	group.mu.Lock()
	group.waitCount--
	shouldFire := group.waitCount <= 0 && !group.visible
	group.mu.Unlock()
	if shouldFire {
		h.MakeVisible(txnID)
	}
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
	entries := make([]*horizonEntry, 0, len(group.entries))
	for _, e := range group.entries {
		entries = append(entries, e)
	}
	group.mu.Unlock()

	allKeys := make(map[string]struct{})
	for _, e := range entries {
		h.entries.Delete(e.txnID)
		for k, newTip := range e.keyNewTips {
			h.engine.pendingTxTips.Delete(newTip)
			allKeys[k] = struct{}{}
		}
	}

	for k := range allKeys {
		if h.engine.cache != nil {
			h.engine.cache.Evict(k)
		}
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

// IsInvisible returns true if the given txnID is in the invisible set.
func (h *HorizonSet) IsInvisible(txnID string) bool {
	_, ok := h.entries.Load(txnID)
	return ok
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
