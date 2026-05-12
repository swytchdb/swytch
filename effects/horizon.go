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
}

// horizonGroup tracks a group of competing Binds that share at least one
// consumed Tip on an overlapping key. All members become visible together
// when the timer fires or when MakeVisible is called.
type horizonGroup struct {
	mu       sync.Mutex
	entries  map[string]*horizonEntry // txnID → entry
	bindKeys map[string][]Tip         // key → consumed tips (for overlap detection)
	allKeys  map[string]struct{}      // union of all keys in group
	timer    *time.Timer
	visible  bool // true once promoted
}

// HorizonSet tracks Binds in their horizon wait period. Effects from
// invisible Binds are excluded from reconstruction until the horizon
// completes (timer fires or fast-path all-ACK).
type HorizonSet struct {
	entries *xsync.Map[string, *horizonEntry] // txnID → entry (fast lookup)
	mu      sync.Mutex                        // protects group creation/merge
	groups  []*horizonGroup                   // all active groups
	engine  *Engine
	timeout time.Duration

	// Test hook: if non-nil, called instead of time.AfterFunc.
	// Returns a timer whose Stop method can be called.
	afterFunc func(d time.Duration, f func()) *time.Timer
}

// newHorizonSet creates a new HorizonSet bound to the given engine.
func newHorizonSet(engine *Engine, timeout time.Duration) *HorizonSet {
	return &HorizonSet{
		entries: xsync.NewMap[string, *horizonEntry](),
		engine:  engine,
		timeout: timeout,
	}
}

// computeHorizonWait returns the wait duration for a bind based on
// peer RTTs. Semantic: wait long enough that if a competing bind was
// emitted on any of these peers concurrently with ours, it's had time
// to reach us. That's bounded by round-trip to the slowest peer in
// the set — we measured those via heartbeats.
//
// Returns the fallback timeout when RTT data is unavailable (no
// RTT provider, or every peer returns 0).
func (h *HorizonSet) computeHorizonWait(peers []pb.NodeID) time.Duration {
	if h.engine == nil || h.engine.rttProvider == nil || len(peers) == 0 {
		return h.timeout
	}
	var maxRTT time.Duration
	haveData := false
	for _, p := range peers {
		r := h.engine.rttProvider.GetRTT(p)
		if r <= 0 {
			continue // no measurement for this peer
		}
		haveData = true
		if r > maxRTT {
			maxRTT = r
		}
	}
	if !haveData {
		return h.timeout
	}
	// 2x the measured RTT — one round-trip plus a round-trip of
	// safety for jitter and clock skew. Measured RTT already
	// captures the real network floor; we don't invent latency on
	// top. Capped at the configured ceiling for unreasonable
	// measurements.
	wait := min(maxRTT*2, h.timeout)
	return wait
}

// Add registers a Bind in the invisible set. If the Bind's keys overlap
// with an existing group (shared consumed tips), the Bind joins that group
// and the timer is reset. Otherwise a new group is created.
//
// relevantPeers is the set of peers whose concurrent binds we need to
// wait for before making this bind visible. On the origin that's the
// subscribers we just replicated to. On a remote-arrival it's all
// currently-alive peers (any of them may hold a competing bind we
// haven't seen yet). Timeout is computed from their RTTs.
func (h *HorizonSet) Add(txnID string, bindOffset Tip, bind *pb.TransactionalBindEffect, relevantPeers []pb.NodeID) {
	// Build per-key consumed tips and new tips
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
	defer h.mu.Unlock()

	// Find all existing groups that overlap with this bind
	newBind := &forkChoiceBindEntry{
		keys: consumedByKey,
	}
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
		// No overlap — create new group
		targetGroup = &horizonGroup{
			entries:  make(map[string]*horizonEntry),
			bindKeys: make(map[string][]Tip),
			allKeys:  make(map[string]struct{}),
		}
		h.groups = append(h.groups, targetGroup)
	} else if len(overlapping) == 1 {
		targetGroup = overlapping[0]
	} else {
		// Multiple overlapping groups — merge into the first
		targetGroup = overlapping[0]
		targetGroup.mu.Lock()
		for _, other := range overlapping[1:] {
			other.mu.Lock()
			// Move entries from other to target
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
			}
			other.visible = true // mark as consumed
			other.mu.Unlock()
		}
		targetGroup.mu.Unlock()
		// Remove consumed groups
		h.groups = filterGroups(h.groups)
	}

	// Add entry to target group
	targetGroup.mu.Lock()
	entry.group = targetGroup
	targetGroup.entries[txnID] = entry
	for k, tips := range consumedByKey {
		targetGroup.bindKeys[k] = append(targetGroup.bindKeys[k], tips...)
	}
	for k := range allKeys {
		targetGroup.allKeys[k] = struct{}{}
	}
	// Reset timer with an RTT-scaled wait.
	if targetGroup.timer != nil {
		targetGroup.timer.Stop()
	}
	wait := h.computeHorizonWait(relevantPeers)
	g := targetGroup // capture for closure
	if h.afterFunc != nil {
		targetGroup.timer = h.afterFunc(wait, func() {
			h.timerFired(g)
		})
	} else {
		targetGroup.timer = time.AfterFunc(wait, func() {
			h.timerFired(g)
		})
	}
	targetGroup.mu.Unlock()

	// Register in fast-lookup map
	h.entries.Store(txnID, entry)

	slog.Debug("HorizonSet.Add", "txnID", txnID, "bindOffset", bindOffset,
		"group_size", len(targetGroup.entries), "wait_ms", wait.Milliseconds())
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
	}
	// Collect all entries to promote
	entries := make([]*horizonEntry, 0, len(group.entries))
	for _, e := range group.entries {
		entries = append(entries, e)
	}
	group.mu.Unlock()

	// Promote all entries
	allKeys := make(map[string]struct{})
	for _, e := range entries {
		// Remove from fast-lookup
		h.entries.Delete(e.txnID)
		// Clean up pendingTxTips for each key's data offset
		for k, newTip := range e.keyNewTips {
			h.engine.pendingTxTips.Delete(newTip)
			allKeys[k] = struct{}{}
		}
	}

	// Evict cache and fire callbacks for all affected keys
	for k := range allKeys {
		if h.engine.cache != nil {
			h.engine.cache.Evict(k)
		}
		if h.engine.OnKeyDataAdded != nil {
			h.engine.OnKeyDataAdded(k)
		}
	}

	// Remove group from active list
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

// StopAll stops all active timers. Called during Engine.Close.
func (h *HorizonSet) StopAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, g := range h.groups {
		g.mu.Lock()
		if g.timer != nil {
			g.timer.Stop()
		}
		g.mu.Unlock()
	}
}

// timerFired is the callback when a horizon group's timer expires.
// All Binds in the group become visible simultaneously.
func (h *HorizonSet) timerFired(g *horizonGroup) {
	g.mu.Lock()
	if g.visible {
		g.mu.Unlock()
		return
	}
	// Pick any txnID to call MakeVisible (which promotes the whole group)
	var anyTxnID string
	for tid := range g.entries {
		anyTxnID = tid
		break
	}
	g.mu.Unlock()

	if anyTxnID != "" {
		slog.Debug("HorizonSet: timer fired", "txnID", anyTxnID)
		h.MakeVisible(anyTxnID)
	}
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
	// Clear trailing slots to avoid leaking pointers
	for i := n; i < len(groups); i++ {
		groups[i] = nil
	}
	return groups[:n]
}
