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
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	pb "github.com/swytchdb/swytch/cluster/proto"
)

// adaptiveSerializationEnabled gates every entry point into the
// adaptive-serialization path: escalation emits, the release loop,
// leader activation from remote effects, and the Forward/ForwardExec
// routing. It is currently disabled because the mechanism breaks SSI
// for the SQL transport — forwarding an emit to a leader evaluates
// deps against the leader's tips, not the origin's snapshot tips,
// so a forwarded write can land on a chain that the origin's
// transaction never observed. Flip back to true once forwarding is
// re-designed to carry the origin's snapshot instead of piggybacking
// on the leader's fresh view.
const adaptiveSerializationEnabled = false

// ForwardingEnabled reports whether adaptive-serialization forwarding is
// compiled in. The redis handler checks it before extracting command keys
// that would only feed Forward/ForwardExec's early return — as a constant,
// the whole forwarding block dead-code-eliminates while disabled.
func ForwardingEnabled() bool { return adaptiveSerializationEnabled }

// tipSerializationThreshold is the number of tips on a key before
// emitting a serialization request. Non-transactional writes (commutative
// ops) don't trigger abort-count escalation, so this provides a second
// trigger path for keys with excessive Tip accumulation.
const tipSerializationThreshold = 50

// keySerializationState tracks the serialization state of a single key.
type keySerializationState struct {
	leader       pb.NodeID
	lastActivity atomic.Int64 // UnixNano of last forwarded or local write
}

// serializationReleaseQuietPeriod is how long a serialized key must be
// quiet before the leader releases serialization.
const serializationReleaseQuietPeriod = 5 * time.Second

// serializationReleaseCheckInterval is how often the leader checks for
// quiet serialized keys.
const serializationReleaseCheckInterval = 2 * time.Second

// initSerializationState creates the serialization state map on the engine.
// Called from NewEngine.
func initSerializationState() *xsync.Map[string, *keySerializationState] {
	return xsync.NewMap[string, *keySerializationState]()
}

// startSerializationRelease launches a background goroutine that
// periodically checks for quiet serialized keys and releases them.
// Only the leader emits release effects. Must be called after
// antiEntropyStop is initialized.
func (e *Engine) startSerializationRelease() {
	if !adaptiveSerializationEnabled {
		return
	}
	if e.serializationState == nil || e.antiEntropyStop == nil {
		return
	}
	e.antiEntropyWg.Add(1)
	go e.serializationReleaseLoop()
}

func (e *Engine) serializationReleaseLoop() {
	defer e.antiEntropyWg.Done()
	ticker := time.NewTicker(serializationReleaseCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.antiEntropyStop:
			return
		case <-ticker.C:
			if e.closed.Load() {
				return
			}
			e.checkSerializationRelease()
		}
	}
}

func (e *Engine) checkSerializationRelease() {
	deadline := time.Now().Add(-serializationReleaseQuietPeriod).UnixNano()
	var toRelease []string

	e.serializationState.Range(func(key string, state *keySerializationState) bool {
		if state.leader != e.nodeID {
			return true // only the leader releases
		}
		if state.lastActivity.Load() < deadline {
			toRelease = append(toRelease, key)
		}
		return true
	})

	for _, key := range toRelease {
		slog.Debug("serialization release: quiet period exceeded", "key", key)
		e.emitSerializationRelease(key)
	}
}

// emitSerializationRelease emits a release serialization effect as a
// transaction. Fork-choice resolves races between concurrent releases.
func (e *Engine) emitSerializationRelease(key string) {
	if !adaptiveSerializationEnabled {
		return
	}
	slog.Info("adaptive serialization: releasing leader",
		"key", key, "leader", e.nodeID)
	ctx := e.NewContext()
	ctx.BeginTx()
	if err := ctx.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Serialization{Serialization: &pb.SerializationEffect{
			LeaderNodeId: uint64(e.nodeID),
			Release:      true,
		}},
	}); err != nil {
		slog.Error("failed to emit serialization release", "key", key, "error", err)
		return
	}
	if err := ctx.Flush(); err != nil {
		slog.Debug("serialization release flush failed (expected under contention)",
			"key", key, "error", err)
	}
	e.serializationState.Delete(key)
}

// CheckSerializationLeader returns the serialization leader for a key,
// or nil if no serialization is active.
func (e *Engine) CheckSerializationLeader(key string) *pb.NodeID {
	if e.serializationState == nil {
		return nil
	}
	state, ok := e.serializationState.Load(key)
	if !ok {
		return nil
	}
	leader := state.leader
	return &leader
}

// RecordSerializationActivity updates the last activity timestamp for a
// serialized key. Called when the leader processes a forwarded command
// or performs a local write on a serialized key.
func (e *Engine) RecordSerializationActivity(key string) {
	if e.serializationState == nil {
		return
	}
	if state, ok := e.serializationState.Load(key); ok {
		state.lastActivity.Store(time.Now().UnixNano())
	}
}

// updateSerializationState syncs the in-memory serialization state from
// a reconstructed ReducedEffect. Called after reconstruction completes
// so the handler can do fast lookups without full snapshots.
//
// Only clears the leader when the result contains an explicit release
// (SerializationReleased flag). A reconstruct that simply doesn't see
// a serialization effect (e.g. stale tips) must not clear a leader
// that was just established.
func (e *Engine) updateSerializationState(key string, result *pb.ReducedEffect) {
	if !adaptiveSerializationEnabled {
		return
	}
	if e.serializationState == nil {
		return
	}
	if result != nil && result.SerializationLeader != nil {
		newLeader := *result.SerializationLeader
		_, existed := e.serializationState.Load(key)
		e.serializationState.Store(key, &keySerializationState{
			leader: pb.NodeID(newLeader),
		})
		if !existed {
			slog.Info("adaptive serialization: leader active",
				"key", key, "leader", newLeader, "self", e.nodeID)
		}
	}
	// Leader is only cleared via emitSerializationRelease, not here.
	// A reconstruct that doesn't see a serialization effect (stale tips)
	// must not clear a leader that was just established.
}

// selectSerializationLeader picks the optimal leader for a key based on
// RTT measurements. Collects contender node IDs from the key's DAG tips,
// then selects the contender with the lowest max RTT to all others.
func (e *Engine) selectSerializationLeader(key string) pb.NodeID {
	if e.rttProvider == nil {
		return e.nodeID // no RTT data, nominate self
	}

	// Collect contender node IDs from the key's tips
	tips := e.index.Contains(key)
	if tips == nil {
		return e.nodeID
	}

	contenders := make(map[pb.NodeID]struct{})
	contenders[e.nodeID] = struct{}{} // self is always a contender
	for _, tipOff := range tips.Tips() {
		eff, err := e.getEffect(tipOff)
		if err != nil {
			continue
		}
		contenders[pb.NodeID(eff.NodeId)] = struct{}{}
	}

	if len(contenders) <= 1 {
		return e.nodeID
	}

	// For each contender, compute the max RTT to all other contenders.
	// We only have RTT from self to peers, so for pairs not involving
	// self we approximate as (RTT_self_to_A + RTT_self_to_B) / 2.
	bestLeader := e.nodeID
	var bestMaxRTT time.Duration
	first := true

	for candidate := range contenders {
		var maxRTT time.Duration
		for other := range contenders {
			if other == candidate {
				continue
			}
			var rtt time.Duration
			if candidate == e.nodeID {
				rtt = e.rttProvider.GetRTT(pb.NodeID(other))
			} else if other == e.nodeID {
				rtt = e.rttProvider.GetRTT(pb.NodeID(candidate))
			} else {
				rttA := e.rttProvider.GetRTT(pb.NodeID(candidate))
				rttB := e.rttProvider.GetRTT(pb.NodeID(other))
				rtt = (rttA + rttB) / 2
			}
			if rtt == 0 {
				rtt = time.Hour // unknown RTT = worst case
			}
			if rtt > maxRTT {
				maxRTT = rtt
			}
		}
		if first || maxRTT < bestMaxRTT {
			bestMaxRTT = maxRTT
			bestLeader = candidate
			first = false
		}
	}

	return bestLeader
}

// ForwardCommand describes a single command to be forwarded.
type ForwardCommand struct {
	Name string
	Args [][]byte
	Keys []string
}

// Forward checks if any key has a serialization leader on another node.
// If so, forwards the command and returns raw RESP bytes.
// Returns nil if the command should execute locally.
func (e *Engine) Forward(commandName string, args [][]byte, keys []string, username string) []byte {
	if !adaptiveSerializationEnabled {
		return nil
	}
	if e.broadcaster == nil || len(keys) == 0 {
		return nil
	}

	// Collect non-self leaders
	var leaderID pb.NodeID
	found := false
	for _, key := range keys {
		leader := e.CheckSerializationLeader(key)
		if leader == nil || *leader == e.nodeID {
			continue
		}
		if found && *leader != leaderID {
			// Multiple different leaders
			return []byte("-TRYAGAIN multiple serialization leaders\r\n")
		}
		leaderID = *leader
		found = true
	}
	if !found {
		return nil
	}

	slog.Debug("adaptive serialization: forwarding command",
		"command", commandName, "leader", leaderID, "keys", keys)

	tx := &pb.ForwardedTransaction{
		OriginNode:     uint64(e.nodeID),
		AuthorizedUser: username,
		Commands: []*pb.ForwardedCommand{{
			CommandName: []byte(commandName),
			Args:        args,
		}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := e.broadcaster.ForwardTransaction(ctx, leaderID, tx)
	if err != nil {
		slog.Debug("adaptive serialization: forward failed", "error", err, "leader", leaderID)
		if e.broadcaster.InMajorityPartition() {
			return []byte("-TRYAGAIN forwarding failed, retry\r\n")
		}
		return []byte("-READONLY forwarding failed and not in majority partition\r\n")
	}

	if resp.Error {
		return []byte("-ERR " + resp.ErrorMessage + "\r\n")
	}

	// Record activity so the leader doesn't release serialization prematurely
	for _, key := range keys {
		e.RecordSerializationActivity(key)
	}

	return resp.RespData
}

// ForwardExec checks if any queued command touches a serialized key.
// If so, forwards the entire transaction and returns raw RESP bytes.
// Returns nil for local execution.
func (e *Engine) ForwardExec(commands []ForwardCommand, watchedKeys []string, username string) []byte {
	if !adaptiveSerializationEnabled {
		return nil
	}
	if e.broadcaster == nil {
		return nil
	}

	// Collect all keys from all commands plus watched keys
	allKeys := make(map[string]struct{})
	for _, cmd := range commands {
		for _, k := range cmd.Keys {
			allKeys[k] = struct{}{}
		}
	}
	for _, k := range watchedKeys {
		allKeys[k] = struct{}{}
	}

	// Find non-self leaders
	var leaderID pb.NodeID
	found := false
	for key := range allKeys {
		leader := e.CheckSerializationLeader(key)
		if leader == nil || *leader == e.nodeID {
			continue
		}
		if found && *leader != leaderID {
			return []byte("-TRYAGAIN multiple serialization leaders\r\n")
		}
		leaderID = *leader
		found = true
	}
	if !found {
		return nil
	}

	slog.Debug("adaptive serialization: forwarding EXEC",
		"leader", leaderID, "commands", len(commands))

	tx := &pb.ForwardedTransaction{
		OriginNode:     uint64(e.nodeID),
		AuthorizedUser: username,
	}
	for _, cmd := range commands {
		tx.Commands = append(tx.Commands, &pb.ForwardedCommand{
			CommandName: []byte(cmd.Name),
			Args:        cmd.Args,
		})
	}
	for _, wk := range watchedKeys {
		tx.WatchedKeys = append(tx.WatchedKeys, []byte(wk))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := e.broadcaster.ForwardTransaction(ctx, leaderID, tx)
	if err != nil {
		slog.Debug("adaptive serialization: forward EXEC failed", "error", err, "leader", leaderID)
		if e.broadcaster.InMajorityPartition() {
			return []byte("-TRYAGAIN forwarding failed, retry\r\n")
		}
		return []byte("-READONLY forwarding failed and not in majority partition\r\n")
	}

	if resp.Error {
		return []byte("-ERR " + resp.ErrorMessage + "\r\n")
	}

	// Record activity for all keys
	for key := range allKeys {
		e.RecordSerializationActivity(key)
	}

	return resp.RespData
}
