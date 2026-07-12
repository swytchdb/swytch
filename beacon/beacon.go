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

package beacon

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/swytchdb/swytch/cluster"
	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
)

// Config holds beacon configuration parsed from CLI flags.
type Config struct {
	JoinAddr      string    // DNS name to resolve for peer discovery (empty = solo)
	ClusterPort   int       // QUIC listen port for cluster traffic
	AdvertiseAddr string    // host:port this node advertises (empty = auto-detect)
	NodeID        pb.NodeID // ephemeral node ID
	Passphrase    string    // shared mTLS passphrase

	// Cloud, when set, replaces DNS peer discovery: candidate addresses come
	// from the cloud's membership roster instead of resolving JoinAddr.
	Cloud *cluster.CloudSync
}

// Beacon manages cluster discovery and dynamic membership via effects.
type Beacon struct {
	cfg    Config
	engine *effects.Engine
	pm     *cluster.PeerManager

	members       []Member
	expectedPeers int // DNS-discovered non-self candidate count, set during bootstrap
	mu            sync.RWMutex

	// topoRefresh coalesces "membership key changed" signals. Capacity 1: a
	// pending signal already means "re-read on the next loop iteration", so
	// additional signals during that window collapse into it.
	topoRefresh chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates a beacon. Call Start to begin discovery and membership.
func New(cfg Config, engine *effects.Engine, pm *cluster.PeerManager) *Beacon {
	return &Beacon{
		cfg:         cfg,
		engine:      engine,
		pm:          pm,
		topoRefresh: make(chan struct{}, 1),
	}
}

// Start discovers peers, subscribes to membership, registers this
// node, and begins the background heartbeat + membership watch. Blocks
// until initial join completes or ctx is cancelled. The caller is
// expected to gate transport listeners (SQL/redis) on Start returning
// nil — a node that starts serving client traffic before bootstrap
// completes will route writes against a synthetic topology and miss
// remote effects that arrive before its first subscription.
func (b *Beacon) Start(ctx context.Context) error {
	b.ctx, b.cancel = context.WithCancel(ctx)

	// Phase 1: peer discovery (DNS or cloud) + temporary topology + peer
	// reachability. Does not read authoritative membership — that would
	// deadlock when every node waits for the others before registering itself.
	if b.cfg.JoinAddr != "" || b.cfg.Cloud != nil {
		if err := b.bootstrap(b.ctx); err != nil {
			return err
		}
	}

	if b.expectedPeers > 0 {
		// Phase 1.5: Wait for at least one peer to become alive+symmetric
		// via heartbeat exchange. With immediate heartbeats sent on
		// connection (PeerManager wiring), this completes in <100ms.
		// Required so registerSelf's SafeMode replication has valid targets.
		symCtx, symCancel := context.WithTimeout(b.ctx, 10*time.Second)
		err := b.waitForSymmetricPeers(symCtx)
		symCancel()
		if err != nil {
			slog.Warn("beacon: no symmetric peers available, skipping prime subscription", "error", err)
		} else {
			// Phase 1.75: Subscribe to the membership key before registering
			// self. NACKs from peers carry their existing membership tips so
			// the local engine has peer data before registerSelf runs.
			b.primeSubscription()
		}
	}

	// Phase 2: Register self in membership. Local emit — fast.
	if err := b.registerSelf(); err != nil {
		return err
	}

	// Phase 3: Wait until the membership key converges on every
	// candidate peer — guaranteed to terminate because all peers are
	// now registering themselves concurrently in their own Phase 2.
	if err := b.waitForMembershipConverged(b.ctx); err != nil {
		return err
	}

	// Phase 4: Reactive topology projection. The membership key is a normal
	// key; the connection table is just a projection of it, rebuilt whenever
	// the key changes (signalled via OnKeyDataAdded → NotifyMembershipChanged).
	// No polling, and crucially no write-back: we never re-register to "fix" a
	// stale read — a stale read is a coherence problem to solve in the engine,
	// not paper over here.
	b.wg.Add(1)
	go b.topologyLoop()

	slog.Info("beacon started",
		"node_id", b.cfg.NodeID,
		"advertise", b.cfg.AdvertiseAddr,
		"join", b.cfg.JoinAddr,
		"cloud", b.cfg.Cloud != nil,
	)
	return nil
}

// Stop sends a REMOVE_OP for this node and stops background loops.
func (b *Beacon) Stop() {
	// Graceful departure: remove our membership entry.
	ctx := b.engine.NewContext()
	_ = ctx.Emit(buildMemberRemove(uint64(b.cfg.NodeID)))
	_ = ctx.Flush()

	if b.cancel != nil {
		b.cancel()
	}
	b.wg.Wait()

	slog.Info("beacon stopped", "node_id", b.cfg.NodeID)
}

// Members returns the current known membership.
func (b *Beacon) Members() []Member {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]Member, len(b.members))
	copy(out, b.members)
	return out
}

// waitForSymmetricPeers blocks until at least one peer is alive+symmetric
// in the health table, or ctx is cancelled.
func (b *Beacon) waitForSymmetricPeers(ctx context.Context) error {
	ht := b.pm.HealthTable()
	if ht == nil {
		return nil
	}
	for {
		if len(ht.AlivePeerIDs()) > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// primeSubscription triggers an early subscription to the membership key
// by calling GetSnapshot. NACKs from peers carry their existing membership
// tips into the local engine. Errors are non-fatal since the subscription
// will be retried during waitForMembershipConverged.
func (b *Beacon) primeSubscription() {
	ctx := b.engine.NewContext()
	_, _, err := ctx.GetSnapshot(MembershipKey)
	if err != nil {
		slog.Debug("beacon: prime subscription incomplete (will retry)", "error", err)
	}
	ctx.Flush()
}

// registerSelf publishes this node's membership entry. Two processes
// cannot bind the same host:port, so any pre-existing entry at our
// advertise address belongs to a dead predecessor — sweep it from the
// roster in the same flush so peers see the takeover atomically.
func (b *Beacon) registerSelf() error {
	ctx := b.engine.NewContext()

	snapshot, _, err := ctx.GetSnapshot(MembershipKey)
	if err != nil {
		// Snapshot read failure (no symmetric peers, partition, etc.) —
		// proceed with just our own insert. If a stale entry shares our
		// address, a later sync will surface it and we'll re-register
		// then.
		slog.Debug("beacon: registerSelf snapshot read failed, skipping collision sweep", "error", err)
	} else {
		selfID := uint64(b.cfg.NodeID)
		for _, m := range parseMembership(snapshot) {
			if m.Addr == b.cfg.AdvertiseAddr && m.NodeID != selfID {
				slog.Info("beacon: evicting stale predecessor at our address",
					"old_node_id", m.NodeID, "addr", m.Addr)
				if err := ctx.Emit(buildMemberRemove(m.NodeID)); err != nil {
					return err
				}
			}
		}
	}

	if err := ctx.Emit(buildMemberInsert(uint64(b.cfg.NodeID), b.cfg.AdvertiseAddr)); err != nil {
		return err
	}
	if err := ctx.Emit(buildMemberTypeTag()); err != nil {
		return err
	}
	return ctx.Flush()
}

// topologyLoop keeps the PeerManager's connection table as a reactive
// projection of the __swytch:members key. It reads once at startup, then
// rebuilds topology each time the key changes — driven by
// NotifyMembershipChanged (wired to OnKeyDataAdded, which fires on both local
// writes and remote effect arrivals). No poll.
//
// It runs in its own goroutine precisely because OnKeyDataAdded fires
// synchronously inside Flush / HandleRemote: doing the read there would
// deadlock the write that triggered it (GetSnapshot → ensureSubscribed can
// block on peer ACKs). So the callback only signals; the read happens here.
//
// There is deliberately no re-register. Membership entries live until an
// explicit REMOVE_OP (graceful departure or the one-time address-collision
// sweep in registerSelf). If our own entry ever reads back missing, that is a
// DAG coherence problem to fix in the engine — re-writing membership to
// compensate is exactly what produced the re-register storm.
func (b *Beacon) topologyLoop() {
	defer b.wg.Done()

	// Initial projection: don't wait for the first signal, or the topology
	// stays pinned to the DNS-synthetic view until something writes the key.
	b.refreshTopology()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-b.topoRefresh:
			b.refreshTopology()
		}
	}
}

// NotifyMembershipChanged signals that the membership key changed. Non-blocking
// and coalescing: a pending signal already means "re-read on the next
// iteration", so extras collapse into it. Safe to call from inside an engine
// callback — it never reenters the engine.
func (b *Beacon) NotifyMembershipChanged() {
	select {
	case b.topoRefresh <- struct{}{}:
	default:
	}
}

// refreshTopology reads the current membership and projects it onto the
// PeerManager. Read-only with respect to membership — it never emits a member
// effect (a GetSnapshot may still compact the chain, which is normal for any
// key).
func (b *Beacon) refreshTopology() {
	ctx := b.engine.NewContext()
	snapshot, _, err := ctx.GetSnapshot(MembershipKey)
	ctx.Flush()
	if err != nil {
		slog.Debug("beacon: membership snapshot read failed", "error", err)
		return
	}
	b.syncMembership(snapshot)
}

// syncMembership updates the PeerManager topology from the given snapshot
// if the member set has changed.
func (b *Beacon) syncMembership(snapshot *pb.ReducedEffect) {
	members := parseMembership(snapshot)

	b.mu.Lock()
	changed := !membersEqual(b.members, members)
	if changed {
		b.members = members
	}
	b.mu.Unlock()

	if changed {
		b.updateTopology(members)
	}
}

// updateTopology builds a ClusterConfig from the member list and applies it.
func (b *Beacon) updateTopology(members []Member) {
	nodes := make([]cluster.NodeConfig, 0, len(members))
	for _, m := range members {
		nodes = append(nodes, cluster.NodeConfig{
			ID:      cluster.NodeId(m.NodeID),
			Address: m.Addr,
		})
	}

	cfg := &cluster.ClusterConfig{
		NodeID:        cluster.NodeId(b.cfg.NodeID),
		Nodes:         nodes,
		TLSPassphrase: b.cfg.Passphrase,
	}

	b.pm.UpdateTopology(cfg)

	slog.Info("beacon: topology updated", "members", len(members))
}

// DetectAdvertiseAddr determines the advertise address by UDP-dialing the
// first DNS candidate (or a well-known address) and reading the local address.
// Called early — before PeerManager starts — so the TLS leaf cert gets the
// correct IP SAN.
func DetectAdvertiseAddr(joinAddr string, clusterPort int) (string, error) {
	target := "8.8.8.8:53"
	if joinAddr != "" {
		addrs, err := ResolveJoinAddr(context.Background(), nil, joinAddr, clusterPort)
		if err == nil && len(addrs) > 0 {
			target = addrs[0]
		}
	}

	conn, err := net.Dial("udp", target)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	host, _, _ := net.SplitHostPort(conn.LocalAddr().String())
	if host == "0.0.0.0" || host == "::" {
		slog.Warn("auto-detected address is unspecified, consider using --cluster-advertise")
	}

	return net.JoinHostPort(host, portStr(clusterPort)), nil
}

// membersEqual returns true if two member slices contain the same set
// (order-independent).
func membersEqual(a, b []Member) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[uint64]string, len(a))
	for _, m := range a {
		set[m.NodeID] = m.Addr
	}
	for _, m := range b {
		if addr, ok := set[m.NodeID]; !ok || addr != m.Addr {
			return false
		}
	}
	return true
}

func portStr(port int) string {
	return strconv.Itoa(port)
}
