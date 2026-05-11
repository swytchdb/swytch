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

	"github.com/swytchdb/cache/cluster"
	pb "github.com/swytchdb/cache/cluster/proto"
	"github.com/swytchdb/cache/effects"
)

// Config holds beacon configuration parsed from CLI flags.
type Config struct {
	JoinAddr      string        // DNS name to resolve for peer discovery (empty = solo)
	ClusterPort   int           // QUIC listen port for cluster traffic
	AdvertiseAddr string        // host:port this node advertises (empty = auto-detect)
	NodeID        pb.NodeID     // ephemeral node ID
	Passphrase    string        // shared mTLS passphrase
	SyncInterval  time.Duration // how often to reconcile local topology with __swytch:members (default 10s)
}

func (c *Config) syncInterval() time.Duration {
	if c.SyncInterval > 0 {
		return c.SyncInterval
	}
	return 10 * time.Second
}

// Beacon manages cluster discovery and dynamic membership via effects.
type Beacon struct {
	cfg    Config
	engine *effects.Engine
	pm     *cluster.PeerManager

	members       []Member
	expectedPeers int // DNS-discovered non-self candidate count, set during bootstrap
	mu            sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New creates a beacon. Call Start to begin discovery and membership.
func New(cfg Config, engine *effects.Engine, pm *cluster.PeerManager) *Beacon {
	return &Beacon{
		cfg:    cfg,
		engine: engine,
		pm:     pm,
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

	// Phase 1: DNS discovery + temporary topology + peer reachability.
	// Does not read authoritative membership — that would deadlock when
	// every node waits for the others before registering itself.
	if b.cfg.JoinAddr != "" {
		if err := b.bootstrap(b.ctx); err != nil {
			return err
		}
	}

	if b.cfg.JoinAddr != "" && b.expectedPeers > 0 {
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

	// Phase 4: Background refresh + membership sync.
	b.wg.Add(1)
	go b.refreshLoop()

	slog.Info("beacon started",
		"node_id", b.cfg.NodeID,
		"advertise", b.cfg.AdvertiseAddr,
		"join", b.cfg.JoinAddr,
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

// refreshLoop reconciles the local topology with the replicated
// membership list on a fixed cadence. Membership entries no longer
// carry a TTL — they live until an explicit REMOVE_OP (graceful
// departure, or address-collision sweep when a fresh process binds the
// same host:port). The loop just pulls the latest snapshot so newly-
// observed joins propagate into the PeerManager topology.
//
// Runs in its own goroutine — the sync intentionally does not run from
// engine OnKeyDataAdded callbacks, which fire synchronously inside
// Flush and would deadlock the first registerSelf write (GetSnapshot
// → ensureSubscribed blocks until peers ACK, but peers are
// simultaneously stuck in their own registerSelf).
func (b *Beacon) refreshLoop() {
	defer b.wg.Done()

	// First sync runs immediately rather than after one tick —
	// otherwise the local topology stays pinned to the DNS-synthetic
	// view until the first tick fires, which at the 10s default is
	// long enough for an early client query to route against an
	// incomplete peer set.
	{
		ctx := b.engine.NewContext()
		snapshot, _, err := ctx.GetSnapshot(MembershipKey)
		if err == nil {
			b.syncMembership(snapshot)
		}
		ctx.Flush()
	}

	ticker := time.NewTicker(b.cfg.syncInterval())
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			ctx := b.engine.NewContext()
			snapshot, _, err := ctx.GetSnapshot(MembershipKey)
			ctx.Flush()
			if err != nil {
				slog.Debug("beacon: membership snapshot read failed", "error", err)
				continue
			}
			if snapshot == nil || snapshot.NetAdds == nil || snapshot.NetAdds[string(nodeIDBytes(uint64(b.cfg.NodeID)))] == nil {
				if err := b.registerSelf(); err != nil {
					panic("beacon: failed to re-register after membership loss: " + err.Error())
				}
				continue
			}
			// If a prior registerSelf could not read the snapshot (e.g.
			// partition or no symmetric peers) it skipped the collision
			// sweep and a stale predecessor at our address may still be
			// in the roster. Re-register so the sweep runs against the
			// now-visible snapshot.
			if hasDuplicateAdvertiseAddr(snapshot, uint64(b.cfg.NodeID), b.cfg.AdvertiseAddr) {
				slog.Info("beacon: stale predecessor still at our address, re-registering",
					"addr", b.cfg.AdvertiseAddr)
				if err := b.registerSelf(); err != nil {
					panic("beacon: failed to re-register after duplicate-address detection: " + err.Error())
				}
				continue
			}
			b.syncMembership(snapshot)
		}
	}
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
