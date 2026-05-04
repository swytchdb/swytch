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
	JoinAddr          string        // DNS name to resolve for peer discovery (empty = solo)
	ClusterPort       int           // QUIC listen port for cluster traffic
	AdvertiseAddr     string        // host:port this node advertises (empty = auto-detect)
	NodeID            pb.NodeID     // ephemeral node ID
	Passphrase        string        // shared mTLS passphrase
	HeartbeatInterval time.Duration // how often to refresh membership TTL (default 10s)
	FailureTimeout    time.Duration // TTL on membership entries (default 30s)
}

func (c *Config) heartbeatInterval() time.Duration {
	if c.HeartbeatInterval > 0 {
		return c.HeartbeatInterval
	}
	return 10 * time.Second
}

func (c *Config) failureTimeout() time.Duration {
	if c.FailureTimeout > 0 {
		return c.FailureTimeout
	}
	return 30 * time.Second
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
	_, _, _, err := b.engine.GetSnapshot(MembershipKey)
	if err != nil {
		slog.Debug("beacon: prime subscription incomplete (will retry)", "error", err)
	}
}

// registerSelf writes this node's INSERT_OP + type tag + initial TTL.
func (b *Beacon) registerSelf() error {
	ctx := b.engine.NewContext()
	if err := ctx.Emit(buildMemberInsert(uint64(b.cfg.NodeID), b.cfg.AdvertiseAddr)); err != nil {
		return err
	}
	if err := ctx.Emit(buildMemberTypeTag()); err != nil {
		return err
	}
	if err := ctx.Emit(buildMemberTTLRefresh(uint64(b.cfg.NodeID), b.cfg.failureTimeout())); err != nil {
		return err
	}
	return ctx.Flush()
}

// refreshLoop periodically bumps this node's membership TTL and
// reconciles the local topology with the replicated membership list.
// Runs in its own goroutine — the membership sync intentionally does
// not run from engine OnKeyDataAdded callbacks, which fire
// synchronously inside Flush and would deadlock the first
// registerSelf write (GetSnapshot → ensureSubscribed blocks until
// peers ACK, but peers are simultaneously stuck in their own
// registerSelf).
func (b *Beacon) refreshLoop() {
	defer b.wg.Done()

	// First sync runs as soon as the loop starts rather than after
	// one heartbeat interval — otherwise the local topology stays
	// pinned to the DNS-synthetic view until the first tick fires,
	// which at the 10s default is long enough for an early client
	// query to route against an incomplete peer set.
	if snapshot, _, _, err := b.engine.GetSnapshot(MembershipKey); err == nil {
		b.syncMembership(snapshot)
	}

	ticker := time.NewTicker(b.cfg.heartbeatInterval())
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			snapshot, _, _, err := b.engine.GetSnapshot(MembershipKey)
			if err != nil {
				slog.Debug("beacon: failed to read membership before TTL refresh", "error", err)
				continue
			}
			ctx := b.engine.NewContext()
			if err := ctx.Emit(buildMemberTTLRefresh(uint64(b.cfg.NodeID), b.cfg.failureTimeout())); err != nil {
				slog.Warn("beacon: failed to emit TTL refresh", "error", err)
				continue
			}
			if err := ctx.Flush(); err != nil {
				slog.Warn("beacon: failed to flush TTL refresh", "error", err)
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
