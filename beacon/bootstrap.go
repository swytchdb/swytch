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
	"errors"
	"log/slog"
	"net"
	"time"

	"github.com/swytchdb/swytch/cluster"
	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
)

// bootstrap performs peer discovery (DNS, or the cloud membership roster when
// --cloud is set), installs a temporary topology, and waits for at least one
// peer to become reachable. It does NOT read authoritative membership — that
// happens in waitForMembershipConverged after self-registration, because
// every node's bootstrap waits for the others to be visible and nobody is
// visible until they've registered themselves. Reading membership before
// registerSelf deadlocks the fleet when nodes start simultaneously.
//
// On return: PeerManager is connected to at least one candidate, or
// we've fallen back to solo mode with a warning.
func (b *Beacon) bootstrap(ctx context.Context) error {
	var candidates []string
	if b.cfg.Cloud != nil {
		candidates = b.cloudCandidates(ctx)
	} else {
		var err error
		candidates, err = b.resolveJoinAddrWithRetry(ctx)
		if err != nil {
			slog.Warn("beacon: DNS resolution failed after retries, starting solo", "error", err)
			return nil
		}
	}

	candidates = b.filterSelf(candidates)
	if len(candidates) == 0 {
		slog.Info("beacon: no peers found, starting solo")
		return nil
	}

	slog.Info("beacon: discovered candidates", "candidates", candidates)
	b.setTemporaryTopology(candidates)
	b.expectedPeers = len(candidates)

	// Wait for at least one candidate to become reachable so membership
	// subscriptions have a peer to exchange NACKs with.
	waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Second)
	defer waitCancel()
	if err := b.pm.WaitForAnyPeer(waitCtx); err != nil {
		slog.Warn("beacon: no candidates reachable, starting solo", "error", err)
		// Bootstrap candidates are DNS guesses, not members. Conceding solo
		// ends their relevance: keeping them would dial dead addresses forever
		// (stale DNS from a rescheduled pod, our own other-address-family
		// record) and expectedPeers > 0 would stall the symmetric-peer wait
		// and membership convergence against peers that never come. Joining a
		// cluster that was merely unreachable here is the same human
		// adjudication as any membership repair — restart a node. Real members
		// are never dropped by this: they arrive later through membership
		// effects, which own the topology from then on.
		b.setTemporaryTopology(nil)
		b.expectedPeers = 0
		return nil
	}
	return nil
}

// cloudCandidates discovers peer addresses from the cloud's membership
// roster. A discovery *error* is retried with capped backoff, same as the DNS
// path — a transient cloud hiccup at startup must not strand this node solo
// while the rest of the fleet joins. A *successful* read that yields no
// members is authoritative (fresh cluster, or we're first) and starts solo.
func (b *Beacon) cloudCandidates(ctx context.Context) []string {
	reduced, err := b.discoverMembersWithRetry(ctx)
	if err != nil {
		if errors.Is(err, effects.ErrCDNBlobMissing) {
			// The roster frontier references ancestry whose CDN blobs are
			// gone — a durable hole, not an outage. Retrying can't heal it
			// and every node in the fleet is equally stuck against it, so
			// supersede the frontier and start solo; peers pick up the
			// repaired roster on their next boot. Nodes restarting in the
			// same instant can each mint a repair, leaving sibling snapshots
			// that keep the key unreadable — accepted: the next
			// non-simultaneous restart repairs over them, since the walk
			// fails with the same ErrCDNBlobMissing beneath the siblings.
			b.repairMembershipFrontier(ctx, err)
			return nil
		}
		slog.Warn("beacon: cloud member discovery failed after retries, starting solo", "error", err)
		return nil
	}
	members := parseMembership(reduced)
	selfID := uint64(b.cfg.NodeID)
	addrs := make([]string, 0, len(members))
	for _, m := range members {
		if m.NodeID == selfID {
			continue
		}
		addrs = append(addrs, m.Addr)
	}
	return addrs
}

// repairMembershipFrontier is the one-shot startup repair for a cloud
// membership frontier whose ancestry is provably holed. Held under
// membershipWriteMu so a cloud-pushed removal burst can't emit a sibling
// branch while the repair snapshot is being minted.
func (b *Beacon) repairMembershipFrontier(ctx context.Context, cause error) {
	slog.Warn("beacon: cloud membership frontier is unreadable, repairing with a superseding snapshot",
		"error", cause)
	b.membershipWriteMu.Lock()
	defer b.membershipWriteMu.Unlock()
	superseded, err := b.cfg.Cloud.RepairFrontier(ctx, MembershipKey)
	if err != nil {
		slog.Error("beacon: membership frontier repair failed, starting solo unrepaired", "error", err)
		return
	}
	slog.Warn("beacon: cloud membership frontier repaired, starting solo",
		"superseded_tips", superseded)
}

// discoverMembersWithRetry reads the cloud membership roster, retrying on
// error with capped backoff until it succeeds or the deadline passes. Same
// rationale as resolveJoinAddrWithRetry: an error means "the cloud isn't
// reachable yet", not "there is no cluster", and conceding solo on the first
// error is what strands nodes in a split-brain membership.
func (b *Beacon) discoverMembersWithRetry(ctx context.Context) (*pb.ReducedEffect, error) {
	const (
		perAttemptTimeout = 10 * time.Second
		discoverDeadline  = 30 * time.Second
		backoffStart      = 250 * time.Millisecond
		backoffMax        = 3 * time.Second
	)

	deadlineCtx, cancel := context.WithTimeout(ctx, discoverDeadline)
	defer cancel()

	backoff := backoffStart
	for {
		attemptCtx, attemptCancel := context.WithTimeout(deadlineCtx, perAttemptTimeout)
		reduced, err := b.cfg.Cloud.DiscoverMembers(attemptCtx, MembershipKey)
		attemptCancel()
		if err == nil {
			return reduced, nil
		}
		slog.Debug("beacon: cloud member discovery failed, retrying", "error", err, "backoff", backoff)

		select {
		case <-deadlineCtx.Done():
			return nil, err
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, backoffMax)
	}
}

// resolveJoinAddrWithRetry resolves JoinAddr, retrying on error with capped
// backoff until it succeeds or the deadline passes. A resolution error at
// startup is almost always a transient race, not a permanent failure: a
// Kubernetes headless service returns NXDOMAIN ("no such host") until at least
// one peer's endpoint is Ready, so an error means "peers aren't up yet", not
// "there is no cluster". Conceding solo on the first error is what stranded
// nodes in a split-brain membership. A *successful* resolution that returns no
// usable peers is authoritative (we may be the first node) — the caller starts
// solo, and late joiners then converge via the reactive membership projection.
func (b *Beacon) resolveJoinAddrWithRetry(ctx context.Context) ([]string, error) {
	const (
		perAttemptTimeout = 10 * time.Second
		resolveDeadline   = 30 * time.Second
		backoffStart      = 250 * time.Millisecond
		backoffMax        = 3 * time.Second
	)

	deadlineCtx, cancel := context.WithTimeout(ctx, resolveDeadline)
	defer cancel()

	backoff := backoffStart
	var lastErr error
	for {
		attemptCtx, attemptCancel := context.WithTimeout(deadlineCtx, perAttemptTimeout)
		candidates, err := ResolveJoinAddr(attemptCtx, nil, b.cfg.JoinAddr, b.cfg.ClusterPort)
		attemptCancel()
		if err == nil {
			return candidates, nil
		}
		lastErr = err
		slog.Debug("beacon: DNS resolution failed, retrying", "error", err, "backoff", backoff)

		select {
		case <-deadlineCtx.Done():
			return nil, lastErr
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, backoffMax)
	}
}

// waitForMembershipConverged blocks until the membership key shows
// every DNS-discovered candidate peer. This is required for correctness:
// a node that accepts client traffic before its topology matches the
// full cluster will call UpdateTopology with a partial peer list,
// disconnect the rest, and subsequent SafeMode writes will either
// fail with region-partitioned or — worse — silently commit without
// replicating to peers the node doesn't know about.
//
// Called AFTER registerSelf so the fleet-wide deadlock is broken:
// every node has published its own entry by the time it starts
// waiting for the others. On solo mode (expectedPeers == 0) this is
// a no-op.
func (b *Beacon) waitForMembershipConverged(ctx context.Context) error {
	if b.expectedPeers == 0 {
		return nil
	}

	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	members, err := b.readMembershipWithRetry(waitCtx, b.expectedPeers)
	if err != nil {
		return err
	}

	slog.Info("beacon: discovered members from effects", "count", len(members))
	b.mu.Lock()
	b.members = members
	b.mu.Unlock()
	b.updateTopology(members)
	return nil
}

// readMembershipWithRetry polls GetSnapshot(MembershipKey) until the
// cluster has enough same-region peers reachable to satisfy SafeMode
// AND the parsed membership contains at least expectedPeers non-self
// entries (matching the DNS-discovered non-self candidate count), or
// ctx is cancelled. parseMembership returns the full member set
// including self; the threshold must compare apples-to-apples or the
// loop returns early with a partial topology that drops a slow
// registrant.
func (b *Beacon) readMembershipWithRetry(ctx context.Context, expectedPeers int) ([]Member, error) {
	backoff := 100 * time.Millisecond
	const maxBackoff = 1 * time.Second
	selfID := uint64(b.cfg.NodeID)

	for {
		ectx := b.engine.NewContext()
		snapshot, _, err := ectx.GetSnapshot(MembershipKey)
		ectx.Flush()
		if err == nil {
			members := parseMembership(snapshot)
			nonSelf := 0
			for _, m := range members {
				if m.NodeID != selfID {
					nonSelf++
				}
			}
			if nonSelf >= expectedPeers {
				return members, nil
			}
			slog.Debug("beacon: membership not fully converged",
				"have", nonSelf, "want", expectedPeers, "backoff", backoff)
		} else if errors.Is(err, effects.ErrRegionPartitioned) {
			slog.Debug("beacon: membership read waiting for peers",
				"error", err, "backoff", backoff)
		} else {
			return nil, err
		}

		select {
		case <-ctx.Done():
			return nil, errors.Join(ctx.Err(), err)
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// filterSelf removes addresses that match this node's advertise address.
func (b *Beacon) filterSelf(candidates []string) []string {
	selfAddr := b.cfg.AdvertiseAddr
	if selfAddr == "" {
		return candidates
	}

	selfHost, selfPort, _ := net.SplitHostPort(selfAddr)

	filtered := make([]string, 0, len(candidates))
	for _, addr := range candidates {
		host, port, _ := net.SplitHostPort(addr)
		if host == selfHost && port == selfPort {
			continue
		}
		filtered = append(filtered, addr)
	}
	return filtered
}

// setTemporaryTopology builds a ClusterConfig with the given addresses as
// peers (using synthetic NodeIDs) and applies it to the PeerManager.
// These are temporary — once real membership effects arrive, the topology
// is replaced with actual NodeIDs.
func (b *Beacon) setTemporaryTopology(candidates []string) {
	nodes := []cluster.NodeConfig{
		{
			ID:      cluster.NodeId(b.cfg.NodeID),
			Address: b.cfg.AdvertiseAddr,
		},
	}

	for i, addr := range candidates {
		// Use synthetic NodeIDs for temporary peers. These are distinguishable
		// from real NodeIDs (which have timestamp in upper 32 bits) by using
		// small sequential values.
		syntheticID := cluster.NodeId(uint64(i + 1))
		nodes = append(nodes, cluster.NodeConfig{
			ID:      syntheticID,
			Address: addr,
		})
	}

	cfg := &cluster.ClusterConfig{
		NodeID:        cluster.NodeId(b.cfg.NodeID),
		Nodes:         nodes,
		TLSPassphrase: b.cfg.Passphrase,
	}

	b.pm.UpdateTopology(cfg)
	slog.Debug("beacon: set temporary topology", "candidates", len(candidates))
}
