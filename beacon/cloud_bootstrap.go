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
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
)

// cloudBootstrap is the --cloud analogue of DNS discovery. With no --join peer to
// gossip with, it reads the membership key's frontier from Cloud purely to learn
// a peer ADDRESS to connect to. It deliberately does NOT join the membership
// key's live state — no subscribe, no index writes — so that once connected, the
// node bootstraps __swytch:members from the peer exactly like a --join node. That
// peer bootstrap is what ships the peer's key filter and authoritative state;
// pre-loading membership from Cloud here would suppress it (the node wouldn't
// diverge, the peer would ACK instead of NACK, and its filter would never
// arrive — surfacing later as false read-misses). Every failure degrades to solo.
//
// The known race — two first nodes booting before either has published — is
// accepted (they converge once one sees the other's writes).
func (b *Beacon) cloudBootstrap(ctx context.Context) error {
	tipsCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	tipsByKey, err := b.cfg.Cloud.GetTips(tipsCtx, MembershipKey)
	cancel()
	if err != nil {
		slog.Warn("beacon: cloud tip lookup failed, starting solo", "error", err)
		return nil
	}
	tips := tipsByKey[MembershipKey]
	if len(tips) == 0 {
		slog.Info("beacon: no membership in cloud, starting solo (first node)")
		return nil
	}

	candidates := b.filterSelf(memberAddrs(b.cloudDiscoverMembers(ctx, tips), uint64(b.cfg.NodeID)))
	if len(candidates) == 0 {
		slog.Info("beacon: cloud membership names no other peers, starting solo")
		return nil
	}

	slog.Info("beacon: discovered candidates via cloud", "candidates", candidates)
	b.setTemporaryTopology(candidates)
	b.expectedPeers = len(candidates)

	// Wait for at least one candidate to become reachable so the membership
	// subscription (Start's next phase) has a peer to bootstrap from — the same
	// gate DNS bootstrap uses.
	waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Second)
	defer waitCancel()
	if err := b.pm.WaitForAnyPeer(waitCtx); err != nil {
		slog.Warn("beacon: no cloud-discovered candidates reachable, starting solo", "error", err)
		return nil
	}
	return nil
}

// cloudDiscoverMembers reads member ADDRESSES off the cloud DAG for discovery
// only. It walks the transitive dep-closure of the given tips, fetching each
// effect from the CDN, and collects the address from every membership INSERT it
// sees — straight off the raw effect, with NO engine interaction (no cache, no
// index, no subscribe). That is deliberate: touching the engine here would join
// the membership key's live state and suppress the peer filter exchange; instead
// the authoritative membership is bootstrapped from the peer after we connect
// (like --join). A stale/removed address just fails to connect, and WaitForAnyPeer
// moves on. Best-effort: a blob missing mid-walk yields a partial address set.
func (b *Beacon) cloudDiscoverMembers(ctx context.Context, tips []effects.Tip) []Member {
	seen := make(map[effects.Tip]bool, len(tips))
	found := make(map[uint64]string)
	queue := append([]effects.Tip(nil), tips...)
	for len(queue) > 0 {
		ref := queue[0]
		queue = queue[1:]
		if seen[ref] {
			continue
		}
		seen[ref] = true

		pref := &pb.EffectRef{NodeId: ref[0], Offset: ref[1]}
		eff, err := b.cfg.Cloud.FetchEffect(ctx, pref)
		if err != nil {
			slog.Debug("beacon: cloud effect fetch failed", "ref", pref, "error", err)
			continue
		}
		if d := eff.GetData(); d != nil && d.Op == pb.EffectOp_INSERT_OP {
			if id := nodeIDFromBytes(d.GetId()); id != 0 {
				if addr := string(d.GetRaw()); addr != "" {
					found[id] = addr
				}
			}
		}
		for _, dep := range eff.Deps {
			queue = append(queue, effects.Tip{dep.GetNodeId(), dep.GetOffset()})
		}
	}

	members := make([]Member, 0, len(found))
	for id, addr := range found {
		members = append(members, Member{NodeID: id, Addr: addr})
	}
	return members
}

// memberAddrs returns the advertise addresses of every member except self.
func memberAddrs(members []Member, selfID uint64) []string {
	addrs := make([]string, 0, len(members))
	for _, m := range members {
		if m.NodeID == selfID {
			continue
		}
		addrs = append(addrs, m.Addr)
	}
	return addrs
}
