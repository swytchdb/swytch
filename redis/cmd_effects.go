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

package redis

import (
	"flag"
	"fmt"
	"log/slog"

	"github.com/swytchdb/swytch/beacon"
	"github.com/swytchdb/swytch/cluster"
	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

// EffectsConfig holds effects-specific configuration for the redis
// CLI. The cluster-shaped fields (passphrase, join, port, advertise)
// are forwarded to beacon.RuntimeConfig; anything redis-specific
// stays here.
type EffectsConfig struct {
	Enabled            bool
	ClusterPassphrase  string
	JoinAddr           string
	ClusterPort        int
	AdvertiseAddr      string
	MemoryLimit        int64
	MemoryLimitPercent float64
	Port               int // main Redis port, used to compute default ClusterPort
}

// RegisterEffectsFlags adds effects-specific flags to the flag set.
func RegisterEffectsFlags(fs *flag.FlagSet) *EffectsConfig {
	cfg := &EffectsConfig{}
	fs.StringVar(&cfg.ClusterPassphrase, "cluster-passphrase", "", "Shared passphrase for cluster mTLS (generate with: swytch gen-passphrase)")
	fs.StringVar(&cfg.JoinAddr, "join", "", "DNS name to resolve for cluster peer discovery")
	fs.IntVar(&cfg.ClusterPort, "cluster-port", 0, "QUIC port for cluster traffic (default: --port + 1000)")
	fs.StringVar(&cfg.AdvertiseAddr, "cluster-advertise", "", "Address:port this node advertises to peers (default: auto-detect)")
	return cfg
}

// activeRuntime holds the running cluster runtime for shutdown.
var activeRuntime *beacon.Runtime

// InitializeEffects stands up the effects engine (always) plus the
// cluster runtime (when a passphrase is set). Keeps a package-level
// handle so StopCluster can tear it down. The cluster port defaults
// to --port + 1000 so a stock redis launch gets 7379, matching the
// previous implicit convention.
func InitializeEffects(cfg *EffectsConfig) error {
	cfg.Enabled = true

	clusterPort := cfg.ClusterPort
	if clusterPort == 0 && cfg.ClusterPassphrase != "" {
		clusterPort = cfg.Port + 1000
	}
	if cfg.ClusterPassphrase == "" && cfg.JoinAddr != "" {
		return fmt.Errorf("--join requires --cluster-passphrase")
	}

	rt, err := beacon.NewRuntime(beacon.RuntimeConfig{
		MemoryLimit:        cfg.MemoryLimit,
		MemoryLimitPercent: cfg.MemoryLimitPercent,
		ClusterPassphrase:  cfg.ClusterPassphrase,
		JoinAddr:           cfg.JoinAddr,
		ClusterPort:        clusterPort,
		AdvertiseAddr:      cfg.AdvertiseAddr,
	})
	if err != nil {
		return fmt.Errorf("start cluster runtime: %w", err)
	}
	activeRuntime = rt

	// Expose the engine's NodeID via the shared globals so legacy
	// callers that reach through shared.GetNodeID() still work.
	shared.InitEffectsGlobals(rt.Engine.NodeID())
	return nil
}

// GetActiveEngine returns the running effects engine (nil before
// InitializeEffects).
func GetActiveEngine() *effects.Engine {
	if activeRuntime == nil {
		return nil
	}
	return activeRuntime.Engine
}

// InitializeCluster wires redis-specific hooks onto the cluster
// runtime: subscription notifications (engine key events → pg wire
// clients) and command forwarding (PeerManager → Handler). Must be
// called after InitializeEffects and NewServer.
func InitializeCluster(cfg *EffectsConfig, handler shared.CommandHandler) error {
	if activeRuntime == nil {
		return fmt.Errorf("InitializeCluster called before InitializeEffects")
	}
	engine := activeRuntime.Engine

	h, ok := handler.(*Handler)
	if !ok {
		return fmt.Errorf("effects handler must be the standard redis Handler")
	}
	h.engine = engine

	// Wire notification callbacks from effects engine to subscription
	// manager. Chain so the beacon membership watcher (installed by
	// the runtime) continues to fire.
	subs := h.dbManager.Subscriptions
	origAdded := engine.OnKeyDataAdded
	engine.OnKeyDataAdded = func(key string) {
		if origAdded != nil {
			origAdded(key)
		}
		subs.Notify([]byte(key))
	}
	origDeleted := engine.OnKeyDeleted
	engine.OnKeyDeleted = func(key string) {
		if origDeleted != nil {
			origDeleted(key)
		}
		subs.NotifyAllWaiters([]byte(key))
	}
	origFlushAll := engine.OnFlushAll
	engine.OnFlushAll = func() {
		if origFlushAll != nil {
			origFlushAll()
		}
		subs.NotifyAll()
	}

	// Serialization-mode forwarding needs the handler to execute
	// forwarded commands locally.
	activeRuntime.SetForwardHandler(h)

	// Cross-cluster pub/sub: no-op in standalone (no PeerManager).
	if pm := activeRuntime.PeerManager; pm != nil && h.pubsubManager != nil {
		router := cluster.NewPubSubRouter(
			cluster.NodeId(engine.NodeID()),
			pm,
			h.pubsubManager,
		)
		engine.OnPubSubMessage = router.HandleInboundMessage
		engine.OnEphemeralSubscribe = router.HandleEphemeralSubscribe
		pm.SetPeerLifecycleHooks(router.OnPeerAdded, func(id cluster.NodeId) {
			router.OnPeerRemoved(id)
			engine.DropPeerFromSubscribers(id)
		})
		shared.SetPubSubClusterRouter(router)
	}

	if cfg.ClusterPassphrase == "" {
		slog.Debug("no cluster passphrase, running in single-node mode")
		return nil
	}
	slog.Info("cluster initialized",
		"node_id", engine.NodeID(),
		"join", cfg.JoinAddr,
	)
	return nil
}

// StopCluster gracefully shuts down the cluster runtime (beacon, peer
// manager, engine). Safe to call multiple times.
func StopCluster() {
	if activeRuntime == nil {
		return
	}
	// Drop the cluster pub/sub router before we tear down the runtime
	// it points at — otherwise any in-flight Publish/Subscribe would
	// reach a dead PeerManager via stale pointers.
	shared.SetPubSubClusterRouter(nil)
	if err := activeRuntime.Stop(); err != nil {
		slog.Error("cluster runtime shutdown", "error", err)
	}
	activeRuntime = nil
}

// PrintEffectsConfig prints effects configuration to stdout.
func PrintEffectsConfig(cfg *EffectsConfig) {
	if cfg.Enabled && activeRuntime != nil {
		fmt.Printf("  Effects engine: node %d\n", activeRuntime.Engine.NodeID())
	}
	if cfg.ClusterPassphrase != "" {
		fmt.Printf("  Cluster mTLS: passphrase-derived\n")
	}
	if cfg.JoinAddr != "" {
		fmt.Printf("  Cluster join: %s\n", cfg.JoinAddr)
	}
}

// EffectsUsage returns the usage string for effects flags.
func EffectsUsage() string {
	return `
Cluster options:
  --cluster-passphrase=<pass>   Shared passphrase for cluster mTLS (generate with: swytch gen-passphrase)
  --join=<dns-name>             DNS name to resolve for cluster peer discovery
  --cluster-port=<num>          QUIC port for cluster traffic (default: --port + 1000)
  --cluster-advertise=<addr>    Address:port this node advertises to peers (default: auto-detect)
`
}
