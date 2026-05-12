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
	"fmt"
	"log/slog"
	"time"

	"github.com/swytchdb/swytch/cluster"
	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/keytrie"
)

// RuntimeConfig describes what a swytch process needs to join a
// cluster. Every transport (redis, sql, anything future) hands the
// same config to NewRuntime and receives back a live engine +
// PeerManager + beacon. Transport-specific wiring (subscription
// callbacks in redis, ORM dialect hooks in sql) stays in the
// transport package.
type RuntimeConfig struct {
	// MemoryLimit caps effects-engine memory at a fixed byte count.
	// Mutually exclusive with MemoryLimitPercent — set one or the
	// other. Forwarded to effects.EngineConfig.
	MemoryLimit int64

	// MemoryLimitPercent, when non-zero, expresses the memory limit
	// as a fraction (0,1] of the memory available to this process
	// (cgroup limit or system RAM). Forwarded to
	// effects.EngineConfig where the cache re-evaluates each tick so
	// the cache tracks live changes in the available budget. Mutually
	// exclusive with MemoryLimit.
	MemoryLimitPercent float64

	// ClusterPassphrase enables cluster mode. Empty → single-node
	// engine, no peer manager, no beacon. Non-empty → full cluster
	// bootstrap. Must match across every node in the cluster.
	ClusterPassphrase string

	// JoinAddr is the DNS name the beacon resolves to find peers at
	// startup. Empty is fine in solo mode; for a real cluster point
	// it at a name that resolves to every peer's cluster-port IP.
	JoinAddr string

	// ClusterPort is the QUIC port used for cluster traffic. 0 =
	// no default; the caller is responsible for supplying a non-zero
	// value when ClusterPassphrase is set.
	ClusterPort int

	// AdvertiseAddr is the <host>:<port> this node advertises to
	// peers. Empty triggers auto-detection via DetectAdvertiseAddr.
	AdvertiseAddr string

	// Logger — optional, defaults to slog.Default().
	Logger *slog.Logger
}

// Runtime is the handle callers keep for the lifetime of the process.
// Fields are exported so transport code can wire additional
// notifications onto the engine (e.g. redis subscription callbacks).
type Runtime struct {
	Engine      *effects.Engine
	PeerManager *cluster.PeerManager
	Beacon      *Beacon

	cfg RuntimeConfig
}

// NewRuntime builds and starts the engine. If cfg.ClusterPassphrase
// is empty the returned Runtime is single-node: just the engine,
// ready to serve local reads/writes. If it's set, the method
// additionally stands up the PeerManager, wires anti-entropy, and
// starts the beacon for DNS-based peer discovery.
//
// Callers should Stop the Runtime on shutdown. Transport code that
// adds its own callbacks via Engine.OnKeyDataAdded etc. is
// responsible for unwiring those — Stop only tears down what
// NewRuntime built.
func NewRuntime(cfg RuntimeConfig) (*Runtime, error) {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	engine := effects.NewEngine(effects.EngineConfig{
		NodeID:             pb.NewNodeID(),
		Index:              keytrie.New(),
		DefaultMode:        effects.SafeMode,
		MemoryLimit:        cfg.MemoryLimit,
		MemoryLimitPercent: cfg.MemoryLimitPercent,
	})
	log.Info("effects engine initialized", "node_id", engine.NodeID())

	rt := &Runtime{
		Engine: engine,
		cfg:    cfg,
	}

	// Single-node mode: done.
	if cfg.ClusterPassphrase == "" {
		log.Debug("no cluster passphrase, running single-node")
		return rt, nil
	}

	if cfg.ClusterPort == 0 {
		return nil, errors.Join(
			fmt.Errorf("cluster mode requires a non-zero ClusterPort"),
			closeEngineOnError(engine),
		)
	}

	// Figure out what address we tell peers to dial us on. Auto-detect
	// before PeerManager starts so the TLS leaf cert embeds the right
	// IP SAN (not 0.0.0.0).
	advertise := cfg.AdvertiseAddr
	if advertise == "" {
		detected, err := DetectAdvertiseAddr(cfg.JoinAddr, cfg.ClusterPort)
		if err != nil {
			log.Warn("failed to auto-detect advertise address, using 0.0.0.0", "error", err)
			advertise = fmt.Sprintf("0.0.0.0:%d", cfg.ClusterPort)
		} else {
			advertise = detected
			log.Info("auto-detected advertise address", "addr", advertise)
		}
	}

	peerCfg := &cluster.ClusterConfig{
		NodeID: cluster.NodeId(engine.NodeID()),
		Nodes: []cluster.NodeConfig{
			{
				ID:      cluster.NodeId(engine.NodeID()),
				Address: advertise,
			},
		},
		TLSPassphrase: cfg.ClusterPassphrase,
	}

	logReader := cluster.NewEngineLogReader(engine.EffectCache())
	effectHandler := cluster.NewEngineEffectHandler(engine)

	pm, err := cluster.NewPeerManager(peerCfg, effectHandler, logReader)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("failed to create peer manager: %w", err),
			closeEngineOnError(engine),
		)
	}
	if err := pm.Start(context.Background()); err != nil {
		return nil, errors.Join(
			fmt.Errorf("failed to start peer manager: %w", err),
			closeEngineOnError(engine),
		)
	}
	rt.PeerManager = pm

	// Engine replicates outbound via the PeerManager.
	engine.SetBroadcaster(pm)

	// When a peer rejoins, all subscribed keys need a fresh bootstrap.
	// Engine also uses the peer-health RTT data to pick serialization
	// mode, so wire that alongside.
	if ht := pm.HealthTable(); ht != nil {
		ht.OnPeerRecovered = func(peerID cluster.NodeId) {
			engine.ReconvergeAllKeys()
		}
		engine.SetRTTProvider(ht)
	}

	engine.StartAntiEntropy(3 * time.Second)

	// Beacon: DNS discovery + dynamic membership.
	b := New(Config{
		JoinAddr:      cfg.JoinAddr,
		ClusterPort:   cfg.ClusterPort,
		AdvertiseAddr: advertise,
		NodeID:        engine.NodeID(),
		Passphrase:    cfg.ClusterPassphrase,
	}, engine, pm)

	if err := b.Start(context.Background()); err != nil {
		return nil, errors.Join(
			fmt.Errorf("failed to start beacon: %w", err),
			closeRuntimeOnError(rt),
		)
	}
	rt.Beacon = b

	log.Info("cluster runtime ready",
		"node_id", engine.NodeID(),
		"cluster_port", cfg.ClusterPort,
		"advertise", advertise,
		"join", cfg.JoinAddr,
	)

	return rt, nil
}

// Stop tears down the beacon, peer manager, and engine in the order
// that avoids use-after-close races (beacon before PM because beacon
// calls into PM; PM before engine because broadcast close paths
// reference the engine).
func (r *Runtime) Stop() error {
	var errs []error
	if r.Beacon != nil {
		r.Beacon.Stop()
		r.Beacon = nil
	}
	if r.PeerManager != nil {
		r.PeerManager.Stop()
		r.PeerManager = nil
	}
	if r.Engine != nil {
		if err := r.Engine.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close engine: %w", err))
		}
		r.Engine = nil
	}
	return errors.Join(errs...)
}

// SetForwardHandler wires the transport's ForwardHandler into the
// peer manager so serialization-mode forwarding can dispatch
// cross-node commands back into the transport for execution. Safe to
// call on a single-node Runtime — a no-op in that case.
func (r *Runtime) SetForwardHandler(h cluster.ForwardHandler) {
	if r.PeerManager != nil {
		r.PeerManager.SetForwardHandler(h)
	}
}

func closeEngineOnError(engine *effects.Engine) error {
	if err := engine.Close(); err != nil {
		return fmt.Errorf("close engine: %w", err)
	}
	return nil
}

func closeRuntimeOnError(r *Runtime) error {
	var errs []error
	if r.Beacon != nil {
		r.Beacon.Stop()
	}
	if r.PeerManager != nil {
		r.PeerManager.Stop()
	}
	if r.Engine != nil {
		if err := r.Engine.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close engine: %w", err))
		}
	}
	return errors.Join(errs...)
}
