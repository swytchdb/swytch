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

package sql

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/swytchdb/swytch/beacon"
	"github.com/swytchdb/swytch/telemetry"
	"github.com/zeebo/xxh3"
)

var Version = "dev"

// Run parses CLI arguments and runs the SQL server until terminated by
// SIGINT/SIGTERM. When a cluster passphrase is supplied the server
// joins the cluster via beacon.Runtime; otherwise it runs standalone
// against a single-node effects engine.
func Run(args []string) error {
	fs := flag.NewFlagSet("sql", flag.ExitOnError)

	listen := fs.String("listen", ":5433", "TCP address to listen on")
	advertisedVersion := fs.String("advertise-version", "17.0 (Swytch)",
		"server_version string advertised during pg handshake")
	logFormat := fs.String("log-format", "text", "Log output format: text or json")
	verbose := fs.Bool("v", false, "Verbose (debug) logging")

	// Cluster flags — same shape as the redis CLI so operators can
	// reuse a single set of deployment envs.
	clusterPassphrase := fs.String("cluster-passphrase", "",
		"Shared passphrase for cluster mTLS (generate with: swytch gen-passphrase)")
	joinAddr := fs.String("join", "",
		"DNS name to resolve for cluster peer discovery")
	clusterPort := fs.Int("cluster-port", 0,
		"QUIC port for cluster traffic (default: --listen port + 1000)")
	clusterAdvertise := fs.String("cluster-advertise", "",
		"Address:port this node advertises to peers (default: auto-detect)")
	maxMemoryStr := fs.String("maxmemory", "64mb",
		"Max effects-engine memory (e.g. 256mb, 4gb)")

	// Telemetry
	noTelemetry := fs.Bool("no-telemetry", false, "Disable anonymous usage telemetry")
	inviteMe := fs.String("invite-me", "", "Register as an early adopter with your email (overrides --no-telemetry)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if *clusterPassphrase == "" && *joinAddr != "" {
		return fmt.Errorf("--join requires --cluster-passphrase")
	}

	telemetryEnabled := !*noTelemetry || *inviteMe != ""
	telemetry.PrintBanner(telemetryEnabled, *inviteMe)

	maxMemoryBytes, maxMemoryPct, err := beacon.ParseMemoryLimit(*maxMemoryStr)
	if err != nil {
		return fmt.Errorf("invalid --maxmemory value: %w", err)
	}

	// Accept a bare port number for --listen (e.g. "5432") and normalise
	// to ":5432" — net.Listen rejects the bare form but docker-compose
	// configs frequently use it.
	*listen = normaliseListenAddr(*listen)

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: level}
	if *logFormat == "json" {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}
	logger := slog.New(handler)
	// Package-internal code in effects/, cluster/, and beacon/ reaches
	// for slog.Default() rather than taking a Logger argument. Install
	// ours as default so -v / --log-format flags apply globally rather
	// than only to the bits we explicitly pass a Logger to.
	slog.SetDefault(logger)

	// Derive cluster port from --listen when unset. Parses the port
	// component only; a listen of ":5433" gives 5433 → cluster 6433.
	cport := *clusterPort
	if cport == 0 && *clusterPassphrase != "" {
		cport = deriveClusterPort(*listen)
		if cport == 0 {
			return fmt.Errorf(
				"could not derive cluster port from --listen=%q; pass --cluster-port explicitly",
				*listen)
		}
	}

	rt, err := beacon.NewRuntime(beacon.RuntimeConfig{
		MemoryLimit:        maxMemoryBytes,
		MemoryLimitPercent: maxMemoryPct,
		ClusterPassphrase:  *clusterPassphrase,
		JoinAddr:           *joinAddr,
		ClusterPort:        cport,
		AdvertiseAddr:      *clusterAdvertise,
		Logger:             logger,
	})
	if err != nil {
		return fmt.Errorf("start cluster runtime: %w", err)
	}

	server, err := NewServer(ServerConfig{
		Address:           *listen,
		AdvertisedVersion: *advertisedVersion,
		Logger:            logger,
		Engine:            rt.Engine,
	})
	if err != nil {
		return errors.Join(err, rt.Stop())
	}

	if err := server.StartAsync(); err != nil {
		return errors.Join(err, rt.Stop())
	}

	// Start telemetry client
	var telemetryCancel context.CancelFunc
	var telemetryDone chan struct{}
	if telemetryEnabled {
		telCtx, cancel := context.WithCancel(context.Background())
		telemetryCancel = cancel
		startTime := time.Now()
		nodeID := fmt.Sprintf("%x", uint64(rt.Engine.NodeID()))
		var clusterID string
		if *joinAddr != "" {
			clusterID = fmt.Sprintf("%x", xxh3.HashString(*joinAddr))
		}
		tc := telemetry.New(telemetry.Config{
			NodeID:       nodeID,
			ClusterID:    clusterID,
			Version:      Version,
			Features:     "sql",
			AdopterEmail: *inviteMe,
			StatsFunc: func() telemetry.HeartbeatStats {
				nodes := 1
				if rt.PeerManager != nil {
					nodes = len(rt.PeerManager.PeerIDs()) + 1
				}
				return telemetry.HeartbeatStats{
					Nodes:         nodes,
					UptimeSeconds: int(time.Since(startTime).Seconds()),
				}
			},
		})
		telemetryDone = make(chan struct{})
		go func() { defer close(telemetryDone); tc.Run(telCtx) }()
	}

	stopTelemetry := func() {
		if telemetryCancel != nil {
			telemetryCancel()
			<-telemetryDone
		}
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info("shutdown signal received", "signal", sig.String())
		stopTelemetry()
	case err := <-server.Done():
		stopTelemetry()
		if err != nil {
			return errors.Join(
				fmt.Errorf("server exited: %w", err),
				rt.Stop(),
			)
		}
		return rt.Stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var errs []error
	if err := server.Stop(shutdownCtx); err != nil {
		errs = append(errs, fmt.Errorf("server stop: %w", err))
	}
	if err := rt.Stop(); err != nil {
		errs = append(errs, fmt.Errorf("runtime stop: %w", err))
	}
	return errors.Join(errs...)
}

// deriveClusterPort extracts the port from a :port or host:port
// listen string and returns listen_port + 1000. Returns 0 on parse
// failure so the caller can surface a clean error.
func deriveClusterPort(listen string) int {
	_, portStr, err := net.SplitHostPort(listen)
	if err != nil {
		return 0
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0
	}
	return port + 1000
}

// normaliseListenAddr prepends ":" when the operator passed a bare
// port number. Everything else is returned unchanged so host:port,
// :port, and [::1]:port forms all still work.
func normaliseListenAddr(addr string) string {
	if addr == "" {
		return addr
	}
	if _, err := strconv.Atoi(addr); err == nil {
		return ":" + addr
	}
	return addr
}
