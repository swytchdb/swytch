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
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/swytchdb/cache/beacon"
	"github.com/swytchdb/cache/metrics"
	"github.com/swytchdb/cache/redis/shared"
	"github.com/swytchdb/cache/telemetry"
	"github.com/swytchdb/cache/tracing"
	"github.com/zeebo/xxh3"
)

// Version is set at build time
var Version = "dev"

// Main is the entry point for the Redis server CLI
func Main(args []string) {
	fs := flag.NewFlagSet("redis", flag.ExitOnError)

	// Standard Redis options
	port := fs.Int("port", 6379, "TCP port to listen on")
	bind := fs.String("bind", "127.0.0.1", "Bind address")
	unixSocket := fs.String("unixsocket", "", "Unix socket path")
	unixSocketPerm := fs.Int("unixsocketperm", 0700, "Unix socket permissions")
	databases := fs.Int("databases", 16, "Number of databases")
	maxMemoryStr := fs.String("maxmemory", "64mb", "Max memory (e.g., 256mb, 4gb)")
	requirePass := fs.String("requirepass", "", "Require password for AUTH (deprecated, use --aclfile)")
	aclFile := fs.String("aclfile", "", "Path to ACL file for user authentication")
	maxClients := fs.Int("maxclients", 0, "Max concurrent clients (0 = unlimited)")
	timeout := fs.Int("timeout", 0, "Client idle timeout in seconds (0 = disabled)")

	// TLS options
	tlsCert := fs.String("tls-cert-file", "", "Path to TLS certificate file (PEM format)")
	tlsKey := fs.String("tls-key-file", "", "Path to TLS private key file (PEM format)")
	tlsCA := fs.String("tls-ca-cert-file", "", "Path to CA certificate for client verification (mTLS)")
	tlsMinVersion := fs.String("tls-min-version", "1.2", "Minimum TLS version: 1.0, 1.1, 1.2, 1.3")

	// Swytch extensions
	threads := fs.Int("threads", 0, "Number of threads (0 = use all CPUs)")

	// Profiling and metrics
	pprofAddr := fs.String("pprof", "", "Enable pprof on this address (e.g., localhost:6060)")
	metricsPort := fs.Int("metrics-port", 0, "Enable Prometheus metrics on this port (e.g., 9090)")

	// Licensing

	// Help and version
	showHelp := fs.Bool("h", false, "Show help")
	showVersion := fs.Bool("version", false, "Show version")
	verbose := fs.Bool("v", false, "Verbose output")
	debug := fs.Bool("debug", false, "Enable debug logging (shows all commands processed)")
	logFormat := fs.String("log-format", "text", "Log output format: text or json")

	// OpenTelemetry tracing
	otelEndpoint := fs.String("otel-endpoint", "", "OTLP HTTP endpoint for tracing (e.g., localhost:4318). Empty = disabled")
	otelInsecure := fs.Bool("otel-insecure", false, "Use HTTP instead of HTTPS for OTLP endpoint")

	// Telemetry
	noTelemetry := fs.Bool("no-telemetry", false, "Disable anonymous usage telemetry")
	inviteMe := fs.String("invite-me", "", "Register as an early adopter with your email (overrides --no-telemetry)")

	// Effects-specific flags (only present in effects builds)
	effectsCfg := RegisterEffectsFlags(fs)

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, `swytch redis - Redis-compatible cache server

Usage: swytch redis [options]

Options:
  --port=<num>              TCP port to listen on (default: 6379)
  --bind=<addr>             Bind address (default: 127.0.0.1)
  --unixsocket=<path>       Unix socket path
  --unixsocketperm=<perm>   Unix socket permissions (default: 0700)
  --databases=<num>         Number of databases (default: 16)
  --maxmemory=<size>        Max memory, e.g., 256mb, 4gb (default: 64mb)
  --requirepass=<pass>      Require password for AUTH (deprecated, use --aclfile)
  --aclfile=<path>          Path to ACL file for user authentication
  --maxclients=<num>        Max concurrent clients (0 = unlimited)
  --timeout=<sec>           Client idle timeout (0 = disabled)
  --threads=<num>           Number of threads (0 = use all CPUs)
  -h                        Show this help
  --version                 Show version
  -v                        Verbose output
  --debug                   Enable debug logging (shows all commands)
  --log-format=<fmt>        Log output format: text or json (default: text)

TLS options:
  --tls-cert-file=<path>    Path to TLS certificate file (PEM format)
  --tls-key-file=<path>     Path to TLS private key file (PEM format)
  --tls-ca-cert-file=<path> Path to CA certificate for client verification (mTLS)
  --tls-min-version=<ver>   Minimum TLS version: 1.0, 1.1, 1.2, 1.3 (default: 1.2)

Telemetry:
  --no-telemetry            Disable anonymous usage telemetry
  --invite-me=<email>       Register as an early adopter (overrides --no-telemetry)

Swytch extensions:
  --pprof=<addr>            Enable pprof profiling on this address (e.g., localhost:6060)
  --metrics-port=<port>     Enable Prometheus metrics on this port (e.g., 9090)
%s
Examples:
  swytch redis                                     # Start with defaults
  swytch redis --port=6380 --maxmemory=256mb       # 256MB on port 6380
  swytch redis --maxmemory=4gb                     # 4GB memory limit
  swytch redis --bind=0.0.0.0 --requirepass=secret    # Bind all with auth
  swytch redis --tls-cert-file=server.crt --tls-key-file=server.key  # TLS mode
  swytch redis --tls-cert-file=server.crt --tls-key-file=server.key --tls-ca-cert-file=ca.crt  # mTLS mode
`, EffectsUsage())
	}

	// Parse flags
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing flags: %v\n", err)
		os.Exit(1)
	}

	if *showHelp {
		fs.Usage()
		return
	}

	if *showVersion {
		fmt.Printf("swytch redis %s\n", Version)
		return
	}

	telemetryEnabled := !*noTelemetry || *inviteMe != ""
	telemetry.PrintBanner(telemetryEnabled, *inviteMe)

	// Configure structured logging
	var logLevel slog.Level
	if *debug {
		logLevel = slog.LevelDebug
	} else if *verbose {
		logLevel = slog.LevelDebug
	} else {
		logLevel = slog.LevelInfo
	}

	var logHandler slog.Handler
	opts := &slog.HandlerOptions{Level: logLevel}
	if *logFormat == "json" {
		logHandler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		logHandler = slog.NewTextHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(tracing.NewTracingHandler(logHandler)))

	// Set GOMAXPROCS based on threads
	if *threads > 0 {
		runtime.GOMAXPROCS(*threads)
	}

	// Parse maxmemory. Absolute returns bytes; percent returns a
	// fraction the engine enforces dynamically via cloxcache's live
	// target.
	maxMemory, maxMemoryPct, err := beacon.ParseMemoryLimit(*maxMemoryStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid --maxmemory value: %v\n", err)
		os.Exit(1)
	}

	// Validate TLS options
	if (*tlsCert != "" && *tlsKey == "") || (*tlsCert == "" && *tlsKey != "") {
		fmt.Fprintf(os.Stderr, "Error: --tls-cert-file and --tls-key-file must be specified together\n")
		os.Exit(1)
	}
	if *tlsCA != "" && *tlsCert == "" {
		fmt.Fprintf(os.Stderr, "Error: --tls-ca-cert-file requires --tls-cert-file and --tls-key-file\n")
		os.Exit(1)
	}
	if *tlsCert != "" {
		if _, err := os.Stat(*tlsCert); os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "Error: TLS certificate file not found: %s\n", *tlsCert)
			os.Exit(1)
		}
		if _, err := os.Stat(*tlsKey); os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "Error: TLS key file not found: %s\n", *tlsKey)
			os.Exit(1)
		}
	}
	if *tlsCA != "" {
		if _, err := os.Stat(*tlsCA); os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "Error: TLS CA certificate file not found: %s\n", *tlsCA)
			os.Exit(1)
		}
	}

	// Build address
	address := fmt.Sprintf("%s:%d", *bind, *port)

	// Calculate capacity per database
	// Rough estimate: 256 bytes per item average
	capacityPerDB := max(int(maxMemory/256/int64(*databases)), 1000)

	// Create server config
	config := ServerConfig{
		Address:        address,
		UnixSocket:     *unixSocket,
		UnixSocketMode: os.FileMode(*unixSocketPerm),
		NumDatabases:   *databases,
		CapacityPerDB:  capacityPerDB,
		MemoryLimit:    maxMemory,
		Password:       *requirePass,
		ACLFile:        *aclFile,
		MaxConnections: *maxClients,
		DebugLogging:   *debug,
		TLSCertFile:    *tlsCert,
		TLSKeyFile:     *tlsKey,
		TLSCAFile:      *tlsCA,
		TLSMinVersion:  *tlsMinVersion,
	}

	if *timeout > 0 {
		config.ReadTimeout = 0 // Redis doesn't have read timeout
	}

	// Create server
	server, err := NewServer(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create server: %v\n", err)
		os.Exit(1)
	}

	// Initialize effects log if configured (effects builds only)
	effectsCfg.MemoryLimit = maxMemory
	effectsCfg.MemoryLimitPercent = maxMemoryPct
	effectsCfg.Port = *port
	if err := InitializeEffects(effectsCfg); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize effects log: %v\n", err)
		os.Exit(1)
	}

	// Initialize OpenTelemetry tracing
	tracingShutdown := tracing.Init(context.Background(), tracing.Config{
		Endpoint:    *otelEndpoint,
		ServiceName: "swytch",
		NodeID:      uint64(shared.GetNodeID()),
		Insecure:    *otelInsecure,
	})
	defer tracingShutdown(context.Background())

	// Initialize cluster replication if configured (effects builds only)
	if err := InitializeCluster(effectsCfg, server.Handler()); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize cluster: %v\n", err)
		os.Exit(1)
	}

	// Start license heartbeat if license is provided
	// Metrics adapter is always created (lightweight wrapper); metrics
	// server only starts when --metrics-port is set.
	adapter := NewMetricsAdapter(server.Stats(), server.Handler(), maxMemory)
	var metricsServer *metrics.Server
	if *metricsPort > 0 {
		metricsServer = metrics.NewServer(adapter, *metricsPort)
		metricsServer.StartAsync()
		slog.Info("Prometheus metrics server started", "port", *metricsPort, "path", "/metrics")
	}

	// Start telemetry client
	var telemetryCancel context.CancelFunc
	var telemetryDone chan struct{}
	if telemetryEnabled && activeRuntime != nil {
		telCtx, cancel := context.WithCancel(context.Background())
		telemetryCancel = cancel
		nodeID := fmt.Sprintf("%x", uint64(activeRuntime.Engine.NodeID()))
		var clusterID string
		if effectsCfg.JoinAddr != "" {
			clusterID = fmt.Sprintf("%x", xxh3.HashString(effectsCfg.JoinAddr))
		}
		tc := telemetry.New(telemetry.Config{
			NodeID:       nodeID,
			ClusterID:    clusterID,
			Version:      Version,
			Features:     "redis",
			AdopterEmail: *inviteMe,
			StatsFunc: func() telemetry.HeartbeatStats {
				nodes := 1
				if activeRuntime != nil && activeRuntime.PeerManager != nil {
					nodes = len(activeRuntime.PeerManager.PeerIDs()) + 1
				}
				ec := activeRuntime.Engine.EffectCache()
				hits, misses, evictions := ec.Stats()
				return telemetry.HeartbeatStats{
					Nodes:         nodes,
					UptimeSeconds: int(adapter.UptimeSeconds()),
					MemoryAvail:   ec.MemoryLimit(),
					MemoryUsage:   ec.Bytes(),
					HitCount:      hits,
					MissCount:     misses,
					Evictions:     evictions,
					EntryCount:    ec.EntryCount(),
					Capacity:      ec.Capacity(),
					AverageK:      ec.AverageK(),
				}
			},
		})
		telemetryDone = make(chan struct{})
		go func() { defer close(telemetryDone); tc.Run(telCtx) }()
	}

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		slog.Info("Shutting down")
		if telemetryCancel != nil {
			telemetryCancel()
			<-telemetryDone
		}
		if metricsServer != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := metricsServer.Stop(ctx); err != nil {
				slog.Error("Error stopping metrics server", "error", err)
			}
		}
		StopCluster()
		if err := server.Stop(); err != nil {
			slog.Error("Error during shutdown", "error", err)
		}
	}()

	// Start pprof server if enabled
	if *pprofAddr != "" {
		go func() {
			slog.Info("pprof server started", "addr", *pprofAddr, "path", "/debug/pprof/")
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				slog.Error("pprof server error", "error", err)
			}
		}()
	}

	// Print startup banner
	fmt.Printf("Swytch Redis Server %s starting\n", Version)
	fmt.Printf("  Port: %d\n", *port)
	fmt.Printf("  Bind: %s\n", *bind)
	if *unixSocket != "" {
		fmt.Printf("  Unix Socket: %s\n", *unixSocket)
	}
	fmt.Printf("  Databases: %d\n", *databases)
	fmt.Printf("  Max Memory: %s\n", formatBytes(uint64(maxMemory)))
	fmt.Printf("  Threads: %d\n", runtime.GOMAXPROCS(0))
	if *requirePass != "" {
		fmt.Printf("  Auth: enabled\n")
	}
	PrintEffectsConfig(effectsCfg)
	if *tlsCert != "" {
		fmt.Printf("  TLS: enabled\n")
		if *tlsCA != "" {
			fmt.Printf("  mTLS: enabled (client certs required)\n")
		}
	}
	if *pprofAddr != "" {
		fmt.Printf("  Pprof: http://%s/debug/pprof/\n", *pprofAddr)
	}
	if *metricsPort > 0 {
		fmt.Printf("  Metrics: http://localhost:%d/metrics\n", *metricsPort)
	}
	if *debug {
		fmt.Printf("  Debug Logging: enabled\n")
	}
	fmt.Println()

	// Start server
	if err := server.ListenAndServe(); err != nil {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}

// Run starts the Redis server with the given arguments
func Run(args []string) error {
	Main(args)
	return nil
}
