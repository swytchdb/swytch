# Swytch

Redis-compatible distributed cache. No leaders, no quorums, no clock sync.

> **Join us on [Discord](https://discord.gg/ZdKsCkBbne)** to discuss the hardest parts of computer science, physics, and — when those get too easy — the weather.

## The short version

Swytch speaks the Redis wire protocol. Your existing client connects, your existing commands run. Strings, hashes, sets, sorted sets, streams, pub/sub, scripting, transactions, ACLs — 463 commands across all the data types you'd expect.

Underneath, it's not Redis. Every mutation produces an *effect* that carries pointers to the effects it observed. The happened-before relation is encoded directly in the data, not inferred from clocks.

That's the whole trick. Replication has no leaders because there's nothing to elect. Merges need no coordination because the dependency graph orders them deterministically. Consistency comes from physics.

## Install

```bash
# macOS / Linux (Homebrew)
brew tap swytchdb/tap
brew install swytch

# macOS / Linux (curl)
curl -fsSL https://getswytch.com/scripts/install.sh | sh

# Windows (PowerShell)
iwr -useb https://getswytch.com/scripts/install.ps1 | iex
```

Or pull the container:

```bash
docker pull ghcr.io/swytchdb/swytch:latest
```

Pinning a version with the curl/iwr installers:

```bash
curl -fsSL https://getswytch.com/scripts/install.sh | SWYTCH_VERSION=v0.1.0 sh
```

```powershell
& ([scriptblock]::Create((iwr -useb https://getswytch.com/scripts/install.ps1))) -Version v0.1.0 -AddToPath
```

Prebuilt binaries, checksums, and cosign signatures for every release are at <https://github.com/swytchdb/swytch/releases>.

## Quick start

```bash
swytch redis
redis-cli -p 6379
```

For sidecar deployments, talk to it over a Unix socket. No TCP overhead, no listener exposed:

```bash
swytch redis --unixsocket /tmp/swytch.sock --bind ""
```

```python
import redis
r = redis.Redis(unix_socket_path="/tmp/swytch.sock")
r.set("hello", "world")
```

### More options

```bash
# Custom port and memory limit
swytch redis --port 6380 --maxmemory 256mb --bind 0.0.0.0

# TLS (client-facing)
swytch redis --tls-cert-file server.crt --tls-key-file server.key

# mTLS (require client certs)
swytch redis --tls-cert-file server.crt --tls-key-file server.key --tls-ca-cert-file ca.crt

# Metrics and tracing
swytch redis --metrics-port 9090 --otel-endpoint localhost:4318
```

`swytch redis -h` for the full list.

### Building from source

```bash
just build              # development
just build-release      # stripped, trimpath
just build-platform linux arm64
just test               # tests
just test-race          # tests with race detector
just proto              # regenerate protobuf definitions
```

## Regional clustering

Start more than one node and they cluster. No leader election, no quorum config, no static peer list. Every node accepts writes. Commutative operations (counter increments, set adds) merge automatically. Conflicting transactions resolve deterministically; every node picks the same winner because every node is running the same function on the same DAG.

### Getting started

Generate a cluster passphrase. This is the only piece of trust shared between nodes:

```bash
swytch gen-passphrase
# YUa6WXJDsloKgx4BQWV2edOiH3U2Ym4O5VLR1jrVvO4
```

Point each node at a DNS name that resolves to the cluster:

```bash
# Node 1
swytch redis --cluster-passphrase "YUa6..." --join my-cache.local

# Node 2
swytch redis --cluster-passphrase "YUa6..." --join my-cache.local
```

That's the setup. Nodes find each other through DNS, form a cluster over QUIC+mTLS, replicate from there. The passphrase derives a shared CA; nodes generate ephemeral leaf certificates on startup. Nothing to distribute, nothing to rotate.

### DNS compatibility

`--join` takes any DNS name. SRV records win if present (host:port per entry); falls back to A/AAAA records combined with `--cluster-port`.

| Environment               | What DNS returns               | How it resolves      |
|---------------------------|--------------------------------|----------------------|
| Consul                    | SRV with host:port per service | SRV                  |
| K8s headless service      | A record per pod IP            | A + cluster port     |
| K8s headless + named port | SRV with pod IP + port         | SRV                  |
| Docker Compose            | A record per container         | A + cluster port     |
| Manual `/etc/hosts`       | A record                       | A + cluster port     |

DNS is bootstrap-only. Once a node has joined, peer discovery is in the effects layer. The only time DNS gets re-resolved is if a node loses every peer and needs to bootstrap again.

### Docker Compose

```yaml
services:
  cache:
    image: swytch
    command: >
      swytch redis
        --bind 0.0.0.0
        --cluster-passphrase ${CLUSTER_PASSPHRASE}
        --join cache
    deploy:
      replicas: 3
```

### Kubernetes

```yaml
apiVersion: v1
kind: Service
metadata:
  name: swytch
spec:
  clusterIP: None  # headless
  selector:
    app: swytch
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: swytch
spec:
  serviceName: swytch
  replicas: 3
  template:
    spec:
      containers:
        - name: swytch
          args:
            - redis
            - --bind=0.0.0.0
            - --cluster-passphrase=$(CLUSTER_PASSPHRASE)
            - --join=swytch
```

### Membership

Membership isn't a separate subsystem. It runs on the same effects engine as everything else: nodes write heartbeat effects to an internal key, crashed nodes expire after 30s, graceful shutdowns drop the entry immediately. Inspect it with any Redis client:

```bash
redis-cli HGETALL __swytch:members
```

## How it works

The rest of this is for engineers who want to understand the model.

### The causal effect log

Lamport's 1978 paper laid out two ways to order events in a distributed system: encode what each operation observed, or use logical clocks to approximate the ordering. The field went with clocks. Forty-some years of increasingly clever clocks since (vector clocks, version vectors, hybrid logical clocks) all variations on inferring causality from timestamps.

Swytch picked the other path.

Each mutation produces an effect: a key identifier, dependency pointers to the prior effects observed, a semantic tag, a node identifier. Effects are immutable and append-only. Each node mints addresses in its own namespace as `(NodeID, Sequence)`, so nothing collides without coordination.

The dependency DAG is the ordering. Effect B depends on A means B happened after A. Two effects sharing the same dependencies observed the same state. None of this is metadata wrapping the real data; it *is* the data.

### Deterministic fork-choice

When transactions race against the same causal base, fork-choice picks a winner deterministically: each commit computes `H = hash(NodeID || HLC)`, lowest H wins. The reason this is valid is straight relativity: events that are spacelike-separated in the causal graph have no true order, so any deterministic function over the DAG is as correct as any other. Hash is just a function we picked because it's symmetric, cheap, and computable from the same inputs everywhere. Same function, same inputs, same answer at every node. A bounded *horizon wait* (Spanner's commit-wait, but with software clocks rather than atomic ones) gives you exactly-once transaction semantics without a vote.

### Light Cone Consistency

From any node:

- **Read-your-writes.** Effects you wrote are immediately visible on the node you wrote them on.
- **Monotonic reads.** Causal time only moves forward.
- **Causal consistency.** If A caused B and you've seen B, you've already seen A.
- **Bounded convergence.** Any effect at node X reaches node Y within `d(X,Y) + e`, where `d` is the network delay between them.
- **Deterministic convergence.** Two nodes observing the same tip set produce bit-identical state.

Convergence time is propagation delay. There's no longer-tail "eventual" window beyond that.

### Safe and holographic modes

**Safe mode** (default): commutative operations continue on both sides of a partition. Transactions block. On reconnect, commutative effects auto-merge; no duplicate commits, no transactions lost.

**Holographic mode** keeps everything writing on both sides — transactions included. Each partition is a complete, internally consistent database. On reconnect, commits that happened on only one side merge through fork-choice; commits that happened on both sides surface as holographic divergence, with both histories preserved in the DAG for application review. Holographic mode is implemented but isn't a self-service flag yet — if your workload needs it, email `holographic@getswytch.com` and we'll talk.

## How it compares

| Property                  | Redis Cluster | Raft / Paxos | Redis Enterprise            | Swytch              |
|---------------------------|---------------|--------------|-----------------------------|---------------------|
| Exactly-once transactions | No            | Yes          | No                          | Yes                 |
| No static leader          | No            | No           | Yes                         | Yes                 |
| No per-write coordination | No            | No           | Yes                         | Yes                 |
| Partition behavior        | Block         | Block        | Continue (commutative only) | Configurable per-key|
| Clock requirements        | None          | Synchronized | Vector / logical            | HLC (software only) |

## Internals

### CloxCache (L0 — RAM)

Lock-free, sharded, adaptive in-memory cache. Items above a learned access-frequency threshold are protected from eviction; the threshold adapts online per-shard. No manual tuning, no `--maxmemory-policy` flag.

### Effects engine

The causal effect log: DAG dependency tracking and horizon wait for transactional commits. Core safety properties verified in TLA+ (`CausalEffectLog.tla`, `ExactlyOnce.tla`).

### Transport

QUIC (RFC 9000) for peer-to-peer. Bidirectional streams for effect replication. mTLS for peer auth, derived from the cluster passphrase via HKDF-SHA256 + Ed25519.

### Observability

Prometheus metrics for connections, per-command counts, cache hits/misses/evictions, memory, latency percentiles (p50/p99), adaptive thresholds, network I/O. OpenTelemetry tracing over OTLP HTTP, with trace context injected into structured logs.

## Configuration reference

### Server flags

| Flag            | Default   | Description                             |
|-----------------|-----------|-----------------------------------------|
| `--port`        | 6379      | TCP port                                |
| `--bind`        | 127.0.0.1 | Bind address                            |
| `--maxmemory`   | 64mb      | Memory limit (b/kb/mb/gb/tb)            |
| `--threads`     | 0         | Worker threads (0 = all CPUs)           |
| `--requirepass` |           | AUTH password (use `--aclfile` instead) |
| `--aclfile`     |           | ACL configuration file                  |
| `--maxclients`  | 0         | Max connections (0 = unlimited)         |
| `--timeout`     | 0         | Client idle timeout in seconds          |
| `--unixsocket`  |           | Unix domain socket path                 |
| `--debug`       | false     | Debug logging                           |
| `--log-format`  | text      | Log format: text or json                |

### Client TLS flags

| Flag                 | Default | Description           |
|----------------------|---------|-----------------------|
| `--tls-cert-file`    |         | TLS certificate (PEM) |
| `--tls-key-file`     |         | TLS private key (PEM) |
| `--tls-ca-cert-file` |         | CA cert for mTLS      |
| `--tls-min-version`  | 1.2     | Minimum TLS version   |

### Cluster flags

| Flag                    | Default      | Description                                        |
|-------------------------|--------------|----------------------------------------------------|
| `--cluster-passphrase`  | *(required)* | Shared passphrase for cluster mTLS                 |
| `--join`                |              | DNS name to resolve for peer discovery             |
| `--cluster-port`        | port + 1000  | QUIC port for cluster traffic                      |
| `--cluster-advertise`   | auto-detect  | Address:port this node advertises to peers         |

### Observability flags

| Flag              | Default | Description                          |
|-------------------|---------|--------------------------------------|
| `--metrics-port`  | 0       | Prometheus metrics port              |
| `--otel-endpoint` |         | OTLP HTTP endpoint                   |
| `--otel-insecure` | false   | Use HTTP instead of HTTPS for OTLP   |
| `--pprof`         |         | pprof address (e.g., localhost:6060) |

### Subcommands

| Command                 | Description                        |
|-------------------------|------------------------------------|
| `swytch redis`          | Start a Redis-compatible server    |
| `swytch gen-passphrase` | Generate a cluster mTLS passphrase |
| `swytch version`        | Show version information           |
| `swytch help`           | Show help                          |

## Research

Forthcoming.

## License

AGPL-3.0. See source file headers. Commercial licensing available; email `hello@getswytch.com`.