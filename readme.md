# Swytch

Redis-compatible, distributed cache. No leaders, no quorums, no clock sync.

> **Join us on [Discord](https://discord.gg/ZdKsCkBbne)** to discuss the hardest parts of computer science, physics,
> and — when those get too easy — the weather.

## The short version

Swytch speaks the Redis wire protocol. Your existing client connects, your existing commands run. Strings, hashes, sets,
sorted sets, streams, pub/sub, scripting, transactions, ACLs — 463 commands across all the data types you'd expect.

Underneath, it's not Redis. Every mutation produces an *effect* that carries pointers to the effects it observed. The
happened-before relation is encoded directly in the data, not inferred from clocks.

That's the whole trick. Replication has no leaders because there's nothing to elect. Merges need no coordination because
the dependency graph orders them deterministically. Consistency comes from physics.

Learn more:

- [read the docs](https://docs.getswytch.com)
- [check out the engine](https://github.com/swytchdb/engine)
- [read the paper](https://arxiv.org/abs/2605.09114)

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

Prebuilt binaries, checksums, and cosign signatures for every release are
at <https://github.com/swytchdb/swytch/releases>.

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

## License

AGPL-3.0. See source file headers. Commercial licensing available; email `hello@getswytch.com`.