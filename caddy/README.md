# Caddy storage module for Swytch

A pool of Caddy instances configured with this module becomes a Swytch
cluster: TLS certificates, ACME account state, and OCSP staples replicate
peer-to-peer, and ACME issuance locks are coordinated through Swytch's
serializable transactional path.

No external KV storage required.

## Build a Caddy binary with this module

The module is published as a separate Go module (`github.com/swytchdb/swytch/caddy`)
to keep its dependency tree off the swytch core. Build with
[xcaddy](https://github.com/caddyserver/xcaddy):

```sh
xcaddy build \
  --with github.com/swytchdb/swytch/caddy
```

That works for downstream users once the module is tagged. While developing
against an unpublished checkout, point xcaddy at both modules with absolute
paths — the relative `replace ../` in `caddy/go.mod` is ignored when caddy
is consumed as a dependency, so the parent module needs an explicit replace:

```sh
xcaddy build \
  --with github.com/swytchdb/swytch/caddy=/abs/path/to/swytch/caddy \
  --replace github.com/swytchdb/swytch=/abs/path/to/swytch
```

(`--with` for the caddy module imports it as a Caddy plugin; `--replace`
on the parent just rewires the dependency without adding an import,
which would fail because the parent module is `package main`.)

## Configure

### Caddyfile

```caddyfile
{
    storage swytch {
        cluster_passphrase <secret>     # empty = single-node (no replication)
        join <dns-name>                 # peers resolve via DNS; optional
        cluster_port <num>              # QUIC port; default 7380/UDP
        cluster_advertise <addr:port>   # this node's reachable address; auto-detect if empty
        key_prefix __caddy:             # default; must live under __caddy:
        lock_ttl 30s                    # ACME issuance lock TTL; default 30s
    }
}

:80 {
    # ... your site config
}
```

Point your Caddy instances at the same `join` DNS name (which
must resolve to at least one peer's `cluster_advertise` address), and they'll
form a cluster. `cluster_passphrase` must match across every node.

### JSON

The struct mapping follows Caddy's defaults — the field names below are
JSON-encoded equivalents of the Caddyfile keywords:

```json
{
  "storage": {
    "module": "swytch",
    "cluster_passphrase": "...",
    "join": "...",
    "cluster_port": 7380,
    "cluster_advertise": "10.0.0.1:7380",
    "key_prefix": "__caddy:",
    "lock_ttl": "30s"
  }
}
```

## Lifecycle notes

- The embedded engine is a process-wide singleton. Caddy reloads reuse it;
  changing `cluster_passphrase`, `join`, `cluster_port`, `cluster_advertise`,
  or `key_prefix` at reload time is rejected — these are all baked into
  the QUIC listener, TLS cert SAN, or peer-discovery state at startup and
  there is no live-update path. Restart the process instead.
- `lock_ttl` is per-storage and DOES take effect on the next reload.
- Single-node mode (`cluster_passphrase` empty) is supported and useful
  for local development. No QUIC port is bound, no peer discovery runs.
