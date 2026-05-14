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

// Package caddy provides a Caddy storage module backed by an embedded
// swytch effects-engine node. A pool of Caddy instances configured with
// this module forms a swytch cluster that replicates TLS state
// peer-to-peer and coordinates ACME locks via swytch's transactional
// path.
package caddy

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	caddycore "github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"

	"github.com/swytchdb/swytch/beacon"
	"github.com/swytchdb/swytch/effects"
)

func init() {
	caddycore.RegisterModule(SwytchStorage{})
}

// Subspace markers within a configured key_prefix. Splitting the
// caller-supplied prefix into a data subspace and a lock subspace stops
// a Caddy key like "l:foo" from colliding with the lock namespace.
const (
	dataSubspace = "d:"
	lockSubspace = "l:"
)

// Defaults for Caddyfile parsing.
const (
	defaultClusterPort = 7380
	defaultKeyPrefix   = "__caddy:"
	defaultLockTTL     = 30 * time.Second
)

// SwytchStorage is a pool of Caddy instances configured with this module that becomes a Swytch
// cluster: TLS certificates, ACME account state, and OCSP staples replicate
// peer-to-peer, and ACME issuance locks are coordinated through Swytch's
// serializable transactional path.
//
// No external KV storage required.
//
// Point your Caddy instances at the same `join` DNS name and they'll
// form a cluster. `join` should resolve to reachable peer addresses:
// SRV records are preferred, otherwise A/AAAA records are used with
// `cluster_port`. `cluster_passphrase` must match across every node.
type SwytchStorage struct {
	// ClusterPassphrase enables cluster mode. Empty = single-node, no
	// replication. Must match across every peer.
	ClusterPassphrase string `json:"cluster_passphrase,omitempty"`

	// Join is the DNS name to discover peers.
	// Empty is fine for single-node deployments.
	Join string `json:"join,omitempty"`

	// ClusterPort is the QUIC port for cluster traffic.
	ClusterPort int `json:"cluster_port,omitempty"`

	// ClusterAdvertise is the <host>:<port> this node advertises.
	// Empty triggers auto-detection.
	ClusterAdvertise string `json:"cluster_advertise,omitempty"`

	// KeyPrefix scopes all keys this module reads/writes. Must live
	// under the reserved `__caddy:` namespace so the effect cache's
	// system-key pinning (which is __swytch:-only) does not pin
	// potentially large certificate/lock data and defeat the cache's
	// memory limit. Default `__caddy:`.
	KeyPrefix string `json:"key_prefix,omitempty"`

	// LockTTL is the duration after which a held lock is considered
	// stale and may be stolen by another acquirer.
	LockTTL caddycore.Duration `json:"lock_ttl,omitempty"`
}

// CaddyModule implements caddy.Module.
func (SwytchStorage) CaddyModule() caddycore.ModuleInfo {
	return caddycore.ModuleInfo{
		ID:  "caddy.storage.swytch",
		New: func() caddycore.Module { return new(SwytchStorage) },
	}
}

// CertMagicStorage implements caddy.StorageConverter — returns a
// certmagic.Storage backed by the singleton runtime that Provision
// claimed a reference to.
func (s *SwytchStorage) CertMagicStorage() (certmagic.Storage, error) {
	rt := singletonRuntime()
	if rt == nil {
		return nil, errors.New("swytch storage: not provisioned")
	}
	return &swytchStore{
		runtime:    rt.rt,
		waiters:    rt.waiters,
		dataPrefix: s.KeyPrefix + dataSubspace,
		lockPrefix: s.KeyPrefix + lockSubspace,
		owner:      ownerToken(rt.rt.Engine),
		lockTTL:    time.Duration(s.LockTTL),
	}, nil
}

// Provision is called once per Caddy reload. It claims a reference on
// the process-wide swytch runtime, starting it on the first call.
//
// String fields (passphrase / join / advertise / prefix) are expanded
// through Caddy's Replacer so users can write `{env.SWYTCH_PASS}` or
// `{file./run/secrets/swytch}` in the Caddyfile.
func (s *SwytchStorage) Provision(ctx caddycore.Context) error {
	repl := caddycore.NewReplacer()
	s.ClusterPassphrase = repl.ReplaceAll(s.ClusterPassphrase, "")
	s.Join = repl.ReplaceAll(s.Join, "")
	s.ClusterAdvertise = repl.ReplaceAll(s.ClusterAdvertise, "")
	s.KeyPrefix = repl.ReplaceAll(s.KeyPrefix, "")

	if s.ClusterPort == 0 {
		s.ClusterPort = defaultClusterPort
	}
	if s.KeyPrefix == "" {
		s.KeyPrefix = defaultKeyPrefix
	}
	if !strings.HasPrefix(s.KeyPrefix, "__caddy:") {
		return fmt.Errorf("swytch storage: key_prefix must live under __caddy: namespace, got %q", s.KeyPrefix)
	}
	if s.LockTTL == 0 {
		s.LockTTL = caddycore.Duration(defaultLockTTL)
	}
	if time.Duration(s.LockTTL) < time.Second {
		return fmt.Errorf("swytch storage: lock_ttl must be at least 1s, got %s", time.Duration(s.LockTTL))
	}

	cfg := beacon.RuntimeConfig{
		ClusterPassphrase: s.ClusterPassphrase,
		JoinAddr:          s.Join,
		ClusterPort:       s.ClusterPort,
		AdvertiseAddr:     s.ClusterAdvertise,
		Logger:            ctx.Slogger(),
	}
	if err := claimRuntime(cfg, s.KeyPrefix); err != nil {
		return fmt.Errorf("swytch storage: %w", err)
	}
	return nil
}

// Cleanup releases this module's reference on the runtime.
func (s *SwytchStorage) Cleanup() error {
	return releaseRuntime()
}

// UnmarshalCaddyfile parses the storage block:
//
//	storage swytch {
//	    cluster_passphrase <pass>
//	    join <dns>
//	    cluster_port <num>
//	    cluster_advertise <addr:port>
//	    key_prefix <str>
//	    lock_ttl <duration>
//	}
func (s *SwytchStorage) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	if !d.Next() {
		return d.Err("expected tokens")
	}
	if d.NextArg() {
		return d.ArgErr()
	}
	for d.NextBlock(0) {
		switch d.Val() {
		case "cluster_passphrase":
			if !d.NextArg() {
				return d.ArgErr()
			}
			s.ClusterPassphrase = d.Val()
		case "join":
			if !d.NextArg() {
				return d.ArgErr()
			}
			s.Join = d.Val()
		case "cluster_port":
			if !d.NextArg() {
				return d.ArgErr()
			}
			port, err := strconv.Atoi(d.Val())
			if err != nil {
				return d.Errf("invalid cluster_port %q: %v", d.Val(), err)
			}
			s.ClusterPort = port
		case "cluster_advertise":
			if !d.NextArg() {
				return d.ArgErr()
			}
			s.ClusterAdvertise = d.Val()
		case "key_prefix":
			if !d.NextArg() {
				return d.ArgErr()
			}
			s.KeyPrefix = d.Val()
		case "lock_ttl":
			if !d.NextArg() {
				return d.ArgErr()
			}
			dur, err := caddycore.ParseDuration(d.Val())
			if err != nil {
				return d.Errf("invalid lock_ttl %q: %v", d.Val(), err)
			}
			s.LockTTL = caddycore.Duration(dur)
		default:
			return d.Errf("unrecognized swytch storage option %q", d.Val())
		}
		if d.NextArg() {
			return d.ArgErr()
		}
	}
	return nil
}

// sharedRuntime owns the long-lived engine that survives Caddy reloads.
// Lock-subspace callbacks are wired once at startup so all SwytchStorage
// instances share a single waiters registry.
type sharedRuntime struct {
	rt        *beacon.Runtime
	waiters   *waiters
	cfg       beacon.RuntimeConfig
	keyPrefix string
	refs      int
}

var (
	runtimeMu      sync.Mutex
	currentRuntime *sharedRuntime
)

// singletonRuntime returns the live shared runtime, if any.
func singletonRuntime() *sharedRuntime {
	runtimeMu.Lock()
	defer runtimeMu.Unlock()
	return currentRuntime
}

// claimRuntime starts the runtime if it isn't running, otherwise checks
// that the requested config is compatible and bumps the refcount.
//
// Everything beacon-level is bound at NewRuntime time and has no live
// update path: the QUIC listener takes the port and advertise address,
// the TLS cert SAN bakes in the advertise address, and the cluster
// passphrase / join name configure peer discovery once. So any change
// to those fields across a Caddy reload is rejected — the operator
// must restart the process. The `lock_ttl` field is per-storage (lives
// on swytchStore, not on the shared runtime) and DOES take effect on
// the next reload.
func claimRuntime(cfg beacon.RuntimeConfig, keyPrefix string) error {
	runtimeMu.Lock()
	defer runtimeMu.Unlock()

	if currentRuntime != nil {
		prev := currentRuntime.cfg
		if prev.ClusterPassphrase != cfg.ClusterPassphrase ||
			prev.JoinAddr != cfg.JoinAddr ||
			prev.ClusterPort != cfg.ClusterPort ||
			prev.AdvertiseAddr != cfg.AdvertiseAddr {
			return fmt.Errorf("cluster configuration cannot change without restarting the process " +
				"(passphrase/join/port/advertise differ from running instance)")
		}
		if currentRuntime.keyPrefix != keyPrefix {
			return fmt.Errorf("key_prefix cannot change without restarting the process "+
				"(was %q, now %q — would orphan existing keys)",
				currentRuntime.keyPrefix, keyPrefix)
		}
		currentRuntime.refs++
		return nil
	}

	rt, err := beacon.NewRuntime(cfg)
	if err != nil {
		return fmt.Errorf("start beacon runtime: %w", err)
	}

	sr := &sharedRuntime{
		rt:        rt,
		waiters:   newWaiters(),
		cfg:       cfg,
		keyPrefix: keyPrefix,
		refs:      1,
	}
	wireLockWakeups(rt.Engine, sr)
	currentRuntime = sr
	return nil
}

// releaseRuntime drops a reference and stops the runtime when the last
// reference goes away.
func releaseRuntime() error {
	runtimeMu.Lock()
	defer runtimeMu.Unlock()

	if currentRuntime == nil {
		return nil
	}
	currentRuntime.refs--
	if currentRuntime.refs > 0 {
		return nil
	}
	rt := currentRuntime.rt
	currentRuntime = nil
	return rt.Stop()
}

// wireLockWakeups chains lock-subspace wake-ups onto the engine's
// notification callbacks. Both OnKeyDataAdded and OnKeyDeleted fire for
// local writes (flushNonTx / flushTx) and for replicated effects
// (HandleRemote). Subscription on first GetSnapshot makes remote writes
// reach this node, so cross-cluster lock release is automatic.
//
// The engine guards its callback invocations with nil checks, so a
// replicated effect arriving in the window between NewRuntime returning
// and the assignments below is harmless — and waiters.subscribe can't
// run until Provision completes anyway.
func wireLockWakeups(eng *effects.Engine, sr *sharedRuntime) {
	lockPrefix := sr.keyPrefix + lockSubspace
	wakeIfLock := func(key string) {
		if strings.HasPrefix(key, lockPrefix) {
			sr.waiters.wake(key)
		}
	}
	prevAdded := eng.OnKeyDataAdded
	eng.OnKeyDataAdded = func(key string) {
		if prevAdded != nil {
			prevAdded(key)
		}
		wakeIfLock(key)
	}
	prevDeleted := eng.OnKeyDeleted
	eng.OnKeyDeleted = func(key string) {
		if prevDeleted != nil {
			prevDeleted(key)
		}
		wakeIfLock(key)
	}
}

// Interface guards
var (
	_ caddycore.Module           = (*SwytchStorage)(nil)
	_ caddycore.Provisioner      = (*SwytchStorage)(nil)
	_ caddycore.CleanerUpper     = (*SwytchStorage)(nil)
	_ caddycore.StorageConverter = (*SwytchStorage)(nil)
	_ caddyfile.Unmarshaler      = (*SwytchStorage)(nil)

	// Storage-side optional interfaces.
	_ certmagic.Storage          = (*swytchStore)(nil)
	_ certmagic.TryLocker        = (*swytchStore)(nil)
	_ certmagic.LockLeaseRenewer = (*swytchStore)(nil)
)
