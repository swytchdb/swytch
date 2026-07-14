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

package caddy

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"

	"github.com/swytchdb/swytch/beacon"
	"github.com/swytchdb/swytch/effects"
)

// newTestStore stands up a single-node engine and a swytchStore wired to
// it. No Caddy module lifecycle, no cluster port bind — the engine runs
// in-process. Returns a cleanup that releases the runtime singleton.
func newTestStore(t *testing.T, lockTTL time.Duration) (*swytchStore, func()) {
	t.Helper()
	resetRuntimeForTest(t)
	eng := effects.NewTestEngine()
	installTestRuntime(eng, defaultKeyPrefix)
	rt := singletonRuntime()
	store := &swytchStore{
		runtime:    rt.rt,
		waiters:    rt.waiters,
		dataPrefix: defaultKeyPrefix + "d:",
		lockPrefix: defaultKeyPrefix + "l:",
		lockTTL:    lockTTL,
	}
	return store, func() {
		// Stop refresh goroutines so they don't outlive the engine.
		store.heldMu.Lock()
		for k, stop := range store.heldLocks {
			close(stop)
			delete(store.heldLocks, k)
		}
		store.heldMu.Unlock()
		resetRuntimeForTest(t)
	}
}

func TestStorage_RoundTrip(t *testing.T) {
	store, cleanup := newTestStore(t, 30*time.Second)
	defer cleanup()
	ctx := context.Background()

	if err := store.Store(ctx, "certs/example.com/cert.pem", []byte("cert-bytes")); err != nil {
		t.Fatalf("Store: %v", err)
	}

	got, err := store.Load(ctx, "certs/example.com/cert.pem")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(got) != "cert-bytes" {
		t.Errorf("Load: got %q, want %q", got, "cert-bytes")
	}

	if !store.Exists(ctx, "certs/example.com/cert.pem") {
		t.Errorf("Exists: expected true for stored key")
	}
	if store.Exists(ctx, "certs/missing.com/cert.pem") {
		t.Errorf("Exists: expected false for missing key")
	}

	info, err := store.Stat(ctx, "certs/example.com/cert.pem")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Key != "certs/example.com/cert.pem" {
		t.Errorf("Stat.Key: got %q", info.Key)
	}
	if info.Size != int64(len("cert-bytes")) {
		t.Errorf("Stat.Size: got %d, want %d", info.Size, len("cert-bytes"))
	}
	if !info.IsTerminal {
		t.Errorf("Stat.IsTerminal: expected true")
	}

	if err := store.Delete(ctx, "certs/example.com/cert.pem"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Load(ctx, "certs/example.com/cert.pem"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("Load after Delete: want fs.ErrNotExist, got %v", err)
	}
}

func TestStorage_List(t *testing.T) {
	store, cleanup := newTestStore(t, 30*time.Second)
	defer cleanup()
	ctx := context.Background()

	keys := []string{
		"certs/example.com/cert.pem",
		"certs/example.com/key.pem",
		"certs/other.com/cert.pem",
		"accounts/le/info.json",
	}
	for _, k := range keys {
		if err := store.Store(ctx, k, []byte("x")); err != nil {
			t.Fatalf("Store %q: %v", k, err)
		}
	}

	recursive, err := store.List(ctx, "certs", true)
	if err != nil {
		t.Fatalf("List recursive: %v", err)
	}
	sort.Strings(recursive)
	want := []string{
		"certs/example.com/cert.pem",
		"certs/example.com/key.pem",
		"certs/other.com/cert.pem",
	}
	if !slices.Equal(recursive, want) {
		t.Errorf("List recursive: got %v want %v", recursive, want)
	}

	nonRec, err := store.List(ctx, "certs", false)
	if err != nil {
		t.Fatalf("List non-recursive: %v", err)
	}
	sort.Strings(nonRec)
	wantNon := []string{"certs/example.com", "certs/other.com"}
	if !slices.Equal(nonRec, wantNon) {
		t.Errorf("List non-recursive: got %v want %v", nonRec, wantNon)
	}
}

func TestLock_SingleNode_AcquireRelease(t *testing.T) {
	store, cleanup := newTestStore(t, 30*time.Second)
	defer cleanup()
	ctx := context.Background()

	if err := store.Lock(ctx, "issue-example.com"); err != nil {
		t.Fatalf("first Lock: %v", err)
	}

	// Second Lock from the same process must block until Unlock.
	acquired := make(chan struct{})
	go func() {
		if err := store.Lock(ctx, "issue-example.com"); err != nil {
			t.Errorf("second Lock: %v", err)
			return
		}
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatalf("second Lock returned before Unlock")
	case <-time.After(150 * time.Millisecond):
	}

	if err := store.Unlock(ctx, "issue-example.com"); err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	select {
	case <-acquired:
	case <-time.After(2 * time.Second):
		t.Fatalf("second Lock did not unblock after Unlock")
	}

	if err := store.Unlock(ctx, "issue-example.com"); err != nil {
		t.Fatalf("final Unlock: %v", err)
	}
}

func TestTryLock(t *testing.T) {
	store, cleanup := newTestStore(t, 30*time.Second)
	defer cleanup()
	ctx := context.Background()

	got, err := store.TryLock(ctx, "free")
	if err != nil {
		t.Fatalf("TryLock free: %v", err)
	}
	if !got {
		t.Fatalf("TryLock on a free lock: got false, want true")
	}

	got, err = store.TryLock(ctx, "free")
	if err != nil {
		t.Fatalf("TryLock held: %v", err)
	}
	if got {
		t.Fatalf("TryLock on a held lock: got true, want false")
	}

	if err := store.Unlock(ctx, "free"); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
}

func TestRenewLockLease(t *testing.T) {
	// Short base TTL so we can observe an extension.
	store, cleanup := newTestStore(t, 500*time.Millisecond)
	defer cleanup()
	ctx := context.Background()

	if err := store.Lock(ctx, "long-issue"); err != nil {
		t.Fatalf("Lock: %v", err)
	}

	// Extend by 2s — far past the base TTL.
	if err := store.RenewLockLease(ctx, "long-issue", 2*time.Second); err != nil {
		t.Fatalf("RenewLockLease: %v", err)
	}

	// Read the lock back and confirm ExpiresAt is at least 1.5s out.
	snap, _, err := store.engine().NewContext().GetSnapshot(store.lockKey("long-issue"))
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if snap == nil || snap.ExpiresAt == nil {
		t.Fatalf("expected held lock with ExpiresAt")
	}
	if remaining := time.Until(snap.ExpiresAt.AsTime()); remaining < 1500*time.Millisecond {
		t.Errorf("RenewLockLease didn't extend: remaining=%s, want >= 1.5s", remaining)
	}

	if err := store.Unlock(ctx, "long-issue"); err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	// After Unlock, renewing should report lock lost.
	err = store.RenewLockLease(ctx, "long-issue", time.Second)
	if !errors.Is(err, errLockLost) {
		t.Errorf("RenewLockLease on released lock: got %v, want errLockLost", err)
	}
}

// TestLock_RefreshStopsOnCtxCancel verifies that cancelling the caller's
// Lock context shuts down the refresh goroutine even without an Unlock
// call. Without this, a Caddy reload that abandons a held lock would
// leak the refresh goroutine and keep extending ExpiresAt forever.
func TestLock_RefreshStopsOnCtxCancel(t *testing.T) {
	ttl := 300 * time.Millisecond
	store, cleanup := newTestStore(t, ttl)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	if err := store.Lock(ctx, "abandoned"); err != nil {
		t.Fatalf("Lock: %v", err)
	}

	// Cancel the context — refresh goroutine should exit and stop
	// extending ExpiresAt.
	cancel()

	// Give the refresh loop a tick to observe the cancel.
	time.Sleep(50 * time.Millisecond)

	// heldLocks should no longer track this key.
	store.heldMu.Lock()
	_, stillTracked := store.heldLocks[store.lockKey("abandoned")]
	store.heldMu.Unlock()
	if stillTracked {
		t.Errorf("heldLocks still tracks lock after ctx cancel")
	}

	// Another acquirer must succeed within ttl + slack — refresh is dead.
	start := time.Now()
	acquired := make(chan error, 1)
	go func() {
		acquired <- store.Lock(context.Background(), "abandoned")
	}()
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("re-Lock: %v", err)
		}
		if elapsed := time.Since(start); elapsed > 2*ttl {
			t.Errorf("re-Lock took %s; refresh may still be running", elapsed)
		}
	case <-time.After(ttl + 2*time.Second):
		t.Fatalf("re-Lock did not acquire — refresh goroutine still extending lease")
	}
	if err := store.Unlock(context.Background(), "abandoned"); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
}

func TestLock_SingleNode_TTLSteal(t *testing.T) {
	// Short TTL so the test runs quickly.
	ttl := 300 * time.Millisecond
	store, cleanup := newTestStore(t, ttl)
	defer cleanup()
	ctx := context.Background()

	if err := store.Lock(ctx, "stuck"); err != nil {
		t.Fatalf("first Lock: %v", err)
	}

	// Kill the refresh goroutine — simulates a node going away with
	// the lock held. We do NOT call Unlock.
	store.stopRefresh(store.lockKey("stuck"))

	// A fresh acquire should succeed within ttl + slack.
	start := time.Now()
	acquired := make(chan error, 1)
	go func() {
		acquired <- store.Lock(ctx, "stuck")
	}()

	deadline := time.After(ttl + 2*time.Second)
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("Lock after TTL: %v", err)
		}
		if elapsed := time.Since(start); elapsed < ttl/2 {
			t.Errorf("Lock returned too fast (%s) — expected to wait near %s", elapsed, ttl)
		}
	case <-deadline:
		t.Fatalf("Lock did not acquire after TTL+slack")
	}

	if err := store.Unlock(ctx, "stuck"); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
}

// Compile-time sanity: the test store satisfies certmagic.Storage.
var _ certmagic.Storage = (*swytchStore)(nil)

// --- Caddyfile parser -----------------------------------------------------

func parseCaddyfile(t *testing.T, body string) (*SwytchStorage, error) {
	t.Helper()
	d := caddyfile.NewTestDispenser(body)
	s := &SwytchStorage{}
	if err := s.UnmarshalCaddyfile(d); err != nil {
		return nil, err
	}
	return s, nil
}

func TestCaddyfile_AllOptions(t *testing.T) {
	s, err := parseCaddyfile(t, `swytch {
		cluster_passphrase secret-123
		join cluster.example.com
		cluster_port 9999
		cluster_advertise 10.0.0.1:9999
		key_prefix __caddy:custom:
		lock_ttl 45s
	}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if s.ClusterPassphrase != "secret-123" {
		t.Errorf("ClusterPassphrase: got %q", s.ClusterPassphrase)
	}
	if s.Join != "cluster.example.com" {
		t.Errorf("Join: got %q", s.Join)
	}
	if s.ClusterPort != 9999 {
		t.Errorf("ClusterPort: got %d", s.ClusterPort)
	}
	if s.ClusterAdvertise != "10.0.0.1:9999" {
		t.Errorf("ClusterAdvertise: got %q", s.ClusterAdvertise)
	}
	if s.KeyPrefix != "__caddy:custom:" {
		t.Errorf("KeyPrefix: got %q", s.KeyPrefix)
	}
	if time.Duration(s.LockTTL) != 45*time.Second {
		t.Errorf("LockTTL: got %s", time.Duration(s.LockTTL))
	}
}

func TestCaddyfile_ConnectionSecret(t *testing.T) {
	s, err := parseCaddyfile(t, `swytch {
		connection_secret cloud-secret-xyz
		cluster_port 9999
	}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if s.ConnectionSecret != "cloud-secret-xyz" {
		t.Errorf("ConnectionSecret: got %q", s.ConnectionSecret)
	}
	if s.ClusterPassphrase != "" {
		t.Errorf("ClusterPassphrase should be empty in cloud mode, got %q", s.ClusterPassphrase)
	}
}

func TestCaddyfile_Empty(t *testing.T) {
	s, err := parseCaddyfile(t, `swytch {
	}`)
	if err != nil {
		t.Fatalf("empty block: %v", err)
	}
	if s.ClusterPassphrase != "" || s.ClusterPort != 0 || s.KeyPrefix != "" {
		t.Errorf("empty block produced non-zero fields: %+v", s)
	}
}

func TestCaddyfile_UnknownOption(t *testing.T) {
	_, err := parseCaddyfile(t, `swytch {
		bogus_field foo
	}`)
	if err == nil {
		t.Fatalf("expected error for unknown option, got nil")
	}
	if !strings.Contains(err.Error(), "bogus_field") {
		t.Errorf("error should name the bad option, got %q", err.Error())
	}
}

func TestCaddyfile_InvalidPort(t *testing.T) {
	_, err := parseCaddyfile(t, `swytch {
		cluster_port not-a-number
	}`)
	if err == nil {
		t.Fatalf("expected error for non-numeric port")
	}
}

func TestCaddyfile_InvalidLockTTL(t *testing.T) {
	_, err := parseCaddyfile(t, `swytch {
		lock_ttl forever
	}`)
	if err == nil {
		t.Fatalf("expected error for malformed duration")
	}
}

// --- Singleton lifecycle --------------------------------------------------

// TestClaimRuntime_RefcountAndIncompatibility exercises claimRuntime /
// releaseRuntime without standing up a real beacon (which would bind a
// cluster port). It uses a fake sharedRuntime directly because
// beacon.NewRuntime requires either ClusterPassphrase="" (single-node,
// no listener) — which we use here — or a real port bind.
func TestClaimRuntime_RefcountAndIncompatibility(t *testing.T) {
	resetRuntimeForTest(t)
	defer resetRuntimeForTest(t)

	base := beacon.RuntimeConfig{} // single-node, no listener
	if err := claimRuntime(base, "__caddy:"); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if singletonRuntime().refs != 1 {
		t.Fatalf("refs after first claim: got %d, want 1", singletonRuntime().refs)
	}

	// Compatible reclaim bumps the count.
	if err := claimRuntime(base, "__caddy:"); err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if singletonRuntime().refs != 2 {
		t.Fatalf("refs after second claim: got %d, want 2", singletonRuntime().refs)
	}

	// Conflicting key_prefix on reclaim must error without bumping.
	err := claimRuntime(base, "__swytch:other:")
	if err == nil {
		t.Fatalf("expected error for incompatible key_prefix")
	}
	if !strings.Contains(err.Error(), "key_prefix") {
		t.Errorf("error should mention key_prefix, got %q", err.Error())
	}
	if singletonRuntime().refs != 2 {
		t.Fatalf("refs after rejected claim: got %d, want 2", singletonRuntime().refs)
	}

	// Conflicting passphrase on reclaim must error.
	wrong := beacon.RuntimeConfig{ClusterPassphrase: "x"}
	if err := claimRuntime(wrong, "__caddy:"); err == nil {
		t.Fatalf("expected error for incompatible passphrase")
	}

	// Conflicting advertise on reclaim must error too — the QUIC listener
	// is already bound and the TLS cert SAN baked in.
	if err := claimRuntime(beacon.RuntimeConfig{AdvertiseAddr: "10.0.0.1:7380"}, "__caddy:"); err == nil {
		t.Fatalf("expected error for incompatible advertise")
	}

	// Conflicting connection_secret on reclaim must error — it derives the
	// cluster identity and is baked in at NewRuntime time.
	if err := claimRuntime(beacon.RuntimeConfig{ConnectionSecret: "cloud-x"}, "__caddy:"); err == nil {
		t.Fatalf("expected error for incompatible connection_secret")
	}

	// Release back to zero — runtime stops.
	if err := releaseRuntime(); err != nil {
		t.Fatalf("first release: %v", err)
	}
	if singletonRuntime().refs != 1 {
		t.Fatalf("refs after first release: got %d, want 1", singletonRuntime().refs)
	}
	if err := releaseRuntime(); err != nil {
		t.Fatalf("second release: %v", err)
	}
	if singletonRuntime() != nil {
		t.Fatalf("runtime should be nil after last release")
	}

	// Extra release on empty is a no-op.
	if err := releaseRuntime(); err != nil {
		t.Errorf("extra release: %v", err)
	}
}

// TestIsConfigValidation locks the subcommand-based detection that makes
// Provision drop into single-node mode under `caddy validate` (no QUIC
// bind, no peer dial). Caddy exposes no in-context validate flag, so this
// argv check is the whole signal — keep it honest.
func TestIsConfigValidation(t *testing.T) {
	saved := os.Args
	t.Cleanup(func() { os.Args = saved })

	cases := []struct {
		name string
		args []string
		want bool
	}{
		{"validate", []string{"caddy", "validate", "--config", "Caddyfile"}, true},
		{"run", []string{"caddy", "run", "--config", "Caddyfile"}, false},
		{"start", []string{"caddy", "start"}, false},
		{"no subcommand", []string{"caddy"}, false},
		{"empty", nil, false},
		// Only the subcommand slot counts — a later "validate" arg (e.g. a
		// path) must not trip it.
		{"validate as later arg", []string{"caddy", "run", "validate"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			os.Args = tc.args
			if got := isConfigValidation(); got != tc.want {
				t.Errorf("isConfigValidation() = %v, want %v", got, tc.want)
			}
		})
	}
}
