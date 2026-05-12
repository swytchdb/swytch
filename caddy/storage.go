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
	"io/fs"
	"strings"
	"sync"
	"time"

	"github.com/caddyserver/certmagic"

	"github.com/swytchdb/swytch/beacon"
	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
)

// swytchStore is the certmagic.Storage implementation backed by an
// embedded effects engine. One instance per Caddy reload — the engine
// behind it is the long-lived singleton.
type swytchStore struct {
	runtime    *beacon.Runtime
	waiters    *waiters
	dataPrefix string // "<key_prefix>d:" — guards user keys from colliding with locks
	lockPrefix string // "<key_prefix>l:" — lock subspace
	lockTTL    time.Duration

	// heldLocks maps lockKey → stop chan for the per-lock refresh
	// goroutine. Closing the channel signals the goroutine to exit.
	heldMu    sync.Mutex
	heldLocks map[string]chan struct{}
}

func (s *swytchStore) engine() *effects.Engine { return s.runtime.Engine }

func (s *swytchStore) dataKey(k string) string    { return s.dataPrefix + k }
func (s *swytchStore) lockKey(name string) string { return s.lockPrefix + name }

// Store writes value at key. Emits a SCALAR LWW write plus a TypeTag meta
// so a future Stat can distinguish "exists" from "metadata-only".
func (s *swytchStore) Store(ctx context.Context, key string, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c := s.engine().NewContext()
	if err := c.Emit(&pb.Effect{
		Key: []byte(s.dataKey(key)),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: value},
		}},
	}); err != nil {
		return err
	}
	if err := c.Emit(&pb.Effect{
		Key:  []byte(s.dataKey(key)),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_STRING}},
	}); err != nil {
		return err
	}
	return c.Flush()
}

// Load returns the value at key. Returns fs.ErrNotExist when the key
// doesn't exist.
func (s *swytchStore) Load(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	snap, _, err := s.engine().NewContext().GetSnapshot(s.dataKey(key))
	if err != nil {
		return nil, err
	}
	if !scalarExists(snap) {
		return nil, fs.ErrNotExist
	}
	raw, ok := snap.Scalar.Value.(*pb.DataEffect_Raw)
	if !ok || raw == nil {
		return nil, fs.ErrNotExist
	}
	// Copy so callers can't observe later mutation if any caching reuses
	// the underlying slice.
	out := make([]byte, len(raw.Raw))
	copy(out, raw.Raw)
	return out, nil
}

// Delete removes a key. Per certmagic semantics, when the name is a
// directory (prefix of other keys) we should remove all keys with that
// prefix. We enumerate via listAll and emit a REMOVE for each.
//
// The cascade is NOT atomic: each REMOVE is a separate effect on the
// same Context but flushNonTx updates the index per-key and broadcasts
// per-key, so a concurrent reader can observe a half-deleted tree. We
// accept this — certmagic doesn't require atomic recursive delete, and
// wrapping the whole cascade in a transaction would cause aborts under
// concurrent issuance/cleanup pressure.
func (s *swytchStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// First try a direct REMOVE — covers the leaf-file case.
	snap, _, err := s.engine().NewContext().GetSnapshot(s.dataKey(key))
	if err != nil {
		return err
	}
	hadDirect := scalarExists(snap)

	// Enumerate any children under this prefix.
	children := s.listAll(key)

	if !hadDirect && len(children) == 0 {
		return fs.ErrNotExist
	}

	c := s.engine().NewContext()
	if hadDirect {
		if err := emitScalarRemove(c, s.dataKey(key)); err != nil {
			return err
		}
	}
	for _, child := range children {
		if err := emitScalarRemove(c, s.dataKey(child)); err != nil {
			return err
		}
	}
	return c.Flush()
}

// Exists reports whether key exists either as a leaf or as a directory
// prefix.
func (s *swytchStore) Exists(ctx context.Context, key string) bool {
	if err := ctx.Err(); err != nil {
		return false
	}
	snap, _, err := s.engine().NewContext().GetSnapshot(s.dataKey(key))
	if err == nil && scalarExists(snap) {
		return true
	}
	// Directory: any child under "<dataPrefix><key>/" makes it exist.
	prefix := s.dataKey(key) + "/"
	found := false
	s.engine().ScanKeys("", prefix+"*", func(string) bool {
		found = true
		return false
	})
	return found
}

// List enumerates keys under path. When recursive=false, only direct
// children are returned and intermediate paths are surfaced (but not
// transitive descendants).
func (s *swytchStore) List(ctx context.Context, path string, recursive bool) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	all := s.listAll(path)
	if recursive {
		return all, nil
	}
	// Non-recursive: collapse each match to its first segment past
	// "<path>/" and dedup.
	prefix := path
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	seen := make(map[string]struct{}, len(all))
	out := make([]string, 0, len(all))
	for _, k := range all {
		rest := strings.TrimPrefix(k, prefix)
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			rest = rest[:i]
		}
		full := prefix + rest
		if _, ok := seen[full]; ok {
			continue
		}
		seen[full] = struct{}{}
		out = append(out, full)
	}
	return out, nil
}

// Stat returns metadata about key.
func (s *swytchStore) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	if err := ctx.Err(); err != nil {
		return certmagic.KeyInfo{}, err
	}
	dk := s.dataKey(key)
	snap, _, err := s.engine().NewContext().GetSnapshot(dk)
	if err != nil {
		return certmagic.KeyInfo{}, err
	}
	if !scalarExists(snap) {
		// Maybe it's a directory.
		prefix := dk + "/"
		isDir := false
		s.engine().ScanKeys("", prefix+"*", func(string) bool {
			isDir = true
			return false
		})
		if !isDir {
			return certmagic.KeyInfo{}, fs.ErrNotExist
		}
		return certmagic.KeyInfo{Key: key, IsTerminal: false}, nil
	}
	var size int64
	if raw, ok := snap.Scalar.Value.(*pb.DataEffect_Raw); ok && raw != nil {
		size = int64(len(raw.Raw))
	}
	var modified time.Time
	if snap.Hlc != nil {
		modified = snap.Hlc.AsTime()
	}
	return certmagic.KeyInfo{
		Key:        key,
		Modified:   modified,
		Size:       size,
		IsTerminal: true,
	}, nil
}

// listAll returns every leaf key under `path` (Caddy semantics), with
// the `dataPrefix` stripped so callers see Caddy-style keys.
func (s *swytchStore) listAll(path string) []string {
	prefix := s.dataKey(path)
	// For non-empty path, append "/*" so "a" doesn't match "ab"; for
	// empty path we want every key under dataPrefix.
	var scanGlob string
	if path != "" && !strings.HasSuffix(prefix, "/") {
		scanGlob = prefix + "/*"
	} else {
		scanGlob = prefix + "*"
	}
	var out []string
	s.engine().ScanKeys("", scanGlob, func(k string) bool {
		if !strings.HasPrefix(k, s.dataPrefix) {
			return true
		}
		out = append(out, strings.TrimPrefix(k, s.dataPrefix))
		return true
	})
	return out
}

// scalarExists reports whether snap is a live value. Context.GetSnapshot
// has already run filterSnapshot — which drops REMOVE_OP tombstones and
// TTL-expired keys — so a snap that survives with a Scalar payload is
// the real thing.
func scalarExists(snap *pb.ReducedEffect) bool {
	return snap != nil && snap.Scalar != nil
}

func emitScalarRemove(c *effects.Context, fullKey string) error {
	return c.Emit(&pb.Effect{
		Key: []byte(fullKey),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_SCALAR,
		}},
	})
}

// Interface guard.
var _ certmagic.Storage = (*swytchStore)(nil)
