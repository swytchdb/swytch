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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ownerToken serializes the engine's ephemeral NodeID into the 8 bytes
// stored in the lock value. NodeID is regenerated on each process start
// (see cluster/proto.NewNodeID) so it uniquely identifies the live
// engine for the lifetime of the process. Little-endian matches the
// rest of the repo's NodeID encoders. Computed once at swytchStore
// construction and cached on the struct.
func ownerToken(engine *effects.Engine) [8]byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(engine.NodeID()))
	return b
}

// snapIsHeld returns true if snap represents a currently-held lock.
// Context.GetSnapshot already filters out REMOVE_OP tombstones via
// filterSnapshot, so a non-nil snap with a Scalar payload is a live
// lock.
func snapIsHeld(snap *pb.ReducedEffect) bool {
	return snap != nil && snap.Scalar != nil
}

// errLockLost signals that a held lock is no longer ours (TTL stolen,
// owned by another process, or a concurrent write rejected our refresh).
// Returned by renewLease; surfaced through RenewLockLease to certmagic
// and used by refreshOnce to stop the auto-refresh goroutine.
var errLockLost = errors.New("swytch caddy lock: lock lost")

// Lock blocks until the named lock can be acquired. Read the snap
// first; only register a wake subscription if the lock is currently
// held (the subscription closes the lost-wake window between read and
// select). The acquire branch relies on SSI/CheckWatches to catch any
// race rather than on the subscription.
func (s *swytchStore) Lock(ctx context.Context, name string) error {
	lockKey := s.lockKey(name)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		acquired, err := s.attemptAcquire(ctx, lockKey)
		if err != nil {
			return err
		}
		if acquired {
			return nil
		}
		if err := s.waitHeld(ctx, lockKey); err != nil {
			return err
		}
	}
}

// attemptAcquire runs a single non-blocking acquire attempt. Returns
// (true, nil) on success (refresh goroutine started); (false, nil) if
// the lock is held or the tx aborted; or an engine error.
//
// The caller's ctx is passed to startRefresh so that a cancellation
// stops the auto-refresh loop without requiring Unlock — otherwise a
// caller that gets ctx-cancelled without calling Unlock would keep
// extending the lease forever, blocking TTL-based steal recovery and
// leaking a goroutine across Caddy reloads.
func (s *swytchStore) attemptAcquire(ctx context.Context, lockKey string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	cctx := s.engine().NewContext()
	cctx.Watch(lockKey)
	cctx.BeginTx()
	if !cctx.CheckWatches() {
		return false, nil
	}
	snap, _, err := cctx.GetSnapshot(lockKey)
	if err != nil {
		cctx.Abort()
		return false, err
	}
	if snapIsHeld(snap) {
		cctx.Abort()
		return false, nil
	}
	expiresAt := timestamppb.New(time.Now().Add(s.lockTTL))
	if err := emitLockAcquire(cctx, lockKey, s.owner, expiresAt); err != nil {
		cctx.Abort()
		return false, err
	}
	if err := cctx.Flush(); err != nil {
		if errors.Is(err, effects.ErrTxnAborted) {
			return false, nil
		}
		return false, err
	}
	s.startRefresh(ctx, lockKey)
	return true, nil
}

// waitHeld subscribes to wake events on a held lock, reads the
// snapshot to learn the lock's TTL, then waits for either a release
// (subscription closed) or the TTL deadline. Subscribing BEFORE the
// snapshot read closes the lost-wake window: any release between the
// read and the select fires the channel.
//
// Returns ctx.Err() on cancel; nil otherwise (caller retries). If the
// snapshot turns out to no longer be held by the time we read it, we
// return nil immediately so the caller tries the acquire path.
func (s *swytchStore) waitHeld(ctx context.Context, lockKey string) error {
	ch := s.waiters.subscribe(lockKey)
	defer s.waiters.unsubscribe(lockKey, ch)

	snap, _, err := s.engine().NewContext().GetSnapshot(lockKey)
	if err != nil {
		return err
	}
	if !snapIsHeld(snap) {
		// Released between attemptAcquire and now — retry immediately.
		return nil
	}
	// A held lock always carries an ExpiresAt (acquire emits one).
	// Missing ExpiresAt indicates upstream corruption — log and fall
	// back to a short retry so we don't loop hot.
	var deadline time.Time
	if snap.ExpiresAt != nil {
		deadline = snap.ExpiresAt.AsTime()
	} else {
		slog.Debug("swytch caddy lock: held lock missing ExpiresAt", "key", lockKey)
		deadline = time.Now().Add(time.Second)
	}
	wait := time.Until(deadline)
	if wait < 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ch:
		return nil
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TryLock implements certmagic.TryLocker — a single non-blocking
// acquire. certmagic uses this to skip optional maintenance when the
// lock is contended, instead of serialising on the blocking Lock.
func (s *swytchStore) TryLock(ctx context.Context, name string) (bool, error) {
	return s.attemptAcquire(ctx, s.lockKey(name))
}

// RenewLockLease implements certmagic.LockLeaseRenewer. certmagic calls
// this between ACME retries to extend a held lock past the next
// expected wait, so the refresh-goroutine cadence (lockTTL/3) doesn't
// have to be the upper bound on a single retry window.
//
// Returns nil on success, errLockLost if the lock is no longer ours
// (TTL stolen, owner mismatch, or a concurrent write preempted us).
func (s *swytchStore) RenewLockLease(ctx context.Context, name string, leaseDuration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.renewLease(s.lockKey(name), leaseDuration)
}

// Unlock releases the named lock if (and only if) this process still
// owns it. Stale ownership (TTL stole the lock) is a silent no-op.
func (s *swytchStore) Unlock(_ context.Context, name string) error {
	lockKey := s.lockKey(name)

	// Stop the refresh goroutine first so it can't race against our
	// REMOVE with a fresh ExpiresAt emit.
	s.stopRefresh(lockKey)

	cctx := s.engine().NewContext()
	cctx.Watch(lockKey)
	cctx.BeginTx()
	if !cctx.CheckWatches() {
		// Watched key was modified between Watch and now — somebody
		// else either refreshed or stole. Either way, we don't own it
		// anymore. Drop without emitting.
		cctx.Abort()
		return nil
	}
	snap, _, err := cctx.GetSnapshot(lockKey)
	if err != nil {
		cctx.Abort()
		return err
	}
	if !snapIsHeld(snap) || !ownerMatches(snap, s.owner) {
		cctx.Abort()
		return nil
	}
	if err := emitScalarRemove(cctx, lockKey); err != nil {
		cctx.Abort()
		return err
	}
	if err := cctx.Flush(); err != nil && !errors.Is(err, effects.ErrTxnAborted) {
		return err
	}
	return nil
}

// startRefresh launches a goroutine that re-emits the ExpiresAt meta at
// lockTTL/3 cadence so a long-running ACME issuance doesn't lose the
// lock to a steal. The goroutine exits on any of:
//   - explicit Unlock (stopRefresh closes the stop chan)
//   - the caller's Lock ctx being cancelled (the work was abandoned;
//     let TTL take over so other peers can recover)
//   - the lock being lost mid-refresh (owner mismatch or snapshot gone)
func (s *swytchStore) startRefresh(lockCtx context.Context, lockKey string) {
	stop := make(chan struct{})
	s.heldMu.Lock()
	if s.heldLocks == nil {
		s.heldLocks = make(map[string]chan struct{})
	}
	// If something is already tracked (shouldn't be — Lock returned
	// success), shut the old refresher down to avoid two goroutines on
	// the same lock.
	if prev, ok := s.heldLocks[lockKey]; ok {
		close(prev)
	}
	s.heldLocks[lockKey] = stop
	s.heldMu.Unlock()

	go s.refreshLoop(lockCtx, lockKey, stop)
}

// stopRefresh signals the refresh goroutine to exit and removes the
// entry from the held-lock table. Idempotent.
func (s *swytchStore) stopRefresh(lockKey string) {
	s.heldMu.Lock()
	stop, ok := s.heldLocks[lockKey]
	if ok {
		delete(s.heldLocks, lockKey)
	}
	s.heldMu.Unlock()
	if ok {
		close(stop)
	}
}

func (s *swytchStore) refreshLoop(lockCtx context.Context, lockKey string, stop <-chan struct{}) {
	interval := max(s.lockTTL/3, 100*time.Millisecond)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-lockCtx.Done():
			slog.Debug("swytch caddy lock: refresh stopped on ctx cancel", "key", lockKey)
			s.heldMu.Lock()
			delete(s.heldLocks, lockKey)
			s.heldMu.Unlock()
			return
		case <-t.C:
			if !s.refreshOnce(lockKey) {
				s.heldMu.Lock()
				delete(s.heldLocks, lockKey)
				s.heldMu.Unlock()
				return
			}
		}
	}
}

// renewLease emits a new ExpiresAt for the held lock, extending the
// lease by ttl. Returns errLockLost when ownership is gone (snapshot
// missing/foreign, or a concurrent write preempted our watch — only
// the owner should write while we hold the lock, so any concurrent
// write IS a steal). Other errors come from the engine.
func (s *swytchStore) renewLease(lockKey string, ttl time.Duration) error {
	cctx := s.engine().NewContext()
	cctx.Watch(lockKey)
	cctx.BeginTx()
	if !cctx.CheckWatches() {
		cctx.Abort()
		return errLockLost
	}
	snap, _, err := cctx.GetSnapshot(lockKey)
	if err != nil {
		cctx.Abort()
		return err
	}
	if !snapIsHeld(snap) || !ownerMatches(snap, s.owner) {
		cctx.Abort()
		return errLockLost
	}
	expiresAt := timestamppb.New(time.Now().Add(ttl))
	if err := cctx.Emit(&pb.Effect{
		Key:  []byte(lockKey),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{ExpiresAt: expiresAt}},
	}); err != nil {
		cctx.Abort()
		return err
	}
	if err := cctx.Flush(); err != nil {
		// ErrTxnAborted means a concurrent write to the watched key
		// raced our commit — same semantics as the CheckWatches=false
		// branch above. Only the lock owner should write while we hold
		// the lock, so any concurrent write IS a steal: surface it as
		// lock loss rather than silently reporting a successful renew.
		if errors.Is(err, effects.ErrTxnAborted) {
			return errLockLost
		}
		return err
	}
	return nil
}

// refreshOnce extends ExpiresAt by lockTTL on the auto-refresh schedule.
// Returns false when the lock has been lost (caller stops refreshing).
func (s *swytchStore) refreshOnce(lockKey string) bool {
	err := s.renewLease(lockKey, s.lockTTL)
	if errors.Is(err, errLockLost) {
		slog.Warn("swytch caddy lock: lock lost during refresh", "key", lockKey)
		return false
	}
	if err != nil {
		slog.Debug("swytch caddy lock: refresh failed", "key", lockKey, "error", err)
	}
	return true
}

// ownerMatches reports whether snap's scalar payload equals owner.
func ownerMatches(snap *pb.ReducedEffect, owner [8]byte) bool {
	if snap == nil || snap.Scalar == nil {
		return false
	}
	raw, ok := snap.Scalar.Value.(*pb.DataEffect_Raw)
	if !ok || raw == nil {
		return false
	}
	return bytes.Equal(raw.Raw, owner[:])
}

func emitLockAcquire(c *effects.Context, lockKey string, owner [8]byte, expiresAt *timestamppb.Timestamp) error {
	if err := c.Emit(&pb.Effect{
		Key: []byte(lockKey),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: append([]byte(nil), owner[:]...)},
		}},
	}); err != nil {
		return err
	}
	return c.Emit(&pb.Effect{
		Key:  []byte(lockKey),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{ExpiresAt: expiresAt}},
	})
}
