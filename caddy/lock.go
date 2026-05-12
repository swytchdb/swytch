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
// rest of the repo's NodeID encoders.
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

// Lock blocks until the named lock can be acquired. The acquire/wait
// loop:
//
//  1. Subscribe to wake events on lockKey BEFORE reading state — closes
//     the lost-wake window between read and select.
//  2. Plain read (no tx open): SSI would abort us at commit if any
//     concurrent write touched a watched key, and the write we're
//     waiting for is precisely that kind of write.
//  3. Held → wait for (release | TTL expiry | ctx cancel) then retry.
//  4. Free → open a tx, Watch + BeginTx + CheckWatches, re-check inside
//     the SSI snapshot to close the read/tx-open race, emit the acquire
//     pair, Flush. ErrTxnAborted → retry.
func (s *swytchStore) Lock(ctx context.Context, name string) error {
	lockKey := s.lockKey(name)
	owner := ownerToken(s.engine())
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		acquired, err := s.tryAcquire(ctx, lockKey, owner)
		if err != nil {
			return err
		}
		if acquired {
			return nil
		}
	}
}

// tryAcquire runs one iteration of the Lock loop. Returns (true, nil)
// once the lock is held and a refresh goroutine is running. (false, nil)
// means "keep retrying"; an error short-circuits the caller.
//
// Wrapping a loop iteration in its own function lets us defer the
// unsubscribe and stop worrying about which early-return path forgot
// to clean up.
func (s *swytchStore) tryAcquire(ctx context.Context, lockKey string, owner [8]byte) (bool, error) {
	ch := s.waiters.subscribe(lockKey)
	defer s.waiters.unsubscribe(lockKey, ch)

	// Context.GetSnapshot applies filterSnapshot, which nullifies
	// expired locks — an expired lock looks free without any sweep
	// job. Raw Engine.GetSnapshot would return the unfiltered tip.
	snap, _, err := s.engine().NewContext().GetSnapshot(lockKey)
	if err != nil {
		return false, err
	}
	if snapIsHeld(snap) {
		return false, s.waitForChange(ctx, lockKey, snap, ch)
	}

	// Believed free — attempt transactional acquire.
	cctx := s.engine().NewContext()
	cctx.Watch(lockKey)
	cctx.BeginTx()
	if !cctx.CheckWatches() {
		return false, nil
	}
	snap, _, err = cctx.GetSnapshot(lockKey)
	if err != nil {
		cctx.Abort()
		return false, err
	}
	if snapIsHeld(snap) {
		cctx.Abort()
		return false, nil
	}
	expiresAt := timestamppb.New(time.Now().Add(s.lockTTL))
	if err := emitLockAcquire(cctx, lockKey, owner, expiresAt); err != nil {
		cctx.Abort()
		return false, err
	}
	if err := cctx.Flush(); err != nil {
		if errors.Is(err, effects.ErrTxnAborted) {
			return false, nil
		}
		return false, err
	}
	s.startRefresh(lockKey, owner)
	return true, nil
}

// waitForChange blocks until the lock is released, its TTL elapses, or
// the caller's context is cancelled. Returns ctx.Err() on cancellation,
// nil otherwise (caller retries).
func (s *swytchStore) waitForChange(ctx context.Context, lockKey string, snap *pb.ReducedEffect, ch <-chan struct{}) error {
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

// Unlock releases the named lock if (and only if) this process still
// owns it. Stale ownership (TTL stole the lock) is a silent no-op.
func (s *swytchStore) Unlock(_ context.Context, name string) error {
	lockKey := s.lockKey(name)
	owner := ownerToken(s.engine())

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
	if !snapIsHeld(snap) || !ownerMatches(snap, owner) {
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
// lock to a steal.
func (s *swytchStore) startRefresh(lockKey string, owner [8]byte) {
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

	go s.refreshLoop(lockKey, owner, stop)
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

func (s *swytchStore) refreshLoop(lockKey string, owner [8]byte, stop <-chan struct{}) {
	interval := max(s.lockTTL/3, 100*time.Millisecond)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			if !s.refreshOnce(lockKey, owner) {
				return
			}
		}
	}
}

// refreshOnce extends ExpiresAt by lockTTL. Returns false when the lock
// has been lost (owner mismatch or snapshot gone) — caller stops refreshing.
func (s *swytchStore) refreshOnce(lockKey string, owner [8]byte) bool {
	cctx := s.engine().NewContext()
	cctx.Watch(lockKey)
	cctx.BeginTx()
	if !cctx.CheckWatches() {
		cctx.Abort()
		return true // a write happened — retry on next tick
	}
	snap, _, err := cctx.GetSnapshot(lockKey)
	if err != nil {
		cctx.Abort()
		slog.Debug("swytch caddy lock: refresh read failed", "key", lockKey, "error", err)
		return true
	}
	if !snapIsHeld(snap) || !ownerMatches(snap, owner) {
		cctx.Abort()
		slog.Warn("swytch caddy lock: lock lost during refresh", "key", lockKey)
		return false
	}
	expiresAt := timestamppb.New(time.Now().Add(s.lockTTL))
	if err := cctx.Emit(&pb.Effect{
		Key:  []byte(lockKey),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{ExpiresAt: expiresAt}},
	}); err != nil {
		cctx.Abort()
		slog.Debug("swytch caddy lock: refresh emit failed", "key", lockKey, "error", err)
		return true
	}
	if err := cctx.Flush(); err != nil && !errors.Is(err, effects.ErrTxnAborted) {
		slog.Debug("swytch caddy lock: refresh flush failed", "key", lockKey, "error", err)
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
