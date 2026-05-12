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

package str

import (
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/redis/shared"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func handleExpire(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("expire")
		return
	}

	key := string(cmd.Args[0])
	seconds, ok := shared.ParseInt64(cmd.Args[1])
	if !ok {
		w.WriteNotInteger()
		return
	}

	// Validate expire time - check for overflow when converting to milliseconds
	// and when adding to current time. Also reject values that are too negative.
	if !validateExpireSecondsRelative(seconds) {
		w.WriteError("ERR invalid expire time in 'expire' command")
		return
	}

	// Parse options
	nx, xx, gt, lt, optErr := parseExpireOptions(cmd.Args, 2)
	if optErr != "" {
		w.WriteError(optErr)
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, tips, err := getAnySnapshotWithTips(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if snap == nil {
			w.WriteZero()
			return
		}

		if seconds < 0 {
			if err := emitDeleteKey(cmd, key, tips); err != nil {
				w.WriteError(err.Error())
				return
			}
			w.WriteOne()
			return
		}

		newExp := time.Now().Add(time.Duration(seconds) * time.Second)
		finalExpire(newExp, snap, nx, xx, gt, lt, w, cmd, key, tips)
	}
	return
}

func finalExpire(newExpireMs time.Time, snap *pb.ReducedEffect, nx bool, xx bool, gt bool, lt bool, w *shared.Writer, cmd *shared.Command, key string, tips []effects.Tip) {
	if !shouldSetExpire(newExpireMs, snap.ExpiresAt, nx, xx, gt, lt) {
		w.WriteZero()
		return
	}

	cmd.Context.BeginTx()
	if err := cmd.Context.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
	}, tips); err != nil {
		w.WriteError(err.Error())
		return
	}
	if err := emitStringTTL(cmd, key, timestamppb.New(newExpireMs)); err != nil {
		w.WriteError(err.Error())
		return
	}
	w.WriteOne()
}

func handleExpireAt(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("expireat")
		return
	}

	key := string(cmd.Args[0])
	timestamp, ok := shared.ParseInt64(cmd.Args[1])
	if !ok {
		w.WriteNotInteger()
		return
	}

	// Validate expire time - check for overflow when converting to milliseconds
	if !validateExpireAtSecondsRelative(timestamp) {
		w.WriteError("ERR invalid expire time in 'expireat' command")
		return
	}

	// Parse options
	nx, xx, gt, lt, optErr := parseExpireOptions(cmd.Args, 2)
	if optErr != "" {
		w.WriteError(optErr)
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, tips, err := getAnySnapshotWithTips(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if snap == nil {
			w.WriteZero()
			return
		}

		if timestamp*1000 <= time.Now().UnixMilli() {
			if err := emitDeleteKey(cmd, key, tips); err != nil {
				w.WriteError(err.Error())
				return
			}
			w.WriteOne()
			return
		}

		newExpireMs := timestamp * 1000
		finalExpire(time.UnixMilli(newExpireMs), snap, nx, xx, gt, lt, w, cmd, key, tips)
	}
	return
}

func handlePExpire(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("pexpire")
		return
	}

	key := string(cmd.Args[0])
	millis, ok := shared.ParseInt64(cmd.Args[1])
	if !ok {
		w.WriteNotInteger()
		return
	}

	// Validate expire time - check for overflow when adding to current time
	if !validateExpireMillisRelative(millis) {
		w.WriteError("ERR invalid expire time in 'pexpire' command")
		return
	}

	// Parse options
	nx, xx, gt, lt, optErr := parseExpireOptions(cmd.Args, 2)
	if optErr != "" {
		w.WriteError(optErr)
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, tips, err := getAnySnapshotWithTips(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if snap == nil {
			w.WriteZero()
			return
		}

		if millis < 0 {
			if err := emitDeleteKey(cmd, key, tips); err != nil {
				w.WriteError(err.Error())
				return
			}
			w.WriteOne()
			return
		}

		newExpireMs := time.Now().UnixMilli() + millis
		finalExpire(time.UnixMilli(newExpireMs), snap, nx, xx, gt, lt, w, cmd, key, tips)
	}
	return
}

func handlePExpireAt(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("pexpireat")
		return
	}

	key := string(cmd.Args[0])
	timestamp, ok := shared.ParseInt64(cmd.Args[1])
	if !ok {
		w.WriteNotInteger()
		return
	}

	// Parse options
	nx, xx, gt, lt, optErr := parseExpireOptions(cmd.Args, 2)
	if optErr != "" {
		w.WriteError(optErr)
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, tips, err := getAnySnapshotWithTips(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if snap == nil {
			w.WriteZero()
			return
		}

		if timestamp <= time.Now().UnixMilli() {
			if err := emitDeleteKey(cmd, key, tips); err != nil {
				w.WriteError(err.Error())
				return
			}
			w.WriteOne()
			return
		}

		newExpireMs := timestamp
		finalExpire(time.UnixMilli(newExpireMs), snap, nx, xx, gt, lt, w, cmd, key, tips)
	}
	return
}

func handleTTL(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("ttl")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true

	runner = func() {
		snap, err := getAnySnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if snap == nil {
			w.WriteInteger(-2)
			return
		}
		if snap.ExpiresAt == nil {
			w.WriteInteger(-1)
			return
		}
		remaining := snap.ExpiresAt.AsTime().Sub(time.Now())
		if remaining <= 0 {
			w.WriteInteger(-2)
			return
		}
		w.WriteInteger(int64(remaining.Seconds()))
	}
	return
}

func handlePTTL(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("pttl")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true

	runner = func() {
		snap, err := getAnySnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if snap == nil {
			w.WriteInteger(-2)
			return
		}
		if snap.ExpiresAt == nil {
			w.WriteInteger(-1)
			return
		}
		remaining := snap.ExpiresAt.AsTime().Sub(time.Now())
		if remaining <= 0 {
			w.WriteInteger(-2)
			return
		}
		w.WriteInteger(remaining.Milliseconds())
	}
	return
}

func handleExpireTime(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("expiretime")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true

	runner = func() {
		snap, err := getAnySnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if snap == nil {
			w.WriteInteger(-2)
			return
		}
		if snap.ExpiresAt == nil {
			w.WriteInteger(-1)
			return
		}
		if snap.ExpiresAt.AsTime().Before(time.Now()) {
			w.WriteInteger(-2)
			return
		}
		w.WriteInteger(snap.ExpiresAt.AsTime().UnixMilli())
	}
	return
}

func handlePExpireTime(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("pexpiretime")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true

	runner = func() {
		snap, err := getAnySnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if snap == nil {
			w.WriteInteger(-2)
			return
		}
		if snap.ExpiresAt == nil {
			w.WriteInteger(-1)
			return
		}
		if snap.ExpiresAt.AsTime().Before(time.Now()) {
			w.WriteInteger(-2)
			return
		}
		w.WriteInteger(snap.ExpiresAt.AsTime().Unix())
	}
	return
}

func handlePersist(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("persist")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true

	runner = func() {
		snap, tips, err := getAnySnapshotWithTips(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if snap == nil {
			w.WriteZero()
			return
		}
		if snap.ExpiresAt == nil {
			w.WriteZero()
			return
		}

		cmd.Context.BeginTx()
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			w.WriteError(err.Error())
			return
		}
		if err := emitStringTTL(cmd, key, nil); err != nil {
			w.WriteError(err.Error())
			return
		}
		w.WriteOne()
	}
	return
}
