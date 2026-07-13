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
	"bytes"
	"encoding/hex"
	"math"
	"strconv"
	"time"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/redis/shared"
	"github.com/zeebo/xxh3"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// getStringSnapshot returns the snapshot for a string key.
// For SCALAR+TYPE_STRING: returns directly.
// For KEYED types with a registered BytesReconstructor (bitmap, HLL):
//
//	returns a synthetic SCALAR snapshot with reconstructed bytes.
//
// For other types: returns wrongType=true.
func getStringSnapshot(cmd *shared.Command, key string) (snap *pb.ReducedEffect, tips []effects.Tip, exists bool, wrongType bool, err error) {
	snap, tips, err = cmd.Context.GetSnapshot(key)
	if err != nil {
		return nil, nil, false, false, err
	}
	if snap == nil || snap.Op == pb.EffectOp_REMOVE_OP {
		return nil, tips, false, false, nil
	}
	// Pure scalar string
	if snap.Collection == pb.CollectionKind_SCALAR &&
		(snap.TypeTag == pb.ValueType_TYPE_STRING || snap.TypeTag == pb.ValueType_TYPE_UNSPECIFIED) {
		return snap, tips, true, false, nil
	}
	// Try reconstruction (bitmap, HLL, etc.)
	if data, ok := shared.ReconstructBytes(snap); ok {
		synth := &pb.ReducedEffect{
			Collection: pb.CollectionKind_SCALAR,
			TypeTag:    pb.ValueType_TYPE_STRING,
			Scalar:     &pb.DataEffect{Value: &pb.DataEffect_Raw{Raw: data}},
			ExpiresAt:  snap.ExpiresAt,
		}
		return synth, tips, true, false, nil
	}
	return nil, tips, true, true, nil
}

func emitStringSet(cmd *shared.Command, key string, value []byte, tips ...[]effects.Tip) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_LAST_WRITE_WINS,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: value},
		}},
	}, tips...)
}

func emitStringTypeTag(cmd *shared.Command, key string) error {
	return cmd.Context.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_STRING}},
	})
}

func emitStringTTL(cmd *shared.Command, key string, expiresAtMs *timestamppb.Timestamp) error {
	return cmd.Context.Emit(&pb.Effect{
		Key:  []byte(key),
		Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{ExpiresAt: expiresAtMs}},
	})
}

func emitDeleteKey(cmd *shared.Command, key string, tips ...[]effects.Tip) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_REMOVE_OP,
			Collection: pb.CollectionKind_SCALAR,
		}},
	}, tips...)
}

func computeExpiresAtMs(expireSeconds, expireMillis, expireAt, expireAtMillis int64) *timestamppb.Timestamp {
	if expireSeconds > 0 {
		return timestamppb.New(time.Now().Add(time.Duration(expireSeconds) * time.Second))
	}
	if expireMillis > 0 {
		return timestamppb.New(time.Now().Add(time.Duration(expireMillis) * time.Millisecond))
	}
	if expireAt > 0 {
		return timestamppb.New(time.Unix(expireAt, 0))
	}
	if expireAtMillis > 0 {
		return timestamppb.New(time.UnixMilli(expireAtMillis))
	}
	return nil
}

// snapshotToRaw renders any scalar snapshot value back to its byte representation.
func snapshotToRaw(snap *pb.ReducedEffect) []byte {
	if snap == nil || snap.Scalar == nil {
		return nil
	}
	switch v := snap.Scalar.Value.(type) {
	case *pb.DataEffect_Raw, *pb.DataEffect_Compressed:
		return snap.Scalar.Decompress()
	case *pb.DataEffect_IntVal:
		return []byte(strconv.FormatInt(v.IntVal, 10))
	case *pb.DataEffect_FloatVal:
		return []byte(strconv.FormatFloat(v.FloatVal, 'f', -1, 64))
	default:
		return nil
	}
}

func snapshotToInt(snap *pb.ReducedEffect) (int64, bool) {
	if snap == nil || snap.Scalar == nil {
		return 0, true
	}
	switch v := snap.Scalar.Value.(type) {
	case *pb.DataEffect_IntVal:
		return v.IntVal, true
	case *pb.DataEffect_Raw, *pb.DataEffect_Compressed:
		return shared.ParseInt64(snap.Scalar.Decompress())
	case nil:
		return 0, true
	default:
		return 0, false
	}
}

func snapshotToFloat(snap *pb.ReducedEffect) (float64, bool) {
	if snap == nil || snap.Scalar == nil {
		return 0, true
	}
	switch v := snap.Scalar.Value.(type) {
	case *pb.DataEffect_FloatVal:
		return v.FloatVal, true
	case *pb.DataEffect_Raw, *pb.DataEffect_Compressed:
		f, err := strconv.ParseFloat(string(snap.Scalar.Decompress()), 64)
		return f, err == nil
	case *pb.DataEffect_IntVal:
		return float64(v.IntVal), true
	case nil:
		return 0, true
	default:
		return 0, false
	}
}

func emitIncrInt(cmd *shared.Command, key string, delta int64, tips ...[]effects.Tip) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_ADDITIVE_INT,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_IntVal{IntVal: delta},
		}},
	}, tips...)
}

func emitIncrFloat(cmd *shared.Command, key string, delta float64, tips ...[]effects.Tip) error {
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_ADDITIVE_FLOAT,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_FloatVal{FloatVal: delta},
		}},
	}, tips...)
}

func handleGet(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("get")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getStringSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteNullBulkString()
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		w.WriteBulkString(snapshotToRaw(snap))
	}
	return
}

func handleSet(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("set")
		return
	}

	key := string(cmd.Args[0])
	value := cmd.Args[1]

	// Parse options: EX seconds | PX milliseconds | EXAT timestamp | PXAT timestamp | NX | XX | IFEQ | IFNE | IFDEQ | IFDNE | KEEPTTL | GET
	var expireSeconds, expireMillis int64
	var expireAt, expireAtMillis int64
	var nx, xx, keepTTL, getOld bool
	var ifeq, ifne, ifdeq, ifdne bool
	var compareValue []byte

	i := 2
	for i < len(cmd.Args) {
		opt := shared.ToUpper(cmd.Args[i])
		switch string(opt) {
		case "EX":
			if i+1 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			i++
			sec, ok := shared.ParseInt64(cmd.Args[i])
			if !ok {
				w.WriteNotInteger()
				return
			}
			if !validateExpireSeconds(sec) {
				w.WriteError("ERR invalid expire time in 'set' command")
				return
			}
			expireSeconds = sec
		case "PX":
			if i+1 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			i++
			ms, ok := shared.ParseInt64(cmd.Args[i])
			if !ok {
				w.WriteNotInteger()
				return
			}
			if !validateExpireMillis(ms) {
				w.WriteError("ERR invalid expire time in 'set' command")
				return
			}
			expireMillis = ms
		case "EXAT":
			if i+1 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			i++
			ts, ok := shared.ParseInt64(cmd.Args[i])
			if !ok {
				w.WriteNotInteger()
				return
			}
			if !validateExpireAtSeconds(ts) {
				w.WriteError("ERR invalid expire time in 'set' command")
				return
			}
			expireAt = ts
		case "PXAT":
			if i+1 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			i++
			ts, ok := shared.ParseInt64(cmd.Args[i])
			if !ok {
				w.WriteNotInteger()
				return
			}
			if !validateExpireAtMillis(ts) {
				w.WriteError("ERR invalid expire time in 'set' command")
				return
			}
			expireAtMillis = ts
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "IFEQ":
			if i+1 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			i++
			ifeq = true
			compareValue = cmd.Args[i]
		case "IFNE":
			if i+1 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			i++
			ifne = true
			compareValue = cmd.Args[i]
		case "IFDEQ":
			if i+1 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			i++
			ifdeq = true
			compareValue = cmd.Args[i]
		case "IFDNE":
			if i+1 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			i++
			ifdne = true
			compareValue = cmd.Args[i]
		case "KEEPTTL":
			keepTTL = true
		case "GET":
			getOld = true
		default:
			w.WriteSyntaxError()
			return
		}
		i++
	}

	// Count how many condition flags are set
	condCount := 0
	if nx {
		condCount++
	}
	if xx {
		condCount++
	}
	if ifeq {
		condCount++
	}
	if ifne {
		condCount++
	}
	if ifdeq {
		condCount++
	}
	if ifdne {
		condCount++
	}

	// NX, XX, IFEQ, IFNE, IFDEQ, IFDNE are mutually exclusive
	if condCount > 1 {
		w.WriteSyntaxError()
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, tips, exists, wrongType, err := getStringSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}

		// Handle GET option - return old value
		if getOld {
			if exists && !wrongType {
				defer w.WriteBulkString(snapshotToRaw(snap))
			} else if wrongType {
				w.WriteWrongType()
				return
			} else {
				defer w.WriteNullBulkString()
			}
		}

		// Check NX/XX conditions
		if nx && exists {
			if !getOld {
				w.WriteNullBulkString()
			}
			return
		}
		if xx && !exists {
			if !getOld {
				w.WriteNullBulkString()
			}
			return
		}

		// Check IFEQ condition: set only if current value equals compareValue
		if ifeq {
			if !exists {
				if !getOld {
					w.WriteNullBulkString()
				}
				return
			}
			if wrongType {
				w.WriteWrongType()
				return
			}
			if !bytes.Equal(snapshotToRaw(snap), compareValue) {
				if !getOld {
					w.WriteNullBulkString()
				}
				return
			}
		}

		// Check IFNE condition: set only if current value does NOT equal compareValue
		if ifne {
			if exists {
				if wrongType {
					w.WriteWrongType()
					return
				}
				if bytes.Equal(snapshotToRaw(snap), compareValue) {
					if !getOld {
						w.WriteNullBulkString()
					}
					return
				}
			}
		}

		// Check IFDEQ condition: set only if hash digest equals compareValue
		if ifdeq {
			if !exists {
				if !getOld {
					w.WriteNullBulkString()
				}
				return
			}
			if wrongType {
				w.WriteWrongType()
				return
			}
			if !isValidHexDigest(compareValue) {
				w.WriteError("ERR digest must be exactly 16 hexadecimal characters")
				return
			}
			hash := xxh3.Hash(snapshotToRaw(snap))
			if !digestEquals(hash, compareValue) {
				if !getOld {
					w.WriteNullBulkString()
				}
				return
			}
		}

		// Check IFDNE condition: set only if hash digest does NOT equal compareValue
		if ifdne {
			if exists {
				if wrongType {
					w.WriteWrongType()
					return
				}
				if !isValidHexDigest(compareValue) {
					w.WriteError("ERR digest must be exactly 16 hexadecimal characters")
					return
				}
				hash := xxh3.Hash(snapshotToRaw(snap))
				if digestEquals(hash, compareValue) {
					if !getOld {
						w.WriteNullBulkString()
					}
					return
				}
			}
		}

		// Emit the SET
		if err := emitStringSet(cmd, key, value, tips); err != nil {
			w.WriteError(err.Error())
			return
		}

		// Compute and emit TTL
		var expiresAtMs *timestamppb.Timestamp
		if keepTTL && exists && snap != nil {
			expiresAtMs = snap.ExpiresAt
		} else {
			expiresAtMs = computeExpiresAtMs(expireSeconds, expireMillis, expireAt, expireAtMillis)
		}
		if expiresAtMs != nil {
			if err := emitStringTTL(cmd, key, expiresAtMs); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		// Emit type tag if key is new or was a different type
		if !exists || wrongType {
			if err := emitStringTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		if !getOld {
			w.WriteOK()
		}

		if s := shared.GetServerStats(); s != nil {
			s.CmdSet.Add(1)
		}
	}
	return
}

func handleSetNX(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 2 {
		w.WriteWrongNumArguments("setnx")
		return
	}

	key := string(cmd.Args[0])
	value := cmd.Args[1]
	keys = []string{key}
	valid = true

	runner = func() {
		_, tips, exists, _, err := getStringSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if exists {
			w.WriteZero()
			return
		}
		if err := emitStringSet(cmd, key, value, tips); err != nil {
			w.WriteError(err.Error())
			return
		}
		if err := emitStringTypeTag(cmd, key); err != nil {
			w.WriteError(err.Error())
			return
		}
		w.WriteOne()
	}
	return
}

func handleSetEX(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("setex")
		return
	}

	key := string(cmd.Args[0])
	seconds, ok := shared.ParseInt64(cmd.Args[1])
	if !ok {
		w.WriteNotInteger()
		return
	}
	if !validateExpireSeconds(seconds) {
		w.WriteError("ERR invalid expire time in 'setex' command")
		return
	}
	value := cmd.Args[2]
	keys = []string{key}
	valid = true

	runner = func() {
		if err := emitStringSet(cmd, key, value); err != nil {
			w.WriteError(err.Error())
			return
		}
		if err := emitStringTTL(cmd, key, computeExpiresAtMs(seconds, 0, 0, 0)); err != nil {
			w.WriteError(err.Error())
			return
		}
		if err := emitStringTypeTag(cmd, key); err != nil {
			w.WriteError(err.Error())
			return
		}
		w.WriteOK()
	}
	return
}

func handlePSetEX(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("psetex")
		return
	}

	key := string(cmd.Args[0])
	millis, ok := shared.ParseInt64(cmd.Args[1])
	if !ok {
		w.WriteNotInteger()
		return
	}
	if !validateExpireMillis(millis) {
		w.WriteError("ERR invalid expire time in 'psetex' command")
		return
	}
	value := cmd.Args[2]
	keys = []string{key}
	valid = true

	runner = func() {
		if err := emitStringSet(cmd, key, value); err != nil {
			w.WriteError(err.Error())
			return
		}
		if err := emitStringTTL(cmd, key, computeExpiresAtMs(0, millis, 0, 0)); err != nil {
			w.WriteError(err.Error())
			return
		}
		if err := emitStringTypeTag(cmd, key); err != nil {
			w.WriteError(err.Error())
			return
		}
		w.WriteOK()
	}
	return
}

func handleGetSet(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 2 {
		w.WriteWrongNumArguments("getset")
		return
	}

	key := string(cmd.Args[0])
	value := cmd.Args[1]
	keys = []string{key}
	valid = true

	runner = func() {
		snap, tips, exists, wrongType, err := getStringSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if exists && wrongType {
			w.WriteWrongType()
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

		if err := emitStringSet(cmd, key, value); err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			if err := emitStringTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		if exists {
			w.WriteBulkString(snapshotToRaw(snap))
		} else {
			w.WriteNullBulkString()
		}
	}
	return
}

func handleGetDel(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("getdel")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true

	runner = func() {
		snap, tips, exists, wrongType, err := getStringSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteNullBulkString()
			return
		}
		if wrongType {
			w.WriteWrongType()
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

		if err := emitDeleteKey(cmd, key); err != nil {
			w.WriteError(err.Error())
			return
		}

		w.WriteBulkString(snapshotToRaw(snap))
	}
	return
}

func handleGetEx(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("getex")
		return
	}

	key := string(cmd.Args[0])

	// Pre-validate options
	var expireSeconds, expireMillis, expireAt, expireAtMillis int64
	var persist bool

	if len(cmd.Args) > 1 {
		opt := shared.ToUpper(cmd.Args[1])
		switch string(opt) {
		case "EX":
			if len(cmd.Args) != 3 {
				w.WriteSyntaxError()
				return
			}
			sec, ok := shared.ParseInt64(cmd.Args[2])
			if !ok {
				w.WriteNotInteger()
				return
			}
			if !validateExpireSeconds(sec) {
				w.WriteError("ERR invalid expire time in 'getex' command")
				return
			}
			expireSeconds = sec
		case "PX":
			if len(cmd.Args) != 3 {
				w.WriteSyntaxError()
				return
			}
			ms, ok := shared.ParseInt64(cmd.Args[2])
			if !ok {
				w.WriteNotInteger()
				return
			}
			if !validateExpireMillis(ms) {
				w.WriteError("ERR invalid expire time in 'getex' command")
				return
			}
			expireMillis = ms
		case "EXAT":
			if len(cmd.Args) != 3 {
				w.WriteSyntaxError()
				return
			}
			ts, ok := shared.ParseInt64(cmd.Args[2])
			if !ok {
				w.WriteNotInteger()
				return
			}
			if !validateExpireAtSeconds(ts) {
				w.WriteError("ERR invalid expire time in 'getex' command")
				return
			}
			expireAt = ts
		case "PXAT":
			if len(cmd.Args) != 3 {
				w.WriteSyntaxError()
				return
			}
			ts, ok := shared.ParseInt64(cmd.Args[2])
			if !ok {
				w.WriteNotInteger()
				return
			}
			if !validateExpireAtMillis(ts) {
				w.WriteError("ERR invalid expire time in 'getex' command")
				return
			}
			expireAtMillis = ts
		case "PERSIST":
			persist = true
		default:
			w.WriteSyntaxError()
			return
		}
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, tips, exists, wrongType, err := getStringSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteNullBulkString()
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		hasExpiry := expireSeconds > 0 || expireMillis > 0 || expireAt > 0 || expireAtMillis > 0 || persist

		if hasExpiry {
			cmd.Context.BeginTx()

			if err := cmd.Context.Emit(&pb.Effect{
				Key:  []byte(key),
				Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
			}, tips); err != nil {
				w.WriteError(err.Error())
				return
			}

			var expiresAtMs *timestamppb.Timestamp
			if persist {
				expiresAtMs = nil
			} else {
				expiresAtMs = computeExpiresAtMs(expireSeconds, expireMillis, expireAt, expireAtMillis)
			}
			if err := emitStringTTL(cmd, key, expiresAtMs); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteBulkString(snapshotToRaw(snap))
	}
	return
}

func handleMGet(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("mget")
		return
	}

	keys = make([]string, len(cmd.Args))
	for i, arg := range cmd.Args {
		keys[i] = string(arg)
	}
	valid = true

	runner = func() {
		type mgetResult struct {
			snap *pb.ReducedEffect
			data []byte
		}
		results := make([]mgetResult, len(keys))
		for i, key := range keys {
			snap, _, err := cmd.Context.GetSnapshot(key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if snap == nil {
				continue
			}
			// Pure scalar string
			if snap.Collection == pb.CollectionKind_SCALAR &&
				(snap.TypeTag == pb.ValueType_TYPE_STRING || snap.TypeTag == pb.ValueType_TYPE_UNSPECIFIED) {
				results[i].snap = snap
				continue
			}
			// Try reconstruction (bitmap, HLL, etc.)
			if data, ok := shared.ReconstructBytes(snap); ok {
				results[i].data = data
				continue
			}
			// MGET is lenient for WRONGTYPE — null — but read uncertainty is an error.
		}

		w.WriteArray(len(keys))
		for _, r := range results {
			if r.snap != nil {
				w.WriteBulkString(snapshotToRaw(r.snap))
			} else if r.data != nil {
				w.WriteBulkString(r.data)
			} else {
				w.WriteNullBulkString()
			}
		}
	}
	return
}

func handleMSet(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 || len(cmd.Args)%2 != 0 {
		w.WriteWrongNumArguments("mset")
		return
	}

	keys = make([]string, len(cmd.Args)/2)
	for i := 0; i < len(cmd.Args); i += 2 {
		keys[i/2] = string(cmd.Args[i])
	}
	valid = true

	runner = func() {
		for i := 0; i < len(cmd.Args); i += 2 {
			key := string(cmd.Args[i])
			value := cmd.Args[i+1]

			if err := emitStringSet(cmd, key, value); err != nil {
				w.WriteError(err.Error())
				return
			}
			if err := emitStringTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}
		w.WriteOK()
	}
	return
}

func handleMSetNX(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 || len(cmd.Args)%2 != 0 {
		w.WriteWrongNumArguments("msetnx")
		return
	}

	keys = make([]string, len(cmd.Args)/2)
	for i := 0; i < len(cmd.Args); i += 2 {
		keys[i/2] = string(cmd.Args[i])
	}
	valid = true

	runner = func() {
		// Check all keys for existence
		type keyInfo struct {
			key  string
			tips []effects.Tip
		}
		infos := make([]keyInfo, 0, len(keys))

		for _, key := range keys {
			snap, tips, _, _, err := getStringSnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if snap != nil {
				// Key exists — abort
				w.WriteZero()
				return
			}
			infos = append(infos, keyInfo{key: key, tips: tips})
		}

		// All keys are new — begin transaction to ensure atomicity
		cmd.Context.BeginTx()

		// Record reads (Noop per key)
		for _, info := range infos {
			if err := cmd.Context.Emit(&pb.Effect{
				Key:  []byte(info.key),
				Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
			}, info.tips); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		// Write all keys
		for i := 0; i < len(cmd.Args); i += 2 {
			key := string(cmd.Args[i])
			value := cmd.Args[i+1]

			if err := emitStringSet(cmd, key, value); err != nil {
				w.WriteError(err.Error())
				return
			}
			if err := emitStringTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteOne()
	}
	return
}

func handleMSetEX(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	// MSETEX numkeys key value [key value ...] [NX|XX] [EX seconds|PX ms|EXAT|PXAT|KEEPTTL]
	// Minimum: numkeys + 1 key-value pair = 3 args
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("msetex")
		return
	}

	numKeys, ok := shared.ParseInt64(cmd.Args[0])
	if !ok || numKeys <= 0 {
		w.WriteError("ERR invalid numkeys value")
		return
	}

	// Check for overflow: Redis rejects numkeys > INT32_MAX
	const maxNumKeys int64 = 2147483647
	if numKeys > maxNumKeys {
		w.WriteError("ERR invalid numkeys value")
		return
	}

	// Need at least numKeys * 2 args for key-value pairs after numkeys
	kvEnd := 1 + int(numKeys)*2
	if len(cmd.Args) < kvEnd {
		w.WriteError("ERR wrong number of key-value pairs")
		return
	}

	// Parse options after key-value pairs
	var expireSeconds, expireMillis int64
	var expireAt, expireAtMillis int64
	var nx, xx, keepTTL bool
	var expireCount int // Track how many expire options are set

	i := kvEnd
	for i < len(cmd.Args) {
		opt := shared.ToUpper(cmd.Args[i])
		switch string(opt) {
		case "EX":
			if i+1 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			i++
			sec, ok := shared.ParseInt64(cmd.Args[i])
			if !ok || sec <= 0 {
				w.WriteNotInteger()
				return
			}
			expireSeconds = sec
			expireCount++
		case "PX":
			if i+1 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			i++
			ms, ok := shared.ParseInt64(cmd.Args[i])
			if !ok || ms <= 0 {
				w.WriteNotInteger()
				return
			}
			expireMillis = ms
			expireCount++
		case "EXAT":
			if i+1 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			i++
			ts, ok := shared.ParseInt64(cmd.Args[i])
			if !ok || ts <= 0 {
				w.WriteNotInteger()
				return
			}
			expireAt = ts
			expireCount++
		case "PXAT":
			if i+1 >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			i++
			ts, ok := shared.ParseInt64(cmd.Args[i])
			if !ok || ts <= 0 {
				w.WriteNotInteger()
				return
			}
			expireAtMillis = ts
			expireCount++
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "KEEPTTL":
			keepTTL = true
			expireCount++
		default:
			w.WriteSyntaxError()
			return
		}
		i++
	}

	// NX and XX are mutually exclusive
	if nx && xx {
		w.WriteSyntaxError()
		return
	}

	// Expiration options are mutually exclusive (EX, PX, EXAT, PXAT, KEEPTTL)
	if expireCount > 1 {
		w.WriteSyntaxError()
		return
	}

	// Extract keys
	keys = make([]string, numKeys)
	for j := 0; j < int(numKeys); j++ {
		keys[j] = string(cmd.Args[1+j*2])
	}
	valid = true

	runner = func() {
		// Gather snapshots for all keys (needed for NX/XX checks and KEEPTTL)
		type keyInfo struct {
			key  string
			tips []effects.Tip
			snap *pb.ReducedEffect
		}
		infos := make([]keyInfo, 0, len(keys))

		for _, key := range keys {
			snap, tips, _, _, err := getStringSnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			infos = append(infos, keyInfo{key: key, tips: tips, snap: snap})
		}

		// NX check: if ANY key exists, abort
		if nx {
			for _, info := range infos {
				if info.snap != nil {
					w.WriteZero()
					return
				}
			}
		}

		// XX check: if ANY key is missing, abort
		if xx {
			for _, info := range infos {
				if info.snap == nil {
					w.WriteZero()
					return
				}
			}
		}

		// Begin transaction if NX or XX (conditional multi-key write)
		if nx || xx {
			cmd.Context.BeginTx()
			for _, info := range infos {
				if err := cmd.Context.Emit(&pb.Effect{
					Key:  []byte(info.key),
					Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
				}, info.tips); err != nil {
					w.WriteError(err.Error())
					return
				}
			}
		}

		// Compute TTL once (same for all keys)
		expiresAtMs := computeExpiresAtMs(expireSeconds, expireMillis, expireAt, expireAtMillis)

		// Write all key-value pairs
		for idx, j := 0, 1; j < kvEnd; j += 2 {
			key := string(cmd.Args[j])
			value := cmd.Args[j+1]

			if err := emitStringSet(cmd, key, value); err != nil {
				w.WriteError(err.Error())
				return
			}
			if err := emitStringTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}

			// Determine TTL
			ttl := expiresAtMs
			if keepTTL && infos[idx].snap != nil {
				ttl = infos[idx].snap.ExpiresAt
			}
			if ttl != nil {
				if err := emitStringTTL(cmd, key, ttl); err != nil {
					w.WriteError(err.Error())
					return
				}
			}
			idx++
		}

		if nx || xx {
			w.WriteOne()
		} else {
			w.WriteOK()
		}
	}
	return
}

func handleIncr(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("incr")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true

	runner = func() {
		incrBy(cmd, key, 1, w)
	}
	return
}

func handleDecr(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("decr")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true

	runner = func() {
		incrBy(cmd, key, -1, w)
	}
	return
}

func handleIncrBy(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 2 {
		w.WriteWrongNumArguments("incrby")
		return
	}

	key := string(cmd.Args[0])
	delta, ok := shared.ParseInt64(cmd.Args[1])
	if !ok {
		w.WriteNotInteger()
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		incrBy(cmd, key, delta, w)
	}
	return
}

func handleDecrBy(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 2 {
		w.WriteWrongNumArguments("decrby")
		return
	}

	key := string(cmd.Args[0])
	delta, ok := shared.ParseInt64(cmd.Args[1])
	if !ok {
		w.WriteNotInteger()
		return
	}

	// Check for overflow when negating: -math.MinInt64 would overflow
	if delta == math.MinInt64 {
		w.WriteError("ERR value is out of range")
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		incrBy(cmd, key, -delta, w)
	}
	return
}

func incrBy(cmd *shared.Command, key string, delta int64, w *shared.Writer) {
	snap, tips, exists, wrongType, err := getStringSnapshot(cmd, key)
	if err != nil {
		w.WriteError(err.Error())
		return
	}
	if wrongType {
		w.WriteWrongType()
		return
	}

	current, ok := snapshotToInt(snap)
	if exists && !ok {
		w.WriteNotInteger()
		return
	}

	newValue := current + delta
	if (delta > 0 && newValue < current) || (delta < 0 && newValue > current) {
		w.WriteError("ERR increment or decrement would overflow")
		return
	}

	if err := emitIncrInt(cmd, key, delta, tips); err != nil {
		w.WriteError(err.Error())
		return
	}
	if exists && snap.ExpiresAt != nil {
		if err := emitStringTTL(cmd, key, snap.ExpiresAt); err != nil {
			w.WriteError(err.Error())
			return
		}
	}
	if !exists {
		if err := emitStringTypeTag(cmd, key); err != nil {
			w.WriteError(err.Error())
			return
		}
	}

	w.WriteInteger(newValue)
}

func handleIncrByFloat(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 2 {
		w.WriteWrongNumArguments("incrbyfloat")
		return
	}

	key := string(cmd.Args[0])
	deltaStr := string(cmd.Args[1])
	delta, err := strconv.ParseFloat(deltaStr, 64)
	if err != nil {
		w.WriteError("ERR value is not a valid float")
		return
	}

	// Check if delta is NaN or Infinity
	if math.IsNaN(delta) || math.IsInf(delta, 0) {
		w.WriteError("ERR increment would produce NaN or Infinity")
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, tips, exists, wrongType, err := getStringSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		current, ok := snapshotToFloat(snap)
		if exists && !ok {
			w.WriteError("ERR value is not a valid float")
			return
		}

		newValue := current + delta
		if math.IsNaN(newValue) || math.IsInf(newValue, 0) {
			w.WriteError("ERR increment would produce NaN or Infinity")
			return
		}

		if err := emitIncrFloat(cmd, key, delta, tips); err != nil {
			w.WriteError(err.Error())
			return
		}
		if exists && snap.ExpiresAt != nil {
			if err := emitStringTTL(cmd, key, snap.ExpiresAt); err != nil {
				w.WriteError(err.Error())
				return
			}
		}
		if !exists {
			if err := emitStringTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteBulkString([]byte(strconv.FormatFloat(newValue, 'f', -1, 64)))
	}
	return
}

func handleAppend(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 2 {
		w.WriteWrongNumArguments("append")
		return
	}

	key := string(cmd.Args[0])
	appendData := cmd.Args[1]
	keys = []string{key}
	valid = true

	runner = func() {
		snap, tips, exists, wrongType, err := getStringSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		var currentData []byte
		if exists {
			currentData = snapshotToRaw(snap)
		}

		newData := make([]byte, len(currentData)+len(appendData))
		copy(newData, currentData)
		copy(newData[len(currentData):], appendData)

		if err := emitStringSet(cmd, key, newData, tips); err != nil {
			w.WriteError(err.Error())
			return
		}
		if exists && snap.ExpiresAt != nil {
			if err := emitStringTTL(cmd, key, snap.ExpiresAt); err != nil {
				w.WriteError(err.Error())
				return
			}
		}
		if !exists {
			if err := emitStringTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteInteger(int64(len(newData)))
	}
	return
}

func handleStrLen(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("strlen")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getStringSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteZero()
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		w.WriteInteger(int64(len(snapshotToRaw(snap))))
	}
	return
}

func handleSetRange(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("setrange")
		return
	}

	key := string(cmd.Args[0])
	offset, ok := shared.ParseInt64(cmd.Args[1])
	if !ok || offset < 0 {
		w.WriteError("ERR offset is out of range")
		return
	}
	value := cmd.Args[2]

	// Check offset before any arithmetic to prevent overflow
	if offset > shared.GetProtoMaxBulkLen() {
		w.WriteError("ERR string exceeds maximum allowed size (proto-max-bulk-len)")
		return
	}

	// Check if resulting string would exceed max size (safe now that offset is bounded)
	needed := offset + int64(len(value))
	if needed > shared.GetProtoMaxBulkLen() {
		w.WriteError("ERR string exceeds maximum allowed size (proto-max-bulk-len)")
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, tips, exists, wrongType, err := getStringSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}

		if !exists && len(value) == 0 {
			w.WriteInteger(0)
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		var data []byte
		if exists {
			raw := snapshotToRaw(snap)
			data = make([]byte, len(raw))
			copy(data, raw)
		}

		neededInt := int(needed)
		if neededInt > len(data) {
			newData := make([]byte, neededInt)
			copy(newData, data)
			data = newData
		}
		copy(data[int(offset):], value)

		if err := emitStringSet(cmd, key, data, tips); err != nil {
			w.WriteError(err.Error())
			return
		}
		if exists && snap.ExpiresAt != nil {
			if err := emitStringTTL(cmd, key, snap.ExpiresAt); err != nil {
				w.WriteError(err.Error())
				return
			}
		}
		if !exists {
			if err := emitStringTypeTag(cmd, key); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		w.WriteInteger(int64(len(data)))
	}
	return
}

func handleGetRange(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("getrange")
		return
	}

	key := string(cmd.Args[0])
	start, ok1 := shared.ParseInt64(cmd.Args[1])
	end, ok2 := shared.ParseInt64(cmd.Args[2])
	if !ok1 || !ok2 {
		w.WriteNotInteger()
		return
	}

	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getStringSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteBulkString(nil)
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		data := snapshotToRaw(snap)
		length := int64(len(data))

		// If both indices are negative and start > end, return empty
		if start < 0 && end < 0 && start > end {
			w.WriteBulkString(nil)
			return
		}

		// Handle negative indices
		startIdx := start
		endIdx := end
		if startIdx < 0 {
			startIdx = length + startIdx
		}
		if endIdx < 0 {
			endIdx = length + endIdx
		}

		// Clamp to valid range
		if startIdx < 0 {
			startIdx = 0
		}
		if endIdx < 0 {
			endIdx = 0
		}
		if endIdx >= length {
			endIdx = length - 1
		}

		if startIdx > endIdx || startIdx >= length {
			w.WriteBulkString(nil)
			return
		}

		w.WriteBulkString(data[startIdx : endIdx+1])
	}
	return
}

func handleLcs(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("lcs")
		return
	}

	key1 := string(cmd.Args[0])
	key2 := string(cmd.Args[1])

	// Parse options
	var wantLen, wantIdx, withMatchLen bool
	var minMatchLen int64 = 0

	for i := 2; i < len(cmd.Args); i++ {
		arg := shared.ToUpper(cmd.Args[i])
		switch string(arg) {
		case "LEN":
			wantLen = true
		case "IDX":
			wantIdx = true
		case "WITHMATCHLEN":
			withMatchLen = true
		case "MINMATCHLEN":
			i++
			if i >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			val, ok := shared.ParseInt64(cmd.Args[i])
			if !ok || val < 0 {
				w.WriteNotInteger()
				return
			}
			minMatchLen = val
		default:
			w.WriteSyntaxError()
			return
		}
	}

	keys = []string{key1, key2}
	valid = true

	runner = func() {
		// Get values via effects engine
		var data1, data2 []byte

		snap1, _, exists1, wrongType1, err := getStringSnapshot(cmd, key1)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType1 {
			w.WriteWrongType()
			return
		}
		if exists1 {
			data1 = snapshotToRaw(snap1)
		}

		snap2, _, exists2, wrongType2, err := getStringSnapshot(cmd, key2)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType2 {
			w.WriteWrongType()
			return
		}
		if exists2 {
			data2 = snapshotToRaw(snap2)
		}

		// Compute LCS
		lcsResult, lcsLen, matches := computeLCS(data1, data2)

		// Return based on mode
		if wantLen && !wantIdx {
			// Just return length
			w.WriteInteger(int64(lcsLen))
			return
		}

		if wantIdx {
			// Filter matches by minMatchLen
			var filteredMatches []lcsMatch
			for _, m := range matches {
				if m.length >= minMatchLen {
					filteredMatches = append(filteredMatches, m)
				}
			}

			// Build IDX response: ["matches", [...], "len", length]
			w.WriteArray(4)
			w.WriteBulkString([]byte("matches"))

			w.WriteArray(len(filteredMatches))
			for _, m := range filteredMatches {
				if withMatchLen {
					w.WriteArray(3)
				} else {
					w.WriteArray(2)
				}

				// First key range [start1, end1]
				w.WriteArray(2)
				w.WriteInteger(m.start1)
				w.WriteInteger(m.end1)

				// Second key range [start2, end2]
				w.WriteArray(2)
				w.WriteInteger(m.start2)
				w.WriteInteger(m.end2)

				if withMatchLen {
					w.WriteInteger(m.length)
				}
			}

			w.WriteBulkString([]byte("len"))
			w.WriteInteger(int64(lcsLen))
			return
		}

		// Default: return LCS string
		w.WriteBulkString(lcsResult)
	}
	return
}

func handleDigest(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 1 {
		w.WriteWrongNumArguments("digest")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true

	runner = func() {
		snap, _, exists, wrongType, err := getStringSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteNullBulkString()
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		hash := xxh3.Hash(snapshotToRaw(snap))

		// Convert to lowercase hex string (16 characters for 64-bit hash)
		var hashBytes [8]byte
		hashBytes[0] = byte(hash >> 56)
		hashBytes[1] = byte(hash >> 48)
		hashBytes[2] = byte(hash >> 40)
		hashBytes[3] = byte(hash >> 32)
		hashBytes[4] = byte(hash >> 24)
		hashBytes[5] = byte(hash >> 16)
		hashBytes[6] = byte(hash >> 8)
		hashBytes[7] = byte(hash)

		hexStr := hex.EncodeToString(hashBytes[:])
		w.WriteBulkString([]byte(hexStr))
	}
	return
}
