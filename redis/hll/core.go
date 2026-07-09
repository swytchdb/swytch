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

package hll

import (
	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

// HLL registers are stored as a SCALAR with a packed 16384-byte array,
// one byte per register. The MAX_BYTES merge rule takes the element-wise
// maximum, so concurrent PFADDs resolve correctly.

// getHLLSnapshot returns the snapshot for an HLL key.
// Returns (snapshot, tips, exists, wrongType, error).
func getHLLSnapshot(cmd *shared.Command, key string) (snap *pb.ReducedEffect, tips []effects.Tip, exists bool, wrongType bool, err error) {
	snap, tips, err = cmd.Context.GetSnapshot(key)
	if err != nil {
		return nil, nil, false, false, err
	}
	if snap == nil || snap.Op == pb.EffectOp_REMOVE_OP {
		return nil, tips, false, false, nil
	}
	if snap.Collection == pb.CollectionKind_SCALAR && snap.TypeTag == pb.ValueType_TYPE_HLL {
		return snap, tips, true, false, nil
	}
	return nil, tips, true, true, nil
}

// reconstructHLL rebuilds an HLLValue from a SCALAR snapshot.
func reconstructHLL(snap *pb.ReducedEffect) *shared.HLLValue {
	hll := &shared.HLLValue{}
	if snap == nil || snap.Scalar == nil {
		return hll
	}
	raw := snap.Scalar.Decompress()
	n := min(len(raw), 16384)
	copy(hll.Registers[:n], raw)
	return hll
}

// mergeHLLFromSnap merges registers from a SCALAR snapshot directly into dst.
func mergeHLLFromSnap(dst *shared.HLLValue, snap *pb.ReducedEffect) {
	if snap == nil || snap.Scalar == nil {
		return
	}
	raw := snap.Scalar.Decompress()
	n := min(len(raw), 16384)
	for i := range n {
		if raw[i] > dst.Registers[i] {
			dst.Registers[i] = raw[i]
		}
	}
}

// emitHLLScalar emits the full register array as a single SCALAR MAX_BYTES effect.
func emitHLLScalar(cmd *shared.Command, key string, registers *[16384]uint8, tips []effects.Tip) error {
	raw := make([]byte, 16384)
	copy(raw, registers[:])
	return cmd.Context.Emit(&pb.Effect{
		Key: []byte(key),
		Kind: &pb.Effect_Data{Data: &pb.DataEffect{
			Op:         pb.EffectOp_INSERT_OP,
			Merge:      pb.MergeRule_MAX_BYTES,
			Collection: pb.CollectionKind_SCALAR,
			Value:      &pb.DataEffect_Raw{Raw: raw},
		}},
	}, tips)
}

// handlePFAdd handles the PFADD command
// PFADD key [element [element ...]]
// Adds elements to a HyperLogLog data structure
// Returns 1 if the internal registers were modified, 0 otherwise
func handlePFAdd(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("pfadd")
		return
	}

	key := string(cmd.Args[0])
	elements := cmd.Args[1:]
	keys = []string{key}

	valid = true
	runner = func() {
		snap, tips, exists, wrongType, err := getHLLSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		// Reconstruct current HLL state to check if registers change
		current := reconstructHLL(snap)

		modified := false
		for _, elem := range elements {
			regIdx, rho := hllHashElement(elem)
			if rho > current.Registers[regIdx] {
				modified = true
				current.Registers[regIdx] = rho
			}
		}

		// If no elements provided and key didn't exist, create empty HLL
		if len(elements) == 0 && !exists {
			modified = true
		}

		// Tag as HLL on first write
		if !exists {
			if err := cmd.Context.Emit(&pb.Effect{
				Key:  []byte(key),
				Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_HLL}},
			}); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		// Emit the full register array as a single scalar
		if modified {
			if err := emitHLLScalar(cmd, key, &current.Registers, tips); err != nil {
				w.WriteError(err.Error())
				return
			}
			w.WriteInteger(1)
		} else {
			w.WriteInteger(0)
		}
	}
	return
}

// handlePFCount handles the PFCOUNT command
// PFCOUNT key [key ...]
// Returns the approximate cardinality of the set(s) observed by the HyperLogLog(s)
func handlePFCount(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("pfcount")
		return
	}

	keys = make([]string, len(cmd.Args))
	for i, arg := range cmd.Args {
		keys[i] = string(arg)
	}

	valid = true
	runner = func() {
		if len(keys) == 1 {
			snap, _, exists, wrongType, err := getHLLSnapshot(cmd, keys[0])
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if !exists {
				w.WriteInteger(0)
				return
			}
			if wrongType {
				w.WriteWrongType()
				return
			}
			hll := reconstructHLL(snap)
			w.WriteInteger(hll.Count())
			return
		}

		// Single pass: check types and merge simultaneously
		merged := &shared.HLLValue{}
		for _, key := range keys {
			snap, _, exists, wrongType, err := getHLLSnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if exists && wrongType {
				w.WriteWrongType()
				return
			}
			if exists {
				mergeHLLFromSnap(merged, snap)
			}
		}

		w.WriteInteger(merged.Count())
	}
	return
}

// handlePFMerge handles the PFMERGE command
// PFMERGE destkey [sourcekey ...]
// Merges multiple HyperLogLog values into a single one
func handlePFMerge(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("pfmerge")
		return
	}

	destKey := string(cmd.Args[0])
	srcKeys := make([]string, len(cmd.Args)-1)
	for i := 1; i < len(cmd.Args); i++ {
		srcKeys[i-1] = string(cmd.Args[i])
	}

	keys = append([]string{destKey}, srcKeys...)

	valid = true
	runner = func() {
		// Check dest key type first
		destSnap, destTips, destExists, wrongType, err := getHLLSnapshot(cmd, destKey)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if destExists && wrongType {
			w.WriteWrongType()
			return
		}

		// Build merged HLL from dest + all sources (single pass)
		merged := reconstructHLL(destSnap)
		for _, key := range srcKeys {
			snap, _, exists, wt, err := getHLLSnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if exists && wt {
				w.WriteWrongType()
				return
			}
			if exists {
				mergeHLLFromSnap(merged, snap)
			}
		}

		// Tag as HLL if dest didn't exist
		if !destExists {
			if err := cmd.Context.Emit(&pb.Effect{
				Key:  []byte(destKey),
				Kind: &pb.Effect_Meta{Meta: &pb.MetaEffect{TypeTag: pb.ValueType_TYPE_HLL}},
			}); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		// Emit the full merged register array as a single scalar
		if err := emitHLLScalar(cmd, destKey, &merged.Registers, destTips); err != nil {
			w.WriteError(err.Error())
			return
		}

		w.WriteOK()
	}
	return
}

// handlePFSelfTest handles the PFSELFTEST command
// Runs internal consistency tests for HyperLogLog implementation
func handlePFSelfTest(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	keys = nil
	valid = true
	runner = func() {
		hll := &shared.HLLValue{}
		for i := range 1000 {
			hll.Add([]byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)})
		}

		count := hll.Count()
		if count < 900 || count > 1100 {
			w.WriteError("ERR HLL self test failed: count deviation too high")
			return
		}

		hll2 := &shared.HLLValue{}
		for i := 1000; i < 2000; i++ {
			hll2.Add([]byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)})
		}

		hll.Merge(hll2)
		mergedCount := hll.Count()
		if mergedCount < 1800 || mergedCount > 2200 {
			w.WriteError("ERR HLL self test failed: merged count deviation too high")
			return
		}

		w.WriteOK()
	}
	return
}

// handlePFDebug handles the PFDEBUG command
// PFDEBUG subcommand key [args...]
func handlePFDebug(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("pfdebug")
		return
	}

	subCmd := shared.ToUpper(cmd.Args[0])
	key := string(cmd.Args[1])
	keys = []string{key}

	valid = true
	runner = func() {
		snap, _, exists, wrongType, err := getHLLSnapshot(cmd, key)
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

		hll := reconstructHLL(snap)

		switch string(subCmd) {
		case "GETREG":
			w.WriteArray(16384)
			for _, reg := range hll.Registers {
				w.WriteInteger(int64(reg))
			}

		case "DECODE":
			w.WriteArray(16384)
			for _, reg := range hll.Registers {
				w.WriteInteger(int64(reg))
			}

		case "ENCODING":
			w.WriteBulkStringStr("dense")

		case "TODENSE":
			w.WriteInteger(0)

		default:
			w.WriteError("ERR Unknown PFDEBUG subcommand '" + string(subCmd) + "'")
		}
	}
	return
}

// hllHashElement hashes an element and returns (registerIndex, rho).
// This mirrors HLLValue.Add() logic but returns the computed values
// instead of mutating a register array.
func hllHashElement(element []byte) (regIdx int, rho uint8) {
	hash := shared.HLLHash(element)
	regIdx = int(hash & 0x3FFF)
	remainingBits := hash << 14
	rho = min(shared.HLLRho(remainingBits), 63)
	return
}
