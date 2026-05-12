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

package zset

import (
	"strconv"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/redis/shared"
)

func handleZPopMin(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 || len(cmd.Args) > 2 {
		w.WriteWrongNumArguments("zpopmin")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	count := 1
	hasCount := len(cmd.Args) > 1
	if hasCount {
		var err error
		count, err = strconv.Atoi(string(cmd.Args[1]))
		if err != nil {
			w.WriteNotInteger()
			return
		}
		if count < 0 {
			w.WriteError("ERR value is out of range, must be positive")
			return
		}
	}

	valid = true
	runner = func() {
		snap, tips, exists, wrongType, err := getZSetSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteArray(0)
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		sorted := zsetSortedMembers(snap)
		if len(sorted) == 0 {
			w.WriteArray(0)
			return
		}

		cmd.Context.BeginTx()

		// Emit Noop to record the read with tips
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			w.WriteError(err.Error())
			return
		}

		// Take first N (min scores)
		n := min(count, len(sorted))
		entries := sorted[:n]

		for _, e := range entries {
			if err := emitZSetRemove(cmd, key, e.Member); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		if w.Protocol() == shared.RESP3 && hasCount {
			w.WriteArray(len(entries))
			for _, entry := range entries {
				w.WriteArray(2)
				w.WriteBulkStringStr(entry.Member)
				w.WriteScore(entry.Score)
			}
		} else {
			w.WriteArray(len(entries) * 2)
			for _, entry := range entries {
				w.WriteBulkStringStr(entry.Member)
				w.WriteScore(entry.Score)
			}
		}
	}
	return
}

func handleZPopMax(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 || len(cmd.Args) > 2 {
		w.WriteWrongNumArguments("zpopmax")
		return
	}

	key := string(cmd.Args[0])
	keys = []string{key}
	count := 1
	hasCount := len(cmd.Args) > 1
	if hasCount {
		var err error
		count, err = strconv.Atoi(string(cmd.Args[1]))
		if err != nil {
			w.WriteNotInteger()
			return
		}
		if count < 0 {
			w.WriteError("ERR value is out of range, must be positive")
			return
		}
	}

	valid = true
	runner = func() {
		snap, tips, exists, wrongType, err := getZSetSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteArray(0)
			return
		}
		if wrongType {
			w.WriteWrongType()
			return
		}

		sorted := zsetSortedMembers(snap)
		if len(sorted) == 0 {
			w.WriteArray(0)
			return
		}

		cmd.Context.BeginTx()

		// Emit Noop to record the read with tips
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			w.WriteError(err.Error())
			return
		}

		// Take last N (max scores) in reverse order
		n := min(count, len(sorted))
		entries := make([]shared.ZSetEntry, n)
		for i := range n {
			entries[i] = sorted[len(sorted)-1-i]
		}

		for _, e := range entries {
			if err := emitZSetRemove(cmd, key, e.Member); err != nil {
				w.WriteError(err.Error())
				return
			}
		}

		if w.Protocol() == shared.RESP3 && hasCount {
			w.WriteArray(len(entries))
			for _, entry := range entries {
				w.WriteArray(2)
				w.WriteBulkStringStr(entry.Member)
				w.WriteScore(entry.Score)
			}
		} else {
			w.WriteArray(len(entries) * 2)
			for _, entry := range entries {
				w.WriteBulkStringStr(entry.Member)
				w.WriteScore(entry.Score)
			}
		}
	}
	return
}

// handleZMPop implements ZMPOP numkeys key [key ...] <MIN | MAX> [COUNT count]
func handleZMPop(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 {
		w.WriteWrongNumArguments("zmpop")
		return
	}

	numKeys, err := strconv.Atoi(string(cmd.Args[0]))
	if err != nil || numKeys <= 0 {
		w.WriteError("ERR numkeys should be greater than 0")
		return
	}

	if len(cmd.Args) < numKeys+2 {
		w.WriteSyntaxError()
		return
	}

	keys = make([]string, numKeys)
	for i := range numKeys {
		keys[i] = string(cmd.Args[1+i])
	}

	// Parse MIN/MAX
	directionArg := shared.ToUpper(cmd.Args[1+numKeys])
	var popMin bool
	switch string(directionArg) {
	case "MIN":
		popMin = true
	case "MAX":
		popMin = false
	default:
		w.WriteSyntaxError()
		return
	}

	// Parse optional COUNT
	count := 1
	hasCount := false
	argIdx := 2 + numKeys
	for argIdx < len(cmd.Args) {
		opt := shared.ToUpper(cmd.Args[argIdx])
		if string(opt) == "COUNT" {
			if hasCount {
				// Duplicate COUNT
				w.WriteSyntaxError()
				return
			}
			hasCount = true
			argIdx++
			if argIdx >= len(cmd.Args) {
				w.WriteSyntaxError()
				return
			}
			count, err = strconv.Atoi(string(cmd.Args[argIdx]))
			if err != nil || count <= 0 {
				w.WriteError("ERR count must be positive")
				return
			}
			argIdx++
		} else {
			w.WriteSyntaxError()
			return
		}
	}

	valid = true
	runner = func() {
		// Try each key in order until we find a non-empty one
		for _, key := range keys {
			snap, tips, exists, wrongType, err := getZSetSnapshot(cmd, key)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if wrongType {
				w.WriteWrongType()
				return
			}
			if !exists {
				continue
			}

			sorted := zsetSortedMembers(snap)
			if len(sorted) == 0 {
				continue
			}

			cmd.Context.BeginTx()

			// Emit Noop to record the read with tips
			if err := cmd.Context.Emit(&pb.Effect{
				Key:  []byte(key),
				Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
			}, tips); err != nil {
				w.WriteError(err.Error())
				return
			}

			n := min(count, len(sorted))

			var entries []shared.ZSetEntry
			if popMin {
				entries = sorted[:n]
			} else {
				entries = make([]shared.ZSetEntry, n)
				for i := range n {
					entries[i] = sorted[len(sorted)-1-i]
				}
			}

			for _, e := range entries {
				if err := emitZSetRemove(cmd, key, e.Member); err != nil {
					w.WriteError(err.Error())
					return
				}
			}

			// Return [key, [[member, score], ...]]
			w.WriteArray(2)
			w.WriteBulkStringStr(key)
			w.WriteArray(len(entries))
			for _, entry := range entries {
				w.WriteArray(2)
				w.WriteBulkStringStr(entry.Member)
				w.WriteScore(entry.Score)
			}
			return
		}

		// No non-empty sorted set found
		w.WriteNullArray()
	}
	return
}
