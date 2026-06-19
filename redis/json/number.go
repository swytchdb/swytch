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

package json

import (
	"errors"

	pb "github.com/swytchdb/swytch/cluster/proto"
	"github.com/swytchdb/swytch/redis/shared"
)

var errPathNotSet = errors.New("ERR JSON path could not be set")

// handleJSONNumIncrBy — JSON.NUMINCRBY key path number
func handleJSONNumIncrBy(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	return numBy(cmd, w, '+', "json.numincrby")
}

// handleJSONNumMultBy — JSON.NUMMULTBY key path number
func handleJSONNumMultBy(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	return numBy(cmd, w, '*', "json.nummultby")
}

// numBy implements NUMINCRBY/NUMMULTBY: a read-modify-write on the number(s) at
// path, resolved as LWW. The RMW runs in an implicit transaction (Noop carries
// the read tips) so a competing write between read and commit aborts+retries.
func numBy(cmd *shared.Command, w *shared.Writer, op byte, name string) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments(name)
		return
	}
	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true
	runner = func() {
		path, err := ParsePath(string(cmd.Args[1]))
		if err != nil {
			w.WriteError("ERR " + err.Error())
			return
		}
		by, err := Parse(cmd.Args[2])
		if err != nil || !isNumber(by) {
			w.WriteError("ERR value is not a number")
			return
		}
		root, tips, exists, wrongType, err := getJSONSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if wrongType {
			writeWrongType(w)
			return
		}
		if !exists {
			w.WriteError("ERR could not perform this operation on a key that doesn't exist")
			return
		}
		matches := path.resolve(assemble(root, ""))

		if path.JSONPath {
			// One result entry per match: the new number, or null for a
			// non-number match. Only numbers are written. The supported path
			// grammar resolves to at most one location, so at most one write.
			out := &Value{Kind: KindArray}
			var newVal *Value
			for _, m := range matches {
				if isNumber(m) {
					newVal = applyNumOp(op, m, by)
					out.Arr = append(out.Arr, newVal)
				} else {
					out.Arr = append(out.Arr, newNull())
				}
			}
			if newVal != nil {
				cmd.Context.BeginTx()
				// Record the read for this key (Noop carries its tips).
				if err := cmd.Context.Emit(&pb.Effect{
					Key:  []byte(key),
					Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
				}, tips); err != nil {
					w.WriteError(err.Error())
					return
				}
				if err := writeNum(cmd, root, key, path, newVal, exists); err != nil {
					w.WriteError(err.Error())
					return
				}
			}
			w.WriteBulkString(Serialize(out))
			return
		}

		// Legacy path: bare number, errors on a missing or non-number path.
		if len(matches) == 0 {
			w.WriteError("ERR Path '" + string(cmd.Args[1]) + "' does not exist")
			return
		}
		cur := matches[0]
		if !isNumber(cur) {
			w.WriteError("ERR wrong type of path value - expected a number but found " + cur.TypeName())
			return
		}
		newVal := applyNumOp(op, cur, by)
		cmd.Context.BeginTx()
		// Record the read for this key (Noop carries its tips).
		if err := cmd.Context.Emit(&pb.Effect{
			Key:  []byte(key),
			Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
		}, tips); err != nil {
			w.WriteError(err.Error())
			return
		}
		if err := writeNum(cmd, root, key, path, newVal, exists); err != nil {
			w.WriteError(err.Error())
			return
		}
		w.WriteBulkString(Serialize(newVal))
	}
	return
}

// writeNum emits the leaf write for the computed number at path.
func writeNum(cmd *shared.Command, root *pb.ReducedEffect, key string, path *Path, v *Value, exists bool) error {
	applied, err := setValueAtPath(func(e *pb.Effect) error { return cmd.Context.Emit(e) }, root, key, path, v, exists)
	if err != nil {
		return err
	}
	if !applied {
		return errPathNotSet
	}
	return nil
}

func isNumber(v *Value) bool { return v != nil && (v.Kind == KindInt || v.Kind == KindFloat) }

func numAsFloat(v *Value) float64 {
	if v.Kind == KindInt {
		return float64(v.Int)
	}
	return v.Float
}

// applyNumOp combines cur and by. Integer operands keep an integer result
// (matching RedisJSON); any float operand promotes to a float result.
func applyNumOp(op byte, cur, by *Value) *Value {
	if cur.Kind == KindInt && by.Kind == KindInt {
		if op == '+' {
			return newInt(cur.Int + by.Int)
		}
		return newInt(cur.Int * by.Int)
	}
	a, b := numAsFloat(cur), numAsFloat(by)
	if op == '+' {
		return newFloat(a + b)
	}
	return newFloat(a * b)
}
