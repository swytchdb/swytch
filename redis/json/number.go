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
	"math"

	pb "github.com/swytchdb/engine/cluster/proto"
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

// handleJSONNumPowBy — JSON.NUMPOWBY key path number
func handleJSONNumPowBy(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	return numBy(cmd, w, '^', "json.numpowby")
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
		targets := writeTargets(root, path)

		// Compute the new number per match (null for a non-number match); collect
		// the writes to apply.
		type pending struct {
			path *Path
			val  *Value
		}
		var writes []pending
		out := &Value{Kind: KindArray}
		for _, t := range targets {
			if isNumber(t.val) {
				nv, err := applyNumOp(op, t.val, by)
				if err != nil {
					w.WriteError(err.Error())
					return
				}
				out.Arr = append(out.Arr, nv)
				writes = append(writes, pending{t.path, nv})
			} else {
				out.Arr = append(out.Arr, newNull())
			}
		}
		apply := func() bool {
			cmd.Context.BeginTx()
			// Record the read for this key (Noop carries its tips).
			if err := cmd.Context.Emit(&pb.Effect{
				Key:  []byte(key),
				Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
			}, tips); err != nil {
				w.WriteError(err.Error())
				return false
			}
			plain := func(e *pb.Effect) error { return cmd.Context.Emit(e) }
			for _, pw := range writes {
				applied, err := setValueAtPath(plain, root, key, pw.path, pw.val, exists)
				if err != nil {
					w.WriteError(err.Error())
					return false
				}
				if !applied {
					w.WriteError(errPathNotSet.Error())
					return false
				}
			}
			return true
		}

		if path.JSONPath {
			if len(writes) > 0 && !apply() {
				return
			}
			w.WriteBulkString(Serialize(out))
			return
		}

		// Legacy path: bare number, errors on a missing or non-number path.
		if len(targets) == 0 {
			w.WriteError("ERR Path '" + string(cmd.Args[1]) + "' does not exist")
			return
		}
		if !isNumber(targets[0].val) {
			w.WriteError("ERR wrong type of path value - expected a number but found " + targets[0].val.TypeName())
			return
		}
		if !apply() {
			return
		}
		w.WriteBulkString(Serialize(out.Arr[0]))
	}
	return
}

func isNumber(v *Value) bool { return v != nil && (v.Kind == KindInt || v.Kind == KindFloat) }

func numAsFloat(v *Value) float64 {
	if v.Kind == KindInt {
		return float64(v.Int)
	}
	return v.Float
}

// applyNumOp combines cur and by for op ('+' add, '*' multiply, '^' power).
// Integer operands keep an integer result (matching RedisJSON); any float
// operand promotes to a float result. Integer exponentiation by a negative power
// has no integer result, which RedisJSON reports as a numeric overflow.
func applyNumOp(op byte, cur, by *Value) (*Value, error) {
	if cur.Kind == KindInt && by.Kind == KindInt {
		switch op {
		case '+':
			return newInt(cur.Int + by.Int), nil
		case '*':
			return newInt(cur.Int * by.Int), nil
		case '^':
			if by.Int < 0 {
				return nil, errors.New("ERR numeric overflow")
			}
			return newInt(intPow(cur.Int, by.Int)), nil
		}
	}
	a, b := numAsFloat(cur), numAsFloat(by)
	switch op {
	case '+':
		return newFloat(a + b), nil
	case '*':
		return newFloat(a * b), nil
	case '^':
		return newFloat(math.Pow(a, b)), nil
	}
	return nil, errors.New("ERR unknown numeric operation")
}

// intPow returns base**exp for exp >= 0 by binary exponentiation.
func intPow(base, exp int64) int64 {
	result := int64(1)
	for exp > 0 {
		if exp&1 == 1 {
			result *= base
		}
		base *= base
		exp >>= 1
	}
	return result
}
