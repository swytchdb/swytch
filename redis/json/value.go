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

// Package json implements the RedisJSON (JSON.*) command family on top of the
// effects engine's virtual-partition keys. This file defines the internal,
// dependency-free JSON value model. Object member order is preserved (a
// RedisJSON requirement) by storing members as an ordered slice, not a Go map.
package json

// Kind enumerates the JSON value kinds. RedisJSON distinguishes "integer" from
// "number", so integral and fractional numbers are separate kinds.
type Kind uint8

const (
	KindNull Kind = iota
	KindBool
	KindInt
	KindFloat
	KindString
	KindArray
	KindObject
)

// Value is the internal JSON model. Exactly one payload field is meaningful per
// Kind. Objects keep insertion order via the Obj slice.
type Value struct {
	Kind  Kind
	Bool  bool
	Int   int64
	Float float64
	Str   string
	Arr   []*Value
	Obj   []Member
}

// Member is one key/value pair of an object, in insertion order.
type Member struct {
	Key string
	Val *Value
}

// TypeName returns the RedisJSON type string reported by JSON.TYPE.
func (v *Value) TypeName() string {
	switch v.Kind {
	case KindNull:
		return "null"
	case KindBool:
		return "boolean"
	case KindInt:
		return "integer"
	case KindFloat:
		return "number"
	case KindString:
		return "string"
	case KindArray:
		return "array"
	case KindObject:
		return "object"
	}
	return "null"
}

// IsContainer reports whether v is an object or array (vs a scalar leaf).
func (v *Value) IsContainer() bool {
	return v.Kind == KindObject || v.Kind == KindArray
}

// objIndex returns the index of key in an object, or -1 if absent.
func (v *Value) objIndex(key string) int {
	for i := range v.Obj {
		if v.Obj[i].Key == key {
			return i
		}
	}
	return -1
}

// objGet returns the value for key and whether it exists.
func (v *Value) objGet(key string) (*Value, bool) {
	if i := v.objIndex(key); i >= 0 {
		return v.Obj[i].Val, true
	}
	return nil, false
}

// objSet inserts or replaces key, preserving insertion order on replace and
// appending on insert.
func (v *Value) objSet(key string, val *Value) {
	if i := v.objIndex(key); i >= 0 {
		v.Obj[i].Val = val
		return
	}
	v.Obj = append(v.Obj, Member{Key: key, Val: val})
}

// objDelete removes key, preserving order. Returns true if removed.
func (v *Value) objDelete(key string) bool {
	if i := v.objIndex(key); i >= 0 {
		v.Obj = append(v.Obj[:i], v.Obj[i+1:]...)
		return true
	}
	return false
}

// DeepCopy returns an independent clone of v.
func (v *Value) DeepCopy() *Value {
	if v == nil {
		return nil
	}
	c := &Value{Kind: v.Kind, Bool: v.Bool, Int: v.Int, Float: v.Float, Str: v.Str}
	if v.Arr != nil {
		c.Arr = make([]*Value, len(v.Arr))
		for i, e := range v.Arr {
			c.Arr[i] = e.DeepCopy()
		}
	}
	if v.Obj != nil {
		c.Obj = make([]Member, len(v.Obj))
		for i, m := range v.Obj {
			c.Obj[i] = Member{Key: m.Key, Val: m.Val.DeepCopy()}
		}
	}
	return c
}

// scalar constructors used by the path/handler layers.
func newNull() *Value          { return &Value{Kind: KindNull} }
func newBool(b bool) *Value     { return &Value{Kind: KindBool, Bool: b} }
func newInt(i int64) *Value     { return &Value{Kind: KindInt, Int: i} }
func newFloat(f float64) *Value { return &Value{Kind: KindFloat, Float: f} }
func newString(s string) *Value { return &Value{Kind: KindString, Str: s} }
