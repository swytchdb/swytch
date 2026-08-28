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
	"strings"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
	"github.com/swytchdb/swytch/redis/shared"
)

// handleJSONSet — JSON.SET key path value [NX | XX]
func handleJSONSet(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 || len(cmd.Args) > 4 {
		w.WriteWrongNumArguments("json.set")
		return
	}
	key := string(cmd.Args[0])
	var nx, xx bool
	if len(cmd.Args) == 4 {
		switch strings.ToUpper(string(cmd.Args[3])) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		default:
			w.WriteSyntaxError()
			return
		}
	}
	keys = []string{key}
	valid = true
	runner = func() {
		path, err := ParsePath(string(cmd.Args[1]))
		if err != nil {
			w.WriteError("ERR " + err.Error())
			return
		}
		newVal, err := Parse(cmd.Args[2])
		if err != nil {
			w.WriteError("ERR invalid JSON")
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

		// Existence of the addressed path gates NX/XX.
		targetExists := exists
		if exists && !path.IsRoot() {
			targetExists = len(path.resolve(assemble(root, ""))) > 0
		}
		if nx && targetExists {
			w.WriteNullBulkString()
			return
		}
		if xx && !targetExists {
			w.WriteNullBulkString()
			return
		}
		if !exists && !path.IsRoot() {
			w.WriteError("ERR new objects must be created at the root")
			return
		}

		emit := tipEmitter(cmd, tips)
		// A concrete path creates-or-replaces its single location; a multi-match
		// path (wildcard, slice, descent, …) replaces every existing match.
		if path.isConcrete() {
			applied, err := setValueAtPath(emit, root, key, path, newVal, exists)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if !applied {
				// A non-creatable path (missing intermediate) returns nil,
				// matching RedisJSON.
				w.WriteNullBulkString()
				return
			}
			w.WriteOK()
			return
		}
		for _, t := range writeTargets(root, path) {
			applied, err := setValueAtPath(emit, root, key, t.path, newVal, exists)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if !applied {
				w.WriteNullBulkString()
				return
			}
		}
		w.WriteOK()
	}
	return
}

// handleJSONGet — JSON.GET key [INDENT i] [NEWLINE n] [SPACE s] [path ...]
func handleJSONGet(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 {
		w.WriteWrongNumArguments("json.get")
		return
	}
	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true
	runner = func() {
		var opts PrintOpts
		var paths []string
		args := cmd.Args[1:]
		for i := 0; i < len(args); i++ {
			switch strings.ToUpper(string(args[i])) {
			case "INDENT":
				if i+1 >= len(args) {
					w.WriteSyntaxError()
					return
				}
				i++
				opts.Indent = string(args[i])
			case "NEWLINE":
				if i+1 >= len(args) {
					w.WriteSyntaxError()
					return
				}
				i++
				opts.Newline = string(args[i])
			case "SPACE":
				if i+1 >= len(args) {
					w.WriteSyntaxError()
					return
				}
				i++
				opts.Space = string(args[i])
			default:
				paths = append(paths, string(args[i]))
			}
		}
		if len(paths) == 0 {
			paths = []string{"$"}
		}

		root, _, exists, wrongType, err := getJSONSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteNullBulkString()
			return
		}
		if wrongType {
			writeWrongType(w)
			return
		}
		doc := assemble(root, "")

		if len(paths) == 1 {
			out, ok := getOne(doc, paths[0], opts)
			if !ok {
				w.WriteError("ERR Path '" + paths[0] + "' does not exist")
				return
			}
			w.WriteBulkString(out)
			return
		}
		// Multiple paths → a JSON object keyed by the path string. If any path
		// is a JSONPath, every value uses array semantics (matches RedisJSON).
		parsed := make([]*Path, len(paths))
		anyJSONPath := false
		for i, ps := range paths {
			p, err := ParsePath(ps)
			if err != nil {
				w.WriteError("ERR " + err.Error())
				return
			}
			parsed[i] = p
			if p.JSONPath {
				anyJSONPath = true
			}
		}
		obj := &Value{Kind: KindObject}
		for i, ps := range paths {
			matches := parsed[i].resolve(doc)
			if anyJSONPath {
				obj.objSet(ps, &Value{Kind: KindArray, Arr: matches})
				continue
			}
			if len(matches) == 0 {
				w.WriteError("ERR Path '" + ps + "' does not exist")
				return
			}
			obj.objSet(ps, matches[0])
		}
		w.WriteBulkString(SerializePretty(obj, opts))
	}
	return
}

// getValue resolves a single path to the value GET would report for it (an
// array for JSONPath, the first match for legacy). ok=false for a missing
// legacy path or a parse error.
func getValue(doc *Value, ps string) (*Value, bool) {
	p, err := ParsePath(ps)
	if err != nil {
		return nil, false
	}
	matches := p.resolve(doc)
	if p.JSONPath {
		return &Value{Kind: KindArray, Arr: matches}, true
	}
	if len(matches) == 0 {
		return nil, false
	}
	return matches[0], true
}

func getOne(doc *Value, ps string, opts PrintOpts) ([]byte, bool) {
	v, ok := getValue(doc, ps)
	if !ok {
		return nil, false
	}
	return SerializePretty(v, opts), true
}

// handleJSONMGet — JSON.MGET key [key ...] path
func handleJSONMGet(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 2 {
		w.WriteWrongNumArguments("json.mget")
		return
	}
	for _, k := range cmd.Args[:len(cmd.Args)-1] {
		keys = append(keys, string(k))
	}
	path := string(cmd.Args[len(cmd.Args)-1])
	valid = true
	runner = func() {
		w.WriteArray(len(keys))
		for _, k := range keys {
			root, _, exists, wrongType, err := getJSONSnapshot(cmd, k)
			if err != nil {
				// The array header is already on the wire, so the failure has to
				// ride as this element rather than replace the whole reply.
				w.WriteError(err.Error())
				continue
			}
			if !exists || wrongType {
				w.WriteNullBulkString()
				continue
			}
			v, ok := getValue(assemble(root, ""), path)
			if !ok {
				w.WriteNullBulkString()
				continue
			}
			w.WriteBulkString(Serialize(v))
		}
	}
	return
}

// handleJSONMSet — JSON.MSET key path value [key path value ...]
func handleJSONMSet(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 3 || len(cmd.Args)%3 != 0 {
		w.WriteWrongNumArguments("json.mset")
		return
	}
	for i := 0; i < len(cmd.Args); i += 3 {
		keys = append(keys, string(cmd.Args[i]))
	}
	valid = true
	runner = func() {
		type item struct {
			key    string
			path   *Path
			val    *Value
			root   *pb.ReducedEffect
			tips   []effects.Tip
			exists bool
		}
		items := make([]item, 0, len(cmd.Args)/3)
		for i := 0; i < len(cmd.Args); i += 3 {
			key := string(cmd.Args[i])
			path, err := ParsePath(string(cmd.Args[i+1]))
			if err != nil {
				w.WriteError("ERR " + err.Error())
				return
			}
			val, err := Parse(cmd.Args[i+2])
			if err != nil {
				w.WriteError("ERR invalid JSON")
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
			if !exists && !path.IsRoot() {
				w.WriteError("ERR new objects must be created at the root")
				return
			}
			items = append(items, item{key, path, val, root, tips, exists})
		}

		// Atomic across all triplets: a transactional bind commits them together.
		cmd.Context.BeginTx()
		plainEmit := func(e *pb.Effect) error { return cmd.Context.Emit(e) }
		for _, it := range items {
			// Record the read for this key (Noop carries its tips).
			if err := cmd.Context.Emit(&pb.Effect{
				Key:  []byte(it.key),
				Kind: &pb.Effect_Noop{Noop: &pb.NoopEffect{}},
			}, it.tips); err != nil {
				w.WriteError(err.Error())
				return
			}
			applied, err := setValueAtPath(plainEmit, it.root, it.key, it.path, it.val, it.exists)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if !applied {
				w.WriteError("ERR JSON path could not be set")
				return
			}
		}
		w.WriteOK()
	}
	return
}

// handleJSONMerge — JSON.MERGE key path value  (RFC 7396 merge patch)
func handleJSONMerge(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) != 3 {
		w.WriteWrongNumArguments("json.merge")
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
		patch, err := Parse(cmd.Args[2])
		if err != nil {
			w.WriteError("ERR invalid JSON")
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
		if !exists && !path.IsRoot() {
			w.WriteError("ERR new objects must be created at the root")
			return
		}

		emit := tipEmitter(cmd, tips)
		// A concrete path merges into (or creates) its single location; a
		// multi-match path merges into every existing match.
		if path.isConcrete() {
			var cur *Value
			if exists {
				if m := path.resolve(assemble(root, "")); len(m) > 0 {
					cur = m[0]
				}
			}
			merged := mergePatch(cur, patch)
			applied, err := setValueAtPath(emit, root, key, path, merged, exists)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if !applied {
				w.WriteNullBulkString()
				return
			}
			w.WriteOK()
			return
		}
		for _, t := range writeTargets(root, path) {
			merged := mergePatch(t.val, patch)
			applied, err := setValueAtPath(emit, root, key, t.path, merged, exists)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if !applied {
				w.WriteNullBulkString()
				return
			}
		}
		w.WriteOK()
	}
	return
}

// mergePatch applies an RFC 7396 merge patch to target, returning the result.
// A non-object patch replaces wholesale; null members delete; nested objects
// merge recursively.
func mergePatch(target, patch *Value) *Value {
	if patch == nil || patch.Kind != KindObject {
		return patch
	}
	var out *Value
	if target != nil && target.Kind == KindObject {
		out = target.DeepCopy()
	} else {
		out = &Value{Kind: KindObject}
	}
	for _, m := range patch.Obj {
		if m.Val.Kind == KindNull {
			out.objDelete(m.Key)
			continue
		}
		existing, _ := out.objGet(m.Key)
		out.objSet(m.Key, mergePatch(existing, m.Val))
	}
	return out
}

// handleJSONDel — JSON.DEL / JSON.FORGET key [path]
func handleJSONDel(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 || len(cmd.Args) > 2 {
		w.WriteWrongNumArguments("json.del")
		return
	}
	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true
	runner = func() {
		pathStr := "$"
		if len(cmd.Args) == 2 {
			pathStr = string(cmd.Args[1])
		}
		path, err := ParsePath(pathStr)
		if err != nil {
			w.WriteError("ERR " + err.Error())
			return
		}
		root, tips, exists, wrongType, err := getJSONSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteInteger(0)
			return
		}
		if wrongType {
			writeWrongType(w)
			return
		}

		emit := tipEmitter(cmd, tips)
		if path.IsRoot() {
			if err := emit(keyDelEffect(key)); err != nil {
				w.WriteError(err.Error())
				return
			}
			w.WriteInteger(1)
			return
		}
		// Remove every matched location; reply the count actually removed. Each
		// removal is by element id, so it is unaffected by the others.
		count := 0
		for _, t := range writeTargets(root, path) {
			segs := t.path.segs
			parentVid, parent, ok := walkToParent(root, segs[:len(segs)-1])
			if !ok {
				continue
			}
			last := segs[len(segs)-1]
			var id []byte
			switch last.kind {
			case segKey:
				if parent.TypeTag != pb.ValueType_TYPE_JSON_OBJECT || elementByKey(parent, last.key) == nil {
					continue
				}
				id = []byte(last.key)
			case segIndex:
				if parent.TypeTag != pb.ValueType_TYPE_JSON_ARRAY {
					continue
				}
				el := elementByIndex(parent, last.idx)
				if el == nil {
					continue
				}
				id = el.GetData().GetId()
			}
			if err := emit(orderedRemoveEffect(key, parentVid, id)); err != nil {
				w.WriteError(err.Error())
				return
			}
			count++
		}
		w.WriteInteger(int64(count))
	}
	return
}

// handleJSONClear — JSON.CLEAR key [path]
func handleJSONClear(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 || len(cmd.Args) > 2 {
		w.WriteWrongNumArguments("json.clear")
		return
	}
	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true
	runner = func() {
		pathStr := "$"
		if len(cmd.Args) == 2 {
			pathStr = string(cmd.Args[1])
		}
		path, err := ParsePath(pathStr)
		if err != nil {
			w.WriteError("ERR " + err.Error())
			return
		}
		root, tips, exists, wrongType, err := getJSONSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteInteger(0)
			return
		}
		if wrongType {
			writeWrongType(w)
			return
		}
		emit := tipEmitter(cmd, tips)
		// Clear every matched container/number; reply the count actually cleared
		// (strings, booleans, and null are left untouched and not counted).
		count := 0
		for _, t := range writeTargets(root, path) {
			cleared, didClear := clearValue(t.val)
			if !didClear {
				continue
			}
			applied, err := setValueAtPath(emit, root, key, t.path, cleared, exists)
			if err != nil {
				w.WriteError(err.Error())
				return
			}
			if applied {
				count++
			}
		}
		w.WriteInteger(int64(count))
	}
	return
}

// clearValue computes the JSON.CLEAR result for a value: arrays/objects become
// empty, numbers become 0, strings/bools/null are unchanged.
func clearValue(v *Value) (*Value, bool) {
	switch v.Kind {
	case KindObject:
		return &Value{Kind: KindObject}, true
	case KindArray:
		return &Value{Kind: KindArray}, true
	case KindInt:
		return &Value{Kind: KindInt, Int: 0}, true
	case KindFloat:
		return &Value{Kind: KindFloat, Float: 0}, true
	}
	return v, false
}

// handleJSONType — JSON.TYPE key [path]
func handleJSONType(cmd *shared.Command, w *shared.Writer, db *shared.Database) (valid bool, keys []string, runner shared.CommandRunner) {
	if len(cmd.Args) < 1 || len(cmd.Args) > 2 {
		w.WriteWrongNumArguments("json.type")
		return
	}
	key := string(cmd.Args[0])
	keys = []string{key}
	valid = true
	runner = func() {
		pathStr := "$"
		if len(cmd.Args) == 2 {
			pathStr = string(cmd.Args[1])
		}
		path, err := ParsePath(pathStr)
		if err != nil {
			w.WriteError("ERR " + err.Error())
			return
		}
		root, _, exists, wrongType, err := getJSONSnapshot(cmd, key)
		if err != nil {
			w.WriteError(err.Error())
			return
		}
		if !exists {
			w.WriteNullBulkString()
			return
		}
		if wrongType {
			writeWrongType(w)
			return
		}
		matches := path.resolve(assemble(root, ""))
		if path.JSONPath {
			w.WriteArray(len(matches))
			for _, m := range matches {
				w.WriteBulkString([]byte(m.TypeName()))
			}
			return
		}
		if len(matches) == 0 {
			w.WriteNullBulkString()
			return
		}
		w.WriteBulkString([]byte(matches[0].TypeName()))
	}
	return
}
