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
	"strconv"
	"strings"
)

var (
	errBadPath = errors.New("invalid path")
)

type segKind uint8

const (
	segKey       segKind = iota // .name / ['name']  — concrete object member
	segIndex                    // [n]               — concrete array index
	segWildcard                 // .* / [*]           — every member / element
	segSlice                    // [start:end:step]
	segUnion                    // [a,b] / ['x','y']  — index and/or key union
	segRecursive                // ..                — descent: match rest at every depth
	segFilter                   // [?(expr)]
)

// sliceSpec is a parsed [start:end:step]; the has* flags distinguish an omitted
// bound (use the default) from an explicit 0.
type sliceSpec struct {
	start, end, step           int
	hasStart, hasEnd, hasStep  bool
}

// unionSel is one selector of a union: a key or an index.
type unionSel struct {
	isKey bool
	key   string
	idx   int
}

type segment struct {
	kind   segKind
	key    string      // segKey
	idx    int          // segIndex (may be negative = from end)
	slice  sliceSpec    // segSlice
	union  []unionSel   // segUnion
	filter *filterExpr  // segFilter
}

// concrete reports whether the segment is a single fixed location (segKey or
// segIndex) — the only kinds that appear in a resolved match's normalized path.
func (s segment) concrete() bool { return s.kind == segKey || s.kind == segIndex }

// Path is a parsed JSON path. JSONPath ($-prefixed) resolves to all matches and
// is reported as an array; legacy (leading '.' or bare) resolves to the first
// match and is reported bare.
type Path struct {
	JSONPath bool
	segs     []segment
}

// IsRoot reports whether the path addresses the document root.
func (p *Path) IsRoot() bool { return len(p.segs) == 0 }

// isConcrete reports whether every segment is a single fixed location, so the
// path addresses exactly one (possibly not-yet-existing) location. Only concrete
// paths can create a new member; multi-match paths (wildcard, slice, union,
// descent, filter) act on existing matches only.
func (p *Path) isConcrete() bool {
	for _, s := range p.segs {
		if !s.concrete() {
			return false
		}
	}
	return true
}

// ParsePath parses a RedisJSON path in either dialect. Both dialects share the
// full grammar — object/array steps, wildcard (* / [*]), array slice
// ([start:end:step]), union ([a,b] / ['x','y']), recursive descent (..), and
// filter ([?(@.x>1)]). The dialects differ only in how the resolved matches are
// reported (JSONPath → array of all; legacy → the first match bare).
func ParsePath(s string) (*Path, error) {
	p := &Path{}
	i := 0
	if len(s) > 0 && s[0] == '$' {
		p.JSONPath = true
		i = 1
	}
	for i < len(s) {
		switch s[i] {
		case '.':
			// Recursive descent: ".." selects the following step at every depth.
			if i+1 < len(s) && s[i+1] == '.' {
				p.segs = append(p.segs, segment{kind: segRecursive})
				i += 2
				if i < len(s) && s[i] == '[' {
					seg, next, err := parseBracket(s, i+1)
					if err != nil {
						return nil, err
					}
					p.segs = append(p.segs, seg)
					i = next
					continue
				}
				seg, next, err := parseName(s, i)
				if err != nil {
					return nil, err
				}
				p.segs = append(p.segs, seg)
				i = next
				continue
			}
			i++
			if i >= len(s) {
				break // lone/trailing '.' (legacy root indicator) — no segment
			}
			if s[i] == '[' {
				seg, next, err := parseBracket(s, i+1)
				if err != nil {
					return nil, err
				}
				p.segs = append(p.segs, seg)
				i = next
				continue
			}
			seg, next, err := parseName(s, i)
			if err != nil {
				return nil, err
			}
			p.segs = append(p.segs, seg)
			i = next
		case '[':
			seg, next, err := parseBracket(s, i+1)
			if err != nil {
				return nil, err
			}
			p.segs = append(p.segs, seg)
			i = next
		default:
			// Legacy bare leading segment (no '$' / '.').
			seg, next, err := parseName(s, i)
			if err != nil {
				return nil, err
			}
			p.segs = append(p.segs, seg)
			i = next
		}
	}
	return p, nil
}

// parseName scans an unquoted member step starting at i (just past a '.' or at a
// bare leading position), stopping at the next '.' or '['. A lone '*' is a
// wildcard.
func parseName(s string, i int) (segment, int, error) {
	start := i
	for i < len(s) && s[i] != '.' && s[i] != '[' {
		i++
	}
	if i == start {
		return segment{}, 0, errBadPath
	}
	name := s[start:i]
	if name == "*" {
		return segment{kind: segWildcard}, i, nil
	}
	return segment{kind: segKey, key: name}, i, nil
}

// parseBracket parses the contents of a '[...]' starting at i (just past '[')
// and returns the segment and the index just past the closing ']'. Handles
// quoted keys, indices, wildcard, slices, unions, and filters.
func parseBracket(s string, i int) (segment, int, error) {
	if i >= len(s) {
		return segment{}, 0, errBadPath
	}
	if s[i] == '?' {
		return parseFilterBracket(s, i)
	}
	inner, next, err := scanBracketInner(s, i)
	if err != nil {
		return segment{}, 0, err
	}
	seg, err := classifyBracket(inner)
	if err != nil {
		return segment{}, 0, err
	}
	return seg, next, nil
}

// scanBracketInner returns the raw text between '[' (i points just past it) and
// the matching ']', respecting quotes, and the index just past ']'.
func scanBracketInner(s string, i int) (string, int, error) {
	start := i
	var quote byte
	for i < len(s) {
		c := s[i]
		if quote != 0 {
			if c == quote {
				quote = 0
			}
			i++
			continue
		}
		switch c {
		case '\'', '"':
			quote = c
		case ']':
			return s[start:i], i + 1, nil
		}
		i++
	}
	return "", 0, errBadPath
}

// classifyBracket turns the raw inner text of a non-filter bracket into a
// segment: wildcard, slice (a ':'), union (a ','), quoted key, or index.
func classifyBracket(inner string) (segment, error) {
	t := strings.TrimSpace(inner)
	if t == "*" {
		return segment{kind: segWildcard}, nil
	}
	if hasTopLevel(t, ':') {
		sl, err := parseSlice(t)
		if err != nil {
			return segment{}, err
		}
		return segment{kind: segSlice, slice: sl}, nil
	}
	if hasTopLevel(t, ',') {
		u, err := parseUnion(t)
		if err != nil {
			return segment{}, err
		}
		return segment{kind: segUnion, union: u}, nil
	}
	if k, ok := unquote(t); ok {
		return segment{kind: segKey, key: k}, nil
	}
	n, err := strconv.Atoi(t)
	if err != nil {
		return segment{}, errBadPath
	}
	return segment{kind: segIndex, idx: n}, nil
}

// hasTopLevel reports whether c occurs in t outside of any quotes.
func hasTopLevel(t string, c byte) bool {
	var quote byte
	for i := 0; i < len(t); i++ {
		switch {
		case quote != 0:
			if t[i] == quote {
				quote = 0
			}
		case t[i] == '\'' || t[i] == '"':
			quote = t[i]
		case t[i] == c:
			return true
		}
	}
	return false
}

// unquote strips a single matching pair of quotes; ok=false if t isn't quoted.
func unquote(t string) (string, bool) {
	if len(t) >= 2 && (t[0] == '\'' || t[0] == '"') && t[len(t)-1] == t[0] {
		return t[1 : len(t)-1], true
	}
	return "", false
}

func parseSlice(t string) (sliceSpec, error) {
	parts := strings.Split(t, ":")
	if len(parts) > 3 {
		return sliceSpec{}, errBadPath
	}
	var sl sliceSpec
	set := func(raw string, n *int, has *bool) error {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return nil
		}
		v, err := strconv.Atoi(raw)
		if err != nil {
			return errBadPath
		}
		*n, *has = v, true
		return nil
	}
	if err := set(parts[0], &sl.start, &sl.hasStart); err != nil {
		return sliceSpec{}, err
	}
	if len(parts) >= 2 {
		if err := set(parts[1], &sl.end, &sl.hasEnd); err != nil {
			return sliceSpec{}, err
		}
	}
	if len(parts) == 3 {
		if err := set(parts[2], &sl.step, &sl.hasStep); err != nil {
			return sliceSpec{}, err
		}
	}
	return sl, nil
}

func parseUnion(t string) ([]unionSel, error) {
	parts := splitTopLevel(t, ',')
	sels := make([]unionSel, 0, len(parts))
	for _, raw := range parts {
		raw = strings.TrimSpace(raw)
		if k, ok := unquote(raw); ok {
			sels = append(sels, unionSel{isKey: true, key: k})
			continue
		}
		n, err := strconv.Atoi(raw)
		if err != nil {
			return nil, errBadPath
		}
		sels = append(sels, unionSel{idx: n})
	}
	return sels, nil
}

// splitTopLevel splits t on c outside of quotes.
func splitTopLevel(t string, c byte) []string {
	var out []string
	var quote byte
	start := 0
	for i := 0; i < len(t); i++ {
		switch {
		case quote != 0:
			if t[i] == quote {
				quote = 0
			}
		case t[i] == '\'' || t[i] == '"':
			quote = t[i]
		case t[i] == c:
			out = append(out, t[start:i])
			start = i + 1
		}
	}
	return append(out, t[start:])
}

// match is one resolved location: its value, and the normalized concrete path
// (segKey/segIndex only) that addresses it — the latter drives writes via
// walkToParent.
type match struct {
	val  *Value
	segs []segment
}

// resolveMatches evaluates the path against an assembled value tree, returning
// every matched node in document order with its concrete path. A pre-order walk
// (match-here, then descend in order) reproduces RedisJSON's match ordering.
func (p *Path) resolveMatches(root *Value) []match {
	var out []match
	var rec func(v *Value, concrete, segs []segment)
	rec = func(v *Value, concrete, segs []segment) {
		if v == nil {
			return
		}
		if len(segs) == 0 {
			out = append(out, match{val: v, segs: concrete})
			return
		}
		s, rest := segs[0], segs[1:]
		switch s.kind {
		case segKey:
			if v.Kind == KindObject {
				if cv, ok := v.objGet(s.key); ok {
					rec(cv, appendKey(concrete, s.key), rest)
				}
			}
		case segIndex:
			if v.Kind == KindArray {
				if i, ok := normIndex(s.idx, len(v.Arr)); ok {
					rec(v.Arr[i], appendIdx(concrete, i), rest)
				}
			}
		case segWildcard:
			switch v.Kind {
			case KindObject:
				for _, m := range v.Obj {
					rec(m.Val, appendKey(concrete, m.Key), rest)
				}
			case KindArray:
				for i, e := range v.Arr {
					rec(e, appendIdx(concrete, i), rest)
				}
			}
		case segSlice:
			if v.Kind == KindArray {
				for _, i := range sliceIndices(s.slice, len(v.Arr)) {
					rec(v.Arr[i], appendIdx(concrete, i), rest)
				}
			}
		case segUnion:
			for _, u := range s.union {
				if u.isKey {
					if v.Kind == KindObject {
						if cv, ok := v.objGet(u.key); ok {
							rec(cv, appendKey(concrete, u.key), rest)
						}
					}
				} else if v.Kind == KindArray {
					if i, ok := normIndex(u.idx, len(v.Arr)); ok {
						rec(v.Arr[i], appendIdx(concrete, i), rest)
					}
				}
			}
		case segRecursive:
			var walk func(n *Value, c []segment)
			walk = func(n *Value, c []segment) {
				rec(n, c, rest)
				switch n.Kind {
				case KindObject:
					for _, m := range n.Obj {
						walk(m.Val, appendKey(c, m.Key))
					}
				case KindArray:
					for i, e := range n.Arr {
						walk(e, appendIdx(c, i))
					}
				}
			}
			walk(v, concrete)
		case segFilter:
			switch v.Kind {
			case KindArray:
				for i, e := range v.Arr {
					if s.filter.test(e) {
						rec(e, appendIdx(concrete, i), rest)
					}
				}
			case KindObject:
				for _, m := range v.Obj {
					if s.filter.test(m.Val) {
						rec(m.Val, appendKey(concrete, m.Key), rest)
					}
				}
			}
		}
	}
	rec(root, nil, p.segs)
	return out
}

// resolve evaluates the path and returns just the matched values in document
// order (the read path; writes use resolveMatches for the concrete paths).
func (p *Path) resolve(root *Value) []*Value {
	ms := p.resolveMatches(root)
	vs := make([]*Value, len(ms))
	for i := range ms {
		vs[i] = ms[i].val
	}
	return vs
}

// appendKey / appendIdx return a fresh concrete-segment slice (no aliasing
// across sibling recursions).
func appendKey(c []segment, k string) []segment {
	n := make([]segment, len(c)+1)
	copy(n, c)
	n[len(c)] = segment{kind: segKey, key: k}
	return n
}

func appendIdx(c []segment, idx int) []segment {
	n := make([]segment, len(c)+1)
	copy(n, c)
	n[len(c)] = segment{kind: segIndex, idx: idx}
	return n
}

// normIndex maps a possibly-negative index into [0,n); ok=false if out of range.
func normIndex(idx, n int) (int, bool) {
	if idx < 0 {
		idx += n
	}
	if idx < 0 || idx >= n {
		return 0, false
	}
	return idx, true
}

// sliceIndices expands a [start:end:step] slice over a length-n array into the
// concrete indices it selects, in iteration order.
func sliceIndices(s sliceSpec, n int) []int {
	step := 1
	if s.hasStep {
		step = s.step
	}
	if step == 0 {
		return nil
	}
	clamp := func(v, lo, hi int) int {
		if v < lo {
			return lo
		}
		if v > hi {
			return hi
		}
		return v
	}
	var out []int
	if step > 0 {
		start, end := 0, n
		if s.hasStart {
			start = s.start
			if start < 0 {
				start += n
			}
			start = clamp(start, 0, n)
		}
		if s.hasEnd {
			end = s.end
			if end < 0 {
				end += n
			}
			end = clamp(end, 0, n)
		}
		for i := start; i < end; i += step {
			out = append(out, i)
		}
		return out
	}
	// Negative step: iterate downward; bounds default to the array ends.
	start, end := n-1, -1
	if s.hasStart {
		start = s.start
		if start < 0 {
			start += n
		}
		start = clamp(start, -1, n-1)
	}
	if s.hasEnd {
		end = s.end
		if end < 0 {
			end += n
		}
		end = clamp(end, -1, n-1)
	}
	for i := start; i > end; i += step {
		if i >= 0 && i < n {
			out = append(out, i)
		}
	}
	return out
}
