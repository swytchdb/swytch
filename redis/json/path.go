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
)

var (
	errBadPath         = errors.New("invalid path")
	errUnsupportedPath = errors.New("unsupported path expression")
)

type segKind uint8

const (
	segKey segKind = iota
	segIndex
)

type segment struct {
	kind segKind
	key  string // segKey
	idx  int    // segIndex (may be negative = from end)
}

// Path is a parsed JSON path. JSONPath ($-prefixed) resolves to all matches and
// is reported as an array; legacy (leading '.' or bare) resolves to the first
// match and is reported bare.
type Path struct {
	JSONPath bool
	segs     []segment
}

// IsRoot reports whether the path addresses the document root.
func (p *Path) IsRoot() bool { return len(p.segs) == 0 }

// ParsePath parses a RedisJSON path. v1 supports concrete object-member and
// array-index steps in both dialects: $, $.a.b, $['a'], $.a[0], .a.b, a.b,
// a[0]. Wildcards, recursive descent, slices, and filters are not yet
// supported (errUnsupportedPath).
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
			i++
			if i >= len(s) {
				break // lone/trailing '.' (legacy root indicator) — no segment
			}
			if s[i] == '.' {
				return nil, errUnsupportedPath // recursive descent
			}
			start := i
			for i < len(s) && s[i] != '.' && s[i] != '[' {
				i++
			}
			if i == start {
				return nil, errBadPath
			}
			key := s[start:i]
			if key == "*" {
				return nil, errUnsupportedPath
			}
			p.segs = append(p.segs, segment{kind: segKey, key: key})
		case '[':
			i++
			seg, next, err := parseBracket(s, i)
			if err != nil {
				return nil, err
			}
			p.segs = append(p.segs, seg)
			i = next
		default:
			// Legacy bare leading segment (no '$' / '.').
			start := i
			for i < len(s) && s[i] != '.' && s[i] != '[' {
				i++
			}
			p.segs = append(p.segs, segment{kind: segKey, key: s[start:i]})
		}
	}
	return p, nil
}

// parseBracket parses the contents of a '[...]' starting at i (just past '[')
// and returns the segment and the index just past the closing ']'.
func parseBracket(s string, i int) (segment, int, error) {
	if i >= len(s) {
		return segment{}, 0, errBadPath
	}
	if q := s[i]; q == '\'' || q == '"' {
		i++
		start := i
		for i < len(s) && s[i] != q {
			i++
		}
		if i >= len(s) {
			return segment{}, 0, errBadPath
		}
		key := s[start:i]
		i++ // closing quote
		if i >= len(s) || s[i] != ']' {
			return segment{}, 0, errBadPath
		}
		return segment{kind: segKey, key: key}, i + 1, nil
	}
	start := i
	for i < len(s) && s[i] != ']' {
		i++
	}
	if i >= len(s) {
		return segment{}, 0, errBadPath
	}
	tok := s[start:i]
	if tok == "*" {
		return segment{}, 0, errUnsupportedPath
	}
	n, err := strconv.Atoi(tok)
	if err != nil {
		return segment{}, 0, errBadPath
	}
	return segment{kind: segIndex, idx: n}, i + 1, nil
}

// resolve evaluates the path against an assembled value tree, returning the
// matched nodes in document order. For v1 concrete paths this is 0 or 1 match.
func (p *Path) resolve(root *Value) []*Value {
	cur := root
	for _, s := range p.segs {
		if cur == nil {
			return nil
		}
		switch s.kind {
		case segKey:
			if cur.Kind != KindObject {
				return nil
			}
			v, ok := cur.objGet(s.key)
			if !ok {
				return nil
			}
			cur = v
		case segIndex:
			if cur.Kind != KindArray {
				return nil
			}
			idx := s.idx
			if idx < 0 {
				idx += len(cur.Arr)
			}
			if idx < 0 || idx >= len(cur.Arr) {
				return nil
			}
			cur = cur.Arr[idx]
		}
	}
	if cur == nil {
		return nil
	}
	return []*Value{cur}
}
