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
	"regexp"
	"strconv"
	"strings"
)

// filterExpr is a parsed [?(...)] predicate. test reports whether a candidate
// element (the value bound to '@') satisfies it.
type filterExpr struct {
	root predicate
}

func (f *filterExpr) test(cur *Value) bool {
	if f == nil || f.root == nil {
		return false
	}
	return f.root.test(cur)
}

// predicate is a boolean node of a filter expression tree.
type predicate interface {
	test(cur *Value) bool
}

type orPred struct{ l, r predicate }

func (p orPred) test(cur *Value) bool { return p.l.test(cur) || p.r.test(cur) }

type andPred struct{ l, r predicate }

func (p andPred) test(cur *Value) bool { return p.l.test(cur) && p.r.test(cur) }

// existsPred is a bare operand: true when it resolves to a present, non-null,
// non-false value (RedisJSON's existence test, e.g. [?(@.isbn)]).
type existsPred struct{ o operand }

func (p existsPred) test(cur *Value) bool {
	v, ok := p.o.value(cur)
	if !ok || v == nil || v.Kind == KindNull {
		return false
	}
	if v.Kind == KindBool {
		return v.Bool
	}
	return true
}

// cmpPred compares two operands. re is precompiled for the "=~" operator.
type cmpPred struct {
	op   string
	l, r operand
	re   *regexp.Regexp
}

func (p cmpPred) test(cur *Value) bool {
	lv, lok := p.l.value(cur)
	rv, rok := p.r.value(cur)
	if p.op == "=~" {
		if !lok || lv == nil || lv.Kind != KindString || p.re == nil {
			return false
		}
		return p.re.MatchString(lv.Str)
	}
	if !lok || !rok {
		// A missing operand satisfies only "!=" (present differs from absent).
		return p.op == "!=" && lok != rok
	}
	return compareValues(p.op, lv, rv)
}

// operand resolves to a value given the current element bound to '@'.
type operand interface {
	value(cur *Value) (*Value, bool)
}

// pathOperand is an '@'-rooted path into the current element (segKey/segIndex).
type pathOperand struct{ segs []segment }

func (o pathOperand) value(cur *Value) (*Value, bool) {
	v := cur
	for _, s := range o.segs {
		if v == nil {
			return nil, false
		}
		switch s.kind {
		case segKey:
			if v.Kind != KindObject {
				return nil, false
			}
			cv, ok := v.objGet(s.key)
			if !ok {
				return nil, false
			}
			v = cv
		case segIndex:
			if v.Kind != KindArray {
				return nil, false
			}
			i, ok := normIndex(s.idx, len(v.Arr))
			if !ok {
				return nil, false
			}
			v = v.Arr[i]
		default:
			return nil, false
		}
	}
	return v, v != nil
}

// litOperand is a constant (number, string, bool, or null).
type litOperand struct{ v *Value }

func (o litOperand) value(*Value) (*Value, bool) { return o.v, true }

// compareValues applies an ordering/equality operator. ==/!= use structural
// equality; ordering compares numbers numerically and strings lexically; mixed
// or non-comparable kinds yield false.
func compareValues(op string, l, r *Value) bool {
	switch op {
	case "==":
		return valueEqual(l, r)
	case "!=":
		return !valueEqual(l, r)
	}
	if isNumber(l) && isNumber(r) {
		a, b := numAsFloat(l), numAsFloat(r)
		switch op {
		case "<":
			return a < b
		case "<=":
			return a <= b
		case ">":
			return a > b
		case ">=":
			return a >= b
		}
		return false
	}
	if l.Kind == KindString && r.Kind == KindString {
		switch op {
		case "<":
			return l.Str < r.Str
		case "<=":
			return l.Str <= r.Str
		case ">":
			return l.Str > r.Str
		case ">=":
			return l.Str >= r.Str
		}
	}
	return false
}

// parseFilterBracket parses a "[?(...)]" starting at i (just past '['), pointing
// at '?'. Returns the filter segment and the index just past the closing ']'.
func parseFilterBracket(s string, i int) (segment, int, error) {
	if i+1 >= len(s) || s[i+1] != '(' {
		return segment{}, 0, errBadPath
	}
	// Scan from the '(' to its matching ')', respecting quotes.
	open := i + 1
	depth := 0
	var quote byte
	j := open
	for ; j < len(s); j++ {
		c := s[j]
		if quote != 0 {
			if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '\'', '"':
			quote = c
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				j++
				goto closed
			}
		}
	}
	return segment{}, 0, errBadPath
closed:
	if j >= len(s) || s[j] != ']' {
		return segment{}, 0, errBadPath
	}
	body := s[open+1 : j-1] // inside the outer parens
	expr, err := parseFilter(body)
	if err != nil {
		return segment{}, 0, err
	}
	return segment{kind: segFilter, filter: expr}, j + 1, nil
}

// --- filter expression recursive-descent parser ---

type filterParser struct {
	s   string
	pos int
}

func parseFilter(s string) (*filterExpr, error) {
	fp := &filterParser{s: s}
	p, err := fp.parseOr()
	if err != nil {
		return nil, err
	}
	fp.skipSpace()
	if fp.pos != len(fp.s) {
		return nil, errBadPath
	}
	return &filterExpr{root: p}, nil
}

func (fp *filterParser) skipSpace() {
	for fp.pos < len(fp.s) && (fp.s[fp.pos] == ' ' || fp.s[fp.pos] == '\t') {
		fp.pos++
	}
}

func (fp *filterParser) consume(tok string) bool {
	fp.skipSpace()
	if strings.HasPrefix(fp.s[fp.pos:], tok) {
		fp.pos += len(tok)
		return true
	}
	return false
}

func (fp *filterParser) parseOr() (predicate, error) {
	l, err := fp.parseAnd()
	if err != nil {
		return nil, err
	}
	for fp.consume("||") {
		r, err := fp.parseAnd()
		if err != nil {
			return nil, err
		}
		l = orPred{l, r}
	}
	return l, nil
}

func (fp *filterParser) parseAnd() (predicate, error) {
	l, err := fp.parseTerm()
	if err != nil {
		return nil, err
	}
	for fp.consume("&&") {
		r, err := fp.parseTerm()
		if err != nil {
			return nil, err
		}
		l = andPred{l, r}
	}
	return l, nil
}

// parseTerm is a parenthesized sub-expression or a comparison/existence test.
func (fp *filterParser) parseTerm() (predicate, error) {
	fp.skipSpace()
	if fp.pos < len(fp.s) && fp.s[fp.pos] == '(' {
		fp.pos++
		p, err := fp.parseOr()
		if err != nil {
			return nil, err
		}
		if !fp.consume(")") {
			return nil, errBadPath
		}
		return p, nil
	}
	l, err := fp.parseOperand()
	if err != nil {
		return nil, err
	}
	op := fp.parseOp()
	if op == "" {
		return existsPred{l}, nil
	}
	r, err := fp.parseOperand()
	if err != nil {
		return nil, err
	}
	pred := cmpPred{op: op, l: l, r: r}
	if op == "=~" {
		if lit, ok := r.(litOperand); ok && lit.v.Kind == KindString {
			re, err := regexp.Compile(lit.v.Str)
			if err != nil {
				return nil, errBadPath
			}
			pred.re = re
		} else {
			return nil, errBadPath
		}
	}
	return pred, nil
}

// parseOp reads a comparison operator, or "" if the next token isn't one.
func (fp *filterParser) parseOp() string {
	fp.skipSpace()
	for _, op := range []string{"==", "!=", "<=", ">=", "=~", "<", ">"} {
		if strings.HasPrefix(fp.s[fp.pos:], op) {
			fp.pos += len(op)
			return op
		}
	}
	return ""
}

func (fp *filterParser) parseOperand() (operand, error) {
	fp.skipSpace()
	if fp.pos >= len(fp.s) {
		return nil, errBadPath
	}
	c := fp.s[fp.pos]
	switch {
	case c == '@':
		return fp.parsePathOperand()
	case c == '\'' || c == '"':
		return fp.parseStringLiteral(c)
	case c == '-' || c == '+' || (c >= '0' && c <= '9'):
		return fp.parseNumberLiteral()
	case strings.HasPrefix(fp.s[fp.pos:], "true"):
		fp.pos += 4
		return litOperand{newBool(true)}, nil
	case strings.HasPrefix(fp.s[fp.pos:], "false"):
		fp.pos += 5
		return litOperand{newBool(false)}, nil
	case strings.HasPrefix(fp.s[fp.pos:], "null"):
		fp.pos += 4
		return litOperand{newNull()}, nil
	}
	return nil, errBadPath
}

// parsePathOperand parses "@" optionally followed by .name / ['name'] / [n]
// steps.
func (fp *filterParser) parsePathOperand() (operand, error) {
	fp.pos++ // '@'
	var segs []segment
	for fp.pos < len(fp.s) {
		c := fp.s[fp.pos]
		if c == '.' {
			fp.pos++
			start := fp.pos
			for fp.pos < len(fp.s) && !isFilterDelim(fp.s[fp.pos]) {
				fp.pos++
			}
			if fp.pos == start {
				return nil, errBadPath
			}
			segs = append(segs, segment{kind: segKey, key: fp.s[start:fp.pos]})
		} else if c == '[' {
			inner, next, err := scanBracketInner(fp.s, fp.pos+1)
			if err != nil {
				return nil, err
			}
			seg, err := classifyBracket(inner)
			if err != nil || !seg.concrete() {
				return nil, errBadPath
			}
			segs = append(segs, seg)
			fp.pos = next
		} else {
			break
		}
	}
	return pathOperand{segs}, nil
}

// isFilterDelim reports characters that end an unquoted '@'-path member.
func isFilterDelim(c byte) bool {
	switch c {
	case '.', '[', ']', ' ', '\t', '=', '!', '<', '>', '&', '|', ')', '~':
		return true
	}
	return false
}

func (fp *filterParser) parseStringLiteral(q byte) (operand, error) {
	fp.pos++ // opening quote
	start := fp.pos
	for fp.pos < len(fp.s) && fp.s[fp.pos] != q {
		fp.pos++
	}
	if fp.pos >= len(fp.s) {
		return nil, errBadPath
	}
	str := fp.s[start:fp.pos]
	fp.pos++ // closing quote
	return litOperand{newString(str)}, nil
}

func (fp *filterParser) parseNumberLiteral() (operand, error) {
	start := fp.pos
	for fp.pos < len(fp.s) {
		c := fp.s[fp.pos]
		if (c >= '0' && c <= '9') || c == '.' || c == '-' || c == '+' || c == 'e' || c == 'E' {
			fp.pos++
			continue
		}
		break
	}
	tok := fp.s[start:fp.pos]
	if i, err := strconv.ParseInt(tok, 10, 64); err == nil {
		return litOperand{newInt(i)}, nil
	}
	f, err := strconv.ParseFloat(tok, 64)
	if err != nil {
		return nil, errBadPath
	}
	return litOperand{newFloat(f)}, nil
}
