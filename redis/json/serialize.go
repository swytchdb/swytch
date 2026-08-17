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

import "strconv"

// PrintOpts controls JSON.GET pretty-printing. All-empty == compact.
type PrintOpts struct {
	Indent  string // inserted once per nesting level
	Newline string // inserted after the opening brace/bracket and each element
	Space   string // inserted after ':' in objects
}

// Serialize renders v to canonical compact JSON bytes. This is the form stored
// in scalar leaf partitions and returned for legacy single-value replies.
func Serialize(v *Value) []byte {
	return appendValue(nil, v)
}

// SerializePretty renders v with JSON.GET formatting options. With an empty
// PrintOpts it is byte-identical to Serialize.
func SerializePretty(v *Value, opts PrintOpts) []byte {
	if opts.Indent == "" && opts.Newline == "" && opts.Space == "" {
		return appendValue(nil, v)
	}
	return appendPretty(nil, v, opts, 0)
}

func appendValue(b []byte, v *Value) []byte {
	if v == nil {
		return append(b, "null"...)
	}
	switch v.Kind {
	case KindNull:
		return append(b, "null"...)
	case KindBool:
		if v.Bool {
			return append(b, "true"...)
		}
		return append(b, "false"...)
	case KindInt:
		return strconv.AppendInt(b, v.Int, 10)
	case KindFloat:
		return strconv.AppendFloat(b, v.Float, 'g', -1, 64)
	case KindString:
		return appendString(b, v.Str)
	case KindArray:
		b = append(b, '[')
		for i, e := range v.Arr {
			if i > 0 {
				b = append(b, ',')
			}
			b = appendValue(b, e)
		}
		return append(b, ']')
	case KindObject:
		b = append(b, '{')
		for i, m := range v.Obj {
			if i > 0 {
				b = append(b, ',')
			}
			b = appendString(b, m.Key)
			b = append(b, ':')
			b = appendValue(b, m.Val)
		}
		return append(b, '}')
	}
	return b
}

func appendPretty(b []byte, v *Value, opts PrintOpts, depth int) []byte {
	if v == nil {
		return append(b, "null"...)
	}
	switch v.Kind {
	case KindArray:
		if len(v.Arr) == 0 {
			return append(b, "[]"...)
		}
		b = append(b, '[')
		for i, e := range v.Arr {
			if i > 0 {
				b = append(b, ',')
			}
			b = append(b, opts.Newline...)
			b = appendIndent(b, opts.Indent, depth+1)
			b = appendPretty(b, e, opts, depth+1)
		}
		b = append(b, opts.Newline...)
		b = appendIndent(b, opts.Indent, depth)
		return append(b, ']')
	case KindObject:
		if len(v.Obj) == 0 {
			return append(b, "{}"...)
		}
		b = append(b, '{')
		for i, m := range v.Obj {
			if i > 0 {
				b = append(b, ',')
			}
			b = append(b, opts.Newline...)
			b = appendIndent(b, opts.Indent, depth+1)
			b = appendString(b, m.Key)
			b = append(b, ':')
			b = append(b, opts.Space...)
			b = appendPretty(b, m.Val, opts, depth+1)
		}
		b = append(b, opts.Newline...)
		b = appendIndent(b, opts.Indent, depth)
		return append(b, '}')
	default:
		return appendValue(b, v)
	}
}

func appendIndent(b []byte, indent string, depth int) []byte {
	for i := 0; i < depth; i++ {
		b = append(b, indent...)
	}
	return b
}

const hexDigits = "0123456789abcdef"

// appendString writes a JSON-escaped, double-quoted string. Matches standard
// JSON escaping (no HTML escaping of <, >, &, unlike encoding/json's default).
func appendString(b []byte, s string) []byte {
	b = append(b, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"':
			b = append(b, '\\', '"')
		case c == '\\':
			b = append(b, '\\', '\\')
		case c == '\n':
			b = append(b, '\\', 'n')
		case c == '\r':
			b = append(b, '\\', 'r')
		case c == '\t':
			b = append(b, '\\', 't')
		case c == '\b':
			b = append(b, '\\', 'b')
		case c == '\f':
			b = append(b, '\\', 'f')
		case c < 0x20:
			b = append(b, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
		default:
			b = append(b, c)
		}
	}
	return append(b, '"')
}
