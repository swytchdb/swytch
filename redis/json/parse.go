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
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strconv"
)

// errInvalidJSON is the stable parse error. RedisJSON emits many specific
// messages; v1 uses a single ERR-prefixed message (callers wrap as needed).
var errInvalidJSON = errors.New("invalid JSON")

// Parse decodes JSON bytes into the ordered model. It streams tokens via
// encoding/json so object key order is preserved (json.Unmarshal into a map
// would lose it), and uses UseNumber so integers vs floats are classified
// without float64 precision loss on large integers.
func Parse(data []byte) (*Value, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	v, err := parseValue(dec)
	if err != nil {
		return nil, errInvalidJSON
	}
	// Reject trailing content after the top-level value.
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errInvalidJSON
	}
	return v, nil
}

func parseValue(dec *json.Decoder) (*Value, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	return parseFromToken(dec, tok)
}

func parseFromToken(dec *json.Decoder, tok json.Token) (*Value, error) {
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			obj := &Value{Kind: KindObject}
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return nil, err
				}
				key, ok := keyTok.(string)
				if !ok {
					return nil, errInvalidJSON
				}
				val, err := parseValue(dec)
				if err != nil {
					return nil, err
				}
				obj.Obj = append(obj.Obj, Member{Key: key, Val: val})
			}
			if _, err := dec.Token(); err != nil { // consume '}'
				return nil, err
			}
			return obj, nil
		case '[':
			arr := &Value{Kind: KindArray}
			for dec.More() {
				val, err := parseValue(dec)
				if err != nil {
					return nil, err
				}
				arr.Arr = append(arr.Arr, val)
			}
			if _, err := dec.Token(); err != nil { // consume ']'
				return nil, err
			}
			return arr, nil
		}
		return nil, errInvalidJSON
	case json.Number:
		s := t.String()
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return &Value{Kind: KindInt, Int: i}, nil
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, errInvalidJSON
		}
		return &Value{Kind: KindFloat, Float: f}, nil
	case string:
		return &Value{Kind: KindString, Str: t}, nil
	case bool:
		return &Value{Kind: KindBool, Bool: t}, nil
	case nil:
		return &Value{Kind: KindNull}, nil
	}
	return nil, errInvalidJSON
}
