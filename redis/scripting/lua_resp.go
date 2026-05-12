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

package scripting

import (
	"bytes"
	"strconv"

	"github.com/swytchdb/swytch/redis/shared"
	lua "github.com/yuin/gopher-lua"
)

// luaToRESP converts a Lua value to RESP format and writes it to the writer.
// Conversion rules:
// - string -> Bulk String
// - number (integer) -> Integer
// - number (float) -> Bulk String
// - table (array) -> Array
// - table with "err" key -> Error
// - table with "ok" key -> Simple String
// - nil/false -> Null
// - true -> Integer 1
func luaToRESP(L *lua.LState, val lua.LValue, w *shared.Writer) {
	luaToRESPWithDepth(L, val, w, 0)
}

// maxLuaToRESPDepth is the maximum recursion depth for converting Lua values to RESP.
// This prevents stack overflow from circular references.
const maxLuaToRESPDepth = 1000

func luaToRESPWithDepth(L *lua.LState, val lua.LValue, w *shared.Writer, depth int) {
	if depth > maxLuaToRESPDepth {
		w.WriteError("ERR reached lua stack limit")
		return
	}

	switch v := val.(type) {
	case *lua.LNilType:
		w.WriteNullBulkString()

	case lua.LBool:
		if v {
			w.WriteOne()
		} else {
			w.WriteNullBulkString()
		}

	case lua.LNumber:
		// Redis converts all Lua numbers to integers (truncated) in RESP2
		// This matches Redis behavior for script return values
		w.WriteInteger(int64(v))

	case lua.LString:
		w.WriteBulkString([]byte(string(v)))

	case *lua.LTable:
		// Check for special tables: {err=...}, {ok=...}, {double=...}
		if errVal := v.RawGetString("err"); errVal != lua.LNil {
			if errStr, ok := errVal.(lua.LString); ok {
				w.WriteError(string(errStr))
				return
			}
		}
		if okVal := v.RawGetString("ok"); okVal != lua.LNil {
			if okStr, ok := okVal.(lua.LString); ok {
				w.WriteSimpleString(string(okStr))
				return
			}
		}
		// {double = number} returns a floating-point value
		// In RESP3 this is a double type, in RESP2 it's a bulk string
		if doubleVal := v.RawGetString("double"); doubleVal != lua.LNil {
			if num, ok := doubleVal.(lua.LNumber); ok {
				w.WriteDouble(float64(num))
				return
			}
		}

		// Regular array table
		length := v.Len()
		w.WriteArray(length)
		for i := 1; i <= length; i++ {
			luaToRESPWithDepth(L, v.RawGetInt(i), w, depth+1)
		}

	default:
		// Unknown type -> null
		w.WriteNullBulkString()
	}
}

// respToLua parses RESP data from a buffer and returns the corresponding Lua value.
// Conversion rules:
// - Simple String -> string
// - Bulk String -> string (nil bulk -> false)
// - Integer -> number
// - Array -> table (nil array -> false)
// - Error -> table {err=message}
func respToLua(L *lua.LState, data []byte) (lua.LValue, int, error) {
	if len(data) == 0 {
		return lua.LNil, 0, nil
	}

	switch data[0] {
	case shared.RESPSimpleString: // +
		end := bytes.Index(data, []byte("\r\n"))
		if end == -1 {
			return lua.LNil, 0, nil
		}
		// Redis converts status replies to tables with {ok=status}
		tbl := L.NewTable()
		tbl.RawSetString("ok", lua.LString(data[1:end]))
		return tbl, end + 2, nil

	case shared.RESPError: // -
		end := bytes.Index(data, []byte("\r\n"))
		if end == -1 {
			return lua.LNil, 0, nil
		}
		// Return as table {err=message}
		tbl := L.NewTable()
		tbl.RawSetString("err", lua.LString(data[1:end]))
		return tbl, end + 2, nil

	case shared.RESPInteger: // :
		end := bytes.Index(data, []byte("\r\n"))
		if end == -1 {
			return lua.LNil, 0, nil
		}
		n, err := strconv.ParseInt(string(data[1:end]), 10, 64)
		if err != nil {
			return lua.LNil, 0, err
		}
		return lua.LNumber(n), end + 2, nil

	case shared.RESPBulkString: // $
		end := bytes.Index(data, []byte("\r\n"))
		if end == -1 {
			return lua.LNil, 0, nil
		}
		length, err := strconv.Atoi(string(data[1:end]))
		if err != nil {
			return lua.LNil, 0, err
		}
		if length == -1 {
			// Null bulk string -> false (Redis Lua convention)
			return lua.LFalse, end + 2, nil
		}
		start := end + 2
		if start+length+2 > len(data) {
			return lua.LNil, 0, nil
		}
		return lua.LString(data[start : start+length]), start + length + 2, nil

	case shared.RESPArray: // *
		end := bytes.Index(data, []byte("\r\n"))
		if end == -1 {
			return lua.LNil, 0, nil
		}
		count, err := strconv.Atoi(string(data[1:end]))
		if err != nil {
			return lua.LNil, 0, err
		}
		if count == -1 {
			// Null array -> false (Redis Lua convention)
			return lua.LFalse, end + 2, nil
		}

		tbl := L.NewTable()
		offset := end + 2
		for i := range count {
			val, consumed, err := respToLua(L, data[offset:])
			if err != nil {
				return lua.LNil, 0, err
			}
			offset += consumed
			tbl.RawSetInt(i+1, val)
		}
		return tbl, offset, nil

	case shared.RESP3Null: // _
		end := bytes.Index(data, []byte("\r\n"))
		if end == -1 {
			return lua.LNil, 0, nil
		}
		return lua.LFalse, end + 2, nil

	case shared.RESP3Boolean: // #
		end := bytes.Index(data, []byte("\r\n"))
		if end == -1 {
			return lua.LNil, 0, nil
		}
		if len(data) > 1 && data[1] == 't' {
			return lua.LTrue, end + 2, nil
		}
		return lua.LFalse, end + 2, nil

	case shared.RESP3Double: // ,
		end := bytes.Index(data, []byte("\r\n"))
		if end == -1 {
			return lua.LNil, 0, nil
		}
		f, err := strconv.ParseFloat(string(data[1:end]), 64)
		if err != nil {
			return lua.LNil, 0, err
		}
		return lua.LNumber(f), end + 2, nil

	case shared.RESP3Map: // %
		end := bytes.Index(data, []byte("\r\n"))
		if end == -1 {
			return lua.LNil, 0, nil
		}
		count, err := strconv.Atoi(string(data[1:end]))
		if err != nil {
			return lua.LNil, 0, err
		}

		tbl := L.NewTable()
		offset := end + 2
		for range count {
			key, consumed, err := respToLua(L, data[offset:])
			if err != nil {
				return lua.LNil, 0, err
			}
			offset += consumed

			val, consumed, err := respToLua(L, data[offset:])
			if err != nil {
				return lua.LNil, 0, err
			}
			offset += consumed
			tbl.RawSet(key, val)
		}
		return tbl, offset, nil

	default:
		return lua.LNil, 0, nil
	}
}

// luaArgsToBytes converts Lua arguments to byte slices for command execution.
func luaArgsToBytes(L *lua.LState, startArg int) [][]byte {
	nArgs := L.GetTop() - startArg + 1
	if nArgs <= 0 {
		return nil
	}

	args := make([][]byte, nArgs)
	for i := range nArgs {
		val := L.Get(startArg + i)
		switch v := val.(type) {
		case lua.LString:
			args[i] = []byte(string(v))
		case lua.LNumber:
			// Convert number to string representation
			f := float64(v)
			if f == float64(int64(f)) {
				args[i] = []byte(strconv.FormatInt(int64(f), 10))
			} else {
				args[i] = []byte(strconv.FormatFloat(f, 'f', -1, 64))
			}
		case lua.LBool:
			if v {
				args[i] = []byte("1")
			} else {
				args[i] = []byte("0")
			}
		default:
			args[i] = []byte(val.String())
		}
	}
	return args
}
