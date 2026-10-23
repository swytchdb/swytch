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
	"encoding/binary"
	"math"
	"unsafe"

	lua "github.com/yuin/gopher-lua"
)

// Struct library for Lua - implements binary packing/unpacking
// Compatible with Redis's struct library (based on Roberto Ierusalimschy's struct library)
// See: http://www.inf.puc-rio.br/~roberto/struct/

// structState holds the parsing state for format strings
type structState struct {
	fmt       string
	pos       int
	bigEndian bool
	align     int
}

// nativeEndian detects the native byte order of the system
var nativeEndian binary.ByteOrder

func init() {
	// Detect native endianness
	var x uint32 = 0x01020304
	if *(*byte)(unsafe.Pointer(&x)) == 0x01 {
		nativeEndian = binary.BigEndian
	} else {
		nativeEndian = binary.LittleEndian
	}
}

// OpenStruct opens the struct library and returns it as a Lua table
func OpenStruct(L *lua.LState) int {
	mod := L.NewTable()
	L.SetFuncs(mod, structFuncs)
	L.Push(mod)

	return 1
}

// LoaderStruct is a preload function for the struct library
func LoaderStruct(L *lua.LState) int {
	return OpenStruct(L)
}

var structFuncs = map[string]lua.LGFunction{
	"pack":   structPack,
	"unpack": structUnpack,
	"size":   structSize,
}

// getByteOrder returns the byte order based on the state
func (s *structState) getByteOrder() binary.ByteOrder {
	if s.bigEndian {
		return binary.BigEndian
	}

	return binary.LittleEndian
}

// nextChar returns the next character in the format string and advances position
func (s *structState) nextChar() (byte, bool) {
	for s.pos < len(s.fmt) {
		c := s.fmt[s.pos]
		s.pos++
		// Skip whitespace
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			continue
		}

		return c, true
	}

	return 0, false
}

// readNumber reads a number from the format string (for things like i4, c10, etc.)
func (s *structState) readNumber(defaultVal int) int {
	if s.pos >= len(s.fmt) {
		return defaultVal
	}

	// Check if there's a number
	if s.fmt[s.pos] < '0' || s.fmt[s.pos] > '9' {
		return defaultVal
	}

	n := 0
	for s.pos < len(s.fmt) && s.fmt[s.pos] >= '0' && s.fmt[s.pos] <= '9' {
		n = n*10 + int(s.fmt[s.pos]-'0')
		s.pos++
	}

	return n
}

// addPadding adds padding bytes to align to the given size
func addPadding(buf []byte, align int) []byte {
	if align <= 1 {
		return buf
	}

	offset := len(buf) % align
	if offset != 0 {
		padding := align - offset
		for range padding {
			buf = append(buf, 0)
		}
	}

	return buf
}

// calcPadding returns the number of padding bytes needed
func calcPadding(offset, align int) int {
	if align <= 1 {
		return 0
	}

	mod := offset % align
	if mod != 0 {
		return align - mod
	}

	return 0
}

// structPack implements struct.pack(fmt, d1, d2, ...)
func structPack(L *lua.LState) int {
	fmtStr := L.CheckString(1)
	argIdx := 2

	state := &structState{
		fmt:       fmtStr,
		pos:       0,
		bigEndian: false,
		align:     1,
	}

	var buf []byte

	for {
		c, ok := state.nextChar()
		if !ok {
			break
		}

		switch c {
		case '>':
			state.bigEndian = true
		case '<':
			state.bigEndian = false
		case '=':
			// Native endian
			state.bigEndian = nativeEndian == binary.BigEndian
		case '!':
			// Set alignment
			state.align = state.readNumber(int(unsafe.Sizeof(int(0))))

		case 'x':
			// Padding byte
			buf = append(buf, 0)

		case 'b':
			// Signed char
			buf = addPadding(buf, min(state.align, 1))
			val := L.CheckInt(argIdx)
			argIdx++

			buf = append(buf, byte(int8(val)))

		case 'B':
			// Unsigned char
			buf = addPadding(buf, min(state.align, 1))
			val := L.CheckInt(argIdx)
			argIdx++

			buf = append(buf, byte(val))

		case 'h':
			// Signed short (2 bytes)
			buf = addPadding(buf, min(state.align, 2))
			val := L.CheckInt(argIdx)
			argIdx++
			tmp := make([]byte, 2)
			state.getByteOrder().PutUint16(tmp, uint16(int16(val)))
			buf = append(buf, tmp...)

		case 'H':
			// Unsigned short (2 bytes)
			buf = addPadding(buf, min(state.align, 2))
			val := L.CheckInt(argIdx)
			argIdx++
			tmp := make([]byte, 2)
			state.getByteOrder().PutUint16(tmp, uint16(val))
			buf = append(buf, tmp...)

		case 'l':
			// Signed long (4 bytes in Redis/struct library)
			buf = addPadding(buf, min(state.align, 4))
			val := L.CheckInt64(argIdx)
			argIdx++
			tmp := make([]byte, 4)
			state.getByteOrder().PutUint32(tmp, uint32(int32(val)))
			buf = append(buf, tmp...)

		case 'L':
			// Unsigned long (4 bytes in Redis/struct library)
			buf = addPadding(buf, min(state.align, 4))
			val := L.CheckInt64(argIdx)
			argIdx++
			tmp := make([]byte, 4)
			state.getByteOrder().PutUint32(tmp, uint32(val))
			buf = append(buf, tmp...)

		case 'q':
			// Signed 64-bit integer
			buf = addPadding(buf, min(state.align, 8))
			val := L.CheckInt64(argIdx)
			argIdx++
			tmp := make([]byte, 8)
			state.getByteOrder().PutUint64(tmp, uint64(val))
			buf = append(buf, tmp...)

		case 'Q':
			// Unsigned 64-bit integer
			buf = addPadding(buf, min(state.align, 8))
			val := L.CheckInt64(argIdx)
			argIdx++
			tmp := make([]byte, 8)
			state.getByteOrder().PutUint64(tmp, uint64(val))
			buf = append(buf, tmp...)

		case 'T':
			// size_t (platform dependent, use 8 bytes for 64-bit)
			buf = addPadding(buf, min(state.align, 8))
			val := L.CheckInt64(argIdx)
			argIdx++
			tmp := make([]byte, 8)
			state.getByteOrder().PutUint64(tmp, uint64(val))
			buf = append(buf, tmp...)

		case 'i', 'I':
			// Signed/unsigned integer with optional size
			size := state.readNumber(4)
			if size < 1 || size > 8 {
				L.ArgError(1, "invalid integer size")

				return 0
			}

			buf = addPadding(buf, min(state.align, size))
			val := L.CheckInt64(argIdx)
			argIdx++
			tmp := make([]byte, 8)
			state.getByteOrder().PutUint64(tmp, uint64(val))

			if state.bigEndian {
				buf = append(buf, tmp[8-size:]...)
			} else {
				buf = append(buf, tmp[:size]...)
			}

		case 'f':
			// Float (4 bytes)
			buf = addPadding(buf, min(state.align, 4))
			val := L.CheckNumber(argIdx)
			argIdx++
			tmp := make([]byte, 4)
			state.getByteOrder().PutUint32(tmp, math.Float32bits(float32(val)))
			buf = append(buf, tmp...)

		case 'd':
			// Double (8 bytes)
			buf = addPadding(buf, min(state.align, 8))
			val := L.CheckNumber(argIdx)
			argIdx++
			tmp := make([]byte, 8)
			state.getByteOrder().PutUint64(tmp, math.Float64bits(float64(val)))
			buf = append(buf, tmp...)

		case 's':
			// Zero-terminated string
			str := L.CheckString(argIdx)
			argIdx++

			buf = append(buf, []byte(str)...)
			buf = append(buf, 0)

		case 'c':
			// Fixed-length char sequence
			size := state.readNumber(1)
			if size == 0 {
				// c0 means get length from previous value (already packed)
				// In pack, c0 means pack the entire string
				str := L.CheckString(argIdx)
				argIdx++

				buf = append(buf, []byte(str)...)
			} else {
				str := L.CheckString(argIdx)
				argIdx++

				data := []byte(str)
				if len(data) > size {
					data = data[:size]
				}

				buf = append(buf, data...)
				// Pad with zeros if string is shorter
				for range size - len(data) {
					buf = append(buf, 0)
				}
			}

		default:
			L.ArgError(1, "invalid format option '"+string(c)+"'")

			return 0
		}
	}

	L.Push(lua.LString(string(buf)))

	return 1
}

// structUnpack implements struct.unpack(fmt, s, [i])
func structUnpack(L *lua.LState) int {
	fmtStr := L.CheckString(1)
	data := []byte(L.CheckString(2))
	pos := L.OptInt(3, 1) - 1 // Lua is 1-indexed

	if pos < 0 || pos > len(data) {
		L.ArgError(3, "initial position out of bounds")

		return 0
	}

	state := &structState{
		fmt:       fmtStr,
		pos:       0,
		bigEndian: false,
		align:     1,
	}

	var results []lua.LValue

	var prevSize int // For c0

	for {
		c, ok := state.nextChar()
		if !ok {
			break
		}

		switch c {
		case '>':
			state.bigEndian = true
		case '<':
			state.bigEndian = false
		case '=':
			state.bigEndian = nativeEndian == binary.BigEndian
		case '!':
			state.align = state.readNumber(int(unsafe.Sizeof(int(0))))

		case 'x':
			// Skip padding byte
			pos++

		case 'b':
			// Signed char
			pos += calcPadding(pos, min(state.align, 1))
			if pos >= len(data) {
				L.ArgError(2, "data string too short")

				return 0
			}

			results = append(results, lua.LNumber(int8(data[pos])))
			pos++

		case 'B':
			// Unsigned char
			pos += calcPadding(pos, min(state.align, 1))
			if pos >= len(data) {
				L.ArgError(2, "data string too short")

				return 0
			}

			results = append(results, lua.LNumber(data[pos]))
			pos++

		case 'h':
			// Signed short
			pos += calcPadding(pos, min(state.align, 2))
			if pos+2 > len(data) {
				L.ArgError(2, "data string too short")

				return 0
			}

			val := state.getByteOrder().Uint16(data[pos:])
			results = append(results, lua.LNumber(int16(val)))
			pos += 2

		case 'H':
			// Unsigned short
			pos += calcPadding(pos, min(state.align, 2))
			if pos+2 > len(data) {
				L.ArgError(2, "data string too short")

				return 0
			}

			val := state.getByteOrder().Uint16(data[pos:])
			results = append(results, lua.LNumber(val))
			pos += 2

		case 'l':
			// Signed long (4 bytes)
			pos += calcPadding(pos, min(state.align, 4))
			if pos+4 > len(data) {
				L.ArgError(2, "data string too short")

				return 0
			}

			val := state.getByteOrder().Uint32(data[pos:])
			results = append(results, lua.LNumber(int32(val)))
			pos += 4

		case 'L':
			// Unsigned long (4 bytes)
			pos += calcPadding(pos, min(state.align, 4))
			if pos+4 > len(data) {
				L.ArgError(2, "data string too short")

				return 0
			}

			val := state.getByteOrder().Uint32(data[pos:])
			results = append(results, lua.LNumber(val))
			pos += 4

		case 'q':
			// Signed 64-bit
			pos += calcPadding(pos, min(state.align, 8))
			if pos+8 > len(data) {
				L.ArgError(2, "data string too short")

				return 0
			}

			val := state.getByteOrder().Uint64(data[pos:])
			results = append(results, lua.LNumber(int64(val)))
			pos += 8

		case 'Q':
			// Unsigned 64-bit
			pos += calcPadding(pos, min(state.align, 8))
			if pos+8 > len(data) {
				L.ArgError(2, "data string too short")

				return 0
			}

			val := state.getByteOrder().Uint64(data[pos:])
			results = append(results, lua.LNumber(val))
			pos += 8

		case 'T':
			// size_t (8 bytes on 64-bit)
			pos += calcPadding(pos, min(state.align, 8))
			if pos+8 > len(data) {
				L.ArgError(2, "data string too short")

				return 0
			}

			val := state.getByteOrder().Uint64(data[pos:])
			results = append(results, lua.LNumber(val))
			pos += 8

		case 'i':
			// Signed integer with optional size
			size := state.readNumber(4)
			if size < 1 || size > 8 {
				L.ArgError(1, "invalid integer size")

				return 0
			}

			pos += calcPadding(pos, min(state.align, size))
			if pos+size > len(data) {
				L.ArgError(2, "data string too short")

				return 0
			}

			tmp := make([]byte, 8)
			if state.bigEndian {
				copy(tmp[8-size:], data[pos:pos+size])
			} else {
				copy(tmp[:size], data[pos:pos+size])
			}

			val := state.getByteOrder().Uint64(tmp)
			// Sign extend if necessary
			if size < 8 {
				signBit := uint64(1) << (uint(size)*8 - 1)
				if val&signBit != 0 {
					mask := ^uint64(0) << (uint(size) * 8)
					val |= mask
				}
			}

			results = append(results, lua.LNumber(int64(val)))
			pos += size

		case 'I':
			// Unsigned integer with optional size
			size := state.readNumber(4)
			if size < 1 || size > 8 {
				L.ArgError(1, "invalid integer size")

				return 0
			}

			pos += calcPadding(pos, min(state.align, size))
			if pos+size > len(data) {
				L.ArgError(2, "data string too short")

				return 0
			}

			tmp := make([]byte, 8)
			if state.bigEndian {
				copy(tmp[8-size:], data[pos:pos+size])
			} else {
				copy(tmp[:size], data[pos:pos+size])
			}

			val := state.getByteOrder().Uint64(tmp)
			results = append(results, lua.LNumber(val))
			pos += size

		case 'f':
			// Float
			pos += calcPadding(pos, min(state.align, 4))
			if pos+4 > len(data) {
				L.ArgError(2, "data string too short")

				return 0
			}

			bits := state.getByteOrder().Uint32(data[pos:])
			results = append(results, lua.LNumber(math.Float32frombits(bits)))
			pos += 4

		case 'd':
			// Double
			pos += calcPadding(pos, min(state.align, 8))
			if pos+8 > len(data) {
				L.ArgError(2, "data string too short")

				return 0
			}

			bits := state.getByteOrder().Uint64(data[pos:])
			results = append(results, lua.LNumber(math.Float64frombits(bits)))
			pos += 8

		case 's':
			// Zero-terminated string
			start := pos
			for pos < len(data) && data[pos] != 0 {
				pos++
			}

			results = append(results, lua.LString(string(data[start:pos])))
			if pos < len(data) {
				pos++ // Skip the null terminator
			}

		case 'c':
			// Fixed-length char sequence
			size := state.readNumber(1)
			if size == 0 {
				// c0: use previous value as length
				size = prevSize
			}

			if pos+size > len(data) {
				L.ArgError(2, "data string too short")

				return 0
			}

			results = append(results, lua.LString(string(data[pos:pos+size])))
			pos += size

		default:
			L.ArgError(1, "invalid format option '"+string(c)+"'")

			return 0
		}

		// Track the last numeric value for c0
		if len(results) > 0 {
			if num, ok := results[len(results)-1].(lua.LNumber); ok {
				prevSize = int(num)
			}
		}
	}

	// Push all results
	for _, v := range results {
		L.Push(v)
	}
	// Push final position (1-indexed)
	L.Push(lua.LNumber(pos + 1))

	return len(results) + 1
}

// structSize implements struct.size(fmt)
func structSize(L *lua.LState) int {
	fmtStr := L.CheckString(1)

	state := &structState{
		fmt:       fmtStr,
		pos:       0,
		bigEndian: false,
		align:     1,
	}

	size := 0

	for {
		c, ok := state.nextChar()
		if !ok {
			break
		}

		switch c {
		case '>', '<', '=':
			// Endianness doesn't affect size
		case '!':
			state.align = state.readNumber(int(unsafe.Sizeof(int(0))))

		case 'x', 'b', 'B':
			size += calcPadding(size, min(state.align, 1))
			size++

		case 'h', 'H':
			size += calcPadding(size, min(state.align, 2))
			size += 2

		case 'l', 'L':
			size += calcPadding(size, min(state.align, 4))
			size += 4

		case 'q', 'Q', 'T':
			size += calcPadding(size, min(state.align, 8))
			size += 8

		case 'i', 'I':
			n := state.readNumber(4)
			if n < 1 || n > 8 {
				L.ArgError(1, "invalid integer size")

				return 0
			}

			size += calcPadding(size, min(state.align, n))
			size += n

		case 'f':
			size += calcPadding(size, min(state.align, 4))
			size += 4

		case 'd':
			size += calcPadding(size, min(state.align, 8))
			size += 8

		case 's':
			L.ArgError(1, "variable-size format 's' not allowed in struct.size")

			return 0

		case 'c':
			n := state.readNumber(1)
			if n == 0 {
				L.ArgError(1, "variable-size format 'c0' not allowed in struct.size")

				return 0
			}

			size += n

		default:
			L.ArgError(1, "invalid format option '"+string(c)+"'")

			return 0
		}
	}

	L.Push(lua.LNumber(size))

	return 1
}

// RegisterStructLib registers the struct library as a global in the Lua state
func RegisterStructLib(L *lua.LState) {
	mod := L.NewTable()
	L.SetFuncs(mod, structFuncs)
	L.SetGlobal("struct", mod)
}
