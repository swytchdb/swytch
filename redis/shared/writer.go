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

package shared

import (
	"bytes"
	"math"
	"strconv"
	"strings"
)

// Writer handles RESP response formatting (supports RESP2 and RESP3)
type Writer struct {
	buf        *bytes.Buffer
	protocol   ProtocolVersion
	stats      *Stats
	wroteError bool
	tmp        [64]byte
	scratch    bytes.Buffer // reusable response-staging buffer, see Scratch
}

// NewWriter creates a new RESP response writer (defaults to RESP2)
func NewWriter(buf *bytes.Buffer) *Writer {
	return &Writer{buf: buf, protocol: RESP2}
}

// SetStats sets the stats object for error tracking
func (w *Writer) SetStats(s *Stats) {
	w.stats = s
}

// ResetErrorFlag resets the error tracking flag (call before each command)
func (w *Writer) ResetErrorFlag() {
	w.wroteError = false
}

// WroteError returns true if WriteError was called since last ResetErrorFlag
func (w *Writer) WroteError() bool {
	return w.wroteError
}

// SetProtocol sets the protocol version for subsequent writes
func (w *Writer) SetProtocol(p ProtocolVersion) {
	w.protocol = p
}

// Protocol returns the current protocol version
func (w *Writer) Protocol() ProtocolVersion {
	return w.protocol
}

// Buffer returns the underlying buffer
func (w *Writer) Buffer() *bytes.Buffer {
	return w.buf
}

// Reset resets the writer with a new buffer
func (w *Writer) Reset(buf *bytes.Buffer) {
	w.buf = buf
}

// Scratch returns the writer's reusable response-staging buffer, reset and
// ready for use. Retaining the buffer on the writer means the per-command
// stage-then-commit in ExecuteInto reuses one high-water allocation instead
// of growing a fresh buffer every command. If the writer is already staging
// into its scratch (a nested ExecuteInto on the same writer), a fresh buffer
// is returned so the outer stage's pending bytes aren't clobbered.
func (w *Writer) Scratch() *bytes.Buffer {
	if w.buf == &w.scratch {
		return &bytes.Buffer{}
	}
	w.scratch.Reset()
	return &w.scratch
}

// WriteRaw writes raw bytes directly to the buffer (pre-formatted RESP)
func (w *Writer) WriteRaw(data []byte) {
	w.buf.Write(data)
}

// WriteSimpleString writes a RESP simple string (+OK\r\n)
func (w *Writer) WriteSimpleString(s string) {
	w.buf.WriteByte(RESPSimpleString)
	w.buf.WriteString(s)
	w.buf.Write(crlf)
}

// WriteError writes a RESP error (-ERR message\r\n)
func (w *Writer) WriteError(msg string) {
	w.buf.WriteByte(RESPError)
	w.buf.WriteString(msg)
	w.buf.Write(crlf)
	w.wroteError = true

	// Track error statistics
	if w.stats != nil {
		// Extract error prefix (first word before space)
		prefix := msg
		if idx := strings.Index(msg, " "); idx > 0 {
			prefix = msg[:idx]
		}
		w.stats.RecordError(prefix)
	}
}

// WriteErrorf writes a formatted RESP error
func (w *Writer) WriteErrorf(format string, args ...any) {
	w.buf.WriteByte(RESPError)
	w.buf.WriteString("ERR ")
	// Simple formatting without fmt to avoid allocation
	w.buf.WriteString(format)
	w.buf.Write(crlf)
	w.wroteError = true

	// Track error statistics
	if w.stats != nil {
		w.stats.RecordError("ERR")
	}
}

// WriteInteger writes a RESP integer (:1000\r\n)
func (w *Writer) WriteInteger(n int64) {
	w.buf.WriteByte(RESPInteger)
	w.buf.Write(strconv.AppendInt(w.tmp[:0], n, 10))
	w.buf.Write(crlf)
}

// WriteBulkString writes a RESP bulk string ($5\r\nhello\r\n)
func (w *Writer) WriteBulkString(b []byte) {
	w.buf.WriteByte(RESPBulkString)
	w.buf.Write(strconv.AppendInt(w.tmp[:0], int64(len(b)), 10))
	w.buf.Write(crlf)
	w.buf.Write(b)
	w.buf.Write(crlf)
}

// WriteBulkStringStr writes a string as a RESP bulk string
func (w *Writer) WriteBulkStringStr(s string) {
	w.buf.WriteByte(RESPBulkString)
	w.buf.Write(strconv.AppendInt(w.tmp[:0], int64(len(s)), 10))
	w.buf.Write(crlf)
	w.buf.WriteString(s)
	w.buf.Write(crlf)
}

// WriteNullBulkString writes a RESP null bulk string ($-1\r\n) for RESP2, or null (_\r\n) for RESP3
func (w *Writer) WriteNullBulkString() {
	if w.protocol == RESP3 {
		w.buf.Write(resp3Null)
	} else {
		w.buf.Write(nullBulkString)
	}
}

// WriteNull writes RESP3 null (_\r\n) or RESP2 null bulk string ($-1\r\n)
func (w *Writer) WriteNull() {
	if w.protocol == RESP3 {
		w.buf.Write(resp3Null)
	} else {
		w.buf.Write(nullBulkString)
	}
}

// WriteDouble writes a RESP3 double (,<value>\r\n)
// Falls back to bulk string for RESP2
func (w *Writer) WriteDouble(f float64) {
	if w.protocol == RESP3 {
		w.buf.WriteByte(RESP3Double)
		w.buf.Write(strconv.AppendFloat(w.tmp[:0], f, 'g', -1, 64))
		w.buf.Write(crlf)
	} else {
		var num [64]byte
		w.WriteBulkString(strconv.AppendFloat(num[:0], f, 'g', -1, 64))
	}
}

// WriteScore writes a Redis score value
// In RESP3: uses double type (,<value>\r\n) with proper inf/nan handling
// In RESP2: uses bulk string with formatted score
func (w *Writer) WriteScore(f float64) {
	if w.protocol == RESP3 {
		w.buf.WriteByte(RESP3Double)
		// Handle special values with Redis-compatible lowercase format
		switch {
		case math.IsNaN(f):
			w.buf.WriteString("nan")
		case math.IsInf(f, 1):
			w.buf.WriteString("inf")
		case math.IsInf(f, -1):
			w.buf.WriteString("-inf")
		default:
			// Format whole numbers as integers to avoid scientific notation
			if f == float64(int64(f)) && f >= -9007199254740992 && f <= 9007199254740992 {
				w.buf.Write(strconv.AppendInt(w.tmp[:0], int64(f), 10))
			} else {
				w.buf.Write(strconv.AppendFloat(w.tmp[:0], f, 'g', -1, 64))
			}
		}
		w.buf.Write(crlf)
	} else {
		// RESP2: use bulk string with formatted score
		switch {
		case math.IsNaN(f):
			w.WriteBulkStringStr("nan")
		case math.IsInf(f, 1):
			w.WriteBulkStringStr("inf")
		case math.IsInf(f, -1):
			w.WriteBulkStringStr("-inf")
		default:
			// Format whole numbers as integers to avoid scientific notation
			if f == float64(int64(f)) && f >= -9007199254740992 && f <= 9007199254740992 {
				var num [64]byte
				w.WriteBulkString(strconv.AppendInt(num[:0], int64(f), 10))
			} else {
				var num [64]byte
				w.WriteBulkString(strconv.AppendFloat(num[:0], f, 'g', -1, 64))
			}
		}
	}
}

// WriteBoolean writes a RESP3 boolean (#t\r\n or #f\r\n)
// Falls back to integer (1 or 0) for RESP2
func (w *Writer) WriteBoolean(b bool) {
	if w.protocol == RESP3 {
		if b {
			w.buf.Write(resp3True)
		} else {
			w.buf.Write(resp3False)
		}
	} else {
		if b {
			w.WriteOne()
		} else {
			w.WriteZero()
		}
	}
}

// WriteMap writes a RESP3 map header (%<count>\r\n)
// Falls back to array with 2*count elements for RESP2
func (w *Writer) WriteMap(count int) {
	if w.protocol == RESP3 {
		w.buf.WriteByte(RESP3Map)
		w.buf.Write(strconv.AppendInt(w.tmp[:0], int64(count), 10))
		w.buf.Write(crlf)
	} else {
		w.WriteArray(count * 2)
	}
}

// WriteSet writes a RESP3 set header (~<count>\r\n)
// Falls back to array for RESP2
func (w *Writer) WriteSet(count int) {
	if w.protocol == RESP3 {
		w.buf.WriteByte(RESP3Set)
		w.buf.Write(strconv.AppendInt(w.tmp[:0], int64(count), 10))
		w.buf.Write(crlf)
	} else {
		w.WriteArray(count)
	}
}

// WriteBlobError writes a RESP3 blob error (!<len>\r\n<data>\r\n)
// Falls back to simple error for RESP2
func (w *Writer) WriteBlobError(errType, msg string) {
	if w.protocol == RESP3 {
		fullMsg := errType + " " + msg
		w.buf.WriteByte(RESP3BlobError)
		w.buf.Write(strconv.AppendInt(w.tmp[:0], int64(len(fullMsg)), 10))
		w.buf.Write(crlf)
		w.buf.WriteString(fullMsg)
		w.buf.Write(crlf)
	} else {
		w.WriteError(errType + " " + msg)
	}
}

// WriteVerbatimString writes a RESP3 verbatim string (=<len>\r\n<enc>:<data>\r\n)
// Falls back to bulk string for RESP2
func (w *Writer) WriteVerbatimString(encoding string, data []byte) {
	if w.protocol == RESP3 {
		// Encoding must be exactly 3 bytes
		if len(encoding) != 3 {
			encoding = "txt"
		}
		totalLen := 3 + 1 + len(data) // encoding + ":" + data
		w.buf.WriteByte(RESP3VerbatimString)
		w.buf.Write(strconv.AppendInt(w.tmp[:0], int64(totalLen), 10))
		w.buf.Write(crlf)
		w.buf.WriteString(encoding)
		w.buf.WriteByte(':')
		w.buf.Write(data)
		w.buf.Write(crlf)
	} else {
		w.WriteBulkString(data)
	}
}

// WriteBigNumber writes a RESP3 big number ((<number>\r\n)
// Falls back to bulk string for RESP2
func (w *Writer) WriteBigNumber(n string) {
	if w.protocol == RESP3 {
		w.buf.WriteByte(RESP3BigNumber)
		w.buf.WriteString(n)
		w.buf.Write(crlf)
	} else {
		w.WriteBulkStringStr(n)
	}
}

// WritePush writes a RESP3 push message header (><count>\r\n)
// Only valid for RESP3 - no RESP2 equivalent (used for pub/sub, invalidation)
func (w *Writer) WritePush(count int) {
	w.buf.WriteByte(RESP3Push)
	w.buf.Write(strconv.AppendInt(w.tmp[:0], int64(count), 10))
	w.buf.Write(crlf)
}

// WriteAttribute writes RESP3 attribute metadata (|<count>\r\n)
// No RESP2 equivalent - silently ignored for RESP2
func (w *Writer) WriteAttribute(count int) {
	if w.protocol == RESP3 {
		w.buf.WriteByte(RESP3Attribute)
		w.buf.Write(strconv.AppendInt(w.tmp[:0], int64(count), 10))
		w.buf.Write(crlf)
	}
	// For RESP2: attributes are silently ignored
}

// WriteArray writes a RESP array header (*<count>\r\n)
func (w *Writer) WriteArray(count int) {
	w.buf.WriteByte(RESPArray)
	w.buf.Write(strconv.AppendInt(w.tmp[:0], int64(count), 10))
	w.buf.Write(crlf)
}

// WriteNullArray writes a RESP null array (*-1\r\n) for RESP2, or null (_\r\n) for RESP3
func (w *Writer) WriteNullArray() {
	if w.protocol == RESP3 {
		w.buf.Write(resp3Null)
	} else {
		w.buf.Write(nullArray)
	}
}

// WriteOK writes +OK\r\n
func (w *Writer) WriteOK() {
	w.buf.Write(respOK)
}

// WritePong writes +PONG\r\n
func (w *Writer) WritePong() {
	w.buf.Write(respPong)
}

// WriteQueued writes +QUEUED\r\n (for transactions)
func (w *Writer) WriteQueued() {
	w.buf.Write(respQueued)
}

// WriteZero writes :0\r\n
func (w *Writer) WriteZero() {
	w.buf.Write(respZero)
}

// WriteOne writes :1\r\n
func (w *Writer) WriteOne() {
	w.buf.Write(respOne)
}

// WriteWrongType writes the WRONGTYPE error
func (w *Writer) WriteWrongType() {
	w.buf.Write(respWrongType)
}

// WriteNoAuth writes the NOAUTH error
func (w *Writer) WriteNoAuth() {
	w.buf.Write(respNoAuth)
}

// WriteWrongNumArguments writes the wrong number of arguments error
func (w *Writer) WriteWrongNumArguments(cmd string) {
	w.buf.WriteByte(RESPError)
	w.buf.WriteString("ERR wrong number of arguments for '")
	w.buf.WriteString(cmd)
	w.buf.WriteString("' command")
	w.buf.Write(crlf)
}

// WriteWrongNumArgs writes the wrong number of arguments error
func (w *Writer) WriteWrongNumArgs(cmd string) {
	w.buf.WriteByte(RESPError)
	w.buf.WriteString("ERR wrong number of args for '")
	w.buf.WriteString(cmd)
	w.buf.WriteString("' command")
	w.buf.Write(crlf)
}

// WriteUnknownCommand writes the unknown command error
func (w *Writer) WriteUnknownCommand(cmd string, args [][]byte) {
	w.buf.WriteByte(RESPError)
	w.buf.WriteString("ERR unknown command '")
	w.buf.WriteString(cmd)
	w.buf.WriteString("', with args beginning with: ")
	for i, arg := range args {
		if i > 0 {
			w.buf.WriteByte(' ')
		}
		w.buf.WriteByte('\'')
		w.buf.Write(arg)
		w.buf.WriteByte('\'')
		if i >= 2 {
			w.buf.WriteString("...")
			break
		}
	}
	w.buf.Write(crlf)
}

// WriteSyntaxError writes a syntax error
func (w *Writer) WriteSyntaxError() {
	w.buf.Write(respSyntaxError)
}

// WriteNotInteger writes the "not an integer" error
func (w *Writer) WriteNotInteger() {
	w.buf.Write(respNotInteger)
}

// WriteOutOfRange writes the "out of range" error
func (w *Writer) WriteOutOfRange() {
	w.buf.Write(respOutOfRange)
}

// Pre-allocated response byte slices for common responses
var (
	crlf            = []byte("\r\n")
	nullBulkString  = []byte("$-1\r\n")
	nullArray       = []byte("*-1\r\n")
	respOK          = []byte("+OK\r\n")
	respPong        = []byte("+PONG\r\n")
	respQueued      = []byte("+QUEUED\r\n")
	respZero        = []byte(":0\r\n")
	respOne         = []byte(":1\r\n")
	respWrongType   = []byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
	respNoAuth      = []byte("-NOAUTH Authentication required.\r\n")
	respSyntaxError = []byte("-ERR syntax error\r\n")
	respNotInteger  = []byte("-ERR value is not an integer or out of range\r\n")
	respOutOfRange  = []byte("-ERR index out of range\r\n")

	// RESP3 pre-allocated responses
	resp3Null  = []byte("_\r\n")
	resp3True  = []byte("#t\r\n")
	resp3False = []byte("#f\r\n")
)
