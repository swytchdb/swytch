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
	"bufio"
	"errors"
	"io"
	"math"
)

// RESP2 data type prefixes
const (
	RESPSimpleString = '+'
	RESPError        = '-'
	RESPInteger      = ':'
	RESPBulkString   = '$'
	RESPArray        = '*'
)

// RESP3 data type prefixes
const (
	RESP3Null           = '_' // Null: _\r\n
	RESP3Double         = ',' // Double: ,1.23\r\n
	RESP3Boolean        = '#' // Boolean: #t\r\n or #f\r\n
	RESP3BlobError      = '!' // Blob Error: !<len>\r\n<data>\r\n
	RESP3VerbatimString = '=' // Verbatim: =<len>\r\n<encoding>:<data>\r\n
	RESP3BigNumber      = '(' // Big Number: (<number>\r\n
	RESP3Map            = '%' // Map: %<count>\r\n<key><value>...
	RESP3Set            = '~' // Set: ~<count>\r\n<elements>...
	RESP3Push           = '>' // Push: ><count>\r\n<elements>...
	RESP3Attribute      = '|' // Attribute: |<count>\r\n<key><value>...<actual_reply>
)

// Protocol limits
const (
	// defaultProtoMaxBulkLen is the maximum bulk string size (512MB)
	defaultProtoMaxBulkLen = 512 * 1024 * 1024
	// maxArrayCount is the maximum number of elements in a RESP array (2^20, matches Redis)
	maxArrayCount = 1048576
)

// Protocol errors
var (
	ErrInvalidProtocol   = errors.New("ERR invalid protocol")
	ErrInvalidBulkLength = errors.New("ERR invalid bulk length")
	ErrInvalidArrayCount = errors.New("ERR invalid array count")
	ErrLineTooLong       = errors.New("ERR line too long")
	ErrUnexpectedEOF     = errors.New("ERR unexpected end of input")
)

// Parser handles RESP2 protocol parsing
type Parser struct {
	reader *bufio.Reader
}

// NewParser creates a new RESP2 protocol parser
func NewParser(r io.Reader) *Parser {
	return &Parser{
		reader: bufio.NewReaderSize(r, ReadBufSize),
	}
}

// NewParserWithReader creates a parser using an existing buffered reader
func NewParserWithReader(r *bufio.Reader) *Parser {
	return &Parser{reader: r}
}

// Reset resets the parser with a new reader
func (p *Parser) Reset(r io.Reader) {
	p.reader.Reset(r)
}

// Buffered returns the number of bytes currently buffered
func (p *Parser) Buffered() int {
	return p.reader.Buffered()
}

// ReadCommand reads and parses the next Redis command
// Returns a pooled Command - caller should call putCommand when done
func (p *Parser) ReadCommand() (*Command, error) {
	return p.ReadCommandInto(GetCommand())
}

// ReadCommandInto reads a command into the provided Command struct
func (p *Parser) ReadCommandInto(cmd *Command) (*Command, error) {
	// Peek first byte to determine protocol type
	b, err := p.reader.Peek(1)
	if err != nil {
		return nil, err
	}

	if b[0] == RESPArray {
		// Standard RESP array command
		return p.readArrayCommand(cmd)
	}

	// Inline command (for telnet compatibility)
	return p.readInlineCommand(cmd)
}

// readArrayCommand parses a RESP array command
// Format: *<count>\r\n$<len>\r\n<arg>\r\n...
func (p *Parser) readArrayCommand(cmd *Command) (*Command, error) {
	// Read the array header
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}

	if len(line) < 1 || line[0] != RESPArray {
		return nil, ErrInvalidProtocol
	}

	// Parse element count
	count, ok := ParseInt64(line[1:])
	if !ok || count < 0 {
		return nil, ErrInvalidArrayCount
	}

	if count > maxArrayCount {
		return nil, ErrInvalidArrayCount
	}

	if count == 0 {
		cmd.Type = CmdUnknown
		return cmd, nil
	}

	// Ensure Args slice has capacity
	if cap(cmd.Args) < int(count) {
		cmd.Args = make([][]byte, 0, count)
	}
	cmd.Args = cmd.Args[:0]

	// Read each element (all should be bulk strings)
	for range count {
		arg, err := p.readBulkString()
		if err != nil {
			return nil, err
		}
		cmd.Args = append(cmd.Args, arg)
	}

	// Parse command type from first argument
	if len(cmd.Args) > 0 {
		name := cmd.Args[0]
		cmd.Type = ParseCommandType(name)
		// Store raw name for unknown commands (useful for logging)
		if cmd.Type == CmdUnknown {
			cmd.RawName = name
		} else {
			PutDataBuf(name)
		}
		// Remove the command name without advancing the slice base. Advancing it
		// loses one slot of pooled capacity per command and eventually forces a
		// new Args backing array on the hot parser path.
		copy(cmd.Args, cmd.Args[1:])
		cmd.Args = cmd.Args[:len(cmd.Args)-1]
	}

	return cmd, nil
}

// readInlineCommand parses an inline (telnet-style) command
// Format: COMMAND arg1 arg2 ...\r\n
func (p *Parser) readInlineCommand(cmd *Command) (*Command, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}

	if len(line) == 0 {
		cmd.Type = CmdNoop
		return cmd, nil
	}

	// Parse space-separated tokens
	if cap(cmd.Args) < 8 {
		cmd.Args = make([][]byte, 0, 8)
	}
	cmd.Args = cmd.Args[:0]

	rest := line
	for len(rest) > 0 {
		token, remaining := NextToken(rest)
		if token == nil {
			break
		}
		// Copy token to avoid race - line points into bufio.Reader's buffer
		// which may be overwritten when the next command is read
		tokenCopy := GetDataBuf(len(token))
		copy(tokenCopy, token)
		cmd.Args = append(cmd.Args, tokenCopy)
		rest = remaining
	}

	if len(cmd.Args) == 0 {
		cmd.Type = CmdNoop
		return cmd, nil
	}

	// Parse command type from first token
	name := cmd.Args[0]
	cmd.Type = ParseCommandType(name)
	// Store raw name for unknown commands (useful for logging)
	if cmd.Type == CmdUnknown {
		cmd.RawName = name
	} else {
		PutDataBuf(name)
	}
	copy(cmd.Args, cmd.Args[1:])
	cmd.Args = cmd.Args[:len(cmd.Args)-1]

	return cmd, nil
}

// readBulkString reads a RESP bulk string
// Format: $<length>\r\n<data>\r\n
func (p *Parser) readBulkString() ([]byte, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}

	if len(line) < 1 || line[0] != RESPBulkString {
		return nil, ErrInvalidProtocol
	}

	// Parse length
	length, ok := ParseInt64(line[1:])
	if !ok {
		return nil, ErrInvalidBulkLength
	}

	// Null bulk string
	if length == -1 {
		return nil, nil
	}

	if length < 0 {
		return nil, ErrInvalidBulkLength
	}

	if length > defaultProtoMaxBulkLen {
		return nil, ErrInvalidBulkLength
	}

	// Read the data
	data := GetDataBuf(int(length))
	_, err = io.ReadFull(p.reader, data)
	if err != nil {
		PutDataBuf(data)
		return nil, err
	}

	// Read trailing \r\n
	_, err = p.reader.Discard(2)
	if err != nil {
		PutDataBuf(data)
		return nil, err
	}

	return data, nil
}

// readLine reads a line terminated by \r\n
func (p *Parser) readLine() ([]byte, error) {
	line, err := p.reader.ReadSlice('\n')
	if err != nil {
		if err == bufio.ErrBufferFull {
			// Line too long
			return nil, ErrLineTooLong
		}
		return nil, err
	}

	// Trim \r\n
	lineLen := len(line)
	if lineLen > 0 && line[lineLen-1] == '\n' {
		lineLen--
	}
	if lineLen > 0 && line[lineLen-1] == '\r' {
		lineLen--
	}

	return line[:lineLen], nil
}

// ParseInt64 parses an int64 from a byte slice without allocation, rejecting overflow.
func ParseInt64(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	neg := false
	i := 0
	switch b[0] {
	case '-':
		neg = true
		i = 1
	case '+':
		i = 1
	}
	if i >= len(b) {
		return 0, false
	}
	var n uint64
	for ; i < len(b); i++ {
		c := b[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		d := uint64(c - '0')
		if n > (math.MaxInt64+1)/10 { // would overflow on multiply
			return 0, false
		}
		n = n*10 + d
		if !neg && n > math.MaxInt64 {
			return 0, false
		}
		if neg && n > math.MaxInt64+1 {
			return 0, false
		}
	}
	if neg {
		return -int64(n), true
	}
	return int64(n), true
}

// ParseUint64 parses a uint64 from a byte slice without allocation
func ParseUint64(b []byte) (uint64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	var n uint64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + uint64(c-'0')
	}
	return n, true
}

// ParseFloat64 parses a float64 from a byte slice
func ParseFloat64(b []byte) (float64, bool) {
	if len(b) == 0 {
		return 0, false
	}

	var neg bool
	switch b[0] {
	case '-':
		neg = true
		b = b[1:]
	case '+':
		b = b[1:]
	}

	var intPart, fracPart int64
	var fracDiv float64 = 1
	var hasFrac bool

	for _, c := range b {
		if c == '.' {
			if hasFrac {
				return 0, false // Multiple decimal points
			}
			hasFrac = true
			continue
		}
		if c < '0' || c > '9' {
			return 0, false
		}
		if hasFrac {
			fracPart = fracPart*10 + int64(c-'0')
			fracDiv *= 10
		} else {
			intPart = intPart*10 + int64(c-'0')
		}
	}

	result := float64(intPart) + float64(fracPart)/fracDiv
	if neg {
		result = -result
	}
	return result, true
}

// NextToken returns the next space-delimited token and the remaining bytes
func NextToken(b []byte) (token, rest []byte) {
	// Skip leading spaces
	for len(b) > 0 && b[0] == ' ' {
		b = b[1:]
	}
	if len(b) == 0 {
		return nil, nil
	}

	// Find end of token
	end := 0
	for end < len(b) && b[end] != ' ' {
		end++
	}

	token = b[:end]
	rest = b[end:]
	if len(rest) > 0 {
		rest = rest[1:] // Skip the space
	}
	return token, rest
}
