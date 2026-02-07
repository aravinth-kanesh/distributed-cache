package protocol

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"
)

var ErrProtocol = errors.New("protocol error")

// Reader reads RESP values from an io.Reader
// I use bufio.Reader with a 64KB buffer to minimise syscalls
type Reader struct {
	rd *bufio.Reader
}

// NewReader creates a new RESP reader
func NewReader(r io.Reader) *Reader {
	return &Reader{
		rd: bufio.NewReaderSize(r, 64*1024),
	}
}

// NewReaderFromBufio creates a Reader from an existing bufio.Reader
func NewReaderFromBufio(br *bufio.Reader) *Reader {
	return &Reader{rd: br}
}

// ReadValue reads a single RESP value from the stream
func (r *Reader) ReadValue() (Value, error) {
	// Peek at the first byte to determine the type
	b, err := r.rd.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch RESPType(b) {
	case SimpleString:
		return r.readSimpleString()
	case Error:
		return r.readError()
	case Integer:
		return r.readInteger()
	case BulkString:
		return r.readBulkString()
	case Array:
		return r.readArray()
	default:
		// Not a RESP type prefix - treat as inline command
		// Put the byte back and read as inline
		if err := r.rd.UnreadByte(); err != nil {
			return Value{}, err
		}
		return r.readInline()
	}
}

// readLine reads until \r\n and returns the line without the terminator
func (r *Reader) readLine() ([]byte, error) {
	line, err := r.rd.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, ErrProtocol
	}

	return line[:len(line)-2], nil
}

// readSimpleString parses +<string>\r\n
func (r *Reader) readSimpleString() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	return SimpleStringVal(string(line)), nil
}

// readError parses -<message>\r\n
func (r *Reader) readError() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	return ErrorVal(string(line)), nil
}

// readInteger parses :<number>\r\n
func (r *Reader) readInteger() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	n, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return Value{}, ErrProtocol
	}
	return IntegerVal(n), nil
}

// readBulkString parses $<len>\r\n<data>\r\n or $-1\r\n for null
func (r *Reader) readBulkString() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}

	length, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return Value{}, ErrProtocol
	}

	// Null bulk string
	if length < 0 {
		return NullBulkString(), nil
	}

	// Read exactly length bytes + \r\n
	buf := make([]byte, length+2)
	_, err = io.ReadFull(r.rd, buf)
	if err != nil {
		return Value{}, err
	}

	// Verify trailing \r\n
	if buf[length] != '\r' || buf[length+1] != '\n' {
		return Value{}, ErrProtocol
	}

	return BulkStringVal(buf[:length]), nil
}

// readArray parses *<count>\r\n followed by count elements
func (r *Reader) readArray() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}

	count, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return Value{}, ErrProtocol
	}

	// Null array
	if count < 0 {
		return NullArray(), nil
	}

	// Empty array
	if count == 0 {
		return ArrayVal([]Value{}), nil
	}

	// Read each element recursively
	elements := make([]Value, count)
	for i := int64(0); i < count; i++ {
		val, err := r.ReadValue()
		if err != nil {
			return Value{}, err
		}
		elements[i] = val
	}

	return ArrayVal(elements), nil
}

// readInline handles non-RESP inline commands (e.g., "PING\r\n")
// This is necessary for redis-cli interactive mode to work
func (r *Reader) readInline() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}

	// Trim and split on spaces
	parts := strings.Fields(string(line))
	if len(parts) == 0 {
		return Value{}, ErrProtocol
	}

	// Convert to an Array of BulkStrings (same format as RESP commands)
	elements := make([]Value, len(parts))
	for i, part := range parts {
		elements[i] = BulkStringVal([]byte(part))
	}

	return ArrayVal(elements), nil
}
