package protocol

import (
	"bufio"
	"io"
	"strconv"
)

var crlf = []byte{'\r', '\n'}

// Writer writes RESP values to an io.Writer
// I use bufio.Writer with a 64KB buffer to batch small writes,
// which reduces syscalls significantly for pipelined responses
type Writer struct {
	wr *bufio.Writer
}

// NewWriter creates a new RESP writer
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		wr: bufio.NewWriterSize(w, 64*1024),
	}
}

// NewWriterFromBufio creates a Writer from an existing bufio.Writer
func NewWriterFromBufio(bw *bufio.Writer) *Writer {
	return &Writer{wr: bw}
}

// WriteValue writes a single RESP value to the buffer
// Call Flush() afterwards to send buffered data
func (w *Writer) WriteValue(v Value) error {
	switch v.Type {
	case SimpleString:
		return w.writeSimpleString(v.Str)
	case Error:
		return w.writeError(v.Str)
	case Integer:
		return w.writeInteger(v.Num)
	case BulkString:
		if v.IsNull {
			return w.writeNullBulkString()
		}
		return w.writeBulkString(v.Bulk)
	case Array:
		if v.IsNull {
			return w.writeNullArray()
		}
		return w.writeArray(v.Array)
	default:
		return w.writeError("ERR unknown value type")
	}
}

// Flush flushes the write buffer to the underlying writer
func (w *Writer) Flush() error {
	return w.wr.Flush()
}

// writeSimpleString writes +<string>\r\n
func (w *Writer) writeSimpleString(s string) error {
	if err := w.wr.WriteByte(byte(SimpleString)); err != nil {
		return err
	}
	if _, err := w.wr.WriteString(s); err != nil {
		return err
	}
	_, err := w.wr.Write(crlf)
	return err
}

// writeError writes -<message>\r\n
func (w *Writer) writeError(s string) error {
	if err := w.wr.WriteByte(byte(Error)); err != nil {
		return err
	}
	if _, err := w.wr.WriteString(s); err != nil {
		return err
	}
	_, err := w.wr.Write(crlf)
	return err
}

// writeInteger writes :<number>\r\n
// I use strconv.AppendInt with a stack-allocated scratch buffer to avoid allocations
func (w *Writer) writeInteger(n int64) error {
	if err := w.wr.WriteByte(byte(Integer)); err != nil {
		return err
	}
	var buf [20]byte // Max int64 is 19 digits + sign
	b := strconv.AppendInt(buf[:0], n, 10)
	if _, err := w.wr.Write(b); err != nil {
		return err
	}
	_, err := w.wr.Write(crlf)
	return err
}

// writeBulkString writes $<len>\r\n<data>\r\n
func (w *Writer) writeBulkString(b []byte) error {
	if err := w.wr.WriteByte(byte(BulkString)); err != nil {
		return err
	}
	var buf [20]byte
	lenBytes := strconv.AppendInt(buf[:0], int64(len(b)), 10)
	if _, err := w.wr.Write(lenBytes); err != nil {
		return err
	}
	if _, err := w.wr.Write(crlf); err != nil {
		return err
	}
	if _, err := w.wr.Write(b); err != nil {
		return err
	}
	_, err := w.wr.Write(crlf)
	return err
}

// writeNullBulkString writes $-1\r\n
func (w *Writer) writeNullBulkString() error {
	_, err := w.wr.WriteString("$-1\r\n")
	return err
}

// writeArray writes *<count>\r\n followed by each element
func (w *Writer) writeArray(vals []Value) error {
	if err := w.wr.WriteByte(byte(Array)); err != nil {
		return err
	}
	var buf [20]byte
	lenBytes := strconv.AppendInt(buf[:0], int64(len(vals)), 10)
	if _, err := w.wr.Write(lenBytes); err != nil {
		return err
	}
	if _, err := w.wr.Write(crlf); err != nil {
		return err
	}
	for _, v := range vals {
		if err := w.WriteValue(v); err != nil {
			return err
		}
	}
	return nil
}

// writeNullArray writes *-1\r\n
func (w *Writer) writeNullArray() error {
	_, err := w.wr.WriteString("*-1\r\n")
	return err
}
