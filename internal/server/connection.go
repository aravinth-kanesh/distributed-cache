package server

import (
	"bufio"
	"net"
	"sync/atomic"
	"time"

	"github.com/aravinth/distributed-cache/internal/protocol"
)

// Connection represents a single client connection
type Connection struct {
	id         uint64
	conn       net.Conn
	reader     *protocol.Reader
	writer     *protocol.Writer
	bufReader  *bufio.Reader
	bufWriter  *bufio.Writer
	createdAt  time.Time
	lastActive atomic.Int64
	addr       string
}

// NewConnection creates a new Connection wrapping a net.Conn
func NewConnection(id uint64, conn net.Conn, br *bufio.Reader, bw *bufio.Writer) *Connection {
	c := &Connection{
		id:        id,
		conn:      conn,
		reader:    protocol.NewReaderFromBufio(br),
		writer:    protocol.NewWriterFromBufio(bw),
		bufReader: br,
		bufWriter: bw,
		createdAt: time.Now(),
		addr:      conn.RemoteAddr().String(),
	}
	c.lastActive.Store(time.Now().UnixNano())
	return c
}

// ReadCommand reads a single RESP command from the connection
func (c *Connection) ReadCommand() (protocol.Value, error) {
	return c.reader.ReadValue()
}

// WriteResponse writes a RESP response to the connection's write buffer
func (c *Connection) WriteResponse(v protocol.Value) error {
	return c.writer.WriteValue(v)
}

// Flush flushes the write buffer to the underlying connection
func (c *Connection) Flush() error {
	return c.writer.Flush()
}

// Close closes the underlying connection
func (c *Connection) Close() error {
	return c.conn.Close()
}

// SetReadDeadline sets the read deadline for idle timeout enforcement
func (c *Connection) SetReadDeadline(d time.Duration) {
	c.conn.SetReadDeadline(time.Now().Add(d))
}

// ID returns the connection ID
func (c *Connection) ID() uint64 {
	return c.id
}

// Addr returns the remote address
func (c *Connection) Addr() string {
	return c.addr
}
