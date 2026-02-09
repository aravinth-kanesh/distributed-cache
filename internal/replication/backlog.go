package replication

import (
	"fmt"
	"sync"
)

// DefaultBacklogSize is the default replication backlog: 1 MB.
// This determines how far behind a slave can fall before needing
// a full resync. For a cache doing ~100K commands/sec at ~100 bytes
// each, 1 MB covers roughly 0.1 seconds of commands.
const DefaultBacklogSize = 1 << 20

// Backlog is a bounded circular buffer that stores raw RESP-encoded
// mutating commands. Each byte written advances the global offset.
// I store the data in a single contiguous []byte with wrap-around
// semantics, plus the offset of the first valid byte, so that a
// slave can request "everything from offset X" and I can serve it
// directly from memory if X is still within the buffer.
type Backlog struct {
	mu   sync.RWMutex
	buf  []byte // Fixed-size ring buffer
	size int    // len(buf), immutable after creation

	// head is the write cursor position within buf (0..size-1).
	head int

	// written is the total number of bytes ever written.
	// CurrentOffset() == written.
	written int64
}

// NewBacklog allocates a ring buffer of the given size.
func NewBacklog(size int) *Backlog {
	if size <= 0 {
		size = DefaultBacklogSize
	}
	return &Backlog{
		buf:  make([]byte, size),
		size: size,
	}
}

// Write appends data to the ring buffer. If the data is larger than the
// buffer, only the tail that fits is kept (oldest data is silently
// overwritten). This is the expected behaviour — a very large command
// simply pushes older data out of the window.
func (b *Backlog) Write(data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()

	n := len(data)
	if n == 0 {
		return
	}

	// If data is larger than the buffer, only keep the tail
	if n >= b.size {
		data = data[n-b.size:]
		n = b.size
		copy(b.buf, data)
		b.head = 0
		b.written += int64(len(data))
		return
	}

	// Write with wrap-around
	firstChunk := b.size - b.head
	if firstChunk >= n {
		copy(b.buf[b.head:], data)
	} else {
		copy(b.buf[b.head:], data[:firstChunk])
		copy(b.buf, data[firstChunk:])
	}

	b.head = (b.head + n) % b.size
	b.written += int64(n)
}

// CurrentOffset returns the global offset (total bytes written).
func (b *Backlog) CurrentOffset() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.written
}

// Available returns true if the given offset is still within the
// backlog's valid window. An offset is available if it falls within
// [written - min(written, size), written].
func (b *Backlog) Available(fromOffset int64) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.available(fromOffset)
}

func (b *Backlog) available(fromOffset int64) bool {
	if fromOffset > b.written {
		return false
	}
	if fromOffset < 0 {
		return false
	}
	oldest := b.written - int64(b.size)
	if oldest < 0 {
		oldest = 0
	}
	return fromOffset >= oldest
}

// Slice returns the bytes from fromOffset to the current end of the
// backlog. Returns an error if the offset is no longer available.
func (b *Backlog) Slice(fromOffset int64) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.available(fromOffset) {
		return nil, fmt.Errorf("offset %d is no longer in the backlog (current: %d, window: %d)",
			fromOffset, b.written, b.size)
	}

	if fromOffset == b.written {
		return nil, nil // Nothing to send
	}

	count := int(b.written - fromOffset)

	// Calculate the start position in the ring buffer.
	// head points to where the NEXT write will go, so the most recent
	// byte is at head-1. The byte at fromOffset is count bytes behind head.
	startPos := (b.head - count + b.size) % b.size

	result := make([]byte, count)
	if startPos+count <= b.size {
		// Contiguous read
		copy(result, b.buf[startPos:startPos+count])
	} else {
		// Wrap-around read
		firstChunk := b.size - startPos
		copy(result, b.buf[startPos:])
		copy(result[firstChunk:], b.buf[:count-firstChunk])
	}

	return result, nil
}
