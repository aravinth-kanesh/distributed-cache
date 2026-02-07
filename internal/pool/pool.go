package pool

import (
	"bufio"
	"io"
	"sync"
)

const (
	// DefaultBufSize is 64KB, matching the typical TCP window size
	DefaultBufSize = 64 * 1024
)

// BufferPool manages reusable bufio.Reader and bufio.Writer instances
// I use sync.Pool to reduce GC pressure from frequent connection setup/teardown
type BufferPool struct {
	readers sync.Pool
	writers sync.Pool
}

// NewBufferPool creates a new buffer pool
func NewBufferPool() *BufferPool {
	return &BufferPool{
		readers: sync.Pool{
			New: func() interface{} {
				return bufio.NewReaderSize(nil, DefaultBufSize)
			},
		},
		writers: sync.Pool{
			New: func() interface{} {
				return bufio.NewWriterSize(nil, DefaultBufSize)
			},
		},
	}
}

// GetReader acquires a bufio.Reader from the pool and resets it
func (p *BufferPool) GetReader(r io.Reader) *bufio.Reader {
	br := p.readers.Get().(*bufio.Reader)
	br.Reset(r)
	return br
}

// PutReader returns a bufio.Reader to the pool
func (p *BufferPool) PutReader(br *bufio.Reader) {
	br.Reset(nil) // Release reference to underlying reader for GC
	p.readers.Put(br)
}

// GetWriter acquires a bufio.Writer from the pool and resets it
func (p *BufferPool) GetWriter(w io.Writer) *bufio.Writer {
	bw := p.writers.Get().(*bufio.Writer)
	bw.Reset(w)
	return bw
}

// PutWriter returns a bufio.Writer to the pool
func (p *BufferPool) PutWriter(bw *bufio.Writer) {
	bw.Reset(nil) // Release reference to underlying writer for GC
	p.writers.Put(bw)
}
