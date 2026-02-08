package persistence

import (
	"bufio"
	"context"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// AOFEntry represents a single command to be written to the AOF.
// I store the raw RESP-encoded bytes to avoid re-serialisation.
type AOFEntry struct {
	Data []byte
}

// AOFWriter manages the append-only file for command durability.
// I use a buffered channel to decouple command execution from disk I/O,
// so the hot path only pays the cost of a non-blocking channel send.
type AOFWriter struct {
	mu        sync.Mutex
	file      *os.File
	bufWriter *bufio.Writer
	ch        chan AOFEntry
	config    Config
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}

	// Rewrite coordination: when true, new entries are also buffered
	// in rewriteBuf for the rewriter to append after iteration completes
	rewriting  atomic.Bool
	rewriteMu  sync.Mutex
	rewriteBuf []AOFEntry

	// Metrics
	bytesWritten   atomic.Int64
	entriesWritten atomic.Int64
	lastFsync      atomic.Int64
}

// NewAOFWriter creates and starts the AOF writer goroutine.
func NewAOFWriter(cfg Config) (*AOFWriter, error) {
	f, err := os.OpenFile(cfg.AOFFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	a := &AOFWriter{
		file:      f,
		bufWriter: bufio.NewWriterSize(f, 256*1024),
		ch:        make(chan AOFEntry, cfg.AOFBufferSize),
		config:    cfg,
		ctx:       ctx,
		cancel:    cancel,
		done:      make(chan struct{}),
	}

	go a.run()

	if cfg.AOFPolicy == FsyncEverySecond {
		go a.fsyncLoop()
	}

	return a, nil
}

// Log sends a command to the AOF channel for async writing.
// I copy the data to avoid aliasing the connection's read buffer.
// The non-blocking send ensures the hot path is never stalled by I/O.
func (a *AOFWriter) Log(data []byte) {
	copied := make([]byte, len(data))
	copy(copied, data)

	entry := AOFEntry{Data: copied}

	// If a rewrite is in progress, also buffer for the rewriter
	if a.rewriting.Load() {
		a.rewriteMu.Lock()
		a.rewriteBuf = append(a.rewriteBuf, entry)
		a.rewriteMu.Unlock()
	}

	select {
	case a.ch <- entry:
	default:
		log.Println("WARN: AOF channel full, dropping entry")
	}
}

// run is the main goroutine that drains the channel and writes to disk.
// I batch writes by draining all available entries after the first one,
// which amortises the cost of Flush and Sync across many entries.
func (a *AOFWriter) run() {
	defer close(a.done)

	for {
		select {
		case entry := <-a.ch:
			a.mu.Lock()
			n, _ := a.bufWriter.Write(entry.Data)
			a.bytesWritten.Add(int64(n))
			a.entriesWritten.Add(1)

			// Drain any additional entries that are ready (batching)
			drain := len(a.ch)
			for i := 0; i < drain; i++ {
				e := <-a.ch
				n, _ = a.bufWriter.Write(e.Data)
				a.bytesWritten.Add(int64(n))
				a.entriesWritten.Add(1)
			}

			a.bufWriter.Flush()

			if a.config.AOFPolicy == FsyncAlways {
				a.file.Sync()
				a.lastFsync.Store(time.Now().UnixNano())
			}

			a.mu.Unlock()

		case <-a.ctx.Done():
			a.drainAndClose()
			return
		}
	}
}

// fsyncLoop runs the periodic fsync for FsyncEverySecond policy.
func (a *AOFWriter) fsyncLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			a.mu.Lock()
			a.bufWriter.Flush()
			a.file.Sync()
			a.lastFsync.Store(time.Now().UnixNano())
			a.mu.Unlock()
		case <-a.ctx.Done():
			return
		}
	}
}

// drainAndClose flushes remaining entries before shutting down.
func (a *AOFWriter) drainAndClose() {
	a.mu.Lock()
	defer a.mu.Unlock()

	for {
		select {
		case entry := <-a.ch:
			a.bufWriter.Write(entry.Data)
		default:
			a.bufWriter.Flush()
			a.file.Sync()
			a.file.Close()
			return
		}
	}
}

// Rotate replaces the AOF file with a new one (used during rewrite).
// The caller must ensure no concurrent Log calls reference the old file.
func (a *AOFWriter) Rotate(newPath string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Flush and close the old file
	a.bufWriter.Flush()
	a.file.Sync()
	a.file.Close()

	// Atomic rename: new file becomes the active AOF
	if err := os.Rename(newPath, a.config.AOFFilePath); err != nil {
		return err
	}

	// Open the renamed file for appending
	f, err := os.OpenFile(a.config.AOFFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	a.file = f
	a.bufWriter.Reset(f)

	return nil
}

// StartRewrite signals the AOF writer to begin buffering new entries
// for the rewriter to consume after iteration completes.
func (a *AOFWriter) StartRewrite() {
	a.rewriteMu.Lock()
	a.rewriteBuf = nil
	a.rewriteMu.Unlock()
	a.rewriting.Store(true)
}

// FinishRewrite stops buffering and returns the accumulated entries.
func (a *AOFWriter) FinishRewrite() []AOFEntry {
	a.rewriting.Store(false)
	a.rewriteMu.Lock()
	buf := a.rewriteBuf
	a.rewriteBuf = nil
	a.rewriteMu.Unlock()
	return buf
}

// Close flushes remaining entries, fsyncs, and closes the file.
func (a *AOFWriter) Close() error {
	a.cancel()
	<-a.done
	return nil
}

// FileSize returns the current AOF file size.
func (a *AOFWriter) FileSize() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	info, err := a.file.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

// IsRewriting returns true if an AOF rewrite is currently in progress.
func (a *AOFWriter) IsRewriting() bool {
	return a.rewriting.Load()
}

// Metrics returns AOF writer statistics.
func (a *AOFWriter) Metrics() (bytesWritten, entriesWritten int64) {
	return a.bytesWritten.Load(), a.entriesWritten.Load()
}
