package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aravinth/distributed-cache/internal/persistence"
	"github.com/aravinth/distributed-cache/internal/pool"
	"github.com/aravinth/distributed-cache/internal/replication"
	"github.com/aravinth/distributed-cache/internal/store"
)

// Config holds the server configuration
type Config struct {
	Port           int
	MaxConnections int
	IdleTimeout    time.Duration
}

// DefaultConfig returns sensible defaults
func DefaultConfig() Config {
	return Config{
		Port:           6379,
		MaxConnections: 10000,
		IdleTimeout:    300 * time.Second,
	}
}

// Server listens for TCP connections and dispatches commands
type Server struct {
	config   Config
	listener net.Listener
	store    *store.ShardedMap
	handler  *Handler
	bufPool  *pool.BufferPool

	// Connection tracking
	connMu   sync.Mutex
	conns    map[uint64]*Connection
	nextID   atomic.Uint64
	connWg   sync.WaitGroup
	shutdown atomic.Bool

	// Metrics
	totalConns atomic.Uint64

	// Replication — nil when not configured
	masterState *replication.MasterState
	slaveState  *replication.SlaveState
}

// New creates a new server with the given config and backing store.
// The persistence and replication arguments are optional — pass nil to disable.
func New(cfg Config, sm *store.ShardedMap, aof *persistence.AOFWriter, snap *persistence.SnapshotEngine, rewriter *persistence.AOFRewriter, rs *replication.ReplState, master *replication.MasterState, slave *replication.SlaveState) *Server {
	return &Server{
		config:      cfg,
		store:       sm,
		handler:     NewHandler(sm, aof, snap, rewriter, rs, master, slave),
		bufPool:     pool.NewBufferPool(),
		conns:       make(map[uint64]*Connection),
		masterState: master,
		slaveState:  slave,
	}
}

// ListenAndServe starts the TCP listener and blocks until the context is cancelled
func (s *Server) ListenAndServe(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.config.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to bind to %s: %w", addr, err)
	}
	s.listener = ln

	log.Printf("dcache listening on %s", addr)

	// Spawn the accept loop in a goroutine so we can watch for cancellation
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.acceptLoop(ctx)
	}()

	select {
	case <-ctx.Done():
		log.Println("shutdown signal received, draining connections...")
		return s.Shutdown()
	case err := <-errCh:
		return err
	}
}

// acceptLoop accepts new connections until the listener is closed
func (s *Server) acceptLoop(ctx context.Context) error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.shutdown.Load() {
				return nil // Graceful shutdown
			}
			// Temporary errors (e.g. too many open files) - back off briefly
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			// If the context is done, this is expected
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("accept error: %w", err)
		}

		// Enforce max connections
		s.connMu.Lock()
		if len(s.conns) >= s.config.MaxConnections {
			s.connMu.Unlock()
			conn.Close()
			continue
		}
		s.connMu.Unlock()

		s.connWg.Add(1)
		go s.handleConnection(ctx, conn)
	}
}

// handleConnection manages a single client connection's lifecycle
func (s *Server) handleConnection(ctx context.Context, rawConn net.Conn) {
	defer s.connWg.Done()

	id := s.nextID.Add(1)
	s.totalConns.Add(1)

	// Get pooled buffers
	br := s.bufPool.GetReader(rawConn)
	bw := s.bufPool.GetWriter(rawConn)
	conn := NewConnection(id, rawConn, br, bw)

	// Register connection
	s.connMu.Lock()
	s.conns[id] = conn
	s.connMu.Unlock()

	defer func() {
		if conn.hijacked.Load() {
			// The replication subsystem has taken ownership of this
			// connection. I remove it from tracking but do NOT close
			// it — the master's streaming goroutine owns the socket.
			s.connMu.Lock()
			delete(s.conns, id)
			s.connMu.Unlock()
			return
		}

		// Flush any buffered writes before closing
		conn.Flush()

		// Unregister connection
		s.connMu.Lock()
		delete(s.conns, id)
		s.connMu.Unlock()

		// Return buffers to pool
		s.bufPool.PutReader(br)
		s.bufPool.PutWriter(bw)

		conn.Close()
	}()

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Set idle timeout so reads don't block indefinitely
		if s.config.IdleTimeout > 0 {
			conn.SetReadDeadline(s.config.IdleTimeout)
		}

		val, err := conn.ReadCommand()
		if err != nil {
			if err == io.EOF {
				return // Client disconnected cleanly
			}
			if s.shutdown.Load() {
				return
			}
			// Timeout or broken connection - close silently
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				return
			}
			// Closed connection
			if strings.Contains(err.Error(), "use of closed network connection") {
				return
			}
			return
		}

		// Update last active timestamp
		conn.lastActive.Store(time.Now().UnixNano())

		// Execute the command
		resp := s.handler.Execute(val, conn)

		// Check if the connection was hijacked by the replication
		// subsystem (PSYNC command). If so, exit the loop — the
		// master's streaming goroutine now owns this connection.
		if conn.hijacked.Load() {
			return
		}

		// Write response
		if err := conn.WriteResponse(resp); err != nil {
			return
		}
		if err := conn.Flush(); err != nil {
			return
		}

		// Handle QUIT command - send OK first, then close
		if val.Type == 0x2a && len(val.Array) > 0 {
			cmdName := strings.ToUpper(string(val.Array[0].Bulk))
			if cmdName == "QUIT" {
				return
			}
		}
	}
}

// Shutdown gracefully stops the server
func (s *Server) Shutdown() error {
	s.shutdown.Store(true)

	// Stop replication subsystems
	if s.slaveState != nil {
		s.slaveState.Disconnect()
	}
	if s.masterState != nil {
		s.masterState.Stop()
	}

	// Stop accepting new connections
	if s.listener != nil {
		s.listener.Close()
	}

	// Close all active connections
	s.connMu.Lock()
	for _, conn := range s.conns {
		conn.Close()
	}
	s.connMu.Unlock()

	// Wait for all connection goroutines to finish
	s.connWg.Wait()

	log.Println("server shut down cleanly")
	return nil
}

// ActiveConnections returns the number of currently connected clients
func (s *Server) ActiveConnections() int {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return len(s.conns)
}

// TotalConnections returns the total number of connections accepted since startup
func (s *Server) TotalConnections() uint64 {
	return s.totalConns.Load()
}

// Handler returns the command handler so callers can inject metrics.
func (s *Server) Handler() *Handler {
	return s.handler
}
