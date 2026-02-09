package replication

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aravinth/distributed-cache/internal/datatype"
	"github.com/aravinth/distributed-cache/internal/persistence"
	"github.com/aravinth/distributed-cache/internal/protocol"
	"github.com/aravinth/distributed-cache/internal/store"
)

// StoreApplier replays commands into the store without triggering
// AOF logging or further replication. I reuse the shared
// ExecuteReplayCommand function from the persistence package to
// avoid duplicating the ~200-line command switch statement.
type StoreApplier struct {
	store   *store.ShardedMap
	strOps  *datatype.StringOps
	listOps *datatype.ListOps
	hashOps *datatype.HashOps
	setOps  *datatype.SetOps
}

// NewStoreApplier creates a new applier for the given store.
func NewStoreApplier(sm *store.ShardedMap) *StoreApplier {
	return &StoreApplier{
		store:   sm,
		strOps:  datatype.NewStringOps(sm),
		listOps: datatype.NewListOps(sm),
		hashOps: datatype.NewHashOps(sm),
		setOps:  datatype.NewSetOps(sm),
	}
}

// ApplyCommand executes a single command into the store.
func (a *StoreApplier) ApplyCommand(cmd string, args []protocol.Value) error {
	return persistence.ExecuteReplayCommand(a.store, a.strOps, a.listOps, a.hashOps, a.setOps, cmd, args)
}

// SlaveState manages the outgoing connection from this server to its
// master. I run the entire replication lifecycle in a single goroutine,
// with automatic reconnection on failure using exponential backoff.
type SlaveState struct {
	replState *ReplState
	store     *store.ShardedMap
	pcfg      persistence.Config
	applier   *StoreApplier

	mu        sync.Mutex
	conn      net.Conn
	reader    *protocol.Reader
	writer    *protocol.Writer
	connected bool

	listenPort int // this server's listening port, sent to master via REPLCONF

	ctx    context.Context
	cancel context.CancelFunc
}

// NewSlaveState creates a new slave replication state.
func NewSlaveState(rs *ReplState, sm *store.ShardedMap, pcfg persistence.Config, listenPort int) *SlaveState {
	return &SlaveState{
		replState:  rs,
		store:      sm,
		pcfg:       pcfg,
		applier:    NewStoreApplier(sm),
		listenPort: listenPort,
	}
}

// ConnectToMaster initiates the replication connection. The actual
// connection and sync happen asynchronously in a background goroutine.
func (s *SlaveState) ConnectToMaster(host string, port int) {
	s.replState.SetRole(RoleSlave)
	s.replState.SetMasterAddr(host, port)

	ctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.ctx = ctx
	s.cancel = cancel
	s.mu.Unlock()

	go s.run(host, port)
}

// Disconnect stops the replication connection.
func (s *SlaveState) Disconnect() {
	s.mu.Lock()
	if s.cancel != nil {
		s.cancel()
	}
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
	s.connected = false
	s.mu.Unlock()
}

// IsConnected returns true if the slave is connected to its master.
func (s *SlaveState) IsConnected() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connected
}

// run is the main replication goroutine. It connects to the master,
// performs the handshake, receives the snapshot (or partial resync),
// then enters the streaming phase. On any error, it reconnects with
// exponential backoff.
func (s *SlaveState) run(host string, port int) {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	backoff := time.Second

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		log.Printf("replication: connecting to master at %s", addr)

		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			log.Printf("replication: connection to %s failed: %v, retrying in %v", addr, err, backoff)
			select {
			case <-time.After(backoff):
			case <-s.ctx.Done():
				return
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}

		// Connection established — reset backoff
		backoff = time.Second

		s.mu.Lock()
		s.conn = conn
		s.reader = protocol.NewReader(conn)
		s.writer = protocol.NewWriter(conn)
		s.connected = true
		s.mu.Unlock()

		log.Printf("replication: connected to master at %s", addr)

		if err := s.handshake(); err != nil {
			log.Printf("replication: handshake failed: %v", err)
			s.closeConn()
			continue
		}

		// Enter the streaming phase — this blocks until error or shutdown
		if err := s.receiveStream(); err != nil {
			log.Printf("replication: stream error: %v", err)
		}

		s.closeConn()
	}
}

// handshake performs the PSYNC handshake with the master.
func (s *SlaveState) handshake() error {
	// Step 1: PING
	if err := s.sendCommand("PING"); err != nil {
		return fmt.Errorf("sending PING: %w", err)
	}
	resp, err := s.readLine()
	if err != nil {
		return fmt.Errorf("reading PING response: %w", err)
	}
	if !strings.HasPrefix(resp, "+PONG") && !strings.HasPrefix(resp, "+") {
		return fmt.Errorf("unexpected PING response: %s", resp)
	}

	// Step 2: REPLCONF listening-port
	if err := s.sendCommand("REPLCONF", "listening-port", strconv.Itoa(s.listenPort)); err != nil {
		return fmt.Errorf("sending REPLCONF listening-port: %w", err)
	}
	resp, err = s.readLine()
	if err != nil {
		return fmt.Errorf("reading REPLCONF response: %w", err)
	}
	if !strings.HasPrefix(resp, "+OK") {
		return fmt.Errorf("unexpected REPLCONF response: %s", resp)
	}

	// Step 3: REPLCONF capa
	if err := s.sendCommand("REPLCONF", "capa", "eof"); err != nil {
		return fmt.Errorf("sending REPLCONF capa: %w", err)
	}
	resp, err = s.readLine()
	if err != nil {
		return fmt.Errorf("reading REPLCONF capa response: %w", err)
	}
	if !strings.HasPrefix(resp, "+OK") {
		return fmt.Errorf("unexpected REPLCONF capa response: %s", resp)
	}

	// Step 4: PSYNC
	replID := s.replState.ReplID()
	offset := s.replState.Offset()

	// On first connection (or after promotion), use ? and -1
	if s.replState.Role() == RoleSlave && offset == 0 {
		replID = "?"
		offset = -1
	}

	if err := s.sendCommand("PSYNC", replID, strconv.FormatInt(offset, 10)); err != nil {
		return fmt.Errorf("sending PSYNC: %w", err)
	}

	resp, err = s.readLine()
	if err != nil {
		return fmt.Errorf("reading PSYNC response: %w", err)
	}

	if strings.HasPrefix(resp, "+FULLRESYNC") {
		// +FULLRESYNC <replID> <offset>
		parts := strings.Fields(resp[1:])
		if len(parts) >= 3 {
			s.replState.SetReplID(parts[1])
			if off, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
				s.replState.SetOffset(off)
			}
		}

		log.Printf("replication: full sync from master, replID=%s", s.replState.ReplID())

		if err := s.receiveFullSync(); err != nil {
			return fmt.Errorf("full sync: %w", err)
		}
	} else if strings.HasPrefix(resp, "+CONTINUE") {
		log.Printf("replication: partial sync from master")
	} else {
		return fmt.Errorf("unexpected PSYNC response: %s", resp)
	}

	return nil
}

// receiveFullSync receives the snapshot bulk string from the master
// and loads it into the store.
func (s *SlaveState) receiveFullSync() error {
	// Read the bulk string header: $<len>\r\n
	resp, err := s.readLine()
	if err != nil {
		return fmt.Errorf("reading snapshot header: %w", err)
	}

	if !strings.HasPrefix(resp, "$") {
		return fmt.Errorf("expected bulk string for snapshot, got: %s", resp)
	}

	size, err := strconv.ParseInt(resp[1:], 10, 64)
	if err != nil {
		return fmt.Errorf("parsing snapshot size: %w", err)
	}

	log.Printf("replication: receiving snapshot (%d bytes)", size)

	// Read exactly size bytes of snapshot data
	snapData := make([]byte, size)
	if _, err := io.ReadFull(s.conn, snapData); err != nil {
		return fmt.Errorf("reading snapshot data: %w", err)
	}

	// Clear the store before loading the snapshot
	s.store.Clear()

	// Load the snapshot using the persistence recovery logic
	recovery := persistence.NewRecovery(s.store, s.pcfg)
	keysLoaded, err := recovery.LoadSnapshotFromReader(bytes.NewReader(snapData))
	if err != nil {
		return fmt.Errorf("loading snapshot: %w", err)
	}

	log.Printf("replication: snapshot loaded, %d keys", keysLoaded)
	return nil
}

// receiveStream reads RESP commands from the master and applies them.
// This runs until the connection is closed or an error occurs.
func (s *SlaveState) receiveStream() error {
	for {
		select {
		case <-s.ctx.Done():
			return nil
		default:
		}

		val, err := s.reader.ReadValue()
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("master closed connection")
			}
			return fmt.Errorf("reading command: %w", err)
		}

		if val.Type != protocol.Array || len(val.Array) == 0 {
			continue
		}

		cmd := strings.ToUpper(string(val.Array[0].Bulk))
		args := val.Array[1:]

		// Handle PING from master (keepalive)
		if cmd == "PING" {
			// Respond with REPLCONF ACK
			s.sendCommand("REPLCONF", "ACK", strconv.FormatInt(s.replState.Offset(), 10))
			continue
		}

		// Handle REPLCONF GETACK from master
		if cmd == "REPLCONF" && len(args) >= 1 && strings.ToUpper(string(args[0].Bulk)) == "GETACK" {
			s.sendCommand("REPLCONF", "ACK", strconv.FormatInt(s.replState.Offset(), 10))
			continue
		}

		// Apply the command to the store
		if err := s.applier.ApplyCommand(cmd, args); err != nil {
			log.Printf("replication: failed to apply %s: %v", cmd, err)
		}

		// Advance the offset by the byte size of the command
		cmdBytes := protocol.MarshalValue(val)
		s.replState.AddOffset(int64(len(cmdBytes)))
	}
}

// sendCommand sends a RESP array command to the master.
func (s *SlaveState) sendCommand(parts ...string) error {
	vals := make([]protocol.Value, len(parts))
	for i, p := range parts {
		vals[i] = protocol.BulkStringVal([]byte(p))
	}
	s.writer.WriteValue(protocol.ArrayVal(vals))
	return s.writer.Flush()
}

// readLine reads a single \r\n-terminated line from the connection.
// I use this for the handshake phase where I need to parse simple
// RESP responses without the full protocol reader.
func (s *SlaveState) readLine() (string, error) {
	// Use a bufio.Reader for line reading during handshake
	br := bufio.NewReader(s.conn)
	line, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	// Re-create the protocol reader with the bufio reader so it
	// picks up any buffered data
	s.reader = protocol.NewReaderFromBufio(br)
	return strings.TrimRight(line, "\r\n"), nil
}

// closeConn closes the connection and updates state.
func (s *SlaveState) closeConn() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
	s.connected = false
}
