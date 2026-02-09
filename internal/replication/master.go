package replication

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aravinth/distributed-cache/internal/persistence"
	"github.com/aravinth/distributed-cache/internal/protocol"
	"github.com/aravinth/distributed-cache/internal/store"
)

// SlaveConnState tracks a connected slave through the sync lifecycle.
type SlaveConnState int

const (
	SlaveStateHandshake   SlaveConnState = iota
	SlaveStateWaitBGSave                        // Waiting for snapshot to complete
	SlaveStateSendingBulk                       // Streaming snapshot file to slave
	SlaveStateOnline                            // Streaming live commands
)

// ConnectedSlave represents a single slave connected to this master.
// I track its state through the handshake and streaming phases.
type ConnectedSlave struct {
	mu       sync.Mutex
	id       uint64
	conn     net.Conn
	writer   *bufio.Writer
	reader   *protocol.Reader
	addr     string
	port     int // slave's listening port (from REPLCONF)
	state    SlaveConnState
	offset   atomic.Int64 // last ACK-ed offset from this slave
	lastPing time.Time
}

// MasterState manages all replication logic when this server is a master.
// I decouple command fan-out from the handler hot path by writing to
// the backlog (lock-free append) and having each slave's goroutine
// independently read from it, similar to how the AOFWriter decouples
// disk I/O from command execution.
type MasterState struct {
	replState  *ReplState
	backlog    *Backlog
	store      *store.ShardedMap
	snapEngine *persistence.SnapshotEngine
	pcfg       persistence.Config

	mu     sync.Mutex
	slaves map[uint64]*ConnectedSlave
	nextID atomic.Uint64

	// notify is a buffered channel (size 1). FeedCommand does a
	// non-blocking send to wake slave goroutines. Multiple feeds
	// between reads coalesce into a single wake-up.
	notify chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
}

// NewMasterState creates a new master replication state.
func NewMasterState(rs *ReplState, sm *store.ShardedMap, snap *persistence.SnapshotEngine, pcfg persistence.Config, backlogSize int) *MasterState {
	ctx, cancel := context.WithCancel(context.Background())
	return &MasterState{
		replState:  rs,
		backlog:    NewBacklog(backlogSize),
		store:      sm,
		snapEngine: snap,
		pcfg:       pcfg,
		slaves:     make(map[uint64]*ConnectedSlave),
		notify:     make(chan struct{}, 1),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Start launches the heartbeat goroutine.
func (m *MasterState) Start(ctx context.Context) {
	go m.heartbeatLoop(ctx)
}

// Stop terminates all slave connections and background goroutines.
func (m *MasterState) Stop() {
	m.cancel()

	m.mu.Lock()
	defer m.mu.Unlock()
	for id, slave := range m.slaves {
		slave.conn.Close()
		delete(m.slaves, id)
	}
}

// FeedCommand writes the RESP-encoded command bytes to the replication
// backlog and signals waiting slave goroutines. This is called from
// Handler.Execute() on the hot path — the non-blocking send ensures
// it never stalls command processing.
func (m *MasterState) FeedCommand(data []byte) {
	m.backlog.Write(data)
	m.replState.AddOffset(int64(len(data)))

	// Non-blocking notify: wake slave goroutines
	select {
	case m.notify <- struct{}{}:
	default:
	}
}

// HandlePSYNC processes the PSYNC handshake from a slave.
// It determines whether to do a full resync or partial resync, sends
// the appropriate response, then hands the connection to a streaming
// goroutine. The caller (server.go) should not use the connection after this.
func (m *MasterState) HandlePSYNC(conn net.Conn, reader *protocol.Reader, replID string, offset int64) {
	id := m.nextID.Add(1)

	slave := &ConnectedSlave{
		id:     id,
		conn:   conn,
		writer: bufio.NewWriterSize(conn, 256*1024),
		reader: reader,
		addr:   conn.RemoteAddr().String(),
		state:  SlaveStateHandshake,
	}

	m.mu.Lock()
	m.slaves[id] = slave
	m.mu.Unlock()

	// Determine sync type
	if replID == "?" || replID != m.replState.ReplID() || offset == -1 || !m.backlog.Available(offset) {
		go m.performFullSync(slave)
	} else {
		go m.performPartialSync(slave, offset)
	}
}

// HandleREPLCONF processes the REPLCONF sub-commands from a slave.
func (m *MasterState) HandleREPLCONF(args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return protocol.ErrWrongArgNum("REPLCONF")
	}

	subCmd := string(args[0].Bulk)
	switch subCmd {
	case "listening-port":
		// Noted but not used beyond INFO
		return protocol.ValOK
	case "capa":
		return protocol.ValOK
	case "ACK":
		// Slave is reporting its offset — handled per-slave in streaming goroutine
		return protocol.ValOK
	default:
		return protocol.ValOK
	}
}

// SlaveCount returns the number of connected slaves.
func (m *MasterState) SlaveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.slaves)
}

// SlaveInfo returns information about connected slaves for INFO output.
func (m *MasterState) SlaveInfo() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	info := make([]string, 0, len(m.slaves))
	i := 0
	for _, slave := range m.slaves {
		state := "online"
		switch slave.state {
		case SlaveStateHandshake:
			state = "handshake"
		case SlaveStateWaitBGSave:
			state = "wait_bgsave"
		case SlaveStateSendingBulk:
			state = "send_bulk"
		}
		info = append(info, fmt.Sprintf("slave%d:ip=%s,port=%d,state=%s,offset=%d,lag=0",
			i, slave.addr, slave.port, state, slave.offset.Load()))
		i++
	}
	return info
}

// performFullSync sends a snapshot followed by buffered and live commands.
func (m *MasterState) performFullSync(slave *ConnectedSlave) {
	log.Printf("replication: starting full sync for slave %s", slave.addr)

	slave.mu.Lock()
	slave.state = SlaveStateWaitBGSave
	slave.mu.Unlock()

	// Record the backlog offset before the snapshot. Any commands
	// arriving during the save will be in the backlog after this offset.
	preSnapOffset := m.backlog.CurrentOffset()

	// Send +FULLRESYNC response
	response := fmt.Sprintf("+FULLRESYNC %s %d\r\n", m.replState.ReplID(), preSnapOffset)
	if _, err := slave.conn.Write([]byte(response)); err != nil {
		log.Printf("replication: failed to send FULLRESYNC to %s: %v", slave.addr, err)
		m.removeSlave(slave.id)
		return
	}

	// Take a snapshot. I reuse the existing SnapshotEngine which writes
	// to disk, then stream the file to the slave. This avoids duplicating
	// the snapshot logic and the on-disk file serves as a backup too.
	var snapPath string
	var err error

	if m.snapEngine != nil {
		snapPath, err = m.snapEngine.Save()
		if err != nil {
			log.Printf("replication: snapshot failed for slave %s: %v", slave.addr, err)
			m.removeSlave(slave.id)
			return
		}
	} else {
		log.Printf("replication: no snapshot engine, cannot full sync slave %s", slave.addr)
		m.removeSlave(slave.id)
		return
	}

	slave.mu.Lock()
	slave.state = SlaveStateSendingBulk
	slave.mu.Unlock()

	// Stream the snapshot file as a RESP bulk string: $<len>\r\n<bytes>
	if err := m.sendSnapshotFile(slave, snapPath); err != nil {
		log.Printf("replication: failed to send snapshot to %s: %v", slave.addr, err)
		m.removeSlave(slave.id)
		return
	}

	// Send any backlog commands that accumulated during the snapshot
	if gap, err := m.backlog.Slice(preSnapOffset); err == nil && len(gap) > 0 {
		if _, err := slave.writer.Write(gap); err != nil {
			log.Printf("replication: failed to send backlog gap to %s: %v", slave.addr, err)
			m.removeSlave(slave.id)
			return
		}
		if err := slave.writer.Flush(); err != nil {
			log.Printf("replication: failed to flush backlog gap to %s: %v", slave.addr, err)
			m.removeSlave(slave.id)
			return
		}
	}

	slave.mu.Lock()
	slave.state = SlaveStateOnline
	slave.offset.Store(m.backlog.CurrentOffset())
	slave.mu.Unlock()

	log.Printf("replication: full sync complete for slave %s, entering streaming", slave.addr)

	m.streamToSlave(slave)
}

// performPartialSync sends only the missing backlog commands.
func (m *MasterState) performPartialSync(slave *ConnectedSlave, fromOffset int64) {
	log.Printf("replication: partial sync for slave %s from offset %d", slave.addr, fromOffset)

	response := "+CONTINUE\r\n"
	if _, err := slave.conn.Write([]byte(response)); err != nil {
		log.Printf("replication: failed to send CONTINUE to %s: %v", slave.addr, err)
		m.removeSlave(slave.id)
		return
	}

	// Send the gap
	gap, err := m.backlog.Slice(fromOffset)
	if err != nil {
		log.Printf("replication: backlog slice failed for %s: %v, falling back to full sync", slave.addr, err)
		m.performFullSync(slave)
		return
	}

	if len(gap) > 0 {
		if _, err := slave.writer.Write(gap); err != nil {
			log.Printf("replication: failed to send gap to %s: %v", slave.addr, err)
			m.removeSlave(slave.id)
			return
		}
		if err := slave.writer.Flush(); err != nil {
			log.Printf("replication: failed to flush gap to %s: %v", slave.addr, err)
			m.removeSlave(slave.id)
			return
		}
	}

	slave.mu.Lock()
	slave.state = SlaveStateOnline
	slave.offset.Store(m.backlog.CurrentOffset())
	slave.mu.Unlock()

	log.Printf("replication: partial sync complete for slave %s, entering streaming", slave.addr)

	m.streamToSlave(slave)
}

// sendSnapshotFile reads the snapshot from disk and sends it as a
// RESP bulk string: $<filesize>\r\n<raw bytes>
func (m *MasterState) sendSnapshotFile(slave *ConnectedSlave, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("opening snapshot: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat snapshot: %w", err)
	}

	// Write the bulk string header
	header := fmt.Sprintf("$%d\r\n", info.Size())
	if _, err := slave.writer.WriteString(header); err != nil {
		return fmt.Errorf("writing bulk header: %w", err)
	}

	// Stream the file contents
	if _, err := io.Copy(slave.writer, f); err != nil {
		return fmt.Errorf("streaming snapshot: %w", err)
	}

	// Flush to ensure everything reaches the slave
	if err := slave.writer.Flush(); err != nil {
		return fmt.Errorf("flushing snapshot: %w", err)
	}

	return nil
}

// streamToSlave is the long-running goroutine that streams live
// commands to a slave. It reads from the backlog whenever notified,
// and sends the new bytes to the slave.
func (m *MasterState) streamToSlave(slave *ConnectedSlave) {
	lastSent := slave.offset.Load()

	for {
		select {
		case <-m.notify:
			// New data available in the backlog
			data, err := m.backlog.Slice(lastSent)
			if err != nil {
				log.Printf("replication: slave %s fell behind, disconnecting: %v", slave.addr, err)
				m.removeSlave(slave.id)
				return
			}

			if len(data) == 0 {
				continue
			}

			slave.mu.Lock()
			if _, err := slave.writer.Write(data); err != nil {
				slave.mu.Unlock()
				log.Printf("replication: write to slave %s failed: %v", slave.addr, err)
				m.removeSlave(slave.id)
				return
			}
			if err := slave.writer.Flush(); err != nil {
				slave.mu.Unlock()
				log.Printf("replication: flush to slave %s failed: %v", slave.addr, err)
				m.removeSlave(slave.id)
				return
			}
			slave.mu.Unlock()

			lastSent = m.backlog.CurrentOffset()
			slave.offset.Store(lastSent)

		case <-m.ctx.Done():
			return
		}
	}
}

// removeSlave closes a slave connection and removes it from the registry.
func (m *MasterState) removeSlave(id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if slave, ok := m.slaves[id]; ok {
		slave.conn.Close()
		delete(m.slaves, id)
		log.Printf("replication: removed slave %s", slave.addr)
	}
}

// heartbeatLoop sends PING to all online slaves every 10 seconds.
// This serves as both a keepalive and a prompt for slaves to send
// their ACK offsets.
func (m *MasterState) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	ping := protocol.MarshalValue(protocol.ArrayVal([]protocol.Value{
		protocol.BulkStringVal([]byte("PING")),
	}))

	for {
		select {
		case <-ticker.C:
			m.mu.Lock()
			for _, slave := range m.slaves {
				if slave.state == SlaveStateOnline {
					slave.mu.Lock()
					slave.writer.Write(ping)
					slave.writer.Flush()
					slave.mu.Unlock()
				}
			}
			m.mu.Unlock()

		case <-ctx.Done():
			return
		}
	}
}
