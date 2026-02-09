package replication

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"sync/atomic"
)

// Role represents whether this server is operating as a master or slave.
type Role int32

const (
	RoleMaster Role = iota
	RoleSlave
)

// String returns a human-readable role name.
func (r Role) String() string {
	switch r {
	case RoleMaster:
		return "master"
	case RoleSlave:
		return "slave"
	default:
		return "unknown"
	}
}

// ReplState holds the replication identity shared by both roles.
// I generate a fresh replID on startup (or inherit one from a master
// during full sync) so that slaves can detect dataset lineage changes.
type ReplState struct {
	// role uses atomic access for the hot path (Execute guard).
	// 0 = master, 1 = slave.
	role atomic.Int32

	// mu protects replID, masterHost, masterPort — these change rarely
	// (only on REPLICAOF or initial sync).
	mu         sync.RWMutex
	replID     string
	masterHost string
	masterPort int

	// offset tracks the replication stream position.
	// Master: total bytes produced. Slave: total bytes consumed.
	offset atomic.Int64
}

// NewReplState creates a new replication state with a fresh replID
// and the master role (default).
func NewReplState() *ReplState {
	rs := &ReplState{
		replID: GenerateReplID(),
	}
	rs.role.Store(int32(RoleMaster))
	return rs
}

// Role returns the current replication role.
func (rs *ReplState) Role() Role {
	return Role(rs.role.Load())
}

// SetRole atomically switches the replication role.
func (rs *ReplState) SetRole(r Role) {
	rs.role.Store(int32(r))
}

// ReplID returns the current replication ID.
func (rs *ReplState) ReplID() string {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.replID
}

// SetReplID updates the replication ID (e.g. inherited from master).
func (rs *ReplState) SetReplID(id string) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.replID = id
}

// Offset returns the current replication offset.
func (rs *ReplState) Offset() int64 {
	return rs.offset.Load()
}

// AddOffset atomically increments the offset by n bytes.
func (rs *ReplState) AddOffset(n int64) {
	rs.offset.Add(n)
}

// SetOffset sets the replication offset to a specific value.
func (rs *ReplState) SetOffset(n int64) {
	rs.offset.Store(n)
}

// MasterAddr returns the master's host and port.
func (rs *ReplState) MasterAddr() (string, int) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.masterHost, rs.masterPort
}

// SetMasterAddr updates the master address.
func (rs *ReplState) SetMasterAddr(host string, port int) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.masterHost = host
	rs.masterPort = port
}

// GenerateReplID produces a 40-character hex string from 20 random bytes,
// matching the format Redis uses for replication IDs.
func GenerateReplID() string {
	b := make([]byte, 20)
	if _, err := rand.Read(b); err != nil {
		// Fallback: this should never happen, but if it does I use
		// a deterministic placeholder rather than panicking.
		return "0000000000000000000000000000000000000000"
	}
	return hex.EncodeToString(b)
}
