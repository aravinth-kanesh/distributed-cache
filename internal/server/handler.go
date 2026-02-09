package server

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/aravinth/distributed-cache/internal/datatype"
	"github.com/aravinth/distributed-cache/internal/persistence"
	"github.com/aravinth/distributed-cache/internal/protocol"
	"github.com/aravinth/distributed-cache/internal/replication"
	"github.com/aravinth/distributed-cache/internal/store"
)

// CommandFunc is the signature for a command handler
type CommandFunc func(args []protocol.Value, conn *Connection) protocol.Value

// Handler routes RESP commands to the appropriate data type operations
type Handler struct {
	store     *store.ShardedMap
	strOps    *datatype.StringOps
	listOps   *datatype.ListOps
	hashOps   *datatype.HashOps
	setOps    *datatype.SetOps
	commands  map[string]CommandFunc
	startTime time.Time

	// Persistence — nil when persistence is disabled
	aofWriter      *persistence.AOFWriter
	snapshotEngine *persistence.SnapshotEngine
	aofRewriter    *persistence.AOFRewriter

	// Replication — nil when replication is not configured
	replState   *replication.ReplState
	masterState *replication.MasterState
	slaveState  *replication.SlaveState

	// Metrics — nil when metrics are disabled
	cmdCount    *prometheus.CounterVec
	cmdDuration *prometheus.HistogramVec
}

// NewHandler creates a new handler and registers all commands.
// The persistence and replication arguments are optional — pass nil to disable.
func NewHandler(sm *store.ShardedMap, aof *persistence.AOFWriter, snap *persistence.SnapshotEngine, rewriter *persistence.AOFRewriter, rs *replication.ReplState, master *replication.MasterState, slave *replication.SlaveState) *Handler {
	h := &Handler{
		store:          sm,
		strOps:         datatype.NewStringOps(sm),
		listOps:        datatype.NewListOps(sm),
		hashOps:        datatype.NewHashOps(sm),
		setOps:         datatype.NewSetOps(sm),
		startTime:      time.Now(),
		aofWriter:      aof,
		snapshotEngine: snap,
		aofRewriter:    rewriter,
		replState:      rs,
		masterState:    master,
		slaveState:     slave,
	}
	h.registerCommands()
	return h
}

// SetMetrics injects the Prometheus command counters into the handler.
// This is called after server creation so we avoid a circular dependency
// between the server and metrics packages.
func (h *Handler) SetMetrics(cc *prometheus.CounterVec, cd *prometheus.HistogramVec) {
	h.cmdCount = cc
	h.cmdDuration = cd
}

// StartTime returns when this handler was created (used by the metrics
// collector to compute uptime).
func (h *Handler) StartTime() time.Time {
	return h.startTime
}

// Execute parses the command from a RESP array and dispatches it
func (h *Handler) Execute(val protocol.Value, conn *Connection) protocol.Value {
	if val.Type != protocol.Array || len(val.Array) == 0 {
		return protocol.ErrorVal("ERR invalid command format")
	}

	cmdName := strings.ToUpper(string(val.Array[0].Bulk))
	args := val.Array[1:]

	// Slave read-only guard: reject mutating commands when operating
	// as a slave. I check this before dispatch so no individual command
	// handler needs to know about replication roles. REPLICAOF, INFO,
	// REPLCONF, and PSYNC are always allowed.
	if h.replState != nil && h.replState.Role() == replication.RoleSlave && persistence.IsMutating(cmdName) {
		if cmdName != "REPLICAOF" && cmdName != "SLAVEOF" {
			return protocol.ErrorVal("READONLY You can't write against a read only replica.")
		}
	}

	fn, exists := h.commands[cmdName]
	if !exists {
		return protocol.ErrUnknownCmd(cmdName)
	}

	// Track command latency when metrics are enabled
	var start time.Time
	if h.cmdDuration != nil {
		start = time.Now()
	}

	result := fn(args, conn)

	// Metrics hook: count commands and observe latency. The nil guard
	// keeps the handler working without Prometheus.
	if h.cmdCount != nil {
		h.cmdCount.WithLabelValues(cmdName).Inc()
	}
	if h.cmdDuration != nil {
		h.cmdDuration.WithLabelValues(cmdName).Observe(time.Since(start).Seconds())
	}

	// AOF hook: log successful mutating commands for durability.
	// I serialise the original RESP array (not the result) so the AOF
	// is a faithful replay log. The non-blocking channel send in
	// AOFWriter.Log ensures this never stalls the hot path.
	if h.aofWriter != nil && persistence.IsMutating(cmdName) && result.Type != protocol.Error {
		h.aofWriter.Log(protocol.MarshalValue(val))
	}

	// Replication hook: feed successful mutating commands to connected
	// slaves. I serialise the same RESP bytes that the AOF uses,
	// keeping the two pipelines consistent.
	if h.masterState != nil && persistence.IsMutating(cmdName) && result.Type != protocol.Error {
		h.masterState.FeedCommand(protocol.MarshalValue(val))
	}

	return result
}

func (h *Handler) registerCommands() {
	h.commands = map[string]CommandFunc{
		// Server/Connection commands
		"PING":     h.cmdPing,
		"ECHO":     h.cmdEcho,
		"QUIT":     h.cmdQuit,
		"SELECT":   h.cmdSelect,
		"INFO":     h.cmdInfo,
		"COMMAND":  h.cmdCommand,
		"DBSIZE":   h.cmdDBSize,
		"FLUSHDB":  h.cmdFlushDB,
		"FLUSHALL": h.cmdFlushDB,

		// Key management commands
		"DEL":     h.cmdDel,
		"EXISTS":  h.cmdExists,
		"EXPIRE":  h.cmdExpire,
		"PEXPIRE": h.cmdPExpire,
		"TTL":     h.cmdTTL,
		"PTTL":    h.cmdPTTL,
		"PERSIST": h.cmdPersist,
		"TYPE":    h.cmdType,
		"RENAME":  h.cmdRename,
		"KEYS":    h.cmdKeys,

		// String commands
		"GET":         h.cmdGet,
		"SET":         h.cmdSet,
		"SETNX":       h.cmdSetNX,
		"GETSET":      h.cmdGetSet,
		"MGET":        h.cmdMGet,
		"MSET":        h.cmdMSet,
		"INCR":        h.cmdIncr,
		"INCRBY":      h.cmdIncrBy,
		"DECR":        h.cmdDecr,
		"DECRBY":      h.cmdDecrBy,
		"INCRBYFLOAT": h.cmdIncrByFloat,
		"APPEND":      h.cmdAppend,
		"STRLEN":      h.cmdStrLen,
		"GETRANGE":    h.cmdGetRange,
		"SETRANGE":    h.cmdSetRange,

		// List commands
		"LPUSH":   h.cmdLPush,
		"RPUSH":   h.cmdRPush,
		"LPOP":    h.cmdLPop,
		"RPOP":    h.cmdRPop,
		"LLEN":    h.cmdLLen,
		"LINDEX":  h.cmdLIndex,
		"LRANGE":  h.cmdLRange,
		"LSET":    h.cmdLSet,
		"LTRIM":   h.cmdLTrim,
		"LREM":    h.cmdLRem,
		"LINSERT": h.cmdLInsert,
		"LPUSHX":  h.cmdLPushX,
		"RPUSHX":  h.cmdRPushX,
		"LMOVE":   h.cmdLMove,

		// Hash commands
		"HSET":         h.cmdHSet,
		"HGET":         h.cmdHGet,
		"HGETALL":      h.cmdHGetAll,
		"HDEL":         h.cmdHDel,
		"HEXISTS":      h.cmdHExists,
		"HLEN":         h.cmdHLen,
		"HKEYS":        h.cmdHKeys,
		"HVALS":        h.cmdHVals,
		"HMGET":        h.cmdHMGet,
		"HMSET":        h.cmdHMSet,
		"HSETNX":       h.cmdHSetNX,
		"HINCRBY":      h.cmdHIncrBy,
		"HINCRBYFLOAT": h.cmdHIncrByFloat,
		"HSTRLEN":      h.cmdHStrLen,

		// Set commands
		"SADD":        h.cmdSAdd,
		"SREM":        h.cmdSRem,
		"SISMEMBER":   h.cmdSIsMember,
		"SMISMEMBER":  h.cmdSMIsMember,
		"SMEMBERS":    h.cmdSMembers,
		"SCARD":       h.cmdSCard,
		"SPOP":        h.cmdSPop,
		"SRANDMEMBER": h.cmdSRandMember,
		"SMOVE":       h.cmdSMove,
		"SUNION":      h.cmdSUnion,
		"SUNIONSTORE": h.cmdSUnionStore,
		"SINTER":      h.cmdSInter,
		"SINTERSTORE": h.cmdSInterStore,
		"SINTERCARD":  h.cmdSInterCard,
		"SDIFF":       h.cmdSDiff,
		"SDIFFSTORE":  h.cmdSDiffStore,

		// Persistence commands
		"BGSAVE":        h.cmdBGSave,
		"BGREWRITEAOF":  h.cmdBGRewriteAOF,
		"LASTSAVE":      h.cmdLastSave,
		"PEXPIREAT":     h.cmdPExpireAt,

		// Replication commands
		"REPLICAOF": h.cmdReplicaOf,
		"SLAVEOF":   h.cmdReplicaOf,
		"REPLCONF":  h.cmdReplConf,
		"PSYNC":     h.cmdPSync,
	}
}

// ==================== Server commands ====================

func (h *Handler) cmdPing(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) == 0 {
		return protocol.ValPong
	}
	if len(args) == 1 {
		return protocol.BulkStringVal(args[0].Bulk)
	}
	return protocol.ErrWrongArgNum("PING")
}

func (h *Handler) cmdEcho(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrWrongArgNum("ECHO")
	}
	return protocol.BulkStringVal(args[0].Bulk)
}

func (h *Handler) cmdQuit(args []protocol.Value, conn *Connection) protocol.Value {
	return protocol.ValOK
}

func (h *Handler) cmdSelect(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrWrongArgNum("SELECT")
	}
	if string(args[0].Bulk) != "0" {
		return protocol.ErrorVal("ERR DB index is out of range")
	}
	return protocol.ValOK
}

func (h *Handler) cmdDBSize(args []protocol.Value, conn *Connection) protocol.Value {
	return protocol.IntegerVal(h.store.KeyCount())
}

func (h *Handler) cmdFlushDB(args []protocol.Value, conn *Connection) protocol.Value {
	h.store.Clear()
	return protocol.ValOK
}

func (h *Handler) cmdInfo(args []protocol.Value, conn *Connection) protocol.Value {
	metrics := h.store.GetMetrics()
	uptime := int64(time.Since(h.startTime).Seconds())

	info := fmt.Sprintf("# Server\r\n"+
		"dcache_version:0.1.0\r\n"+
		"uptime_in_seconds:%d\r\n"+
		"uptime_in_days:%d\r\n"+
		"\r\n# Stats\r\n"+
		"total_commands_processed:%d\r\n"+
		"total_connections_received:0\r\n"+
		"\r\n# Keyspace\r\n"+
		"db0:keys=%d,expires=0\r\n"+
		"\r\n# Memory\r\n"+
		"used_memory:0\r\n"+
		"\r\n# Clients\r\n"+
		"connected_clients:0\r\n"+
		"\r\n# Cache\r\n"+
		"cache_hits:%d\r\n"+
		"cache_misses:%d\r\n"+
		"total_gets:%d\r\n"+
		"total_sets:%d\r\n"+
		"total_deletes:%d\r\n",
		uptime,
		uptime/86400,
		metrics["gets"]+metrics["sets"]+metrics["deletes"],
		metrics["keys"],
		metrics["hits"],
		metrics["misses"],
		metrics["gets"],
		metrics["sets"],
		metrics["deletes"],
	)

	// Replication section
	if h.replState != nil {
		role := h.replState.Role()
		info += "\r\n# Replication\r\n"
		info += fmt.Sprintf("role:%s\r\n", role.String())
		info += fmt.Sprintf("master_replid:%s\r\n", h.replState.ReplID())
		info += fmt.Sprintf("master_repl_offset:%d\r\n", h.replState.Offset())

		if role == replication.RoleMaster && h.masterState != nil {
			info += fmt.Sprintf("connected_slaves:%d\r\n", h.masterState.SlaveCount())
			for _, line := range h.masterState.SlaveInfo() {
				info += line + "\r\n"
			}
		} else if role == replication.RoleSlave {
			host, port := h.replState.MasterAddr()
			info += fmt.Sprintf("master_host:%s\r\n", host)
			info += fmt.Sprintf("master_port:%d\r\n", port)
			linkStatus := "down"
			if h.slaveState != nil && h.slaveState.IsConnected() {
				linkStatus = "up"
			}
			info += fmt.Sprintf("master_link_status:%s\r\n", linkStatus)
			info += fmt.Sprintf("slave_repl_offset:%d\r\n", h.replState.Offset())
		}
	}

	return protocol.BulkStringVal([]byte(info))
}

func (h *Handler) cmdCommand(args []protocol.Value, conn *Connection) protocol.Value {
	// Return empty array to satisfy redis-cli startup handshake
	return protocol.ValEmptyArray
}

// ==================== Key management commands ====================

func (h *Handler) cmdDel(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) == 0 {
		return protocol.ErrWrongArgNum("DEL")
	}
	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = string(a.Bulk)
	}
	deleted := h.store.DeleteMulti(keys...)
	return protocol.IntegerVal(deleted)
}

func (h *Handler) cmdExists(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) == 0 {
		return protocol.ErrWrongArgNum("EXISTS")
	}
	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = string(a.Bulk)
	}
	count := h.store.ExistsMulti(keys...)
	return protocol.IntegerVal(count)
}

func (h *Handler) cmdExpire(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 2 {
		return protocol.ErrWrongArgNum("EXPIRE")
	}
	key := string(args[0].Bulk)
	seconds, err := strconv.ParseInt(string(args[1].Bulk), 10, 64)
	if err != nil {
		return protocol.ErrNotInteger
	}
	entry, exists := h.store.GetEntry(key)
	if !exists {
		return protocol.ValZero
	}
	entry.SetTTL(seconds)
	return protocol.ValOne
}

func (h *Handler) cmdPExpire(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 2 {
		return protocol.ErrWrongArgNum("PEXPIRE")
	}
	key := string(args[0].Bulk)
	ms, err := strconv.ParseInt(string(args[1].Bulk), 10, 64)
	if err != nil {
		return protocol.ErrNotInteger
	}
	entry, exists := h.store.GetEntry(key)
	if !exists {
		return protocol.ValZero
	}
	entry.ExpiresAt = time.Now().Add(time.Duration(ms) * time.Millisecond).UnixNano()
	return protocol.ValOne
}

func (h *Handler) cmdTTL(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrWrongArgNum("TTL")
	}
	entry, exists := h.store.GetEntry(string(args[0].Bulk))
	if !exists {
		return protocol.ValNegTwo
	}
	return protocol.IntegerVal(entry.TTL())
}

func (h *Handler) cmdPTTL(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrWrongArgNum("PTTL")
	}
	entry, exists := h.store.GetEntry(string(args[0].Bulk))
	if !exists {
		return protocol.ValNegTwo
	}
	return protocol.IntegerVal(entry.PTTL())
}

func (h *Handler) cmdPersist(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrWrongArgNum("PERSIST")
	}
	entry, exists := h.store.GetEntry(string(args[0].Bulk))
	if !exists {
		return protocol.ValZero
	}
	if entry.Persist() {
		return protocol.ValOne
	}
	return protocol.ValZero
}

func (h *Handler) cmdType(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrWrongArgNum("TYPE")
	}
	t := h.store.Type(string(args[0].Bulk))
	if t == store.EntryType(255) {
		return protocol.SimpleStringVal("none")
	}
	return protocol.SimpleStringVal(t.String())
}

func (h *Handler) cmdRename(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 2 {
		return protocol.ErrWrongArgNum("RENAME")
	}
	if !h.store.Rename(string(args[0].Bulk), string(args[1].Bulk)) {
		return protocol.ErrNoSuchKey
	}
	return protocol.ValOK
}

func (h *Handler) cmdKeys(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrWrongArgNum("KEYS")
	}
	pattern := string(args[0].Bulk)
	allKeys := h.store.Keys()

	if pattern == "*" {
		vals := make([]protocol.Value, len(allKeys))
		for i, k := range allKeys {
			vals[i] = protocol.BulkStringVal([]byte(k))
		}
		return protocol.ArrayVal(vals)
	}

	var matched []protocol.Value
	for _, k := range allKeys {
		if matchPattern(pattern, k) {
			matched = append(matched, protocol.BulkStringVal([]byte(k)))
		}
	}
	if matched == nil {
		matched = []protocol.Value{}
	}
	return protocol.ArrayVal(matched)
}

// ==================== String commands ====================

func (h *Handler) cmdGet(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrWrongArgNum("GET")
	}
	val, ok := h.strOps.Get(string(args[0].Bulk))
	if !ok {
		return protocol.ValNullBulk
	}
	return protocol.BulkStringVal(val)
}

func (h *Handler) cmdSet(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) < 2 {
		return protocol.ErrWrongArgNum("SET")
	}
	key := string(args[0].Bulk)
	value := args[1].Bulk

	var ttlSeconds int64
	var nx, xx bool

	i := 2
	for i < len(args) {
		flag := strings.ToUpper(string(args[i].Bulk))
		switch flag {
		case "EX":
			i++
			if i >= len(args) {
				return protocol.ErrSyntax
			}
			secs, err := strconv.ParseInt(string(args[i].Bulk), 10, 64)
			if err != nil {
				return protocol.ErrNotInteger
			}
			ttlSeconds = secs
		case "PX":
			i++
			if i >= len(args) {
				return protocol.ErrSyntax
			}
			ms, err := strconv.ParseInt(string(args[i].Bulk), 10, 64)
			if err != nil {
				return protocol.ErrNotInteger
			}
			ttlSeconds = (ms + 999) / 1000
		case "NX":
			nx = true
		case "XX":
			xx = true
		default:
			return protocol.ErrSyntax
		}
		i++
	}

	if nx && xx {
		return protocol.ErrSyntax
	}

	if nx {
		if h.strOps.SetNX(key, value) {
			return protocol.ValOK
		}
		return protocol.ValNullBulk
	}

	if xx {
		if h.strOps.SetXX(key, value) {
			return protocol.ValOK
		}
		return protocol.ValNullBulk
	}

	if ttlSeconds > 0 {
		h.strOps.SetWithTTL(key, value, ttlSeconds)
	} else {
		h.strOps.Set(key, value)
	}

	return protocol.ValOK
}

func (h *Handler) cmdSetNX(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 2 {
		return protocol.ErrWrongArgNum("SETNX")
	}
	if h.strOps.SetNX(string(args[0].Bulk), args[1].Bulk) {
		return protocol.ValOne
	}
	return protocol.ValZero
}

func (h *Handler) cmdGetSet(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 2 {
		return protocol.ErrWrongArgNum("GETSET")
	}
	old, ok := h.strOps.GetSet(string(args[0].Bulk), args[1].Bulk)
	if !ok {
		return protocol.ValNullBulk
	}
	return protocol.BulkStringVal(old)
}

func (h *Handler) cmdMGet(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) == 0 {
		return protocol.ErrWrongArgNum("MGET")
	}
	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = string(a.Bulk)
	}
	values := h.strOps.MGet(keys...)
	result := make([]protocol.Value, len(values))
	for i, v := range values {
		if v == nil {
			result[i] = protocol.ValNullBulk
		} else {
			result[i] = protocol.BulkStringVal(v)
		}
	}
	return protocol.ArrayVal(result)
}

func (h *Handler) cmdMSet(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) < 2 || len(args)%2 != 0 {
		return protocol.ErrWrongArgNum("MSET")
	}
	pairs := make(map[string][]byte, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		pairs[string(args[i].Bulk)] = args[i+1].Bulk
	}
	h.strOps.MSet(pairs)
	return protocol.ValOK
}

func (h *Handler) cmdIncr(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrWrongArgNum("INCR")
	}
	val, ok := h.strOps.Incr(string(args[0].Bulk))
	if !ok {
		return protocol.ErrNotInteger
	}
	return protocol.IntegerVal(val)
}

func (h *Handler) cmdIncrBy(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 2 {
		return protocol.ErrWrongArgNum("INCRBY")
	}
	delta, err := strconv.ParseInt(string(args[1].Bulk), 10, 64)
	if err != nil {
		return protocol.ErrNotInteger
	}
	val, ok := h.strOps.IncrBy(string(args[0].Bulk), delta)
	if !ok {
		return protocol.ErrNotInteger
	}
	return protocol.IntegerVal(val)
}

func (h *Handler) cmdDecr(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrWrongArgNum("DECR")
	}
	val, ok := h.strOps.Decr(string(args[0].Bulk))
	if !ok {
		return protocol.ErrNotInteger
	}
	return protocol.IntegerVal(val)
}

func (h *Handler) cmdDecrBy(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 2 {
		return protocol.ErrWrongArgNum("DECRBY")
	}
	delta, err := strconv.ParseInt(string(args[1].Bulk), 10, 64)
	if err != nil {
		return protocol.ErrNotInteger
	}
	val, ok := h.strOps.DecrBy(string(args[0].Bulk), delta)
	if !ok {
		return protocol.ErrNotInteger
	}
	return protocol.IntegerVal(val)
}

func (h *Handler) cmdIncrByFloat(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 2 {
		return protocol.ErrWrongArgNum("INCRBYFLOAT")
	}
	delta, err := strconv.ParseFloat(string(args[1].Bulk), 64)
	if err != nil {
		return protocol.ErrNotFloat
	}
	val, ok := h.strOps.IncrByFloat(string(args[0].Bulk), delta)
	if !ok {
		return protocol.ErrNotFloat
	}
	return protocol.BulkStringVal([]byte(strconv.FormatFloat(val, 'f', -1, 64)))
}

func (h *Handler) cmdAppend(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 2 {
		return protocol.ErrWrongArgNum("APPEND")
	}
	length := h.strOps.Append(string(args[0].Bulk), args[1].Bulk)
	if length == -1 {
		return protocol.ErrWrongType
	}
	return protocol.IntegerVal(int64(length))
}

func (h *Handler) cmdStrLen(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrWrongArgNum("STRLEN")
	}
	length := h.strOps.StrLen(string(args[0].Bulk))
	if length == -1 {
		return protocol.ErrWrongType
	}
	return protocol.IntegerVal(int64(length))
}

func (h *Handler) cmdGetRange(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 3 {
		return protocol.ErrWrongArgNum("GETRANGE")
	}
	start, err1 := strconv.Atoi(string(args[1].Bulk))
	end, err2 := strconv.Atoi(string(args[2].Bulk))
	if err1 != nil || err2 != nil {
		return protocol.ErrNotInteger
	}
	val := h.strOps.GetRange(string(args[0].Bulk), start, end)
	return protocol.BulkStringVal(val)
}

func (h *Handler) cmdSetRange(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 3 {
		return protocol.ErrWrongArgNum("SETRANGE")
	}
	offset, err := strconv.Atoi(string(args[1].Bulk))
	if err != nil {
		return protocol.ErrNotInteger
	}
	length := h.strOps.SetRange(string(args[0].Bulk), offset, args[2].Bulk)
	return protocol.IntegerVal(int64(length))
}

// ==================== List commands ====================

func (h *Handler) cmdLPush(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) < 2 {
		return protocol.ErrWrongArgNum("LPUSH")
	}
	key := string(args[0].Bulk)
	values := make([][]byte, len(args)-1)
	for i := 1; i < len(args); i++ {
		values[i-1] = args[i].Bulk
	}
	length := h.listOps.LPush(key, values...)
	if length == -1 {
		return protocol.ErrWrongType
	}
	return protocol.IntegerVal(length)
}

func (h *Handler) cmdRPush(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) < 2 {
		return protocol.ErrWrongArgNum("RPUSH")
	}
	key := string(args[0].Bulk)
	values := make([][]byte, len(args)-1)
	for i := 1; i < len(args); i++ {
		values[i-1] = args[i].Bulk
	}
	length := h.listOps.RPush(key, values...)
	if length == -1 {
		return protocol.ErrWrongType
	}
	return protocol.IntegerVal(length)
}

func (h *Handler) cmdLPop(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrWrongArgNum("LPOP")
	}
	val, ok := h.listOps.LPop(string(args[0].Bulk))
	if !ok {
		return protocol.ValNullBulk
	}
	return protocol.BulkStringVal(val)
}

func (h *Handler) cmdRPop(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrWrongArgNum("RPOP")
	}
	val, ok := h.listOps.RPop(string(args[0].Bulk))
	if !ok {
		return protocol.ValNullBulk
	}
	return protocol.BulkStringVal(val)
}

func (h *Handler) cmdLLen(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrWrongArgNum("LLEN")
	}
	length := h.listOps.LLen(string(args[0].Bulk))
	if length == -1 {
		return protocol.ErrWrongType
	}
	return protocol.IntegerVal(length)
}

func (h *Handler) cmdLIndex(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 2 {
		return protocol.ErrWrongArgNum("LINDEX")
	}
	index, err := strconv.ParseInt(string(args[1].Bulk), 10, 64)
	if err != nil {
		return protocol.ErrNotInteger
	}
	val, ok := h.listOps.LIndex(string(args[0].Bulk), index)
	if !ok {
		return protocol.ValNullBulk
	}
	return protocol.BulkStringVal(val)
}

func (h *Handler) cmdLRange(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 3 {
		return protocol.ErrWrongArgNum("LRANGE")
	}
	start, err1 := strconv.ParseInt(string(args[1].Bulk), 10, 64)
	stop, err2 := strconv.ParseInt(string(args[2].Bulk), 10, 64)
	if err1 != nil || err2 != nil {
		return protocol.ErrNotInteger
	}
	elements := h.listOps.LRange(string(args[0].Bulk), start, stop)
	result := make([]protocol.Value, len(elements))
	for i, e := range elements {
		result[i] = protocol.BulkStringVal(e)
	}
	return protocol.ArrayVal(result)
}

func (h *Handler) cmdLSet(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 3 {
		return protocol.ErrWrongArgNum("LSET")
	}
	index, err := strconv.ParseInt(string(args[1].Bulk), 10, 64)
	if err != nil {
		return protocol.ErrNotInteger
	}
	if !h.listOps.LSet(string(args[0].Bulk), index, args[2].Bulk) {
		return protocol.ErrorVal("ERR index out of range")
	}
	return protocol.ValOK
}

func (h *Handler) cmdLTrim(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 3 {
		return protocol.ErrWrongArgNum("LTRIM")
	}
	start, err1 := strconv.ParseInt(string(args[1].Bulk), 10, 64)
	stop, err2 := strconv.ParseInt(string(args[2].Bulk), 10, 64)
	if err1 != nil || err2 != nil {
		return protocol.ErrNotInteger
	}
	h.listOps.LTrim(string(args[0].Bulk), start, stop)
	return protocol.ValOK
}

func (h *Handler) cmdLRem(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 3 {
		return protocol.ErrWrongArgNum("LREM")
	}
	count, err := strconv.ParseInt(string(args[1].Bulk), 10, 64)
	if err != nil {
		return protocol.ErrNotInteger
	}
	removed := h.listOps.LRem(string(args[0].Bulk), count, args[2].Bulk)
	return protocol.IntegerVal(removed)
}

func (h *Handler) cmdLInsert(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 4 {
		return protocol.ErrWrongArgNum("LINSERT")
	}
	pos := strings.ToUpper(string(args[1].Bulk))
	var before bool
	switch pos {
	case "BEFORE":
		before = true
	case "AFTER":
		before = false
	default:
		return protocol.ErrSyntax
	}
	length := h.listOps.LInsert(string(args[0].Bulk), before, args[2].Bulk, args[3].Bulk)
	return protocol.IntegerVal(length)
}

func (h *Handler) cmdLPushX(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) < 2 {
		return protocol.ErrWrongArgNum("LPUSHX")
	}
	key := string(args[0].Bulk)
	values := make([][]byte, len(args)-1)
	for i := 1; i < len(args); i++ {
		values[i-1] = args[i].Bulk
	}
	return protocol.IntegerVal(h.listOps.LPushX(key, values...))
}

func (h *Handler) cmdRPushX(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) < 2 {
		return protocol.ErrWrongArgNum("RPUSHX")
	}
	key := string(args[0].Bulk)
	values := make([][]byte, len(args)-1)
	for i := 1; i < len(args); i++ {
		values[i-1] = args[i].Bulk
	}
	return protocol.IntegerVal(h.listOps.RPushX(key, values...))
}

func (h *Handler) cmdLMove(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 4 {
		return protocol.ErrWrongArgNum("LMOVE")
	}
	srcDir := strings.ToUpper(string(args[2].Bulk))
	dstDir := strings.ToUpper(string(args[3].Bulk))
	if (srcDir != "LEFT" && srcDir != "RIGHT") || (dstDir != "LEFT" && dstDir != "RIGHT") {
		return protocol.ErrSyntax
	}
	val, ok := h.listOps.LMove(string(args[0].Bulk), string(args[1].Bulk), srcDir, dstDir)
	if !ok {
		return protocol.ValNullBulk
	}
	return protocol.BulkStringVal(val)
}

// ==================== Hash commands ====================

func (h *Handler) cmdHSet(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return protocol.ErrWrongArgNum("HSET")
	}
	key := string(args[0].Bulk)
	fieldValues := make([][]byte, len(args)-1)
	for i := 1; i < len(args); i++ {
		fieldValues[i-1] = args[i].Bulk
	}
	added := h.hashOps.HSet(key, fieldValues...)
	if added == -1 {
		return protocol.ErrWrongType
	}
	return protocol.IntegerVal(int64(added))
}

func (h *Handler) cmdHGet(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 2 {
		return protocol.ErrWrongArgNum("HGET")
	}
	val, ok := h.hashOps.HGet(string(args[0].Bulk), string(args[1].Bulk))
	if !ok {
		return protocol.ValNullBulk
	}
	return protocol.BulkStringVal(val)
}

func (h *Handler) cmdHGetAll(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrWrongArgNum("HGETALL")
	}
	all := h.hashOps.HGetAll(string(args[0].Bulk))
	result := make([]protocol.Value, 0, len(all)*2)
	for k, v := range all {
		result = append(result, protocol.BulkStringVal([]byte(k)))
		result = append(result, protocol.BulkStringVal(v))
	}
	return protocol.ArrayVal(result)
}

func (h *Handler) cmdHDel(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) < 2 {
		return protocol.ErrWrongArgNum("HDEL")
	}
	key := string(args[0].Bulk)
	fields := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		fields[i-1] = string(args[i].Bulk)
	}
	deleted := h.hashOps.HDel(key, fields...)
	return protocol.IntegerVal(int64(deleted))
}

func (h *Handler) cmdHExists(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 2 {
		return protocol.ErrWrongArgNum("HEXISTS")
	}
	if h.hashOps.HExists(string(args[0].Bulk), string(args[1].Bulk)) {
		return protocol.ValOne
	}
	return protocol.ValZero
}

func (h *Handler) cmdHLen(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrWrongArgNum("HLEN")
	}
	length := h.hashOps.HLen(string(args[0].Bulk))
	if length == -1 {
		return protocol.ErrWrongType
	}
	return protocol.IntegerVal(int64(length))
}

func (h *Handler) cmdHKeys(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrWrongArgNum("HKEYS")
	}
	keys := h.hashOps.HKeys(string(args[0].Bulk))
	result := make([]protocol.Value, len(keys))
	for i, k := range keys {
		result[i] = protocol.BulkStringVal([]byte(k))
	}
	return protocol.ArrayVal(result)
}

func (h *Handler) cmdHVals(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrWrongArgNum("HVALS")
	}
	vals := h.hashOps.HVals(string(args[0].Bulk))
	result := make([]protocol.Value, len(vals))
	for i, v := range vals {
		result[i] = protocol.BulkStringVal(v)
	}
	return protocol.ArrayVal(result)
}

func (h *Handler) cmdHMGet(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) < 2 {
		return protocol.ErrWrongArgNum("HMGET")
	}
	key := string(args[0].Bulk)
	fields := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		fields[i-1] = string(args[i].Bulk)
	}
	values := h.hashOps.HMGet(key, fields...)
	result := make([]protocol.Value, len(values))
	for i, v := range values {
		if v == nil {
			result[i] = protocol.ValNullBulk
		} else {
			result[i] = protocol.BulkStringVal(v)
		}
	}
	return protocol.ArrayVal(result)
}

func (h *Handler) cmdHMSet(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return protocol.ErrWrongArgNum("HMSET")
	}
	key := string(args[0].Bulk)
	pairs := make(map[string][]byte, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		pairs[string(args[i].Bulk)] = args[i+1].Bulk
	}
	h.hashOps.HMSet(key, pairs)
	return protocol.ValOK
}

func (h *Handler) cmdHSetNX(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 3 {
		return protocol.ErrWrongArgNum("HSETNX")
	}
	if h.hashOps.HSetNX(string(args[0].Bulk), string(args[1].Bulk), args[2].Bulk) {
		return protocol.ValOne
	}
	return protocol.ValZero
}

func (h *Handler) cmdHIncrBy(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 3 {
		return protocol.ErrWrongArgNum("HINCRBY")
	}
	delta, err := strconv.ParseInt(string(args[2].Bulk), 10, 64)
	if err != nil {
		return protocol.ErrNotInteger
	}
	val, ok := h.hashOps.HIncrBy(string(args[0].Bulk), string(args[1].Bulk), delta)
	if !ok {
		return protocol.ErrNotInteger
	}
	return protocol.IntegerVal(val)
}

func (h *Handler) cmdHIncrByFloat(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 3 {
		return protocol.ErrWrongArgNum("HINCRBYFLOAT")
	}
	delta, err := strconv.ParseFloat(string(args[2].Bulk), 64)
	if err != nil {
		return protocol.ErrNotFloat
	}
	val, ok := h.hashOps.HIncrByFloat(string(args[0].Bulk), string(args[1].Bulk), delta)
	if !ok {
		return protocol.ErrNotFloat
	}
	return protocol.BulkStringVal([]byte(strconv.FormatFloat(val, 'f', -1, 64)))
}

func (h *Handler) cmdHStrLen(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 2 {
		return protocol.ErrWrongArgNum("HSTRLEN")
	}
	return protocol.IntegerVal(int64(h.hashOps.HStrLen(string(args[0].Bulk), string(args[1].Bulk))))
}

// ==================== Set commands ====================

func (h *Handler) cmdSAdd(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) < 2 {
		return protocol.ErrWrongArgNum("SADD")
	}
	key := string(args[0].Bulk)
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = string(args[i].Bulk)
	}
	added := h.setOps.SAdd(key, members...)
	if added == -1 {
		return protocol.ErrWrongType
	}
	return protocol.IntegerVal(int64(added))
}

func (h *Handler) cmdSRem(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) < 2 {
		return protocol.ErrWrongArgNum("SREM")
	}
	key := string(args[0].Bulk)
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = string(args[i].Bulk)
	}
	removed := h.setOps.SRem(key, members...)
	return protocol.IntegerVal(int64(removed))
}

func (h *Handler) cmdSIsMember(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 2 {
		return protocol.ErrWrongArgNum("SISMEMBER")
	}
	if h.setOps.SIsMember(string(args[0].Bulk), string(args[1].Bulk)) {
		return protocol.ValOne
	}
	return protocol.ValZero
}

func (h *Handler) cmdSMIsMember(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) < 2 {
		return protocol.ErrWrongArgNum("SMISMEMBER")
	}
	key := string(args[0].Bulk)
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = string(args[i].Bulk)
	}
	results := h.setOps.SMIsMember(key, members...)
	vals := make([]protocol.Value, len(results))
	for i, r := range results {
		if r {
			vals[i] = protocol.ValOne
		} else {
			vals[i] = protocol.ValZero
		}
	}
	return protocol.ArrayVal(vals)
}

func (h *Handler) cmdSMembers(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrWrongArgNum("SMEMBERS")
	}
	members := h.setOps.SMembers(string(args[0].Bulk))
	result := make([]protocol.Value, len(members))
	for i, m := range members {
		result[i] = protocol.BulkStringVal([]byte(m))
	}
	return protocol.ArrayVal(result)
}

func (h *Handler) cmdSCard(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 1 {
		return protocol.ErrWrongArgNum("SCARD")
	}
	card := h.setOps.SCard(string(args[0].Bulk))
	if card == -1 {
		return protocol.ErrWrongType
	}
	return protocol.IntegerVal(int64(card))
}

func (h *Handler) cmdSPop(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) < 1 || len(args) > 2 {
		return protocol.ErrWrongArgNum("SPOP")
	}
	key := string(args[0].Bulk)

	if len(args) == 2 {
		count, err := strconv.Atoi(string(args[1].Bulk))
		if err != nil {
			return protocol.ErrNotInteger
		}
		members := h.setOps.SPopN(key, count)
		result := make([]protocol.Value, len(members))
		for i, m := range members {
			result[i] = protocol.BulkStringVal([]byte(m))
		}
		return protocol.ArrayVal(result)
	}

	member, ok := h.setOps.SPop(key)
	if !ok {
		return protocol.ValNullBulk
	}
	return protocol.BulkStringVal([]byte(member))
}

func (h *Handler) cmdSRandMember(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) < 1 || len(args) > 2 {
		return protocol.ErrWrongArgNum("SRANDMEMBER")
	}
	key := string(args[0].Bulk)

	count := 1
	returnArray := false
	if len(args) == 2 {
		var err error
		count, err = strconv.Atoi(string(args[1].Bulk))
		if err != nil {
			return protocol.ErrNotInteger
		}
		returnArray = true
	}

	members := h.setOps.SRandMember(key, count)

	if returnArray {
		result := make([]protocol.Value, len(members))
		for i, m := range members {
			result[i] = protocol.BulkStringVal([]byte(m))
		}
		return protocol.ArrayVal(result)
	}

	if len(members) == 0 {
		return protocol.ValNullBulk
	}
	return protocol.BulkStringVal([]byte(members[0]))
}

func (h *Handler) cmdSMove(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 3 {
		return protocol.ErrWrongArgNum("SMOVE")
	}
	if h.setOps.SMove(string(args[0].Bulk), string(args[1].Bulk), string(args[2].Bulk)) {
		return protocol.ValOne
	}
	return protocol.ValZero
}

func (h *Handler) cmdSUnion(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) == 0 {
		return protocol.ErrWrongArgNum("SUNION")
	}
	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = string(a.Bulk)
	}
	members := h.setOps.SUnion(keys...)
	result := make([]protocol.Value, len(members))
	for i, m := range members {
		result[i] = protocol.BulkStringVal([]byte(m))
	}
	return protocol.ArrayVal(result)
}

func (h *Handler) cmdSUnionStore(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) < 2 {
		return protocol.ErrWrongArgNum("SUNIONSTORE")
	}
	dst := string(args[0].Bulk)
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = string(args[i].Bulk)
	}
	count := h.setOps.SUnionStore(dst, keys...)
	return protocol.IntegerVal(int64(count))
}

func (h *Handler) cmdSInter(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) == 0 {
		return protocol.ErrWrongArgNum("SINTER")
	}
	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = string(a.Bulk)
	}
	members := h.setOps.SInter(keys...)
	result := make([]protocol.Value, len(members))
	for i, m := range members {
		result[i] = protocol.BulkStringVal([]byte(m))
	}
	return protocol.ArrayVal(result)
}

func (h *Handler) cmdSInterStore(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) < 2 {
		return protocol.ErrWrongArgNum("SINTERSTORE")
	}
	dst := string(args[0].Bulk)
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = string(args[i].Bulk)
	}
	count := h.setOps.SInterStore(dst, keys...)
	return protocol.IntegerVal(int64(count))
}

func (h *Handler) cmdSInterCard(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) < 2 {
		return protocol.ErrWrongArgNum("SINTERCARD")
	}
	numkeys, err := strconv.Atoi(string(args[0].Bulk))
	if err != nil {
		return protocol.ErrNotInteger
	}
	if numkeys < 1 || numkeys+1 > len(args) {
		return protocol.ErrSyntax
	}
	keys := make([]string, numkeys)
	for i := 0; i < numkeys; i++ {
		keys[i] = string(args[i+1].Bulk)
	}
	limit := 0
	remaining := args[numkeys+1:]
	if len(remaining) == 2 && strings.ToUpper(string(remaining[0].Bulk)) == "LIMIT" {
		limit, err = strconv.Atoi(string(remaining[1].Bulk))
		if err != nil {
			return protocol.ErrNotInteger
		}
	}
	count := h.setOps.SInterCard(limit, keys...)
	return protocol.IntegerVal(int64(count))
}

func (h *Handler) cmdSDiff(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) == 0 {
		return protocol.ErrWrongArgNum("SDIFF")
	}
	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = string(a.Bulk)
	}
	members := h.setOps.SDiff(keys...)
	result := make([]protocol.Value, len(members))
	for i, m := range members {
		result[i] = protocol.BulkStringVal([]byte(m))
	}
	return protocol.ArrayVal(result)
}

func (h *Handler) cmdSDiffStore(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) < 2 {
		return protocol.ErrWrongArgNum("SDIFFSTORE")
	}
	dst := string(args[0].Bulk)
	keys := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys[i-1] = string(args[i].Bulk)
	}
	count := h.setOps.SDiffStore(dst, keys...)
	return protocol.IntegerVal(int64(count))
}

// ==================== Persistence commands ====================

func (h *Handler) cmdBGSave(args []protocol.Value, conn *Connection) protocol.Value {
	if h.snapshotEngine == nil {
		return protocol.ErrorVal("ERR snapshots are disabled")
	}
	if h.snapshotEngine.IsSaving() {
		return protocol.ErrorVal("ERR background save already in progress")
	}
	go func() {
		if _, err := h.snapshotEngine.Save(); err != nil {
			fmt.Printf("BGSAVE error: %v\n", err)
		}
	}()
	return protocol.SimpleStringVal("Background saving started")
}

func (h *Handler) cmdBGRewriteAOF(args []protocol.Value, conn *Connection) protocol.Value {
	if h.aofRewriter == nil {
		return protocol.ErrorVal("ERR AOF is disabled")
	}
	if h.aofWriter != nil && h.aofWriter.IsRewriting() {
		return protocol.ErrorVal("ERR AOF rewrite already in progress")
	}
	go func() {
		if err := h.aofRewriter.Rewrite(); err != nil {
			fmt.Printf("BGREWRITEAOF error: %v\n", err)
		}
	}()
	return protocol.SimpleStringVal("Background AOF rewrite started")
}

func (h *Handler) cmdLastSave(args []protocol.Value, conn *Connection) protocol.Value {
	if h.snapshotEngine == nil {
		return protocol.IntegerVal(0)
	}
	return protocol.IntegerVal(h.snapshotEngine.LastSaveTime())
}

func (h *Handler) cmdPExpireAt(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 2 {
		return protocol.ErrWrongArgNum("PEXPIREAT")
	}
	key := string(args[0].Bulk)
	ms, err := strconv.ParseInt(string(args[1].Bulk), 10, 64)
	if err != nil {
		return protocol.ErrNotInteger
	}
	entry, exists := h.store.GetEntry(key)
	if !exists {
		return protocol.ValZero
	}
	entry.ExpiresAt = ms * 1e6 // Milliseconds to nanoseconds
	return protocol.ValOne
}

// ==================== Replication commands ====================

func (h *Handler) cmdReplicaOf(args []protocol.Value, conn *Connection) protocol.Value {
	if len(args) != 2 {
		return protocol.ErrWrongArgNum("REPLICAOF")
	}

	host := string(args[0].Bulk)
	portStr := string(args[1].Bulk)

	// REPLICAOF NO ONE — promote to master
	if strings.ToUpper(host) == "NO" && strings.ToUpper(portStr) == "ONE" {
		if h.replState == nil {
			return protocol.ValOK
		}
		if h.slaveState != nil {
			h.slaveState.Disconnect()
		}
		h.replState.SetRole(replication.RoleMaster)
		h.replState.SetReplID(replication.GenerateReplID())
		return protocol.ValOK
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return protocol.ErrorVal("ERR invalid port number")
	}

	if h.replState == nil {
		return protocol.ErrorVal("ERR replication not initialised")
	}

	// Disconnect from current master if already a slave
	if h.slaveState != nil {
		h.slaveState.Disconnect()
	}

	h.slaveState.ConnectToMaster(host, port)
	return protocol.ValOK
}

func (h *Handler) cmdReplConf(args []protocol.Value, conn *Connection) protocol.Value {
	if h.masterState != nil {
		return h.masterState.HandleREPLCONF(args)
	}
	return protocol.ValOK
}

func (h *Handler) cmdPSync(args []protocol.Value, conn *Connection) protocol.Value {
	if h.masterState == nil {
		return protocol.ErrorVal("ERR this server is not a master")
	}
	if len(args) != 2 {
		return protocol.ErrWrongArgNum("PSYNC")
	}

	replID := string(args[0].Bulk)
	offset, err := strconv.ParseInt(string(args[1].Bulk), 10, 64)
	if err != nil {
		return protocol.ErrorVal("ERR invalid offset")
	}

	// Flush any pending response before hijacking
	conn.Flush()

	// Hijack the connection: the replication subsystem takes ownership.
	// The server's handleConnection loop will exit without closing it.
	conn.hijacked.Store(true)

	h.masterState.HandlePSYNC(conn.RawConn(), conn.Reader(), replID, offset)

	// Return a special empty value — the connection is no longer ours
	return protocol.Value{}
}

// ==================== Utility ====================

// matchPattern matches a simple glob pattern (supports * and ?)
func matchPattern(pattern, s string) bool {
	if pattern == "*" {
		return true
	}
	pi, si := 0, 0
	starIdx, matchIdx := -1, 0
	for si < len(s) {
		if pi < len(pattern) && (pattern[pi] == '?' || pattern[pi] == s[si]) {
			pi++
			si++
		} else if pi < len(pattern) && pattern[pi] == '*' {
			starIdx = pi
			matchIdx = si
			pi++
		} else if starIdx != -1 {
			pi = starIdx + 1
			matchIdx++
			si = matchIdx
		} else {
			return false
		}
	}
	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}
	return pi == len(pattern)
}
