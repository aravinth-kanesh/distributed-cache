package persistence

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aravinth/distributed-cache/internal/datatype"
	"github.com/aravinth/distributed-cache/internal/protocol"
	"github.com/aravinth/distributed-cache/internal/store"
)

// Recovery orchestrates the startup recovery process:
// 1. Find and load the latest snapshot (if any)
// 2. Replay AOF entries that arrived after the snapshot
// 3. Report statistics (keys loaded, time taken)
type Recovery struct {
	store  *store.ShardedMap
	config Config

	// Operation handlers for AOF replay — I call directly into the
	// store/datatype layer rather than going through the full server
	// handler to avoid re-logging commands to the AOF.
	strOps  *datatype.StringOps
	listOps *datatype.ListOps
	hashOps *datatype.HashOps
	setOps  *datatype.SetOps
}

// NewRecovery creates a new recovery orchestrator.
func NewRecovery(sm *store.ShardedMap, cfg Config) *Recovery {
	return &Recovery{
		store:   sm,
		config:  cfg,
		strOps:  datatype.NewStringOps(sm),
		listOps: datatype.NewListOps(sm),
		hashOps: datatype.NewHashOps(sm),
		setOps:  datatype.NewSetOps(sm),
	}
}

// Run executes the full recovery process.
// Returns the number of keys loaded and any error.
func (r *Recovery) Run() (int64, error) {
	start := time.Now()
	var totalKeys int64

	// Step 1: Load latest snapshot
	snapshotPath := r.findLatestSnapshot()
	if snapshotPath != "" {
		keysLoaded, err := r.loadSnapshot(snapshotPath)
		if err != nil {
			return 0, fmt.Errorf("loading snapshot %s: %w", snapshotPath, err)
		}
		totalKeys = keysLoaded
		log.Printf("recovery: loaded %d keys from snapshot %s", keysLoaded, snapshotPath)
	}

	// Step 2: Replay AOF (contains only commands after the last snapshot)
	if r.config.AOFEnabled {
		if _, err := os.Stat(r.config.AOFFilePath); err == nil {
			replayed, err := r.replayAOF(r.config.AOFFilePath)
			if err != nil {
				return totalKeys, fmt.Errorf("replaying AOF: %w", err)
			}
			if replayed > 0 {
				log.Printf("recovery: replayed %d commands from AOF", replayed)
			}
			totalKeys = r.store.KeyCount()
		}
	}

	log.Printf("recovery: complete in %v, %d keys loaded", time.Since(start).Round(time.Millisecond), totalKeys)
	return totalKeys, nil
}

// findLatestSnapshot finds the most recent snapshot file.
// First checks for the -latest.rdb symlink, then scans the directory.
func (r *Recovery) findLatestSnapshot() string {
	latestLink := filepath.Join(r.config.SnapshotDir, r.config.SnapshotPrefix+"-latest.rdb")
	if target, err := os.Readlink(latestLink); err == nil {
		// Resolve relative symlink
		if !filepath.IsAbs(target) {
			target = filepath.Join(r.config.SnapshotDir, target)
		}
		if _, err := os.Stat(target); err == nil {
			return target
		}
	}

	// Fallback: scan directory for .rdb files
	entries, err := os.ReadDir(r.config.SnapshotDir)
	if err != nil {
		return ""
	}

	var latest string
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, r.config.SnapshotPrefix+"-") &&
			strings.HasSuffix(name, ".rdb") &&
			!strings.HasSuffix(name, "-latest.rdb") {
			candidate := filepath.Join(r.config.SnapshotDir, name)
			if candidate > latest {
				latest = candidate
			}
		}
	}

	return latest
}

// loadSnapshot decodes a binary snapshot file into the store.
func (r *Recovery) loadSnapshot(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	dec := NewSnapshotDecoder(f)

	_, _, err = dec.ReadHeader()
	if err != nil {
		return 0, fmt.Errorf("reading header: %w", err)
	}

	var keysLoaded int64

	for {
		entryType, key, expiresAt, data, err := dec.ReadEntry()
		if err != nil {
			return keysLoaded, fmt.Errorf("reading entry: %w", err)
		}

		if entryType == SnapTypeEOF {
			break
		}

		// Skip entries that have already expired
		if expiresAt > 0 && time.Now().UnixNano() > expiresAt {
			continue
		}

		switch entryType {
		case SnapTypeString:
			val := data.([]byte)
			entry := store.NewEntry(store.TypeString, val)
			entry.ExpiresAt = expiresAt
			r.store.Set(key, entry)

		case SnapTypeList:
			elements := data.([][]byte)
			list := datatype.NewListValue()
			entry := store.NewEntry(store.TypeList, list)
			entry.ExpiresAt = expiresAt
			r.store.Set(key, entry)
			// Populate list via RPush to maintain order
			r.listOps.RPush(key, elements...)

		case SnapTypeHash:
			fields := data.(map[string][]byte)
			hash := datatype.NewHashValue()
			entry := store.NewEntry(store.TypeHash, hash)
			entry.ExpiresAt = expiresAt
			r.store.Set(key, entry)
			// Populate hash
			fvs := make([][]byte, 0, len(fields)*2)
			for field, val := range fields {
				fvs = append(fvs, []byte(field), val)
			}
			r.hashOps.HSet(key, fvs...)

		case SnapTypeSet:
			members := data.(map[string]struct{})
			set := datatype.NewSetValue()
			entry := store.NewEntry(store.TypeSet, set)
			entry.ExpiresAt = expiresAt
			r.store.Set(key, entry)
			// Populate set
			memberList := make([]string, 0, len(members))
			for m := range members {
				memberList = append(memberList, m)
			}
			r.setOps.SAdd(key, memberList...)
		}

		keysLoaded++
	}

	if err := dec.VerifyChecksum(); err != nil {
		return keysLoaded, err
	}

	return keysLoaded, nil
}

// replayAOF replays AOF commands into the store.
// The AOF file is a sequence of RESP arrays — I parse them with the
// protocol reader and execute each command directly.
func (r *Recovery) replayAOF(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	reader := protocol.NewReader(f)
	var replayed int64

	for {
		val, err := reader.ReadValue()
		if err != nil {
			if err == io.EOF {
				break
			}
			// Partial command at end of file (crash during write) — stop here
			log.Printf("recovery: AOF parse error at command %d, stopping replay: %v", replayed, err)
			break
		}

		if val.Type != protocol.Array || len(val.Array) == 0 {
			continue
		}

		cmd := strings.ToUpper(string(val.Array[0].Bulk))
		args := val.Array[1:]

		if err := r.executeCommand(cmd, args); err != nil {
			log.Printf("recovery: skipping AOF command %s: %v", cmd, err)
			continue
		}

		replayed++
	}

	return replayed, nil
}

// executeCommand replays a single parsed command into the store.
// I handle each command type directly rather than going through the
// handler to avoid re-logging to AOF and for cleaner error handling.
func (r *Recovery) executeCommand(cmd string, args []protocol.Value) error {
	switch cmd {
	// String commands
	case "SET":
		if len(args) < 2 {
			return nil
		}
		key := string(args[0].Bulk)
		value := args[1].Bulk
		r.strOps.Set(key, value)
		// Handle EX/PX flags
		for i := 2; i < len(args)-1; i++ {
			flag := strings.ToUpper(string(args[i].Bulk))
			switch flag {
			case "EX":
				secs, _ := parseInt64Bytes(args[i+1].Bulk)
				r.strOps.SetWithTTL(key, value, secs)
				i++
			case "PX":
				ms, _ := parseInt64Bytes(args[i+1].Bulk)
				entry, exists := r.store.GetEntry(key)
				if exists {
					entry.ExpiresAt = time.Now().Add(time.Duration(ms) * time.Millisecond).UnixNano()
				}
				i++
			}
		}
	case "SETNX":
		if len(args) >= 2 {
			r.strOps.SetNX(string(args[0].Bulk), args[1].Bulk)
		}
	case "GETSET":
		if len(args) >= 2 {
			r.strOps.GetSet(string(args[0].Bulk), args[1].Bulk)
		}
	case "MSET":
		if len(args) >= 2 && len(args)%2 == 0 {
			pairs := make(map[string][]byte, len(args)/2)
			for i := 0; i < len(args); i += 2 {
				pairs[string(args[i].Bulk)] = args[i+1].Bulk
			}
			r.strOps.MSet(pairs)
		}
	case "INCR":
		if len(args) >= 1 {
			r.strOps.Incr(string(args[0].Bulk))
		}
	case "INCRBY":
		if len(args) >= 2 {
			delta, _ := parseInt64Bytes(args[1].Bulk)
			r.strOps.IncrBy(string(args[0].Bulk), delta)
		}
	case "DECR":
		if len(args) >= 1 {
			r.strOps.Decr(string(args[0].Bulk))
		}
	case "DECRBY":
		if len(args) >= 2 {
			delta, _ := parseInt64Bytes(args[1].Bulk)
			r.strOps.DecrBy(string(args[0].Bulk), delta)
		}
	case "INCRBYFLOAT":
		if len(args) >= 2 {
			r.strOps.IncrByFloat(string(args[0].Bulk), parseFloat64(args[1].Bulk))
		}
	case "APPEND":
		if len(args) >= 2 {
			r.strOps.Append(string(args[0].Bulk), args[1].Bulk)
		}
	case "SETRANGE":
		if len(args) >= 3 {
			offset, _ := parseInt64Bytes(args[1].Bulk)
			r.strOps.SetRange(string(args[0].Bulk), int(offset), args[2].Bulk)
		}

	// List commands
	case "LPUSH":
		if len(args) >= 2 {
			vals := bulkSlice(args[1:])
			r.listOps.LPush(string(args[0].Bulk), vals...)
		}
	case "RPUSH":
		if len(args) >= 2 {
			vals := bulkSlice(args[1:])
			r.listOps.RPush(string(args[0].Bulk), vals...)
		}
	case "LPOP":
		if len(args) >= 1 {
			r.listOps.LPop(string(args[0].Bulk))
		}
	case "RPOP":
		if len(args) >= 1 {
			r.listOps.RPop(string(args[0].Bulk))
		}
	case "LSET":
		if len(args) >= 3 {
			idx, _ := parseInt64Bytes(args[1].Bulk)
			r.listOps.LSet(string(args[0].Bulk), idx, args[2].Bulk)
		}
	case "LTRIM":
		if len(args) >= 3 {
			start, _ := parseInt64Bytes(args[1].Bulk)
			stop, _ := parseInt64Bytes(args[2].Bulk)
			r.listOps.LTrim(string(args[0].Bulk), start, stop)
		}
	case "LREM":
		if len(args) >= 3 {
			count, _ := parseInt64Bytes(args[1].Bulk)
			r.listOps.LRem(string(args[0].Bulk), count, args[2].Bulk)
		}
	case "LINSERT":
		if len(args) >= 4 {
			before := strings.ToUpper(string(args[1].Bulk)) == "BEFORE"
			r.listOps.LInsert(string(args[0].Bulk), before, args[2].Bulk, args[3].Bulk)
		}
	case "LPUSHX":
		if len(args) >= 2 {
			vals := bulkSlice(args[1:])
			r.listOps.LPushX(string(args[0].Bulk), vals...)
		}
	case "RPUSHX":
		if len(args) >= 2 {
			vals := bulkSlice(args[1:])
			r.listOps.RPushX(string(args[0].Bulk), vals...)
		}
	case "LMOVE":
		if len(args) >= 4 {
			r.listOps.LMove(string(args[0].Bulk), string(args[1].Bulk),
				strings.ToUpper(string(args[2].Bulk)), strings.ToUpper(string(args[3].Bulk)))
		}

	// Hash commands
	case "HSET":
		if len(args) >= 3 {
			fvs := make([][]byte, len(args)-1)
			for i := 1; i < len(args); i++ {
				fvs[i-1] = args[i].Bulk
			}
			r.hashOps.HSet(string(args[0].Bulk), fvs...)
		}
	case "HDEL":
		if len(args) >= 2 {
			fields := make([]string, len(args)-1)
			for i := 1; i < len(args); i++ {
				fields[i-1] = string(args[i].Bulk)
			}
			r.hashOps.HDel(string(args[0].Bulk), fields...)
		}
	case "HMSET":
		if len(args) >= 3 {
			pairs := make(map[string][]byte, (len(args)-1)/2)
			for i := 1; i < len(args)-1; i += 2 {
				pairs[string(args[i].Bulk)] = args[i+1].Bulk
			}
			r.hashOps.HMSet(string(args[0].Bulk), pairs)
		}
	case "HSETNX":
		if len(args) >= 3 {
			r.hashOps.HSetNX(string(args[0].Bulk), string(args[1].Bulk), args[2].Bulk)
		}
	case "HINCRBY":
		if len(args) >= 3 {
			delta, _ := parseInt64Bytes(args[2].Bulk)
			r.hashOps.HIncrBy(string(args[0].Bulk), string(args[1].Bulk), delta)
		}
	case "HINCRBYFLOAT":
		if len(args) >= 3 {
			r.hashOps.HIncrByFloat(string(args[0].Bulk), string(args[1].Bulk), parseFloat64(args[2].Bulk))
		}

	// Set commands
	case "SADD":
		if len(args) >= 2 {
			members := make([]string, len(args)-1)
			for i := 1; i < len(args); i++ {
				members[i-1] = string(args[i].Bulk)
			}
			r.setOps.SAdd(string(args[0].Bulk), members...)
		}
	case "SREM":
		if len(args) >= 2 {
			members := make([]string, len(args)-1)
			for i := 1; i < len(args); i++ {
				members[i-1] = string(args[i].Bulk)
			}
			r.setOps.SRem(string(args[0].Bulk), members...)
		}
	case "SPOP":
		if len(args) >= 1 {
			r.setOps.SPop(string(args[0].Bulk))
		}
	case "SMOVE":
		if len(args) >= 3 {
			r.setOps.SMove(string(args[0].Bulk), string(args[1].Bulk), string(args[2].Bulk))
		}
	case "SUNIONSTORE":
		if len(args) >= 2 {
			keys := make([]string, len(args)-1)
			for i := 1; i < len(args); i++ {
				keys[i-1] = string(args[i].Bulk)
			}
			r.setOps.SUnionStore(string(args[0].Bulk), keys...)
		}
	case "SINTERSTORE":
		if len(args) >= 2 {
			keys := make([]string, len(args)-1)
			for i := 1; i < len(args); i++ {
				keys[i-1] = string(args[i].Bulk)
			}
			r.setOps.SInterStore(string(args[0].Bulk), keys...)
		}
	case "SDIFFSTORE":
		if len(args) >= 2 {
			keys := make([]string, len(args)-1)
			for i := 1; i < len(args); i++ {
				keys[i-1] = string(args[i].Bulk)
			}
			r.setOps.SDiffStore(string(args[0].Bulk), keys...)
		}

	// Key management commands
	case "DEL":
		if len(args) >= 1 {
			keys := make([]string, len(args))
			for i, a := range args {
				keys[i] = string(a.Bulk)
			}
			r.store.DeleteMulti(keys...)
		}
	case "EXPIRE":
		if len(args) >= 2 {
			secs, _ := parseInt64Bytes(args[1].Bulk)
			entry, exists := r.store.GetEntry(string(args[0].Bulk))
			if exists {
				entry.SetTTL(secs)
			}
		}
	case "PEXPIRE":
		if len(args) >= 2 {
			ms, _ := parseInt64Bytes(args[1].Bulk)
			entry, exists := r.store.GetEntry(string(args[0].Bulk))
			if exists {
				entry.ExpiresAt = time.Now().Add(time.Duration(ms) * time.Millisecond).UnixNano()
			}
		}
	case "PEXPIREAT":
		if len(args) >= 2 {
			ms, _ := parseInt64Bytes(args[1].Bulk)
			entry, exists := r.store.GetEntry(string(args[0].Bulk))
			if exists {
				entry.ExpiresAt = ms * 1e6 // Milliseconds to nanoseconds
			}
		}
	case "PERSIST":
		if len(args) >= 1 {
			entry, exists := r.store.GetEntry(string(args[0].Bulk))
			if exists {
				entry.Persist()
			}
		}
	case "RENAME":
		if len(args) >= 2 {
			r.store.Rename(string(args[0].Bulk), string(args[1].Bulk))
		}
	case "FLUSHDB", "FLUSHALL":
		r.store.Clear()
	}

	return nil
}

// Helper functions for parsing AOF argument values

func parseInt64Bytes(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}
	var neg bool
	var n int64
	i := 0
	if b[0] == '-' {
		neg = true
		i++
	}
	for ; i < len(b); i++ {
		if b[i] < '0' || b[i] > '9' {
			return 0, false
		}
		n = n*10 + int64(b[i]-'0')
	}
	if neg {
		n = -n
	}
	return n, true
}

func parseFloat64(b []byte) float64 {
	f, _ := fmt.Sscanf(string(b), "%f", new(float64))
	if f == 0 {
		return 0
	}
	var v float64
	fmt.Sscanf(string(b), "%f", &v)
	return v
}

func bulkSlice(args []protocol.Value) [][]byte {
	result := make([][]byte, len(args))
	for i, a := range args {
		result[i] = a.Bulk
	}
	return result
}
