package persistence

import (
	"bufio"
	"fmt"
	"os"
	"strconv"

	"github.com/aravinth/distributed-cache/internal/datatype"
	"github.com/aravinth/distributed-cache/internal/protocol"
	"github.com/aravinth/distributed-cache/internal/store"
)

// AOFRewriter compacts the AOF by iterating the current store state and
// writing the minimal command set to a temporary file. Once complete, it
// atomically replaces the old AOF. For example, 1000 INCRs on the same
// key become a single SET with the final value.
type AOFRewriter struct {
	store  *store.ShardedMap
	config Config
	aof    *AOFWriter
}

// NewAOFRewriter creates a new rewriter.
func NewAOFRewriter(sm *store.ShardedMap, cfg Config, aof *AOFWriter) *AOFRewriter {
	return &AOFRewriter{
		store:  sm,
		config: cfg,
		aof:    aof,
	}
}

// Rewrite performs the AOF compaction.
// I iterate the store shard-by-shard and emit equivalent commands.
// During the rewrite, the AOFWriter buffers new commands. After
// iteration completes, I append those buffered commands to the new
// file, then atomically swap.
func (r *AOFRewriter) Rewrite() error {
	tmpPath := r.config.AOFFilePath + ".rewrite"

	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("creating rewrite file: %w", err)
	}

	bw := bufio.NewWriterSize(f, 256*1024)

	// Signal the AOF writer to start buffering new entries
	r.aof.StartRewrite()

	// Iterate the store and emit minimal commands
	r.store.ForEachShard(func(data map[string]*store.Entry) {
		for key, entry := range data {
			r.emitEntry(bw, key, entry)
		}
	})

	// Flush the iteration output
	if err := bw.Flush(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		r.aof.FinishRewrite()
		return fmt.Errorf("flushing rewrite file: %w", err)
	}

	// Get the buffered entries that arrived during iteration
	buffered := r.aof.FinishRewrite()

	// Append buffered entries to the new file
	for _, entry := range buffered {
		if _, err := bw.Write(entry.Data); err != nil {
			f.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("writing buffered entry: %w", err)
		}
	}

	if err := bw.Flush(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("flushing buffered entries: %w", err)
	}

	// Fsync the new file before swapping
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("fsyncing rewrite file: %w", err)
	}
	f.Close()

	// Atomically swap the new file into place
	return r.aof.Rotate(tmpPath)
}

// emitEntry writes the RESP commands needed to reconstruct a single entry.
func (r *AOFRewriter) emitEntry(w *bufio.Writer, key string, entry *store.Entry) {
	switch entry.Type {
	case store.TypeString:
		val, ok := entry.Value.([]byte)
		if !ok {
			return
		}
		r.emitCommand(w, "SET", key, string(val))

	case store.TypeList:
		list, ok := entry.Value.(*datatype.ListValue)
		if !ok {
			return
		}
		elements := list.Elements()
		if len(elements) == 0 {
			return
		}
		// Emit RPUSH in batches of 100 to avoid huge commands
		for i := 0; i < len(elements); i += 100 {
			end := i + 100
			if end > len(elements) {
				end = len(elements)
			}
			args := make([]string, 0, 1+end-i)
			args = append(args, key)
			for _, elem := range elements[i:end] {
				args = append(args, string(elem))
			}
			r.emitCommand(w, "RPUSH", args...)
		}

	case store.TypeHash:
		hash, ok := entry.Value.(*datatype.HashValue)
		if !ok {
			return
		}
		data := hash.DataSnapshot()
		if len(data) == 0 {
			return
		}
		args := make([]string, 0, 1+len(data)*2)
		args = append(args, key)
		for field, val := range data {
			args = append(args, field, string(val))
		}
		r.emitCommand(w, "HSET", args...)

	case store.TypeSet:
		set, ok := entry.Value.(*datatype.SetValue)
		if !ok {
			return
		}
		members := set.MembersSnapshot()
		if len(members) == 0 {
			return
		}
		args := make([]string, 0, 1+len(members))
		args = append(args, key)
		for member := range members {
			args = append(args, member)
		}
		r.emitCommand(w, "SADD", args...)
	}

	// Emit PEXPIREAT if the entry has an expiry
	if entry.ExpiresAt > 0 {
		ms := entry.ExpiresAt / 1e6 // Nanoseconds to milliseconds
		r.emitCommand(w, "PEXPIREAT", key, strconv.FormatInt(ms, 10))
	}
}

// emitCommand writes a single RESP array command to the writer.
func (r *AOFRewriter) emitCommand(w *bufio.Writer, cmd string, args ...string) {
	// Build a RESP array: *<count>\r\n followed by bulk strings
	vals := make([]protocol.Value, 0, 1+len(args))
	vals = append(vals, protocol.BulkStringVal([]byte(cmd)))
	for _, arg := range args {
		vals = append(vals, protocol.BulkStringVal([]byte(arg)))
	}
	data := protocol.MarshalValue(protocol.ArrayVal(vals))
	w.Write(data)
}
