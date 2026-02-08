package persistence

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/aravinth/distributed-cache/internal/datatype"
	"github.com/aravinth/distributed-cache/internal/store"
)

// SnapshotEngine manages periodic and on-demand binary snapshots.
// I iterate shards sequentially — copying map references under RLock
// then encoding outside the lock — so at most 1/256th of writes are
// briefly blocked at any moment.
type SnapshotEngine struct {
	store  *store.ShardedMap
	config Config

	saving       atomic.Bool
	lastSaveTime atomic.Int64
	lastSaveKeys atomic.Int64

	// Optional AOF writer — rotated after each successful snapshot
	aof *AOFWriter
}

// NewSnapshotEngine creates a new snapshot engine.
func NewSnapshotEngine(sm *store.ShardedMap, cfg Config, aof *AOFWriter) *SnapshotEngine {
	return &SnapshotEngine{
		store:  sm,
		config: cfg,
		aof:    aof,
	}
}

// Start begins the periodic snapshot goroutine.
func (s *SnapshotEngine) Start(ctx context.Context) {
	if s.config.SnapshotInterval <= 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(s.config.SnapshotInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				path, err := s.Save()
				if err != nil {
					log.Printf("periodic snapshot failed: %v", err)
				} else {
					log.Printf("periodic snapshot saved: %s", path)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Save performs a snapshot to disk.
// Returns the path to the saved file.
func (s *SnapshotEngine) Save() (string, error) {
	if !s.saving.CompareAndSwap(false, true) {
		return "", fmt.Errorf("snapshot already in progress")
	}
	defer s.saving.Store(false)

	timestamp := time.Now().UnixNano()
	filename := fmt.Sprintf("%s-%d.rdb", s.config.SnapshotPrefix, timestamp/1e9)
	path := filepath.Join(s.config.SnapshotDir, filename)
	tmpPath := path + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return "", fmt.Errorf("creating snapshot file: %w", err)
	}

	bw := bufio.NewWriterSize(f, 256*1024)
	enc := NewSnapshotEncoder(bw)

	keyCount := s.store.KeyCount()
	if err := enc.WriteHeader(timestamp, keyCount); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return "", fmt.Errorf("writing header: %w", err)
	}

	// Iterate shard-by-shard. ForEachShard copies map references under
	// RLock then releases, so encoding happens outside any store lock.
	s.store.ForEachShard(func(data map[string]*store.Entry) {
		for key, entry := range data {
			s.encodeEntry(enc, key, entry)
		}
	})

	if err := enc.WriteFooter(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return "", fmt.Errorf("writing footer: %w", err)
	}

	if err := bw.Flush(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return "", fmt.Errorf("flushing: %w", err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return "", fmt.Errorf("fsyncing: %w", err)
	}
	f.Close()

	// Atomic rename: incomplete .tmp files are ignored on recovery
	if err := os.Rename(tmpPath, path); err != nil {
		return "", fmt.Errorf("renaming snapshot: %w", err)
	}

	// Update the latest symlink
	latestLink := filepath.Join(s.config.SnapshotDir, s.config.SnapshotPrefix+"-latest.rdb")
	os.Remove(latestLink)
	// Use the filename (not full path) for relative symlink
	os.Symlink(filename, latestLink)

	s.lastSaveTime.Store(time.Now().Unix())
	s.lastSaveKeys.Store(keyCount)

	// Rotate AOF after successful snapshot — the snapshot has all data
	// up to this point, so we start a fresh AOF for subsequent commands
	if s.aof != nil {
		newAOFPath := s.config.AOFFilePath + ".new"
		nf, err := os.Create(newAOFPath)
		if err == nil {
			nf.Close()
			if err := s.aof.Rotate(newAOFPath); err != nil {
				log.Printf("warning: AOF rotation after snapshot failed: %v", err)
			}
		}
	}

	return path, nil
}

// encodeEntry writes a single entry to the snapshot encoder.
func (s *SnapshotEngine) encodeEntry(enc *SnapshotEncoder, key string, entry *store.Entry) {
	switch entry.Type {
	case store.TypeString:
		val, ok := entry.Value.([]byte)
		if !ok {
			return
		}
		enc.WriteStringEntry(key, entry.ExpiresAt, val)

	case store.TypeList:
		list, ok := entry.Value.(*datatype.ListValue)
		if !ok {
			return
		}
		elements := list.Elements()
		enc.WriteListEntry(key, entry.ExpiresAt, elements)

	case store.TypeHash:
		hash, ok := entry.Value.(*datatype.HashValue)
		if !ok {
			return
		}
		data := hash.DataSnapshot()
		enc.WriteHashEntry(key, entry.ExpiresAt, data)

	case store.TypeSet:
		set, ok := entry.Value.(*datatype.SetValue)
		if !ok {
			return
		}
		members := set.MembersSnapshot()
		enc.WriteSetEntry(key, entry.ExpiresAt, members)
	}
}

// IsSaving returns true if a snapshot is currently in progress.
func (s *SnapshotEngine) IsSaving() bool {
	return s.saving.Load()
}

// LastSaveTime returns the Unix timestamp of the last successful save.
func (s *SnapshotEngine) LastSaveTime() int64 {
	return s.lastSaveTime.Load()
}
