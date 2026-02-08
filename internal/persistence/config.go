package persistence

import "time"

// FsyncPolicy controls how often I fsync the AOF file to disk.
// This is the fundamental durability vs performance trade-off.
type FsyncPolicy int

const (
	// FsyncAlways fsyncs after every write batch. Maximum durability, lowest throughput.
	FsyncAlways FsyncPolicy = iota

	// FsyncEverySecond fsyncs once per second. Default. Good balance —
	// I lose at most 1 second of data on crash.
	FsyncEverySecond

	// FsyncNone lets the OS decide when to flush. Fastest, but the
	// data loss window is unbounded.
	FsyncNone
)

// Config holds all persistence-related configuration.
type Config struct {
	// AOF settings
	AOFEnabled    bool
	AOFFilePath   string
	AOFPolicy     FsyncPolicy
	AOFBufferSize int
	AOFRewriteMin int64 // Minimum AOF size in bytes before auto-rewrite

	// Snapshot settings
	SnapshotEnabled  bool
	SnapshotDir      string
	SnapshotInterval time.Duration
	SnapshotPrefix   string

	// Shared
	DataDir string
}

// DefaultConfig returns sensible defaults for persistence.
func DefaultConfig() Config {
	return Config{
		AOFEnabled:    true,
		AOFFilePath:   "data/appendonly.aof",
		AOFPolicy:     FsyncEverySecond,
		AOFBufferSize: 65536,
		AOFRewriteMin: 64 * 1024 * 1024, // 64 MB

		SnapshotEnabled:  true,
		SnapshotDir:      "data/",
		SnapshotInterval: 5 * time.Minute,
		SnapshotPrefix:   "dcache",

		DataDir: "data/",
	}
}
