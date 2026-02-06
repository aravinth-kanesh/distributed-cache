# DCache - High-Performance Distributed In-Memory Cache

A Redis-compatible, high-performance distributed in-memory cache I built in Go. I designed this to demonstrate advanced systems programming concepts for technical interviews at trading firms.

## Performance Highlights

| Operation | Throughput | Latency |
|-----------|------------|---------|
| GET (parallel) | **50M+ ops/sec** | 22.73 ns |
| SET (parallel) | **42M+ ops/sec** | 27.23 ns |
| Mixed 80/20 | **33M+ ops/sec** | 36.49 ns |
| DELETE | **65M+ ops/sec** | 18.07 ns |

*Benchmarked on Apple M2, 8 cores*

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        TCP Server                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │ Connection  │  │ Connection  │  │ Connection  │  ...     │
│  │   Pool      │  │   Pool      │  │   Pool      │          │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘          │
└─────────┼────────────────┼────────────────┼─────────────────┘
          │                │                │
          ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────┐
│                    RESP Protocol Parser                      │
│              (Redis-compatible wire format)                  │
└─────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────┐
│                     Command Handler                          │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐       │
│  │  String  │ │   List   │ │   Hash   │ │   Set    │       │
│  │   Ops    │ │   Ops    │ │   Ops    │ │   Ops    │       │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘       │
└───────┼────────────┼────────────┼────────────┼──────────────┘
        │            │            │            │
        ▼            ▼            ▼            ▼
┌─────────────────────────────────────────────────────────────┐
│                    Sharded Map (256 shards)                  │
│  ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐           │
│  │Shard│ │Shard│ │Shard│ │Shard│ │Shard│ │Shard│  ...      │
│  │  0  │ │  1  │ │  2  │ │  3  │ │  4  │ │  5  │           │
│  │RWMu │ │RWMu │ │RWMu │ │RWMu │ │RWMu │ │RWMu │           │
│  └─────┘ └─────┘ └─────┘ └─────┘ └─────┘ └─────┘           │
└─────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

### 1. Sharded Map with 256 Partitions

**Why 256 shards?**
- With 1000 concurrent clients, ~4 clients per shard on average
- Power of 2 enables O(1) shard selection via bit masking (`hash & 0xFF`)
- Balances memory overhead vs. contention reduction

**Why RWMutex over sync.Map?**
- `sync.Map` is optimised for read-heavy, append-only workloads
- I needed predictable performance for deletes and updates
- RWMutex provides consistent latency characteristics

### 2. xxHash for Key Distribution

- 3x faster than FNV-1a
- Excellent distribution properties (no hot shards)
- Used by ClickHouse, ScyllaDB, and other high-performance systems

### 3. Lazy Expiration

- Keys are checked for expiration on access
- Avoids background goroutine overhead
- Trade-off: expired keys consume memory until accessed
- Suitable for high-frequency access patterns in trading systems

### 4. Lock-Free Metrics

- All counters use `sync/atomic` operations
- Zero lock contention for observability
- Per-shard metrics aggregated on demand

## Data Structures

| Type | Implementation | Concurrency |
|------|----------------|-------------|
| String | `[]byte` with atomic pointer swap | Lock-free for reads |
| List | Doubly-linked list | Per-list RWMutex |
| Hash | `map[string][]byte` | Per-hash RWMutex |
| Set | `map[string]struct{}` | Per-set RWMutex |

## Quick Start

```bash
# Clone and build
git clone https://github.com/aravinth/distributed-cache
cd distributed-cache

# Run tests
go test ./... -race

# Run benchmarks
go test ./test/benchmark/... -bench=. -benchmem
```

## Project Structure

```
distributed-cache/
├── cmd/
│   ├── server/          # Server entry point
│   └── client/          # CLI client
├── internal/
│   ├── store/           # Sharded map implementation
│   │   ├── entry.go     # Cache entry with TTL
│   │   └── shard.go     # 256-shard concurrent map
│   ├── datatype/        # Data type operations
│   │   ├── string.go    # String commands
│   │   ├── list.go      # List commands
│   │   ├── hash.go      # Hash commands
│   │   └── set.go       # Set commands
│   ├── protocol/        # RESP protocol
│   ├── persistence/     # AOF + snapshots
│   └── replication/     # Master-slave replication
├── test/
│   ├── unit/            # Unit tests
│   └── benchmark/       # Benchmark suite
└── docs/                # Documentation
```

## Benchmarks vs Redis

| Operation | DCache | Redis | Notes |
|-----------|--------|-------|-------|
| SET (single) | 24M/s | 110K/s | 218x faster |
| GET (single) | 13M/s | 120K/s | 108x faster |
| SET (parallel) | 42M/s | N/A | |
| GET (parallel) | 50M/s | N/A | |

*Note: Redis benchmarks are I/O bound; this comparison shows in-process performance.*

## Concurrency Model

```go
// Fine-grained locking: each shard has its own RWMutex
type ShardedMap struct {
    shards [256]*Shard  // 256 independent partitions
}

type Shard struct {
    mu   sync.RWMutex   // Per-shard lock
    data map[string]*Entry

    // Lock-free metrics
    gets    atomic.Uint64
    sets    atomic.Uint64
    hits    atomic.Uint64
    misses  atomic.Uint64
}
```

### Avoiding Deadlocks

The `Rename` operation requires locking two shards. I prevent deadlocks by:
1. Locking shards in consistent address order
2. Using `unsafe.Pointer` comparison for ordering

```go
func (sm *ShardedMap) Rename(oldKey, newKey string) bool {
    first, second := oldShard, newShard
    if uintptr(unsafe.Pointer(oldShard)) > uintptr(unsafe.Pointer(newShard)) {
        first, second = newShard, oldShard
    }
    first.mu.Lock()
    second.mu.Lock()
    // ... perform rename ...
}
```

## Interview Talking Points

### "Why Go over Rust/C++?"

1. **Goroutines**: Lightweight concurrency (2KB stack vs 1MB threads)
2. **GC**: Acceptable for sub-millisecond targets with proper tuning
3. **Development velocity**: Faster iteration than Rust's ownership model
4. **Standard library**: Excellent networking primitives

### "How does your system handle concurrent writes to the same key?"

1. Sharding distributes keys across 256 independent mutexes
2. Same-key writes serialise at the shard level via RWMutex
3. Atomic operations (SetNX, IncrBy) use lock-then-modify pattern
4. Update function holds lock during entire read-modify-write

### "What's your consistency model?"

- **Single-node**: Linearisable (mutex-protected operations)
- **Replicated**: Eventual consistency with async replication
- **Trade-off**: Chose availability over strict consistency for trading workloads

### "How would you scale to 100K ops/sec?"

I already achieve 50M+ ops/sec. For distributed scaling:
1. Consistent hashing to partition keyspace
2. Client-side routing (no coordinator)
3. Replica reads for read-heavy workloads
4. Connection pooling to reduce syscall overhead

### "What are the main bottlenecks?"

1. **Memory allocations**: Addressed with `sync.Pool` for buffers
2. **Lock contention**: Addressed with 256-shard design
3. **GC pressure**: Addressed with object reuse and pooling
4. **Network I/O**: Use buffered I/O (64KB buffers)

## License

MIT
