# DCache - High-Performance Distributed In-Memory Cache

A Redis-compatible, high-performance distributed in-memory cache I built in Go. I designed this to demonstrate advanced systems programming concepts for technical interviews at trading firms.

## Features

- **Core Store**: 256-shard concurrent map with RWMutex per shard, xxHash, 4 data types (string, list, hash, set)
- **RESP Protocol**: Full Redis wire compatibility — works with `redis-cli`, `redis-benchmark`, and all Redis client libraries
- **Persistence**: Append-only file (AOF) with configurable fsync + binary snapshots with CRC-32C checksums
- **Replication**: Async master-slave replication with PSYNC, full/partial resync, and connection hijacking
- **Observability**: Prometheus metrics on `/metrics` with per-command latency histograms
- **80+ Commands**: Complete coverage of string, list, hash, set, key management, and server commands

## Performance Highlights

| Operation | Throughput | Latency |
|-----------|------------|---------|
| GET (parallel) | **50M+ ops/sec** | 22.73 ns |
| SET (parallel) | **42M+ ops/sec** | 27.23 ns |
| Mixed 80/20 | **33M+ ops/sec** | 36.49 ns |
| DELETE | **65M+ ops/sec** | 18.07 ns |

*Benchmarked on Apple M2, 8 cores*

## Quick Start

```bash
# Clone and build
git clone https://github.com/aravinth/distributed-cache
cd distributed-cache
make build

# Run the server
./dcache

# Or with all options
./dcache --port 6379 --appendonly --aof-fsync everysec --save-interval 300 --metrics-port 9090
```

### Connect with redis-cli

```bash
redis-cli
> PING
PONG
> SET mykey "hello"
OK
> GET mykey
"hello"
> LPUSH mylist a b c
(integer) 3
> HSET myhash name dcache version 1
(integer) 2
> SADD myset x y z
(integer) 3
> INFO
```

### Replication

```bash
# Terminal 1: Start master
./dcache --port 6379

# Terminal 2: Start slave
./dcache --port 6380 --replicaof localhost:6379

# Write to master, read from slave
redis-cli -p 6379 SET hello world
redis-cli -p 6380 GET hello    # → "world"
```

### Docker

```bash
# Start dcache + Prometheus + Grafana
docker compose up --build

# dcache on :6379, metrics on :9090, Prometheus on :9091, Grafana on :3000
```

## Architecture

```
  Client ──→ TCP Server (connection-per-goroutine)
                 │
                 ▼
          RESP Protocol Parser
                 │
                 ▼
          Command Handler (Execute)
           │    │    │    │
           ▼    ▼    ▼    ▼
         String List Hash  Set      ← Data Type Operations
           │    │    │    │
           └────┴────┴────┘
                 │
                 ▼
          Sharded Map (256 shards, RWMutex per shard)
                 │
        ┌────────┼────────┐
        ▼        ▼        ▼
     AOF Log   Backlog   Metrics
     (disk)   (ring buf)  (Prometheus)
        │        │
        ▼        ▼
    Recovery   Slaves
```

### Command Execution Flow

Every command passes through `Handler.Execute()`, which is the single hook point for:

1. **Slave read-only guard** — reject writes when operating as a slave
2. **Command dispatch** — route to the appropriate handler
3. **Metrics** — increment command counter, observe latency histogram
4. **AOF hook** — log successful mutating commands for durability
5. **Replication hook** — feed successful mutations to connected slaves

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `6379` | TCP port |
| `--maxclients` | `10000` | Maximum concurrent connections |
| `--timeout` | `300` | Idle connection timeout (seconds) |
| `--dir` | `data/` | Data directory for persistence |
| `--appendonly` | `true` | Enable append-only file |
| `--aof-fsync` | `everysec` | AOF fsync policy: `always`, `everysec`, `no` |
| `--save-interval` | `300` | Snapshot interval in seconds (0 = disabled) |
| `--replicaof` | | Make this server a replica: `host:port` |
| `--repl-backlog-size` | `1048576` | Replication backlog size (bytes) |
| `--metrics-port` | `9090` | Prometheus HTTP port (0 = disabled) |

## Monitoring

When `--metrics-port` is set, DCache exposes Prometheus metrics at `http://localhost:9090/metrics`.

### Key Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `dcache_commands_total` | Counter | Commands processed (by name) |
| `dcache_command_duration_seconds` | Histogram | Per-command latency |
| `dcache_keys_total` | Gauge | Total keys in store |
| `dcache_cache_hits_total` | Gauge | Cache hit count |
| `dcache_cache_misses_total` | Gauge | Cache miss count |
| `dcache_cache_hit_ratio` | Gauge | Hit ratio (0.0–1.0) |
| `dcache_connections_active` | Gauge | Current connections |
| `dcache_replication_slaves_connected` | Gauge | Connected replicas |
| `dcache_snapshot_in_progress` | Gauge | Snapshot running (0/1) |

## Key Design Decisions

### 1. Sharded Map with 256 Partitions

- With 1000 concurrent clients, ~4 clients per shard on average
- Power of 2 enables O(1) shard selection via bit masking (`hash & 0xFF`)
- RWMutex provides consistent latency for read-heavy and delete-heavy workloads

### 2. xxHash for Key Distribution

- 3x faster than FNV-1a, excellent distribution (no hot shards)
- Used by ClickHouse, ScyllaDB, and other high-performance systems

### 3. Lazy Expiration

- Keys checked on access — zero background overhead
- Trade-off: expired keys consume memory until accessed
- Suitable for high-frequency access patterns in trading systems

### 4. Lock-Free Metrics

- All counters use `sync/atomic` — zero lock contention for observability
- Per-shard metrics aggregated on demand

### 5. Persistence: AOF + Snapshots

- **AOF**: Buffered channel (64K), non-blocking `Log()`, configurable fsync
- **Snapshots**: Shard-by-shard binary encode with CRC-32C checksums
- **Recovery**: Load latest snapshot, replay AOF tail

### 6. Async Replication

- Master streams mutations via bounded ring buffer backlog (1MB default)
- Full sync: snapshot transfer + backlog gap
- Partial sync: backlog slice from last offset
- Connection hijacking: PSYNC switches from request-response to streaming

## Data Structures

| Type | Implementation | Concurrency |
|------|----------------|-------------|
| String | `[]byte` with atomic pointer swap | Lock-free for reads |
| List | Doubly-linked list | Per-list RWMutex |
| Hash | `map[string][]byte` | Per-hash RWMutex |
| Set | `map[string]struct{}` | Per-set RWMutex |

## Supported Commands

**Server**: PING, ECHO, QUIT, SELECT, INFO, COMMAND, DBSIZE, FLUSHDB, FLUSHALL
**Keys**: DEL, EXISTS, EXPIRE, PEXPIRE, TTL, PTTL, PERSIST, TYPE, RENAME, KEYS
**Strings**: GET, SET (EX/PX/NX/XX), SETNX, GETSET, MGET, MSET, INCR, INCRBY, DECR, DECRBY, INCRBYFLOAT, APPEND, STRLEN, GETRANGE, SETRANGE
**Lists**: LPUSH, RPUSH, LPOP, RPOP, LLEN, LINDEX, LRANGE, LSET, LTRIM, LREM, LINSERT, LPUSHX, RPUSHX, LMOVE
**Hashes**: HSET, HGET, HGETALL, HDEL, HEXISTS, HLEN, HKEYS, HVALS, HMGET, HSETNX, HINCRBY, HINCRBYFLOAT, HSTRLEN
**Sets**: SADD, SREM, SISMEMBER, SMISMEMBER, SMEMBERS, SCARD, SPOP, SRANDMEMBER, SMOVE, SUNION, SUNIONSTORE, SINTER, SINTERSTORE, SINTERCARD, SDIFF, SDIFFSTORE
**Persistence**: BGSAVE, BGREWRITEAOF, LASTSAVE
**Replication**: REPLICAOF, SLAVEOF, REPLCONF, PSYNC

## Project Structure

```
distributed-cache/
├── cmd/server/main.go          # Entry point with flags + signal handling
├── internal/
│   ├── store/                  # 256-shard concurrent map
│   │   ├── entry.go            # Cache entry with TTL
│   │   └── shard.go            # ShardedMap + per-shard metrics
│   ├── datatype/               # Data type operations
│   │   ├── string.go           # String commands
│   │   ├── list.go             # Doubly-linked list
│   │   ├── hash.go             # Hash table
│   │   └── set.go              # Set operations
│   ├── protocol/               # RESP wire format
│   │   ├── resp.go             # Types + sentinels
│   │   ├── reader.go           # Parser (+ inline support)
│   │   └── writer.go           # Serialiser
│   ├── server/                 # TCP server
│   │   ├── server.go           # Accept loop + graceful shutdown
│   │   ├── connection.go       # Connection wrapper
│   │   └── handler.go          # Command routing + hooks
│   ├── persistence/            # AOF + snapshots
│   │   ├── config.go           # Persistence config
│   │   ├── aof.go              # AOF writer (buffered channel)
│   │   ├── aof_rewrite.go      # AOF compaction
│   │   ├── snapshot.go         # Binary snapshot engine
│   │   ├── snapshot_format.go  # Encoder/decoder + CRC-32C
│   │   ├── recovery.go         # Startup recovery
│   │   └── mutating.go         # Mutating command set
│   ├── replication/            # Master-slave replication
│   │   ├── role.go             # ReplState + role management
│   │   ├── backlog.go          # Ring buffer for repl stream
│   │   ├── master.go           # PSYNC, sync, streaming
│   │   └── slave.go            # Connect, handshake, apply
│   ├── metrics/                # Prometheus observability
│   │   ├── prometheus.go       # Collectors + metric definitions
│   │   └── server.go           # HTTP /metrics endpoint
│   └── pool/                   # sync.Pool for bufio buffers
│       └── pool.go
├── test/
│   ├── unit/                   # Unit tests
│   └── benchmark/              # Benchmark suite
├── docs/
│   └── design-decisions.md     # Technical decisions explained
├── scripts/
│   └── bench.sh                # redis-benchmark harness
├── Dockerfile                  # Multi-stage build
├── docker-compose.yml          # dcache + Prometheus + Grafana
├── prometheus.yml              # Scrape config
├── Makefile                    # build, test, bench, docker
└── README.md
```

## Benchmarks vs Redis

| Operation | DCache | Redis | Notes |
|-----------|--------|-------|-------|
| SET (single) | 24M/s | 110K/s | 218x faster |
| GET (single) | 13M/s | 120K/s | 108x faster |
| SET (parallel) | 42M/s | N/A | |
| GET (parallel) | 50M/s | N/A | |

*Note: Redis benchmarks are I/O bound; this comparison shows in-process performance.*

### Running Benchmarks

```bash
# Go benchmarks (in-process)
make bench

# redis-benchmark (over TCP)
make redis-bench
```

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

The `Rename` operation requires locking two shards. I prevent deadlocks by locking shards in consistent address order:

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

### "How does replication work?"

1. Master records all mutations in a bounded ring buffer backlog
2. New slaves receive a full snapshot + backlog gap, then stream live
3. Reconnecting slaves with valid offsets get a partial resync from the backlog
4. Connection hijacking: PSYNC switches from request-response to streaming without closing the socket
5. Slaves are read-only — writes are rejected before command dispatch

### "How do you ensure durability?"

1. AOF logs every mutating command to disk via a buffered channel
2. Configurable fsync: `always` (max safety), `everysec` (default), `no` (fastest)
3. Binary snapshots with CRC-32C checksums run periodically or on demand
4. Recovery: load latest snapshot, replay AOF tail, verify checksums

### "What are the main bottlenecks?"

1. **Memory allocations**: Addressed with `sync.Pool` for buffers
2. **Lock contention**: Addressed with 256-shard design
3. **GC pressure**: Addressed with object reuse and pooling
4. **Network I/O**: Use buffered I/O (64KB buffers)

## License

MIT
