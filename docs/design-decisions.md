# Design Decisions Document

## Overview

Here I explain the key technical decisions I made in designing and implementing DCache, a high-performance distributed in-memory cache. Each decision includes my reasoning, alternatives I considered, and trade-offs.

---

## 1. Language Choice: Go

### Decision
Implement in Go rather than Rust, C++, or Java.

### Reasoning

| Factor | Go | Rust | C++ | Java |
|--------|-----|------|-----|------|
| Concurrency | Goroutines (2KB) | Async/threads | Threads | Threads |
| Memory safety | GC + race detector | Ownership system | Manual | GC |
| Development speed | Fast | Slow (learning curve) | Medium | Fast |
| Performance | High | Highest | Highest | Medium |
| Ecosystem | Growing | Growing | Mature | Mature |

**Key factors:**

1. **Goroutines**: 1M goroutines in 2GB RAM vs. 2000 threads in same space
2. **GC is acceptable**: With proper tuning (GOGC, sync.Pool), GC pauses are <1ms
3. **Race detector**: Built-in `go test -race` catches concurrency bugs early
4. **Standard library**: `net`, `sync`, `encoding` packages are production-ready

### Trade-offs

- **Gave up**: Rust's zero-cost abstractions, C++'s raw performance
- **Gained**: Faster development, simpler deployment, excellent tooling

### Interview Answer

> "I chose Go for its lightweight concurrency model - goroutines use 2KB vs 1MB for OS threads, allowing me to handle 1000+ concurrent connections efficiently. The race detector helped catch concurrency bugs during development. While Rust would offer marginally better performance, Go's development velocity was more valuable for this project."

---

## 2. Concurrency: Sharded Map with 256 Partitions

### Decision
Use a sharded map with 256 independent shards, each with its own RWMutex.

### Alternatives Considered

| Approach | Pros | Cons |
|----------|------|------|
| Single global mutex | Simple | Severe contention |
| sync.Map | Good for reads | Poor delete performance, unpredictable |
| Lock-free CAS | Best throughput | Complex, ABA problems |
| **Sharded RWMutex** | Predictable, good balance | Memory overhead |

### Why 256 Shards?

```
Contention probability = 1 - (1 - 1/N)^C
where N = shards, C = concurrent clients

For N=256, C=1000: P(contention) ≈ 0.98% per operation
For N=64, C=1000: P(contention) ≈ 3.8% per operation
For N=16, C=1000: P(contention) ≈ 14.7% per operation
```

256 provides good contention reduction while keeping memory overhead manageable.

### Why Power of 2?

Enables fast modulo via bit masking:
```go
// Fast: single AND instruction
shardIndex := hash & 0xFF  // hash % 256

// Slow: division instruction
shardIndex := hash % 256
```

### Interview Answer

> "I chose 256 shards as a balance between contention reduction and memory overhead. With 1000 concurrent clients, each shard sees ~4 clients on average. The power-of-2 count enables O(1) shard selection via bit masking rather than expensive modulo division. RWMutex allows concurrent reads within the same shard while serialising writes."

---

## 3. Hash Function: xxHash

### Decision
Use xxHash (xxhash64) for key-to-shard mapping.

### Alternatives Considered

| Hash Function | Speed | Distribution | Use Case |
|---------------|-------|--------------|----------|
| FNV-1a | Moderate | Good | General purpose |
| **xxHash** | Fast | Excellent | High-throughput |
| CityHash | Fast | Excellent | Google internal |
| SipHash | Moderate | Excellent | Hash DoS resistant |

### Why xxHash?

1. **Speed**: 3x faster than FNV-1a in benchmarks
2. **Distribution**: Excellent avalanche properties
3. **Adoption**: Used by ClickHouse, ScyllaDB, LZ4

### Interview Answer

> "xxHash provides excellent distribution and is about 3x faster than FNV-1a. Good distribution is critical for sharding - I don't want hot shards. The library is well-tested and used in production by ClickHouse and ScyllaDB."

---

## 4. Expiration: Lazy Deletion

### Decision
Check expiration on access rather than using background cleanup.

### Alternatives Considered

| Approach | Pros | Cons |
|----------|------|------|
| Background goroutine | Consistent memory usage | CPU overhead, complexity |
| **Lazy deletion** | Simple, no overhead | Memory not freed until access |
| Hybrid | Best of both | Complex implementation |

### Why Lazy?

1. **Zero CPU overhead**: No background scanning
2. **Simpler implementation**: No coordination needed
3. **Acceptable trade-off**: In high-frequency trading, keys are accessed often

### Trade-off

Expired keys consume memory until accessed. For trading systems with frequent access patterns, this is acceptable. For long-tail access patterns, a hybrid approach would be better.

### Interview Answer

> "I chose lazy expiration to avoid background goroutine overhead. In trading systems where data is accessed frequently, expired keys are evicted quickly on access. For systems with long-tail access patterns, I'd implement a hybrid approach with periodic sampling like Redis's activeExpireCycle."

---

## 5. Data Types: Per-Type Locking

### Decision
Each data type (List, Hash, Set) has its own embedded mutex.

### Reasoning

```go
type ListValue struct {
    mu   sync.RWMutex  // Per-list lock
    head *ListNode
    tail *ListNode
    len  atomic.Int64  // Lock-free length
}
```

I chose this approach to provide:
1. **Fine-grained locking**: Operations on different lists don't block each other
2. **Type safety**: Each type can optimise its own locking strategy
3. **Atomic length**: `LLen` is O(1) and lock-free

### Trade-off

More memory per value (RWMutex is 24 bytes). Acceptable for typical use cases where you have thousands, not millions, of complex data structures.

### Interview Answer

> "Each complex data type has its own mutex to enable concurrent access to different keys with the same shard. For example, two LPUSH operations on different lists in the same shard don't block each other. The atomic length counter allows O(1) LLEN without locking."

---

## 6. Atomic Operations: Lock-then-Modify Pattern

### Decision
For atomic operations like INCR, use the store's `Update` function which holds the shard lock during read-modify-write.

### Problem

```go
// WRONG: Race condition
func IncrBy(key string, delta int64) int64 {
    val := store.Get(key)      // Read
    newVal := val + delta      // Modify
    store.Set(key, newVal)     // Write
    return newVal
}
```

Between Get and Set, another goroutine could modify the value.

### Solution

```go
// CORRECT: Atomic update
func IncrBy(key string, delta int64) int64 {
    var result int64
    store.Update(key, func(entry *Entry) *Entry {
        current := parseint64(entry.Value)
        result = current + delta
        entry.Value = formatInt64(result)
        return entry
    })
    return result
}
```

The `Update` function holds the shard lock during the entire callback.

### Interview Answer

> "For atomic operations like INCR, I use a lock-then-modify pattern. The store's Update function takes a callback that receives the current value and returns the new value. The shard mutex is held for the entire callback, guaranteeing atomicity without requiring external synchronisation."

---

## 7. Deadlock Prevention in Rename

### Decision
Lock shards in consistent address order when multiple shards need locking.

### Problem

```go
// DEADLOCK RISK
func Rename(oldKey, newKey string) {
    oldShard.Lock()
    newShard.Lock()  // What if another goroutine locks in reverse order?
}
```

### Solution

```go
func Rename(oldKey, newKey string) {
    first, second := oldShard, newShard
    if uintptr(unsafe.Pointer(oldShard)) > uintptr(unsafe.Pointer(newShard)) {
        first, second = newShard, oldShard
    }
    first.Lock()
    second.Lock()
    // Safe: consistent ordering
}
```

### Interview Answer

> "To prevent deadlocks when locking multiple shards, I lock them in consistent address order. Using `unsafe.Pointer` comparison ensures all goroutines acquire locks in the same order, preventing circular wait conditions."

---

## 8. Memory Efficiency: Empty Struct for Sets

### Decision
Use `map[string]struct{}` for sets instead of `map[string]bool`.

### Reasoning

```go
// Memory: 1 byte per entry
map[string]bool{}

// Memory: 0 bytes per entry
map[string]struct{}{}
```

Go's empty struct `struct{}` has size 0 and all instances share the same address.

### Interview Answer

> "I use `map[string]struct{}` for sets because empty structs have zero size. For a set with 1 million members, this saves 1MB of memory compared to `map[string]bool`."

---

## 9. Protocol: RESP (Redis Serialisation Protocol)

### Decision
Implement RESP for wire protocol, making the cache compatible with Redis clients.

### Reasoning

1. **Ecosystem**: Works with `redis-cli`, `redis-benchmark`, existing clients
2. **Simplicity**: Text-based, easy to debug with `nc` or `telnet`
3. **Efficiency**: Binary-safe, supports pipelining

### Trade-off

RESP is less efficient than a custom binary protocol. For this project, ecosystem compatibility outweighs the ~10% protocol overhead.

### Interview Answer

> "I chose RESP for Redis compatibility. This lets me use existing tools like `redis-cli` and `redis-benchmark` for testing. The protocol overhead is acceptable, and the debugging benefits of a text-based protocol are significant during development."

---

## 10. Testing: Race Detector First

### Decision
Run all tests with `-race` flag by default during development.

### Reasoning

```bash
# Catches data races at runtime
go test ./... -race
```

Go's race detector found several issues during development:
1. Non-atomic counter updates
2. Missing lock in early INCR implementation
3. Unsafe map iteration

### Interview Answer

> "I run all tests with Go's race detector enabled. It caught several concurrency bugs during development, including a race condition in my initial INCR implementation. The 2-10x slowdown is acceptable during testing for the bugs it catches."

---

## 11. Persistence: AOF + Binary Snapshots

### Decision
Use two complementary persistence mechanisms — an append-only file for command-level durability and binary snapshots for fast recovery.

### AOF Design

The AOF hook sits in `Handler.Execute()` — a single `if` block after the command succeeds. No individual command handlers are modified. I use a buffered channel (64K slots) with a dedicated writer goroutine that drains in batches, amortising syscall overhead. Non-blocking send on the hot path means the AOF never stalls command execution.

Three fsync policies:
- `always` — maximum durability, at most 1 command lost
- `everysec` — default, lose at most 1 second of data on crash
- `no` — OS decides, fastest but unbounded loss window

### Snapshot Design

I iterate shards sequentially. For each shard: copy map references under RLock (fast pointer copies), release lock, then encode outside any lock. At most 1/256th of writes are briefly blocked at any moment. Binary format with CRC-32C checksums (hardware-accelerated via SSE 4.2).

### AOF Rewrite

Iterates the store via `ForEachShard`, emitting the minimal command set (1000 INCRs become one SET). Writes to a temp file while the original AOF keeps logging. On completion, atomically renames.

### Interview Answer

> "I use two persistence mechanisms: AOF for command-level durability and binary snapshots for fast recovery. The AOF hook is decoupled from the hot path via a buffered channel — the writer goroutine drains in batches and fsyncs according to policy. Snapshots iterate shards sequentially, copying map references under RLock then encoding outside the lock, so at most 1/256th of writes are briefly blocked."

---

## 12. Replication: Async Master-Slave with PSYNC

### Decision
Implement Redis-style async replication with full and partial resync via a bounded ring buffer backlog.

### Backlog Design

I use a 1MB circular ring buffer (configurable) that stores raw RESP bytes. The master writes to it on every mutating command. Each slave's streaming goroutine independently reads from it, similar to how the AOF writer decouples disk I/O from command execution. Partial resync is possible when a slave's last offset is still within the buffer.

### Connection Hijacking

PSYNC switches a connection from request-response mode to unidirectional streaming. The handler sets `conn.hijacked = true`, the server's connection loop exits without closing the socket, and the master's streaming goroutine takes ownership. This avoids the overhead of a separate connection for replication.

### Slave Read-Only Guard

I check this in `Execute()` before dispatch, so no individual command handler needs to know about replication roles. REPLICAOF and SLAVEOF are always allowed (to enable promotion).

### Shared Replay Logic

I extracted `ExecuteReplayCommand` as an exported function so both Recovery (AOF replay) and the slave's `StoreApplier` share the same command execution logic — a single ~200-line switch statement rather than duplicate code.

### Interview Answer

> "I use async replication following Redis's model. The master writes mutations to a 1MB ring buffer backlog and each slave's goroutine independently reads from it. For PSYNC, I implemented connection hijacking — the socket switches from request-response to unidirectional streaming without closing. Partial resync from the backlog avoids expensive full snapshots when slaves reconnect quickly."

---

## 13. Observability: Prometheus Metrics

### Decision
Expose all internal metrics via a Prometheus `/metrics` endpoint on a separate HTTP port.

### Design

I use a custom `prometheus.Collector` that pulls current values from the existing atomic counters on each scrape. This means the metrics collection has zero impact on the hot path — the per-shard counters (gets, sets, hits, misses) are already lock-free atomics. For command-level latency, I add a `time.Since(start)` observation in `Execute()`, guarded behind a nil check so the handler works without metrics.

The metrics HTTP server runs on a separate port (default 9090) from the Redis protocol port (6379).

### Interview Answer

> "I expose all metrics via Prometheus using a custom Collector that scrapes the existing atomic counters on each scrape interval. There's zero overhead on the command hot path for store-level metrics since they're already lock-free atomics. For per-command latency histograms, I add one `time.Now()` call at the start of Execute and observe the duration after dispatch."

---

## Summary: Design Philosophy

1. **Simple over clever**: RWMutex over lock-free unless proven necessary
2. **Measure first**: Benchmarks guided optimisation decisions
3. **Trade-offs are explicit**: Document what I gave up and why
4. **Production-ready defaults**: 256 shards, lazy expiration, RESP protocol
5. **Single hook point**: Execute() is the central extensibility mechanism — AOF, replication, and metrics all hook in here
6. **Decouple from hot path**: Buffered channels for AOF, ring buffer for replication, scrape-time collection for metrics
