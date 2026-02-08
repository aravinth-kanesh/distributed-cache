package store

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

const (
	// DefaultShardCount is the number of shards in the map
	// 256 shards provides good balance between memory overhead and contention reduction
	// With 1000 concurrent clients, this means ~4 clients per shard on average
	// Must be a power of 2 for efficient modulo via bit masking
	DefaultShardCount = 256

	// shardMask is used for fast modulo: hash & shardMask instead of hash % shardCount
	shardMask = DefaultShardCount - 1

	// initialShardCapacity is the initial capacity for each shard's map
	// Pre-allocation reduces rehashing overhead
	initialShardCapacity = 64
)

// ShardedMap is a thread-safe map split into multiple shards
// Each shard has its own RWMutex, allowing concurrent access to different shards
type ShardedMap struct {
	shards  [DefaultShardCount]*Shard
	metrics *StoreMetrics
}

// Shard represents a single partition of the map
// Each shard is protected by its own RWMutex
type Shard struct {
	mu   sync.RWMutex
	data map[string]*Entry

	// Per-shard metrics (atomic counters for lock-free observation)
	gets    atomic.Uint64
	sets    atomic.Uint64
	deletes atomic.Uint64
	hits    atomic.Uint64
	misses  atomic.Uint64
}

// StoreMetrics aggregates metrics across all shards
type StoreMetrics struct {
	KeyCount     atomic.Int64
	ExpiredCount atomic.Uint64
}

// NewShardedMap creates a new sharded map with initialised shards
func NewShardedMap() *ShardedMap {
	sm := &ShardedMap{
		metrics: &StoreMetrics{},
	}

	for i := 0; i < DefaultShardCount; i++ {
		sm.shards[i] = &Shard{
			data: make(map[string]*Entry, initialShardCapacity),
		}
	}

	return sm
}

// getShard returns the shard for a given key using xxHash and bit masking
// This is O(1) and avoids expensive modulo division
func (sm *ShardedMap) getShard(key string) *Shard {
	hash := xxhash.Sum64String(key)
	return sm.shards[hash&shardMask]
}

// GetShardIndex returns the shard index for a given key (useful for debugging)
func (sm *ShardedMap) GetShardIndex(key string) int {
	hash := xxhash.Sum64String(key)
	return int(hash & shardMask)
}

// Get retrieves an entry by key
// Returns nil and false if the key doesn't exist or is expired
func (sm *ShardedMap) Get(key string) (*Entry, bool) {
	shard := sm.getShard(key)

	shard.mu.RLock()
	entry, exists := shard.data[key]
	shard.mu.RUnlock()

	// Update metrics atomically (lock-free)
	shard.gets.Add(1)

	if !exists {
		shard.misses.Add(1)
		return nil, false
	}

	// Check expiration (lazy deletion)
	if entry.IsExpired() {
		// Key is expired - delete it
		sm.Delete(key)
		shard.misses.Add(1)
		sm.metrics.ExpiredCount.Add(1)
		return nil, false
	}

	shard.hits.Add(1)

	// Update access time atomically for potential LRU
	entry.Touch()

	return entry, true
}

// Set stores an entry with the given key
// If the key already exists, it will be overwritten
func (sm *ShardedMap) Set(key string, entry *Entry) {
	shard := sm.getShard(key)

	shard.mu.Lock()
	_, existed := shard.data[key]
	shard.data[key] = entry
	shard.mu.Unlock()

	shard.sets.Add(1)
	if !existed {
		sm.metrics.KeyCount.Add(1)
	}
}

// SetNX sets the entry only if the key doesn't exist (atomic operation)
// Returns true if the key was set, false if it already existed
func (sm *ShardedMap) SetNX(key string, entry *Entry) bool {
	shard := sm.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if existing, exists := shard.data[key]; exists {
		// Check if existing key is expired
		if !existing.IsExpired() {
			return false
		}
		// Key exists but is expired, treat as not existing
		sm.metrics.ExpiredCount.Add(1)
	}

	shard.data[key] = entry
	sm.metrics.KeyCount.Add(1)
	shard.sets.Add(1)
	return true
}

// SetXX sets the entry only if the key already exists (atomic operation)
// Returns true if the key was set, false if it didn't exist
func (sm *ShardedMap) SetXX(key string, entry *Entry) bool {
	shard := sm.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	existing, exists := shard.data[key]
	if !exists {
		return false
	}

	// Check if existing key is expired
	if existing.IsExpired() {
		delete(shard.data, key)
		sm.metrics.KeyCount.Add(-1)
		sm.metrics.ExpiredCount.Add(1)
		return false
	}

	shard.data[key] = entry
	shard.sets.Add(1)
	return true
}

// GetSet atomically sets a new value and returns the old value
// Returns nil if the key didn't exist
func (sm *ShardedMap) GetSet(key string, entry *Entry) (*Entry, bool) {
	shard := sm.getShard(key)

	shard.mu.Lock()
	old, existed := shard.data[key]
	shard.data[key] = entry
	shard.mu.Unlock()

	shard.gets.Add(1)
	shard.sets.Add(1)

	if !existed {
		sm.metrics.KeyCount.Add(1)
		return nil, false
	}

	// Check if old key was expired
	if old.IsExpired() {
		sm.metrics.ExpiredCount.Add(1)
		return nil, false
	}

	return old, true
}

// Delete removes a key from the map
// Returns true if the key existed, false otherwise
func (sm *ShardedMap) Delete(key string) bool {
	shard := sm.getShard(key)

	shard.mu.Lock()
	_, existed := shard.data[key]
	if existed {
		delete(shard.data, key)
	}
	shard.mu.Unlock()

	if existed {
		shard.deletes.Add(1)
		sm.metrics.KeyCount.Add(-1)
	}

	return existed
}

// DeleteMulti removes multiple keys from the map
// Returns the count of keys that were deleted
func (sm *ShardedMap) DeleteMulti(keys ...string) int64 {
	var deleted int64

	// Group keys by shard for efficiency
	shardKeys := make(map[int][]string)
	for _, key := range keys {
		idx := sm.GetShardIndex(key)
		shardKeys[idx] = append(shardKeys[idx], key)
	}

	// Delete from each shard
	for idx, ks := range shardKeys {
		shard := sm.shards[idx]
		shard.mu.Lock()
		for _, k := range ks {
			if _, exists := shard.data[k]; exists {
				delete(shard.data, k)
				deleted++
			}
		}
		shard.mu.Unlock()
	}

	if deleted > 0 {
		sm.metrics.KeyCount.Add(-deleted)
	}

	return deleted
}

// Exists checks if a key exists in the map (without updating access time)
// Returns false for expired keys
func (sm *ShardedMap) Exists(key string) bool {
	shard := sm.getShard(key)

	shard.mu.RLock()
	entry, exists := shard.data[key]
	shard.mu.RUnlock()

	if !exists {
		return false
	}

	// Check expiration
	if entry.IsExpired() {
		return false
	}

	return true
}

// ExistsMulti checks existence of multiple keys
// Returns a map of key -> exists
func (sm *ShardedMap) ExistsMulti(keys ...string) int64 {
	var count int64

	for _, key := range keys {
		if sm.Exists(key) {
			count++
		}
	}

	return count
}

// Type returns the type of the value stored at key
// Returns -1 if the key doesn't exist
func (sm *ShardedMap) Type(key string) EntryType {
	entry, exists := sm.Get(key)
	if !exists {
		return EntryType(255) // Invalid type
	}
	return entry.Type
}

// Rename renames a key atomically
// Returns an error if the source key doesn't exist
func (sm *ShardedMap) Rename(oldKey, newKey string) bool {
	if oldKey == newKey {
		return true
	}

	oldShard := sm.getShard(oldKey)
	newShard := sm.getShard(newKey)

	// Lock in consistent order to prevent deadlocks
	if oldShard == newShard {
		oldShard.mu.Lock()
		defer oldShard.mu.Unlock()
	} else {
		// Lock shards in address order to prevent deadlock
		first, second := oldShard, newShard
		if uintptr(unsafe.Pointer(oldShard)) > uintptr(unsafe.Pointer(newShard)) {
			first, second = newShard, oldShard
		}
		first.mu.Lock()
		defer first.mu.Unlock()
		second.mu.Lock()
		defer second.mu.Unlock()
	}

	entry, exists := oldShard.data[oldKey]
	if !exists {
		return false
	}

	// Check if expired
	if entry.IsExpired() {
		delete(oldShard.data, oldKey)
		sm.metrics.KeyCount.Add(-1)
		sm.metrics.ExpiredCount.Add(1)
		return false
	}

	// Check if newKey exists (will be overwritten)
	_, newExists := newShard.data[newKey]

	// Perform rename
	delete(oldShard.data, oldKey)
	newShard.data[newKey] = entry

	// Adjust key count if needed
	if !newExists {
		// We deleted one and added one, but they're different keys
		// Actually, we just moved, so count stays the same
	} else {
		// We overwrote an existing key, so count decreases by 1
		sm.metrics.KeyCount.Add(-1)
	}

	return true
}

// Keys returns all keys in the map (expensive operation)
// Use with caution in production
func (sm *ShardedMap) Keys() []string {
	var keys []string

	for i := 0; i < DefaultShardCount; i++ {
		shard := sm.shards[i]
		shard.mu.RLock()
		for k, entry := range shard.data {
			if !entry.IsExpired() {
				keys = append(keys, k)
			}
		}
		shard.mu.RUnlock()
	}

	return keys
}

// KeyCount returns the approximate number of keys in the map
// This is an atomic read and very fast
func (sm *ShardedMap) KeyCount() int64 {
	return sm.metrics.KeyCount.Load()
}

// Clear removes all entries from the map
func (sm *ShardedMap) Clear() {
	for i := 0; i < DefaultShardCount; i++ {
		shard := sm.shards[i]
		shard.mu.Lock()
		shard.data = make(map[string]*Entry, initialShardCapacity)
		shard.mu.Unlock()
	}
	sm.metrics.KeyCount.Store(0)
}

// ForEach iterates over all entries in the map
// The callback should not modify the map
func (sm *ShardedMap) ForEach(fn func(key string, entry *Entry) bool) {
	for i := 0; i < DefaultShardCount; i++ {
		shard := sm.shards[i]
		shard.mu.RLock()
		for k, entry := range shard.data {
			if !entry.IsExpired() {
				if !fn(k, entry) {
					shard.mu.RUnlock()
					return
				}
			}
		}
		shard.mu.RUnlock()
	}
}

// ForEachShard iterates one shard at a time, copying map references under
// RLock then processing outside the lock. This minimises lock hold time
// compared to ForEach, which holds the RLock for the entire callback.
// I use this for snapshots where encoding can be slow.
func (sm *ShardedMap) ForEachShard(fn func(data map[string]*Entry)) {
	for i := 0; i < DefaultShardCount; i++ {
		shard := sm.shards[i]
		shard.mu.RLock()
		snapshot := make(map[string]*Entry, len(shard.data))
		for k, v := range shard.data {
			if !v.IsExpired() {
				snapshot[k] = v
			}
		}
		shard.mu.RUnlock()
		fn(snapshot)
	}
}

// ExpireScan scans for and removes expired keys
// Returns the number of expired keys removed
func (sm *ShardedMap) ExpireScan(maxKeys int) int64 {
	var expired int64
	keysChecked := 0

	for i := 0; i < DefaultShardCount && keysChecked < maxKeys; i++ {
		shard := sm.shards[i]
		shard.mu.Lock()

		for k, entry := range shard.data {
			if keysChecked >= maxKeys {
				break
			}
			keysChecked++

			if entry.IsExpired() {
				delete(shard.data, k)
				expired++
			}
		}

		shard.mu.Unlock()
	}

	if expired > 0 {
		sm.metrics.KeyCount.Add(-expired)
		sm.metrics.ExpiredCount.Add(uint64(expired))
	}

	return expired
}

// Metrics aggregation methods

// GetMetrics returns aggregated metrics from all shards
func (sm *ShardedMap) GetMetrics() map[string]uint64 {
	var gets, sets, deletes, hits, misses uint64

	for i := 0; i < DefaultShardCount; i++ {
		shard := sm.shards[i]
		gets += shard.gets.Load()
		sets += shard.sets.Load()
		deletes += shard.deletes.Load()
		hits += shard.hits.Load()
		misses += shard.misses.Load()
	}

	return map[string]uint64{
		"gets":          gets,
		"sets":          sets,
		"deletes":       deletes,
		"hits":          hits,
		"misses":        misses,
		"keys":          uint64(sm.metrics.KeyCount.Load()),
		"expired_count": sm.metrics.ExpiredCount.Load(),
	}
}

// HitRatio returns the cache hit ratio (0.0 to 1.0)
func (sm *ShardedMap) HitRatio() float64 {
	var hits, misses uint64

	for i := 0; i < DefaultShardCount; i++ {
		shard := sm.shards[i]
		hits += shard.hits.Load()
		misses += shard.misses.Load()
	}

	total := hits + misses
	if total == 0 {
		return 0.0
	}

	return float64(hits) / float64(total)
}

// GetEntry retrieves an entry without updating metrics (internal use)
func (sm *ShardedMap) GetEntry(key string) (*Entry, bool) {
	shard := sm.getShard(key)

	shard.mu.RLock()
	entry, exists := shard.data[key]
	shard.mu.RUnlock()

	if !exists || entry.IsExpired() {
		return nil, false
	}

	return entry, true
}

// Update atomically updates an entry if it exists
// The update function is called with the shard lock held
func (sm *ShardedMap) Update(key string, fn func(*Entry) *Entry) bool {
	shard := sm.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.data[key]
	if !exists || entry.IsExpired() {
		return false
	}

	newEntry := fn(entry)
	if newEntry != nil {
		shard.data[key] = newEntry
	}

	return true
}

// GetOrSet gets an existing entry or sets a new one atomically
// Returns the entry and true if it existed, false if it was created
func (sm *ShardedMap) GetOrSet(key string, entry *Entry) (*Entry, bool) {
	shard := sm.getShard(key)

	// Try read lock first (optimistic)
	shard.mu.RLock()
	existing, exists := shard.data[key]
	shard.mu.RUnlock()

	if exists && !existing.IsExpired() {
		existing.Touch()
		return existing, true
	}

	// Need to write
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Double-check after acquiring write lock
	existing, exists = shard.data[key]
	if exists && !existing.IsExpired() {
		existing.Touch()
		return existing, true
	}

	// Handle expired key
	if exists {
		sm.metrics.ExpiredCount.Add(1)
	} else {
		sm.metrics.KeyCount.Add(1)
	}

	shard.data[key] = entry
	shard.sets.Add(1)
	return entry, false
}
