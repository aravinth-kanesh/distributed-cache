package unit

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aravinth/distributed-cache/internal/store"
)

func TestShardedMap_BasicOperations(t *testing.T) {
	sm := store.NewShardedMap()

	// Test Set and Get
	entry := store.NewEntry(store.TypeString, []byte("value1"))
	sm.Set("key1", entry)

	got, exists := sm.Get("key1")
	if !exists {
		t.Fatal("expected key1 to exist")
	}

	val, ok := got.Value.([]byte)
	if !ok {
		t.Fatal("expected value to be []byte")
	}

	if string(val) != "value1" {
		t.Errorf("expected value1, got %s", string(val))
	}

	// Test Exists
	if !sm.Exists("key1") {
		t.Error("expected key1 to exist")
	}
	if sm.Exists("nonexistent") {
		t.Error("expected nonexistent key to not exist")
	}

	// Test Delete
	deleted := sm.Delete("key1")
	if !deleted {
		t.Error("expected key1 to be deleted")
	}

	_, exists = sm.Get("key1")
	if exists {
		t.Error("expected key1 to not exist after deletion")
	}

	// Test Delete non-existent
	deleted = sm.Delete("nonexistent")
	if deleted {
		t.Error("expected nonexistent key deletion to return false")
	}
}

func TestShardedMap_SetNX(t *testing.T) {
	sm := store.NewShardedMap()

	entry1 := store.NewEntry(store.TypeString, []byte("value1"))
	entry2 := store.NewEntry(store.TypeString, []byte("value2"))

	// First SetNX should succeed
	if !sm.SetNX("key1", entry1) {
		t.Error("first SetNX should succeed")
	}

	// Second SetNX should fail
	if sm.SetNX("key1", entry2) {
		t.Error("second SetNX should fail")
	}

	// Verify original value is preserved
	got, _ := sm.Get("key1")
	val, _ := got.Value.([]byte)
	if string(val) != "value1" {
		t.Errorf("expected value1, got %s", string(val))
	}
}

func TestShardedMap_SetXX(t *testing.T) {
	sm := store.NewShardedMap()

	entry1 := store.NewEntry(store.TypeString, []byte("value1"))
	entry2 := store.NewEntry(store.TypeString, []byte("value2"))

	// SetXX on non-existent key should fail
	if sm.SetXX("key1", entry2) {
		t.Error("SetXX on non-existent key should fail")
	}

	// Create key first
	sm.Set("key1", entry1)

	// SetXX should now succeed
	if !sm.SetXX("key1", entry2) {
		t.Error("SetXX on existing key should succeed")
	}

	// Verify new value
	got, _ := sm.Get("key1")
	val, _ := got.Value.([]byte)
	if string(val) != "value2" {
		t.Errorf("expected value2, got %s", string(val))
	}
}

func TestShardedMap_GetSet(t *testing.T) {
	sm := store.NewShardedMap()

	entry1 := store.NewEntry(store.TypeString, []byte("value1"))
	entry2 := store.NewEntry(store.TypeString, []byte("value2"))

	// GetSet on non-existent key
	old, existed := sm.GetSet("key1", entry1)
	if existed {
		t.Error("expected no old value")
	}
	if old != nil {
		t.Error("expected nil old value")
	}

	// GetSet on existing key
	old, existed = sm.GetSet("key1", entry2)
	if !existed {
		t.Error("expected old value to exist")
	}
	val, _ := old.Value.([]byte)
	if string(val) != "value1" {
		t.Errorf("expected old value1, got %s", string(val))
	}
}

func TestShardedMap_Expiration(t *testing.T) {
	sm := store.NewShardedMap()

	// Create entry with short TTL
	entry := store.NewEntryWithTTL(store.TypeString, []byte("value"), 1)
	sm.Set("expiring", entry)

	// Should exist immediately
	_, exists := sm.Get("expiring")
	if !exists {
		t.Error("key should exist immediately after set")
	}

	// Wait for expiration
	time.Sleep(1100 * time.Millisecond)

	// Should not exist after expiration
	_, exists = sm.Get("expiring")
	if exists {
		t.Error("key should not exist after expiration")
	}
}

func TestShardedMap_DeleteMulti(t *testing.T) {
	sm := store.NewShardedMap()

	// Create multiple keys
	for i := 0; i < 10; i++ {
		entry := store.NewEntry(store.TypeString, []byte(fmt.Sprintf("value%d", i)))
		sm.Set(fmt.Sprintf("key%d", i), entry)
	}

	// Delete some keys
	deleted := sm.DeleteMulti("key1", "key3", "key5", "key7", "nonexistent")
	if deleted != 4 {
		t.Errorf("expected 4 keys deleted, got %d", deleted)
	}

	// Verify
	if sm.Exists("key1") || sm.Exists("key3") || sm.Exists("key5") || sm.Exists("key7") {
		t.Error("deleted keys should not exist")
	}
	if !sm.Exists("key0") || !sm.Exists("key2") {
		t.Error("non-deleted keys should still exist")
	}
}

func TestShardedMap_KeyCount(t *testing.T) {
	sm := store.NewShardedMap()

	if sm.KeyCount() != 0 {
		t.Error("empty map should have 0 keys")
	}

	// Add keys
	for i := 0; i < 100; i++ {
		entry := store.NewEntry(store.TypeString, []byte("value"))
		sm.Set(fmt.Sprintf("key%d", i), entry)
	}

	if sm.KeyCount() != 100 {
		t.Errorf("expected 100 keys, got %d", sm.KeyCount())
	}

	// Delete some
	for i := 0; i < 50; i++ {
		sm.Delete(fmt.Sprintf("key%d", i))
	}

	if sm.KeyCount() != 50 {
		t.Errorf("expected 50 keys after deletion, got %d", sm.KeyCount())
	}
}

func TestShardedMap_Clear(t *testing.T) {
	sm := store.NewShardedMap()

	// Add keys
	for i := 0; i < 1000; i++ {
		entry := store.NewEntry(store.TypeString, []byte("value"))
		sm.Set(fmt.Sprintf("key%d", i), entry)
	}

	sm.Clear()

	if sm.KeyCount() != 0 {
		t.Errorf("expected 0 keys after clear, got %d", sm.KeyCount())
	}
}

func TestShardedMap_ForEach(t *testing.T) {
	sm := store.NewShardedMap()

	// Add keys
	for i := 0; i < 100; i++ {
		entry := store.NewEntry(store.TypeString, []byte(fmt.Sprintf("value%d", i)))
		sm.Set(fmt.Sprintf("key%d", i), entry)
	}

	count := 0
	sm.ForEach(func(key string, entry *store.Entry) bool {
		count++
		return true
	})

	if count != 100 {
		t.Errorf("expected ForEach to visit 100 keys, visited %d", count)
	}

	// Test early termination
	count = 0
	sm.ForEach(func(key string, entry *store.Entry) bool {
		count++
		return count < 10
	})

	if count != 10 {
		t.Errorf("expected ForEach to stop at 10, stopped at %d", count)
	}
}

func TestShardedMap_Metrics(t *testing.T) {
	sm := store.NewShardedMap()

	// Perform operations
	for i := 0; i < 100; i++ {
		entry := store.NewEntry(store.TypeString, []byte("value"))
		sm.Set(fmt.Sprintf("key%d", i), entry)
	}

	for i := 0; i < 150; i++ {
		sm.Get(fmt.Sprintf("key%d", i%100))
	}

	for i := 0; i < 50; i++ {
		sm.Get(fmt.Sprintf("nonexistent%d", i))
	}

	metrics := sm.GetMetrics()

	if metrics["sets"] != 100 {
		t.Errorf("expected 100 sets, got %d", metrics["sets"])
	}

	if metrics["gets"] != 200 {
		t.Errorf("expected 200 gets, got %d", metrics["gets"])
	}

	if metrics["hits"] != 150 {
		t.Errorf("expected 150 hits, got %d", metrics["hits"])
	}

	if metrics["misses"] != 50 {
		t.Errorf("expected 50 misses, got %d", metrics["misses"])
	}

	hitRatio := sm.HitRatio()
	expectedRatio := 150.0 / 200.0
	if hitRatio < expectedRatio-0.01 || hitRatio > expectedRatio+0.01 {
		t.Errorf("expected hit ratio %.2f, got %.2f", expectedRatio, hitRatio)
	}
}

func TestShardedMap_ShardDistribution(t *testing.T) {
	sm := store.NewShardedMap()

	// Add many keys and check distribution
	shardCounts := make(map[int]int)
	numKeys := 10000

	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		shardIdx := sm.GetShardIndex(key)
		shardCounts[shardIdx]++
	}

	// Check that we use all shards
	if len(shardCounts) < store.DefaultShardCount/2 {
		t.Errorf("poor shard distribution: only %d shards used", len(shardCounts))
	}

	// Check for reasonable distribution (no shard should have more than 3x average)
	avgPerShard := float64(numKeys) / float64(store.DefaultShardCount)
	maxAllowed := int(avgPerShard * 3)

	for shardIdx, count := range shardCounts {
		if count > maxAllowed {
			t.Errorf("shard %d has %d keys (max allowed: %d)", shardIdx, count, maxAllowed)
		}
	}
}

// Concurrent tests

func TestShardedMap_ConcurrentReadWrite(t *testing.T) {
	sm := store.NewShardedMap()

	const numGoroutines = 100
	const numOperations = 1000

	var wg sync.WaitGroup

	// Writers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				entry := store.NewEntry(store.TypeString, []byte(fmt.Sprintf("value-%d", j)))
				sm.Set(key, entry)
			}
		}(i)
	}

	// Readers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				sm.Get(key)
			}
		}(i)
	}

	wg.Wait()
}

func TestShardedMap_ConcurrentSameKey(t *testing.T) {
	sm := store.NewShardedMap()

	const numGoroutines = 100
	const numOperations = 1000

	var wg sync.WaitGroup
	key := "contested-key"

	// Concurrent writers to same key
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				entry := store.NewEntry(store.TypeString, []byte(fmt.Sprintf("value-%d-%d", id, j)))
				sm.Set(key, entry)
			}
		}(i)
	}

	// Concurrent readers of same key
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				sm.Get(key)
			}
		}()
	}

	wg.Wait()

	// Key should exist with some value
	_, exists := sm.Get(key)
	if !exists {
		t.Error("key should exist after concurrent operations")
	}
}

func TestShardedMap_ConcurrentMixedOperations(t *testing.T) {
	sm := store.NewShardedMap()

	const numGoroutines = 50
	const numOperations = 500

	var wg sync.WaitGroup

	// Pre-populate
	for i := 0; i < 1000; i++ {
		entry := store.NewEntry(store.TypeString, []byte("value"))
		sm.Set(fmt.Sprintf("key%d", i), entry)
	}

	// Writers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key%d", j%1000)
				entry := store.NewEntry(store.TypeString, []byte(fmt.Sprintf("value-%d", j)))
				sm.Set(key, entry)
			}
		}(i)
	}

	// Readers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key%d", j%1000)
				sm.Get(key)
			}
		}()
	}

	// Deleters
	for i := 0; i < numGoroutines/5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations/10; j++ {
				key := fmt.Sprintf("key%d", j%1000)
				sm.Delete(key)
			}
		}()
	}

	wg.Wait()
}

func TestShardedMap_ConcurrentSetNX(t *testing.T) {
	sm := store.NewShardedMap()

	const numGoroutines = 100
	key := "setnx-key"

	var successCount int64
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			entry := store.NewEntry(store.TypeString, []byte(fmt.Sprintf("value-%d", id)))
			if sm.SetNX(key, entry) {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Only one SetNX should succeed
	if successCount != 1 {
		t.Errorf("expected exactly 1 SetNX to succeed, got %d", successCount)
	}
}

func TestEntry_TTL(t *testing.T) {
	// Entry without TTL
	entry1 := store.NewEntry(store.TypeString, []byte("value"))
	if entry1.TTL() != -1 {
		t.Errorf("entry without TTL should return -1, got %d", entry1.TTL())
	}

	// Entry with TTL
	entry2 := store.NewEntryWithTTL(store.TypeString, []byte("value"), 10)
	ttl := entry2.TTL()
	if ttl < 9 || ttl > 10 {
		t.Errorf("expected TTL ~10, got %d", ttl)
	}

	// Expired entry
	entry3 := store.NewEntryWithTTL(store.TypeString, []byte("value"), 0)
	entry3.ExpiresAt = time.Now().Add(-time.Second).UnixNano()
	if entry3.TTL() != -2 {
		t.Errorf("expired entry should return -2, got %d", entry3.TTL())
	}
}

func TestEntry_Touch(t *testing.T) {
	entry := store.NewEntry(store.TypeString, []byte("value"))

	firstAccess := entry.LastAccessed()
	time.Sleep(10 * time.Millisecond)

	entry.Touch()
	secondAccess := entry.LastAccessed()

	if secondAccess <= firstAccess {
		t.Error("Touch should update access time")
	}
}

func TestEntry_Persist(t *testing.T) {
	entry := store.NewEntryWithTTL(store.TypeString, []byte("value"), 10)

	if entry.ExpiresAt == 0 {
		t.Error("entry should have expiration")
	}

	if !entry.Persist() {
		t.Error("Persist should return true for entry with TTL")
	}

	if entry.ExpiresAt != 0 {
		t.Error("entry should not have expiration after Persist")
	}

	if entry.Persist() {
		t.Error("Persist should return false for entry without TTL")
	}
}
