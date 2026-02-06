package benchmark

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/aravinth/distributed-cache/internal/datatype"
	"github.com/aravinth/distributed-cache/internal/store"
)

// Pre-generate keys for benchmarks to avoid string allocation overhead
var benchKeys []string
var benchValues [][]byte

func init() {
	const numKeys = 100000
	benchKeys = make([]string, numKeys)
	benchValues = make([][]byte, numKeys)

	for i := 0; i < numKeys; i++ {
		benchKeys[i] = fmt.Sprintf("key-%d", i)
		benchValues[i] = []byte(fmt.Sprintf("value-%d-with-some-additional-data-to-make-it-realistic", i))
	}
}

// ============== ShardedMap Benchmarks ==============

func BenchmarkShardedMap_Set(b *testing.B) {
	sm := store.NewShardedMap()
	entry := store.NewEntry(store.TypeString, []byte("benchmark-value"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sm.Set(benchKeys[i%len(benchKeys)], entry)
	}
}

func BenchmarkShardedMap_Set_Parallel(b *testing.B) {
	sm := store.NewShardedMap()
	entry := store.NewEntry(store.TypeString, []byte("benchmark-value"))

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sm.Set(benchKeys[i%len(benchKeys)], entry)
			i++
		}
	})
}

func BenchmarkShardedMap_Get(b *testing.B) {
	sm := store.NewShardedMap()

	// Pre-populate
	for _, key := range benchKeys {
		entry := store.NewEntry(store.TypeString, []byte("value"))
		sm.Set(key, entry)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sm.Get(benchKeys[i%len(benchKeys)])
	}
}

func BenchmarkShardedMap_Get_Parallel(b *testing.B) {
	sm := store.NewShardedMap()

	// Pre-populate
	for _, key := range benchKeys {
		entry := store.NewEntry(store.TypeString, []byte("value"))
		sm.Set(key, entry)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sm.Get(benchKeys[i%len(benchKeys)])
			i++
		}
	})
}

func BenchmarkShardedMap_Delete(b *testing.B) {
	sm := store.NewShardedMap()

	// Pre-populate (more than we'll delete)
	for _, key := range benchKeys {
		entry := store.NewEntry(store.TypeString, []byte("value"))
		sm.Set(key, entry)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sm.Delete(benchKeys[i%len(benchKeys)])
	}
}

func BenchmarkShardedMap_Mixed_80Read20Write(b *testing.B) {
	sm := store.NewShardedMap()

	// Pre-populate
	for _, key := range benchKeys {
		entry := store.NewEntry(store.TypeString, []byte("value"))
		sm.Set(key, entry)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := benchKeys[i%len(benchKeys)]
			if i%10 < 8 { // 80% reads
				sm.Get(key)
			} else { // 20% writes
				entry := store.NewEntry(store.TypeString, benchValues[i%len(benchValues)])
				sm.Set(key, entry)
			}
			i++
		}
	})
}

func BenchmarkShardedMap_Mixed_50Read50Write(b *testing.B) {
	sm := store.NewShardedMap()

	// Pre-populate
	for _, key := range benchKeys {
		entry := store.NewEntry(store.TypeString, []byte("value"))
		sm.Set(key, entry)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := benchKeys[i%len(benchKeys)]
			if i%2 == 0 { // 50% reads
				sm.Get(key)
			} else { // 50% writes
				entry := store.NewEntry(store.TypeString, benchValues[i%len(benchValues)])
				sm.Set(key, entry)
			}
			i++
		}
	})
}

func BenchmarkShardedMap_SetNX(b *testing.B) {
	sm := store.NewShardedMap()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry := store.NewEntry(store.TypeString, []byte("value"))
		sm.SetNX(benchKeys[i%len(benchKeys)], entry)
	}
}

func BenchmarkShardedMap_Exists(b *testing.B) {
	sm := store.NewShardedMap()

	// Pre-populate half the keys
	for i := 0; i < len(benchKeys)/2; i++ {
		entry := store.NewEntry(store.TypeString, []byte("value"))
		sm.Set(benchKeys[i], entry)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sm.Exists(benchKeys[i%len(benchKeys)])
	}
}

// ============== String Operations Benchmarks ==============

func BenchmarkStringOps_Set(b *testing.B) {
	sm := store.NewShardedMap()
	strOps := datatype.NewStringOps(sm)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strOps.Set(benchKeys[i%len(benchKeys)], benchValues[i%len(benchValues)])
	}
}

func BenchmarkStringOps_Get(b *testing.B) {
	sm := store.NewShardedMap()
	strOps := datatype.NewStringOps(sm)

	// Pre-populate
	for i, key := range benchKeys {
		strOps.Set(key, benchValues[i%len(benchValues)])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strOps.Get(benchKeys[i%len(benchKeys)])
	}
}

func BenchmarkStringOps_Incr(b *testing.B) {
	sm := store.NewShardedMap()
	strOps := datatype.NewStringOps(sm)

	// Pre-populate with numeric values
	for _, key := range benchKeys[:1000] { // Use fewer keys for incr
		strOps.Set(key, []byte("0"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strOps.Incr(benchKeys[i%1000])
	}
}

func BenchmarkStringOps_Incr_Parallel(b *testing.B) {
	sm := store.NewShardedMap()
	strOps := datatype.NewStringOps(sm)

	// Pre-populate with numeric values
	for _, key := range benchKeys[:1000] {
		strOps.Set(key, []byte("0"))
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			strOps.Incr(benchKeys[i%1000])
			i++
		}
	})
}

func BenchmarkStringOps_Append(b *testing.B) {
	sm := store.NewShardedMap()
	strOps := datatype.NewStringOps(sm)
	appendValue := []byte("appended")

	// Pre-populate
	for _, key := range benchKeys[:1000] {
		strOps.Set(key, []byte("initial"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strOps.Append(benchKeys[i%1000], appendValue)
	}
}

// ============== List Operations Benchmarks ==============

func BenchmarkListOps_LPush(b *testing.B) {
	sm := store.NewShardedMap()
	listOps := datatype.NewListOps(sm)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		listOps.LPush(benchKeys[i%1000], benchValues[i%len(benchValues)])
	}
}

func BenchmarkListOps_LPop(b *testing.B) {
	sm := store.NewShardedMap()
	listOps := datatype.NewListOps(sm)

	// Pre-populate lists
	for _, key := range benchKeys[:100] {
		for j := 0; j < 1000; j++ {
			listOps.RPush(key, benchValues[j%len(benchValues)])
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		listOps.LPop(benchKeys[i%100])
	}
}

func BenchmarkListOps_LRange(b *testing.B) {
	sm := store.NewShardedMap()
	listOps := datatype.NewListOps(sm)

	// Create a list with 1000 elements
	for i := 0; i < 1000; i++ {
		listOps.RPush("testlist", benchValues[i%len(benchValues)])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		listOps.LRange("testlist", 0, 99) // Get first 100 elements
	}
}

func BenchmarkListOps_LIndex(b *testing.B) {
	sm := store.NewShardedMap()
	listOps := datatype.NewListOps(sm)

	// Create a list with 1000 elements
	for i := 0; i < 1000; i++ {
		listOps.RPush("testlist", benchValues[i%len(benchValues)])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		listOps.LIndex("testlist", int64(i%1000))
	}
}

// ============== Hash Operations Benchmarks ==============

func BenchmarkHashOps_HSet(b *testing.B) {
	sm := store.NewShardedMap()
	hashOps := datatype.NewHashOps(sm)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		field := fmt.Sprintf("field-%d", i%100)
		hashOps.HSet(benchKeys[i%1000], []byte(field), benchValues[i%len(benchValues)])
	}
}

func BenchmarkHashOps_HGet(b *testing.B) {
	sm := store.NewShardedMap()
	hashOps := datatype.NewHashOps(sm)

	// Pre-populate
	for _, key := range benchKeys[:100] {
		for j := 0; j < 100; j++ {
			field := fmt.Sprintf("field-%d", j)
			hashOps.HSet(key, []byte(field), benchValues[j%len(benchValues)])
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		field := fmt.Sprintf("field-%d", i%100)
		hashOps.HGet(benchKeys[i%100], field)
	}
}

func BenchmarkHashOps_HGetAll(b *testing.B) {
	sm := store.NewShardedMap()
	hashOps := datatype.NewHashOps(sm)

	// Create a hash with 100 fields
	for j := 0; j < 100; j++ {
		field := fmt.Sprintf("field-%d", j)
		hashOps.HSet("testhash", []byte(field), benchValues[j%len(benchValues)])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hashOps.HGetAll("testhash")
	}
}

func BenchmarkHashOps_HIncrBy(b *testing.B) {
	sm := store.NewShardedMap()
	hashOps := datatype.NewHashOps(sm)

	// Pre-populate with numeric values
	for j := 0; j < 100; j++ {
		field := fmt.Sprintf("field-%d", j)
		hashOps.HSet("testhash", []byte(field), []byte("0"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		field := fmt.Sprintf("field-%d", i%100)
		hashOps.HIncrBy("testhash", field, 1)
	}
}

// ============== Set Operations Benchmarks ==============

func BenchmarkSetOps_SAdd(b *testing.B) {
	sm := store.NewShardedMap()
	setOps := datatype.NewSetOps(sm)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		member := fmt.Sprintf("member-%d", i)
		setOps.SAdd(benchKeys[i%1000], member)
	}
}

func BenchmarkSetOps_SIsMember(b *testing.B) {
	sm := store.NewShardedMap()
	setOps := datatype.NewSetOps(sm)

	// Pre-populate
	for _, key := range benchKeys[:100] {
		for j := 0; j < 100; j++ {
			member := fmt.Sprintf("member-%d", j)
			setOps.SAdd(key, member)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		member := fmt.Sprintf("member-%d", i%100)
		setOps.SIsMember(benchKeys[i%100], member)
	}
}

func BenchmarkSetOps_SMembers(b *testing.B) {
	sm := store.NewShardedMap()
	setOps := datatype.NewSetOps(sm)

	// Create a set with 100 members
	for j := 0; j < 100; j++ {
		member := fmt.Sprintf("member-%d", j)
		setOps.SAdd("testset", member)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		setOps.SMembers("testset")
	}
}

func BenchmarkSetOps_SUnion(b *testing.B) {
	sm := store.NewShardedMap()
	setOps := datatype.NewSetOps(sm)

	// Create 3 sets with 100 members each
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("set-%d", i)
		for j := 0; j < 100; j++ {
			member := fmt.Sprintf("member-%d", i*50+j) // Overlapping members
			setOps.SAdd(key, member)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		setOps.SUnion("set-0", "set-1", "set-2")
	}
}

func BenchmarkSetOps_SInter(b *testing.B) {
	sm := store.NewShardedMap()
	setOps := datatype.NewSetOps(sm)

	// Create 3 sets with overlapping members
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("set-%d", i)
		for j := 0; j < 100; j++ {
			member := fmt.Sprintf("member-%d", i*30+j) // Some overlap
			setOps.SAdd(key, member)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		setOps.SInter("set-0", "set-1", "set-2")
	}
}

// ============== Contention Benchmarks ==============

func BenchmarkShardedMap_HighContention_SingleKey(b *testing.B) {
	sm := store.NewShardedMap()
	key := "hot-key"
	entry := store.NewEntry(store.TypeString, []byte("value"))
	sm.Set(key, entry)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sm.Get(key)
		}
	})
}

func BenchmarkShardedMap_HighContention_SingleKey_Write(b *testing.B) {
	sm := store.NewShardedMap()
	key := "hot-key"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			entry := store.NewEntry(store.TypeString, []byte("value"))
			sm.Set(key, entry)
		}
	})
}

func BenchmarkShardedMap_LowContention_RandomKeys(b *testing.B) {
	sm := store.NewShardedMap()

	// Pre-populate
	for _, key := range benchKeys {
		entry := store.NewEntry(store.TypeString, []byte("value"))
		sm.Set(key, entry)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			key := benchKeys[r.Intn(len(benchKeys))]
			sm.Get(key)
		}
	})
}

// ============== Memory Allocation Benchmarks ==============

func BenchmarkShardedMap_Set_Allocs(b *testing.B) {
	sm := store.NewShardedMap()
	value := []byte("benchmark-value-with-some-realistic-size-data")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		entry := store.NewEntry(store.TypeString, value)
		sm.Set(benchKeys[i%len(benchKeys)], entry)
	}
}

func BenchmarkShardedMap_Get_Allocs(b *testing.B) {
	sm := store.NewShardedMap()

	// Pre-populate
	for _, key := range benchKeys {
		entry := store.NewEntry(store.TypeString, []byte("value"))
		sm.Set(key, entry)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sm.Get(benchKeys[i%len(benchKeys)])
	}
}

// ============== Scalability Benchmarks ==============

func BenchmarkShardedMap_Parallel_2(b *testing.B)   { benchmarkParallel(b, 2) }
func BenchmarkShardedMap_Parallel_4(b *testing.B)   { benchmarkParallel(b, 4) }
func BenchmarkShardedMap_Parallel_8(b *testing.B)   { benchmarkParallel(b, 8) }
func BenchmarkShardedMap_Parallel_16(b *testing.B)  { benchmarkParallel(b, 16) }
func BenchmarkShardedMap_Parallel_32(b *testing.B)  { benchmarkParallel(b, 32) }
func BenchmarkShardedMap_Parallel_64(b *testing.B)  { benchmarkParallel(b, 64) }
func BenchmarkShardedMap_Parallel_128(b *testing.B) { benchmarkParallel(b, 128) }

func benchmarkParallel(b *testing.B, parallelism int) {
	sm := store.NewShardedMap()

	// Pre-populate
	for _, key := range benchKeys {
		entry := store.NewEntry(store.TypeString, []byte("value"))
		sm.Set(key, entry)
	}

	b.SetParallelism(parallelism)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := benchKeys[i%len(benchKeys)]
			if i%10 < 8 { // 80% reads
				sm.Get(key)
			} else { // 20% writes
				entry := store.NewEntry(store.TypeString, benchValues[i%len(benchValues)])
				sm.Set(key, entry)
			}
			i++
		}
	})
}

// ============== Throughput Test ==============

func TestThroughput(t *testing.T) {
	sm := store.NewShardedMap()
	strOps := datatype.NewStringOps(sm)

	const numOps = 1000000
	const numWorkers = 100

	// Pre-populate
	for i := 0; i < 10000; i++ {
		strOps.Set(fmt.Sprintf("key-%d", i), []byte("value"))
	}

	var wg sync.WaitGroup
	opsPerWorker := numOps / numWorkers

	start := testing.AllocsPerRun(1, func() {})
	_ = start

	var totalOps int64
	var mu sync.Mutex

	wg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {
			defer wg.Done()
			localOps := 0
			for i := 0; i < opsPerWorker; i++ {
				key := fmt.Sprintf("key-%d", i%10000)
				if i%10 < 8 {
					strOps.Get(key)
				} else {
					strOps.Set(key, []byte("new-value"))
				}
				localOps++
			}
			mu.Lock()
			totalOps += int64(localOps)
			mu.Unlock()
		}(w)
	}

	wg.Wait()

	t.Logf("Total operations: %d", totalOps)
}
