package unit

import (
	"fmt"
	"sync"
	"testing"

	"github.com/aravinth/distributed-cache/internal/datatype"
	"github.com/aravinth/distributed-cache/internal/store"
)

// String tests

func TestStringOps_GetSet(t *testing.T) {
	sm := store.NewShardedMap()
	strOps := datatype.NewStringOps(sm)

	// Get non-existent key
	_, ok := strOps.Get("key1")
	if ok {
		t.Error("expected Get on non-existent key to return false")
	}

	// Set and Get
	strOps.Set("key1", []byte("value1"))

	val, ok := strOps.Get("key1")
	if !ok {
		t.Error("expected Get to return true")
	}
	if string(val) != "value1" {
		t.Errorf("expected value1, got %s", string(val))
	}
}

func TestStringOps_SetNX(t *testing.T) {
	sm := store.NewShardedMap()
	strOps := datatype.NewStringOps(sm)

	if !strOps.SetNX("key1", []byte("value1")) {
		t.Error("first SetNX should succeed")
	}

	if strOps.SetNX("key1", []byte("value2")) {
		t.Error("second SetNX should fail")
	}

	val, _ := strOps.Get("key1")
	if string(val) != "value1" {
		t.Errorf("expected value1, got %s", string(val))
	}
}

func TestStringOps_MGet(t *testing.T) {
	sm := store.NewShardedMap()
	strOps := datatype.NewStringOps(sm)

	strOps.Set("key1", []byte("value1"))
	strOps.Set("key2", []byte("value2"))
	strOps.Set("key3", []byte("value3"))

	result := strOps.MGet("key1", "key2", "nonexistent", "key3")

	if len(result) != 4 {
		t.Errorf("expected 4 results, got %d", len(result))
	}

	if string(result[0]) != "value1" {
		t.Error("wrong value for key1")
	}
	if string(result[1]) != "value2" {
		t.Error("wrong value for key2")
	}
	if result[2] != nil {
		t.Error("nonexistent key should return nil")
	}
	if string(result[3]) != "value3" {
		t.Error("wrong value for key3")
	}
}

func TestStringOps_Incr(t *testing.T) {
	sm := store.NewShardedMap()
	strOps := datatype.NewStringOps(sm)

	// Incr on non-existent key
	val, ok := strOps.Incr("counter")
	if !ok || val != 1 {
		t.Errorf("expected 1, got %d", val)
	}

	// Incr on existing key
	val, ok = strOps.Incr("counter")
	if !ok || val != 2 {
		t.Errorf("expected 2, got %d", val)
	}

	// IncrBy
	val, ok = strOps.IncrBy("counter", 10)
	if !ok || val != 12 {
		t.Errorf("expected 12, got %d", val)
	}

	// DecrBy
	val, ok = strOps.DecrBy("counter", 5)
	if !ok || val != 7 {
		t.Errorf("expected 7, got %d", val)
	}
}

func TestStringOps_Append(t *testing.T) {
	sm := store.NewShardedMap()
	strOps := datatype.NewStringOps(sm)

	// Append to non-existent key
	len1 := strOps.Append("key1", []byte("hello"))
	if len1 != 5 {
		t.Errorf("expected length 5, got %d", len1)
	}

	// Append to existing key
	len2 := strOps.Append("key1", []byte(" world"))
	if len2 != 11 {
		t.Errorf("expected length 11, got %d", len2)
	}

	val, _ := strOps.Get("key1")
	if string(val) != "hello world" {
		t.Errorf("expected 'hello world', got '%s'", string(val))
	}
}

func TestStringOps_GetRange(t *testing.T) {
	sm := store.NewShardedMap()
	strOps := datatype.NewStringOps(sm)

	strOps.Set("key1", []byte("Hello, World!"))

	// Normal range
	val := strOps.GetRange("key1", 0, 4)
	if string(val) != "Hello" {
		t.Errorf("expected 'Hello', got '%s'", string(val))
	}

	// Negative indices
	val = strOps.GetRange("key1", -6, -1)
	if string(val) != "World!" {
		t.Errorf("expected 'World!', got '%s'", string(val))
	}

	// Out of bounds
	val = strOps.GetRange("key1", 0, 100)
	if string(val) != "Hello, World!" {
		t.Errorf("expected full string, got '%s'", string(val))
	}
}

// List tests

func TestListOps_PushPop(t *testing.T) {
	sm := store.NewShardedMap()
	listOps := datatype.NewListOps(sm)

	// LPush
	len1 := listOps.LPush("list1", []byte("a"), []byte("b"), []byte("c"))
	if len1 != 3 {
		t.Errorf("expected length 3, got %d", len1)
	}

	// LLen
	if listOps.LLen("list1") != 3 {
		t.Error("expected length 3")
	}

	// LPop
	val, ok := listOps.LPop("list1")
	if !ok || string(val) != "c" {
		t.Errorf("expected 'c', got '%s'", string(val))
	}

	// RPush
	listOps.RPush("list1", []byte("d"))

	// RPop
	val, ok = listOps.RPop("list1")
	if !ok || string(val) != "d" {
		t.Errorf("expected 'd', got '%s'", string(val))
	}
}

func TestListOps_LRange(t *testing.T) {
	sm := store.NewShardedMap()
	listOps := datatype.NewListOps(sm)

	listOps.RPush("list1", []byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"))

	// Full range
	result := listOps.LRange("list1", 0, -1)
	if len(result) != 5 {
		t.Errorf("expected 5 elements, got %d", len(result))
	}

	// Partial range
	result = listOps.LRange("list1", 1, 3)
	if len(result) != 3 {
		t.Errorf("expected 3 elements, got %d", len(result))
	}
	if string(result[0]) != "b" || string(result[1]) != "c" || string(result[2]) != "d" {
		t.Error("wrong elements in range")
	}

	// Negative indices
	result = listOps.LRange("list1", -3, -1)
	if len(result) != 3 {
		t.Errorf("expected 3 elements, got %d", len(result))
	}
}

func TestListOps_LIndex(t *testing.T) {
	sm := store.NewShardedMap()
	listOps := datatype.NewListOps(sm)

	listOps.RPush("list1", []byte("a"), []byte("b"), []byte("c"))

	// Positive index
	val, ok := listOps.LIndex("list1", 1)
	if !ok || string(val) != "b" {
		t.Errorf("expected 'b', got '%s'", string(val))
	}

	// Negative index
	val, ok = listOps.LIndex("list1", -1)
	if !ok || string(val) != "c" {
		t.Errorf("expected 'c', got '%s'", string(val))
	}

	// Out of range
	_, ok = listOps.LIndex("list1", 100)
	if ok {
		t.Error("expected false for out of range index")
	}
}

func TestListOps_LSet(t *testing.T) {
	sm := store.NewShardedMap()
	listOps := datatype.NewListOps(sm)

	listOps.RPush("list1", []byte("a"), []byte("b"), []byte("c"))

	if !listOps.LSet("list1", 1, []byte("x")) {
		t.Error("LSet should succeed")
	}

	val, _ := listOps.LIndex("list1", 1)
	if string(val) != "x" {
		t.Errorf("expected 'x', got '%s'", string(val))
	}
}

// Hash tests

func TestHashOps_BasicOperations(t *testing.T) {
	sm := store.NewShardedMap()
	hashOps := datatype.NewHashOps(sm)

	// HSet
	added := hashOps.HSet("hash1", []byte("field1"), []byte("value1"), []byte("field2"), []byte("value2"))
	if added != 2 {
		t.Errorf("expected 2 fields added, got %d", added)
	}

	// HGet
	val, ok := hashOps.HGet("hash1", "field1")
	if !ok || string(val) != "value1" {
		t.Error("wrong value for field1")
	}

	// HExists
	if !hashOps.HExists("hash1", "field1") {
		t.Error("field1 should exist")
	}
	if hashOps.HExists("hash1", "nonexistent") {
		t.Error("nonexistent field should not exist")
	}

	// HLen
	if hashOps.HLen("hash1") != 2 {
		t.Error("expected length 2")
	}

	// HDel
	deleted := hashOps.HDel("hash1", "field1")
	if deleted != 1 {
		t.Errorf("expected 1 field deleted, got %d", deleted)
	}
}

func TestHashOps_HGetAll(t *testing.T) {
	sm := store.NewShardedMap()
	hashOps := datatype.NewHashOps(sm)

	hashOps.HSet("hash1", []byte("a"), []byte("1"), []byte("b"), []byte("2"), []byte("c"), []byte("3"))

	all := hashOps.HGetAll("hash1")
	if len(all) != 3 {
		t.Errorf("expected 3 fields, got %d", len(all))
	}
}

func TestHashOps_HIncrBy(t *testing.T) {
	sm := store.NewShardedMap()
	hashOps := datatype.NewHashOps(sm)

	// IncrBy on non-existent field
	val, ok := hashOps.HIncrBy("hash1", "counter", 5)
	if !ok || val != 5 {
		t.Errorf("expected 5, got %d", val)
	}

	// IncrBy on existing field
	val, ok = hashOps.HIncrBy("hash1", "counter", 3)
	if !ok || val != 8 {
		t.Errorf("expected 8, got %d", val)
	}
}

// Set tests

func TestSetOps_BasicOperations(t *testing.T) {
	sm := store.NewShardedMap()
	setOps := datatype.NewSetOps(sm)

	// SAdd
	added := setOps.SAdd("set1", "a", "b", "c")
	if added != 3 {
		t.Errorf("expected 3 members added, got %d", added)
	}

	// Add duplicate
	added = setOps.SAdd("set1", "a", "d")
	if added != 1 {
		t.Errorf("expected 1 new member added, got %d", added)
	}

	// SIsMember
	if !setOps.SIsMember("set1", "a") {
		t.Error("a should be a member")
	}
	if setOps.SIsMember("set1", "x") {
		t.Error("x should not be a member")
	}

	// SCard
	if setOps.SCard("set1") != 4 {
		t.Errorf("expected 4 members, got %d", setOps.SCard("set1"))
	}

	// SRem
	removed := setOps.SRem("set1", "a", "b")
	if removed != 2 {
		t.Errorf("expected 2 members removed, got %d", removed)
	}
}

func TestSetOps_SMembers(t *testing.T) {
	sm := store.NewShardedMap()
	setOps := datatype.NewSetOps(sm)

	setOps.SAdd("set1", "a", "b", "c")

	members := setOps.SMembers("set1")
	if len(members) != 3 {
		t.Errorf("expected 3 members, got %d", len(members))
	}

	// Check all members are present
	memberSet := make(map[string]bool)
	for _, m := range members {
		memberSet[m] = true
	}
	if !memberSet["a"] || !memberSet["b"] || !memberSet["c"] {
		t.Error("missing members")
	}
}

func TestSetOps_SetOperations(t *testing.T) {
	sm := store.NewShardedMap()
	setOps := datatype.NewSetOps(sm)

	setOps.SAdd("set1", "a", "b", "c")
	setOps.SAdd("set2", "b", "c", "d")

	// Union
	union := setOps.SUnion("set1", "set2")
	if len(union) != 4 {
		t.Errorf("expected 4 in union, got %d", len(union))
	}

	// Intersection
	inter := setOps.SInter("set1", "set2")
	if len(inter) != 2 {
		t.Errorf("expected 2 in intersection, got %d", len(inter))
	}

	// Difference
	diff := setOps.SDiff("set1", "set2")
	if len(diff) != 1 {
		t.Errorf("expected 1 in difference, got %d", len(diff))
	}
}

func TestSetOps_SPop(t *testing.T) {
	sm := store.NewShardedMap()
	setOps := datatype.NewSetOps(sm)

	setOps.SAdd("set1", "a", "b", "c")

	popped, ok := setOps.SPop("set1")
	if !ok {
		t.Error("SPop should succeed")
	}
	if popped != "a" && popped != "b" && popped != "c" {
		t.Errorf("unexpected popped value: %s", popped)
	}

	if setOps.SCard("set1") != 2 {
		t.Error("expected 2 members after pop")
	}
}

// Concurrent tests for data types

func TestStringOps_ConcurrentIncr(t *testing.T) {
	sm := store.NewShardedMap()
	strOps := datatype.NewStringOps(sm)

	const numGoroutines = 100
	const numIncrements = 100

	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numIncrements; j++ {
				strOps.Incr("counter")
			}
		}()
	}

	wg.Wait()

	val, _ := strOps.Get("counter")
	expected := int64(numGoroutines * numIncrements)

	n, _ := parseInt64(val)
	if n != expected {
		t.Errorf("expected %d, got %d", expected, n)
	}
}

func parseInt64(b []byte) (int64, bool) {
	if len(b) == 0 {
		return 0, false
	}

	var neg bool
	var n int64
	i := 0

	if b[0] == '-' {
		neg = true
		i++
	} else if b[0] == '+' {
		i++
	}

	if i >= len(b) {
		return 0, false
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

func TestListOps_ConcurrentPushPop(t *testing.T) {
	sm := store.NewShardedMap()
	listOps := datatype.NewListOps(sm)

	const numGoroutines = 50
	const numOperations = 100

	var wg sync.WaitGroup

	// Pushers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				listOps.LPush("list1", []byte(fmt.Sprintf("value-%d-%d", id, j)))
			}
		}(i)
	}

	// Poppers
	for i := 0; i < numGoroutines/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				listOps.LPop("list1")
			}
		}()
	}

	wg.Wait()
}

func TestHashOps_ConcurrentAccess(t *testing.T) {
	sm := store.NewShardedMap()
	hashOps := datatype.NewHashOps(sm)

	const numGoroutines = 50
	const numOperations = 100

	var wg sync.WaitGroup

	// Writers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				field := fmt.Sprintf("field-%d", j%10)
				value := []byte(fmt.Sprintf("value-%d-%d", id, j))
				hashOps.HSet("hash1", []byte(field), value)
			}
		}(i)
	}

	// Readers
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				field := fmt.Sprintf("field-%d", j%10)
				hashOps.HGet("hash1", field)
			}
		}()
	}

	wg.Wait()
}

func TestSetOps_ConcurrentAccess(t *testing.T) {
	sm := store.NewShardedMap()
	setOps := datatype.NewSetOps(sm)

	const numGoroutines = 50
	const numOperations = 100

	var wg sync.WaitGroup

	// Adders
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				member := fmt.Sprintf("member-%d-%d", id, j)
				setOps.SAdd("set1", member)
			}
		}(i)
	}

	// Removers
	for i := 0; i < numGoroutines/2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations/2; j++ {
				member := fmt.Sprintf("member-%d-%d", id, j)
				setOps.SRem("set1", member)
			}
		}(i)
	}

	wg.Wait()
}
