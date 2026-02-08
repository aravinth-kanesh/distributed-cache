package datatype

import (
	"strconv"
	"sync"

	"github.com/aravinth/distributed-cache/internal/store"
)

// HashValue represents a hash table
// Each hash has its own mutex for fine-grained locking
type HashValue struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewHashValue creates a new empty hash
func NewHashValue() *HashValue {
	return &HashValue{
		data: make(map[string][]byte),
	}
}

// Len returns the number of fields in the hash
func (h *HashValue) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.data)
}

// DataSnapshot returns a copy of the hash data under RLock.
// I use this for persistence — copying under lock then encoding outside.
func (h *HashValue) DataSnapshot() map[string][]byte {
	h.mu.RLock()
	defer h.mu.RUnlock()
	result := make(map[string][]byte, len(h.data))
	for k, v := range h.data {
		result[k] = v
	}
	return result
}

// HashOps provides hash operations on the store
type HashOps struct {
	store *store.ShardedMap
}

// NewHashOps creates a new HashOps instance
func NewHashOps(s *store.ShardedMap) *HashOps {
	return &HashOps{store: s}
}

// getOrCreateHash gets an existing hash or creates a new one
func (h *HashOps) getOrCreateHash(key string) (*HashValue, bool) {
	entry, exists := h.store.Get(key)

	if !exists {
		hash := NewHashValue()
		newEntry := store.NewEntry(store.TypeHash, hash)
		h.store.Set(key, newEntry)
		return hash, true
	}

	if entry.Type != store.TypeHash {
		return nil, false
	}

	return entry.Value.(*HashValue), true
}

// getHash gets an existing hash
func (h *HashOps) getHash(key string) (*HashValue, bool) {
	entry, exists := h.store.Get(key)
	if !exists {
		return nil, false
	}

	if entry.Type != store.TypeHash {
		return nil, false
	}

	return entry.Value.(*HashValue), true
}

// HSet sets fields in the hash
// Returns the number of new fields added
func (h *HashOps) HSet(key string, fieldValues ...[]byte) int {
	if len(fieldValues)%2 != 0 {
		return -1 // Invalid: must have pairs
	}

	hash, ok := h.getOrCreateHash(key)
	if !ok {
		return -1
	}

	hash.mu.Lock()
	defer hash.mu.Unlock()

	var added int
	for i := 0; i < len(fieldValues); i += 2 {
		field := string(fieldValues[i])
		value := fieldValues[i+1]

		if _, exists := hash.data[field]; !exists {
			added++
		}

		copied := make([]byte, len(value))
		copy(copied, value)
		hash.data[field] = copied
	}

	return added
}

// HGet gets a field value from the hash
func (h *HashOps) HGet(key string, field string) ([]byte, bool) {
	hash, ok := h.getHash(key)
	if !ok {
		return nil, false
	}

	hash.mu.RLock()
	defer hash.mu.RUnlock()

	val, exists := hash.data[field]
	return val, exists
}

// HGetAll gets all field-value pairs from the hash
func (h *HashOps) HGetAll(key string) map[string][]byte {
	hash, ok := h.getHash(key)
	if !ok {
		return map[string][]byte{}
	}

	hash.mu.RLock()
	defer hash.mu.RUnlock()

	result := make(map[string][]byte, len(hash.data))
	for k, v := range hash.data {
		result[k] = v
	}

	return result
}

// HDel deletes fields from the hash
// Returns the number of fields deleted
func (h *HashOps) HDel(key string, fields ...string) int {
	hash, ok := h.getHash(key)
	if !ok {
		return 0
	}

	hash.mu.Lock()
	defer hash.mu.Unlock()

	var deleted int
	for _, field := range fields {
		if _, exists := hash.data[field]; exists {
			delete(hash.data, field)
			deleted++
		}
	}

	// Delete hash if empty
	if len(hash.data) == 0 {
		h.store.Delete(key)
	}

	return deleted
}

// HExists checks if a field exists in the hash
func (h *HashOps) HExists(key string, field string) bool {
	hash, ok := h.getHash(key)
	if !ok {
		return false
	}

	hash.mu.RLock()
	defer hash.mu.RUnlock()

	_, exists := hash.data[field]
	return exists
}

// HLen returns the number of fields in the hash
func (h *HashOps) HLen(key string) int {
	hash, ok := h.getHash(key)
	if !ok {
		entry, exists := h.store.Get(key)
		if exists && entry.Type != store.TypeHash {
			return -1 // Wrong type
		}
		return 0
	}

	return hash.Len()
}

// HKeys returns all field names in the hash
func (h *HashOps) HKeys(key string) []string {
	hash, ok := h.getHash(key)
	if !ok {
		return []string{}
	}

	hash.mu.RLock()
	defer hash.mu.RUnlock()

	keys := make([]string, 0, len(hash.data))
	for k := range hash.data {
		keys = append(keys, k)
	}

	return keys
}

// HVals returns all values in the hash
func (h *HashOps) HVals(key string) [][]byte {
	hash, ok := h.getHash(key)
	if !ok {
		return [][]byte{}
	}

	hash.mu.RLock()
	defer hash.mu.RUnlock()

	vals := make([][]byte, 0, len(hash.data))
	for _, v := range hash.data {
		vals = append(vals, v)
	}

	return vals
}

// HMGet gets multiple field values from the hash
func (h *HashOps) HMGet(key string, fields ...string) [][]byte {
	result := make([][]byte, len(fields))

	hash, ok := h.getHash(key)
	if !ok {
		return result // All nil
	}

	hash.mu.RLock()
	defer hash.mu.RUnlock()

	for i, field := range fields {
		if val, exists := hash.data[field]; exists {
			result[i] = val
		}
	}

	return result
}

// HMSet sets multiple fields (deprecated, use HSet)
func (h *HashOps) HMSet(key string, fieldValues map[string][]byte) {
	hash, ok := h.getOrCreateHash(key)
	if !ok {
		return
	}

	hash.mu.Lock()
	defer hash.mu.Unlock()

	for field, value := range fieldValues {
		copied := make([]byte, len(value))
		copy(copied, value)
		hash.data[field] = copied
	}
}

// HSetNX sets a field only if it doesn't exist
// Returns true if the field was set
func (h *HashOps) HSetNX(key string, field string, value []byte) bool {
	hash, ok := h.getOrCreateHash(key)
	if !ok {
		return false
	}

	hash.mu.Lock()
	defer hash.mu.Unlock()

	if _, exists := hash.data[field]; exists {
		return false
	}

	copied := make([]byte, len(value))
	copy(copied, value)
	hash.data[field] = copied

	return true
}

// HIncrBy increments the integer value of a field
func (h *HashOps) HIncrBy(key string, field string, delta int64) (int64, bool) {
	hash, ok := h.getOrCreateHash(key)
	if !ok {
		return 0, false
	}

	hash.mu.Lock()
	defer hash.mu.Unlock()

	var current int64
	if val, exists := hash.data[field]; exists {
		parsed, ok := parseInt64(val)
		if !ok {
			return 0, false
		}
		current = parsed
	}

	newVal := current + delta
	hash.data[field] = []byte(strconv.FormatInt(newVal, 10))

	return newVal, true
}

// HIncrByFloat increments the float value of a field
func (h *HashOps) HIncrByFloat(key string, field string, delta float64) (float64, bool) {
	hash, ok := h.getOrCreateHash(key)
	if !ok {
		return 0, false
	}

	hash.mu.Lock()
	defer hash.mu.Unlock()

	var current float64
	if val, exists := hash.data[field]; exists {
		parsed, err := strconv.ParseFloat(string(val), 64)
		if err != nil {
			return 0, false
		}
		current = parsed
	}

	newVal := current + delta
	hash.data[field] = []byte(strconv.FormatFloat(newVal, 'f', -1, 64))

	return newVal, true
}

// HStrLen returns the string length of a field value
func (h *HashOps) HStrLen(key string, field string) int {
	hash, ok := h.getHash(key)
	if !ok {
		return 0
	}

	hash.mu.RLock()
	defer hash.mu.RUnlock()

	val, exists := hash.data[field]
	if !exists {
		return 0
	}

	return len(val)
}

// HScan iterates over fields matching a pattern
// Returns cursor and field-value pairs
func (h *HashOps) HScan(key string, cursor int, match string, count int) (int, []string) {
	hash, ok := h.getHash(key)
	if !ok {
		return 0, []string{}
	}

	hash.mu.RLock()
	defer hash.mu.RUnlock()

	// Simple implementation: return all fields
	// In production, implement proper cursor-based iteration
	result := make([]string, 0, len(hash.data)*2)
	for k, v := range hash.data {
		if match == "" || matchPattern(match, k) {
			result = append(result, k, string(v))
		}
	}

	return 0, result // Return cursor 0 to indicate completion
}

// HRandField returns random fields from the hash
func (h *HashOps) HRandField(key string, count int, withValues bool) [][]byte {
	hash, ok := h.getHash(key)
	if !ok {
		return [][]byte{}
	}

	hash.mu.RLock()
	defer hash.mu.RUnlock()

	if len(hash.data) == 0 {
		return [][]byte{}
	}

	// Collect all keys
	keys := make([]string, 0, len(hash.data))
	for k := range hash.data {
		keys = append(keys, k)
	}

	// Simple random selection (for proper randomness, use math/rand)
	absCount := count
	if absCount < 0 {
		absCount = -absCount
	}
	if absCount > len(keys) {
		absCount = len(keys)
	}

	result := make([][]byte, 0, absCount*(1+btoi(withValues)))
	for i := 0; i < absCount; i++ {
		idx := i % len(keys) // Simple selection
		result = append(result, []byte(keys[idx]))
		if withValues {
			result = append(result, hash.data[keys[idx]])
		}
	}

	return result
}

// btoi converts bool to int
func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// matchPattern matches a simple glob pattern
// Supports * (match any) and ? (match single char)
func matchPattern(pattern, s string) bool {
	if pattern == "*" {
		return true
	}

	pi, si := 0, 0
	starIdx, matchIdx := -1, 0

	for si < len(s) {
		if pi < len(pattern) && (pattern[pi] == '?' || pattern[pi] == s[si]) {
			pi++
			si++
		} else if pi < len(pattern) && pattern[pi] == '*' {
			starIdx = pi
			matchIdx = si
			pi++
		} else if starIdx != -1 {
			pi = starIdx + 1
			matchIdx++
			si = matchIdx
		} else {
			return false
		}
	}

	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}

	return pi == len(pattern)
}
