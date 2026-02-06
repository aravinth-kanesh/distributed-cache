package datatype

import (
	"strconv"
	"sync/atomic"

	"github.com/aravinth/distributed-cache/internal/store"
)

// StringOps provides string operations on the store
type StringOps struct {
	store *store.ShardedMap
}

// NewStringOps creates a new StringOps instance
func NewStringOps(s *store.ShardedMap) *StringOps {
	return &StringOps{store: s}
}

// Get retrieves the string value for a key
// Returns nil if the key doesn't exist or is not a string
func (s *StringOps) Get(key string) ([]byte, bool) {
	entry, exists := s.store.Get(key)
	if !exists {
		return nil, false
	}

	if entry.Type != store.TypeString {
		return nil, false
	}

	val, ok := entry.Value.([]byte)
	return val, ok
}

// Set stores a string value
func (s *StringOps) Set(key string, value []byte) {
	// Copy the value to prevent aliasing issues
	copied := make([]byte, len(value))
	copy(copied, value)

	entry := store.NewEntry(store.TypeString, copied)
	s.store.Set(key, entry)
}

// SetWithTTL stores a string value with a TTL in seconds
func (s *StringOps) SetWithTTL(key string, value []byte, ttlSeconds int64) {
	copied := make([]byte, len(value))
	copy(copied, value)

	entry := store.NewEntryWithTTL(store.TypeString, copied, ttlSeconds)
	s.store.Set(key, entry)
}

// SetNX sets the key only if it doesn't exist
// Returns true if the key was set
func (s *StringOps) SetNX(key string, value []byte) bool {
	copied := make([]byte, len(value))
	copy(copied, value)

	entry := store.NewEntry(store.TypeString, copied)
	return s.store.SetNX(key, entry)
}

// SetXX sets the key only if it already exists
// Returns true if the key was set
func (s *StringOps) SetXX(key string, value []byte) bool {
	copied := make([]byte, len(value))
	copy(copied, value)

	entry := store.NewEntry(store.TypeString, copied)
	return s.store.SetXX(key, entry)
}

// GetSet atomically sets a new value and returns the old value
func (s *StringOps) GetSet(key string, value []byte) ([]byte, bool) {
	copied := make([]byte, len(value))
	copy(copied, value)

	entry := store.NewEntry(store.TypeString, copied)
	old, existed := s.store.GetSet(key, entry)

	if !existed {
		return nil, false
	}

	if old.Type != store.TypeString {
		return nil, false
	}

	val, ok := old.Value.([]byte)
	return val, ok
}

// MGet retrieves multiple string values
// Returns a slice of values (nil for non-existent or non-string keys)
func (s *StringOps) MGet(keys ...string) [][]byte {
	result := make([][]byte, len(keys))

	for i, key := range keys {
		val, ok := s.Get(key)
		if ok {
			result[i] = val
		}
	}

	return result
}

// MSet sets multiple key-value pairs
func (s *StringOps) MSet(pairs map[string][]byte) {
	for key, value := range pairs {
		s.Set(key, value)
	}
}

// MSetNX sets multiple key-value pairs only if none of the keys exist
// Returns true if all keys were set, false if any key already existed
func (s *StringOps) MSetNX(pairs map[string][]byte) bool {
	// First check if any key exists
	for key := range pairs {
		if s.store.Exists(key) {
			return false
		}
	}

	// Set all keys
	for key, value := range pairs {
		s.Set(key, value)
	}

	return true
}

// Append appends a value to an existing string
// Creates the key with the value if it doesn't exist
// Returns the length of the string after appending
func (s *StringOps) Append(key string, value []byte) int {
	entry, exists := s.store.Get(key)

	if !exists {
		s.Set(key, value)
		return len(value)
	}

	if entry.Type != store.TypeString {
		return -1 // Wrong type
	}

	existing, _ := entry.Value.([]byte)
	newValue := make([]byte, len(existing)+len(value))
	copy(newValue, existing)
	copy(newValue[len(existing):], value)

	entry.Value = newValue
	s.store.Set(key, entry)

	return len(newValue)
}

// StrLen returns the length of the string value
// Returns 0 if the key doesn't exist, -1 if it's not a string
func (s *StringOps) StrLen(key string) int {
	val, ok := s.Get(key)
	if !ok {
		entry, exists := s.store.Get(key)
		if exists && entry.Type != store.TypeString {
			return -1
		}
		return 0
	}
	return len(val)
}

// GetRange returns a substring of the string value
// Supports negative indices (counting from end)
func (s *StringOps) GetRange(key string, start, end int) []byte {
	val, ok := s.Get(key)
	if !ok {
		return []byte{}
	}

	length := len(val)

	// Handle negative indices
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}

	// Clamp indices
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if start > end || start >= length {
		return []byte{}
	}

	return val[start : end+1]
}

// SetRange overwrites part of the string value
// Pads with zero bytes if offset is beyond the string length
func (s *StringOps) SetRange(key string, offset int, value []byte) int {
	existing, _ := s.Get(key)

	// Calculate new length
	newLen := offset + len(value)
	if len(existing) > newLen {
		newLen = len(existing)
	}

	// Create new buffer
	newValue := make([]byte, newLen)
	copy(newValue, existing)
	copy(newValue[offset:], value)

	s.Set(key, newValue)
	return len(newValue)
}

// Numeric operations

// Incr increments the integer value by 1
// Returns the new value and an error if the value is not an integer
func (s *StringOps) Incr(key string) (int64, bool) {
	return s.IncrBy(key, 1)
}

// IncrBy increments the integer value by the given amount
// This operation is atomic - uses GetOrSet followed by atomic Update
func (s *StringOps) IncrBy(key string, delta int64) (int64, bool) {
	// First, try to get or create the key atomically
	initialEntry := store.NewEntry(store.TypeString, formatInt64(delta))
	existingEntry, existed := s.store.GetOrSet(key, initialEntry)

	if !existed {
		// Key didn't exist, we created it with delta value
		return delta, true
	}

	// Key exists - need to update atomically
	if existingEntry.Type != store.TypeString {
		return 0, false
	}

	var result int64
	var success bool

	// Use Update for atomic read-modify-write
	s.store.Update(key, func(entry *store.Entry) *store.Entry {
		val, ok := entry.Value.([]byte)
		if !ok {
			success = false
			return entry
		}

		current, ok := parseInt64(val)
		if !ok {
			success = false
			return entry
		}

		result = current + delta
		entry.Value = formatInt64(result)
		success = true
		return entry
	})

	return result, success
}

// Decr decrements the integer value by 1
func (s *StringOps) Decr(key string) (int64, bool) {
	return s.IncrBy(key, -1)
}

// DecrBy decrements the integer value by the given amount
func (s *StringOps) DecrBy(key string, delta int64) (int64, bool) {
	return s.IncrBy(key, -delta)
}

// IncrByFloat increments the float value by the given amount
func (s *StringOps) IncrByFloat(key string, delta float64) (float64, bool) {
	entry, exists := s.store.Get(key)

	if !exists {
		s.Set(key, []byte(strconv.FormatFloat(delta, 'f', -1, 64)))
		return delta, true
	}

	if entry.Type != store.TypeString {
		return 0, false
	}

	val, ok := entry.Value.([]byte)
	if !ok {
		return 0, false
	}

	current, err := strconv.ParseFloat(string(val), 64)
	if err != nil {
		return 0, false
	}

	newVal := current + delta
	entry.Value = []byte(strconv.FormatFloat(newVal, 'f', -1, 64))
	s.store.Set(key, entry)

	return newVal, true
}

// parseInt64 parses a byte slice as an int64
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

// formatInt64 formats an int64 as a byte slice
func formatInt64(n int64) []byte {
	return []byte(strconv.FormatInt(n, 10))
}

// AtomicString is a lock-free string value using atomic pointer swap
// Used for high-performance string updates where atomicity is critical
type AtomicString struct {
	ptr atomic.Pointer[[]byte]
}

// NewAtomicString creates a new AtomicString
func NewAtomicString(value []byte) *AtomicString {
	as := &AtomicString{}
	as.Set(value)
	return as
}

// Set atomically sets the value
func (as *AtomicString) Set(value []byte) {
	copied := make([]byte, len(value))
	copy(copied, value)
	as.ptr.Store(&copied)
}

// Get atomically gets the value
func (as *AtomicString) Get() []byte {
	ptr := as.ptr.Load()
	if ptr == nil {
		return nil
	}
	return *ptr
}

// Swap atomically swaps and returns the old value
func (as *AtomicString) Swap(value []byte) []byte {
	copied := make([]byte, len(value))
	copy(copied, value)
	old := as.ptr.Swap(&copied)
	if old == nil {
		return nil
	}
	return *old
}

// CompareAndSwap atomically compares and swaps the value
func (as *AtomicString) CompareAndSwap(expected, value []byte) bool {
	copied := make([]byte, len(value))
	copy(copied, value)

	current := as.ptr.Load()
	if current == nil {
		if expected == nil {
			return as.ptr.CompareAndSwap(nil, &copied)
		}
		return false
	}

	if !bytesEqual(*current, expected) {
		return false
	}

	return as.ptr.CompareAndSwap(current, &copied)
}

// bytesEqual compares two byte slices
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
