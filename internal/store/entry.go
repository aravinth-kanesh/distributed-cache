package store

import (
	"sync/atomic"
	"time"
)

// EntryType represents the type of value stored in an entry
type EntryType uint8

const (
	TypeString EntryType = iota
	TypeList
	TypeHash
	TypeSet
	TypeSortedSet
)

// String returns the string representation of the entry type
func (t EntryType) String() string {
	switch t {
	case TypeString:
		return "string"
	case TypeList:
		return "list"
	case TypeHash:
		return "hash"
	case TypeSet:
		return "set"
	case TypeSortedSet:
		return "zset"
	default:
		return "unknown"
	}
}

// Entry represents a cache entry with metadata
// Core data structure stored in each shard
type Entry struct {
	// Type indicates what kind of value this entry holds
	Type EntryType

	// Value holds the actual data (type-specific)
	// For strings: []byte
	// For lists: *ListValue
	// For hashes: *HashValue
	// For sets: *SetValue
	Value interface{}

	// ExpiresAt is the Unix nanosecond timestamp when this entry expires
	// A value of 0 means no expiration
	ExpiresAt int64

	// CreatedAt is the Unix nanosecond timestamp when this entry was created
	CreatedAt int64

	// accessedAt is the Unix nanosecond timestamp of last access
	// Updated atomically for potential LRU eviction
	accessedAt atomic.Int64
}

// NewEntry creates a new entry with the given type and value
func NewEntry(entryType EntryType, value interface{}) *Entry {
	now := time.Now().UnixNano()
	e := &Entry{
		Type:      entryType,
		Value:     value,
		CreatedAt: now,
	}
	e.accessedAt.Store(now)
	return e
}

// NewEntryWithTTL creates a new entry with a TTL (time-to-live) in seconds
func NewEntryWithTTL(entryType EntryType, value interface{}, ttlSeconds int64) *Entry {
	e := NewEntry(entryType, value)
	if ttlSeconds > 0 {
		e.ExpiresAt = time.Now().Add(time.Duration(ttlSeconds) * time.Second).UnixNano()
	}
	return e
}

// NewEntryWithExpireAt creates a new entry that expires at a specific time
func NewEntryWithExpireAt(entryType EntryType, value interface{}, expireAt time.Time) *Entry {
	e := NewEntry(entryType, value)
	if !expireAt.IsZero() {
		e.ExpiresAt = expireAt.UnixNano()
	}
	return e
}

// IsExpired returns true if the entry has expired
// Lazy expiration check - called on access
func (e *Entry) IsExpired() bool {
	if e.ExpiresAt == 0 {
		return false
	}
	return time.Now().UnixNano() > e.ExpiresAt
}

// TTL returns the remaining time-to-live in seconds
// Returns -1 if no expiration is set, -2 if expired
func (e *Entry) TTL() int64 {
	if e.ExpiresAt == 0 {
		return -1
	}
	remaining := e.ExpiresAt - time.Now().UnixNano()
	if remaining <= 0 {
		return -2
	}
	return remaining / int64(time.Second)
}

// PTTL returns the remaining time-to-live in milliseconds
// Returns -1 if no expiration is set, -2 if expired
func (e *Entry) PTTL() int64 {
	if e.ExpiresAt == 0 {
		return -1
	}
	remaining := e.ExpiresAt - time.Now().UnixNano()
	if remaining <= 0 {
		return -2
	}
	return remaining / int64(time.Millisecond)
}

// Touch updates the access timestamp atomically
// Used for LRU tracking without requiring a lock
func (e *Entry) Touch() {
	e.accessedAt.Store(time.Now().UnixNano())
}

// LastAccessed returns the last access timestamp
func (e *Entry) LastAccessed() int64 {
	return e.accessedAt.Load()
}

// SetExpire sets the expiration time for this entry
func (e *Entry) SetExpire(expireAt time.Time) {
	if expireAt.IsZero() {
		e.ExpiresAt = 0
	} else {
		e.ExpiresAt = expireAt.UnixNano()
	}
}

// SetTTL sets the TTL in seconds (0 removes expiration)
func (e *Entry) SetTTL(seconds int64) {
	if seconds <= 0 {
		e.ExpiresAt = 0
	} else {
		e.ExpiresAt = time.Now().Add(time.Duration(seconds) * time.Second).UnixNano()
	}
}

// Persist removes the expiration from this entry
func (e *Entry) Persist() bool {
	if e.ExpiresAt == 0 {
		return false
	}
	e.ExpiresAt = 0
	return true
}

// AsString returns the value as a byte slice if it's a string type
func (e *Entry) AsString() ([]byte, bool) {
	if e.Type != TypeString {
		return nil, false
	}
	v, ok := e.Value.([]byte)
	return v, ok
}

// AsInt64 attempts to parse the string value as an int64
func (e *Entry) AsInt64() (int64, bool) {
	if e.Type != TypeString {
		return 0, false
	}
	v, ok := e.Value.([]byte)
	if !ok {
		return 0, false
	}
	return parseInt64(v)
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
	if n == 0 {
		return []byte{'0'}
	}

	var neg bool
	if n < 0 {
		neg = true
		n = -n
	}

	// Max int64 is 19 digits, plus sign
	buf := make([]byte, 0, 20)

	for n > 0 {
		buf = append(buf, byte('0'+n%10))
		n /= 10
	}

	if neg {
		buf = append(buf, '-')
	}

	// Reverse
	for i, j := 0, len(buf)-1; i < j; i, j = i+1, j-1 {
		buf[i], buf[j] = buf[j], buf[i]
	}

	return buf
}
