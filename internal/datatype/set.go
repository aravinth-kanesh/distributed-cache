package datatype

import (
	"sync"

	"github.com/aravinth/distributed-cache/internal/store"
)

// SetValue represents a set (unique collection of strings)
// Uses map[string]struct{} for memory efficiency (empty struct takes 0 bytes)
// Each set has its own mutex for fine-grained locking
type SetValue struct {
	mu   sync.RWMutex
	data map[string]struct{}
}

// NewSetValue creates a new empty set
func NewSetValue() *SetValue {
	return &SetValue{
		data: make(map[string]struct{}),
	}
}

// Len returns the number of members in the set
func (s *SetValue) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// MembersSnapshot returns a copy of the set members under RLock.
// I use this for persistence — copying under lock then encoding outside.
func (s *SetValue) MembersSnapshot() map[string]struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string]struct{}, len(s.data))
	for k := range s.data {
		result[k] = struct{}{}
	}
	return result
}

// SetOps provides set operations on the store
type SetOps struct {
	store *store.ShardedMap
}

// NewSetOps creates a new SetOps instance
func NewSetOps(s *store.ShardedMap) *SetOps {
	return &SetOps{store: s}
}

// getOrCreateSet gets an existing set or creates a new one
func (s *SetOps) getOrCreateSet(key string) (*SetValue, bool) {
	entry, exists := s.store.Get(key)

	if !exists {
		set := NewSetValue()
		newEntry := store.NewEntry(store.TypeSet, set)
		s.store.Set(key, newEntry)
		return set, true
	}

	if entry.Type != store.TypeSet {
		return nil, false
	}

	return entry.Value.(*SetValue), true
}

// getSet gets an existing set
func (s *SetOps) getSet(key string) (*SetValue, bool) {
	entry, exists := s.store.Get(key)
	if !exists {
		return nil, false
	}

	if entry.Type != store.TypeSet {
		return nil, false
	}

	return entry.Value.(*SetValue), true
}

// SAdd adds members to the set
// Returns the number of members added (not including already existing)
func (s *SetOps) SAdd(key string, members ...string) int {
	set, ok := s.getOrCreateSet(key)
	if !ok {
		return -1 // Wrong type
	}

	set.mu.Lock()
	defer set.mu.Unlock()

	var added int
	for _, member := range members {
		if _, exists := set.data[member]; !exists {
			set.data[member] = struct{}{}
			added++
		}
	}

	return added
}

// SRem removes members from the set
// Returns the number of members removed
func (s *SetOps) SRem(key string, members ...string) int {
	set, ok := s.getSet(key)
	if !ok {
		return 0
	}

	set.mu.Lock()
	defer set.mu.Unlock()

	var removed int
	for _, member := range members {
		if _, exists := set.data[member]; exists {
			delete(set.data, member)
			removed++
		}
	}

	// Delete set if empty
	if len(set.data) == 0 {
		s.store.Delete(key)
	}

	return removed
}

// SIsMember checks if a member exists in the set
func (s *SetOps) SIsMember(key string, member string) bool {
	set, ok := s.getSet(key)
	if !ok {
		return false
	}

	set.mu.RLock()
	defer set.mu.RUnlock()

	_, exists := set.data[member]
	return exists
}

// SMIsMember checks if multiple members exist in the set
// Returns a slice of booleans
func (s *SetOps) SMIsMember(key string, members ...string) []bool {
	result := make([]bool, len(members))

	set, ok := s.getSet(key)
	if !ok {
		return result // All false
	}

	set.mu.RLock()
	defer set.mu.RUnlock()

	for i, member := range members {
		_, result[i] = set.data[member]
	}

	return result
}

// SMembers returns all members of the set
func (s *SetOps) SMembers(key string) []string {
	set, ok := s.getSet(key)
	if !ok {
		return []string{}
	}

	set.mu.RLock()
	defer set.mu.RUnlock()

	members := make([]string, 0, len(set.data))
	for member := range set.data {
		members = append(members, member)
	}

	return members
}

// SCard returns the number of members in the set
func (s *SetOps) SCard(key string) int {
	set, ok := s.getSet(key)
	if !ok {
		entry, exists := s.store.Get(key)
		if exists && entry.Type != store.TypeSet {
			return -1 // Wrong type
		}
		return 0
	}

	return set.Len()
}

// SPop removes and returns a random member from the set
func (s *SetOps) SPop(key string) (string, bool) {
	set, ok := s.getSet(key)
	if !ok {
		return "", false
	}

	set.mu.Lock()
	defer set.mu.Unlock()

	if len(set.data) == 0 {
		return "", false
	}

	// Get any member (Go map iteration is random)
	var member string
	for m := range set.data {
		member = m
		break
	}

	delete(set.data, member)

	// Delete set if empty
	if len(set.data) == 0 {
		s.store.Delete(key)
	}

	return member, true
}

// SPopN removes and returns up to count random members
func (s *SetOps) SPopN(key string, count int) []string {
	set, ok := s.getSet(key)
	if !ok {
		return []string{}
	}

	set.mu.Lock()
	defer set.mu.Unlock()

	if len(set.data) == 0 || count <= 0 {
		return []string{}
	}

	if count > len(set.data) {
		count = len(set.data)
	}

	result := make([]string, 0, count)
	for member := range set.data {
		if len(result) >= count {
			break
		}
		result = append(result, member)
		delete(set.data, member)
	}

	// Delete set if empty
	if len(set.data) == 0 {
		s.store.Delete(key)
	}

	return result
}

// SRandMember returns random members from the set without removing them
func (s *SetOps) SRandMember(key string, count int) []string {
	set, ok := s.getSet(key)
	if !ok {
		return []string{}
	}

	set.mu.RLock()
	defer set.mu.RUnlock()

	if len(set.data) == 0 {
		return []string{}
	}

	allowDuplicates := count < 0
	if count < 0 {
		count = -count
	}

	result := make([]string, 0, count)

	if allowDuplicates {
		// Can return same member multiple times
		members := make([]string, 0, len(set.data))
		for m := range set.data {
			members = append(members, m)
		}
		for i := 0; i < count; i++ {
			// Simple selection (for proper randomness, use math/rand)
			result = append(result, members[i%len(members)])
		}
	} else {
		// Return unique members only
		if count > len(set.data) {
			count = len(set.data)
		}
		for member := range set.data {
			if len(result) >= count {
				break
			}
			result = append(result, member)
		}
	}

	return result
}

// SMove moves a member from one set to another
func (s *SetOps) SMove(src, dst, member string) bool {
	srcSet, ok := s.getSet(src)
	if !ok {
		return false
	}

	srcSet.mu.Lock()
	if _, exists := srcSet.data[member]; !exists {
		srcSet.mu.Unlock()
		return false
	}

	delete(srcSet.data, member)

	// Delete source set if empty
	if len(srcSet.data) == 0 {
		srcSet.mu.Unlock()
		s.store.Delete(src)
	} else {
		srcSet.mu.Unlock()
	}

	// Add to destination
	s.SAdd(dst, member)

	return true
}

// Set operations

// SUnion returns the union of multiple sets
func (s *SetOps) SUnion(keys ...string) []string {
	result := make(map[string]struct{})

	for _, key := range keys {
		set, ok := s.getSet(key)
		if !ok {
			continue
		}

		set.mu.RLock()
		for member := range set.data {
			result[member] = struct{}{}
		}
		set.mu.RUnlock()
	}

	members := make([]string, 0, len(result))
	for member := range result {
		members = append(members, member)
	}

	return members
}

// SUnionStore stores the union of multiple sets in a destination key
func (s *SetOps) SUnionStore(dst string, keys ...string) int {
	members := s.SUnion(keys...)

	if len(members) == 0 {
		s.store.Delete(dst)
		return 0
	}

	// Create new set
	set := NewSetValue()
	for _, member := range members {
		set.data[member] = struct{}{}
	}

	entry := store.NewEntry(store.TypeSet, set)
	s.store.Set(dst, entry)

	return len(members)
}

// SInter returns the intersection of multiple sets
func (s *SetOps) SInter(keys ...string) []string {
	if len(keys) == 0 {
		return []string{}
	}

	// Start with first set
	firstSet, ok := s.getSet(keys[0])
	if !ok {
		return []string{}
	}

	firstSet.mu.RLock()
	result := make(map[string]struct{}, len(firstSet.data))
	for member := range firstSet.data {
		result[member] = struct{}{}
	}
	firstSet.mu.RUnlock()

	// Intersect with remaining sets
	for _, key := range keys[1:] {
		set, ok := s.getSet(key)
		if !ok {
			return []string{} // Intersection with empty set is empty
		}

		set.mu.RLock()
		for member := range result {
			if _, exists := set.data[member]; !exists {
				delete(result, member)
			}
		}
		set.mu.RUnlock()

		if len(result) == 0 {
			return []string{}
		}
	}

	members := make([]string, 0, len(result))
	for member := range result {
		members = append(members, member)
	}

	return members
}

// SInterStore stores the intersection of multiple sets
func (s *SetOps) SInterStore(dst string, keys ...string) int {
	members := s.SInter(keys...)

	if len(members) == 0 {
		s.store.Delete(dst)
		return 0
	}

	set := NewSetValue()
	for _, member := range members {
		set.data[member] = struct{}{}
	}

	entry := store.NewEntry(store.TypeSet, set)
	s.store.Set(dst, entry)

	return len(members)
}

// SInterCard returns the cardinality of the intersection
func (s *SetOps) SInterCard(limit int, keys ...string) int {
	if len(keys) == 0 {
		return 0
	}

	// Start with first set
	firstSet, ok := s.getSet(keys[0])
	if !ok {
		return 0
	}

	firstSet.mu.RLock()
	candidates := make([]string, 0, len(firstSet.data))
	for member := range firstSet.data {
		candidates = append(candidates, member)
	}
	firstSet.mu.RUnlock()

	count := 0
	for _, member := range candidates {
		inAll := true
		for _, key := range keys[1:] {
			if !s.SIsMember(key, member) {
				inAll = false
				break
			}
		}
		if inAll {
			count++
			if limit > 0 && count >= limit {
				return count
			}
		}
	}

	return count
}

// SDiff returns the difference between the first set and other sets
func (s *SetOps) SDiff(keys ...string) []string {
	if len(keys) == 0 {
		return []string{}
	}

	firstSet, ok := s.getSet(keys[0])
	if !ok {
		return []string{}
	}

	firstSet.mu.RLock()
	result := make(map[string]struct{}, len(firstSet.data))
	for member := range firstSet.data {
		result[member] = struct{}{}
	}
	firstSet.mu.RUnlock()

	// Remove members that exist in other sets
	for _, key := range keys[1:] {
		set, ok := s.getSet(key)
		if !ok {
			continue
		}

		set.mu.RLock()
		for member := range set.data {
			delete(result, member)
		}
		set.mu.RUnlock()
	}

	members := make([]string, 0, len(result))
	for member := range result {
		members = append(members, member)
	}

	return members
}

// SDiffStore stores the difference in a destination key
func (s *SetOps) SDiffStore(dst string, keys ...string) int {
	members := s.SDiff(keys...)

	if len(members) == 0 {
		s.store.Delete(dst)
		return 0
	}

	set := NewSetValue()
	for _, member := range members {
		set.data[member] = struct{}{}
	}

	entry := store.NewEntry(store.TypeSet, set)
	s.store.Set(dst, entry)

	return len(members)
}

// SScan iterates over set members matching a pattern
func (s *SetOps) SScan(key string, cursor int, match string, count int) (int, []string) {
	set, ok := s.getSet(key)
	if !ok {
		return 0, []string{}
	}

	set.mu.RLock()
	defer set.mu.RUnlock()

	// Simple implementation: return all matching members
	result := make([]string, 0, len(set.data))
	for member := range set.data {
		if match == "" || matchPattern(match, member) {
			result = append(result, member)
		}
	}

	return 0, result // Return cursor 0 to indicate completion
}
