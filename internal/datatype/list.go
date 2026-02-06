package datatype

import (
	"sync"
	"sync/atomic"

	"github.com/aravinth/distributed-cache/internal/store"
)

// ListNode represents a node in a doubly-linked list
type ListNode struct {
	data []byte
	prev *ListNode
	next *ListNode
}

// ListValue represents a doubly-linked list
// Each list has its own mutex for fine-grained locking
type ListValue struct {
	mu   sync.RWMutex
	head *ListNode
	tail *ListNode
	len  atomic.Int64 // Atomic for lock-free length checks
}

// NewListValue creates a new empty list
func NewListValue() *ListValue {
	return &ListValue{}
}

// Len returns the length of the list (lock-free)
func (l *ListValue) Len() int64 {
	return l.len.Load()
}

// ListOps provides list operations on the store
type ListOps struct {
	store *store.ShardedMap
}

// NewListOps creates a new ListOps instance
func NewListOps(s *store.ShardedMap) *ListOps {
	return &ListOps{store: s}
}

// getOrCreateList gets an existing list or creates a new one
func (l *ListOps) getOrCreateList(key string) (*ListValue, bool) {
	entry, exists := l.store.Get(key)

	if !exists {
		// Create new list
		list := NewListValue()
		newEntry := store.NewEntry(store.TypeList, list)
		l.store.Set(key, newEntry)
		return list, true
	}

	if entry.Type != store.TypeList {
		return nil, false
	}

	return entry.Value.(*ListValue), true
}

// getList gets an existing list
func (l *ListOps) getList(key string) (*ListValue, bool) {
	entry, exists := l.store.Get(key)
	if !exists {
		return nil, false
	}

	if entry.Type != store.TypeList {
		return nil, false
	}

	return entry.Value.(*ListValue), true
}

// LPush inserts values at the head of the list
// Returns the length of the list after the push
func (l *ListOps) LPush(key string, values ...[]byte) int64 {
	list, ok := l.getOrCreateList(key)
	if !ok {
		return -1 // Wrong type
	}

	list.mu.Lock()
	defer list.mu.Unlock()

	for _, value := range values {
		copied := make([]byte, len(value))
		copy(copied, value)

		node := &ListNode{data: copied}

		if list.head == nil {
			list.head = node
			list.tail = node
		} else {
			node.next = list.head
			list.head.prev = node
			list.head = node
		}

		list.len.Add(1)
	}

	return list.len.Load()
}

// RPush inserts values at the tail of the list
// Returns the length of the list after the push
func (l *ListOps) RPush(key string, values ...[]byte) int64 {
	list, ok := l.getOrCreateList(key)
	if !ok {
		return -1
	}

	list.mu.Lock()
	defer list.mu.Unlock()

	for _, value := range values {
		copied := make([]byte, len(value))
		copy(copied, value)

		node := &ListNode{data: copied}

		if list.tail == nil {
			list.head = node
			list.tail = node
		} else {
			node.prev = list.tail
			list.tail.next = node
			list.tail = node
		}

		list.len.Add(1)
	}

	return list.len.Load()
}

// LPop removes and returns the first element of the list
func (l *ListOps) LPop(key string) ([]byte, bool) {
	list, ok := l.getList(key)
	if !ok {
		return nil, false
	}

	list.mu.Lock()
	defer list.mu.Unlock()

	if list.head == nil {
		return nil, false
	}

	node := list.head
	list.head = node.next

	if list.head == nil {
		list.tail = nil
	} else {
		list.head.prev = nil
	}

	list.len.Add(-1)

	// Delete list if empty
	if list.len.Load() == 0 {
		l.store.Delete(key)
	}

	return node.data, true
}

// RPop removes and returns the last element of the list
func (l *ListOps) RPop(key string) ([]byte, bool) {
	list, ok := l.getList(key)
	if !ok {
		return nil, false
	}

	list.mu.Lock()
	defer list.mu.Unlock()

	if list.tail == nil {
		return nil, false
	}

	node := list.tail
	list.tail = node.prev

	if list.tail == nil {
		list.head = nil
	} else {
		list.tail.next = nil
	}

	list.len.Add(-1)

	// Delete list if empty
	if list.len.Load() == 0 {
		l.store.Delete(key)
	}

	return node.data, true
}

// LLen returns the length of the list
func (l *ListOps) LLen(key string) int64 {
	list, ok := l.getList(key)
	if !ok {
		entry, exists := l.store.Get(key)
		if exists && entry.Type != store.TypeList {
			return -1 // Wrong type
		}
		return 0
	}

	return list.Len()
}

// LIndex returns the element at the specified index
// Supports negative indices (counting from end)
func (l *ListOps) LIndex(key string, index int64) ([]byte, bool) {
	list, ok := l.getList(key)
	if !ok {
		return nil, false
	}

	list.mu.RLock()
	defer list.mu.RUnlock()

	length := list.len.Load()

	// Handle negative index
	if index < 0 {
		index = length + index
	}

	if index < 0 || index >= length {
		return nil, false
	}

	// Optimise: traverse from the closer end
	var node *ListNode
	if index < length/2 {
		// Traverse from head
		node = list.head
		for i := int64(0); i < index; i++ {
			node = node.next
		}
	} else {
		// Traverse from tail
		node = list.tail
		for i := length - 1; i > index; i-- {
			node = node.prev
		}
	}

	return node.data, true
}

// LSet sets the element at the specified index
func (l *ListOps) LSet(key string, index int64, value []byte) bool {
	list, ok := l.getList(key)
	if !ok {
		return false
	}

	list.mu.Lock()
	defer list.mu.Unlock()

	length := list.len.Load()

	// Handle negative index
	if index < 0 {
		index = length + index
	}

	if index < 0 || index >= length {
		return false
	}

	// Find node
	var node *ListNode
	if index < length/2 {
		node = list.head
		for i := int64(0); i < index; i++ {
			node = node.next
		}
	} else {
		node = list.tail
		for i := length - 1; i > index; i-- {
			node = node.prev
		}
	}

	copied := make([]byte, len(value))
	copy(copied, value)
	node.data = copied

	return true
}

// LRange returns elements in the specified range
// Supports negative indices
func (l *ListOps) LRange(key string, start, stop int64) [][]byte {
	list, ok := l.getList(key)
	if !ok {
		return [][]byte{}
	}

	list.mu.RLock()
	defer list.mu.RUnlock()

	length := list.len.Load()

	// Handle negative indices
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	// Clamp
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop || start >= length {
		return [][]byte{}
	}

	result := make([][]byte, 0, stop-start+1)

	// Find start node
	node := list.head
	for i := int64(0); i < start; i++ {
		node = node.next
	}

	// Collect elements
	for i := start; i <= stop && node != nil; i++ {
		result = append(result, node.data)
		node = node.next
	}

	return result
}

// LTrim trims the list to the specified range
func (l *ListOps) LTrim(key string, start, stop int64) bool {
	list, ok := l.getList(key)
	if !ok {
		return false
	}

	list.mu.Lock()
	defer list.mu.Unlock()

	length := list.len.Load()

	// Handle negative indices
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	// Clamp
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}

	// If range is invalid, delete the list
	if start > stop || start >= length {
		list.head = nil
		list.tail = nil
		list.len.Store(0)
		l.store.Delete(key)
		return true
	}

	// Find new head
	newHead := list.head
	for i := int64(0); i < start; i++ {
		newHead = newHead.next
	}

	// Find new tail
	newTail := newHead
	for i := start; i < stop; i++ {
		newTail = newTail.next
	}

	// Update list
	list.head = newHead
	list.head.prev = nil
	list.tail = newTail
	list.tail.next = nil
	list.len.Store(stop - start + 1)

	return true
}

// LRem removes count occurrences of elements equal to value
// count > 0: remove from head to tail
// count < 0: remove from tail to head
// count = 0: remove all occurrences
func (l *ListOps) LRem(key string, count int64, value []byte) int64 {
	list, ok := l.getList(key)
	if !ok {
		return 0
	}

	list.mu.Lock()
	defer list.mu.Unlock()

	var removed int64
	var toRemove []*ListNode

	if count >= 0 {
		// Remove from head to tail
		node := list.head
		for node != nil && (count == 0 || removed < count) {
			if bytesEqual(node.data, value) {
				toRemove = append(toRemove, node)
				removed++
			}
			node = node.next
		}
	} else {
		// Remove from tail to head
		count = -count
		node := list.tail
		for node != nil && removed < count {
			if bytesEqual(node.data, value) {
				toRemove = append(toRemove, node)
				removed++
			}
			node = node.prev
		}
	}

	// Remove nodes
	for _, node := range toRemove {
		if node.prev != nil {
			node.prev.next = node.next
		} else {
			list.head = node.next
		}

		if node.next != nil {
			node.next.prev = node.prev
		} else {
			list.tail = node.prev
		}

		list.len.Add(-1)
	}

	// Delete list if empty
	if list.len.Load() == 0 {
		l.store.Delete(key)
	}

	return removed
}

// LInsert inserts value before or after the pivot
// Returns the length of the list after the insert, or -1 if pivot not found
func (l *ListOps) LInsert(key string, before bool, pivot, value []byte) int64 {
	list, ok := l.getList(key)
	if !ok {
		return -1
	}

	list.mu.Lock()
	defer list.mu.Unlock()

	// Find pivot
	var pivotNode *ListNode
	node := list.head
	for node != nil {
		if bytesEqual(node.data, pivot) {
			pivotNode = node
			break
		}
		node = node.next
	}

	if pivotNode == nil {
		return -1
	}

	copied := make([]byte, len(value))
	copy(copied, value)
	newNode := &ListNode{data: copied}

	if before {
		newNode.next = pivotNode
		newNode.prev = pivotNode.prev
		if pivotNode.prev != nil {
			pivotNode.prev.next = newNode
		} else {
			list.head = newNode
		}
		pivotNode.prev = newNode
	} else {
		newNode.prev = pivotNode
		newNode.next = pivotNode.next
		if pivotNode.next != nil {
			pivotNode.next.prev = newNode
		} else {
			list.tail = newNode
		}
		pivotNode.next = newNode
	}

	list.len.Add(1)
	return list.len.Load()
}

// LPushX inserts value at the head only if the list exists
func (l *ListOps) LPushX(key string, values ...[]byte) int64 {
	list, ok := l.getList(key)
	if !ok {
		return 0
	}

	list.mu.Lock()
	defer list.mu.Unlock()

	for _, value := range values {
		copied := make([]byte, len(value))
		copy(copied, value)

		node := &ListNode{data: copied}
		node.next = list.head
		if list.head != nil {
			list.head.prev = node
		}
		list.head = node
		if list.tail == nil {
			list.tail = node
		}

		list.len.Add(1)
	}

	return list.len.Load()
}

// RPushX inserts value at the tail only if the list exists
func (l *ListOps) RPushX(key string, values ...[]byte) int64 {
	list, ok := l.getList(key)
	if !ok {
		return 0
	}

	list.mu.Lock()
	defer list.mu.Unlock()

	for _, value := range values {
		copied := make([]byte, len(value))
		copy(copied, value)

		node := &ListNode{data: copied}
		node.prev = list.tail
		if list.tail != nil {
			list.tail.next = node
		}
		list.tail = node
		if list.head == nil {
			list.head = node
		}

		list.len.Add(1)
	}

	return list.len.Load()
}

// LMove moves an element from source to destination list
// srcDir and dstDir can be "LEFT" or "RIGHT"
func (l *ListOps) LMove(srcKey, dstKey string, srcDir, dstDir string) ([]byte, bool) {
	// Pop from source
	var value []byte
	var ok bool

	if srcDir == "LEFT" {
		value, ok = l.LPop(srcKey)
	} else {
		value, ok = l.RPop(srcKey)
	}

	if !ok {
		return nil, false
	}

	// Push to destination
	if dstDir == "LEFT" {
		l.LPush(dstKey, value)
	} else {
		l.RPush(dstKey, value)
	}

	return value, true
}
