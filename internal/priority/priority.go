// Package priority provides priority queue.
package priority

// An Item is something we manage in a priority queue.
type Item struct {
	Value    string // The value of the item; arbitrary.
	Priority int64  // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A Queue implements heap.Interface and holds Items.
type Queue []*Item

// Len ...
func (pq Queue) Len() int {
	_ = "STUB: not implemented"

	// Less ...
	return 0
}

func (pq Queue) Less(i, j int) bool { _ = "STUB: not implemented"; return false }

// Swap ...
func (pq Queue) Swap(i, j int) { _ = "STUB: not implemented"; return }

// Push value into queue.
func (pq *Queue) Push(x any) { _ = "STUB: not implemented"; return }

// Pop value from queue.
func (pq *Queue) Pop() any { _ = "STUB: not implemented"; return *new(any) }

// for safety

// MakeQueue allows to create priority queue.
func MakeQueue() Queue { _ = "STUB: not implemented"; return *new(Queue) }
