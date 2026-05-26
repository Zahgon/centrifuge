package dissolve

import (
	"sync"
)

// Job to do.
type Job func() error

// queue is an unbounded queue of Job.
// The queue is goroutine safe.
// Inspired by http://blog.dubbelboer.com/2015/04/25/go-faster-queue.html (MIT)
type queue interface {
	// Add an Job to the back of the queue
	// will return false if the queue is closed.
	// In that case the Job is dropped.
	Add(Job) bool

	// Remove will remove a Job from the queue.
	// If false is returned, it either means 1) there were no items on the queue
	// or 2) the queue is closed.
	Remove() (Job, bool)

	// Close the queue and discard all entries in the queue
	// all goroutines in wait() will return
	Close()

	// Closed returns true if the queue has been closed
	// The call cannot guarantee that the queue hasn't been
	// closed while the function returns, so only "true" has a definite meaning.
	Closed() bool

	// Wait for a Job to be added or queue to be closed.
	// If there is items on the queue the first will
	// be returned immediately.
	// Will return "", false if the queue is closed.
	// Otherwise the return value of "remove" is returned.
	Wait() (Job, bool)
}

type queueImpl struct {
	mu      sync.RWMutex
	cond    *sync.Cond
	nodes   []Job
	head    int
	tail    int
	cnt     int
	size    int
	closed  bool
	initCap int
}

var initialCapacity = 2

// newQueue returns a new Job queue with initial capacity.
func newQueue() queue { _ = "STUB: not implemented"; return *new(queue) }

// WriteMany mutex must be held when calling
func (q *queueImpl) resize(n int) { _ = "STUB: not implemented"; return }

// Add a Job to the back of the queue
// will return false if the queue is closed.
// In that case the Job is dropped.
func (q *queueImpl) Add(i Job) bool { _ = "STUB: not implemented"; return false }

// Also tested a grow rate of 1.5, see: http://stackoverflow.com/questions/2269063/buffer-growth-strategy
// In Go this resulted in a higher memory usage.

// Close the queue and discard all entries in the queue
// all goroutines in wait() will return
func (q *queueImpl) Close() { _ = "STUB: not implemented"; return }

// Closed returns true if the queue has been closed
// The call cannot guarantee that the queue hasn't been
// closed while the function returns, so only "true" has a definite meaning.
func (q *queueImpl) Closed() bool { _ = "STUB: not implemented"; return false }

// Wait for a Job to be added.
// If there is items on the queue the first will
// be returned immediately.
// Will return nil, false if the queue is closed.
// Otherwise the return value of "remove" is returned.
func (q *queueImpl) Wait() (Job, bool) { _ = "STUB: not implemented"; return *new(Job), false }

// Remove will remove a Job from the queue.
// If false is returned, it either means 1) there were no items on the queue
// or 2) the queue is closed.
func (q *queueImpl) Remove() (Job, bool) { _ = "STUB: not implemented"; return *new(Job), false }
