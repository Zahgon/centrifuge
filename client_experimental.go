package centrifuge

import (
	"errors"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/queue"
)

var errNoSubscription = errors.New("no subscription to a channel")

// WritePublication allows sending publications to Client subscription directly
// without HUB and Broker semantics. The possible use case is to turn subscription
// to a channel into an individual data stream.
// This API is EXPERIMENTAL and may be changed/removed.
func (c *Client) WritePublication(channel string, publication *Publication, sp StreamPosition) error {
	_ = "STUB: not implemented"
	return nil
}

// AcquireStorage returns an attached connection storage (a map) and a function to be
// called when the application finished working with the storage map. Be accurate when
// using this API – avoid acquiring storage for a long time - i.e. on the time of IO operations.
// Do the work fast and release with the updated map. The API designed this way to allow
// reading, modifying or fully overriding storage map and avoid making deep copies each time.
// Note, that if storage map has not been initialized yet - i.e. if it's nil - then it will
// be initialized to an empty map and then returned – so you never receive nil map when
// acquiring. The purpose of this map is to simplify handling user-defined state during the
// lifetime of connection. Try to keep this map reasonably small.
// This API is EXPERIMENTAL and may be changed/removed.
func (c *Client) AcquireStorage() (map[string]any, func(map[string]any)) {
	_ = "STUB: not implemented"
	return nil, nil
}

// OnStateSnapshot allows settings StateSnapshotHandler.
// This API is EXPERIMENTAL and may be changed/removed.
func (c *Client) OnStateSnapshot(h StateSnapshotHandler) { _ = "STUB: not implemented"; return }

// StateSnapshot allows collecting current state copy.
// Mostly useful for connection introspection from the outside.
// This API is EXPERIMENTAL and may be changed/removed.
func (c *Client) StateSnapshot() (any, error) { _ = "STUB: not implemented"; return *new(any), nil }

func (c *Client) writeQueueItems(items []queue.Item) error { _ = "STUB: not implemented"; return nil }

// close in goroutine to not block message broadcast.

// ChannelBatchConfig allows configuring how to write push messages to a channel
// during broadcasts (applied for publication, join and leave pushes).
// This API is EXPERIMENTAL and may be changed/removed.
// If MaxSize is set to 0 then no batching by size will be performed.
// If MaxDelay is set to 0 then no batching by time will be performed.
// If both MaxSize and MaxDelay are set to 0 then no batching will be performed.
type ChannelBatchConfig struct {
	// MaxSize is the maximum number of messages to batch before flushing.
	MaxSize int64
	// MaxDelay is the maximum time to wait before flushing.
	MaxDelay time.Duration
	// FlushLatestPublication if true, then Centrifuge flushes only the latest publication
	// in the batch upon reaching the MaxSize or MaxDelay. Skipping on this level does
	// not work with delta compression.
	FlushLatestPublication bool
}

// channelWriter buffers queue.Item objects and flushes them after a fixed delay
// or when a specific batch size is reached.
type channelWriter struct {
	mu         sync.Mutex
	buffer     []queue.Item
	timer      *time.Timer
	flushFn    func([]queue.Item) error
	latestOnly bool
	// latestPubs tracks the latest publication per key for FlushLatestPublication mode.
	// Items are ordered by last-update time so that offsets are emitted in ascending order.
	// For non-map publications (Key=""), all collapse into a single entry under "".
	latestPubs []queue.Item
}

// newChannelWriter creates a new channelWriter with the given flush callback.
func newChannelWriter(flushFn func([]queue.Item) error) *channelWriter {
	_ = "STUB: not implemented"
	return nil
}

// close stops the timer and optionally flushes remaining items.
func (w *channelWriter) close(flushRemaining bool) { _ = "STUB: not implemented"; return }

// Add appends an item to the buffer or records it as the latest publication per key.
// When FlushLatestPublication is enabled, publications are coalesced by key — only the
// latest publication for each key is kept. For non-map publications (Key=""), all collapse
// into a single entry. Items are ordered by last-update time so offsets stay ascending.
// It starts a delay timer if this is the first item, and flushes immediately if the batch size is reached.
func (w *channelWriter) Add(item queue.Item, config ChannelBatchConfig) {
	_ = "STUB: not implemented"
	return
}

// Remove existing entry with the same key (if any) to maintain offset order.

// Append to the end — latest update has the highest offset.

// Total items count includes all latest pubs.

// Start timer on first item.

// Flush immediately if batch size is reached.

// Set timer to nil so waitTimer knows it was cancelled.

// waitTimer waits for the timer to fire, then flushes the batch.
func (w *channelWriter) waitTimer(tm *time.Timer) {
	_ = "STUB: not implemented"
	// Wait for the timer to fire.
	return
}

// If timer was stopped, do nothing.

// Flush if any items exist.

// Mark the timer as no longer active.

// flushLocked flushes the current batch. Caller must hold the lock.
func (w *channelWriter) flushLocked() { _ = "STUB: not implemented"; return }

// Emit non-publication items first, then per-key latest publications
// in last-updated order (ascending offsets).

// perChannelWriter groups items by configuration (batch size and delay).
type perChannelWriter struct {
	mu      sync.RWMutex
	writers map[string]*channelWriter
	flushFn func([]queue.Item) error
}

// newPerChannelWriter creates a new channel writer.
func newPerChannelWriter(flushFn func([]queue.Item) error) *perChannelWriter {
	_ = "STUB: not implemented"
	return nil
}

// Close cancels all active timers in each channelWriter and discards any pending items.
func (pcw *perChannelWriter) Close(flushRemaining bool) { _ = "STUB: not implemented"; return }

// getWriter returns the channelWriter for the given channel's configuration,
// creating one if necessary.
func (pcw *perChannelWriter) getWriter(channel string) *channelWriter {
	_ = "STUB: not implemented"
	return nil
}

// Double-check existence after acquiring write lock.

func (pcw *perChannelWriter) delWriter(channel string, flushRemaining bool) {
	_ = "STUB: not implemented"
	return
}

// Add routes an item to its configuration-specific aggregator.
func (pcw *perChannelWriter) Add(item queue.Item, ch string, config ChannelBatchConfig) {
	_ = "STUB: not implemented"
	return
}

// TimerCanceler is the interface returned from ScheduleTimer which allows the task to be cancelled.
// EXPERIMENTAL API.
type TimerCanceler interface {
	// Cancel the timer.
	Cancel()
}

// TimerScheduler is the interface for scheduling timers.
// EXPERIMENTAL API.
type TimerScheduler interface {
	// ScheduleTimer adds a callback for later execution. The TimerCanceler is returned.
	ScheduleTimer(duration time.Duration, callback func()) TimerCanceler
}
