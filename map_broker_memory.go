package centrifuge

import (
	"context"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/memstream"
	"github.com/centrifugal/centrifuge/internal/priority"
)

// MemoryMapBroker is builtin default MapBroker which allows running Centrifuge-based
// server without any external storage. All data managed inside process memory.
//
// With this MapBroker you can only run single Centrifuge node. If you need to scale
// you should consider using another MapBroker implementation instead – for example
// RedisMapBroker.
type MemoryMapBroker struct {
	node              *Node
	eventHandler      BrokerEventHandler
	mapHub            *mapHub
	closeOnce         sync.Once
	closeCh           chan struct{}
	pubLocks          map[int]*sync.Mutex
	resultCache       map[string]map[string]resultCacheEntry // ch -> idempotencyKey -> entry
	resultCacheMu     sync.RWMutex
	nextExpireCheck   int64
	resultExpireQueue priority.Queue
}

type resultCacheEntry struct {
	Position StreamPosition
	ExpireAt int64 // UnixMilli
}

var _ MapBroker = (*MemoryMapBroker)(nil)

// MemoryMapBrokerConfig is a memory map broker config.
type MemoryMapBrokerConfig struct{}

// NewMemoryMapBroker initializes MemoryMapBroker.
func NewMemoryMapBroker(n *Node, _ MemoryMapBrokerConfig) (*MemoryMapBroker, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// RegisterEventHandler registers event handler and runs memory map broker.
func (e *MemoryMapBroker) RegisterEventHandler(h BrokerEventHandler) error {
	_ = "STUB: not implemented"
	return nil
}

// Close shuts down the broker.
func (e *MemoryMapBroker) Close(_ context.Context) error { _ = "STUB: not implemented"; return nil }

func (e *MemoryMapBroker) pubLock(ch string) *sync.Mutex { _ = "STUB: not implemented"; return nil }

func (e *MemoryMapBroker) Clear(_ context.Context, ch string, _ MapClearOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// Subscribe is noop here.
func (e *MemoryMapBroker) Subscribe(_ ...string) error {
	_ = "STUB: not implemented"

	// Unsubscribe is noop here.
	return nil
}

func (e *MemoryMapBroker) Unsubscribe(_ ...string) error {
	_ = "STUB: not implemented"

	// Publish publishes data to channel with optional key for keyed state.
	return nil
}

func (e *MemoryMapBroker) Publish(ctx context.Context, ch string, key string, opts MapPublishOptions) (MapUpdateResult, error) {
	_ = "STUB: not implemented"
	return *new(MapUpdateResult), nil
}

// Resolve and validate channel options.

// Reject CAS and Version in ephemeral mode.

// state publication stores full state (Data).

// For CAS mismatch, include current key state for immediate retry.
// Client uses: CurrentEntry.Offset + Position.Epoch for the next CAS attempt.

// Publish streamPub to subscribers.

// Remove removes a key from keyed state.
func (e *MemoryMapBroker) Remove(ctx context.Context, ch string, key string, opts MapRemoveOptions) (MapUpdateResult, error) {
	_ = "STUB: not implemented"
	return *new(MapUpdateResult), nil
}

// Resolve and validate channel options.

// Reject CAS in ephemeral mode.

// ReadStream retrieves publications from stream.
func (e *MemoryMapBroker) ReadStream(ctx context.Context, ch string, opts MapReadStreamOptions) (MapStreamResult, error) {
	_ = "STUB: not implemented"
	return *new(MapStreamResult), nil
}

// ReadState retrieves keyed state with revisions.
func (e *MemoryMapBroker) ReadState(ctx context.Context, ch string, opts MapReadStateOptions) (MapStateResult, error) {
	_ = "STUB: not implemented"
	return *new(MapStateResult), nil
}

// Stats returns state statistics.
func (e *MemoryMapBroker) Stats(ctx context.Context, ch string) (MapStats, error) {
	_ = "STUB: not implemented"
	return *new(MapStats), nil
}

func (e *MemoryMapBroker) getResultFromCache(ch string, key string) (StreamPosition, bool) {
	_ = "STUB: not implemented"
	return *new(StreamPosition), false
}

func (e *MemoryMapBroker) saveResultToCache(ch string, key string, sp StreamPosition, resultExpireMs int64) {
	_ = "STUB: not implemented"
	return
}

func (e *MemoryMapBroker) clearResultCache(ch string) { _ = "STUB: not implemented"; return }

func (e *MemoryMapBroker) expireResultCache() { _ = "STUB: not implemented"; return }

// Compact heap when stale entries accumulate excessively.

// mapHub manages keyed state for all channels.
//
// Lock ordering (acquire in this order to prevent deadlock):
//
//	pubLock(ch)  →  mapHub.Lock/RLock  →  mapChannel.mu
//
// pubLock: serializes Publish/Remove per channel (including stream.Add and HandlePublication).
// mapHub.Lock: protects channels map, expiration queues, and channel creation/deletion.
// mapHub.RLock: concurrent reads of channel state.
// mapChannel.mu: protects sortedKeys rebuild during getState (held under mapHub.RLock).
//
// expireKeysIteration uses two phases to respect this ordering:
//
//	Phase 1: mapHub.Lock — collect expired keys, remove from state.
//	Phase 2: pubLock → mapHub.Lock — add to stream, deliver events.
type mapHub struct {
	sync.RWMutex
	node            *Node
	channels        map[string]*mapChannel
	nextExpireCheck int64
	expireQueue     priority.Queue
	expires         map[string]int64
	nextRemoveCheck int64
	removeQueue     priority.Queue
	removes         map[string]int64
	closeCh         chan struct{}
	// Key TTL tracking
	nextKeyExpireCheck     int64
	keyExpireQueue         priority.Queue                         // priority queue of {ch:key, expireAt}
	keyExpires             map[string]int64                       // "ch:key" -> expireAt
	eventHandler           BrokerEventHandler                     // for publishing removal events
	channelOptionsResolver func(channel string) MapChannelOptions // for key expiration events
	pubLocks               map[int]*sync.Mutex                    // for ordering HandlePublication calls
}

// mapChannel represents keyed state for a single channel.
type mapChannel struct {
	mu              sync.Mutex // protects sortedKeys rebuild in getState
	stream          *memstream.Stream
	state           map[string]*stateEntry // key -> entry
	ordered         bool
	scores          map[string]int64 // key -> score (for ordered state)
	sortedKeys      []string         // cached sorted keys
	sortedKeysDirty bool             // true if sortedKeys needs rebuilding
	lastSortOrdered bool             // tracks whether last sort used ordered or unordered
	lastSortAsc     bool             // tracks last sort direction for ordered state
}

type stateEntry struct {
	Key          string
	Revision     StreamPosition
	Publication  *Publication
	Score        int64  // For ordered state
	ExpireAt     int64  // Millisecond timestamp (UnixMilli) for key TTL expiration (0 = no expiration)
	Version      uint64 // Per-key version for ordering (0 = disabled)
	VersionEpoch string // Per-key version epoch
}

func newMapHub(node *Node, pubLocks map[int]*sync.Mutex, closeCh chan struct{}) *mapHub {
	_ = "STUB: not implemented"
	return nil
}

func (h *mapHub) setChannelOptionsResolver(r func(channel string) MapChannelOptions) {
	_ = "STUB: not implemented"
	return
}

func (h *mapHub) setEventHandler(handler BrokerEventHandler) { _ = "STUB: not implemented"; return }

func (h *mapHub) runCleanups() { _ = "STUB: not implemented"; return }

func (h *mapHub) expireStreams() { _ = "STUB: not implemented"; return }

func (h *mapHub) removeChannels() { _ = "STUB: not implemented"; return }

// expiredKeyEvent holds a Phase 1 snapshot of an expired key candidate. Phase 2
// re-validates the entry under pubLock(ch) → hub lock before deleting state and
// appending the removal to the stream atomically.
type expiredKeyEvent struct {
	channel    string
	key        string
	expireAt   int64
	tags       map[string]string
	streamSize int
}

// expireKeys handles TTL-based expiration of individual state keys.
// When a key expires, it removes it from the state, updates aggregation counts,
// and publishes a removal event.
func (h *mapHub) expireKeys() { _ = "STUB: not implemented"; return }

func (h *mapHub) expireKeysIteration(nextKeyExpireCheck *int64) {
	_ = "STUB: not implemented"
	// Phase 1: Under hub lock — collect expired key candidates only. State is NOT
	// mutated here. Mutating state in Phase 1 without holding pubLock(ch) would
	// expose subscribers to an inconsistent ReadState→ReadStream window where the
	// key is gone from state but the corresponding removal event is not yet on the
	// stream — they would later receive a removal for a key they never saw. Phase 2
	// acquires pubLock(ch) → hub lock per channel and atomically deletes state +
	// appends the removal to the stream + invokes the handler, matching the lock
	// ordering and atomicity of Remove() and Publish().
	return
}

// Track oldest expired timestamp for lag metric.

// format: "channel\x00key"

// Check if expiration time was updated (key was refreshed)

// Re-queue with updated expiration

// Parse channel and key from combined string

// Verify entry's expiration matches (wasn't refreshed)

// now is UnixMilli
// Entry was refreshed, re-queue

// Track the oldest expired timestamp for the lag metric.

// Compact heap when stale entries exceed 2x live entries.
// Stale entries accumulate from TTL refreshes that push new items without
// removing old ones. Periodic compaction rebuilds the queue from h.keyExpires,
// the source of truth for per-key deadlines. This is correct because every
// key insertion/refresh updates h.keyExpires with the latest deadline, and
// every key removal deletes from h.keyExpires. The queue may contain outdated
// entries (old deadlines for refreshed keys), but they are harmlessly skipped
// at pop time when the deadline doesn't match h.keyExpires.

// Report cleanup lag metric outside the lock.

// Phase 2: Under pubLock(ch) → hub lock — delete state, append removal stream
// entry, and dispatch the event atomically per channel. Re-validate the entry
// to handle refreshes or removals that landed after the Phase 1 snapshot.

// Still expired with the same deadline — delete state and stream-append atomically.

// Entry was refreshed between Phase 1 and Phase 2 — re-queue.

// makeChKey creates a combined channel:key string for the expiration map.
func (h *mapHub) makeChKey(ch, key string) string { _ = "STUB: not implemented"; return "" }

// parseChKey splits a combined channel:key string.
func (h *mapHub) parseChKey(chKey string) (string, string) {
	_ = "STUB: not implemented"
	return "", ""
}

func (h *mapHub) add(ch string, key string, statePub *Publication, streamPub *Publication, chOpts MapChannelOptions, opts MapPublishOptions) (StreamPosition, *Publication, SuppressReason, error) {
	_ = "STUB: not implemented"
	return *new(StreamPosition), nil, *new(SuppressReason), nil
}

// Get previous publication for delta (key-based: same key's previous state).

// Canonical check order across brokers: Version → KeyMode → CAS.
// Dedup checks (version) drop duplicates first; constraint checks
// (KeyMode, CAS) report meaningful intent failures last.
// Version is gated by HasStream — streamless channels skip dedup.

// Check KeyMode condition before proceeding

// KeyModeIfNew but key already exists - suppress publish
// But optionally refresh TTL if RefreshTTLOnSuppress is set

// Update TTL tracking

// Keepalive must extend MetaTTL too — without this the channel
// can be garbage-collected by removeChannels even while keys are
// being refreshed, forcing an epoch reset on the next publish.

// KeyModeIfExists but key doesn't exist - skip

// CAS check: verify expected position (offset + epoch)

// Key doesn't exist - position mismatch

// Check both offset AND epoch

// Return current publication for immediate retry.
// Client uses: CurrentEntry.Offset + Position.Epoch for the next CAS attempt.

// Handle stream

// Set offset on publication for delivery

// No stream, just use current position

// Handle keyed state.

// Calculate expiration time (milliseconds for sub-second TTL precision).

// Store statePub in state (contains full state Data).
// Preserve stored version when caller publishes without one (matches Redis).
// Overwriting with 0 would erase dedup protection against late-arriving
// older versions from a concurrent producer.

// Mark sorted keys as dirty for any state change

// Handle key TTL expiration tracking

// Always push new heap entry. When a key is refreshed, the old heap entry
// becomes stale and will be discarded in expireKeysIteration (which checks
// storedExpireAt > poppedExpireAt). Pushing unconditionally ensures the heap
// has an entry with the correct (latest) expiration time.

func (h *mapHub) remove(ch string, key string, chOpts MapChannelOptions, opts MapRemoveOptions) (StreamPosition, *Publication, SuppressReason, error) {
	_ = "STUB: not implemented"
	return *new(StreamPosition), nil, *new(SuppressReason), nil
}

// Channel doesn't exist. When CAS is requested, the caller's
// ExpectedPosition cannot match (no meta yet) — mirror the Redis
// Lua which auto-creates meta with a fresh epoch and then returns
// position_mismatch against the caller's stale epoch. Without
// ExpectedPosition, surface KeyNotFound as before.

// CAS check runs BEFORE the missing-key short-circuit so we mirror the
// Redis broker's Lua behavior (map_broker_add.lua: is_leave=1 with
// expected_offset set and the key missing returns "position_mismatch").
// Optimistic-concurrency clients use SuppressReasonPositionMismatch as
// the signal to refetch state and retry; returning KeyNotFound here
// would break that flow across brokers.

// Key doesn't exist, nothing to remove. No CAS was requested, so
// surface KeyNotFound — same as Redis's path when expected_offset
// is unset and the key is missing.

// Capture tags before deletion for the removal publication.

// Remove from state

// Mark dirty for any removal

// Clean up key expiration tracking

// Create removal publication (reused for both stream and eventHandler).

// Add to stream if converging mode

// Refresh MetaTTL so the channel isn't garbage-collected while active.

func (h *mapHub) clear(ch string) { _ = "STUB: not implemented"; return }

// Clean up key expiration tracking for all keys in the channel.

// Remove channel and associated tracking entries.

func (h *mapHub) getStream(ch string, opts MapReadStreamOptions) (MapStreamResult, error) {
	_ = "STUB: not implemented"
	// Resolve MetaTTL from channel config.
	return *new(MapStreamResult), nil
}

// Channel not found — need write lock to create stream position.

// Still not found under write lock — create and return.

// Channel was created by another goroutine between RUnlock and Lock.
// Release write lock and re-acquire read lock for the read path.

// Deleted in the tiny window between Unlock and RLock.

// Validate epoch if provided.

func (h *mapHub) getState(ch string, opts MapReadStateOptions) (MapStateResult, error) {
	_ = "STUB: not implemented"
	// Resolve MetaTTL from channel config.
	return *new(MapStateResult), nil
}

// Acquire read lock to find the channel. If not found, upgrade to write lock
// to create stream position (establishing the epoch for consistency).

// Channel not found — need write lock to create stream position.

// Still not found under write lock — create and return.

// Channel was created by another goroutine between RUnlock and Lock.
// Release write lock and re-acquire read lock for the read path.

// Deleted in the tiny window between Unlock and RLock.

// Channel found — hold RLock for reads, channel.mu for sorted keys.

// Check if client requested specific state revision

// Handle single key lookup (Key filter) — takes priority over Limit.

// Limit=0: return only stream position (no entries).

// Rebuild sorted keys if dirty or sort order/direction changed since last call.

// Key-based cursor pagination for continuity during concurrent modifications.
// Instead of integer offset (which shifts when entries are added/removed),
// we use the last seen key as cursor. This ensures:
// - Entries that move across cursor boundary are modified (new offset) -> in stream
// - No entries are permanently skipped
// - Duplicates are filtered by offset > streamPos or deduped by key

// Unordered cursor: just the key (lexicographic)

// Set cursor to last key in this page (for next page)

// ordered cursor: "score\x00key"

// Unordered cursor: just the key

// Build only the pubs we need (avoids allocating full slice then slicing)

// Return the stored Publication pointer directly - it already has Key and Offset set

func (h *mapHub) getStats(ch string) (MapStats, error) {
	_ = "STUB: not implemented"
	return *new(MapStats), nil
}

// createStreamPosition initializes a channel with an empty stream if it doesn't exist.
// This is intentionally called from read paths (ReadState, ReadStream) because we need
// a stable epoch for the channel — clients use it to detect position invalidation.
// Without this, a channel created lazily on the first Publish could have its epoch change
// between a ReadState and the subsequent ReadStream, breaking recovery.
func (h *mapHub) createStreamPosition(ch string) StreamPosition {
	_ = "STUB: not implemented"
	return *new(StreamPosition)
}

func (h *mapHub) updateMetaTTL(ch string, metaTTL time.Duration) { _ = "STUB: not implemented"; return }
