package centrifuge

import (
	"context"
	"sync"
	"time"

	"github.com/centrifugal/protocol"
)

// notifChCapacity bounds the per-channel notification queue used by
// SharedPollManager.notify. Each entry is one tracked-key notification waiting
// to be batched into the next backend poll. Sized to absorb typical bursts
// (large reconnect waves, cache-invalidation storms). When the buffer is full,
// extra notifications are dropped and counted by the droppedNotifyCount metric
// — the next timer-based poll cycle covers anything that was lost.
const notifChCapacity = 4096

// SharedPollManager manages all shared poll channels for a Node.
// One instance per Node. Thread-safe.
type SharedPollManager struct {
	node       *Node
	shutdownCh chan struct{}
	wg         sync.WaitGroup // tracks running refresh workers for graceful shutdown
	sem        chan struct{}  // shared concurrency limiter for backend calls
	epoch      string         // random string generated at startup, used for versionless mode

	mu       sync.RWMutex
	channels map[string]*sharedPollChannelState

	brokerSubMu    sync.RWMutex
	brokerSubChans map[string]Broker              // key-channel → subscribed broker
	brokerSubKeys  map[string]map[string]struct{} // base channel → set of key-channels
}

type sharedPollChannelState struct {
	mu sync.Mutex // protects all fields below

	// opts is captured at first track for this channel state and treated as
	// immutable for the lifetime of the state. The refresh worker reads opts
	// (RefreshInterval, batching, KeepLatestData, versioned/versionless mode, …)
	// without re-resolving — runtime config changes via GetSharedPollChannelOptions
	// only take effect after the channel state fully drains and is recreated
	// (i.e., after all subscribers leave and the shutdown delay expires).
	opts SharedPollChannelOptions

	// epoch is a random string generated when this channel state is created.
	// Clients compare epochs on reconnect and reset stored versions when
	// the epoch changes. This covers channel state recreation (e.g., after
	// shutdown delay expires and all items are cleaned up) without requiring
	// a full server restart.
	epoch string

	// itemIndex: key → tracked entry (version, data).
	itemIndex map[string]*sharedPollTrackedEntry

	// versionCounter is a monotonic counter for synthetic versions (versionless mode).
	versionCounter uint64

	// notifCh receives individual key notifications from SharedPollNotify.
	// Buffered, non-blocking send — drops if full and increments the
	// droppedNotifyCount metric. Capacity is sized for typical bursts; under
	// extreme load drops are surfaced via the metric and the next timer-based
	// poll closes the gap.
	notifCh chan string

	// refreshWorker lifecycle.
	workerRunning bool
	workerCancel  context.CancelFunc
	workerCtx     context.Context // checked by track() to detect cancelled worker
	workerGen     uint64          // incremented on each worker start, checked on exit
	// shutdownTimer delays channel cleanup after last item removed.
	shutdownTimer *time.Timer
	removed       bool // set by shutdown timer under mu; track() checks this
}

type sharedPollTrackedEntry struct {
	version          uint64 // last version from backend (or synthetic in versionless mode)
	data             []byte // only when KeepLatestData is true
	dataHash         uint64 // xxhash64 hash of data, only used in versionless mode
	freshFromPublish bool   // set by SharedPollPublish, cleared each timer poll cycle
	needsBroadcast   bool   // set when a version=0 subscriber joins an existing key; cleared after broadcast

	// subscribeReady is non-nil if the broker.Subscribe for this key is
	// currently in flight (called by an earlier track*() that already
	// installed the entry but released s.mu before calling the broker).
	// Concurrent track*() callers for the same key must wait on this chan
	// before reporting success — otherwise a subsequent subscribe failure
	// + rollback would orphan their caller's client in the keyed hub with
	// no broker subscription. The chan is closed (and nilled under s.mu)
	// once subscribe completes; the result is recorded in subscribeErr
	// just before close, so the chan-close memory barrier makes it visible
	// to waiters.
	subscribeReady chan struct{}
	subscribeErr   error

	// pendingHubJoin counts trackKeys callers that have successfully
	// reserved this entry but have NOT yet finalized — either by joining
	// the hub via keyedManager.addSubscribers or by rolling back. While
	// pendingHubJoin > 0, untrack must NOT delete the entry: a concurrent
	// trackKeys caller may have returned success and be about to join the
	// hub, and deleting now would silently orphan that caller (in hub,
	// no itemIndex, no broker subscription). Each trackKeys call
	// increments under s.mu and returns a release closure that decrements
	// under s.mu and runs untrack-style cleanup if the counter and hub
	// subscriber count are both zero.
	pendingHubJoin int
}

func xxHash64(data []byte) uint64 { _ = "STUB: not implemented"; return 0 }

// sharedPollKeyChannel builds a PUB/SUB channel name scoped to a specific key.
// Format: "<len>:<channel><key>" — length-prefix encoding, safe for any broker
// (no null bytes, no special characters in the framing).
func sharedPollKeyChannel(channel, key string) string { _ = "STUB: not implemented"; return "" }

// parseSharedPollKeyChannel splits a key-scoped PUB/SUB channel into base channel and key.
// Returns ("", "") if the channel is not in the expected length-prefix format.
func parseSharedPollKeyChannel(keyCh string) (string, string) {
	_ = "STUB: not implemented"
	return "", ""
}

func newSharedPollManager(node *Node) *SharedPollManager { _ = "STUB: not implemented"; return nil }

// noopReleaseTrack is returned from track/trackKeys on early-exit paths
// (shutdown, etc.) where no reservation was acquired. Lets callers
// unconditionally invoke release() without nil-checking.
func noopReleaseTrack() {
	_ = "STUB: not implemented"

	// track registers an item in the shared poll channel state and ensures
	// a refresh worker is running. Hub registration (addSubscriber) is handled
	// by the generic keyed layer in handleTrack — NOT here.
	//
	// On success returns a release closure that the caller MUST invoke after
	// either joining the hub via keyedManager.addSubscribers or definitively
	// abandoning the track (rollback path). The closure decrements the
	// pendingHubJoin reservation and cleans up the entry if it has no
	// remaining holders. Without this contract a concurrent rollback in
	// another goroutine could orphan an in-flight hub join — see
	// pendingHubJoin doc.
	return
}

func (m *SharedPollManager) track(channel string, opts SharedPollChannelOptions, key string) (bool, uint64, func(), error) {
	_ = "STUB: not implemented"
	// Check global shutdown.
	return false, 0, nil, nil
}

// Get or create channel state.

// Re-check shutdown under state lock.

// If this state was removed by shutdown timer, replace it and loop
// until we hold the lock on a non-removed state.
//
// Re-check m.channels[channel] under m.mu and reuse any fresh state a
// concurrent caller installed — an unconditional overwrite would
// orphan that caller's worker (nothing outside m.channels keeps a
// reference for cancellation, so the lost worker would stay parked in
// select with an uncancelled context, and m.close()'s wg.Wait would
// hang forever).
//
// Loop, because the cur we ended up with may itself be removed by a
// third goroutine between when it was placed and when we acquire
// cur.mu — that would otherwise fall through to add a key + restart
// the worker on an about-to-be-finalized state, leaking the new
// worker the same way.

// Cancel any pending shutdown timer.

// Get or create itemIndex entry. New entries start at version=0.
// Client-provided versions never enter itemIndex.

// Reserve a hub-join slot. Decremented by the returned release closure
// or by the internal failure paths below.

// Ensure refresh worker is running.

// Identify ownership: either we OWN the in-flight subscribe for this
// key (isNewKey + PublishEnabled), or another track*() owns it and we
// must wait. ownEntry / waitCh capture pointers, not keys — between
// here and the broker.Subscribe result a concurrent untrack+retrack
// can replace s.itemIndex[key] with a different entry, and we must
// only close/delete the entry WE created.
//
// Use s.opts.PublishEnabled (frozen at channel-state creation) rather
// than the caller's opts.PublishEnabled. If the caller-passed opts
// drift from the channel's first-tracker opts, gating on the caller's
// flag would skip broker.Subscribe for a key on a PublishEnabled
// channel — publish() then routes through the broker per s.opts but
// this node is not subscribed for the key, so cross-node publications
// silently miss local subscribers.

// Wait for the concurrent in-flight subscribe (if any). The chan close
// is a memory barrier for waitEntry.subscribeErr.

// The owner of the in-flight subscribe failed and rolled back
// its entry. We must fail too — surfacing success would orphan
// our caller in the keyed hub with no broker subscription.
// Decrement our reservation (the wait entry; ownEntry is nil
// in the wait branch). No synchronous channel cleanup needed
// — the owner's failure path already handled that.

// Always notify waiters BEFORE touching itemIndex — the waiters
// are waiting on ownEntry.subscribeReady, not on whatever is
// currently in s.itemIndex[key] (a concurrent untrack+retrack may
// have installed a different entry by now).

// Synchronous cleanup of our owned entry (broker.Subscribe
// failed so brokerSubChans has no record). Pointer identity
// check protects against a concurrent untrack+retrack that
// replaced s.itemIndex[key] with a different entry.

// Decrement reservations. ownEntry's counter is moot (entry
// deleted) but the call is symmetric and harmless.

// trackKeyResult holds the outcome for a single key tracked via trackKeys.
type trackKeyResult struct {
	isNew        bool
	entryVersion uint64
}

// ownedKey pairs a key with the entry pointer this trackKeys call
// installed for it. Pointer identity matters: a concurrent untrack +
// retrack can replace s.itemIndex[key] with a different entry while our
// broker.Subscribe is in flight, and we must only close/delete the entry
// WE own — touching another goroutine's entry would either double-close
// its subscribeReady chan (panic) or erase its registration.
type ownedKey struct {
	key   string
	entry *sharedPollTrackedEntry
}

// reservation pairs a key with the entry pointer whose pendingHubJoin
// this trackKeys call incremented. Used to undo the increment (and
// possibly clean up the entry) on rollback or release. Pointer identity
// matters: a concurrent untrack+retrack may have installed a different
// entry by the time we run cleanup, and decrementing the wrong entry
// would underflow it or interfere with another caller's bookkeeping.
type reservation struct {
	key   string
	entry *sharedPollTrackedEntry
}

// releaseTrackReservations decrements pendingHubJoin on each reserved
// entry and performs untrack-style cleanup for any entry whose counter
// reaches zero AND has no remaining hub subscribers. The hub-count check
// closes the inter-client orphan race: another in-flight trackKeys
// caller (waiter who returned success and is about to call
// addSubscribers) keeps the entry alive via its own reservation until
// it has joined the hub.
//
// Pointer identity (s.itemIndex[key] == r.entry) is required before
// deleting — a concurrent untrack+retrack may have installed a different
// entry, and deleting now would erase that caller's freshly installed
// entry.
//
// Used by the release closure returned to successful trackKeys callers,
// invoked after either addSubscribers (entry stays via hub.count>0) or
// rollback (entry deletes if no other holders).
func (m *SharedPollManager) releaseTrackReservations(channel string, s *sharedPollChannelState, reservations []reservation) {
	_ = "STUB: not implemented"
	return
}

// releasePendingHubJoin decrements pendingHubJoin on each reservation
// without performing any entry or channel-state cleanup. Used by
// trackKeys / track internal failure paths (broker.Subscribe failure or
// wait-failure) where the failing path already runs its own synchronous
// cleanup — we just need to drop the reservations we held on wait
// entries so other concurrent trackKeys callers see a correct count.
// Owned entries are deleted synchronously by the failing path; their
// counter goes away with the entry, so the decrement is harmless.
func (m *SharedPollManager) releasePendingHubJoin(s *sharedPollChannelState, reservations []reservation) {
	_ = "STUB: not implemented"
	return
}

// trackKeys registers multiple items in the shared poll channel state in
// one batch. It subscribes to all new keys with a single broker.Subscribe
// call. On success returns a release closure the caller MUST invoke
// after either joining the hub via keyedManager.addSubscribers or
// definitively abandoning the track (rollback path). See track() and
// pendingHubJoin doc for the orphan race this guards against.
func (m *SharedPollManager) trackKeys(channel string, opts SharedPollChannelOptions, keys []string) ([]trackKeyResult, func(), error) {
	_ = "STUB: not implemented"
	return nil, nil, nil
}

// Check global shutdown.

// Get or create channel state.

// Re-check shutdown under state lock.

// If this state was removed by shutdown timer, replace it. See the
// matching loop in track() for the race rationale: both an
// unconditional overwrite and a single-shot re-check would still
// orphan a worker — a fresh state we land on may itself be in the
// process of being shut down by a third goroutine.

// Cancel any pending shutdown timer.

// Register all keys and collect results, owned-new-entry pointers,
// reservation list (for pendingHubJoin bookkeeping), and any in-flight
// subscribe chans we must wait on. We capture entry pointers (not key
// strings) because a concurrent untrack+retrack can replace
// s.itemIndex[key] with a different entry between here and when our
// broker.Subscribe completes — close/delete must only touch the
// entries WE installed, and pending-counter decrement must only touch
// the entry we incremented.
//
// Use s.opts.PublishEnabled (frozen at channel-state creation) rather
// than the caller's opts.PublishEnabled. See track() for the drift
// rationale — gating broker.Subscribe on the caller's flag would
// silently skip subscriptions on a PublishEnabled channel and miss
// cross-node publications for that key.

// reservedSet dedups: if the same key appears multiple times in the
// keys slice, we reserve once per unique entry. Otherwise duplicate
// keys would over-increment pendingHubJoin and the matching release
// would underflow (or worse, decrement another caller's count).

// ownedSet identifies entries this call installed so a duplicate key
// later in the same `keys` slice does not get classified as someone
// else's in-flight subscribe — which would have us wait on our own
// subscribeReady chan and deadlock.

// Ensure refresh worker is running.

// Wait for any concurrent in-flight broker subscribes for keys we did
// not create. The chan close acts as a memory barrier for subscribeErr.
// If any in-flight subscribe failed, our track fails too — surfacing
// success while a key's broker subscription is missing would orphan
// the caller's client in the keyed hub.

// Roll back the entries WE created so subsequent callers can
// retry from scratch (synchronous channel-state cleanup), and
// decrement our pending counter on wait entries.

// Always notify waiters BEFORE touching itemIndex — they're
// waiting on our captured ownEntry pointers, not on whatever the
// map currently holds (a concurrent untrack+retrack may have
// installed a different entry).

// Synchronous cleanup of our owned entries (broker.Subscribe
// failed; brokerSubChans has no record). Pointer identity
// check guards against concurrent untrack+retrack.

// rollbackOwnedKeys signals failure on each owned entry's subscribeReady
// chan and deletes the entry from itemIndex when it is still the installed
// one. Used when a concurrent in-flight subscribe (for a key we did NOT
// own) failed — we must abandon any entries we ourselves installed in this
// same trackKeys call so subsequent track*() callers retry from scratch.
func (m *SharedPollManager) rollbackOwnedKeys(channel string, s *sharedPollChannelState, owned []ownedKey, err error) {
	_ = "STUB: not implemented"
	return
}

// untrack removes an item from shared poll tracking when no connections
// remain. Called after the client has been removed from the hub via
// keyedHub.removeSubscriber.
//
// The pendingHubJoin guard prevents deleting an entry while a concurrent
// trackKeys caller still holds a reservation — that caller may have
// returned success and be about to join the hub, and deleting now would
// silently orphan it (in hub, no itemIndex, no broker subscription). See
// pendingHubJoin doc on sharedPollTrackedEntry.
func (m *SharedPollManager) untrack(channel string, key string) { _ = "STUB: not implemented"; return }

// warmKeyData holds cached data for a warm key that can be delivered directly.
type warmKeyData struct {
	key             string
	internalVersion uint64                // synthetic/real version for per-connection dedup
	pub             *protocol.Publication // publication with wire version
}

// getWarmKeyData returns cached data for warm keys. Only returns entries when
// KeepLatestData is enabled and the entry has data (version > 0).
func (m *SharedPollManager) getWarmKeyData(channel string, keys []string) []warmKeyData {
	_ = "STUB: not implemented"
	return nil
}

// markNeedsBroadcast flags existing keys so polls re-broadcast their data even
// if unchanged. Also triggers a notify for keys that have data (version > 0) and
// were not already flagged — this provides near-immediate delivery via the
// notification fast path instead of waiting for the next timer poll. The
// at-most-once notify per key ensures mass reconnect (1000 clients, same key)
// causes only one backend call per "wave", not one per client.
//
// Must be called AFTER addSubscriber so that broadcasts from the triggered notify
// (or a concurrent timer poll) can reach the subscribing client.
func (m *SharedPollManager) markNeedsBroadcast(channel string, keys []string) {
	_ = "STUB: not implemented"
	return
}

// Only notify keys that already have data — version=0 keys are
// being handled by an in-flight cold key notify.

// Notify outside the lock — triggers backend call for near-immediate delivery.
// Keys are combined by the worker's batch dedup when batching is configured.

// notify sends a key notification to the channel's notification channel.
// Non-blocking: drops the notification if the buffer is full.
// Silently ignores unknown channels (channel must already be tracked).
//
// The read lock is held across the send so that a concurrent shutdown cannot
// replace m.channels[channel] with a fresh state while we send into the now-
// orphaned notifCh (whose worker has been cancelled and will never drain it).
// The send is non-blocking (select with default) and the metric increments are
// cheap, so holding the RLock here does not introduce contention.
func (m *SharedPollManager) notify(channel string, key string) { _ = "STUB: not implemented"; return }

// Non-blocking send — drop if full.

// getCachedData returns cached publications for items where the server has a newer
// version than the client. Returns nil when nothing to return (omitted from protobuf).
// Only returns data when KeepLatestData is enabled for the channel.
func (m *SharedPollManager) getCachedData(channel string, items []TrackItem) []*protocol.Publication {
	_ = "STUB: not implemented"
	return nil
}

// hasChannel reports whether the SharedPollManager has state for the given channel.
func (m *SharedPollManager) hasChannel(channel string) bool {
	_ = "STUB: not implemented"
	return false
}

// initialChannelEpoch returns the epoch a freshly-created sharedPollChannelState
// starts with. For versionless mode the server generates a per-channel epoch
// (changes when state is recreated). For versioned mode the epoch is supplied
// by the publisher on each publish/refresh, so initial state is empty and
// gets populated via flipEpoch on the first incoming publish.
func initialChannelEpoch(opts SharedPollChannelOptions) string {
	_ = "STUB: not implemented"
	return ""
}

// Epoch returns the epoch string for subscribe replies.
//
// Versionless channels: server-generated, stable for the lifetime of channel
// state. Changes when state is recreated after shutdown delay expires.
//
// Versioned channels: publisher-supplied, set on first publish/refresh.
// Changes (epoch flip) trigger unsubscribe of all current subscribers with
// insufficient-state code so they re-track from version 0 on resubscribe.
func (m *SharedPollManager) Epoch(channel string, isVersionless bool) string {
	_ = "STUB: not implemented"
	return ""
}

// flipEpochAndCollectClients atomically updates the stored channel epoch,
// resets all per-key state, and collects the *Client references currently
// subscribed (via the keyed hub). Caller must invoke Client.Unsubscribe on
// each returned client with no locks held — calling Unsubscribe under
// s.mu would deadlock since Unsubscribe's cleanup path takes s.mu via
// untrack.
//
// All state mutation (epoch + entries) and the client snapshot are taken
// under s.mu to prevent a window where a client subscribing concurrently
// with the flip could be unsubbed despite having seen the new epoch in
// its subscribe reply.
//
// If newEpoch == s.epoch, returns nil and performs no mutation.
func (s *sharedPollChannelState) flipEpochAndCollectClients(hub *keyedHub, newEpoch string) []*Client {
	_ = "STUB: not implemented"
	return nil
}

// Nested s.mu -> h.mu is consistent with existing patterns in this file
// (e.g. SharedPollRevokeKeys uses removeAllSubscribers under s.mu).

// close stops all refresh workers and waits for them to finish.
func (m *SharedPollManager) close() { _ = "STUB: not implemented"; return }

// Unsubscribe all broker subscriptions, batched by broker.

func (m *SharedPollManager) subscribeToBrokerKeys(channel string, keys []string) error {
	_ = "STUB: not implemented"
	return nil
}

// Per-channel sharded lock — mirrors Node.addSubscription so that a
// concurrent shared-poll unsubscribe (queued via subDissolver) cannot
// race between our broker.Subscribe and brokerSubChans update and
// silently unsubscribe the key at the broker after we've recorded it
// as subscribed. Without this, the queued unsub's filter check sees
// the key absent (because we hadn't installed our entry yet when it
// ran), proceeds to broker.Unsubscribe(kc) after we have already
// re-subscribed, and then deletes brokerSubChans[kc] — leaving the
// new tracker in the hub with no broker subscription.

func (m *SharedPollManager) unsubscribeFromBrokerKeys(channel string, keys []string) {
	_ = "STUB: not implemented"
	return
}

// Hold the per-channel sharded lock across the filter recheck, the
// broker.Unsubscribe call, and the brokerSubChans update — so a
// concurrent retrack (which also takes the lock in
// subscribeToBrokerKeys) cannot squeeze a fresh broker.Subscribe
// between our filter check and our Unsubscribe. Mirrors the
// Node.removeSubscription pattern.

// Filter out keys that were re-tracked between queueing and execution.

// unsubscribeAllBrokerKeys unsubscribes all key-channels for a base channel.
// Used on channel shutdown.
func (m *SharedPollManager) unsubscribeAllBrokerKeys(channel string) {
	_ = "STUB: not implemented"
	return
}

// Per-channel sharded lock — same rationale as unsubscribeFromBrokerKeys.
// A concurrent track() that re-creates the channel state and issues a
// fresh broker.Subscribe could otherwise race the shutdown-time
// Unsubscribe and end up with a tracked key but no broker subscription.

// Check if channel was re-created while shutting down.

func (m *SharedPollManager) publish(ctx context.Context, channel string, key string, version uint64, epoch string, data []byte) error {
	_ = "STUB: not implemented"
	// Resolve channel options. Prefer the running channel state's opts
	// (immutable for its lifetime). Fall back to the config callback so a
	// publisher node that has never tracked the channel still gets the
	// correct routing decision.
	return nil
}

// Routing is purely a function of PublishEnabled — not of local
// subscriber state. When PublishEnabled is true we always go through
// the broker so cross-node subscribers receive the publication,
// regardless of whether this node has tracked the key (and regardless
// of any in-flight broker.Subscribe). For shared-poll keyed channels
// the broker's Publication.Epoch carries the publisher's per-channel
// epoch (the wire field is shared with stream channels' stream-position
// epoch — semantics differ per channel type, but the wire is the same).

// Local-only mode: apply directly. Only this node's subscribers see it.

// stats returns the number of active channels and total tracked keys.
func (m *SharedPollManager) stats() (int, int) { _ = "STUB: not implemented"; return 0, 0 }

func (m *SharedPollManager) handlePublishedData(channel string, key string, version uint64, epoch string, data []byte) {
	_ = "STUB: not implemented"
	return
}

// SharedPollPublish should not reach here in versionless mode
// (rejected by publish()), but guard defensively.

// Epoch comparison runs *before* the entry-lookup early-return so the
// channel's stored epoch stays current even on nodes that don't track
// this specific key. Otherwise the first subscriber to ever track a
// key on a quiet node would trigger an unnecessary unsubscribe cycle
// when the cold-key auto-poll detects the existing publisher epoch.

// Key not tracked on this node.

// SharedPollRevokeKeys removes items for matching connections. Sends
// Publication{Removed: true} to affected subscribers and cleans up hub + itemIndex.
func (m *SharedPollManager) SharedPollRevokeKeys(channel string, keys []string, users []string, excludeUsers []string) {
	_ = "STUB: not implemented"
	return
}

// Broadcast removals to affected subscribers.

// Clean up hub and itemIndex. Track keys we actually delete from
// itemIndex so we can drop their broker subscriptions too — otherwise
// the broker keeps pushing cross-node publications that handlePublishedData
// silently no-ops (entry == nil), wasting traffic until full channel
// shutdown.

// Remove from itemIndex if no subscribers remain AND no in-flight
// trackKeys reservation is holding the key alive. Mirrors the
// untrack guard so a concurrent track caller about to addSubscribers
// is not orphaned.

// scheduleShutdown starts a delayed cleanup timer.
// A zero delay means default (1s). Use a negative delay (e.g. -1) for immediate shutdown.
func (s *sharedPollChannelState) scheduleShutdown(m *SharedPollManager, channel string, delay time.Duration) {
	_ = "STUB: not implemented"
	return
}

// Immediate shutdown: mirror the timer body. Release s.mu before
// touching m.mu — lock ordering is m.mu → s.mu everywhere.

// doShutdownLocked marks the state as removed and cancels the worker. Caller
// must hold s.mu and must not call any m.mu operation while holding s.mu —
// lock ordering is m.mu → s.mu. The follow-up m.mu work is done in
// finalizeShutdown after the caller releases s.mu.
func (s *sharedPollChannelState) doShutdownLocked(m *SharedPollManager, channel string) {
	_ = "STUB: not implemented"
	return
}

// finalizeShutdown removes the channel from the manager and unsubscribes broker
// keys. Must be called with s.mu released (acquires m.mu and may call back into
// keyedManager / dissolver). Safe to call when m.channels[channel] no longer
// points to s — that branch is a no-op.
//
// The keyedManager call is removeChannelIfEmpty (not removeChannel) to avoid
// a race: a new client whose handleTrack ran concurrently with our shutdown
// may have already added itself as a subscriber via
// keyedManager.addSubscribers, populating the channel's hub. An
// unconditional removeChannel would delete that state and orphan the new
// subscriber from future broadcasts (which look up via getHub).
func (s *sharedPollChannelState) finalizeShutdown(m *SharedPollManager, channel string) {
	_ = "STUB: not implemented"
	return
}

// cancelShutdown stops a pending shutdown timer. Caller must hold s.mu.
func (s *sharedPollChannelState) cancelShutdown() { _ = "STUB: not implemented"; return }

func (s *sharedPollChannelState) runRefreshWorker(ctx context.Context, node *Node, channel string, gen uint64, m *SharedPollManager) {
	_ = "STUB: not implemented"
	return
}

// Notification batching state.

// When size-based batching is configured without a delay, use the
// refresh interval as the cap so notifications don't sit indefinitely.

// Pass work time (excluding intentional spread delays)
// so backpressure reacts to actual backend load.

// Period-based timer: subtract cycle duration so the
// period stays close to the configured interval.

// No batching — fire immediately.

// runNotifiedRefreshCycle runs an immediate backend poll for just the notified keys.
// Unlike runRefreshCycle, there's no spread delay or chunking — the batch is already bounded.
func (s *sharedPollChannelState) runNotifiedRefreshCycle(ctx context.Context, node *Node, channel string, keys []string, sem chan struct{}) {
	_ = "STUB: not implemented"
	// Filter to keys still in itemIndex.
	return
}

// Acquire semaphore.

// Build event items.

// Notified refresh does not track absences — absence tracking is only
// meaningful for full-channel timer-based polls.

func (s *sharedPollChannelState) runRefreshCycle(ctx context.Context, node *Node, channel string, sem chan struct{}) time.Duration {
	_ = "STUB: not implemented"
	// 1. Collect all item keys from itemIndex.
	return *new(time.Duration)
}

// Clear flag.
// Skip — data is fresh from publish.

// Split into chunks.

// Spread chunk dispatches evenly over the refresh interval to avoid
// bursting all backend calls at once. The delay between dispatches is
// interval / num_chunks. The semaphore still limits actual concurrency.

// time.NewTimer + explicit Stop avoids leaking a timer per iteration
// when ctx is cancelled (time.After leaves the unselected timer to
// fire later, garbage-collected only after the duration elapses).

// Build event items.

// Return total intentional spread delay so callers (e.g. backpressure)
// can distinguish work time from spread time.

type pendingBroadcast struct {
	key     string
	version uint64
	pub     *protocol.Publication
	prep    preparedData
	removal bool
}

// buildPreparedPollData computes delta data for a shared poll publication.
// If prevData is available, computes a fossil delta patch; otherwise returns
// an empty preparedData (no delta). prevVersion is the version of the bytes
// in prevData (entry.version BEFORE this publish); keyedWritePublication
// uses it to verify the delta's base matches the client's current data,
// falling back to FULL when concurrent broadcasts mean the client never
// received the version this patch is built against. The actual per-client
// encoding (JSON escaping, protocol framing) is done lazily in
// keyedWritePublication.
func buildPreparedPollData(pub *protocol.Publication, prevData []byte, prevVersion uint64) preparedData {
	_ = "STUB: not implemented"
	return *new(preparedData)
}

// onNotifiedRefreshResponse processes a backend response triggered by a
// notification (targeted poll). Identical to onRefreshResponse except for the
// metric source label.
func (s *sharedPollChannelState) onNotifiedRefreshResponse(channel string, respEpoch string, items []SharedPollRefreshItem, hub *keyedHub, node *Node) {
	_ = "STUB: not implemented"
	return
}

// onRefreshResponse processes a backend response from the periodic timer poll.
func (s *sharedPollChannelState) onRefreshResponse(channel string, respEpoch string, items []SharedPollRefreshItem, hub *keyedHub, node *Node) {
	_ = "STUB: not implemented"
	return
}

// applyRefreshResponse is the shared implementation for both notified and
// timer-driven refresh responses. The two source kinds differ only in the
// metric label used for instrumentation — all per-item handling (versioned vs
// versionless change detection, epoch flip, broadcast, removals, prev_data /
// KeepLatestData delta base capture) is identical.
func (s *sharedPollChannelState) applyRefreshResponse(channel string, respEpoch string, items []SharedPollRefreshItem, hub *keyedHub, node *Node, source string) {
	_ = "STUB: not implemented"
	// Versioned channels: detect publisher epoch change before any per-item
	// processing. A flip resets all per-key state and unsubscribes current
	// subscribers; the items in this response then repopulate state under
	// the new epoch via the standard path below.
	return
}

// Versionless backend response: detect changes by content hash.

// First data for this key.

// Choose (version, data) so the pair always matches. Without
// this, a concurrent SharedPollPublish that bumped
// entry.version while the backend response was in flight
// would have us emit (entry.version, e.Data) — the new
// (higher) version paired with the old bytes. The receiving
// client advances keyState.version to entry.version, then
// suppresses any later legitimate broadcast at the same
// version, pinning the client to wrong data until the next
// entry update.
//
// KeepLatestData: entry.data is the live publish payload at
// entry.version. Safe to broadcast both; clear needsBroadcast.
//
// !KeepLatestData: entry.data is empty. The only valid pair
// we can emit is the backend's (e.Version, e.Data). Emitting
// that pins the client to the backend's older version, which
// is fine — a future publish at any higher version will
// pass the keyState.version filter. But only do so when the
// versions match (no concurrent publish raced ahead); if
// they don't, leave needsBroadcast set so the next poll
// retries once the backend catches up.

// else: backend behind a concurrent publish; retry next poll.

// Capture prevData before updating — this is the delta base.

// Build publications outside the lock. Batch-allocate Publication structs
// in a single slice to avoid one heap allocation per changed key.

// Fan out outside the lock.

// Clean up removed items from hub and itemIndex.
