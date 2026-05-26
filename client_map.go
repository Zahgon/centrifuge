package centrifuge

import (
	"time"

	"github.com/centrifugal/protocol"
)

// Map subscriptions provide synchronized state across clients. Unlike normal pub/sub
// subscriptions, map subscriptions maintain a state of key-value entries plus a stream
// of changes for recovery.
//
// Subscription protocol phases:
//
//  1. State phase: Client paginates through current key-value state. Each response
//     includes a stream position (offset/epoch) marking the state's point in time.
//
//  2. Stream phase (optional): Client paginates through stream history to catch up on
//     changes since the state was taken. May skip if state is up-to-date.
//
//  3. Live phase: Server coordinates the transition to real-time:
//     - Starts buffering pub/sub messages before subscribing
//     - Subscribes to pub/sub channel
//     - Reads final stream catch-up since client's position
//     - Merges stream with buffered messages (deduplication by offset)
//     - Sends merged publications to client and enables live updates
//
// This buffering mechanism ensures no messages are lost during the gap between
// the final stream read and pub/sub subscription becoming active.
//
// Key differences from normal subscriptions:
//
// EnablePositioning and EnableRecovery are auto-set from the channel's Mode
// (configured in MapChannelOptions via GetMapChannelOptions resolver):
//
//   - MapModeEphemeral: Streamless mode. No stream history is maintained.
//     State is always available, but recovery on reconnect requires a full state re-sync.
//     CAS (ExpectedPosition) and Version-based dedup are not available.
//     EnablePositioning and EnableRecovery are both set to false.
//
//   - MapModeRecoverable / MapModePersistent: Stream mode. Publications are tracked with
//     offsets, stream history is maintained, and clients can recover missed publications
//     on reconnect. CAS and Version features are available.
//     EnablePositioning and EnableRecovery are both set to true.
//
// When Type is SubscriptionTypeMap in SubscribeOptions, the subscription gets:
//   - State delivery (always)
//   - Stream position tracking (if Mode.HasStream())
//   - Stream-based recovery (if Mode.HasStream())
//   - Delta compression support for stream catch-up (if negotiated)
//   - Tags filtering for stream and live publications (if allowed)
//
// Map Presence Subscriptions
//
// Map presence subscriptions allow clients to watch who is online in a channel.
// They are a special type of map subscription that tracks client or user presence.
//
// Presence is configured using full channel names:
//
//   - MapClientPresenceChannel (e.g., "presence-clients:game1") - When set, client presence
//     is published to this channel. Each entry is keyed by client ID and contains
//     full ClientInfo. Use for tracking individual connections.
//
//   - MapUserPresenceChannel (e.g., "presence-users:game1") - When set, user presence is
//     published to this channel. Each entry is keyed by user ID with minimal data.
//     Provides natural deduplication when users have multiple connections.
//
// Authorization flow:
//
// Presence subscriptions go through OnSubscribe handler with SubscribeEvent.Type set
// to SubscriptionTypeMap (same as map data subscriptions). The handler can use the
// channel name to distinguish presence channels from data channels.
//
// Client subscribes to presence channel -> OnSubscribe called with Type=SubscriptionTypeMap
// -> Handler returns SubscribeReply{Options: SubscribeOptions{Type: SubscriptionTypeMap}}
// -> Subscription proceeds
//
// Presence data lifecycle:
//
//   - On subscribe (with configured presence prefix): presence published
//   - Periodically refreshed via TTL to handle connection drops
//   - On unsubscribe/disconnect: presence removed (with stream entry for real-time notification)
//   - TTL expiration: automatic cleanup if client disappears without clean disconnect

// Map subscription phase constants.
const (
	MapPhaseLive   int32 = 0 // Join live pub/sub, switch to real-time streaming (default)
	MapPhaseStream int32 = 1 // Paginating over stream (history catch-up)
	MapPhaseState  int32 = 2 // Paginating over state (map state)
)

// subscribeResultTypeMap is the Type value for map subscriptions in protocol.SubscribeResult.
const subscribeResultTypeMap = 1

const (
	// defaultMapPageSize is the default page size when client does not specify one.
	defaultMapPageSize = 100
	// defaultMapMinPageSize is the default minimum page size for map pagination.
	defaultMapMinPageSize = 100
	// defaultMapMaxPageSize is the default maximum page size for map pagination.
	defaultMapMaxPageSize = 1000
)

// validateAndCreateTagsFilter validates the tags filter from the request and creates a tagsFilter.
// Returns (nil, nil) if req.Tf is nil. Returns (nil, error) if validation fails.
func (c *Client) validateAndCreateTagsFilter(req *protocol.SubscribeRequest, allowTagsFilter bool, channel string) (*tagsFilter, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// escapeStateForDelta JSON-escapes Data in state publications for delta-enabled JSON
// transport. This ensures the client receives data as JSON strings (matching the format
// used for real-time and recovered publications), so it can store exact bytes for delta.
func escapeStateForDelta(pubs []*protocol.Publication, deltaEnabled bool, isJSON bool) []*protocol.Publication {
	_ = "STUB: not implemented"
	return nil
}

// mapSubscribeState tracks state for map subscriptions that are still loading.
type mapSubscribeState struct {
	options             SubscribeOptions // From OnSubscribe callback
	epoch               string           // Epoch from first response (for validation)
	offset              uint64           // Offset from first state page (frozen for consistency)
	startedAt           int64            // UnixNano when catch-up started (for timeout)
	streamStart         uint64           // Stream top captured on first stream request
	offsetCaptured      bool             // True after offset is captured (since 0 is valid offset)
	streamStartCaptured bool             // True after streamStart is captured (since 0 is valid offset)
	isPresence          bool             // True if this is a presence subscription
	subscribingCh       chan struct{}    // Closed when subscription completes (for race handling)
	tagsFilter          *tagsFilter      // Client tags filter for state/stream publications
	serverTagsFilter    *tagsFilter      // Server tags filter for state/stream publications
}

// handleMapSubscribeCommand handles the full map subscribe command flow:
// validates, checks for continuation, calls OnSubscribe handler for initial requests.
func (c *Client) handleMapSubscribeCommand(
	req *protocol.SubscribeRequest,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	_ = "STUB: not implemented"
	return nil
}

// Sweep expired catch-ups on other channels. Handles abandoned catch-ups where
// the client stopped sending requests but stayed connected. The current channel
// is skipped — its expiry is checked below with a proper DisconnectStale.

// For map subscription continuation requests (pagination or non-state phase with existing state),
// bypass the OnSubscribe callback - we already authorized on the first request.

// handleMapSubscribe routes map subscription requests to the appropriate phase handler.
// This is called after OnSubscribe callback has authorized the map subscription.
func (c *Client) handleMapSubscribe(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	_ = "STUB: not implemented"
	return nil

	// Auto-set positioning flags from Mode.
}

// Route based on phase.

// handleMapStatePhase handles stateless state pagination.
func (c *Client) handleMapStatePhase(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	_ = "STUB: not implemented"
	return nil

	// Acquire pagination lock for this channel.
}

// Track map subscription state on first state request (no cursor).

// Validate and store tags filter on first request.

// Subsequent request - verify we have authorization.

// Use stored options.

// Build read options.

// Use cache for subscription state delivery

// If client provided position, validate epoch.

// Read state page.

// Get state for tags filter and epoch update.

// Capture epoch and offset on first page (frozen for consistency).
// The offset is used to return a consistent value on all subsequent pages,
// ensuring the stream catch-up starts from where the first state page was read.

// Filter state entries modified after client's position (for subsequent pages).
// This ensures entries that were updated after the first page was read don't appear
// in later pages, which would cause duplicates when client catches up from stream.

// Keep entries with offset <= client's saved offset.
// These are guaranteed to not appear in stream catch-up.

// Apply server tags filter to state publications.

// Apply client tags filter to state publications.

// Check for direct STATE→LIVE transition on last page.

// Disconnect raced with MapStateRead — subscription is being cleaned up.

// Use frozen offset from first page when available (multi-page pagination).
// stateResult.Position reflects the stream top at the time of THIS page read,
// but publications made during pagination won't appear in state pages AND would
// be missed by stream catch-up if we use the current page's offset. The frozen
// offset from the first page ensures stream catch-up covers the full gap.

// Streamless: always go LIVE on last page (no stream to paginate through).

// Positioned: skip STREAM phase if stream hasn't advanced much.

// Use limit as threshold - if stream is within one page, go LIVE.

// If error or stream too far ahead, fall through to normal STATE response.

// Use frozen offset from first page for consistency. On subsequent pages,
// stream.Top() may have advanced, but we return the first page's offset so
// the client's stream catch-up starts from a consistent point.

// Build response.

// Convert state entries (use State field, not Publications).

// JSON-escape state data for delta-enabled JSON transport so the client can
// store exact bytes for subsequent delta application.

// mapTransitionToLiveParams holds parameters that differ between the four
// methods that transition a map subscription to the live phase. The shared
// protocol (buffer -> subscribe -> stream-read -> merge -> respond -> finalize)
// is implemented once in handleMapTransitionToLive.
type mapTransitionToLiveParams struct {
	sincePosition             StreamPosition // Stream position to read from
	statePubs                 []*Publication // State publications for the response (nil when not applicable)
	allowStreamless           bool           // Whether streamless mode is allowed
	isRecovery                bool           // Whether this is a recovery (sets WasRecovering/Recovered)
	tagsFilterFromState       *tagsFilter    // Inherited client tags filter from prior phase
	serverTagsFilterFromState *tagsFilter    // Inherited server tags filter from prior phase
	metricsAction             string         // Metrics action string
}

// handleMapTransitionToLive implements the shared buffer-subscribe-read-merge protocol
// used by all four methods that transition a map subscription to the live phase:
// handleMapStateToLive, handleMapStreamToLive, and handleMapRecoveryJoin.
func (c *Client) handleMapTransitionToLive(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	opts SubscribeOptions,
	isPresence bool,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
	params mapTransitionToLiveParams,
) error {
	_ = "STUB: not implemented"
	return nil

	// Build subscription info first, validate before subscribing.
}

// Process tags filter if provided.

// Use tags filter from prior phase if not provided in this request.

// Server tags filter is always inherited from the state set at subscribe time.

// Negotiate delta type if requested.

// Start coordination: buffer -> add subscription -> read stream -> merge.

// Positioned mode: read stream from sincePosition to catch any updates.

// Default to MaxPageSize.

// No limit by default.

// Read limit+1 to distinguish "exactly at limit" from "too far behind".

// If recovering and the epoch doesn't match, force full re-subscribe.
// This covers: empty→real (client never had epoch), real→empty (after MapClear
// deleted meta row), and real→different (after Clear + new publications).
//
// For state→live (isRecovery=false), params.sincePosition.Epoch is the
// epoch returned by the broker during the state phase. A mismatch here
// means the broker flipped epochs between state and stream reads (e.g.
// MapClear or meta-TTL eviction landed in between); without this check
// the client would merge the prior-epoch state with new-epoch live pubs
// and silently lose any keys not republished. The `!= ""` guard keeps
// ephemeral-mode subscribes (which have no epoch) working.

// If we got more than the limit, client is too far behind.

// Convert stream publications to protocol format.

// Lock buffer and read buffered publications.

// Merge recovered and buffered publications.

// Update offset if we saw higher.

// Apply server tags filter to stream publications (after offset calculation).

// Apply client tags filter to stream publications.

// Apply delta compression to recovered publications if enabled.

// Streamless mode: use buffered publications directly (no stream read, no merge).

// Apply tags filter to buffered publications.

// Convert state publications to protocol format (if any).

// Build response with phase=0 (LIVE).

// Encode reply first so an encode failure is surfaced before we touch
// c.channels — keeps the rollback path simple.

// Build channel context with map flag.

// Install channelContext BEFORE writing the reply so that any follow-up
// commands from the SDK that arrive between the reply send and StopBuffering
// observe the channel as subscribed. Buffered PUB/SUB publications stay
// queued until StopBuffering below — they cannot reach the client yet.
// Move from mapSubscribing to channels. Always look up from the map under
// the lock to avoid closing a subscribingCh that was already closed by a
// concurrent disconnect handler (cleanupMapSubscribingAll).
//
// Re-check c.status under the lock — Client.close() may have run between
// addSubscription and here. If it has, c.channels was snapshotted before our
// entry existed (so close() did NOT remove our hub subscription), and
// cleanupMapSubscribingAll dropped the mapSubscribing entry. Writing
// channelContext now would leave a ghost subscription in the hub with no
// cleanup path. Roll back instead. Mirrors the pattern in subscribeCmd.

// Stop buffering after response written.

// Add presence and join handling.

// handleMapStateToLive handles direct transition from STATE to LIVE phase.
// This is called on the last state page when stream is close enough to go LIVE directly.
func (c *Client) handleMapStateToLive(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	state *mapSubscribeState,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
	statePubs []*Publication,
	statePos StreamPosition,
) error {
	_ = "STUB: not implemented"
	return nil
}

// handleMapStreamPhase handles stateless stream pagination (history catch-up).
// Server controls when to transition to LIVE based on captured streamStart.
func (c *Client) handleMapStreamPhase(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	_ = "STUB: not implemented"
	return nil

	// Reject stream phase in streamless mode.
}

// Check for existing subscription state or recovery mode.

// No existing state and not recovering - permission denied.

// Recovery mode: create subscription state on the fly.
// Client is reconnecting with offset/epoch, skip state phase.

// Acquire pagination lock for this channel.

// Validate epoch if provided.

// Capture stream top on first stream request.

// Check if close enough to go LIVE: offset + limit >= streamStart
// This means client can catch up to streamStart in one more read.

// Transition to LIVE - do the buffering/merge flow.

// Not close enough yet - return stream page with phase=1 so client continues.

// Read stream.

// For intermediate STREAM pages, use the last publication's offset rather than
// stream.Top(). Returning stream.Top() would cause the client to jump ahead,
// skipping publications between the last returned pub and stream.Top().

// Apply tags filter to stream publications.

// Build response.

// Convert publications.

// JSON-escape stream publication data for delta-enabled JSON transport so the client
// can store exact bytes for subsequent delta application.

func (c *Client) getMapPageSize(req *protocol.SubscribeRequest, chOpts MapChannelOptions) int {
	_ = "STUB: not implemented"
	return 0
}

// handleMapStreamToLive transitions from stream pagination to live phase.
// This is called when client is close enough to catch up in one read.
func (c *Client) handleMapStreamToLive(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	state *mapSubscribeState,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	_ = "STUB: not implemented"
	return nil
}

// handleMapLivePhase handles joining pub/sub with coordination.
// Used for recovery or paginated join: after pagination or reconnect, returns only stream catch-up.
func (c *Client) handleMapLivePhase(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	_ = "STUB: not implemented"
	return nil
}

// Get stored state if exists (for two-phase), or use reply options (for direct live).

// Validate epoch if client provided one.

// Direct-LIVE recovery: no STATE phase ran, so the server filter from
// reply.Options never landed in mapSubscribing. Inherit it here so it
// applies to recovered + live publications. Without this the server-side
// RBAC filter is bypassed on clean reconnect.

// Recovery or paginated join - stream catch-up needed.

// handleMapRecoveryJoin handles recovery or paginated join (stream catch-up only).
func (c *Client) handleMapRecoveryJoin(
	req *protocol.SubscribeRequest,
	reply SubscribeReply,
	opts SubscribeOptions,
	isPresence bool,
	tagsFilterFromState *tagsFilter,
	serverTagsFilterFromState *tagsFilter,
	cmd *protocol.Command,
	started time.Time,
	rw *replyWriter,
) error {
	_ = "STUB: not implemented"
	return nil
}

// buildMapChannelFlags builds channel flags for map subscriptions.
func (c *Client) buildMapChannelFlags(deltaEnabled bool, delta string, isPresence bool, opts SubscribeOptions, reply SubscribeReply) uint16 {
	_ = "STUB: not implemented"
	return 0
}

// Mark as map subscription.

// setupMapPresenceAndJoin handles presence and join event setup for map subscriptions.
func (c *Client) setupMapPresenceAndJoin(channel string, opts SubscribeOptions) {
	_ = "STUB: not implemented"
	// Add presence if enabled (uses MapBroker for map channels).
	return
}

// Add map client presence if channel is configured.

// Add map user presence if channel is configured.

// Emit join event if enabled. Synchronous (NOT `go`) so publishJoin
// reaches the broker before this function returns. If we spawned a
// goroutine, a quick disconnect's synchronous publishLeave could win
// the race to the broker — observers would see [leave, join] on the
// wire, corrupting any membership state derived from join/leave events.

// writeMapSubscribeReply writes a map subscribe result to the client.
func (c *Client) writeMapSubscribeReply(
	channel string,
	cmd *protocol.Command,
	res *protocol.SubscribeResult,
	started time.Time,
	rw *replyWriter,
) error {
	_ = "STUB: not implemented"
	return nil
}

// isMapCatchUpExpired checks whether the map catch-up has exceeded the configured timeout.
func (c *Client) isMapCatchUpExpired(state *mapSubscribeState, chOpts MapChannelOptions) bool {
	_ = "STUB: not implemented"
	return false
}

// Disabled.

// acquireMapPaginationLock tries to acquire a pagination lock for the channel.
// Returns true if lock acquired, false if another pagination is in progress.
func (c *Client) acquireMapPaginationLock(channel string) bool {
	_ = "STUB: not implemented"
	return false
}

// releaseMapPaginationLock releases the pagination lock for the channel.
func (c *Client) releaseMapPaginationLock(channel string) { _ = "STUB: not implemented"; return }

// sweepExpiredMapSubscribing removes expired mapSubscribing entries for channels
// other than skipChannel. The skipped channel is handled by the caller with a
// proper DisconnectSlow. Cleans up abandoned catch-ups where the client stopped
// sending requests but stayed connected. Called at the start of every map subscribe
// command — O(n) where n is in-progress catch-ups (typically 0–2), no-op when map
// is empty.
func (c *Client) sweepExpiredMapSubscribing(skipChannel string) {
	_ = "STUB: not implemented"
	// Snapshot under read lock so we can resolve channel options without holding
	// c.mu — resolveMapChannelOptions invokes a user-supplied callback and must
	// not run under c.mu (deadlock / priority-inversion risk if the callback
	// touches per-client state or external services).
	return
}

// Resolve options + check expiry outside any lock.

// Delete only entries whose *mapSubscribeState pointer still matches the
// snapshot — guards against a resubscribe that replaced the entry between
// the RUnlock and the Lock.

// cleanupMapSubscribing removes map subscribing state for a channel.
func (c *Client) cleanupMapSubscribing(channel string) { _ = "STUB: not implemented"; return }

// cleanupMapSubscribingAll removes all in-progress map subscriptions on disconnect.
func (c *Client) cleanupMapSubscribingAll() { _ = "STUB: not implemented"; return }

// mapPresenceTTL returns TTL for map presence entries.
// Uses 3x the presence update interval to allow for some delay while still expiring if updates stop.
func (c *Client) mapPresenceTTL() time.Duration {
	_ = "STUB: not implemented"
	return *new(time.Duration)
}

// Minimum 30 seconds.

// addMapClientPresence adds client presence to the given channel.
// Key is clientId, stores full ClientInfo.
func (c *Client) addMapClientPresence(presenceChannel string, info *ClientInfo) error {
	_ = "STUB: not implemented"
	// Use KeyModeIfNew with RefreshTTLOnSuppress to:
	// - Publish JOIN event only if this is a new presence entry
	// - Refresh TTL without publishing if entry already exists (quick reconnect)
	// KeyTTL is configured in GetMapChannelOptions for presence channels.
	return nil
}

// addMapUserPresence adds user presence to the given channel.
// Key is userId, no ClientInfo stored (just the key for uniqueness).
// No-op for anonymous connections (empty user ID) — user-presence has no
// meaningful key without a user ID, and MapPublish would reject the empty key.
func (c *Client) addMapUserPresence(presenceChannel string) error {
	_ = "STUB: not implemented"
	return nil
}

// Use KeyModeIfNew with RefreshTTLOnSuppress to:
// - Publish JOIN event only if this is a new user
// - Refresh TTL without publishing if user already exists
// KeyTTL is configured in GetMapChannelOptions for presence channels.

// updateMapPresence updates presence for a map channel using MapBroker.
// This is called periodically by updateChannelPresence to refresh the TTL.
// Handles presence channels based on configured prefixes.
func (c *Client) updateMapPresence(info *ClientInfo, ctx ChannelContext) error {
	_ = "STUB: not implemented"
	// Use KeyModeIfNew with RefreshTTLOnSuppress for TTL refresh:
	// - Since key already exists, publish is suppressed (no offset increment)
	// - TTL is refreshed without generating stream entries
	// Stream options (StreamSize/TTL/MetaTTL) use defaults from GetMapChannelOptions.
	return nil
}

// Update client presence if channel is configured.

// Update user presence if channel is configured. Skip for anonymous
// connections (empty user ID) — user-presence is keyless without a user.

// removeMapPresence removes presence for a map channel using MapBroker.
// Called on explicit unsubscribe or disconnect. Only removes client presence,
// user presence entries expire via TTL (acts as debounce for quick reconnects).
func (c *Client) removeMapPresence(channel string, ctx ChannelContext) error {
	_ = "STUB: not implemented"
	// Remove from :presence if EmitPresence is enabled.
	return nil
}

// Remove client presence if channel is configured.
// Stream options (StreamSize/TTL/MetaTTL) use defaults from GetMapChannelOptions.

// User presence entries are NOT removed on disconnect - they only expire via TTL.
// This provides debounce/grace period for quick reconnects.

// Remove key=clientId from channel if MapRemoveClientOnUnsubscribe is enabled.
// This is for ephemeral state like cursors where each client publishes
// to a key that equals their client ID.
// Stream options (StreamSize/TTL/MetaTTL) use defaults from GetMapChannelOptions.
