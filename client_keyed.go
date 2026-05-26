package centrifuge

import (
	"time"

	"github.com/centrifugal/protocol"
)

// encodeKeyedPush encodes a publication as a Push (or Reply wrapping a Push) for this
// client's transport protocol. Used by keyed (shared poll) writes which bypass the
// Hub's per-protocol-key encoding.
func (c *Client) encodeKeyedPush(channel string, pub *protocol.Publication) ([]byte, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// keyedChannelDeltaState holds per-channel delta configuration for keyed subscriptions.
type keyedChannelDeltaState struct {
	deltaType DeltaType // negotiated delta type for this channel
}

// keyedKeyState holds per-key state for a keyed subscription.
type keyedKeyState struct {
	version    uint64 // per-connection version (from client track() or updated on delivery)
	deltaReady bool   // true after first full publication delivered for this key
	expireAt   int64  // unix timestamp when track signature expires; 0 = no expiry
}

// keyedState holds per-connection keyed subscription state.
type keyedState struct {
	// channels: channel → delta config. Only set when delta is negotiated.
	channels map[string]*keyedChannelDeltaState
	// trackedKeys: channel → (itemKey → per-key state).
	// The version here is the per-connection version (from client track()
	// or updated on publication delivery). NOT the server-side itemIndex version.
	trackedKeys map[string]map[string]*keyedKeyState
	// minTrackExpireAt: channel → lower bound on earliest key expiry.
	// Used as fast-path to skip key iteration when nothing can be expired.
	// 0 means no keys have expiry set (skip check entirely).
	minTrackExpireAt map[string]int64
}

// Keyed sub-refresh request types (wire protocol values).
const (
	typeTrack   int32 = 1
	typeUntrack int32 = 2
)

// handleTrack processes SubRefreshRequest with type=typeTrack (track).
// A request can carry multiple signed batches (req.Track) — the SDK packs
// every cached signature library entry into a single sub_refresh frame on
// reconnect replay, so one handler invocation may cover N signatures.
//
// Duplicate keys across batches are deduped with last-batch-wins semantics:
// version and per-batch ExpireAt come from the LAST batch the key appears
// in. Both batches' signatures are still validated, so the client is fully
// authorized for the key either way.
func (c *Client) handleTrack(req *protocol.SubRefreshRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	_ = "STUB: not implemented"
	return nil
}

// Build a deduped key index up front. Used to (a) drive the optimistic
// limit check against DISTINCT-key count (matches the per-connection map
// shape) and (b) flatten items inside the trackHandler callback.

// index into req.Track / eventBatches — picks the per-batch ExpireAt.

// last batch wins for version + batchIdx

// Build a set of keys that will be immediately removed by Step 8 (inline
// untrack). Both limit checks below net these out so a replay that tracks
// N keys but untracks M of them only consumes N-M slots — not N.

// Optimistic limit check — counts DISTINCT new keys minus those that will
// be immediately removed via inline untrack. Also subtracts already-tracked
// keys appearing in inlineUntrackSet, since Step 8 removes them and frees
// slots. Re-checked under write lock.

// Call OnTrack handler (Centrifugo validates HMAC for every batch).

// Handlers that don't care about per-batch TTL may return an empty
// reply.Batches — treat that as "no expiry to record" for every batch.
// A non-empty Batches slice of the wrong length is a programmer error.

// Build per-call helper slices from the deduped flat index.

// Get or create keyed channel state.

// Step 1: Register in SharedPollManager FIRST (before any per-connection
// state is written). This ensures per-connection state never points to
// keys the server isn't tracking — fixing the state-divergence bug
// where a failed broker subscribe left phantom keys in trackedKeys.
// Do NOT addSubscriber yet — client must not receive broadcasts before response.
// Classification:
//   cold: new to server → auto-poll (backend call) after addSubscriber.
//   warm: existing key, client needs data (version=0 or stale version) →
//         direct delivery from cache if KeepLatestData, else notify +
//         needsBroadcast for near-immediate backend poll.
//   (none): existing key, client up to date → no action.
//
// trackKeys takes a pendingHubJoin reservation for each tracked key
// that the returned releaseTrackReservation closure MUST drop — either
// after addSubscribers (Step 5) on success, or in the rollback path
// below. Without that release, a concurrent client's rollback would
// orphan our caller in the hub.

// Step 2: Commit per-connection state under the write lock with a
// final limit re-check. This re-check is the authoritative gate —
// concurrent track calls that passed the optimistic RLock check at the
// top all converge here and only the first to fit wins. On failure we
// roll back the server-side track from Step 1.

// Re-tracking an existing key is a version update, not a new slot —
// only count new keys, and exclude those being inline-untracked. Also
// subtract already-tracked keys that will be removed by Step 8, since
// they free up slots.

// Roll back the server-side track from Step 1. Release the
// reservation BEFORE calling untrack: release decrements
// pendingHubJoin and, if no concurrent caller still holds it
// and the hub has no subscribers, deletes the entry itself.
// The subsequent untrack covers the case where another caller
// joined the hub between trackKeys and now — release leaves
// the entry alive (pending or hub.count > 0) and untrack is a
// no-op for keys the caller didn't own.

// Commit: store client-provided versions + per-batch expireAt in per-connection state.

// Update fast-path hint for track expiry checks.

// Step 2: Collect cached data for items where server has newer version.

// Step 3: Update per-connection versions for cached items to prevent
// duplicate delivery via subsequent broadcasts.
// Capture (keyState, prevVersion, prevDeltaReady) so we can roll back
// if the response encode fails below — without rollback the connection
// would mark cached items as delivered while the SDK never received them.

// Step 4: Build and write response (enqueued before any broadcasts).
// For type=1 (track) the response carries the MIN TTL across all
// batches in the request — the SDK schedules its consolidating
// refresh at the earliest deadline received across all responses
// (single global timer, no per-entry expiry tracking needed).

// Roll back per-connection version updates from Step 3 — the SDK
// never received the reply, so we must not pretend it has the
// cached versions. Without this, the next live broadcast at the
// same version is filtered out and the client misses a publication.

// addSubscribers has not run on this path, so drop the trackKeys
// reservation here — otherwise pendingHubJoin stays >0 and the
// itemIndex entries leak forever (untrack refuses to delete while
// pending>0). Encode failure is not reachable in current code,
// but the release is a one-line guard against future regressions.

// Step 5: NOW register in hub — client starts receiving broadcasts.
// Response is already enqueued, so broadcasts are ordered after it.
// keyedWritePublication checks pubVersion <= keyState.version, so cached
// items won't be re-delivered.
//
// keyedManager.addSubscribers performs "ensure-state-then-add"
// atomically under the manager's lock. This is required because a
// concurrent finalizeShutdown of an older sharedPollChannelState may
// race here: a non-atomic getOrCreateChannel + addSubscriber
// sequence could end up with the client subscribed to a hub that
// the finalizeShutdown then deletes from the manager, leaving the
// client orphaned from future broadcasts (which look up via
// getHub).

// Release the pendingHubJoin reservation now that we're in the hub.
// hub.subscriberCount(key) is now >= 1 for each key we tracked, so
// release just decrements the counter — no entries are deleted.

// Compute warm key delivery plan AFTER addSubscriber. KeepLatestData →
// direct delivery from cache (zero backend calls). Otherwise → deferred
// via notify + needsBroadcast (one backend call per key per reconnect
// wave).
//
// Snapshotting after addSubscriber closes a race: if a publish lands
// between the snapshot and addSubscriber, the broadcast goes only to
// existing subscribers (not us yet), so we would deliver a stale
// snapshot and the version filter would suppress later same-version
// broadcasts — silently pinning the client to a stale value until the
// next entry update. With the snapshot taken after addSubscriber, any
// concurrent broadcast reaches us via the hub and advances the per-
// connection version; our direct-delivery call then no-ops, leaving
// client and server in sync.

// Step 5.5: Direct delivery for warm keys with cached data.
// Uses internal version for per-connection dedup — keyedWritePublication
// updates keyState.version to the internal version, so subsequent broadcasts
// with the same version are skipped (no double delivery).

// Step 6: Auto-notify cold keys AFTER addSubscriber so the broadcast
// from the notified refresh can reach this client.

// Step 7: Deferred warm keys — flag + notify AFTER addSubscriber.
// markNeedsBroadcast sets the flag and sends at-most-one notify per
// key, triggering a backend call for near-immediate delivery. Keys
// already flagged by a concurrent client are skipped (deduplication).

// Step 8: Process inline untrack — keys that were part of the signed
// batch but have been locally untracked by the client since the
// signature was obtained. HMAC validation above covers the full batch;
// we remove these keys now so the client receives no broadcasts for them.
// Placed after addSubscriber (step 5) so hub state is coherent: we add
// then immediately remove, never leaving a gap where a key is absent.
// Only keys that were actually tracked are acted on — random keys sent
// by the client are silently ignored.

// c.mu is held across the hub.removeSubscriber loop so a
// concurrent handleTrack callback (with async TrackHandler this
// CAN run in parallel for the same client) cannot land its
// chanKeys insert + addSubscribers between our chanKeys delete
// and our hub remove. See cleanupKeyed for the wider rationale.
// Released before the untrackHandler callback so user code does
// not run under c.mu.

// handleUntrack processes SubRefreshRequest with type=2 (untrack).
//
// c.mu is held across the hub.removeSubscriber loop so a concurrent
// handleTrack callback completion cannot land its chanKeys insert +
// addSubscribers between our chanKeys delete and our hub remove. See
// cleanupKeyed for the orphan-in-hub race the wider lock guards against.
// Released before the untrackHandler callback so user code does not run
// under c.mu.
func (c *Client) handleUntrack(req *protocol.SubRefreshRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	_ = "STUB: not implemented"
	return nil
}

// testHookKeyedHubRemoveStart, if non-nil, is invoked from cleanupKeyed /
// handleUntrack / checkTrackExpiration / handleTrack Step 8 AFTER
// per-connection state has been mutated but BEFORE the hub.removeSubscriber
// loop runs. Used by tests to deterministically inject a concurrent re-track
// and reproduce the orphan-in-hub race. Production sets nil; overhead is one
// nil check per cleanup call.
var testHookKeyedHubRemoveStart func()

// cleanupKeyed removes all keyed tracking for a channel when a client
// unsubscribes or disconnects.
//
// c.mu is held across the hub.removeSubscriber loop so a concurrent
// handleTrack callback completion (which takes c.mu before chanKeys insert
// and then calls addSubscribers) cannot race between our chanKeys delete and
// our hub remove — that race would leave chanKeys claiming the key tracked
// while the hub no longer has this client, silently dropping all future
// broadcasts. Lock order c.mu → sharedPollManager.mu → s.mu → hub.mu is
// consistent with broadcast paths, which release hub.mu before taking c.mu.
func (c *Client) cleanupKeyed(channel string) { _ = "STUB: not implemented"; return }

// checkTrackExpiration silently removes tracked keys whose signatures have expired.
// No removal publications are sent — the client SDK handles expiry via its refresh flow.
func (c *Client) checkTrackExpiration(channel string, delay time.Duration) {
	_ = "STUB: not implemented"
	return
}

// Fast path: check per-channel hint under read lock.

// Nothing can be expired yet.

// Slow path: write lock, iterate keys, find and remove expired.

// Recompute accurate min after removing expired keys.

// Clean up hub and SharedPollManager (no removal publications sent).
// c.mu is held across the loop — see cleanupKeyed for the orphan-in-hub
// race rationale. Released before the log call so user logger code does
// not run under c.mu.

// keyedWritePublication writes a publication to a client for a keyed channel.
// It checks the per-connection version and only delivers if the publication
// version is newer. Updates per-connection version on delivery.
// Handles per-key delta readiness: first publication per key is always full,
// subsequent publications may use delta if available.
//
// Encoding runs outside c.mu so a slow encode does not block other client
// operations. The version re-check, the enqueue, and the per-connection
// state update all run under c.mu in a single critical section: this
// serializes concurrent broadcasts to the same client at the queue
// boundary, so anything that lands in the wire queue is the freshest
// version observed under the lock and any concurrent broadcast carrying an
// older-or-equal version is filtered out — preventing wire-order inversion
// where a slower-encoding older version would otherwise enqueue behind a
// faster-encoding newer one.
//
// State (keyState.version / keyState.deltaReady) is updated only after the
// publication is successfully enqueued. If encode fails, no-write conditions
// trigger, or enqueue returns an error, state stays unchanged — otherwise
// the client would silently miss the publication and subsequent broadcasts
// at lower/equal versions would be filtered out, leaving server and client
// out of sync (and, for delta channels, the next broadcast would send a
// delta against a base the client never received).
func (c *Client) keyedWritePublication(channel string, key string, pubVersion uint64, pub *protocol.Publication, prep preparedData) {
	_ = "STUB: not implemented"
	return
}

// Compute tentative delta decision against current state — do NOT mutate
// state yet. The final decision is re-checked under the lock in Phase 3
// because keyState.version can advance between phases.

// Encode outside the lock. Encoding can JSON-escape large payloads or
// build a delta frame — keeping it outside c.mu avoids stalling other
// operations on this client. Two concurrent broadcasts for the same key
// can therefore encode in parallel; the lock-protected enqueue below
// resolves their ordering.
//
// We always encode the FULL form so Phase 3 can fall back to it without
// re-encoding under the lock. The fallback is needed when the delta's
// base version (prep.keyedDeltaPrevVersion) doesn't match the client's
// current keyState.version — a sign that an intermediate broadcast was
// missed, so applying this patch would produce garbage.

// JSON+delta: must JSON-escape data so client stores bytes for delta base.

// Mirror writePublication's no-write conditions so we don't enqueue or
// advance state when the publication would be dropped (these return nil
// from writePublication, indistinguishable from a real write).

// Resolve batch config — user-supplied callback, must run outside c.mu.

// Trace before the critical section to avoid a c.mu.RLock acquisition
// inside traceOutPush while we're holding c.mu.Lock below. Tracing
// before enqueue is consistent with writePublication's existing pattern.

// Critical section: re-check version, decide delta-vs-full, enqueue,
// and update state under c.mu. Holding the lock through enqueue is what
// serializes concurrent broadcasts to this client and preserves causal
// version order on the wire. writeEncodedPushData uses messageWriter /
// perChannelWriter, each of which has its own internal mutex that does
// NOT touch c.mu — safe to call while holding it. (Going through
// writePublication here would deadlock on its c.mu.RLock for the
// deltaSub branch.)

// A concurrent broadcast already delivered this or a newer version.

// Decide delta vs full under the lock. The delta is only safe to send
// when the client's current keyState.version equals the version of the
// bytes the patch was computed against — otherwise the client doesn't
// hold the right base and applying the patch yields garbage. This
// condition is broken by concurrent broadcasts that update the server's
// entry between when this broadcast's prep was built and when we
// observe keyState here.

// Enqueue failed (queue closed/overflow); client is being torn down.
// Don't advance state — cleanupKeyed on close will drop it anyway.

// Advance state. For delta channels, a successful FULL delivery also
// flips deltaReady so subsequent broadcasts can use delta.

// keyedWriteRemoval writes a removal publication and removes the key from
// per-connection tracking.
func (c *Client) keyedWriteRemoval(channel string, key string, pub *protocol.Publication) {
	_ = "STUB: not implemented"
	return
}
