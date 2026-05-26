package centrifuge

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/centrifugal/centrifuge/internal/recovery"
	"github.com/centrifugal/centrifuge/internal/saferand"

	"github.com/centrifugal/protocol"
)

// Empty Replies/Pushes for pings.
var jsonPingReply = []byte(`{}`)
var protobufPingReply []byte
var jsonPingPush = []byte(`{}`)
var protobufPingPush []byte

var randSource *saferand.Rand

func init() {
	protobufPingReply, _ = protocol.DefaultProtobufReplyEncoder.Encode(&protocol.Reply{})
	protobufPingPush, _ = protocol.DefaultProtobufPushEncoder.Encode(&protocol.Push{})
	randSource = saferand.New(time.Now().UnixNano())
}

// clientEventHub allows dealing with client event handlers.
// All its methods are not goroutine-safe and supposed to be called
// once inside Node ConnectHandler.
type clientEventHub struct {
	aliveHandler         AliveHandler
	disconnectHandler    DisconnectHandler
	subscribeHandler     SubscribeHandler
	unsubscribeHandler   UnsubscribeHandler
	publishHandler       PublishHandler
	mapPublishHandler    MapPublishHandler
	mapRemoveHandler     MapRemoveHandler
	refreshHandler       RefreshHandler
	subRefreshHandler    SubRefreshHandler
	rpcHandler           RPCHandler
	messageHandler       MessageHandler
	presenceHandler      PresenceHandler
	presenceStatsHandler PresenceStatsHandler
	historyHandler       HistoryHandler
	stateSnapshotHandler StateSnapshotHandler
	trackHandler         TrackHandler
	untrackHandler       UntrackHandler
}

// OnAlive allows setting AliveHandler.
// AliveHandler called periodically for active client connection.
func (c *Client) OnAlive(h AliveHandler) { _ = "STUB: not implemented"; return }

// OnRefresh allows setting RefreshHandler.
// RefreshHandler called when it's time to refresh expiring client connection.
func (c *Client) OnRefresh(h RefreshHandler) { _ = "STUB: not implemented"; return }

// OnDisconnect allows setting DisconnectHandler.
// DisconnectHandler called when client disconnected.
func (c *Client) OnDisconnect(h DisconnectHandler) { _ = "STUB: not implemented"; return }

// OnMessage allows setting MessageHandler.
// MessageHandler called when client sent asynchronous message.
func (c *Client) OnMessage(h MessageHandler) { _ = "STUB: not implemented"; return }

// OnRPC allows setting RPCHandler.
// RPCHandler will be executed on every incoming RPC call.
func (c *Client) OnRPC(h RPCHandler) { _ = "STUB: not implemented"; return }

// OnSubRefresh allows setting SubRefreshHandler.
// SubRefreshHandler called when it's time to refresh client subscription.
func (c *Client) OnSubRefresh(h SubRefreshHandler) { _ = "STUB: not implemented"; return }

// OnSubscribe allows setting SubscribeHandler.
// SubscribeHandler called when client subscribes on a channel.
func (c *Client) OnSubscribe(h SubscribeHandler) { _ = "STUB: not implemented"; return }

// OnUnsubscribe allows setting UnsubscribeHandler.
// UnsubscribeHandler called when client unsubscribes from channel.
func (c *Client) OnUnsubscribe(h UnsubscribeHandler) { _ = "STUB: not implemented"; return }

// OnPublish allows setting PublishHandler.
// PublishHandler called when client publishes message into channel.
func (c *Client) OnPublish(h PublishHandler) { _ = "STUB: not implemented"; return }

// OnMapPublish allows setting MapPublishHandler.
// MapPublishHandler called when client publishes into map channel.
func (c *Client) OnMapPublish(h MapPublishHandler) { _ = "STUB: not implemented"; return }

// OnMapRemove allows setting MapRemoveHandler.
// MapRemoveHandler called when client wants to remove a key from map channel.
func (c *Client) OnMapRemove(h MapRemoveHandler) { _ = "STUB: not implemented"; return }

// OnPresence allows setting PresenceHandler.
// PresenceHandler called when Presence request from client received.
// At this moment you can only return a custom error or disconnect client.
func (c *Client) OnPresence(h PresenceHandler) { _ = "STUB: not implemented"; return }

// OnPresenceStats allows settings PresenceStatsHandler.
// PresenceStatsHandler called when Presence Stats request from client received.
// At this moment you can only return a custom error or disconnect client.
func (c *Client) OnPresenceStats(h PresenceStatsHandler) { _ = "STUB: not implemented"; return }

// OnHistory allows settings HistoryHandler.
// HistoryHandler called when History request from client received.
// At this moment you can only return a custom error or disconnect client.
func (c *Client) OnHistory(h HistoryHandler) { _ = "STUB: not implemented"; return }

// OnTrack allows setting TrackHandler.
// TrackHandler called when client sends a track request on a keyed channel.
func (c *Client) OnTrack(h TrackHandler) { _ = "STUB: not implemented"; return }

// OnUntrack allows setting UntrackHandler.
// UntrackHandler called when client untracks keys on a keyed channel.
func (c *Client) OnUntrack(h UntrackHandler) { _ = "STUB: not implemented"; return }

const (
	// flagSubscribed will be set upon successful Subscription to a channel.
	// Until that moment channel exists in client Channels map only to track
	// duplicate subscription requests.
	flagSubscribed uint16 = 1 << iota
	flagEmitPresence
	flagEmitJoinLeave
	flagPushJoinLeave
	flagPositioning
	flagServerSide
	flagClientSideRefresh
	flagDeltaAllowed
	flagMap                  // Channel uses map subscription (presence via MapBroker)
	flagMapPresence          // Presence subscription (:clients or :users suffix)
	flagMapClientPresence    // Emit to {channel}:clients, key=clientId, full ClientInfo
	flagMapUserPresence      // Emit to {channel}:users, key=userId, no info
	flagCleanupOnUnsubscribe // Clean up keys by client_id when subscription ends
	flagKeyed                // Channel uses keyed subscription (shared poll track/untrack)
)

// ChannelContext contains extra context for channel connection subscribed to.
// Note: this struct is aligned to consume less memory.
type ChannelContext struct {
	subscribingCh            chan struct{}
	info                     []byte
	streamPosition           StreamPosition
	expireAt                 int64
	positionCheckTime        int64
	metaTTLSeconds           int64
	flags                    uint16
	mapClientPresenceChannel string
	mapUserPresenceChannel   string
	// Source is a source of subscription application can set in SubscribeHandler.
	Source uint8
}

func channelHasFlag(flags, flag uint16) bool { _ = "STUB: not implemented"; return false }

type timerOp uint8

const (
	timerOpStale    timerOp = 1
	timerOpPresence timerOp = 2
	timerOpExpire   timerOp = 3
	timerOpPing     timerOp = 4
	timerOpPong     timerOp = 5
)

type status uint8

const (
	statusConnecting status = 1
	statusConnected  status = 2
	statusClosed     status = 3
)

// ConnectRequest can be used in a unidirectional connection case to
// pass initial connection information from a client-side.
type ConnectRequest struct {
	// Token is an optional token from a client.
	Token string
	// Data is an optional custom data from a client.
	Data []byte
	// Name of a client.
	Name string
	// Version of a client.
	Version string
	// Subs is a map with channel subscription state (for recovery on connect).
	Subs map[string]SubscribeRequest
	// Headers represent headers which may be used for headers emulation feature.
	Headers map[string]string
}

// SubscribeRequest contains state of subscription to a channel.
type SubscribeRequest struct {
	// Recover enables publication recovery for a channel.
	Recover bool
	// Epoch last seen by a client.
	Epoch string
	// Offset last seen by a client.
	Offset uint64
}

func (r *ConnectRequest) toProto() *protocol.ConnectRequest { _ = "STUB: not implemented"; return nil }

// Client represents client connection to server.
type Client struct {
	mu                     sync.RWMutex
	connectMu              sync.Mutex // allows syncing connect with disconnect.
	presenceMu             sync.Mutex // allows syncing presence routine with client closing.
	ctx                    context.Context
	transport              Transport
	node                   *Node
	exp                    int64
	channels               map[string]ChannelContext
	messageWriter          *writer
	perChannelWriter       *perChannelWriter
	pubSubSync             *recovery.PubSubSync
	uid                    string
	session                string
	user                   string
	info                   []byte
	storage                map[string]any
	storageMu              sync.Mutex
	metricName             string // Make a unique.Handle.
	metricVersion          string // Make a unique.Handle.
	labels                 map[string]string
	labelCombinationCached atomic.Pointer[clientLabelCombination] // Cached pointer to shared label combination
	authenticated          bool
	clientSideRefresh      bool
	status                 status
	timerOp                timerOp
	nextPresence           int64
	nextExpire             int64
	nextPing               int64
	nextPong               int64
	lastSeen               int64
	lastPing               int64
	pingInterval           time.Duration
	pongTimeout            time.Duration
	eventHub               *clientEventHub
	timer                  *time.Timer
	timerCanceler          TimerCanceler // TimerCanceler if TimerScheduler is used.
	startWriterOnce        sync.Once
	pingPongLatency        atomic.Int64
	connectedAtMS          int64
	replyWithoutQueue      bool
	unusable               bool

	// mapSubscribing tracks map subscriptions that are still loading (not yet live).
	mapSubscribing map[string]*mapSubscribeState
	// mapPaginationLocks tracks channels currently being paginated to prevent concurrent pagination.
	mapPaginationLocks map[string]struct{}

	// keyed holds per-connection keyed subscription state (shared poll).
	// nil until first keyed subscribe.
	keyed *keyedState
}

// ClientCloseFunc must be called on Transport handler close to clean up Client.
type ClientCloseFunc func() error

// NewClient initializes new Client.
func NewClient(ctx context.Context, n *Node, t Transport) (*Client, ClientCloseFunc, error) {
	_ = "STUB: not implemented"
	return nil, *new(ClientCloseFunc), nil
}

var defaultUniErrorCodeToDisconnect = map[uint32]Disconnect{
	ErrorExpired.Code:          DisconnectExpired,
	ErrorTokenExpired.Code:     DisconnectExpired,
	ErrorTooManyRequests.Code:  DisconnectTooManyRequests,
	ErrorPermissionDenied.Code: DisconnectPermissionDenied,
}

func (c *Client) extractUnidirectionalDisconnect(err error) Disconnect {
	_ = "STUB: not implemented"
	return *new(Disconnect)
}

// Connect supposed to be called only from a unidirectional transport layer
// to pass initial information about connection and thus initiate Node.OnConnecting
// event. Bidirectional transport initiate connecting workflow automatically
// since client passes Connect command upon successful connection establishment
// with a server. If there is an error during connect method processing Centrifuge
// extracts Disconnect from it and closes the connection with that Disconnect message.
func (c *Client) Connect(req ConnectRequest) { _ = "STUB: not implemented"; return }

// ConnectNoErrorToDisconnect is the same as Client.Connect but does not try to extract
// Disconnect code from the error returned by the connect logic, instead it just returns
// the error to the caller. This error must be handled by the caller on the Transport level,
// and the connection must be closed on Transport level upon receiving an error.
func (c *Client) ConnectNoErrorToDisconnect(req ConnectRequest) error {
	_ = "STUB: not implemented"
	return nil
}

// ProtocolConnect accepts protocol.ConnectRequest directly. It adds dependency to protocol package,
// so prefer using Connect or ConnectNoErrorToDisconnect methods until necessary.
func (c *Client) ProtocolConnect(req *protocol.ConnectRequest) {
	_ = "STUB: not implemented"
	// unidirectionalConnect never returns errors when errorToDisconnect is true.
	return
}

// ProtocolConnectNoErrorToDisconnect accepts protocol.ConnectRequest directly. It adds dependency to
// protocol package, so prefer ConnectNoErrorToDisconnect methods until necessary.
func (c *Client) ProtocolConnectNoErrorToDisconnect(req *protocol.ConnectRequest) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *Client) getDisconnectPushReply(d Disconnect) ([]byte, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func hasFlag(flags, flag uint64) bool { _ = "STUB: not implemented"; return false }

func (c *Client) issueCommandReadEvent(cmd *protocol.Command, size int) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *Client) issueCommandProcessedEvent(event CommandProcessedEvent) {
	_ = "STUB: not implemented"
	return
}

func (c *Client) unidirectionalConnect(connectRequest *protocol.ConnectRequest, connectCmdSize int, errorToDisconnect bool) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *Client) onTimerOp() { _ = "STUB: not implemented"; return }

// Lock must be held outside.
func (c *Client) scheduleNextTimer() { _ = "STUB: not implemented"; return }

// Cancel any existing timer.

// Lock must be held outside.
func (c *Client) stopTimer() { _ = "STUB: not implemented"; return }

func getPingData(uni bool, protoType ProtocolType) []byte { _ = "STUB: not implemented"; return nil }

func (c *Client) sendPing() { _ = "STUB: not implemented"; return }

// TODO: can/should we write pings directly without going through messageWriter?
//err := c.messageWriter.config.WriteFn(queue.Item{
//	Data:      getPingData(unidirectional, c.transport.Protocol()),
//	FrameType: protocol.FrameTypeServerPing,
//})
//if err != nil {
//	go func() { _ = c.close(DisconnectWriteError) }()
//	return
//}

func (c *Client) checkPong() { _ = "STUB: not implemented"; return }

// Lock must be held outside.
func (c *Client) addPingUpdate(isFirst bool, scheduleNext bool) { _ = "STUB: not implemented"; return }

// Send first ping in random interval between PingInterval/2 and PingInterval to
// spread ping-pongs in time (useful when many connections reconnect
// almost immediately).

// Lock must be held outside.
func (c *Client) addPresenceUpdate(isFirst bool, scheduleNext bool) {
	_ = "STUB: not implemented"
	return
}

// Spread in time first presence update.

// Lock must be held outside.
func (c *Client) addExpireUpdate(after time.Duration, scheduleNext bool) {
	_ = "STUB: not implemented"
	return
}

// closeStale closes connection if it's not authenticated yet, or it's
// unusable but still not closed. At moment used to close client connections
// which have not sent valid connect command in a reasonable time interval after
// establishing connection with a server.
func (c *Client) closeStale() { _ = "STUB: not implemented"; return }

func (c *Client) writeEncodedPushData(data []byte, ch string, key string, frameType protocol.FrameType, batchConfig ChannelBatchConfig) error {
	_ = "STUB: not implemented"
	return nil
}

// Per channel writer helps to batch messages on the channel level working as
// an intermediary buffer before client's connection writer.

// close in goroutine to not block message broadcast.

// publishJoinAndPresence publishes join notification and sets up map presence
// for a channel after subscribe. Must be called with non-nil clientInfo.
func (c *Client) publishJoinAndPresence(channel string, chCtx ChannelContext, clientInfo *ClientInfo) {
	_ = "STUB: not implemented"
	return
}

// updateChannelPresence updates client presence info for channel so it
// won't expire until client disconnect.
func (c *Client) updateChannelPresence(ch string, chCtx ChannelContext) error {
	_ = "STUB: not implemented"
	// Check if any presence is enabled for this channel.
	return nil
}

// Context returns client Context. This context will be canceled
// as soon as client connection closes.
func (c *Client) Context() context.Context { _ = "STUB: not implemented"; return *new(context.Context) }

func (c *Client) checkSubscriptionExpiration(channel string, channelContext ChannelContext, delay time.Duration, resultCB func(bool)) {
	_ = "STUB: not implemented"
	return
}

// Subscription expired.

// The only way subscription could be refreshed in this case is via
// SUB_REFRESH command sent from client but looks like that command
// with new refreshed token have not been received in configured window.

// Give subscription a chance to be refreshed via SubRefreshHandler.

// updatePresence used for various periodic actions we need to do with client connections.
func (c *Client) updatePresence() { _ = "STUB: not implemented"; return }

// No need to proceed after close.

func (c *Client) checkPosition(checkDelay time.Duration, ch string, chCtx ChannelContext) bool {
	_ = "STUB: not implemented"
	return false
}

// Check later.

// ID returns unique client connection id.
func (c *Client) ID() string {
	_ = "STUB: not implemented"

	// sessionID returns unique client session id. Session ID is not shared to other
	// connections in any way.
	return ""
}

func (c *Client) sessionID() string {
	_ = "STUB: not implemented"

	// UserID returns user id associated with client connection.
	return ""
}

func (c *Client) UserID() string {
	_ = "STUB: not implemented"

	// Info returns connection info.
	return ""
}

func (c *Client) Info() []byte { _ = "STUB: not implemented"; return nil }

// ConnectedAtMS returns timestamp in milliseconds when client connected.
func (c *Client) ConnectedAtMS() int64 { _ = "STUB: not implemented"; return 0 }

// LatestPingPongLatency returns latest ping-pong latency duration. It may be not
// available if no ping-pong messages were exchanged yet, or in case of unidirectional
// transport. In that case second return value will be false.
func (c *Client) LatestPingPongLatency() (time.Duration, bool) {
	_ = "STUB: not implemented"
	return *new(time.Duration), false
}

// If ping-pong latency is negative then it means that we have not sent
// any ping yet, and thus we do not have any latency info. Or in case of
// unidirectional connection we do not have this info also.

// Transport returns client connection transport information.
func (c *Client) Transport() TransportInfo {
	_ = "STUB: not implemented"

	// Channels returns a slice of channels client connection currently subscribed to.
	return *new(TransportInfo)
}

func (c *Client) Channels() []string { _ = "STUB: not implemented"; return nil }

// ChannelsWithContext returns a map of channels client connection currently subscribed to
// with a ChannelContext.
func (c *Client) ChannelsWithContext() map[string]ChannelContext {
	_ = "STUB: not implemented"
	return nil
}

// IsSubscribed returns true if client subscribed to a channel.
func (c *Client) IsSubscribed(ch string) bool { _ = "STUB: not implemented"; return false }

// Labels returns custom labels attached to the client connection.
// These labels are set via ConnectReply.Labels. The returned map must not be modified
// by the caller - use it for reading only. Labels are set once during connect and never
// change.
func (c *Client) Labels() map[string]string {
	_ = "STUB: not implemented"

	// Send data to client. This sends an asynchronous message – data will be
	// just written to connection. on client side this message can be handled
	// with Message handler.
	return nil
}

func (c *Client) Send(data []byte) error { _ = "STUB: not implemented"; return nil }

func (c *Client) encodeReply(reply *protocol.Reply) ([]byte, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (c *Client) getSendPushReply(data []byte) ([]byte, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Unsubscribe allows unsubscribing client from channel.
func (c *Client) Unsubscribe(ch string, unsubscribe ...Unsubscribe) {
	_ = "STUB: not implemented"
	return
}

func (c *Client) sendUnsubscribe(ch string, unsub Unsubscribe) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *Client) getUnsubscribePushReply(ch string, unsub Unsubscribe) ([]byte, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Disconnect client connection with specific disconnect code and reason.
// If zero args or nil passed then DisconnectForceNoReconnect is used.
//
// This method internally creates a new goroutine at the moment to do
// closing stuff. An extra goroutine is required to solve disconnect
// and alive callback ordering/sync problems. Will be a noop if client
// already closed. As this method runs a separate goroutine client
// connection will be closed eventually (i.e. not immediately).
func (c *Client) Disconnect(disconnect ...Disconnect) { _ = "STUB: not implemented"; return }

func (c *Client) close(disconnect Disconnect) error { _ = "STUB: not implemented"; return nil }

// Unsubscribe from all channels (handles both normal and map subscriptions).

// Clean up any in-progress map subscriptions that aren't in channels yet.

// close writer and send messages remaining in writer queue if any.

func (c *Client) traceInCmd(cmd *protocol.Command) { _ = "STUB: not implemented"; return }

func (c *Client) traceOutReply(rep *protocol.Reply) { _ = "STUB: not implemented"; return }

func (c *Client) traceOutPush(push *protocol.Push) { _ = "STUB: not implemented"; return }

// Lock must be held outside.
func (c *Client) clientInfo(ch string) *ClientInfo { _ = "STUB: not implemented"; return nil }

const redacted = "<REDACTED>"

func redactCommand(cmd *protocol.Command) *protocol.Command { _ = "STUB: not implemented"; return nil }

// HandleCommand processes a single protocol.Command. Supposed to be called only
// from a transport connection reader.
func (c *Client) HandleCommand(cmd *protocol.Command, cmdProtocolSize int) bool {
	_ = "STUB: not implemented"
	return false
}

// isPong is a helper method to check whether the command from the client
// is a pong to server ping. It's actually an empty command.
func isPong(cmd *protocol.Command) bool { _ = "STUB: not implemented"; return false }

func (c *Client) handleCommandFinished(cmd *protocol.Command, frameType protocol.FrameType, err error, reply *protocol.Reply, started time.Time, ch string) {
	_ = "STUB: not implemented"
	return
}

func (c *Client) handleCommandDispatchError(ch string, cmd *protocol.Command, frameType protocol.FrameType, err error, started time.Time) (*Disconnect, bool) {
	_ = "STUB: not implemented"
	return nil, false
}

func (c *Client) dispatchCommand(cmd *protocol.Command, cmdSize int) (*Disconnect, bool) {
	_ = "STUB: not implemented"
	return nil, false
}

// No ping was issued, unnecessary pong.

// upon receiving pong we change a sign of lastPing value. This way we can handle
// unnecessary pongs sent by the client and still use lastPing value in Client.checkPong.

// Now as pong processed make sure that command has id > 0 (except Send).

func (c *Client) writeEncodedCommandReply(ch string, frameType protocol.FrameType, cmd *protocol.Command, rep *protocol.Reply, rw *replyWriter) {
	_ = "STUB: not implemented"
	return
}

// Note: avoid adding *Reply to item since it's pooled.

func (c *Client) checkExpired() { _ = "STUB: not implemented"; return }

// Connection was successfully refreshed.

// A protection against too long connection and subscription TTL which is likely
// a bug and may result into overflow of time.Duration type usage (~292 years max in Go).
// It's possible to go without expiration at all rather than having longer TTL.
const maxTTLSeconds = 365 * 24 * 3600

func (c *Client) expire() { _ = "STUB: not implemented"; return }

func (c *Client) handleConnect(req *protocol.ConnectRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *Client) triggerConnect() { _ = "STUB: not implemented"; return }

func (c *Client) scheduleOnConnectTimers() {
	_ = "STUB: not implemented"
	// Make presence and refresh handlers always run after client connect event.
	return
}

// Only schedule next timer once here after setting required points in time for ops.

func (c *Client) Refresh(opts ...RefreshOption) error { _ = "STUB: not implemented"; return nil }

// connection check enabled

// connection refreshed, update client timestamp and set new expiration timeout

func (c *Client) getRefreshPushReply(res *protocol.Refresh) ([]byte, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (c *Client) releaseRefreshCommandReply(reply *protocol.Reply) {
	_ = "STUB: not implemented"
	return
}

func (c *Client) getRefreshCommandReply(res *protocol.RefreshResult) (*protocol.Reply, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (c *Client) handleRefresh(req *protocol.RefreshRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	_ = "STUB: not implemented"
	return nil
}

// Client not supposed to send refresh command in case of server-side refresh mechanism.

// connection check enabled

// connection refreshed, update client timestamp and set new expiration timeout

// onSubscribeError cleans up a channel from client channels if an error during subscribe happened.
// Channel kept in a map during subscribe request to check for duplicate subscription attempts.
func (c *Client) onSubscribeError(channel string) { _ = "STUB: not implemented"; return }

func (c *Client) handleSubscribe(req *protocol.SubscribeRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	_ = "STUB: not implemented"
	return nil
}

// Shared poll check — must be before map routing.

// Route map subscription types (map, client presence, user presence) to map handler.

// Regular subscription flow.

// Synchronous (NOT `go`) so publishJoin reaches the broker before
// this subscribe callback returns. If we spawned a goroutine, a
// quick client disconnect could fire the synchronous publishLeave
// (from close → unsubscribe loop) ahead of our Join, leaving
// observers with [leave, join] on the wire.

func (c *Client) getSubscribedChannelContext(channel string) (ChannelContext, bool) {
	_ = "STUB: not implemented"
	return *new(ChannelContext), false
}

func (c *Client) handleSubRefresh(req *protocol.SubRefreshRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	_ = "STUB: not implemented"
	return nil
}

// Must be subscribed to refresh subscription.

// Route by type for keyed channels (shared poll track/untrack).

// Client not supposed to send sub refresh command in case of server-side
// subscription refresh mechanism.

func (c *Client) releaseSubRefreshCommandReply(reply *protocol.Reply) {
	_ = "STUB: not implemented"
	return
}

func (c *Client) getSubRefreshCommandReply(res *protocol.SubRefreshResult) (*protocol.Reply, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (c *Client) handleUnsubscribe(req *protocol.UnsubscribeRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *Client) releaseUnsubscribeCommandReply(reply *protocol.Reply) {
	_ = "STUB: not implemented"
	return
}

func (c *Client) getUnsubscribeCommandReply(res *protocol.UnsubscribeResult) (*protocol.Reply, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (c *Client) handlePublish(req *protocol.PublishRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *Client) handleMapPublish(req *protocol.PublishRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	_ = "STUB: not implemented"
	return nil
}

// Handler must return the key explicitly. There is no fallback to the
// client-supplied event.Key — for namespaces where the client supplies
// the key, the handler should pass it through (reply.Key = event.Key).
// For namespaces with server-driven keying (e.g. client_id / user_id),
// the handler resolves the key and returns it; if the resolution yields
// an empty key (e.g. user_id for an anonymous user), the handler should
// reject the publish itself with an explicit error.

func (c *Client) handleMapRemove(req *protocol.PublishRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	_ = "STUB: not implemented"
	return nil
}

// Handler must return the key explicitly. There is no fallback to the
// client-supplied event.Key — see handleMapPublish for the reasoning.

func (c *Client) releasePublishCommandReply(reply *protocol.Reply) {
	_ = "STUB: not implemented"
	return
}

func (c *Client) getPublishCommandReply(res *protocol.PublishResult) (*protocol.Reply, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (c *Client) handlePresence(req *protocol.PresenceRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *Client) releasePresenceCommandReply(reply *protocol.Reply) {
	_ = "STUB: not implemented"
	return
}

func (c *Client) getPresenceCommandReply(res *protocol.PresenceResult) (*protocol.Reply, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (c *Client) handlePresenceStats(req *protocol.PresenceStatsRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *Client) releasePresenceStatsCommandReply(reply *protocol.Reply) {
	_ = "STUB: not implemented"
	return
}

func (c *Client) getPresenceStatsCommandReply(res *protocol.PresenceStatsResult) (*protocol.Reply, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (c *Client) handleHistory(req *protocol.HistoryRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *Client) releaseHistoryCommandReply(reply *protocol.Reply) {
	_ = "STUB: not implemented"
	return
}

func (c *Client) getHistoryCommandReply(res *protocol.HistoryResult) (*protocol.Reply, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

var emptyReply = &protocol.Reply{}

func (c *Client) handlePing(_ *protocol.Command, _ time.Time, _ *replyWriter) error {
	_ = "STUB: not implemented"
	// Ping not supported by protocol v2 at the moment. Supporting it requires adding
	// ping method to SDKs first. But nobody asked yet.
	return nil
}

func (c *Client) writeError(ch string, frameType protocol.FrameType, cmd *protocol.Command, errorReply *protocol.Reply, rw *replyWriter) {
	_ = "STUB: not implemented"
	return
}

func (c *Client) writeDisconnectOrErrorFlush(ch string, frameType protocol.FrameType, cmd *protocol.Command, err error, started time.Time, rw *replyWriter) {
	_ = "STUB: not implemented"
	return
}

type replyWriter struct {
	write func(*protocol.Reply)
}

func (c *Client) handleRPC(req *protocol.RPCRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *Client) releaseRPCCommandReply(r *protocol.Reply) { _ = "STUB: not implemented"; return }

func (c *Client) getRPCCommandReply(res *protocol.RPCResult) (*protocol.Reply, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (c *Client) handleSend(req *protocol.SendRequest, cmd *protocol.Command, started time.Time) error {
	_ = "STUB: not implemented"
	// Send handler is a bit special since it's a one way command: client does not expect any reply.
	return nil
}

// Return DisconnectNotAvailable here since otherwise client won't even know
// server does not have asynchronous message handler set.

func (c *Client) unlockServerSideSubscriptions(subCtxMap map[string]subscribeContext) {
	_ = "STUB: not implemented"
	return
}

// isInTest may be true during Centrifuge test run. We use it to inject code required to
// cover various edge case scenarios.
var isInTest = false

const (
	testChannelRedisClientSubscribeRecoveryDeadlock1 = "TestRedisClientSubscribeRecoveryDeadlock1"
	testChannelRedisClientSubscribeRecoveryDeadlock2 = "TestRedisClientSubscribeRecoveryDeadlock2"
)

// connectCmd handles connect command from client - client must send connect
// command immediately after establishing connection with server.
func (c *Client) connectCmd(req *protocol.ConnectRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	_ = "STUB: not implemented"
	return nil
}

// Try to find Credentials in context.

// Client successfully connected.

// Precompute and cache the label combination pointer for zero-allocation metrics hot path
// Do this before addClient so metrics recorded by addClient can use the cached combination

// Server will do refresh itself.

// Only for tests.

// Synchronous: see comment on the same call in handleSubscribe.
// A spawned goroutine racing a disconnect's publishLeave can land
// Join after Leave on the wire.

func (c *Client) getConnectPushReply(res *protocol.ConnectResult) (*protocol.Reply, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (c *Client) startWriter(batchDelay time.Duration, maxMessagesInFrame int, queueInitialCap int, queueShrinkDelay time.Duration, writeWithTimer bool) {
	_ = "STUB: not implemented"
	return
}

// Batch metric updates - accumulate counts locally first

// Accumulate metrics locally

// Update metrics once per unique label combination
// Use cached client label combination for zero-allocation hot path
// The combination is pre-cached during connect, so just load it directly

// Batch update - add count and size together

// Timer-driven mode: non-blocking, triggered by enqueue operations.

// Traditional mode: dedicated goroutine for immediate writes.

func (c *Client) releaseConnectCommandReply(reply *protocol.Reply) {
	_ = "STUB: not implemented"
	return
}

func (c *Client) getConnectCommandReply(res *protocol.ConnectResult) (*protocol.Reply, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Subscribe client to a channel.
func (c *Client) Subscribe(channel string, opts ...SubscribeOption) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *Client) getSubscribePushReply(channel string, res *protocol.SubscribeResult) ([]byte, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (c *Client) validateSubscribeRequest(cmd *protocol.SubscribeRequest) (*Error, *Disconnect) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Check for map subscription continuation (pagination or live join).
// These requests should be allowed even if mapSubscribing entry exists.
// Type 1 = map data, Type 2 = clients presence, Type 3 = users presence.
// All map-based subscriptions (type >= 1) share the same validation logic.

// Allow continuation requests (cursor set or non-state phase).

// If already fully subscribed, reject.

// If already in map subscribing and this is an initial request, reject.

// New map subscription - check channel limit.

// TODO: it would be better to combine with normal flow, including having subscribingCh for map subs also.
// TODO: also, normal sub flow should look at len(c.mapSubscribing) also.

// Regular subscription validation.

// Put channel to a map to track duplicate subscriptions. This channel should
// be removed from a map upon an error during subscribe. Also initialize subscribingCh
// which is used to sync unsubscribe requests with inflight subscriptions (useful when
// subscribe is performed in a separate goroutine).

func errorDisconnectContext(replyError *Error, disconnect *Disconnect) subscribeContext {
	_ = "STUB: not implemented"
	return *new(subscribeContext)
}

type subscribeContext struct {
	result         *protocol.SubscribeResult
	clientInfo     *ClientInfo
	err            *Error
	disconnect     *Disconnect
	channelContext ChannelContext
}

func isStreamRecovered(
	historyResult HistoryResult, cmdOffset uint64, cmdEpoch string, tf *tagsFilter,
) ([]*protocol.Publication, bool) {
	_ = "STUB: not implemented"
	return nil, false
}

// Epochs do not match, cannot recover.

func isCacheRecovered(
	latestPub *Publication, recoveredPub *Publication, currentSP StreamPosition, cmdOffset uint64, cmdEpoch string,
) ([]*protocol.Publication, bool) {
	_ = "STUB: not implemented"
	return nil, false
}

// Check if client state matches current state.

const (
	subscriptionFlagChannelCompression = 1 << iota
	subscriptionFlagRejectUnrecovered
)

// subscribeCmd handles subscribe command - clients send this when subscribe
// on channel, if channel is private then we must validate provided sign here before
// actually subscribe client on channel. Optionally we can send missed messages to
// client if it provided last message id seen in channel.
func (c *Client) subscribeCmd(req *protocol.SubscribeRequest, reply SubscribeReply, cmd *protocol.Command, serverSide bool, started time.Time, rw *replyWriter) subscribeContext {
	_ = "STUB: not implemented"
	return *new(subscribeContext)
}

// Start syncing recovery and PUB/SUB.
// The important thing is to call StopBuffering for this channel
// after response with Publications written to connection.

// Client provided subscribe request with recover flag on. Try to recover missed
// publications automatically from history (we assume here that the history configured wisely).

// One more chance to recover in case we know cache was populated.

// Result contains stream position in case of ErrorUnrecoverablePosition
// during recovery.

// In RecoveryModeCache case client is only interested in last message. So if delta encoding is
// not used then we can only send the last publication.

// There can be a case when recovery returned a limited set of publications
// thus last publication offset will be smaller than history current offset.
// In this case res.Recovered will be false. So we take a maximum here.

// Some entries could be filtered, but it's normal so we put max seen offset as the latest.

// Only append recovered publications in case continuity in a channel can be achieved.

// Allow delta for the following real-time publications since recovery is successful
// and makeRecoveredPubsDeltaFossil already created publication with base data if required.

// In case of successful recovery attach stream offset from request to subscribe response.
// This simplifies client implementation as it doesn't need to distinguish between cases when
// subscribe response has recovered publications, or it has no recovered publications.
// Valid stream position will be then caught up upon processing publications.

// Append publications from subscribe reply (e.g., initial full state).

// Write subscription reply only if initiated by client.

// Will be called later in case of server side sub.

// Need to flush data from writer so subscription response is
// sent before any subscription publication.

// In case of server-side sub this will be done later by the caller.

// Move subscribingCh from existing channel context to the new one.

// Stop syncing recovery and PUB/SUB.
// In case of server side subscription we will do this later.

func (c *Client) makeRecoveredPubsDeltaFossil(recoveredPubs []*protocol.Publication) []*protocol.Publication {
	_ = "STUB: not implemented"
	return nil
}

// For JSON case we need to use JSON string (js) for data.

// Probably during recovery we should not make deltas? This is something to investigate, in
// RecoveryModeCache case this won't be used since there is only one publication max recovered.

// makeRecoveredMapPubsDeltaFossil is a per-key variant of makeRecoveredPubsDeltaFossil
// for map subscriptions. Live publications use per-key delta (delta from previous state
// value for the same key), so recovery publications must use the same strategy. The
// sequential delta used by makeRecoveredPubsDeltaFossil would produce wrong bases when
// publications for different keys are interleaved.
func (c *Client) makeRecoveredMapPubsDeltaFossil(recoveredPubs []*protocol.Publication) []*protocol.Publication {
	_ = "STUB: not implemented"
	return nil
}

// Removals are not delta-encoded (matches live behavior).

//nolint:gosec // i is from range recoveredPubs

// First occurrence of this key — send full data.

// Subsequent occurrence — compute per-key delta.

func copyMapPubWithData(pub *protocol.Publication, data []byte, delta bool) *protocol.Publication {
	_ = "STUB: not implemented"
	return nil
}

func (c *Client) releaseSubscribeCommandReply(reply *protocol.Reply) {
	_ = "STUB: not implemented"
	return
}

func (c *Client) getSubscribeCommandReply(res *protocol.SubscribeResult) (*protocol.Reply, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (c *Client) handleInsufficientState(ch string, serverSide bool) {
	_ = "STUB: not implemented"
	return
}

func (c *Client) isAsyncUnsubscribe(serverSide bool) bool { _ = "STUB: not implemented"; return false }

func (c *Client) handleInsufficientStateDisconnect() { _ = "STUB: not implemented"; return }

func (c *Client) handleAsyncUnsubscribe(ch string, unsub Unsubscribe) {
	_ = "STUB: not implemented"
	return
}

func (c *Client) writePublicationUpdatePosition(
	ch string, pub *protocol.Publication, prep preparedData, sp StreamPosition, maxLagExceeded bool,
	batchConfig ChannelBatchConfig,
) error {
	_ = "STUB: not implemented"
	return nil
}

// Publication with Offset, but client does not use positioning.

// This is a special pub to trigger insufficient state. Noop in non-positioning case.

// For non-positioning case, if publication should be filtered, skip it

// PUB/SUB lag is too big.
// We can introduce an option to mark connection with insufficient state flag instead
// of disconnecting it immediately. In that case connection will eventually reconnect
// due to periodic sync. While connection channel is in the insufficient state we must
// skip publications coming to it. This mode may be useful to spread the resubscribe load.

// Tell client about insufficient state, can reconnect/resubscribe to recover the state.

// Channel subscribed when no data existed (empty epoch) — e.g. due to
// a lagging read replica that didn't have the meta row yet. The first
// publication carries the real epoch — adopt it. This avoids a needless
// re-subscribe and is safe: the only way to have epoch="" is "no data
// existed at subscribe time", so there is no stale state to protect.
// Applies to both stream and map subscriptions.

// Real epoch mismatch (e.g. after Clear) — insufficient state.

// Missed message detected.
// We can introduce an option to mark connection with insufficient state flag instead
// of disconnecting it immediately. In that case connection will eventually reconnect
// due to periodic sync. While connection channel is in the insufficient state we must
// skip publications coming to it. This mode may be useful to spread the resubscribe load.

// Tell client about insufficient state, can reconnect/resubscribe to recover the state.

// Epoch is correct, but due to the lag in PUB/SUB processing we received non-actual update
// here. Safe to just skip for the subscriber.

// If publication should be filtered, skip sending it but keep the offset updated

func (c *Client) writePublicationNoDelta(ch string, pub *protocol.Publication, data []byte, sp StreamPosition, batchConfig ChannelBatchConfig) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *Client) writePublication(ch string, pub *protocol.Publication, prep preparedData, sp StreamPosition, maxLagExceeded bool, batchConfig ChannelBatchConfig) error {
	_ = "STUB: not implemented"
	return nil
}

// For publications without offset, if filtering is needed, we can skip them
// early since there's no position tracking to maintain.

// For this path (no Offset) delta may come from channel medium layer, so that we can use it
// here if allowed for the connection.

// Set flagDeltaAllowed so subsequent pubs use delta.

func (c *Client) writeJoin(ch string, join *protocol.Join, data []byte, batchConfig ChannelBatchConfig) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *Client) writeLeave(ch string, leave *protocol.Leave, data []byte, batchConfig ChannelBatchConfig) error {
	_ = "STUB: not implemented"
	return nil
}

// Lock must be held outside.
func (c *Client) unsubscribe(channel string, unsubscribe Unsubscribe, disconnect *Disconnect) error {
	_ = "STUB: not implemented"
	return nil
}

// Also check for in-progress map subscriptions.

// If channel is not in channels map, check if it's only in mapSubscribing.

// Wait for normal subscription in progress.

// If client is not yet subscribed on a client-side channel, and subscribe
// command is in progress - we need to wait for it to finish before proceeding.
// We hang no longer than maxWaitTimeout here, if timeout happens - it's a signal
// of server malfunction since long subscribes should not happen. In this case,
// we disconnect client to let it re-init the state from scratch.

// Wait for map subscription in progress.

// Identity-match: only close/delete if the entry still corresponds to
// the *mapSubscribeState we were waiting on. A fresh resubscribe may
// have replaced it between the RUnlock and this Lock — clobbering
// that fresh entry would leak its subscribingCh waiters.

// Clean up normal subscription.

// Clean up map subscribing state. Identity-match against the snapshot so we
// don't close a fresh entry installed by a concurrent resubscribe.

// Multiple goroutines can reach this point for the same channel — e.g. the
// presence ticker spawns a fresh handleAsyncUnsubscribe goroutine on every
// tick until the channel is actually removed, and ticks can overlap if the
// first goroutine is still blocked on a network call or the write lock.
// Each goroutine captured its own chCtx snapshot under RLock and would
// otherwise re-run presence/leave/removeSubscription/unsubscribeHandler
// (which double-fires user callbacks and panics on patterns like a single
// `close(doneCh)` in OnUnsubscribe). Only the goroutine that won the delete
// race owns the cleanup; the rest exit here.

// Remove presence and/or run map cleanup on unsubscribe.

// Clean up keyed subscription state (shared poll).

func (c *Client) logDisconnectBadRequest(message string) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *Client) logWriteInternalErrorFlush(ch string, frameType protocol.FrameType, cmd *protocol.Command, err error, message string, started time.Time, rw *replyWriter) {
	_ = "STUB: not implemented"
	return
}

func toClientErr(err error) *Error { _ = "STUB: not implemented"; return nil }

func disconnectFromError(err error) (*Disconnect, bool) {
	_ = "STUB: not implemented"
	return nil, false
}
