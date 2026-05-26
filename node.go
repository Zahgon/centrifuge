package centrifuge

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/controlpb"
	"github.com/centrifugal/centrifuge/internal/controlproto"
	"github.com/centrifugal/centrifuge/internal/dissolve"
	"github.com/centrifugal/centrifuge/internal/nowtime"

	"github.com/FZambia/eagle"
	"github.com/centrifugal/protocol"
	"golang.org/x/sync/singleflight"
)

// Node is a heart of Centrifuge library – it keeps and manages client connections,
// maintains information about other Centrifuge nodes in cluster, keeps references
// to common things (like Broker and PresenceManager, Hub) etc.
// By default, Node uses in-memory implementations of Broker and PresenceManager -
// MemoryBroker and MemoryPresenceManager which allow running a single Node only.
// To scale use other implementations of Broker and PresenceManager like builtin
// RedisBroker and RedisPresenceManager.
type Node struct {
	mu sync.RWMutex
	// unique id for this node.
	uid string
	// startedAt is unix time of node start.
	startedAt int64
	// config for node.
	config Config
	// hub to manage client connections.
	hub *Hub
	// controller is responsible for inter-node communication.
	controller Controller
	// broker is responsible for PUB/SUB and history streaming mechanics.
	broker Broker
	// mapBroker is responsible for map subscriptions.
	mapBroker MapBroker
	// presenceManager is responsible for presence information management.
	presenceManager PresenceManager
	// nodes contains registry of known nodes.
	nodes *nodeRegistry
	// metrics registry.
	metrics *metrics
	// shutdown is a flag which is only true when node is going to shut down.
	shutdown bool
	// shutdownCh is a channel which is closed when node shutdown initiated.
	shutdownCh chan struct{}
	// clientEvents to manage event handlers attached to node.
	clientEvents *eventHub
	// logger allows to log throughout library code and proxy log entries to
	// configured log handler.
	logger *logger
	// cache control encoder in Node.
	controlEncoder controlproto.Encoder
	// cache control decoder in Node.
	controlDecoder controlproto.Decoder
	// subLocks synchronizes access to adding/removing subscriptions.
	subLocks map[int]*sync.Mutex

	metricsMu       sync.Mutex
	metricsExporter *eagle.Eagle
	metricsSnapshot *eagle.Metrics

	// subDissolver used to reliably clear unused subscriptions in Broker.
	subDissolver *dissolve.Dissolver

	// nowTimeGetter provides access to current time.
	nowTimeGetter nowtime.Getter

	surveyHandler  SurveyHandler
	surveyRegistry map[uint64]chan survey
	surveyMu       sync.RWMutex
	surveyID       uint64

	notificationHandler NotificationHandler
	nodeInfoSendHandler NodeInfoSendHandler

	emulationSurveyHandler *emulationSurveyHandler

	mediums     map[string]*channelMedium
	mediumLocks map[int]*sync.Mutex // Sharded locks for mediums map.

	timerScheduler TimerScheduler

	// keyedManager manages keyed channel state (track/untrack, reverse index).
	keyedManager *keyedManager
	// sharedPollManager manages shared poll refresh workers.
	sharedPollManager *SharedPollManager
}

const (
	numSubLocks            = 16384
	numMediumLocks         = 16384
	numSubDissolverWorkers = 64
)

// TransportAcceptedLabels contains labels for transport connection metrics.
// This struct is designed to be extensible - additional label fields can be
// added in the future without breaking compatibility.
type TransportAcceptedLabels struct {
	// Transport is the transport type (e.g., "websocket", "http_stream", "sse").
	Transport string
	// AcceptProtocol is the transport protocol used to accept connection (can be "h1", "h2", "h3").
	AcceptProtocol string
}

// New creates Node with provided Config.
func New(c Config) (*Node, error) { _ = "STUB: not implemented"; return nil, nil }

// 1MB by default.

// 30 days by default.

// index chooses bucket number in range [0, numBuckets).
func index(s string, numBuckets int) int { _ = "STUB: not implemented"; return 0 }

// Config returns Node's Config.
func (n *Node) Config() Config {
	_ = "STUB: not implemented"

	// ID returns unique Node identifier. This is a UUID v4 value.
	return *new(Config)
}

func (n *Node) ID() string { _ = "STUB: not implemented"; return "" }

func (n *Node) subLock(ch string) *sync.Mutex { _ = "STUB: not implemented"; return nil }

func (n *Node) mediumLock(ch string) *sync.Mutex { _ = "STUB: not implemented"; return nil }

// SetController allows setting Controller implementation to use.
func (n *Node) SetController(c Controller) {
	_ = "STUB: not implemented"

	// SetBroker allows setting Broker implementation to use.
	// For historical reasons and to keep existing API, we also check if Broker implements Controller
	// and if so we set it as Node's Controller (but only if Controller not explicitly set).
	return
}

func (n *Node) SetBroker(b Broker) { _ = "STUB: not implemented"; return }

// SetPresenceManager allows setting PresenceManager to use.
func (n *Node) SetPresenceManager(m PresenceManager) { _ = "STUB: not implemented"; return }

// SetMapBroker allows setting MapBroker to use.
func (n *Node) SetMapBroker(e MapBroker) {
	_ = "STUB: not implemented"

	// resolveMapChannelOptions returns validated channel options for a map channel.
	// Returns an error if GetMapChannelOptions is not configured or the channel
	// options are invalid.
	return
}

func (n *Node) resolveMapChannelOptions(channel string) (MapChannelOptions, error) {
	_ = "STUB: not implemented"
	return *new(MapChannelOptions), nil
}

// Hub returns node's Hub.
func (n *Node) Hub() *Hub {
	_ = "STUB: not implemented"

	// Run performs node startup actions. At moment must be called once on start
	// after Controller and Broker set to Node.
	return nil
}

func (n *Node) Run() error { _ = "STUB: not implemented"; return nil }

// Initialize shared poll manager if configured.

// logEnabled allows check whether a LogLevel enabled or not.
func (n *Node) logEnabled(level LogLevel) bool { _ = "STUB: not implemented"; return false }

// IncMapBrokerCleanupErrors increments the map broker cleanup error counter for observability.
func (n *Node) IncMapBrokerCleanupErrors(name string) { _ = "STUB: not implemented"; return }

// AddMapBrokerCleanupKeysRemoved adds to the map broker cleanup keys removed counter for observability.
func (n *Node) AddMapBrokerCleanupKeysRemoved(name string, count int64) {
	_ = "STUB: not implemented"
	return
}

// SetMapBrokerCleanupLag sets the map broker cleanup lag gauge for observability.
func (n *Node) SetMapBrokerCleanupLag(name string, seconds float64) {
	_ = "STUB: not implemented"
	return
}

// Shutdown sets shutdown flag to Node so handlers could stop accepting
// new requests and disconnects clients with shutdown reason.
func (n *Node) Shutdown(ctx context.Context) error { _ = "STUB: not implemented"; return nil }

// Stop shared poll workers before hub shutdown.

// NotifyShutdown returns a channel which will be closed on node shutdown.
func (n *Node) NotifyShutdown() chan struct{} { _ = "STUB: not implemented"; return nil }

func (n *Node) updateGauges() { _ = "STUB: not implemented"; return }

func (n *Node) updateMetrics() { _ = "STUB: not implemented"; return }

// Centrifuge library uses Prometheus metrics for instrumentation. But we also try to
// aggregate Prometheus metrics periodically and share this information between Nodes.
func (n *Node) initMetrics() error { _ = "STUB: not implemented"; return nil }

func (n *Node) sendNodePing() { _ = "STUB: not implemented"; return }

func (n *Node) cleanNodeInfo() { _ = "STUB: not implemented"; return }

func (n *Node) handleNotification(fromNodeID string, req *controlpb.Notification) error {
	_ = "STUB: not implemented"
	return nil
}

func (n *Node) handleSurveyRequest(fromNodeID string, req *controlpb.SurveyRequest) error {
	_ = "STUB: not implemented"
	return nil
}

func (n *Node) handleSurveyResponse(uid string, resp *controlpb.SurveyResponse) error {
	_ = "STUB: not implemented"
	return nil
}

// Survey channel allocated with capacity enough to receive all survey replies,
// default case here means that channel has no reader anymore, so it's safe to
// skip message. This extra survey reply can come from extra node that just
// joined.

// SurveyResult from node.
type SurveyResult struct {
	Code uint32
	Data []byte
}

type survey struct {
	UID    string
	Result SurveyResult
}

var errSurveyHandlerNotRegistered = errors.New("no survey handler registered")

const defaultSurveyTimeout = 10 * time.Second

// Survey allows collecting data from all running Centrifuge nodes. This method publishes
// control messages, then waits for replies from all running nodes. The maximum time to wait
// can be controlled over context timeout. If provided context does not have a deadline for
// survey then this method uses default 10 seconds timeout. Keep in mind that Survey does not
// scale very well as number of Centrifuge Node grows. Though it has reasonably good performance
// to perform rare tasks even with relatively large number of nodes.
// If toNodeID is not an empty string then a survey will be sent only to the concrete node in
// a cluster, otherwise a survey sent to all running nodes. See a corresponding Node.OnSurvey
// method to handle received surveys.
// Survey ops starting with `centrifuge_` are reserved by Centrifuge library.
func (n *Node) Survey(ctx context.Context, op string, data []byte, toNodeID string) (map[string]SurveyResult, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// If no timeout provided then fallback to defaultSurveyTimeout to avoid endless surveys.

// Invoke handler on this node since control message handler
// ignores those sent from the current Node.

// Info contains information about all known server nodes.
type Info struct {
	Nodes []NodeInfo
}

// Metrics aggregation over time interval for node.
type Metrics struct {
	Interval float64
	Items    map[string]float64
}

// NodeInfo contains information about node.
type NodeInfo struct {
	UID         string
	Name        string
	Version     string
	NumClients  uint32
	NumUsers    uint32
	NumSubs     uint32
	NumChannels uint32
	Uptime      uint32
	Metrics     *Metrics
	Data        []byte
}

// Info returns aggregated stats from all nodes.
func (n *Node) Info() (Info, error) { _ = "STUB: not implemented"; return *new(Info), nil }

// handleControl handles messages from control channel - control messages used for internal
// communication between nodes to share state or proto.
func (n *Node) handleControl(data []byte) error { _ = "STUB: not implemented"; return nil }

// Sent by this node.

// control proto v2.

// handlePublication handles messages published into channel and
// coming from Broker. The goal of method is to deliver this message
// to all clients on this node currently subscribed to channel.
func (n *Node) handlePublication(ch string, sp StreamPosition, pub, prevPub, localPrevPub *Publication) error {
	_ = "STUB: not implemented"
	return nil
}

func (n *Node) getBatchConfig(channel string) ChannelBatchConfig {
	_ = "STUB: not implemented"
	return *new(ChannelBatchConfig)
}

// handleJoin handles join messages - i.e. broadcasts it to
// interested local clients subscribed to channel.
func (n *Node) handleJoin(ch string, info *ClientInfo) error { _ = "STUB: not implemented"; return nil }

// handleLeave handles leave messages - i.e. broadcasts it to
// interested local clients subscribed to channel.
func (n *Node) handleLeave(ch string, info *ClientInfo) error {
	_ = "STUB: not implemented"
	return nil
}

func (n *Node) publish(ch string, data []byte, opts ...PublishOption) (PublishResult, error) {
	_ = "STUB: not implemented"
	return *new(PublishResult), nil
}

// PublishResult returned from Publish operation.
type PublishResult struct {
	StreamPosition
	// Suppressed is true when the operation was suppressed (e.g. due to idempotency key deduplication).
	Suppressed bool
	// SuppressReason explains why the operation was suppressed (empty when Suppressed is false).
	SuppressReason SuppressReason
}

// Publish sends data to all clients subscribed on channel at this moment. All running
// nodes will receive Publication and send it to all local channel subscribers.
//
// Data expected to be valid marshaled JSON or any binary payload.
// Connections that work over JSON protocol can not handle binary payloads.
// Connections that work over Protobuf protocol can work both with JSON and binary payloads.
//
// So the rule here: if you have channel subscribers that work using JSON
// protocol then you can not publish binary data to these channel.
//
// Channels in Centrifuge are ephemeral and its settings not persisted over different
// publish operations. So if you want to have a channel with history stream behind you
// need to provide WithHistory option on every publish. To simplify working with different
// channels you can make some type of publish wrapper in your own code.
//
// The returned PublishResult contains embedded StreamPosition that describes
// position inside stream Publication was added too. For channels without history
// enabled (i.e. when Publications only sent to PUB/SUB system) StreamPosition will
// be an empty struct (i.e. PublishResult.Offset will be zero).
func (n *Node) Publish(channel string, data []byte, opts ...PublishOption) (PublishResult, error) {
	_ = "STUB: not implemented"
	return *new(PublishResult), nil
}

// publishJoin allows publishing join message into channel when someone subscribes on it
// or leave message when someone unsubscribes from channel.
func (n *Node) publishJoin(ch string, info *ClientInfo) error {
	_ = "STUB: not implemented"
	return nil
}

// publishLeave allows publishing join message into channel when someone subscribes on it
// or leave message when someone unsubscribes from channel.
func (n *Node) publishLeave(ch string, info *ClientInfo) error {
	_ = "STUB: not implemented"
	return nil
}

var errNotificationHandlerNotRegistered = errors.New("notification handler not registered")

// Notify allows sending an asynchronous notification to all other nodes
// (or to a single specific node). Unlike Survey, it does not wait for any
// response. If toNodeID is not an empty string then a notification will
// be sent to a concrete node in cluster, otherwise a notification sent to
// all running nodes. See a corresponding Node.OnNotification method to
// handle received notifications.
func (n *Node) Notify(op string, data []byte, toNodeID string) error {
	_ = "STUB: not implemented"
	return nil
}

// Invoke handler on this node since control message handler
// ignores those sent from the current Node.

// Already on this node and called notificationHandler above, no
// need to send notification over network.

// publishControl publishes message into control channel so all running
// nodes will receive and handle it.
func (n *Node) publishControl(cmd *controlpb.Command, nodeID string) error {
	_ = "STUB: not implemented"
	return nil
}

func (n *Node) getMetrics(metrics eagle.Metrics) *controlpb.Metrics {
	_ = "STUB: not implemented"
	return nil
}

// pubNode sends control message to all nodes - this message
// contains information about current node.
func (n *Node) pubNode(nodeID string) error { _ = "STUB: not implemented"; return nil }

// We only send metrics once when updated.

// controlpbFilterFromProto converts a protocol.FilterNode tree into the
// wire-equivalent controlpb.FilterNode tree used inside control messages.
// Returns nil for a nil input.
func controlpbFilterFromProto(f *FilterNode) *controlpb.FilterNode {
	_ = "STUB: not implemented"
	return nil
}

// protoFilterFromControlpb is the inverse of controlpbFilterFromProto, used by
// the control message dispatcher when receiving a label-filtered command.
// Returns nil for a nil input.
func protoFilterFromControlpb(f *controlpb.FilterNode) *FilterNode {
	_ = "STUB: not implemented"
	return nil
}

func (n *Node) pubSubscribe(user string, ch string, opts SubscribeOptions) error {
	_ = "STUB: not implemented"
	return nil
}

func (n *Node) pubRefresh(user string, opts RefreshOptions) error {
	_ = "STUB: not implemented"
	return nil
}

// pubUnsubscribe publishes unsubscribe control message to all nodes – so all
// nodes could unsubscribe user from channel.
func (n *Node) pubUnsubscribe(user string, ch string, unsubscribe Unsubscribe, clientID, sessionID string, labelFilter *FilterNode) error {
	_ = "STUB: not implemented"
	return nil
}

// pubDisconnect publishes disconnect control message to all nodes – so all
// nodes could disconnect user from server.
func (n *Node) pubDisconnect(user string, disconnect Disconnect, clientID string, sessionID string, whitelist []string, labelFilter *FilterNode) error {
	_ = "STUB: not implemented"
	return nil
}

// addClient registers authenticated connection in clientConnectionHub
// this allows to make operations with user connection on demand.
func (n *Node) addClient(c *Client) { _ = "STUB: not implemented"; return }

// removeClient removes client connection from connection registry.
func (n *Node) removeClient(c *Client) { _ = "STUB: not implemented"; return }

// addSubscription registers subscription of connection on channel in both
// Hub and Broker.
func (n *Node) addSubscription(ch string, sub subInfo) (int64, error) {
	_ = "STUB: not implemented"
	return 0, nil
}

// Subscribe to appropriate broker based on subscription type.

// removeSubscription removes subscription of connection on channel
// from Hub and Broker (or MapBroker for map channels).
func (n *Node) removeSubscription(ch string, c *Client) error {
	_ = "STUB: not implemented"
	return nil
}

// Unsubscribe from appropriate broker based on channel type.

// Cool down a bit since broker is not ready to process unsubscription.

// nodeCmd handles node control command i.e. updates information about known nodes.
func (n *Node) nodeCmd(node *controlpb.Node) error { _ = "STUB: not implemented"; return nil }

// New Node in cluster

// shutdownCmd handles shutdown control command sent when node leaves cluster.
func (n *Node) shutdownCmd(nodeID string) error { _ = "STUB: not implemented"; return nil }

// Subscribe subscribes user to a channel.
// Note, that OnSubscribe event won't be called in this case
// since this is a server-side subscription. If user have been already
// subscribed to a channel then its subscription will be updated and
// subscribe notification will be sent to a client-side.
func (n *Node) Subscribe(userID string, channel string, opts ...SubscribeOption) error {
	_ = "STUB: not implemented"
	return nil
}

// Send subscribe control message to other nodes.

// Subscribe on this node.

// Unsubscribe unsubscribes user from a channel.
// If a channel is empty string then user will be unsubscribed from all channels.
func (n *Node) Unsubscribe(userID string, channel string, opts ...UnsubscribeOption) error {
	_ = "STUB: not implemented"
	return nil
}

// Send unsubscribe control message to other nodes.

// Unsubscribe on this node.

// Disconnect allows closing all user connections on all nodes.
func (n *Node) Disconnect(userID string, opts ...DisconnectOption) error {
	_ = "STUB: not implemented"
	return nil
}

// Disconnect user from this node

// Send disconnect control message to other nodes.

// Disconnect on this node.

// Refresh user connection.
// Without any options will make user connections non-expiring.
// Note, that OnRefresh event won't be called in this case
// since this is a server-side refresh.
func (n *Node) Refresh(userID string, opts ...RefreshOption) error {
	_ = "STUB: not implemented"
	return nil
}

// Refresh on this node.

func (n *Node) getPresenceManager(ch string) PresenceManager {
	_ = "STUB: not implemented"
	return *new(PresenceManager)
}

// addPresence proxies presence adding to PresenceManager.
func (n *Node) addPresence(ch string, uid string, info *ClientInfo) error {
	_ = "STUB: not implemented"
	return nil
}

// removePresence proxies presence removing to PresenceManager.
func (n *Node) removePresence(ch string, clientID string, userID string) error {
	_ = "STUB: not implemented"
	return nil
}

var (
	presenceGroup      singleflight.Group
	presenceStatsGroup singleflight.Group
	historyGroup       singleflight.Group
	mapStateGroup      singleflight.Group
	mapStreamGroup     singleflight.Group
	mapStatsGroup      singleflight.Group
)

// PresenceResult wraps presence.
type PresenceResult struct {
	Presence map[string]*ClientInfo
}

func (n *Node) presence(ch string, presenceManager PresenceManager) (PresenceResult, error) {
	_ = "STUB: not implemented"
	return *new(PresenceResult), nil
}

// Presence returns a map with information about active clients in channel.
func (n *Node) Presence(ch string) (PresenceResult, error) {
	_ = "STUB: not implemented"
	return *new(PresenceResult), nil
}

func infoFromProto(v *protocol.ClientInfo) *ClientInfo { _ = "STUB: not implemented"; return nil }

func infoToProto(v *ClientInfo) *protocol.ClientInfo { _ = "STUB: not implemented"; return nil }

func pubToProto(pub *Publication) *protocol.Publication { _ = "STUB: not implemented"; return nil }

func pubFromProto(pub *protocol.Publication) *Publication { _ = "STUB: not implemented"; return nil }

// PresenceStatsResult wraps presence stats.
type PresenceStatsResult struct {
	PresenceStats
}

func (n *Node) presenceStats(ch string, presenceManager PresenceManager) (PresenceStatsResult, error) {
	_ = "STUB: not implemented"
	return *new(PresenceStatsResult), nil
}

// PresenceStats returns presence stats from PresenceManager.
func (n *Node) PresenceStats(ch string) (PresenceStatsResult, error) {
	_ = "STUB: not implemented"
	return *new(PresenceStatsResult), nil
}

// HistoryResult contains Publications and current stream top StreamPosition.
type HistoryResult struct {
	// StreamPosition embedded here describes current stream top offset and epoch.
	StreamPosition
	// Publications extracted from history storage according to HistoryFilter.
	Publications []*Publication
}

func (n *Node) getBroker(ch string) Broker { _ = "STUB: not implemented"; return *new(Broker) }

func (n *Node) getMapBroker(ch string) MapBroker { _ = "STUB: not implemented"; return *new(MapBroker) }

func (n *Node) history(ch string, opts *HistoryOptions) (HistoryResult, error) {
	_ = "STUB: not implemented"
	return *new(HistoryResult), nil
}

// History allows extracting Publications in channel.
// The channel must belong to namespace where history is on.
func (n *Node) History(ch string, opts ...HistoryOption) (HistoryResult, error) {
	_ = "STUB: not implemented"
	return *new(HistoryResult), nil
}

// recoverHistory recovers publications since StreamPosition last seen by client.
func (n *Node) recoverHistory(ch string, since StreamPosition, historyMetaTTL time.Duration) (HistoryResult, error) {
	_ = "STUB: not implemented"
	return *new(HistoryResult), nil
}

// recoverCache recovers last publication in channel.
func (n *Node) recoverCache(ch string, historyMetaTTL time.Duration, tf *tagsFilter) (*Publication, *Publication, StreamPosition, error) {
	_ = "STUB: not implemented"
	return nil, nil, *new(StreamPosition), nil
}

// streamTop returns current stream top StreamPosition for a channel.
func (n *Node) streamTop(ch string, historyMetaTTL time.Duration) (StreamPosition, error) {
	_ = "STUB: not implemented"
	return *new(StreamPosition), nil
}

func (n *Node) mapStreamTop(ch string) (StreamPosition, error) {
	_ = "STUB: not implemented"
	return *new(StreamPosition), nil
}

func (n *Node) checkPosition(ch string, clientPosition StreamPosition, historyMetaTTL time.Duration, isMap bool) (bool, error) {
	_ = "STUB: not implemented"
	return false, nil
}

// If the map broker guarantees no-gaps delivery to local subscribers,
// the periodic position sync is redundant — trust the broker.

// If the stream broker guarantees no-gaps delivery to local subscribers,
// skip the position sync request entirely.

// No medium for channel or position sync disabled – check position over Broker.

// RemoveHistory removes channel history.
func (n *Node) RemoveHistory(ch string) error { _ = "STUB: not implemented"; return nil }

type nodeRegistry struct {
	// mu allows synchronizing access to node registry.
	mu sync.RWMutex
	// currentUID keeps uid of current node
	currentUID string
	// nodes is a map with information about known nodes.
	nodes map[string]*controlpb.Node
	// updates track time we last received ping from node. Used to clean up nodes map.
	updates map[string]int64
}

func newNodeRegistry(currentUID string) *nodeRegistry { _ = "STUB: not implemented"; return nil }

func (r *nodeRegistry) list() []*controlpb.Node { _ = "STUB: not implemented"; return nil }

func (r *nodeRegistry) size() int { _ = "STUB: not implemented"; return 0 }

func (r *nodeRegistry) get(uid string) (*controlpb.Node, bool) {
	_ = "STUB: not implemented"
	return nil, false
}

func (r *nodeRegistry) add(info *controlpb.Node) bool { _ = "STUB: not implemented"; return false }

func (r *nodeRegistry) remove(uid string) { _ = "STUB: not implemented"; return }

func (r *nodeRegistry) clean(delay time.Duration) { _ = "STUB: not implemented"; return }

// No need to clean info for current node.

// As we do all operations with nodes under lock this should never happen.

// Too many seconds since this node have been last seen - remove it from map.

// OnSurvey allows setting SurveyHandler. This should be done before Node.Run called.
func (n *Node) OnSurvey(handler SurveyHandler) { _ = "STUB: not implemented"; return }

// OnNotification allows setting NotificationHandler. This should be done before Node.Run called.
func (n *Node) OnNotification(handler NotificationHandler) { _ = "STUB: not implemented"; return }

// OnNodeInfoSend allows setting NodeInfoSendHandler. This should be done before Node.Run called.
func (n *Node) OnNodeInfoSend(handler NodeInfoSendHandler) { _ = "STUB: not implemented"; return }

// eventHub allows binding client event handlers.
// All eventHub methods are not goroutine-safe and supposed
// to be called once before Node Run called.
type eventHub struct {
	connectingHandler       ConnectingHandler
	connectHandler          ConnectHandler
	transportWriteHandler   TransportWriteHandler
	commandReadHandler      CommandReadHandler
	commandProcessedHandler CommandProcessedHandler
	cacheEmptyHandler       CacheEmptyHandler
	sharedPollHandler       SharedPollHandler
}

// OnConnecting allows setting ConnectingHandler.
// ConnectingHandler will be called when client sends Connect command to server.
// In this handler server can reject connection or provide Credentials for it.
func (n *Node) OnConnecting(handler ConnectingHandler) { _ = "STUB: not implemented"; return }

// OnConnect allows setting ConnectHandler.
// ConnectHandler called after client connection successfully established,
// authenticated and Connect Reply already sent to client. This is a place where
// application can start communicating with client.
func (n *Node) OnConnect(handler ConnectHandler) { _ = "STUB: not implemented"; return }

// OnTransportWrite allows setting TransportWriteHandler. This should be done before Node.Run called.
func (n *Node) OnTransportWrite(handler TransportWriteHandler) { _ = "STUB: not implemented"; return }

// OnCommandRead allows setting CommandReadHandler. This should be done before Node.Run called.
func (n *Node) OnCommandRead(handler CommandReadHandler) { _ = "STUB: not implemented"; return }

// OnCommandProcessed allows setting CommandProcessedHandler. This should be done before Node.Run called.
func (n *Node) OnCommandProcessed(handler CommandProcessedHandler) {
	_ = "STUB: not implemented"
	return
}

// OnCacheEmpty allows setting CacheEmptyHandler.
// CacheEmptyHandler called when client subscribes on a channel with RecoveryModeCache but there is no
// cached value in channel. In response to this handler it's possible to tell Centrifuge what to do with
// subscribe request – keep it, or return error.
func (n *Node) OnCacheEmpty(h CacheEmptyHandler) { _ = "STUB: not implemented"; return }

// OnSharedPoll allows setting SharedPollHandler.
// SharedPollHandler is called by the refresh worker to fetch current item
// data from the backend. Called per-channel, not per-client.
func (n *Node) OnSharedPoll(handler SharedPollHandler) { _ = "STUB: not implemented"; return }

// SharedPollNotify submits notifications that trigger immediate backend polls
// for the specified keys. Notifications are batched per channel according to
// SharedPollChannelOptions before triggering polls. Safe for concurrent use.
// Notifications for unknown channels are silently dropped.
func (n *Node) SharedPollNotify(notifications []SharedPollNotificationItem) {
	_ = "STUB: not implemented"
	return
}

// SharedPollPublish pushes data directly to a SharedPoll channel for a specific key.
// The version must be in the same space as versions returned by the OnSharedPoll handler —
// stale versions (≤ current) are ignored within a given epoch. When PublishEnabled is set
// in channel options, the publication is distributed to all nodes via Broker PUB/SUB.
// Otherwise, local-only.
//
// epoch is an optional channel-level string identifying the publisher's epoch.
// If it differs from the channel's stored epoch, all current subscribers are
// unsubscribed with insufficient-state code so they re-track from version 0 on
// resubscribe — this lets a publisher that resets its in-memory version counter
// (e.g., after a process restart) deliver fresh state without freezing connected
// clients. Use empty epoch to skip this check (pure version comparison).
func (n *Node) SharedPollPublish(ctx context.Context, channel string, key string, version uint64, epoch string, data []byte) error {
	_ = "STUB: not implemented"
	return nil
}

// HandlePublication coming from Broker.
func (n *Node) HandlePublication(ch string, pub *Publication, sp StreamPosition, delta bool, prevPub *Publication) error {
	_ = "STUB: not implemented"
	return nil
}

// Route shared poll key-scoped publications to SharedPollManager.

// Fallback: check if it's a direct channel match (local-only path).

// Deliver epoch in the first publication (offset==1) so clients learn
// the channel epoch. This covers first-ever publish and post-Clear
// scenarios. Subsequent publications omit epoch to save wire bytes.

// Note, avoid using subLock in HandlePublication – this leads to the deadlock.

// HandleJoin coming from Broker.
func (n *Node) HandleJoin(ch string, info *ClientInfo) error { _ = "STUB: not implemented"; return nil }

// HandleLeave coming from Broker.
func (n *Node) HandleLeave(ch string, info *ClientInfo) error {
	_ = "STUB: not implemented"
	return nil
}

// HandleControl coming from Broker.
func (n *Node) HandleControl(data []byte) error { _ = "STUB: not implemented"; return nil }

// MapStateRead retrieves keyed snapshot for a channel.
func (n *Node) MapStateRead(ctx context.Context, ch string, opts MapReadStateOptions) (MapStateResult, error) {
	_ = "STUB: not implemented"
	return *new(MapStateResult), nil
}

func (n *Node) mapStateKey(ch string, opts MapReadStateOptions) string {
	_ = "STUB: not implemented"
	return ""
}

// MapStreamRead retrieves keyed stream for a channel.
func (n *Node) MapStreamRead(ctx context.Context, ch string, opts MapReadStreamOptions) (MapStreamResult, error) {
	_ = "STUB: not implemented"
	return *new(MapStreamResult), nil
}

// Detect unrecoverable position: if we requested entries after a known offset
// but the first returned entry has a higher offset, entries were lost due to
// stream trimming — the client cannot recover cleanly.

func (n *Node) mapStreamKey(ch string, opts MapReadStreamOptions) string {
	_ = "STUB: not implemented"
	return ""
}

// mapStreamPosition returns the current stream position for a map channel.
// This is useful for capturing the stream top before starting stream pagination.
func (n *Node) mapStreamPosition(ctx context.Context, ch string) (StreamPosition, error) {
	_ = "STUB: not implemented"
	return *new(StreamPosition), nil
}

// ReadStream with Limit=0 returns only the current stream position.

// MapStatsResult wraps keyed stats result.
type MapStatsResult struct {
	MapStats
}

// MapStats retrieves stats for a map channel.
func (n *Node) MapStats(ctx context.Context, ch string) (MapStatsResult, error) {
	_ = "STUB: not implemented"
	return *new(MapStatsResult), nil
}

// MapPublish publishes data to a map channel.
// This updates the snapshot and optionally broadcasts to subscribers.
func (n *Node) MapPublish(ctx context.Context, ch string, key string, opts MapPublishOptions) (MapUpdateResult, error) {
	_ = "STUB: not implemented"
	return *new(MapUpdateResult), nil
}

// MapRemove removes a key from a map channel.
// This removes the key from snapshot and optionally broadcasts removal to subscribers.
func (n *Node) MapRemove(ctx context.Context, ch string, key string, opts MapRemoveOptions) (MapUpdateResult, error) {
	_ = "STUB: not implemented"
	return *new(MapUpdateResult), nil
}

// MapClear deletes all data for a map channel (state and stream).
// Use for cleanup when a channel data is no longer needed.
func (n *Node) MapClear(ctx context.Context, ch string, opts MapClearOptions) error {
	_ = "STUB: not implemented"
	return nil
}
