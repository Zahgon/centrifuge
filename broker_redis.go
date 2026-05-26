package centrifuge

import (
	"context"
	"errors"
	"sync"

	_ "embed"

	"github.com/redis/rueidis"
)

var (
	errPubSubConnUnavailable = errors.New("redis: pub/sub connection temporary unavailable")
)

const (
	// redisSubscribeBatchLimit is a maximum number of channels to include in a single
	// batch subscribe call.
	redisSubscribeBatchLimit = 512
	// redisControlChannelSuffix is a suffix for control channel.
	redisControlChannelSuffix = ".control"
	// redisNodeChannelPrefix is a suffix for node channel.
	redisNodeChannelPrefix = ".node."
	// redisClientChannelPrefix is a prefix before channel name for client messages.
	redisClientChannelPrefix = ".client."
	// redisPubSubShardChannelSuffix is a suffix in channel name which we use to establish a sharded PUB/SUB connection.
	redisPubSubShardChannelSuffix = ".shard"
)

var _ Broker = (*RedisBroker)(nil)
var _ Controller = (*RedisBroker)(nil)

type pubSubStart struct {
	once  sync.Once
	errCh chan error
}

type controlPubSubStart struct {
	once  sync.Once
	errCh chan error
}

type shardWrapper struct {
	shard               *RedisShard
	subClientsMu        sync.Mutex
	subClients          [][]rueidis.DedicatedClient
	pubSubStartChannels [][]*pubSubStart
	controlPubSubStart  *controlPubSubStart
	logFields           map[string]any
	pubSubRunner        brokerPubSubRunner
}

// brokerPubSubRunner abstracts the subscriber-side pub/sub strategy for
// RedisBroker. One instance per shard, selected at construction time. init is
// called from NewRedisBroker (no goroutines), run from RegisterBrokerEventHandler
// (launches subscribers and returns; readiness is signaled via
// shardWrapper.pubSubStartChannels). subClientsIndex maps a partition-derived
// cluster shard index to the first-dimension index used in
// shardWrapper.subClients (default is identity; non-default strategies may
// remap, e.g. partition → node).
type brokerPubSubRunner interface {
	init(s *shardWrapper, shard *RedisShard) error
	run(s *shardWrapper, h BrokerEventHandler) error
	subClientsIndex(clusterShardIdx int) int
}

// RedisBroker uses Redis to implement Broker functionality. This broker allows
// scaling Centrifuge-based server to many instances and load balance client
// connections between them. Centrifuge nodes will be connected over Redis PUB/SUB.
// RedisBroker supports standalone Redis, Redis in master-replica setup with Sentinel,
// Redis Cluster. Also, it supports client-side consistent sharding between isolated
// Redis setups.
// By default, Redis >= 5 required (due to the fact RedisBroker uses STREAM data
// structure to keep publication history for a channel).
type RedisBroker struct {
	controlRound uint64
	node         *Node
	sharding     bool
	config       RedisBrokerConfig
	shards       []*shardWrapper
	// partitionTags is non-nil when UsePrecomputedPartitionTags is enabled;
	// indexed by partition index, returns the hash tag string. Read-only
	// after construction (shared with the package-level precomputed table).
	partitionTags           []string
	publishIdempotentScript *rueidis.Lua
	historyListScript       *rueidis.Lua
	historyStreamScript     *rueidis.Lua
	addHistoryListScript    *rueidis.Lua
	addHistoryStreamScript  *rueidis.Lua
	shardChannel            string
	messagePrefix           string
	controlChannel          string
	nodeChannel             string
	closeOnce               sync.Once
	closeCh                 chan struct{}
}

// RedisBrokerConfig is a config for Broker.
type RedisBrokerConfig struct {
	// Prefix to use before every channel name and key in Redis. By default,
	// RedisBroker will use prefix "centrifuge".
	Prefix string

	// Shards is a slice of RedisShard to use. At least one shard must be provided.
	// Data will be consistently sharded by channel over provided Redis shards.
	Shards []*RedisShard

	// UseLists allows enabling usage of Redis LIST instead of STREAM data
	// structure to keep history. LIST support exist mostly for backward
	// compatibility since STREAM seems superior. If you have a use case
	// where you need to turn on this option in new setup - please share,
	// otherwise LIST support can be removed at some point in the future.
	// Iteration over history in reversed order not supported with lists.
	UseLists bool

	// Subscribe on replica Redis nodes. This only works for Redis Cluster
	// and Sentinel setups and requires replica client to be initialized in
	// each RedisShard using RedisShardConfig.ReplicaClientEnabled.
	SubscribeOnReplica bool

	// SkipPubSub enables mode when Redis broker only saves history, without
	// publishing to channels and using PUB/SUB.
	SkipPubSub bool

	// Name of broker, for observability purposes – i.e. becomes part of metrics/logs.
	// By default, empty string is used.
	Name string

	// NumShardedPubSubPartitions when greater than zero allows turning on a mode in which
	// broker will use Redis Cluster with sharded PUB/SUB feature available in
	// Redis >= 7: https://redis.io/docs/manual/pubsub/#sharded-pubsub
	//
	// To achieve sharded PUB/SUB efficiency RedisBroker reduces 16384 Redis Cluster
	// slots to the NumShardedPubSubPartitions value and starts a separate PUB/SUB for each
	// partition. This is necessary because in Centrifuge case one node can work with
	// thousands of different channels – and we can't afford running a separate
	// PUB/SUB connection for each of 16384 possible slots. We re-use partition
	// connection for many channels and make sure that all channels in the partition
	// point to the same Redis Cluster slot.
	//
	// By default, sharded PUB/SUB is not used in Redis Cluster case - Centrifuge uses
	// globally distributed PUBLISH commands in Redis Cluster where each publish is
	// distributed to all nodes in Redis Cluster.
	//
	// Note (!), that turning on NumShardedPubSubPartitions will cause Centrifuge to generate
	// different key names for history and different Redis channel names than in the base
	// Redis Cluster mode due to reasons outlined above.
	NumShardedPubSubPartitions int

	// UsePrecomputedPartitionTags switches sharded PUB/SUB partition hash tags
	// from the bare partition index ("0", "1", ...) to a precomputed table
	// chosen so CRC16 hash slots distribute evenly across any cluster size.
	// The bare-index scheme can collide badly on larger clusters — see
	// https://github.com/centrifugal/centrifuge/issues/554. Enabling this
	// option spreads partitions across slots so cluster nodes get balanced
	// PUB/SUB load even at higher shard counts.
	//
	// When enabled, NumShardedPubSubPartitions must equal one of the sizes
	// returned by redispartition.PrecomputedSizes() (16, 32, 64, 128, 256,
	// 512, 1024, 2048, 4096). Construction returns an error otherwise. The
	// exact tag table is bundled in internal/redispartition/precomputed.go;
	// the tags are stable and will not change.
	//
	// Toggling this option changes the Redis key/channel naming scheme, so
	// flipping it on a running deployment requires a coordinated restart
	// with state cleared (existing channels and history will appear under
	// different keys).
	//
	// Default: false (backward-compatible bare-index tags).
	UsePrecomputedPartitionTags bool

	// numSubscribeShards defines how many subscribe shards will be used by Centrifuge.
	// Each subscribe shard uses a dedicated connection to Redis for making subscriptions.
	// Zero value means 1.
	numSubscribeShards int

	// numResubscribeShards defines how many subscriber goroutines will be used by
	// Centrifuge for resubscribing process for each subscribe shard. Zero value tells
	// Centrifuge to use 16 subscriber goroutines per subscribe shard.
	numResubscribeShards int

	// numPubSubProcessors allows configuring number of workers which will process
	// messages coming from Redis PUB/SUB. Zero value tells Centrifuge to use the
	// number calculated as:
	// runtime.NumCPU / numSubscribeShards / NumShardedPubSubPartitions (if used) (minimum 1).
	numPubSubProcessors int

	// LoadSHA1 enables loading SHA1 from Redis via SCRIPT LOAD instead of calculating
	// it on the client side. This is useful for FIPS compliance.
	LoadSHA1 bool
}

// NewRedisBroker initializes Redis Broker.
func NewRedisBroker(n *Node, config RedisBrokerConfig) (*RedisBroker, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// newBrokerPubSubRunnerHook is an optional package-private hook used by
// auxiliary modules to install an alternative pub/sub runner. The default
// implementation returns nil so the broker falls back to defaultBrokerPubSubRunner.
var newBrokerPubSubRunnerHook func(b *RedisBroker, shard *RedisShard) brokerPubSubRunner

// defaultBrokerPubSubRunner is the standard partition-sharded pub/sub runner.
// All per-shard state lives on the shardWrapper, the runner is stateless beyond
// a back-pointer to the broker.
type defaultBrokerPubSubRunner struct {
	broker *RedisBroker
}

func (r *defaultBrokerPubSubRunner) init(s *shardWrapper, shard *RedisShard) error {
	b := r.broker
	subChannels := make([][]rueidis.DedicatedClient, 0)
	pubSubStartChannels := make([][]*pubSubStart, 0)

	if b.useShardedPubSub(shard) {
		for i := 0; i < b.config.NumShardedPubSubPartitions; i++ {
			subChannels = append(subChannels, make([]rueidis.DedicatedClient, 0))
			pubSubStartChannels = append(pubSubStartChannels, make([]*pubSubStart, 0))
		}
	} else {
		subChannels = append(subChannels, make([]rueidis.DedicatedClient, 0))
		pubSubStartChannels = append(pubSubStartChannels, make([]*pubSubStart, 0))
	}

	for i := 0; i < len(subChannels); i++ {
		for j := 0; j < b.config.numSubscribeShards; j++ {
			subChannels[i] = append(subChannels[i], nil)
			pubSubStartChannels[i] = append(pubSubStartChannels[i], &pubSubStart{errCh: make(chan error, 1)})
		}
	}

	s.subClients = subChannels
	s.pubSubStartChannels = pubSubStartChannels
	return nil
}

func (r *defaultBrokerPubSubRunner) subClientsIndex(clusterShardIdx int) int {
	_ = "STUB: not implemented"
	return 0
}

func (r *defaultBrokerPubSubRunner) run(s *shardWrapper, h BrokerEventHandler) error {
	_ = "STUB: not implemented"
	return nil
}

// Cluster shards.

// PUB/SUB shards.

var (
	//go:embed internal/redis_lua/broker_publish_idempotent.lua
	publishIdempotentSource string

	//go:embed internal/redis_lua/broker_history_add_list.lua
	addHistoryListSource string

	//go:embed internal/redis_lua/broker_history_add_stream.lua
	addHistoryStreamSource string

	//go:embed internal/redis_lua/broker_history_list.lua
	historyListSource string

	//go:embed internal/redis_lua/broker_history_stream.lua
	historyStreamSource string
)

func (b *RedisBroker) getShard(channel string) *shardWrapper { _ = "STUB: not implemented"; return nil }

func (b *RedisBroker) RegisterControlEventHandler(h ControlEventHandler) error {
	_ = "STUB: not implemented"
	return nil
}

// PublishControl - see Broker.PublishControl.
func (b *RedisBroker) PublishControl(data []byte, nodeID string, _ string) error {
	_ = "STUB: not implemented"
	return nil
}

func (b *RedisBroker) publishControl(s *shardWrapper, data []byte, nodeID string) error {
	_ = "STUB: not implemented"
	return nil
}

// RegisterBrokerEventHandler – see Broker.RegisterBrokerEventHandler.
func (b *RedisBroker) RegisterBrokerEventHandler(h BrokerEventHandler) error {
	_ = "STUB: not implemented"
	// Run all shards.
	return nil
}

func (b *RedisBroker) checkCapabilities(shard *RedisShard) error {
	_ = "STUB: not implemented"
	return nil

	// Check whether Redis Streams supported.
}

// Check whether Redis Cluster sharded PUB/SUB supported.

// runForever keeps another function running indefinitely.
// The reason this loop is not inside the function itself is
// so that defer can be used to clean-up nicely.
func (b *RedisBroker) runForever(fn func()) { _ = "STUB: not implemented"; return }

// Wait for a while to prevent busy loop when reconnecting to Redis.

func getBaseLogFields(s *shardWrapper) map[string]any { _ = "STUB: not implemented"; return nil }

func (b *RedisBroker) runControlShard(s *shardWrapper, h ControlEventHandler) error {
	_ = "STUB: not implemented"
	return nil
}

func (b *RedisBroker) Close(_ context.Context) error { _ = "STUB: not implemented"; return nil }

func (b *RedisBroker) runControlPubSub(s *RedisShard, logFields map[string]any, eventHandler ControlEventHandler, startOnce func(error)) {
	_ = "STUB: not implemented"
	return
}

// Run workers to spread message processing work over worker goroutines.

// Buffer is full, drop the message. It's expected that PUB/SUB layer
// only provides at most once delivery guarantee.
// Blocking here will block Redis connection read loop which is not a
// good thing and can lead to slower command processing and potentially
// to deadlocks (see https://github.com/redis/rueidis/issues/596).

const (
	controlPubSubProcessorBufferSize = 4096
)

// makePubSubCallbacks builds the pubSubCallbacks used by the pub/sub runner.
func (b *RedisBroker) makePubSubCallbacks(s *shardWrapper) pubSubCallbacks {
	_ = "STUB: not implemented"
	return *new(pubSubCallbacks)
}

func (b *RedisBroker) runPubSub(s *shardWrapper, logFields map[string]any, eventHandler BrokerEventHandler, clusterShardIndex, psShardIndex int, useShardedPubSub bool, startOnce func(error)) {
	_ = "STUB: not implemented"
	return
}

func (b *RedisBroker) useShardedPubSub(s *RedisShard) bool { _ = "STUB: not implemented"; return false }

// Publish - see Broker.Publish.
func (b *RedisBroker) Publish(ch string, data []byte, opts PublishOptions) (PublishResult, error) {
	_ = "STUB: not implemented"
	return *new(PublishResult), nil
}

func (b *RedisBroker) publish(s *shardWrapper, ch string, data []byte, opts PublishOptions) (PublishResult, error) {
	_ = "STUB: not implemented"
	return *new(PublishResult), nil
}

// In no history case we communicate delta flag over Publication field. This field is then
// cleaned up before passing to the Node layer when handling Redis message.

// PublishJoin - see Broker.PublishJoin.
func (b *RedisBroker) PublishJoin(ch string, info *ClientInfo) error {
	_ = "STUB: not implemented"
	return nil
}

func (b *RedisBroker) publishJoin(s *shardWrapper, ch string, info *ClientInfo) error {
	_ = "STUB: not implemented"
	return nil
}

// PublishLeave - see Broker.PublishLeave.
func (b *RedisBroker) PublishLeave(ch string, info *ClientInfo) error {
	_ = "STUB: not implemented"
	return nil
}

func (b *RedisBroker) publishLeave(s *shardWrapper, ch string, info *ClientInfo) error {
	_ = "STUB: not implemented"
	return nil
}

// Subscribe - see Broker.Subscribe.
func (b *RedisBroker) Subscribe(channels ...string) error { _ = "STUB: not implemented"; return nil }

func (b *RedisBroker) subscribe(s *shardWrapper, ch string) error {
	_ = "STUB: not implemented"
	return nil
}

type brokerConnKey struct {
	shardIdx        int
	clusterShardIdx int
	psShardIdx      int
}

type brokerConnGroup struct {
	shard    *shardWrapper
	channels []string
}

func (b *RedisBroker) subscribeBatch(channels []string, unsub bool) error {
	_ = "STUB: not implemented"
	return nil
}

// Track completed groups so we can roll back on partial failure.

// rollbackSubscribeBatch unsubscribes channels from groups that were already
// successfully subscribed. Best-effort: errors are ignored.
func (b *RedisBroker) rollbackSubscribeBatch(groups map[brokerConnKey]*brokerConnGroup, completed []brokerConnKey) {
	_ = "STUB: not implemented"
	return
}

// Unsubscribe - see Broker.Unsubscribe.
func (b *RedisBroker) Unsubscribe(channels ...string) error { _ = "STUB: not implemented"; return nil }

func (b *RedisBroker) unsubscribe(s *shardWrapper, ch string) error {
	_ = "STUB: not implemented"
	return nil
}

// History - see Broker.History.
func (b *RedisBroker) History(ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	_ = "STUB: not implemented"
	return nil, *new(StreamPosition), nil
}

func (b *RedisBroker) history(s *shardWrapper, ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	_ = "STUB: not implemented"
	return nil, *new(StreamPosition), nil
}

// RemoveHistory - see Broker.RemoveHistory.
func (b *RedisBroker) RemoveHistory(ch string) error { _ = "STUB: not implemented"; return nil }

func (b *RedisBroker) removeHistory(s *shardWrapper, ch string) error {
	_ = "STUB: not implemented"
	return nil
}

func (b *RedisBroker) messageChannelID(s *RedisShard, ch string) channelID {
	_ = "STUB: not implemented"
	return *new(channelID)
}

// In cluster mode, always use hash tags to ensure channels and keys (history,
// result cache) are in the same hash slot. This is required for Lua scripts in
// serverless Redis (e.g., AWS Elasticache Serverless) where PUBLISH commands
// inside Lua scripts are slot-affecting. Standard Redis Cluster works fine with
// this since PUBLISH is broadcast to all nodes regardless of hash slot.
// See https://github.com/centrifugal/centrifugo/issues/1087#issuecomment-3667377731

// Build: messagePrefix + "{" + ch + "}"

// Non-cluster path: no hash tags needed

// Sharded pub/sub path: uses partition-based hash tags

// Pre-calculate capacity: messagePrefix + "{" + idxStr + "}." + ch

// pubSubPartitionHashTag returns the Redis Cluster hash tag for the given partition
// index. Used both when constructing PUB/SUB channel names (so messages route
// to a specific slot) and by alternative pub/sub strategies that need to
// determine slot ownership for a partition. Publisher and subscriber must
// agree on the scheme — keep this as the single source of truth.
//
// When UsePrecomputedPartitionTags is enabled, returns the precomputed tag
// for the index (selected for even slot distribution). Otherwise returns
// the bare integer index, preserving backward-compatible behaviour.
func (b *RedisBroker) pubSubPartitionHashTag(partitionIdx int) string {
	_ = "STUB: not implemented"
	return ""
}

func (b *RedisBroker) pubSubShardChannelID(clusterShardIndex int, psShardIndex int, useShardedPubSub bool) channelID {
	_ = "STUB: not implemented"
	return *new(channelID)
}

// Fast path: shardChannel + "." + psShardIndex

// Sharded path: shardChannel + "." + psShardIndex + ".{" + pubSubPartitionHashTag + "}"

func (b *RedisBroker) nodeChannelID(nodeID string) channelID {
	_ = "STUB: not implemented"
	return *new(channelID)
}

func (b *RedisBroker) resultCacheKey(s *RedisShard, ch string, idempotencyKey string) channelID {
	_ = "STUB: not implemented"

	// Fast path: prefix + ".result." + ch + "." + idempotencyKey
	return *new(channelID)
}

// Sharded cluster: prefix + ".result." + "{" + idx + "}." + ch + "." + idempotencyKey

// Non-sharded cluster: prefix + ".result." + "{" + ch + "}" + "." + idempotencyKey

func (b *RedisBroker) historyListKey(s *RedisShard, ch string) channelID {
	_ = "STUB: not implemented"

	// Fast path: prefix + ".list." + ch
	return *new(channelID)
}

// Sharded cluster: prefix + ".list." + "{" + idx + "}." + ch

// Non-sharded cluster: prefix + ".list." + "{" + ch + "}"

func (b *RedisBroker) historyStreamKey(s *RedisShard, ch string) channelID {
	_ = "STUB: not implemented"

	// Fast path: prefix + ".stream." + ch
	return *new(channelID)
}

// Sharded cluster: prefix + ".stream." + "{" + idx + "}." + ch

// Non-sharded cluster: prefix + ".stream." + "{" + ch + "}"

func (b *RedisBroker) historyMetaKey(s *RedisShard, ch string) channelID {
	_ = "STUB: not implemented"
	return *new(channelID)
}

// Fast path: prefix + infix + ch

// Sharded cluster: prefix + infix + "{" + idx + "}." + ch

// Non-sharded cluster: prefix + infix + "{" + ch + "}"

func (b *RedisBroker) extractChannel(isCluster bool, chID channelID) string {
	_ = "STUB: not implemented"
	return ""
}

// Handle sharded PUB/SUB case: {idx}.channel

// Invalid: expected {idx}.channel format

// Invalid: missing dot separator

// Handle cluster case: must have {channel} format

// Non-cluster: plain channel name

// Define prefixes to distinguish Join and Leave messages coming from PUB/SUB.
var (
	joinTypePrefix  = []byte("__j__")
	leaveTypePrefix = []byte("__l__")
)

func (b *RedisBroker) handleRedisClientMessage(isCluster bool, eventHandler BrokerEventHandler, chID channelID, data []byte) error {
	_ = "STUB: not implemented"
	return nil
}

// When adding to history and publishing happens atomically in Broker
// position info is prepended to Publication payload. In this case we should attach
// it to unmarshalled Publication.

// Use Publication's Offset/Epoch for StreamPosition when available (map broker fan-out).
// This takes precedence over metadata prefix since keyed publications carry position in the message.

// Clear — epoch travels in StreamPosition, not in Publication to clients.

// In at most once scenario we are passing delta in Publication itself. But need to clean it
// before passing further.

// PrevData from map broker fan-out: carry previous data for delta computation.

// Clean before passing to Node layer.

func (b *RedisBroker) historyStream(s *RedisShard, ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	_ = "STUB: not implemented"
	return nil, *new(StreamPosition), nil
}

// ex. "4-0", 4 is our offset.

func (b *RedisBroker) historyList(s *RedisShard, ch string, filter HistoryFilter) ([]*Publication, StreamPosition, error) {
	_ = "STUB: not implemented"
	return nil, *new(StreamPosition), nil
}

type pushType int

const (
	pubPushType   pushType = 0
	joinPushType  pushType = 1
	leavePushType pushType = 2
)

var (
	metaSep    = []byte("__")
	contentSep = ":"
)

// See tests for supported format examples.
func extractPushData(data []byte) ([]byte, pushType, StreamPosition, bool, []byte, bool) {
	_ = "STUB: not implemented"
	return nil, *new(pushType), *new(StreamPosition), false, nil, false
}

// __j__payload.

// __l__payload.

// p1:offset:epoch__payload

// offset:epoch

// d1:offset:epoch:prev_payload_length:prev_payload:payload_length:payload

// Unexpected error.

// Unknown content type.

type deltaPublicationPush struct {
	Offset            uint64
	Epoch             string
	PrevPayloadLength int
	PrevPayload       string
	PayloadLength     int
	Payload           string
}

func parseDeltaPush(input string) (deltaPublicationPush, error) {
	_ = "STUB: not implemented"
	// d1:offset:epoch:prev_payload_length:prev_payload:payload_length:payload
	return *new(deltaPublicationPush), nil
}

// Remove prefix

// offset:epoch:prev_payload_length:prev_payload:payload_length:payload

// epoch:prev_payload_length:prev_payload:payload_length:payload

// prev_payload_length:prev_payload:payload_length:payload

// Extract prev_payload based on prev_payload_length

// payload_length:payload

// Extract payload based on payload_length
