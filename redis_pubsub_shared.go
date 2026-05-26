package centrifuge

import (
	"sync"
	"time"

	"github.com/redis/rueidis"
)

const (
	pubSubProcessorBufferSize = 4096
)

// pubSubCallbacks carries the type-varying behavior as function pointers.
// Both RedisBroker and RedisMapBroker provide their own callbacks.
type pubSubCallbacks struct {
	// handleMessage processes a message received from PUB/SUB.
	handleMessage func(isCluster bool, handler BrokerEventHandler, ch string, data []byte) error
	// shardChannelID returns the shard channel ID for a given cluster shard index and pub/sub shard index.
	shardChannelID func(clusterIdx, psIdx int, useShardedPubSub bool) string
	// messageChannelID returns the pub/sub channel name for a given user channel.
	messageChannelID func(ch string) string
	// shardForChannel returns the RedisShard for a given channel (for filtering during resubscribe).
	shardForChannel func(ch string) *RedisShard
}

func getPubSubStartLogFields(s *RedisShard, logFields map[string]any) map[string]any {
	_ = "STUB: not implemented"
	return nil
}

func logResubscribed(node *Node, numChannels int, elapsed time.Duration, logFields map[string]any) {
	_ = "STUB: not implemented"
	return
}

// runPubSubLoop is the unified PUB/SUB loop used by both RedisBroker and RedisMapBroker.
// It handles connection setup, message processing, resubscription, and error handling.
func runPubSubLoop(
	shard *RedisShard,
	subClientsMu *sync.Mutex,
	subClients [][]rueidis.DedicatedClient,
	cb pubSubCallbacks,
	node *Node,
	name string,
	subscribeOnReplica bool,
	numProcessors, numResubscribeShards, numSubscribeShards, numPartitions int,
	logFields map[string]any,
	eventHandler BrokerEventHandler,
	clusterShardIndex, psShardIndex int,
	useShardedPubSub bool,
	startOnce func(error),
) {
	_ = "STUB: not implemented"
	return
}

// Run PUB/SUB message processors to spread received message processing work over worker goroutines.

// Buffer monitoring goroutine.

// Buffer is full, drop the message. It's expected that PUB/SUB layer
// only provides at most once delivery guarantee.
// Blocking here will block Redis connection read loop which is not a
// good thing and can lead to slower command processing and potentially
// to deadlocks (see https://github.com/redis/rueidis/issues/596).

// Helps to handle slot migration.

// Compare-and-swap: only nil the slot if it still holds OUR
// conn. A subsequent run of this same loop (after topology
// rebuild closed our `done`) may have already written its own
// fresh conn into this slot before our defer fires. Without
// the equality check, our nil write would clobber a live
// connection.
