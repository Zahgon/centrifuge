package centrifuge

import (
	"context"
	"sync"
	"time"

	_ "embed"

	"github.com/redis/rueidis"
)

var (
	//go:embed internal/redis_lua/map_broker_add.lua
	brokerStatePublishScriptSource string
	//go:embed internal/redis_lua/map_broker_read_ordered.lua
	brokerStateReadOrderedScriptSource string
	//go:embed internal/redis_lua/map_broker_read_unordered.lua
	brokerStateReadUnorderedScriptSource string
	//go:embed internal/redis_lua/map_broker_stream_read.lua
	brokerStateReadStreamScriptSource string
	//go:embed internal/redis_lua/map_broker_read_meta.lua
	brokerStateReadMetaScriptSource string
	//go:embed internal/redis_lua/map_broker_stats.lua
	brokerStateStatsScriptSource string
	//go:embed internal/redis_lua/map_broker_find_expired.lua
	brokerStateFindExpiredScriptSource string
	//go:embed internal/redis_lua/map_broker_batch_remove.lua
	brokerStateBatchRemoveScriptSource string
)

type brokerShardWrapper struct {
	shard               *RedisShard
	subClientsMu        sync.Mutex
	subClients          [][]rueidis.DedicatedClient
	pubSubStartChannels [][]*pubSubStart
	pubSubRunner        mapBrokerPubSubRunner
}

// mapBrokerPubSubRunner abstracts the subscriber-side pub/sub strategy for
// RedisMapBroker. Mirrors brokerPubSubRunner; one instance per shard, selected
// at construction time. subClientsIndex maps a partition-derived cluster shard
// index to the first-dimension index used in brokerShardWrapper.subClients
// (default is identity; non-default strategies may remap, e.g. partition → node).
type mapBrokerPubSubRunner interface {
	init(s *brokerShardWrapper, shard *RedisShard) error
	run(s *brokerShardWrapper, h BrokerEventHandler) error
	subClientsIndex(clusterShardIdx int) int
}

// RedisMapBroker is a Redis-based MapBroker.
// Note – it does not work properly with Redis eviction, use with disabled eviction
// to avoid undefined state.
//
// Message Formats
// ===============
//
// This broker uses simplified message formats published by Lua scripts:
//
// 1. No prefix:
//   - Raw protobuf bytes (Publication)
//   - Used by direct PUBLISH calls without Lua scripts when stream is not used.
//
// 2. Non-delta publications:
//   - Format: "offset:epoch:Publication"
//   - Where Publication is in protobuf format.
//
// 3. Delta publications:
//   - Format: "d:offset:epoch:prev_len:prev_publication:curr_len:curr_publication"
//   - Where prev_publication and curr_publication are protocol.Publication in protobuf.
//   - Enables atomic publishing of current + previous publication for delta compression of publication data.
//   - Prev is atomically fetched from stream.
//
// Storage:
// - Streams (XADD): keeps protocol.Publication
// - States (HSET): For keyed state - may keep latest protocol.Publication or custom state.
//
// Pagination:
//   - ordered state use ZRANGEBYSCORE/ZRANGEBYLEX with LIMIT — exact page sizes.
//   - Unordered state use HSCAN with COUNT — COUNT is only a hint, Redis may return
//     more entries than requested (especially for small hashes in listpack encoding).
//     Callers should not rely on exact Limit enforcement for unordered reads.
type RedisMapBroker struct {
	node *Node
	conf RedisMapBrokerConfig

	shards []*brokerShardWrapper

	// partitionTags is non-nil when UsePrecomputedPartitionTags is enabled;
	// indexed by partition index, returns the hash tag string. Read-only
	// after construction (shared with the package-level precomputed table).
	partitionTags []string

	addScript           *rueidis.Lua
	readOrderedScript   *rueidis.Lua
	readUnorderedScript *rueidis.Lua
	readStreamScript    *rueidis.Lua
	readMetaScript      *rueidis.Lua
	presenceStatsScript *rueidis.Lua
	findExpiredScript   *rueidis.Lua
	batchRemoveScript   *rueidis.Lua

	closeCh       chan struct{}
	closeOnce     sync.Once
	shardChannel  string
	messagePrefix string
}

var _ MapBroker = (*RedisMapBroker)(nil)

// RedisMapBrokerConfig is a config for RedisMapBroker.
type RedisMapBrokerConfig struct {
	// Shards is a slice of RedisShard to use. At least one shard must be provided.
	Shards []*RedisShard
	// Prefix to use before every channel name and key in Redis.
	Prefix string
	// Name of broker, for observability purposes – i.e. becomes part of metrics/logs labels.
	// By default, empty string is used.
	Name string
	// LoadSHA1 enables loading SHA1 from Redis via SCRIPT LOAD instead of calculating
	// it on the client side. This is useful for FIPS compliance.
	LoadSHA1 bool
	// IdempotentResultTTL is a time-to-live for idempotent result.
	IdempotentResultTTL time.Duration
	// SubscribeOnReplica allows subscribing on replica Redis nodes.
	SubscribeOnReplica bool
	// SkipPubSub enables mode when broker only works with data structures, without
	// publishing to channels and using PUB/SUB.
	SkipPubSub bool
	// NumShardedPubSubPartitions when greater than zero allows turning on a mode in which
	// broker will use Redis Cluster with sharded PUB/SUB feature available in
	// Redis >= 7: https://redis.io/docs/manual/pubsub/#sharded-pubsub
	NumShardedPubSubPartitions int

	// UsePrecomputedPartitionTags switches sharded PUB/SUB partition hash tags
	// from the bare partition index ("0", "1", ...) to a precomputed table
	// chosen so CRC16 hash slots distribute evenly across any cluster size.
	// See RedisBrokerConfig.UsePrecomputedPartitionTags for details and
	// migration constraints.
	//
	// When enabled, NumShardedPubSubPartitions must equal one of the sizes
	// returned by redispartition.PrecomputedSizes() (16, 32, 64, 128, 256,
	// 512, 1024, 2048, 4096). Construction returns an error otherwise. The
	// exact tag table is bundled in internal/redispartition/precomputed.go;
	// the tags are stable and will not change.
	//
	// Default: false (backward-compatible bare-index tags).
	UsePrecomputedPartitionTags bool

	// numSubscribeShards defines how many subscribe shards will be used.
	numSubscribeShards int
	// numResubscribeShards defines how many subscriber goroutines will be used for
	// resubscribing process for each subscribe shard.
	numResubscribeShards int
	// numPubSubProcessors allows configuring number of workers which will process
	// messages coming from Redis PUB/SUB.
	numPubSubProcessors int

	// CleanupInterval defines how often to run the cleanup worker that
	// generates remove events for expired keyed state entries (presence and state).
	// Default is 1 second. Set to -1 to disable cleanup (make sure you understand the consequences).
	// Applies to all channels using TTL-based state.
	CleanupInterval time.Duration
	// CleanupBatchSize defines max entries to process per channel per cleanup cycle.
	// Default is 100. Applies to all keyed state (presence and state).
	CleanupBatchSize int
}

// NewRedisMapBroker initializes RedisMapBroker.
func NewRedisMapBroker(n *Node, conf RedisMapBrokerConfig) (*RedisMapBroker, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// parseStateValue parses a state value in format: offset:epoch:payload
// Returns offset, epoch, payload, and error if parsing fails.
func parseStateValue(val []byte) (uint64, string, []byte, error) {
	_ = "STUB: not implemented"
	return 0, "", nil, nil
}

// Find first colon (offset separator)

// Parse offset

// Find second colon (epoch separator)

// Extract epoch

// Everything after second colon is payload

func (e *RedisMapBroker) useShardedPubSub(s *RedisShard) bool {
	_ = "STUB: not implemented"
	return false
}

func (e *RedisMapBroker) getShard(channel string) *brokerShardWrapper {
	_ = "STUB: not implemented"
	return nil
}

func (e *RedisMapBroker) streamKey(s *RedisShard, ch string) string {
	_ = "STUB: not implemented"
	return ""
}

func (e *RedisMapBroker) metaKey(s *RedisShard, ch string) string {
	_ = "STUB: not implemented"
	return ""
}

func (e *RedisMapBroker) stateHashKey(s *RedisShard, ch string) string {
	_ = "STUB: not implemented"
	return ""
}

func (e *RedisMapBroker) stateOrderKey(s *RedisShard, ch string) string {
	_ = "STUB: not implemented"
	return ""
}

func (e *RedisMapBroker) stateExpireKey(s *RedisShard, ch string) string {
	_ = "STUB: not implemented"
	return ""
}

func (e *RedisMapBroker) stateMetaKey(s *RedisShard, ch string) string {
	_ = "STUB: not implemented"
	return ""
}

func (e *RedisMapBroker) cleanupRegistrationKeyForChannel(s *RedisShard, ch string) string {
	_ = "STUB: not implemented"
	// Get the cleanup registration key with proper hash tag for the channel's partition
	// Single registration ZSET works for ALL keyed state (presence, state, etc.)
	return ""
}

func (e *RedisMapBroker) resultCacheKey(s *RedisShard, ch string, idempotencyKey string) string {
	_ = "STUB: not implemented"
	return ""
}

// buildKey is a helper function to build Redis keys with proper cluster hash tag support
func (e *RedisMapBroker) buildKey(s *RedisShard, ch string, infix string) string {
	_ = "STUB: not implemented"
	return ""
}

func (e *RedisMapBroker) messageChannelID(s *RedisShard, ch string) string {
	_ = "STUB: not implemented"
	return ""
}

// Close closes the broker.
func (e *RedisMapBroker) Close(_ context.Context) error { _ = "STUB: not implemented"; return nil }

func (e *RedisMapBroker) Clear(ctx context.Context, ch string, _ MapClearOptions) error {
	_ = "STUB: not implemented"
	return nil
}

func boolToStr(b bool) string { _ = "STUB: not implemented"; return "" }

// millis converts a duration to a milliseconds string.
// Zero/negative returns "0".
func millis(d time.Duration) string { _ = "STUB: not implemented"; return "" }

// Publish publishes data to a stateful channel with optional keyed state.
func (e *RedisMapBroker) Publish(ctx context.Context, ch string, key string, opts MapPublishOptions) (MapUpdateResult, error) {
	_ = "STUB: not implemented"
	return *new(MapUpdateResult), nil
}

// Resolve channel options once for this operation.

// Reject CAS and Version in ephemeral mode.

// Fast path for non-history, non-idempotent, non-keyed publications.

// Stream publication (used for stream and pub/sub).

// stateBytes is nil — Lua will use streamBytes for both state and stream.

// State meta key tracks epoch for consistency between state and stream.
// In streamless mode, skip it to prevent multi-node epoch mismatch clearing state.

// Setup cleanup registration if KeyTTL is set (keyed state with expiration)

// Prepare ExpectedPosition arguments for CAS

// In Redis Cluster, all KEYS in a Lua script must hash to the same slot. Empty string
// keys hash to slot 0, which differs from the hash-tagged real keys. Compute a slot-
// aligned nil key placeholder and substitute it for any empty KEYS. The Lua script
// converts these back to '' using ARGV[23].

// Pre-compute per-key version field names for Lua.

// message_key
// message_payload (Publication - for stream and publishing)

// channel (for Lua to publish)

// new_epoch_if_empty

// is_remove

// use_hpexpire
// channel_for_cleanup (for cleanup registration)
// key_mode
// refresh_ttl_on_suppress
// expected_offset (for CAS)
// expected_epoch (for CAS)
// state_payload (for state storage, empty to use message_payload)
// nil_key (slot-aligned placeholder for unused KEYS)
// version_field (pre-computed "v:KEY" or "")
// version_epoch_field (pre-computed "ve:KEY" or "")
// now (current time in milliseconds)

// Remove removes a key from keyed state state.
func (e *RedisMapBroker) Remove(ctx context.Context, ch string, key string, opts MapRemoveOptions) (MapUpdateResult, error) {
	_ = "STUB: not implemented"
	return *new(MapUpdateResult), nil
}

// Resolve channel options once for this operation.

// Reject CAS in ephemeral mode.

// For unpublish, we use state keys to track which keys exist

// State meta key tracks epoch for consistency between state and stream.
// In streamless mode, skip it to prevent multi-node epoch mismatch clearing state.

// Create a Publication with key and removed=true to signal removal.
// Include opts.Tags so server-side tags filtering can route the removal correctly.

// Handle idempotency key for remove operations.

// Prepare ExpectedPosition arguments for CAS

// Compute slot-aligned nil key for unused KEYS in cluster mode (see Publish for details).

// Pre-compute per-key version field names for cleanup on remove.

// message_key
// message_payload (Publication with Removed=true for stream)

// channel (for Lua to publish)

// new_epoch_if_empty

// result_key_expire
// use_delta, version, version_epoch
// is_leave (this triggers removal)
// score
// map_member_ttl
// use_hpexpire
// channel_for_cleanup (not used for unpublish)
// key_mode (not used for unpublish)
// refresh_ttl_on_suppress (not used for unpublish)
// expected_offset (for CAS)
// expected_epoch (for CAS)
// state_payload (not used for unpublish)
// nil_key (slot-aligned placeholder for unused KEYS)
// version_field (pre-computed "v:KEY" or "")
// version_epoch_field (pre-computed "ve:KEY" or "")
// now (current time in milliseconds)

// ReadState retrieves state entries with per-entry revisions for a channel.
// Each entry includes its revision so client can filter: entry.Revision <= state_revision.
// If opts.Revision is provided and epoch changed, returns empty entries.
// Returns entries, stream position, next cursor for pagination, and error.
// Cursor "0" or "" means end of iteration.
func (e *RedisMapBroker) ReadState(ctx context.Context, ch string, opts MapReadStateOptions) (MapStateResult, error) {
	_ = "STUB: not implemented"
	// Resolve channel options once for this operation.
	return *new(MapStateResult), nil
}

// Handle single key lookup (Key filter) — takes priority over Limit.

// Limit=0: return only stream position (no entries).

//func (e *RedisMapBroker) ReadStateZero(ctx context.Context, ch string, opts MapReadStateOptions) (MapStateResult, error) {
//	// Resolve channel options once for this operation.
//	chOpts, err := ResolveAndValidateMapChannelOptions(e.node.config.Map.GetMapChannelOptions, ch)
//	if err != nil {
//		return MapStateResult{}, err
//	}
//
//	// Handle single key lookup (Key filter) — takes priority over Limit.
//	if opts.Key != "" {
//		return e.readSingleKeyWithOpts(ctx, ch, opts, chOpts)
//	}
//	// Limit=0: return only stream position (no entries).
//	if opts.Limit == 0 {
//		streamResult, err := e.ReadStream(ctx, ch, MapReadStreamOptions{
//			Filter: StreamFilter{Limit: 0},
//		})
//		if err != nil {
//			return MapStateResult{}, err
//		}
//		return MapStateResult{Position: streamResult.Position}, nil
//	}
//	if chOpts.ordered {
//		return e.readOrderedState(ctx, ch, opts, chOpts)
//	}
//	return e.readUnorderedStateZero(ctx, ch, opts, chOpts)
//}

// readSingleKeyWithOpts retrieves a single key from the state using HGET instead of HSCAN.
// This is more efficient for single key lookups and supports CAS read-modify-write patterns.
func (e *RedisMapBroker) readSingleKeyWithOpts(ctx context.Context, ch string, opts MapReadStateOptions, chOpts MapChannelOptions) (MapStateResult, error) {
	_ = "STUB: not implemented"
	return *new(MapStateResult), nil
}

// Streamless mode: just read the key, no meta needed.

// Execute HGET and metadata read in pipeline

// Parse HGET result

// Parse metadata

// Validate epoch if client provided state revision

// Key not found - return empty

// Parse value: offset:epoch:payload

// Unmarshal Publication from protobuf payload

func (e *RedisMapBroker) readUnorderedState(ctx context.Context, ch string, opts MapReadStateOptions, chOpts MapChannelOptions) (MapStateResult, error) {
	_ = "STUB: not implemented"
	return *new(MapStateResult), nil
}

// Cursor must be "0" to start for Redis HSCAN

// In Lua scripts, limit=0 means "return all", limit>0 means paginate.
// Convert negative limit (e.g. -1 = all) to 0 for Lua.

// Convert Redis HSCAN "0" (complete) to empty string

// Validate epoch if client provided state revision

// Epoch changed, client needs to restart from beginning

// Parse state values with revisions

// Parse value: offset:epoch:payload

// Skip malformed entries

// Unmarshal Publication from protobuf payload

// Skip malformed entries

//
//func (e *RedisMapBroker) readUnorderedStateZero(
//	ctx context.Context,
//	ch string,
//	opts MapReadStateOptions,
//	chOpts MapChannelOptions,
//) (MapStateResult, error) {
//	s := e.getShard(ch)
//
//	stateTTL := "0"
//	if chOpts.MetaTTL > 0 {
//		stateTTL = millis(chOpts.MetaTTL)
//	}
//
//	cursor := opts.Cursor
//	if cursor == "" {
//		cursor = "0"
//	}
//
//	var (
//		streamPos  StreamPosition
//		nextCursor string
//		pubs       []*Publication
//	)
//
//	streamlessFlag := "0"
//	if chOpts.Mode.IsEphemeral() {
//		streamlessFlag = "1"
//	}
//
//	// In Lua scripts, limit=0 means "return all", limit>0 means paginate.
//	luaLimit := opts.Limit
//	if luaLimit < 0 {
//		luaLimit = 0
//	}
//
//	err := e.readUnorderedScript.ExecWithReader(
//		ctx,
//		s.shard.client,
//		[]string{
//			e.stateHashKey(s.shard, ch),
//			e.stateExpireKey(s.shard, ch),
//			e.metaKey(s.shard, ch),
//			e.stateMetaKey(s.shard, ch),
//		},
//		[]string{
//			cursor,
//			strconv.Itoa(luaLimit),
//			strconv.FormatInt(time.Now().UnixMilli(), 10),
//			millis(chOpts.MetaTTL),
//			stateTTL,
//			streamlessFlag,
//		},
//		func(r *bufio.Reader) error {
//			rr := resp.NewReader(r)
//			if err := rr.ExpectArrayWithLen(4); err != nil {
//				return err
//			}
//
//			// --- reply[0]: offset ---
//			offsetBytes, err := rr.ReadStringBytes()
//			if err != nil {
//				return err
//			}
//			if offsetBytes != nil {
//				streamPos.Offset, err = strconv.ParseUint(
//					unsafe.String(&offsetBytes[0], len(offsetBytes)), 10, 64,
//				)
//				if err != nil {
//					return err
//				}
//			}
//
//			// --- reply[1]: epoch ---
//			epochBytes, err := rr.ReadStringBytes()
//			if err != nil {
//				return err
//			}
//			if epochBytes != nil {
//				streamPos.Epoch = unsafe.String(&epochBytes[0], len(epochBytes))
//			}
//
//			// --- reply[2]: cursor ---
//			cursorBytes, err := rr.ReadStringBytes()
//			if err != nil && !errors.Is(err, rueidis.Nil) {
//				return err
//			}
//			if cursorBytes != nil {
//				nextCursor = unsafe.String(&cursorBytes[0], len(cursorBytes))
//			}
//			if nextCursor == "0" {
//				nextCursor = "" // Convert Redis HSCAN "0" (complete) to empty string
//			}
//
//			// --- reply[3]: key-value array ---
//			kvCount, err := rr.ExpectArray()
//			if err != nil {
//				return err
//			}
//			pubs = make([]*Publication, 0, kvCount/2)
//
//			for i := int64(0); i < kvCount; i += 2 {
//				// Read next key
//				keyBytes, err := rr.ReadStringBytes()
//				if err != nil {
//					return err
//				}
//				// Make a safe copy
//				key := convert.BytesToString(append([]byte(nil), keyBytes...))
//
//				// Read corresponding value
//				valBytes, err := rr.ReadStringBytes()
//				if err != nil {
//					return err
//				}
//				// Parse value: offset:epoch:payload
//				entryOffset, _, payloadBytes, err := parseStateValue(valBytes)
//				if err != nil {
//					// skip malformed entries
//					continue
//				}
//				// Unmarshal Publication from protobuf payload
//				var protoPub protocol.Publication
//				if err := protoPub.UnmarshalVT(payloadBytes); err != nil {
//					// Skip malformed entries
//					continue
//				}
//
//				pub := pubFromProto(&protoPub)
//				pub.Key = key
//				pub.Offset = entryOffset
//				pubs = append(pubs, pub)
//			}
//
//			return nil
//		},
//	)
//
//	if err != nil {
//		return MapStateResult{}, err
//	}
//
//	// Validate state revision (unchanged semantics)
//	if opts.Revision != nil &&
//		opts.Revision.Epoch != streamPos.Epoch {
//		return MapStateResult{Position: streamPos, Cursor: nextCursor}, ErrorUnrecoverablePosition
//	}
//
//	return MapStateResult{Publications: pubs, Position: streamPos, Cursor: nextCursor}, nil
//}

// readOrderedState uses key-based cursor pagination for continuity.
// Cursor format: "score\x00key" to ensure no entries are skipped during concurrent modifications.
func (e *RedisMapBroker) readOrderedState(ctx context.Context, ch string, opts MapReadStateOptions, chOpts MapChannelOptions) (MapStateResult, error) {
	_ = "STUB: not implemented"
	return *new(MapStateResult), nil
}

// Parse cursor: "score\x00key" format

// In Lua scripts, limit=0 means "return all", limit>0 means paginate.

// ARGV[1] = limit
// ARGV[2] = cursor_score
// ARGV[3] = cursor_key
// ARGV[4] = now
// ARGV[5] = meta_ttl
// ARGV[6] = state_ttl
// ARGV[7] = streamless
// ARGV[8] = asc ("1" = ascending, "0" = descending)

// Validate epoch if client provided state revision

// Epoch changed, client needs to restart from beginning

// Parse state values with revisions

// Parse value: offset:epoch:payload

// Skip malformed entries

// Unmarshal Publication from protobuf payload

// Skip malformed entries

// Build cursor for next page

//func (e *RedisMapBroker) ReadStreamZero(
//	ctx context.Context,
//	ch string,
//	opts MapReadStreamOptions,
//) (MapStreamResult, error) {
//	s := e.getShard(ch)
//
//	chOpts, err := ResolveAndValidateMapChannelOptions(e.node.config.Map.GetMapChannelOptions, ch)
//	if err != nil {
//		return MapStreamResult{}, err
//	}
//
//	var includePubs = true
//	var offset string
//
//	if opts.Filter.Since != nil {
//		if opts.Filter.Reverse {
//			if opts.Filter.Since.Offset == 0 {
//				includePubs = false
//			} else {
//				offset = strconv.FormatUint(opts.Filter.Since.Offset-1, 10)
//			}
//		} else {
//			offset = strconv.FormatUint(opts.Filter.Since.Offset+1, 10)
//		}
//	} else {
//		offset = "-"
//		if opts.Filter.Reverse {
//			offset = "+"
//		}
//	}
//
//	limit := opts.Filter.Limit
//	if limit == 0 {
//		includePubs = false
//	}
//	if limit < 0 {
//		limit = 0
//	}
//
//	reverse := "0"
//	if opts.Filter.Reverse {
//		reverse = "1"
//	}
//
//	metaExpire := "0"
//	if chOpts.MetaTTL > 0 {
//		metaExpire = millis(chOpts.MetaTTL)
//	}
//
//	includePubsStr := "0"
//	if includePubs {
//		includePubsStr = "1"
//	}
//
//	var (
//		streamPos StreamPosition
//		pubs      []*Publication
//	)
//
//	err = e.readStreamScript.ExecWithReader(
//		ctx,
//		s.shard.client,
//		[]string{
//			e.streamKey(s.shard, ch),
//			e.metaKey(s.shard, ch),
//		},
//		[]string{
//			includePubsStr,
//			offset,
//			strconv.Itoa(limit),
//			reverse,
//			metaExpire,
//			e.node.ID(),
//		},
//		func(r *bufio.Reader) error {
//			rr := resp.NewReader(r)
//
//			// ---- top-level reply ----
//			n, err := rr.ExpectArray()
//			if err != nil {
//				return err
//			}
//			if n < 2 {
//				return fmt.Errorf("wrong number of replies: %d", n)
//			}
//
//			// ---- reply[0]: top offset ----
//			switch rr.PeekKind() {
//			case resp.KindInt:
//				v, err := rr.ReadInt64()
//				if err != nil {
//					return err
//				}
//				streamPos.Offset = uint64(v)
//			case resp.KindString:
//				buf, err := rr.ReadStringBytes()
//				if err != nil {
//					return err
//				}
//				v, err := strconv.ParseUint(
//					unsafe.String(&buf[0], len(buf)),
//					10,
//					64,
//				)
//				if err != nil {
//					return err
//				}
//				streamPos.Offset = v
//			default:
//				return fmt.Errorf("unexpected RESP kind for offset")
//			}
//
//			buf, err := rr.ReadStringBytes()
//			if err != nil {
//				return err
//			}
//			// Make a safe copy of the bytes.
//			safeBuf := make([]byte, len(buf))
//			copy(safeBuf, buf)
//			streamPos.Epoch = convert.BytesToString(safeBuf)
//
//			// Validate epoch if provided in Since filter.
//			if opts.Filter.Since != nil && opts.Filter.Since.Epoch != "" && opts.Filter.Since.Epoch != streamPos.Epoch {
//				return ErrorUnrecoverablePosition
//			}
//
//			if !includePubs || n < 3 {
//				return nil
//			}
//
//			// ---- reply[2]: publications ----
//			pubCount, err := rr.ExpectArray()
//			if err != nil {
//				return err
//			}
//
//			pubs = make([]*Publication, 0, pubCount)
//
//			for i := int64(0); i < pubCount; i++ {
//				// entry = [id, fields]
//				if _, err := rr.ExpectArray(); err != nil {
//					return err
//				}
//
//				// ---- id ----
//				idBuf, err := rr.ReadStringBytes()
//				if err != nil {
//					return err
//				}
//
//				idStr := unsafe.String(&idBuf[0], len(idBuf))
//				hyphen := strings.IndexByte(idStr, '-')
//				if hyphen <= 0 {
//					return fmt.Errorf("invalid stream id")
//				}
//
//				pubOffset, err := strconv.ParseUint(idStr[:hyphen], 10, 64)
//				if err != nil {
//					return err
//				}
//
//				// ---- fields ----
//				fieldCount, err := rr.ExpectArray()
//				if err != nil {
//					return err
//				}
//
//				var payload *bpool.ByteBuffer
//
//				for j := int64(0); j < fieldCount; j += 2 {
//					// key
//					key, err := rr.ReadStringBytes()
//					if err != nil {
//						return err
//					}
//					isData := len(key) == 1 && key[0] == 'd'
//
//					// value
//					if isData {
//						vbuf, err := rr.ReadStringBytes()
//						if err != nil {
//							return err
//						}
//						payload = bpool.GetByteBuffer(len(vbuf))
//						payload.B = payload.B[:len(vbuf)]
//						copy(payload.B, vbuf)
//					} else {
//						if err := rr.SkipValue(); err != nil {
//							return err
//						}
//					}
//				}
//
//				if payload == nil {
//					return errors.New("no payload data found in entry")
//				}
//
//				var protoPub protocol.Publication
//				if err := protoPub.UnmarshalVT(payload.B); err != nil {
//					bpool.PutByteBuffer(payload)
//					return err
//				}
//				bpool.PutByteBuffer(payload)
//
//				protoPub.Offset = pubOffset
//				pubs = append(pubs, &Publication{
//					Offset:  protoPub.Offset,
//					Data:    protoPub.Data,
//					Info:    infoFromProto(protoPub.GetInfo()),
//					Tags:    protoPub.GetTags(),
//					Time:    protoPub.Time,
//					Key:     protoPub.GetKey(),
//					Removed: protoPub.GetRemoved(),
//					Score:   protoPub.GetScore(),
//				})
//			}
//			return nil
//		},
//	)
//
//	if err != nil {
//		return MapStreamResult{}, err
//	}
//
//	return MapStreamResult{Publications: pubs, Position: streamPos}, nil
//}

// ReadStreamZero2 is a 2-call version of ReadStreamZero with zero-alloc optimizations.
// Call 1: Get metadata (epoch, top_offset) using simpler Lua script with ExecWithReader
// Call 2: Read publications using native XRANGE/XREVRANGE with DoWithReader
// Key difference: Must filter out publications with offset > top_offset (non-atomic).
//func (e *RedisMapBroker) ReadStreamZero2(ctx context.Context, ch string, opts MapReadStreamOptions) (MapStreamResult, error) {
//	s := e.getShard(ch)
//
//	chOpts, err := ResolveAndValidateMapChannelOptions(e.node.config.Map.GetMapChannelOptions, ch)
//	if err != nil {
//		return MapStreamResult{}, err
//	}
//
//	// 1. Parse options (same as ReadStream/ReadStreamZero)
//	var includePubs = true
//	var offset string
//	if opts.Filter.Since != nil {
//		if opts.Filter.Reverse {
//			if opts.Filter.Since.Offset == 0 {
//				includePubs = false
//			} else {
//				offset = strconv.FormatUint(opts.Filter.Since.Offset-1, 10)
//			}
//		} else {
//			offset = strconv.FormatUint(opts.Filter.Since.Offset+1, 10)
//		}
//	} else {
//		offset = "-"
//		if opts.Filter.Reverse {
//			offset = "+"
//		}
//	}
//
//	limit := opts.Filter.Limit
//	if limit == 0 {
//		includePubs = false
//	}
//	if limit < 0 {
//		limit = 0
//	}
//
//	metaExpire := "0"
//	if chOpts.MetaTTL > 0 {
//		metaExpire = millis(chOpts.MetaTTL)
//	}
//
//	// 2. Call 1: Get metadata with ExecWithReader (zero-alloc)
//	var streamPos StreamPosition
//
//	err = e.readMetaScript.ExecWithReader(
//		ctx,
//		s.shard.client,
//		[]string{e.metaKey(s.shard, ch)},
//		[]string{metaExpire, e.node.ID()},
//		func(r *bufio.Reader) error {
//			rr := resp.NewReader(r)
//
//			// Top-level must be an array of at least 2 elements
//			n, err := rr.ExpectArray()
//			if err != nil {
//				return err
//			}
//			if n < 2 {
//				return fmt.Errorf("wrong number of replies: %d", n)
//			}
//
//			// ---- reply[0]: top offset ----
//			switch rr.PeekKind() {
//			case resp.KindInt:
//				v, err := rr.ReadInt64()
//				if err != nil && !errors.Is(err, rueidis.Nil) {
//					return err
//				}
//				streamPos.Offset = uint64(v)
//			case resp.KindString:
//				b, err := rr.ReadStringBytes()
//				if err != nil {
//					return err
//				}
//				if len(b) > 0 {
//					v, err := strconv.ParseUint(unsafe.String(&b[0], len(b)), 10, 64)
//					if err != nil {
//						return err
//					}
//					streamPos.Offset = v
//				}
//			default:
//				return fmt.Errorf("unexpected RESP type for offset: %q", rr.PeekKind())
//			}
//
//			// ---- reply[1]: epoch ----
//			buf, err := rr.ReadStringBytes()
//			if err != nil {
//				return err
//			}
//			// Make a safe copy of the bytes.
//			safeBuf := make([]byte, len(buf))
//			copy(safeBuf, buf)
//			streamPos.Epoch = convert.BytesToString(safeBuf)
//
//			return nil
//		},
//	)
//	if err != nil {
//		return MapStreamResult{}, err
//	}
//
//	// Validate epoch if provided in Since filter.
//	if opts.Filter.Since != nil && opts.Filter.Since.Epoch != "" && opts.Filter.Since.Epoch != streamPos.Epoch {
//		return MapStreamResult{}, ErrorUnrecoverablePosition
//	}
//
//	// 3. Early return if metadata-only
//	if !includePubs {
//		return MapStreamResult{Position: streamPos}, nil
//	}
//
//	topOffset := streamPos.Offset
//
//	// 4. Call 2: Execute XRANGE with DoWithReader (zero-alloc)
//	streamKey := e.streamKey(s.shard, ch)
//	var cmd rueidis.Completed
//
//	if opts.Filter.Reverse {
//		builder := s.shard.client.B().Xrevrange().Key(streamKey).End(offset).Start("-")
//		if limit > 0 {
//			cmd = builder.Count(int64(limit)).Build()
//		} else {
//			cmd = builder.Build()
//		}
//	} else {
//		builder := s.shard.client.B().Xrange().Key(streamKey).Start(offset).End("+")
//		if limit > 0 {
//			cmd = builder.Count(int64(limit)).Build()
//		} else {
//			cmd = builder.Build()
//		}
//	}
//
//	var pubs []*Publication
//
//	err = s.shard.client.DoWithReader(ctx, cmd, func(r *bufio.Reader) error {
//		rr := resp.NewReader(r)
//
//		// Top-level must be an array
//		entryCount, err := rr.ExpectArray()
//		if err != nil {
//			return err
//		}
//
//		if entryCount > 0 {
//			pubs = make([]*Publication, 0, entryCount)
//		}
//
//		for i := int64(0); i < entryCount; i++ {
//			// Each entry = [id, fields]
//			if _, err := rr.ExpectArray(); err != nil {
//				return err
//			}
//
//			// ---- id ----
//			idBytes, err := rr.ReadStringBytes()
//			if err != nil {
//				return err
//			}
//			idStr := unsafe.String(&idBytes[0], len(idBytes))
//			hyphen := strings.IndexByte(idStr, '-')
//			if hyphen <= 0 {
//				return fmt.Errorf("invalid stream id")
//			}
//
//			pubOffset, err := strconv.ParseUint(idStr[:hyphen], 10, 64)
//			if err != nil {
//				return err
//			}
//
//			// Filter entries written after metadata read
//			if pubOffset > topOffset {
//				if err := rr.SkipValue(); err != nil { // skip fields array
//					return err
//				}
//				continue
//			}
//
//			// ---- fields ----
//			fieldCount, err := rr.ExpectArray()
//			if err != nil {
//				return err
//			}
//
//			var payload *bpool.ByteBuffer
//
//			for j := int64(0); j < fieldCount; j += 2 {
//				// key
//				keyBytes, err := rr.ReadStringBytes()
//				if err != nil {
//					return err
//				}
//				isData := len(keyBytes) == 1 && keyBytes[0] == 'd'
//
//				// value
//				valBytes, err := rr.ReadStringBytes()
//				if err != nil {
//					return err
//				}
//
//				if isData {
//					payload = bpool.GetByteBuffer(len(valBytes))
//					payload.B = payload.B[:len(valBytes)]
//					copy(payload.B, valBytes)
//				}
//			}
//
//			if payload == nil {
//				return errors.New("no payload data found in entry")
//			}
//
//			var protoPub protocol.Publication
//			if err := protoPub.UnmarshalVT(payload.B); err != nil {
//				bpool.PutByteBuffer(payload)
//				return err
//			}
//			bpool.PutByteBuffer(payload)
//
//			protoPub.Offset = pubOffset
//			pubs = append(pubs, &Publication{
//				Offset:  protoPub.Offset,
//				Data:    protoPub.Data,
//				Info:    infoFromProto(protoPub.GetInfo()),
//				Tags:    protoPub.GetTags(),
//				Time:    protoPub.Time,
//				Key:     protoPub.GetKey(),
//				Removed: protoPub.GetRemoved(),
//				Score:   protoPub.GetScore(),
//			})
//		}
//
//		return nil
//	})
//
//	if err != nil {
//		return MapStreamResult{}, err
//	}
//
//	return MapStreamResult{Publications: pubs, Position: streamPos}, nil
//}

// ReadStream retrieves publication stream for a channel.
func (e *RedisMapBroker) ReadStream(ctx context.Context, ch string, opts MapReadStreamOptions) (MapStreamResult, error) {
	_ = "STUB: not implemented"
	return *new(MapStreamResult), nil
}

// Use offset+1 because XRANGE is inclusive, but "since" should be exclusive

// 0 means "get all" in the Lua script

// Validate epoch if provided in Since filter.

// entry[1] is an array of [field, value, field, value, ...]

// ReadStream2 is a 2-call version of ReadStream that splits metadata and publication reads.
// Call 1: Get metadata (epoch, top_offset) using simpler Lua script
// Call 2: Read publications using native XRANGE/XREVRANGE
// Key difference: Must filter out publications with offset > top_offset (non-atomic).
func (e *RedisMapBroker) ReadStream2(ctx context.Context, ch string, opts MapReadStreamOptions) (MapStreamResult, error) {
	_ = "STUB: not implemented"
	return *new(MapStreamResult), nil
}

// 1. Parse options (same as ReadStream)

// Use offset+1 because XRANGE is inclusive, but "since" should be exclusive

// 2. Call 1: Get metadata using new simpler Lua script

// Validate epoch if provided in Since filter.

// 3. Early return if metadata-only

// 4. Call 2: Execute native XRANGE/XREVRANGE

// 5. Parse stream entries and filter by topOffset

// Extract offset from stream ID "123-0" -> 123

// CRITICAL: Filter entries written after metadata read

// entry[1] is an array of [field, value, field, value, ...]

// Stats returns short stats of current presence data.
// This is a read-only operation - cleanup is handled by the cleanup worker.
func (e *RedisMapBroker) Stats(ctx context.Context, ch string) (MapStats, error) {
	_ = "STUB: not implemented"
	return *new(MapStats), nil
}

// ReadPresenceState retrieves presence state with per-entry revisions for converged membership.
// Each entry includes its revision so client can filter: entry.Revision <= state_revision.
// If opts.Revision is provided and epoch changed, returns empty entries.
// Returns Publications with Key=ClientID, Info=ClientInfo, Offset/Epoch for revision.
func (e *RedisMapBroker) ReadPresenceState(ctx context.Context, ch string, opts MapReadStateOptions) ([]*Publication, StreamPosition, error) {
	_ = "STUB: not implemented"
	// ReadPresenceState is just ReadState - it already returns Publications with Key and Offset set.
	// This reuses all the state reading logic.
	return nil, *new(StreamPosition), nil
}

// ReadPresenceStream retrieves presence event stream (joins/leaves) for a channel.
// Returns Publications with Info=ClientInfo and Removed flag (true for leave, false for join).
func (e *RedisMapBroker) ReadPresenceStream(ctx context.Context, ch string, opts MapReadStreamOptions) (MapStreamResult, error) {
	_ = "STUB: not implemented"
	// ReadPresenceStream is literally just ReadStream - presence streams store Publications
	// Publication.Removed distinguishes join (false) from leave (true) events.
	return *new(MapStreamResult), nil
}

// RegisterEventHandler registers a BrokerEventHandler to handle messages from Pub/Sub.
func (e *RedisMapBroker) RegisterEventHandler(h BrokerEventHandler) error {
	_ = "STUB: not implemented"
	// Run all shards.
	return nil
}

// newMapBrokerPubSubRunnerHook is an optional package-private hook used by
// auxiliary modules to install an alternative pub/sub runner. The default
// implementation returns nil so the broker falls back to defaultMapBrokerPubSubRunner.
var newMapBrokerPubSubRunnerHook func(e *RedisMapBroker, shard *RedisShard) mapBrokerPubSubRunner

// defaultMapBrokerPubSubRunner is the standard partition-sharded pub/sub runner.
// All per-shard state lives on the brokerShardWrapper, the runner is stateless
// beyond a back-pointer.
type defaultMapBrokerPubSubRunner struct {
	broker *RedisMapBroker
}

func (r *defaultMapBrokerPubSubRunner) init(s *brokerShardWrapper, shard *RedisShard) error {
	e := r.broker
	subChannels := make([][]rueidis.DedicatedClient, 0)
	pubSubStartChannels := make([][]*pubSubStart, 0)

	if e.useShardedPubSub(shard) {
		for i := 0; i < e.conf.NumShardedPubSubPartitions; i++ {
			subChannels = append(subChannels, make([]rueidis.DedicatedClient, 0))
			pubSubStartChannels = append(pubSubStartChannels, make([]*pubSubStart, 0))
		}
	} else {
		subChannels = append(subChannels, make([]rueidis.DedicatedClient, 0))
		pubSubStartChannels = append(pubSubStartChannels, make([]*pubSubStart, 0))
	}

	for i := 0; i < len(subChannels); i++ {
		for j := 0; j < e.conf.numSubscribeShards; j++ {
			subChannels[i] = append(subChannels[i], nil)
			pubSubStartChannels[i] = append(pubSubStartChannels[i], &pubSubStart{errCh: make(chan error, 1)})
		}
	}

	s.subClients = subChannels
	s.pubSubStartChannels = pubSubStartChannels
	return nil
}

func (r *defaultMapBrokerPubSubRunner) subClientsIndex(clusterShardIdx int) int {
	_ = "STUB: not implemented"
	return 0
}

func (r *defaultMapBrokerPubSubRunner) run(s *brokerShardWrapper, h BrokerEventHandler) error {
	_ = "STUB: not implemented"
	return nil
}

// Cluster shards.

// PUB/SUB shards.

// runForever keeps another function running indefinitely.
func (e *RedisMapBroker) runForever(fn func()) { _ = "STUB: not implemented"; return }

// cleanupChannelBatchSize is the max number of channels to fetch per ZRANGEBYSCORE call
// in the cleanup worker. The worker loops until all expired channels are processed.
const cleanupChannelBatchSize = 10000

// cleanupChannelConcurrency is the max number of concurrent cleanup Lua calls per partition.
// Goroutines automatically pipeline over rueidis's multiplexed connection, reducing
// network round-trip overhead from N sequential calls to ~N/concurrency batches.
const cleanupChannelConcurrency = 64

// runCleanupLagWorker periodically checks the oldest expired entry across all
// cleanup ZSETs and reports it as a lag metric. This runs in a separate goroutine
// to avoid adding extra Redis calls to the hot cleanup path.
func (e *RedisMapBroker) runCleanupLagWorker(ctx context.Context) {
	_ = "STUB: not implemented"
	return
}

func (e *RedisMapBroker) updateCleanupLag(ctx context.Context) { _ = "STUB: not implemented"; return }

// runCleanupWorker runs the background cleanup worker that generates LEAVE events
// for expired presence entries. This ensures guaranteed delivery of LEAVE events
// even when clients disconnect without explicit leave.
func (e *RedisMapBroker) runCleanupWorker(ctx context.Context) { _ = "STUB: not implemented"; return }

// runCleanupCycle processes all shards in parallel and cleans up expired entries.
func (e *RedisMapBroker) runCleanupCycle(ctx context.Context) { _ = "STUB: not implemented"; return }

// cleanupShard processes all partitions within a shard in parallel.
func (e *RedisMapBroker) cleanupShard(ctx context.Context, shard *RedisShard, now int64) {
	_ = "STUB: not implemented"
	return
}

// cleanupPartition processes all expired channels within a single partition.
// It loops until all expired channels are handled (no artificial cap).
// Within each batch, channels are processed concurrently for pipelining.
func (e *RedisMapBroker) cleanupPartition(ctx context.Context, shard *RedisShard, cleanupKey string, now int64) {
	_ = "STUB: not implemented"
	return
}

// Process channels concurrently. Goroutines pipeline over rueidis's
// multiplexed connection, turning N sequential round-trips into
// ~N/cleanupChannelConcurrency pipelined batches.

// getChannelsForCleanup returns channels that have expired entries (score <= now).
func (e *RedisMapBroker) getChannelsForCleanup(ctx context.Context, client rueidis.Client, cleanupKey string, now int64) ([]string, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// cleanupChannel runs the 2-phase cleanup for a single channel:
// Phase 1: find expired keys and read their state values (read-only Lua)
// Phase 2: construct removal protobufs in Go (with tags), then batch-remove atomically (Lua)
//
// Drains up to maxBatchesPerCall batches per invocation so a single hot channel
// with a large backlog converges quickly instead of being capped at
// CleanupBatchSize keys per CleanupInterval. Other channels in the same partition
// remain unaffected since partition-level fan-out runs independent goroutines.
func (e *RedisMapBroker) cleanupChannel(ctx context.Context, shard *RedisShard, ch string, cleanupKey string, now int64) error {
	_ = "STUB: not implemented"
	// Get channel options for this channel.
	return nil
}

// Phase 1: find expired keys and read their state values.

// Phase 2: construct removal protobufs in Go (with tags from state), then batch-remove.

// Extract tags from the stored publication protobuf.

// Construct removal protobuf with tags.

// If we drained less than a full batch, the queue is empty for now.

// expiredKeyEntry holds data returned from the find-expired Lua script.
type expiredKeyEntry struct {
	key         string
	stateValue  []byte
	expireScore string
}

// cleanupRemovalEntry holds a pre-constructed removal protobuf for the batch-remove script.
type cleanupRemovalEntry struct {
	key         string
	payload     []byte
	expireScore string
}

// findExpiredKeys runs the read-only Lua script to find expired keys and their state values.
func (e *RedisMapBroker) findExpiredKeys(ctx context.Context, shard *RedisShard, ch string, now int64) ([]expiredKeyEntry, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// KEYS[1]: state hash key
// KEYS[2]: state expire zset key

// ARGV[1]: now
// ARGV[2]: batch_size

// Parse triplets: (key, state_value, expire_score)

// batchRemoveExpired runs the batch-remove Lua script to atomically delete expired entries,
// write removal events to stream, and publish via PUB/SUB.
func (e *RedisMapBroker) batchRemoveExpired(ctx context.Context, shard *RedisShard, ch string, cleanupKey string, chOpts MapChannelOptions, removals []cleanupRemovalEntry) error {
	_ = "STUB: not implemented"
	// Determine publish command
	return nil
}

// Build ARGV: fixed args + triplets per entry

// ARGV[1]: num_entries
// ARGV[2]: channel
// ARGV[3]: publish_command
// ARGV[4]: stream_size
// ARGV[5]: stream_ttl
// ARGV[6]: meta_expire
// ARGV[7]: new_epoch_if_empty
// ARGV[8]: channel_for_cleanup
// ARGV[9]: streamless

// KEYS[1]
// KEYS[2]
// KEYS[3]
// KEYS[4]
// KEYS[5]
// KEYS[6]
// KEYS[7]

// makePubSubCallbacks builds the pubSubCallbacks used by the pub/sub runner.
func (e *RedisMapBroker) makePubSubCallbacks(s *brokerShardWrapper) pubSubCallbacks {
	_ = "STUB: not implemented"
	return *new(pubSubCallbacks)
}

func (e *RedisMapBroker) runPubSub(s *brokerShardWrapper, logFields map[string]any, eventHandler BrokerEventHandler, clusterShardIndex, psShardIndex int, useShardedPubSub bool, startOnce func(error)) {
	_ = "STUB: not implemented"
	return
}

// pubSubPartitionHashTag returns the Redis Cluster hash tag for the given partition
// index. Used both when constructing PUB/SUB channel names (so messages route
// to a specific slot) and by alternative pub/sub strategies that need to
// determine slot ownership for a partition. Publisher and subscriber must
// agree on the scheme — keep this as the single source of truth.
//
// When UsePrecomputedPartitionTags is enabled, returns the precomputed tag
// for the index (selected for even slot distribution). Otherwise returns
// the bare integer index, preserving backward-compatible behaviour.
func (e *RedisMapBroker) pubSubPartitionHashTag(partitionIdx int) string {
	_ = "STUB: not implemented"
	return ""
}

func (e *RedisMapBroker) pubSubShardChannelID(clusterShardIndex int, psShardIndex int, useShardedPubSub bool) string {
	_ = "STUB: not implemented"
	return ""
}

func (e *RedisMapBroker) extractChannel(chID string) string { _ = "STUB: not implemented"; return "" }

// Handle sharded PUB/SUB case: {idx}.channel

// Invalid: expected {idx}.channel format

// Invalid: missing dot separator

// Non-cluster: plain channel name

func (e *RedisMapBroker) handleRedisClientMessage(isCluster bool, eventHandler BrokerEventHandler, chID string, data []byte) error {
	_ = "STUB: not implemented"
	// Parse message: supports backward compat (no prefix), non-delta, and delta formats
	return nil
}

// Unmarshal as Publication (unified format)

// Regular publication

// Delta prev data is in state value format: "offset:epoch:protobuf".
// Extract the protobuf payload.

// Subscribe to channels.
func (e *RedisMapBroker) Subscribe(channels ...string) error { _ = "STUB: not implemented"; return nil }

func (e *RedisMapBroker) subscribe(s *brokerShardWrapper, ch string) error {
	_ = "STUB: not implemented"
	return nil
}

type mapBrokerConnKey struct {
	shardIdx        int
	clusterShardIdx int
	psShardIdx      int
}

type mapBrokerConnGroup struct {
	shard    *brokerShardWrapper
	channels []string
}

func (e *RedisMapBroker) subscribeBatch(channels []string, unsub bool) error {
	_ = "STUB: not implemented"
	return nil
}

// Track completed groups so we can roll back on partial failure.

// rollbackSubscribeBatch unsubscribes channels from groups that were already
// successfully subscribed. Best-effort: errors are ignored.
func (e *RedisMapBroker) rollbackSubscribeBatch(groups map[mapBrokerConnKey]*mapBrokerConnGroup, completed []mapBrokerConnKey) {
	_ = "STUB: not implemented"
	return
}

// Unsubscribe from channels.
func (e *RedisMapBroker) Unsubscribe(channels ...string) error {
	_ = "STUB: not implemented"
	return nil
}

func (e *RedisMapBroker) unsubscribe(s *brokerShardWrapper, ch string) error {
	_ = "STUB: not implemented"
	return nil
}

// parseMessage parses message formats:
// 1. No prefix (backward compat): raw protobuf bytes
// 2. Non-delta: offset:epoch:protobuf
// 3. Delta: d:offset:epoch:prev_len:prev_protobuf:curr_len:curr_protobuf
// Returns offset, epoch, push bytes, delta flag, prev protobuf, and error if parsing fails.
func parseMessage(data []byte) (uint64, string, []byte, bool, []byte, error) {
	_ = "STUB: not implemented"
	return 0, "", nil, false, nil, nil
}

// Check for delta prefix

// Delta format: d:offset:epoch:prev_len:prev_protobuf:curr_len:curr_protobuf
// Skip "d:"

// Check if first byte is a digit (offset start) or colon separator

// No prefix - raw protobuf (backward compatibility)

// Non-delta format: offset:epoch:protobuf

// No colon found - treat as raw protobuf

// Parse offset

// Failed to parse offset - treat as raw protobuf

// Find second colon (after epoch)

// Extract epoch

// Everything after second colon is protobuf bytes

// parseDeltaMessage parses delta format: offset:epoch:prev_len:prev_protobuf:curr_len:curr_protobuf
func parseDeltaMessage(data []byte) (uint64, string, []byte, bool, []byte, error) {
	_ = "STUB: not implemented"
	// Parse offset
	return 0, "", nil, false, nil, nil
}

// Parse epoch

// Parse prev_len

// Extract prev_protobuf

// Parse curr_len

// Skip ':'

// Extract curr_protobuf

// parseAddScriptResult parses the result from the add script.
func parseAddScriptResult(replies []rueidis.RedisMessage) (MapUpdateResult, error) {
	_ = "STUB: not implemented"
	return *new(MapUpdateResult), nil
}

// Check for suppression reason (3rd value is the reason string, empty means not suppressed)

// For CAS mismatch, return current key state for immediate retry.
// Client uses: CurrentEntry.Offset + Position.Epoch for the next CAS attempt.

// Parse value: offset:epoch:payload
