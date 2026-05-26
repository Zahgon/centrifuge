package centrifuge

import (
	"context"
	"time"

	_ "embed"

	"github.com/redis/rueidis"
)

var _ PresenceManager = (*RedisPresenceManager)(nil)

// RedisPresenceManager keeps presence in Redis thus allows scaling nodes.
type RedisPresenceManager struct {
	node                *Node
	config              RedisPresenceManagerConfig
	shards              []*RedisShard
	sharding            bool
	addPresenceScript   *rueidis.Lua
	remPresenceScript   *rueidis.Lua
	presenceScript      *rueidis.Lua
	presenceStatsScript *rueidis.Lua
}

// RedisPresenceManagerConfig is a config for RedisPresenceManager.
type RedisPresenceManagerConfig struct {
	// Prefix to use before every channel name and key in Redis. By default,
	// "centrifuge" prefix will be used.
	Prefix string

	// PresenceTTL is an interval how long to consider presence info
	// valid after receiving presence update. This allows to automatically
	// clean up unnecessary presence entries after TTL passed. Zero value
	// means 60 seconds.
	PresenceTTL time.Duration

	// Shards is a slice of RedisShard to use. At least one shard must be provided.
	// Data will be consistently sharded by channel over provided Redis shards.
	Shards []*RedisShard

	// EnableUserMapping when returns true tells RedisPresenceManager to additionally store
	// user to num client connections hash map and sorted set with unique users in Redis.
	// This increases Redis memory usage since additional structures are used, but provides
	// a way to optimize presence stats retrieving as we can calculate stats quickly on
	// Redis side instead of loading the entire presence information. By default, user mapping
	// is not maintained.
	EnableUserMapping func(channel string) bool

	// UseHashFieldTTL allows using HEXPIRE command to set TTL for hash field. It's only available
	// since Redis 7.4.0 thus disabled by default. Using hash field TTL can be useful to avoid
	// maintaining expiration index in ZSET – so both useful from the throughput and memory usage
	// perspective.
	UseHashFieldTTL bool

	// ReadFromReplica enables reading presence information from replica Redis servers.
	// This only works in Redis Cluster and Sentinel setups and requires replica client
	// to be initialized in each RedisShard using RedisShardConfig.ReplicaClientEnabled.
	ReadFromReplica bool

	// LoadSHA1 enables loading SHA1 from Redis via SCRIPT LOAD instead of calculating
	// it on the client side. This is useful for FIPS compliance.
	LoadSHA1 bool
}

var (
	//go:embed internal/redis_lua/presence_add.lua
	addPresenceScriptSource string

	//go:embed internal/redis_lua/presence_rem.lua
	remPresenceScriptSource string

	//go:embed internal/redis_lua/presence_get.lua
	presenceScriptSource string

	//go:embed internal/redis_lua/presence_stats_get.lua
	presenceStatsScriptSource string
)

// NewRedisPresenceManager creates new RedisPresenceManager.
func NewRedisPresenceManager(n *Node, config RedisPresenceManagerConfig) (*RedisPresenceManager, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (m *RedisPresenceManager) Close(_ context.Context) error {
	_ = "STUB: not implemented"
	return nil
}

func (m *RedisPresenceManager) getShard(channel string) *RedisShard {
	_ = "STUB: not implemented"
	return nil
}

// AddPresence - see PresenceManager interface description.
func (m *RedisPresenceManager) AddPresence(ch string, uid string, info *ClientInfo) error {
	_ = "STUB: not implemented"
	return nil
}

func (m *RedisPresenceManager) addPresenceScriptKeysArgs(s *RedisShard, ch string, uid string, info *ClientInfo) ([]string, []string, error) {
	_ = "STUB: not implemented"
	return nil, nil, nil
}

func (m *RedisPresenceManager) useUserMappingArg(ch string) string {
	_ = "STUB: not implemented"
	return ""
}

func (m *RedisPresenceManager) useHashFieldTTLArg() string { _ = "STUB: not implemented"; return "" }

func (m *RedisPresenceManager) addPresence(s *RedisShard, ch string, uid string, info *ClientInfo) error {
	_ = "STUB: not implemented"
	return nil
}

// RemovePresence - see PresenceManager interface description.
func (m *RedisPresenceManager) RemovePresence(ch string, clientID string, userID string) error {
	_ = "STUB: not implemented"
	return nil
}

func (m *RedisPresenceManager) removePresenceScriptKeysArgs(s *RedisShard, ch string, uid string, userID string) ([]string, []string, error) {
	_ = "STUB: not implemented"
	return nil, nil, nil
}

func (m *RedisPresenceManager) removePresence(s *RedisShard, ch string, clientID string, userID string) error {
	_ = "STUB: not implemented"
	return nil
}

// Presence - see PresenceManager interface description.
func (m *RedisPresenceManager) Presence(ch string) (map[string]*ClientInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (m *RedisPresenceManager) presenceScriptKeysArgs(s *RedisShard, ch string) ([]string, []string, error) {
	_ = "STUB: not implemented"
	return nil, nil, nil
}

func (m *RedisPresenceManager) presenceStatsScriptKeysArgs(s *RedisShard, ch string) ([]string, []string, error) {
	_ = "STUB: not implemented"
	return nil, nil, nil
}

func (m *RedisPresenceManager) presence(s *RedisShard, ch string) (map[string]*ClientInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func mapStringClientInfo(result []rueidis.RedisMessage) (map[string]*ClientInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (m *RedisPresenceManager) presenceStats(s *RedisShard, ch string) (PresenceStats, error) {
	_ = "STUB: not implemented"
	return *new(PresenceStats), nil
}

// PresenceStats - see PresenceManager interface description.
func (m *RedisPresenceManager) PresenceStats(ch string) (PresenceStats, error) {
	_ = "STUB: not implemented"
	return *new(PresenceStats), nil
}

func (m *RedisPresenceManager) presenceHashKey(s *RedisShard, ch string) channelID {
	_ = "STUB: not implemented"

	// Fast path: simple concatenation is optimal for non-cluster
	return *new(channelID)
}

// Cluster path: use builder to avoid multiple allocations

func (m *RedisPresenceManager) presenceSetKey(s *RedisShard, ch string) channelID {
	_ = "STUB: not implemented"

	// Fast path: simple concatenation is optimal for non-cluster
	return *new(channelID)
}

// Cluster path: use builder to avoid multiple allocations

func (m *RedisPresenceManager) userSetKey(s *RedisShard, ch string) channelID {
	_ = "STUB: not implemented"

	// Fast path: simple concatenation is optimal for non-cluster
	return *new(channelID)
}

// Cluster path: use builder to avoid multiple allocations

func (m *RedisPresenceManager) userHashKey(s *RedisShard, ch string) channelID {
	_ = "STUB: not implemented"

	// Fast path: simple concatenation is optimal for non-cluster
	return *new(channelID)
}

// Cluster path: use builder to avoid multiple allocations
