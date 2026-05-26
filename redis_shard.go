package centrifuge

import (
	"crypto/tls"
	"net"
	"sync"
	"time"

	"github.com/redis/rueidis"
)

type (
	// channelID is unique channel identifier in Redis.
	channelID string
)

const (
	defaultRedisIOTimeout      = 4 * time.Second
	defaultRedisConnectTimeout = time.Second
)

type RedisShard struct {
	config        RedisShardConfig
	client        rueidis.Client
	replicaClient rueidis.Client
	closeCh       chan struct{}
	closeOnce     sync.Once
	isCluster     bool
	isSentinel    bool
	finalAddress  []string
}

var knownRedisURLPrefixes = []string{
	"redis://",
	"rediss://",
	"redis+sentinel://",
	"rediss+sentinel://",
	"redis+cluster://",
	"unix://",
	"tcp://",
}

type fromAddressOptions struct {
	ClientOption         rueidis.ClientOption
	IsCluster            bool
	IsSentinel           bool
	ReplicaClientEnabled bool
}

func optionsFromAddress(address string, options rueidis.ClientOption) (fromAddressOptions, error) {
	_ = "STUB: not implemented"
	return *new(fromAddressOptions), nil
}

// NewRedisShard initializes new Redis shard.
func NewRedisShard(_ *Node, conf RedisShardConfig) (*RedisShard, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// Cluster mode is not explicitly set but client is a cluster client – thus set isCluster to true.
// This scenario covered with tests for our main integrations: see TestNewRedisShard.
// Centrifuge need to know that it's working with Redis Cluster to construct proper keys.

// RedisShardConfig contains Redis connection options.
type RedisShardConfig struct {
	// Address is a Redis server connection address. Address can be:
	// - host:port
	// - tcp://[[[user]:password]@]host:port[/db][?option1=value1&optionN=valueN]
	// - redis://[[[user]:password]@]host:port[/db][?option1=value1&optionN=valueN]
	// - rediss://[[[user]:password]@]host:port[/db][?option1=value1&optionN=valueN]
	// - unix://[[[user]:password]@]path[?option1=value1&optionN=valueN]
	// It's also possible to use Address with redis+sentinel:// scheme to connect to Redis Sentinel:
	// - redis+sentinel://[[[user]:password]@]host:port?sentinel_master_name=mymaster?addr=host2:port2&addr=host3:port3
	// In case of using redis+sentinel://, sentinel_master_name is required and host:port points to Sentinel instance.
	// It's also possible to connect to Redis Cluster by providing ClusterAddresses instead of Address.
	// It's also possible to connect to Redis Sentinel by providing SentinelAddresses instead of Address.
	Address string
	// ClusterAddresses is a slice of seed cluster addresses to connect to.
	// Each address should be in form of host:port. If ClusterAddresses set then
	// RedisShardConfig.Address not used at all.
	ClusterAddresses []string
	// SentinelAddresses is a slice of Sentinel addresses. Each address should
	// be in form of host:port. If set then Redis address will be automatically
	// discovered from Sentinel. For Sentinel the name of the master instance
	// Sentinel monitors (SentinelMasterName) must be provided. If SentinelAddresses
	// set then RedisShardConfig.Address not used at all.
	SentinelAddresses []string

	// SentinelMasterName is a name of Redis instance master Sentinel monitors.
	SentinelMasterName string
	// SentinelUser is a user for Sentinel ACL-based auth.
	SentinelUser string
	// SentinelPassword is a password for Sentinel. Works with Sentinel >= 5.0.1.
	SentinelPassword string
	// SentinelClientName is a client name for established connections to Sentinel.
	SentinelClientName string
	// SentinelTLSConfig is a TLS configuration for Sentinel connections.
	SentinelTLSConfig *tls.Config

	// DB is Redis database number. If not set then database 0 used.
	// Does not make sense in Redis Cluster case.
	DB int
	// User is a username for Redis ACL-based auth.
	User string
	// Password is password to use when connecting to Redis. If zero then password not used.
	Password string
	// ClientName for established connections with Redis. See https://redis.io/commands/client-setname/
	ClientName string
	// TLSConfig contains connection TLS configuration.
	TLSConfig *tls.Config

	// ConnectTimeout is a timeout on connect operation.
	// By default, 1 second is used.
	ConnectTimeout time.Duration
	// IOTimeout is a timeout on Redis connection operations. This is used as a write deadline
	// for connection, also Redis client we use internally periodically (once in a second) PINGs
	// Redis with this timeout for PING operation to find out stale/broken/blocked connections.
	// By default, 4 seconds is used.
	IOTimeout time.Duration

	// ForceRESP2 if set to true forces using RESP2 protocol for communicating with Redis.
	// By default, Redis client tries to detect supported Redis protocol automatically
	// trying RESP3 first.
	ForceRESP2 bool

	// ReplicaClientEnabled once set to true will initialize replica client for this shard.
	// Replica client can then be used for read-only operations from replica nodes in Redis
	// Cluster or Redis Sentinel setups (single Redis is not allowed). Replica client will
	// be initialized with the same options as the main client but with ReplicaOnly option
	// set to true.
	ReplicaClientEnabled bool

	// AuthCredentialsFn is an optional function to dynamically provide auth credentials.
	// When set, it is called by the Redis client to obtain credentials for each new connection,
	// enabling short-lived token-based authentication (e.g. GCP IAM, AWS IAM).
	AuthCredentialsFn func(RedisAuthCredentialsContext) (RedisAuthCredentials, error)
}

// RedisAuthCredentialsContext is passed to AuthCredentialsFn.
type RedisAuthCredentialsContext struct {
	// Address is the address of the Redis server being connected to.
	Address net.Addr
}

// RedisAuthCredentials contains the credentials returned by AuthCredentialsFn.
type RedisAuthCredentials struct {
	Username string
	Password string
}

type RedisShardMode string

const (
	RedisShardModeStandalone RedisShardMode = "standalone"
	RedisShardModeCluster    RedisShardMode = "cluster"
	RedisShardModeSentinel   RedisShardMode = "sentinel"
)

func (s *RedisShard) Mode() RedisShardMode { _ = "STUB: not implemented"; return *new(RedisShardMode) }

func (s *RedisShard) Close() { _ = "STUB: not implemented"; return }

func (s *RedisShard) string() string { _ = "STUB: not implemented"; return "" }

// consistentIndex is an adapted function from https://github.com/dgryski/go-jump
// package by Damian Gryski. It implements the Jump Consistent Hash algorithm
// from the Google paper "A Fast, Minimal Memory, Consistent Hash Algorithm"
// (Lamping & Veach, 2014).
//
// It consistently chooses a hash bucket number in the range [0, numBuckets)
// for the given string. numBuckets must be >= 1.
//
// Key property: When adding a shard, only ~1/(n+1) keys are redistributed.
// This is critical for minimizing data movement when scaling.
func consistentIndex(s string, numBuckets int) int { _ = "STUB: not implemented"; return 0 }
