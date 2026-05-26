package centrifuge

import (
	"context"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge/internal/memstream"
	"github.com/centrifugal/centrifuge/internal/priority"
)

// MemoryBroker is builtin default Broker which allows running Centrifuge-based
// server without any external broker. All data managed inside process memory.
//
// With this Broker you can only run single Centrifuge node. If you need to scale
// you should consider using another Broker implementation instead – for example
// RedisBroker.
//
// Running single node can be sufficient for many use cases especially when you
// need maximum performance and not too many online clients. Consider configuring
// your load balancer to have one backup Centrifuge node for HA in this case.
type MemoryBroker struct {
	node         *Node
	historyHub   *historyHub
	eventHandler BrokerEventHandler

	// pubLocks synchronize access to publishing. We have to sync publish
	// to handle publications in the order of offset to prevent InsufficientState
	// errors.
	// TODO: maybe replace with sharded pool of workers with buffered channels.
	pubLocks map[int]*sync.Mutex

	closeOnce sync.Once
	closeCh   chan struct{}

	nextExpireCheck   int64
	resultExpireQueue priority.Queue
	resultCache       map[string]resultCacheEntry
	resultCacheMu     sync.RWMutex
}

var _ Broker = (*MemoryBroker)(nil)

// MemoryBrokerConfig is a memory broker config.
type MemoryBrokerConfig struct{}

const numPubLocks = 4096

const defaultIdempotentResultExpireSeconds = 300

// NewMemoryBroker initializes MemoryBroker.
func NewMemoryBroker(n *Node, _ MemoryBrokerConfig) (*MemoryBroker, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// RegisterBrokerEventHandler runs memory broker.
func (b *MemoryBroker) RegisterBrokerEventHandler(h BrokerEventHandler) error {
	_ = "STUB: not implemented"
	return nil
}

// Close is noop for now.
func (b *MemoryBroker) Close(_ context.Context) error { _ = "STUB: not implemented"; return nil }

func (b *MemoryBroker) pubLock(ch string) *sync.Mutex { _ = "STUB: not implemented"; return nil }

// Publish adds message into history hub and calls node method to handle message.
// We don't have any PUB/SUB here as MemoryBroker is single node only.
func (b *MemoryBroker) Publish(ch string, data []byte, opts PublishOptions) (PublishResult, error) {
	_ = "STUB: not implemented"
	return *new(PublishResult), nil
}

func (b *MemoryBroker) getResultFromCache(ch string, key string) (StreamPosition, bool) {
	_ = "STUB: not implemented"
	return *new(StreamPosition), false
}

func (b *MemoryBroker) saveResultToCache(ch string, key string, sp StreamPosition, resultExpireSeconds int64) {
	_ = "STUB: not implemented"
	return
}

func (b *MemoryBroker) expireResultCache() { _ = "STUB: not implemented"; return }

// Compact heap when stale entries accumulate from repeated publishes
// with the same idempotency key — each call pushes a new heap item
// without removing the old one.

// PublishJoin - see Broker interface description.
func (b *MemoryBroker) PublishJoin(ch string, info *ClientInfo) error {
	_ = "STUB: not implemented"
	return nil
}

// PublishLeave - see Broker interface description.
func (b *MemoryBroker) PublishLeave(ch string, info *ClientInfo) error {
	_ = "STUB: not implemented"
	return nil
}

// Subscribe is noop here.
func (b *MemoryBroker) Subscribe(_ ...string) error {
	_ = "STUB: not implemented"

	// Unsubscribe node from channel. Noop here.
	return nil
}

func (b *MemoryBroker) Unsubscribe(_ ...string) error {
	_ = "STUB: not implemented"

	// History - see Broker interface description.
	return nil
}

func (b *MemoryBroker) History(ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	_ = "STUB: not implemented"
	return nil, *new(StreamPosition), nil
}

// RemoveHistory - see Broker interface description.
func (b *MemoryBroker) RemoveHistory(ch string) error { _ = "STUB: not implemented"; return nil }

type historyHub struct {
	sync.RWMutex
	streams         map[string]*memstream.Stream
	nextExpireCheck int64
	expireQueue     priority.Queue
	expires         map[string]int64
	historyMetaTTL  time.Duration
	nextRemoveCheck int64
	removeQueue     priority.Queue
	removes         map[string]int64
	closeCh         chan struct{}
}

func newHistoryHub(historyMetaTTL time.Duration, closeCh chan struct{}) *historyHub {
	_ = "STUB: not implemented"
	return nil
}

func (h *historyHub) close() { _ = "STUB: not implemented"; return }

func (h *historyHub) runCleanups() { _ = "STUB: not implemented"; return }

func (h *historyHub) removeStreams() { _ = "STUB: not implemented"; return }

func (h *historyHub) expireStreams() { _ = "STUB: not implemented"; return }

func (h *historyHub) add(ch string, pub *Publication, opts PublishOptions) (StreamPosition, *Publication, bool, error) {
	_ = "STUB: not implemented"
	return *new(StreamPosition), nil, false, nil
}

// May be nil is there were no previous publications.

// We can skip the unordered publication.

// Lock must be held outside.
func (h *historyHub) createStream(ch string) StreamPosition {
	_ = "STUB: not implemented"
	return *new(StreamPosition)
}

func getPosition(stream *memstream.Stream) StreamPosition {
	_ = "STUB: not implemented"
	return *new(StreamPosition)
}

func (h *historyHub) get(ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	_ = "STUB: not implemented"
	return nil, *new(StreamPosition), nil
}

// Lock must be held outside.
func (h *historyHub) getLocked(ch string, opts HistoryOptions) ([]*Publication, StreamPosition, error) {
	_ = "STUB: not implemented"
	return nil, *new(StreamPosition), nil
}

func (h *historyHub) remove(ch string) error { _ = "STUB: not implemented"; return nil }
