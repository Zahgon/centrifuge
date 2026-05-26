package recovery

import (
	"sync"

	"github.com/centrifugal/protocol"
)

// PubSubSync wraps logic to synchronize recovery with PUB/SUB.
type PubSubSync struct {
	subSyncMu sync.RWMutex
	subSync   map[string]*subscribeState
}

// NewPubSubSync creates new PubSubSyncer.
func NewPubSubSync() *PubSubSync { _ = "STUB: not implemented"; return nil }

type subscribeState struct {
	// The following fields help us to synchronize PUB/SUB and history messages
	// during publication recovery process in channel.
	inSubscribe     uint32
	pubBufferMu     sync.Mutex
	pubBufferLocked bool
	pubBuffer       []*protocol.Publication
}

// SyncPublication ...
func (c *PubSubSync) SyncPublication(channel string, pub *protocol.Publication, syncedFn func()) {
	_ = "STUB: not implemented"
	return
}

// client currently in process of subscribing to the channel. In this case we keep
// publications in a slice buffer. Publications from this temporary buffer will be sent in
// subscribe reply.

// Sync point not reached yet - put Publication to tmp slice.

// Sync point already passed - send Publication into connection.

// StartBuffering ...
func (c *PubSubSync) StartBuffering(channel string) { _ = "STUB: not implemented"; return }

// StopBuffering ...
func (c *PubSubSync) StopBuffering(channel string) { _ = "STUB: not implemented"; return }

func (c *PubSubSync) LockBufferAndReadBuffered(channel string) []*protocol.Publication {
	_ = "STUB: not implemented"
	return nil
}

// Since this point and until StopBuffering pubBufferMu will be locked so that SyncPublication waits till pubBufferMu unlocking.
