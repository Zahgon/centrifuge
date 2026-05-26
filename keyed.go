package centrifuge

import "sync"

// keyedChannelOptions are generic options for any keyed channel.
type keyedChannelOptions struct {
	// MaxTrackedPerConnection limits how many keys a single connection
	// can track in this channel. Zero value means 5000.
	MaxTrackedPerConnection int
}

// keyedManager manages keyed channel state: per-channel reverse index
// (key→subscribers) and keyed hub for per-key fan-out.
type keyedManager struct {
	node     *Node
	mu       sync.RWMutex
	channels map[string]*keyedChannelState
}

type keyedChannelState struct {
	hub  *keyedHub
	opts keyedChannelOptions
}

func newKeyedManager(node *Node) *keyedManager { _ = "STUB: not implemented"; return nil }

func (m *keyedManager) getOrCreateChannel(channel string, opts keyedChannelOptions) *keyedChannelState {
	_ = "STUB: not implemented"
	return nil
}

func (m *keyedManager) getHub(channel string) *keyedHub { _ = "STUB: not implemented"; return nil }

func (m *keyedManager) removeChannel(channel string) { _ = "STUB: not implemented"; return }

// addSubscribers ensures a keyedChannelState exists for the channel and
// atomically adds the given client as a subscriber to each key. The
// "create state + add subscriber" sequence runs under m.mu so a concurrent
// removeChannelIfEmpty cannot delete the state between creation and the
// first addSubscriber — that race would otherwise leave the client in an
// orphaned hub that future broadcasts (via getHub) no longer reach.
func (m *keyedManager) addSubscribers(channel string, keys []string, c *Client, opts keyedChannelOptions) {
	_ = "STUB: not implemented"
	return
}

// removeChannelIfEmpty deletes the channel from the manager only when its
// hub has no subscribers. Used by sharedPollChannelState.finalizeShutdown
// instead of the unconditional removeChannel, so a new client whose track
// has just added itself to the hub via addSubscribers is not orphaned by
// an in-flight shutdown from an older sharedPollChannelState.
func (m *keyedManager) removeChannelIfEmpty(channel string) { _ = "STUB: not implemented"; return }

const defaultMaxTrackedPerConnection = 5000

func (m *keyedManager) maxTrackedPerConnection(channel string) int {
	_ = "STUB: not implemented"
	return 0
}
