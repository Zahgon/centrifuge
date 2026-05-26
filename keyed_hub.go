package centrifuge

import (
	"sync"

	"github.com/centrifugal/protocol"
)

// keyedHub is a per-channel reverse index: key → set of subscriber clients.
// Used by shared poll subscriptions for per-key fan-out.
type keyedHub struct {
	mu    sync.RWMutex
	items map[string]map[string]*Client // key → clientUID → *Client
}

func newKeyedHub() *keyedHub { _ = "STUB: not implemented"; return nil }

func (h *keyedHub) addSubscriber(key string, c *Client) { _ = "STUB: not implemented"; return }

func (h *keyedHub) removeSubscriber(key string, c *Client) (keyEmpty bool) {
	_ = "STUB: not implemented"
	return false
}

func (h *keyedHub) subscribers(key string) []*Client { _ = "STUB: not implemented"; return nil }

func (h *keyedHub) allKeys() []string { _ = "STUB: not implemented"; return nil }

func (h *keyedHub) subscriberCount(key string) int { _ = "STUB: not implemented"; return 0 }

func (h *keyedHub) hasSubscriber(key string, c *Client) bool {
	_ = "STUB: not implemented"
	return false
}

func (h *keyedHub) removeAllSubscribers(key string) { _ = "STUB: not implemented"; return }

func (h *keyedHub) numKeys() int { _ = "STUB: not implemented"; return 0 }

// collectAllClients returns the deduplicated set of *Client refs subscribed
// to any key on this hub. Used to enumerate every client of a shared-poll
// channel — for epoch-flip-driven unsubscribe in particular.
//
// Holds h.mu.RLock during enumeration only; the returned slice is safe to
// iterate without holding hub or channel-state locks.
func (h *keyedHub) collectAllClients() []*Client { _ = "STUB: not implemented"; return nil }

// broadcastToKey sends a publication to all subscribers of a key.
// Each subscriber's per-connection version is checked — only clients
// with a version lower than pubVersion receive the publication.
// Must be called WITHOUT holding sharedPollChannelState.mu.
func (h *keyedHub) broadcastToKey(channel string, key string, pubVersion uint64, pub *protocol.Publication, prep preparedData) {
	_ = "STUB: not implemented"
	return
}

// broadcastRemoval sends a removal publication to all subscribers of a key.
// Must be called WITHOUT holding sharedPollChannelState.mu.
func (h *keyedHub) broadcastRemoval(channel string, key string) { _ = "STUB: not implemented"; return }

// broadcastRemovalToUsers sends removal publications only to connections
// belonging to the specified users (or excluding specified users).
func (h *keyedHub) broadcastRemovalToUsers(channel string, key string, users []string, excludeUsers []string) {
	_ = "STUB: not implemented"
	return
}

// removeSubscribersForUsers removes subscribers matching user/exclude filters.
func (h *keyedHub) removeSubscribersForUsers(key string, users []string, excludeUsers []string) {
	_ = "STUB: not implemented"
	return
}
