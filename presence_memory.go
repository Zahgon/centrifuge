package centrifuge

import (
	"context"
	"sync"
)

// MemoryPresenceManager is builtin default PresenceManager which allows running
// Centrifuge-based server without any external storage. All data managed inside process
// memory.
//
// With this PresenceManager you can only run single Centrifuge node. If you need to scale
// you should consider using another PresenceManager implementation instead – for example
// RedisPresenceManager.
//
// Running single node can be sufficient for many use cases especially when you
// need maximum performance and not too many online clients. Consider configuring
// your load balancer to have one backup Centrifuge node for HA in this case.
type MemoryPresenceManager struct {
	node        *Node
	config      MemoryPresenceManagerConfig
	presenceHub *presenceHub
}

var _ PresenceManager = (*MemoryPresenceManager)(nil)

// MemoryPresenceManagerConfig is a MemoryPresenceManager config.
type MemoryPresenceManagerConfig struct{}

// NewMemoryPresenceManager initializes MemoryPresenceManager.
func NewMemoryPresenceManager(n *Node, c MemoryPresenceManagerConfig) (*MemoryPresenceManager, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// AddPresence - see PresenceManager interface description.
func (m *MemoryPresenceManager) AddPresence(ch string, uid string, info *ClientInfo) error {
	_ = "STUB: not implemented"
	return nil
}

// RemovePresence - see PresenceManager interface description.
func (m *MemoryPresenceManager) RemovePresence(ch string, clientID string, _ string) error {
	_ = "STUB: not implemented"
	return nil
}

// Presence - see PresenceManager interface description.
func (m *MemoryPresenceManager) Presence(ch string) (map[string]*ClientInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil

	// PresenceStats - see PresenceManager interface description.
}

func (m *MemoryPresenceManager) PresenceStats(ch string) (PresenceStats, error) {
	_ = "STUB: not implemented"
	return *new(PresenceStats), nil
}

// Close is noop for now.
func (m *MemoryPresenceManager) Close(_ context.Context) error {
	_ = "STUB: not implemented"
	return nil
}

type presenceHub struct {
	sync.RWMutex
	presence map[string]map[string]*ClientInfo
}

func newPresenceHub() *presenceHub { _ = "STUB: not implemented"; return nil }

func (h *presenceHub) add(ch string, uid string, info *ClientInfo) error {
	_ = "STUB: not implemented"
	return nil
}

func (h *presenceHub) remove(ch string, uid string) error { _ = "STUB: not implemented"; return nil }

// clean up map if needed

func (h *presenceHub) get(ch string) (map[string]*ClientInfo, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

// return empty map

func (h *presenceHub) getStats(ch string) (PresenceStats, error) {
	_ = "STUB: not implemented"
	return *new(PresenceStats), nil
}

// return empty map
