// Package natsbroker defines custom Nats Broker for Centrifuge library.
package natsbroker

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/centrifugal/centrifuge"
	"github.com/nats-io/nats.go"
)

type (
	// channelID is unique channel identifier in Nats.
	channelID string
)

// Config of NatsBroker.
type Config struct {
	Servers string
	Prefix  string
}

var _ centrifuge.Broker = (*NatsBroker)(nil)
var _ centrifuge.Controller = (*NatsBroker)(nil)

// NatsBroker is a broker on top of Nats messaging system.
type NatsBroker struct {
	node   *centrifuge.Node
	config Config

	nc                  *nats.Conn
	subsMu              sync.Mutex
	subs                map[channelID]*nats.Subscription
	eventHandler        centrifuge.BrokerEventHandler
	controlEventHandler centrifuge.ControlEventHandler
}

// History ...
func (b *NatsBroker) History(_ string, _ centrifuge.HistoryOptions) ([]*centrifuge.Publication, centrifuge.StreamPosition, error) {
	_ = "STUB: not implemented"
	return nil, *new(centrifuge.StreamPosition), nil
}

// RemoveHistory ...
func (b *NatsBroker) RemoveHistory(_ string) error { _ = "STUB: not implemented"; return nil }

// New creates NatsBroker.
func New(n *centrifuge.Node, conf Config) (*NatsBroker, error) {
	_ = "STUB: not implemented"
	return nil, nil
}

func (b *NatsBroker) controlChannel() channelID { _ = "STUB: not implemented"; return *new(channelID) }

func (b *NatsBroker) nodeChannel(nodeID string) channelID {
	_ = "STUB: not implemented"
	return *new(channelID)
}

func (b *NatsBroker) clientChannel(ch string) channelID {
	_ = "STUB: not implemented"
	return *new(channelID)
}

func (b *NatsBroker) extractChannel(subject string) string { _ = "STUB: not implemented"; return "" }

// RegisterBrokerEventHandler ...
func (b *NatsBroker) RegisterBrokerEventHandler(h centrifuge.BrokerEventHandler) error {
	_ = "STUB: not implemented"
	return nil
}

// Close is not implemented.
func (b *NatsBroker) Close(_ context.Context) error { _ = "STUB: not implemented"; return nil }

type pushType int

const (
	pubPushType   pushType = 0
	joinPushType  pushType = 1
	leavePushType pushType = 2
)

type push struct {
	Type pushType        `json:"type,omitempty"`
	Data json.RawMessage `json:"data"`
}

// Publish - see centrifuge.Broker interface description.
func (b *NatsBroker) Publish(ch string, data []byte, opts centrifuge.PublishOptions) (centrifuge.PublishResult, error) {
	_ = "STUB: not implemented"
	return *new(centrifuge.PublishResult), nil
}

// PublishJoin - see centrifuge.Broker interface description.
func (b *NatsBroker) PublishJoin(ch string, info *centrifuge.ClientInfo) error {
	_ = "STUB: not implemented"
	return nil
}

// PublishLeave - see centrifuge.Broker interface description.
func (b *NatsBroker) PublishLeave(ch string, info *centrifuge.ClientInfo) error {
	_ = "STUB: not implemented"
	return nil
}

// PublishControl - see centrifuge.Broker interface description.
func (b *NatsBroker) PublishControl(data []byte, nodeID, _ string) error {
	_ = "STUB: not implemented"
	return nil
}

func (b *NatsBroker) handleClientMessage(subject string, data []byte) error {
	_ = "STUB: not implemented"
	return nil
}

func (b *NatsBroker) handleClient(m *nats.Msg) { _ = "STUB: not implemented"; return }

func (b *NatsBroker) handleControl(m *nats.Msg) { _ = "STUB: not implemented"; return }

func (b *NatsBroker) RegisterControlEventHandler(h centrifuge.ControlEventHandler) error {
	_ = "STUB: not implemented"
	return nil
}

// Subscribe - see centrifuge.Broker interface description.
func (b *NatsBroker) Subscribe(channels ...string) error { _ = "STUB: not implemented"; return nil }

// Do not support wildcard subscriptions.

// Unsubscribe - see centrifuge.Broker interface description.
func (b *NatsBroker) Unsubscribe(channels ...string) error { _ = "STUB: not implemented"; return nil }
