package centrifuge

import (
	"time"

	"github.com/centrifugal/protocol"
)

// handleSharedPollSubscribe handles subscribe requests for shared poll channels (type=4).
// Lightweight subscribe: no broker, no hub, no positioning/recovery.
func (c *Client) handleSharedPollSubscribe(req *protocol.SubscribeRequest, cmd *protocol.Command, started time.Time, rw *replyWriter) error {
	_ = "STUB: not implemented"
	return nil
}

// Pre-register channel to track duplicate subscriptions (matches regular subscribe flow).

// Delta negotiation.

// Register channel with flagKeyed | flagSubscribed | flagClientSideRefresh.
// flagDeltaAllowed is set unconditionally because keyed channels manage
// per-key delta readiness in keyedWritePublication — the channel-level
// first-full-then-delta progression does not apply.

// Ensure keyed channel state exists.
