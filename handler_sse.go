package centrifuge

import (
	"net/http"
	"sync"
)

// SSEConfig represents config for SSEHandler.
type SSEConfig struct {
	PingPongConfig
	// MaxRequestBodySize limits initial request body size (when SSE starts with POST).
	MaxRequestBodySize int
}

// SSEHandler handles WebSocket client connections. WebSocket protocol
// is a bidirectional connection between a client and a server for low-latency
// communication.
type SSEHandler struct {
	node   *Node
	config SSEConfig
}

// NewSSEHandler creates new SSEHandler.
func NewSSEHandler(node *Node, config SSEConfig) *SSEHandler { _ = "STUB: not implemented"; return nil }

// Since SSE is usually starts with a GET request (at least in browsers) we are looking
// for connect request in URL params. This should be a properly encoded command(s) in
// Centrifuge protocol.
const connectUrlParam = "cf_connect"

const defaultMaxSSEBodySize = 64 * 1024

func (h *SSEHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = "STUB: not implemented"
	return
}

// need to execute this after client closeFn.

// An endpoint MUST NOT generate an HTTP/2 message containing connection-specific header fields.
// Source: RFC7540.

const (
	transportSSE = "sse"
)

type sseTransport struct {
	mu           sync.Mutex
	req          *http.Request
	ack          chan struct{}
	messages     chan [][]byte
	disconnectCh chan struct{}
	closedCh     chan struct{}
	config       sseTransportConfig
	closed       bool
}

type sseTransportConfig struct {
	pingPong   PingPongConfig
	protoMajor uint8
}

func newSSETransport(req *http.Request, config sseTransportConfig, ack chan struct{}) *sseTransport {
	_ = "STUB: not implemented"
	return nil
}

func (t *sseTransport) Name() string { _ = "STUB: not implemented"; return "" }

func (t *sseTransport) AcceptProtocol() string { _ = "STUB: not implemented"; return "" }

func (t *sseTransport) Protocol() ProtocolType {
	_ = "STUB: not implemented"
	return *

	// ProtocolVersion returns transport protocol version.
	new(ProtocolType)
}

func (t *sseTransport) ProtocolVersion() ProtocolVersion {
	_ = "STUB: not implemented"
	return *

	// Unidirectional returns whether transport is unidirectional.
	new(ProtocolVersion)
}

func (t *sseTransport) Unidirectional() bool {
	_ = "STUB: not implemented"

	// Emulation ...
	return false
}

func (t *sseTransport) Emulation() bool {
	_ = "STUB: not implemented"

	// DisabledPushFlags ...
	return false
}

func (t *sseTransport) DisabledPushFlags() uint64 {
	_ = "STUB: not implemented"

	// PingPongConfig ...
	return 0
}

func (t *sseTransport) PingPongConfig() PingPongConfig {
	_ = "STUB: not implemented"
	return *new(PingPongConfig)
}

func (t *sseTransport) Write(message []byte) error { _ = "STUB: not implemented"; return nil }

func (t *sseTransport) WriteMany(messages ...[]byte) error { _ = "STUB: not implemented"; return nil }

func (t *sseTransport) Close(_ Disconnect) error { _ = "STUB: not implemented"; return nil }
