package centrifuge

import (
	"net/http"
	"sync"
	"time"
)

// HTTPStreamConfig represents config for HTTPStreamHandler.
type HTTPStreamConfig struct {
	PingPongConfig
	// MaxRequestBodySize limits request body size.
	MaxRequestBodySize int
}

// HTTPStreamHandler handles WebSocket client connections. WebSocket protocol
// is a bidirectional connection between a client and a server for low-latency
// communication.
type HTTPStreamHandler struct {
	node   *Node
	config HTTPStreamConfig
}

// NewHTTPStreamHandler creates new HTTPStreamHandler.
func NewHTTPStreamHandler(node *Node, config HTTPStreamConfig) *HTTPStreamHandler {
	_ = "STUB: not implemented"
	return nil
}

const (
	defaultMaxHTTPStreamingBodySize  = 64 * 1024
	streamingResponseWriteTimeout    = time.Second
	statusCodeClientConnectionClosed = 499
)

func (h *HTTPStreamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = "STUB: not implemented"
	return
}

// For pre-flight browser requests.

// need to execute this after client closeFn.

// An endpoint MUST NOT generate an HTTP/2 message containing connection-specific header fields.
// Source: RFC7540.

const (
	transportHTTPStream = "http_stream"
)

type httpStreamTransport struct {
	mu           sync.Mutex
	req          *http.Request
	ack          chan struct{}
	messages     chan [][]byte
	disconnectCh chan struct{}
	closedCh     chan struct{}
	closed       bool
	config       httpStreamTransportConfig
}

type httpStreamTransportConfig struct {
	protocolType ProtocolType
	pingPong     PingPongConfig
	protoMajor   uint8
}

func newHTTPStreamTransport(req *http.Request, config httpStreamTransportConfig, ack chan struct{}) *httpStreamTransport {
	_ = "STUB: not implemented"
	return nil
}

func (t *httpStreamTransport) Name() string { _ = "STUB: not implemented"; return "" }

func (t *httpStreamTransport) AcceptProtocol() string { _ = "STUB: not implemented"; return "" }

func (t *httpStreamTransport) Protocol() ProtocolType {
	_ = "STUB: not implemented"
	return *new(ProtocolType)
}

// ProtocolVersion returns transport protocol version.
func (t *httpStreamTransport) ProtocolVersion() ProtocolVersion {
	_ = "STUB: not implemented"
	return *

	// Unidirectional returns whether transport is unidirectional.
	new(ProtocolVersion)
}

func (t *httpStreamTransport) Unidirectional() bool {
	_ = "STUB: not implemented"

	// Emulation ...
	return false
}

func (t *httpStreamTransport) Emulation() bool {
	_ = "STUB: not implemented"

	// DisabledPushFlags ...
	return false
}

func (t *httpStreamTransport) DisabledPushFlags() uint64 {
	_ = "STUB: not implemented"

	// PingPongConfig ...
	return 0
}

func (t *httpStreamTransport) PingPongConfig() PingPongConfig {
	_ = "STUB: not implemented"
	return *new(PingPongConfig)
}

func (t *httpStreamTransport) Write(message []byte) error { _ = "STUB: not implemented"; return nil }

func (t *httpStreamTransport) WriteMany(messages ...[]byte) error {
	_ = "STUB: not implemented"
	return nil
}

func (t *httpStreamTransport) Close(_ Disconnect) error { _ = "STUB: not implemented"; return nil }
