package centrifuge

import (
	"errors"
	"net/http"

	"github.com/centrifugal/protocol"
)

// EmulationConfig is a config for EmulationHandler.
type EmulationConfig struct {
	// MaxRequestBodySize limits request body size (in bytes). By default we accept 64kb max.
	MaxRequestBodySize int
}

// EmulationHandler allows receiving client protocol commands from client and proxy
// them to the right node (where client session lives). This makes it possible to use
// unidirectional transports for server-to-clients data flow but still emulate
// bidirectional connection - thanks to this handler. Redirection to the correct node
// works over Survey.
type EmulationHandler struct {
	node     *Node
	config   EmulationConfig
	emuLayer *emulationLayer
}

// NewEmulationHandler creates new EmulationHandler.
func NewEmulationHandler(node *Node, config EmulationConfig) *EmulationHandler {
	_ = "STUB: not implemented"
	return nil
}

func (s *EmulationHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	_ = "STUB: not implemented"
	return
}

// For pre-flight browser requests.

type emulationLayer struct {
	node *Node
}

func newEmulationLayer(node *Node) *emulationLayer { _ = "STUB: not implemented"; return nil }

func (l *emulationLayer) Emulate(req *protocol.EmulationRequest) error {
	_ = "STUB: not implemented"
	return nil
}

const emulationOp = "centrifuge_emulation"

var errNodeNotFound = errors.New("node not found")

func (n *Node) sendEmulation(req *protocol.EmulationRequest) error {
	_ = "STUB: not implemented"
	return nil
}

type emulationSurveyHandler struct {
	node *Node
}

func newEmulationSurveyHandler(node *Node) *emulationSurveyHandler {
	_ = "STUB: not implemented"
	return nil
}

const (
	emulationErrorCodeBadRequest = 1
	emulationErrorCodeNoSession  = 2
)

func (h *emulationSurveyHandler) HandleEmulation(e SurveyEvent, cb SurveyCallback) {
	_ = "STUB: not implemented"
	return
}
