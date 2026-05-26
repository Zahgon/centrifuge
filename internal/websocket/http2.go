package websocket

import (
	"io"
	"net"
	"net/http"
	"time"
)

var (
	_ net.Conn = (*http2Stream)(nil)
)

// http2Stream is a wrapper for HTTP/2 extended CONNECT tunnel.
type http2Stream struct {
	io.ReadCloser
	io.Writer
	rc *http.ResponseController
}

func (s *http2Stream) Read(p []byte) (int, error) { _ = "STUB: not implemented"; return 0, nil }

func (s *http2Stream) Write(p []byte) (int, error) { _ = "STUB: not implemented"; return 0, nil }

func (s *http2Stream) Flush() error { _ = "STUB: not implemented"; return nil }

func (s *http2Stream) Close() error { _ = "STUB: not implemented"; return nil }

// LocalAddr is not implemented for HTTP/2 streams.
// May be taken from request if needed.
func (s *http2Stream) LocalAddr() net.Addr {
	_ = "STUB: not implemented"
	return *

	// RemoteAddr is not implemented for HTTP/2 streams.
	// May be taken from request if needed.
	new(net.Addr)
}

func (s *http2Stream) RemoteAddr() net.Addr {
	_ = "STUB: not implemented"
	return *

	// SetDeadline ...
	new(net.Addr)
}

func (s *http2Stream) SetDeadline(t time.Time) error { _ = "STUB: not implemented"; return nil }

// SetReadDeadline ...
func (s *http2Stream) SetReadDeadline(t time.Time) error { _ = "STUB: not implemented"; return nil }

// SetWriteDeadline ...
func (s *http2Stream) SetWriteDeadline(t time.Time) error { _ = "STUB: not implemented"; return nil }
