// Copyright 2013 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package websocket

import (
	"bufio"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

const (
	// Frame header byte 0 bits from Section 5.2 of RFC 6455
	finalBit = 1 << 7
	rsv1Bit  = 1 << 6
	rsv2Bit  = 1 << 5
	rsv3Bit  = 1 << 4

	// Frame header byte 1 bits from Section 5.2 of RFC 6455
	maskBit = 1 << 7

	maxFrameHeaderSize         = 2 + 8 + 4 // Fixed header + length + mask
	maxControlFramePayloadSize = 125

	writeWait = time.Second

	defaultReadBufferSize  = 4096
	defaultWriteBufferSize = 4096

	continuationFrame = 0
	noFrame           = -1
)

// Close codes defined in RFC 6455, section 11.7.
const (
	CloseNormalClosure           = 1000
	CloseGoingAway               = 1001
	CloseProtocolError           = 1002
	CloseUnsupportedData         = 1003
	CloseNoStatusReceived        = 1005
	CloseAbnormalClosure         = 1006
	CloseInvalidFramePayloadData = 1007
	ClosePolicyViolation         = 1008
	CloseMessageTooBig           = 1009
	CloseMandatoryExtension      = 1010
	CloseInternalServerErr       = 1011
	CloseServiceRestart          = 1012
	CloseTryAgainLater           = 1013
	CloseTLSHandshake            = 1015
)

// The message types are defined in RFC 6455, section 11.8.
const (
	// TextMessage denotes a text data message. The text message payload is
	// interpreted as UTF-8 encoded text data.
	TextMessage = 1

	// BinaryMessage denotes a binary data message.
	BinaryMessage = 2

	// CloseMessage denotes a close control message. The optional message
	// payload contains a numeric code and text. Use the FormatCloseMessage
	// function to format a close message payload.
	CloseMessage = 8

	// PingMessage denotes a ping control message. The optional message payload
	// is UTF-8 encoded text.
	PingMessage = 9

	// PongMessage denotes a pong control message. The optional message payload
	// is UTF-8 encoded text.
	PongMessage = 10
)

// ErrCloseSent is returned when the application writes a message to the
// connection after sending a close message.
var ErrCloseSent = errors.New("websocket: close sent")

// ErrReadLimit is returned when reading a message that is larger than the
// read limit set for the connection.
var ErrReadLimit = errors.New("websocket: read limit exceeded")

// netError satisfies the net Error interface.
type netError struct {
	msg       string
	temporary bool
	timeout   bool
}

func (e *netError) Error() string   { _ = "STUB: not implemented"; return "" }
func (e *netError) Temporary() bool { _ = "STUB: not implemented"; return false }
func (e *netError) Timeout() bool {
	_ = "STUB: not implemented"

	// CloseError represents a close message.
	return false
}

type CloseError struct {
	// Text is the optional text payload.
	Text string
	// Code is defined in RFC 6455, section 11.7.
	Code int
}

func (e *CloseError) Error() string { _ = "STUB: not implemented"; return "" }

// IsCloseError returns boolean indicating whether the error is a *CloseError
// with one of the specified codes.
func IsCloseError(err error, codes ...int) bool { _ = "STUB: not implemented"; return false }

// IsUnexpectedCloseError returns boolean indicating whether the error is a
// *CloseError with a code not in the list of expected codes.
func IsUnexpectedCloseError(err error, expectedCodes ...int) bool {
	_ = "STUB: not implemented"
	return false
}

var (
	errWriteTimeout        = &netError{msg: "websocket: write timeout", timeout: true, temporary: true}
	errUnexpectedEOF       = &CloseError{Code: CloseAbnormalClosure, Text: io.ErrUnexpectedEOF.Error()}
	errBadWriteOpCode      = errors.New("websocket: bad write message type")
	errWriteClosed         = errors.New("websocket: write closed")
	errInvalidControlFrame = errors.New("websocket: invalid control frame")
)

func newMaskKey() [4]byte { _ = "STUB: not implemented"; return nil }

func hideTempErr(err error) error { _ = "STUB: not implemented"; return nil }

//nolint:staticcheck

func isControl(frameType int) bool { _ = "STUB: not implemented"; return false }

func isData(frameType int) bool { _ = "STUB: not implemented"; return false }

var validReceivedCloseCodes = map[int]bool{
	// see http://www.iana.org/assignments/websocket/websocket.xhtml#close-code-number

	CloseNormalClosure:           true,
	CloseGoingAway:               true,
	CloseProtocolError:           true,
	CloseUnsupportedData:         true,
	CloseNoStatusReceived:        false,
	CloseAbnormalClosure:         false,
	CloseInvalidFramePayloadData: true,
	ClosePolicyViolation:         true,
	CloseMessageTooBig:           true,
	CloseMandatoryExtension:      true,
	CloseInternalServerErr:       true,
	CloseServiceRestart:          true,
	CloseTryAgainLater:           true,
	CloseTLSHandshake:            false,
}

func isValidReceivedCloseCode(code int) bool { _ = "STUB: not implemented"; return false }

// BufferPool represents a pool of buffers. The *sync.Pool type satisfies this
// interface. The type of the value stored in a pool is not specified.
type BufferPool interface {
	// Get gets a value from the pool or returns nil if the pool is empty.
	Get() interface{}
	// Put adds a value to the pool.
	Put(interface{})
}

// writePoolData is the type added to the write buffer pool. This wrapper is
// used to prevent applications from peeking at and depending on the values
// added to the pool.
type writePoolData struct{ buf []byte }

// The Conn type represents a WebSocket connection.
type Conn struct {
	conn net.Conn

	reader  io.ReadCloser // the current reader returned to the application
	readErr error

	mu            chan struct{} // used as mutex to protect write to conn
	writeDeadline time.Time
	writer        io.WriteCloser // the current writer returned to the application
	writePool     BufferPool
	writeErr      error

	newDecompressionReader func(io.Reader) io.ReadCloser
	messageReader          *messageReader // the current low-level reader
	handlePong             func([]byte) error
	handlePing             func([]byte) error
	newCompressionWriter   func(io.WriteCloser, int) io.WriteCloser
	br                     *bufio.Reader
	writeBuf               []byte // frame is constructed in this buffer.
	readLength             int64  // Message size.
	readLimit              int64  // Maximum message size.
	compressionLevel       int
	readMaskPos            int
	writeBufSize           int
	writeErrMu             sync.Mutex
	// bytes remaining in current frame.
	// set setReadRemaining to safely update this value and prevent overflow
	readRemaining          int64
	readMaskKey            [4]byte
	readDecompress         bool // whether last read frame had RSV1 set
	readFinal              bool // true the current message has more frames.
	enableWriteCompression bool
	isServer               bool
}

func newConn(conn net.Conn, isServer bool, readBufferSize, writeBufferSize int, writeBufferPool BufferPool, br *bufio.Reader, writeBuf []byte) *Conn {
	_ = "STUB: not implemented"
	return nil
}

// must be large enough for control frame

// setReadRemaining tracks the number of bytes remaining on the connection. If n
// overflows, an ErrReadLimit is returned.
func (c *Conn) setReadRemaining(n int64) error { _ = "STUB: not implemented"; return nil }

// Close closes the underlying network connection without sending or waiting
// for a close message.
func (c *Conn) Close() error { _ = "STUB: not implemented"; return nil }

func (c *Conn) IsCompressionNegotiated() bool { _ = "STUB: not implemented"; return false }

// LocalAddr returns the local network address.
func (c *Conn) LocalAddr() net.Addr {
	_ = "STUB: not implemented"
	return *

	// RemoteAddr returns the remote network address.
	new(net.Addr)
}

func (c *Conn) RemoteAddr() net.Addr { _ = "STUB: not implemented"; return *new(net.Addr) }

// Write methods

func (c *Conn) writeFatal(err error) error { _ = "STUB: not implemented"; return nil }

func (c *Conn) read(n int) ([]byte, error) { _ = "STUB: not implemented"; return nil, nil }

func (c *Conn) write(frameType int, deadline time.Time, buf0, buf1 []byte) error {
	_ = "STUB: not implemented"
	return nil
}

func (c *Conn) writeBufs(bufs ...[]byte) error { _ = "STUB: not implemented"; return nil }

// WriteControl writes a control message with the given deadline. The allowed
// message types are CloseMessage, PingMessage and PongMessage.
func (c *Conn) WriteControl(messageType int, data []byte, deadline time.Time) error {
	_ = "STUB: not implemented"
	return nil
}

// No timeout for zero time.

// beginMessage prepares a connection and message writer for a new message.
func (c *Conn) beginMessage(mw *messageWriter, messageType int) error {
	_ = "STUB: not implemented"
	// Close previous writer if not already closed by the application. It's
	// probably better to return an error in this situation, but we cannot
	// change this without breaking existing applications.
	return nil
}

// NextWriter returns a writer for the next message to send. The writer's Close
// method flushes the complete message to the network.
//
// There can be at most one open writer on a connection. NextWriter closes the
// previous writer if the application has not already done so.
//
// All message types (TextMessage, BinaryMessage, CloseMessage, PingMessage and
// PongMessage) are supported.
func (c *Conn) NextWriter(messageType int) (io.WriteCloser, error) {
	_ = "STUB: not implemented"
	return *new(io.WriteCloser), nil
}

type messageWriter struct {
	c         *Conn
	err       error
	pos       int  // end of data in writeBuf.
	frameType int  // type of the current frame.
	compress  bool // whether next call to flushFrame should set RSV1
}

func (w *messageWriter) endMessage(err error) error { _ = "STUB: not implemented"; return nil }

// flushFrame writes buffered data and extra as a frame to the network. The
// final argument indicates that this is the last frame in the message.
func (w *messageWriter) flushFrame(final bool, extra []byte) error {
	_ = "STUB: not implemented"
	return nil
}

// Check for invalid control frames.

// Assume that the frame starts at beginning of c.writeBuf.

// Adjust up if mask not included in the header.

// Setup for next frame.

func (w *messageWriter) ncopy(max int) (int, error) { _ = "STUB: not implemented"; return 0, nil }

func (w *messageWriter) Write(p []byte) (int, error) { _ = "STUB: not implemented"; return 0, nil }

// Don't buffer large messages.

func (w *messageWriter) WriteString(p string) (int, error) {
	_ = "STUB: not implemented"
	return 0, nil
}

func (w *messageWriter) ReadFrom(r io.Reader) (nn int64, err error) {
	_ = "STUB: not implemented"
	return 0, nil
}

func (w *messageWriter) Close() error { _ = "STUB: not implemented"; return nil }

// WritePreparedMessage writes prepared message into connection.
func (c *Conn) WritePreparedMessage(pm *PreparedMessage) error {
	_ = "STUB: not implemented"
	return nil
}

// WriteMessage is a helper method for getting a writer using NextWriter,
// writing the message and closing the writer.
func (c *Conn) WriteMessage(messageType int, data []byte) error {
	_ = "STUB: not implemented"
	return nil
}

// Fast path with no allocations and single frame.

// SetWriteDeadline sets the write deadline on the underlying network
// connection. After a write has timed out, the websocket state is corrupt and
// all future writes will return an error. A zero value for t means writes will
// not time out.
func (c *Conn) SetWriteDeadline(t time.Time) error { _ = "STUB: not implemented"; return nil }

// Read methods

func (c *Conn) advanceFrame() (int, error) {
	_ = "STUB: not implemented"
	// 1. Skip remainder of previous frame.
	return 0, nil
}

// 2. Read and parse first two bytes of frame header.
// To aid debugging, collect and report all errors in the first two bytes
// of the header.

// 3. Read and parse frame length as per
// https://tools.ietf.org/html/rfc6455#section-5.2
//
// The length of the "Payload data", in bytes: if 0-125, that is the payload
// length.
// - If 126, the following 2 bytes interpreted as a 16-bit unsigned
// integer are the payload length.
// - If 127, the following 8 bytes interpreted as
// a 64-bit unsigned integer (the most significant bit MUST be 0) are the
// payload length. Multibyte length quantities are expressed in network byte
// order.

// 4. Handle frame masking.

// 5. For text and binary messages, enforce read limit and return.

// Don't allow readLength to overflow in the presence of a large readRemaining
// counter.

// 6. Read control frame payload.

// 7. Process control frame payload.

func (c *Conn) handleProtocolError(message string) error { _ = "STUB: not implemented"; return nil }

// NextReader returns the next data message received from the peer. The
// returned messageType is either TextMessage or BinaryMessage.
//
// There can be at most one open reader on a connection. NextReader discards
// the previous message if the application has not already consumed it.
//
// Applications must break out of the application's read loop when this method
// returns a non-nil error value. Errors returned from this method are
// permanent. Once this method returns a non-nil error, all subsequent calls to
// this method return the same error.
func (c *Conn) NextReader() (messageType int, r io.Reader, err error) {
	_ = "STUB: not implemented"
	// Close previous reader, only relevant for decompression.
	return 0, *new(io.Reader), nil
}

type messageReader struct{ c *Conn }

func (r *messageReader) Read(b []byte) (int, error) { _ = "STUB: not implemented"; return 0, nil }

func (r *messageReader) Close() error {
	_ = "STUB: not implemented"

	// ReadMessage is a helper method for getting a reader using NextReader and
	// reading from that reader to a buffer.
	return nil
}

func (c *Conn) ReadMessage() (messageType int, p []byte, err error) {
	_ = "STUB: not implemented"
	return 0, nil, nil
}

// SetReadDeadline sets the read deadline on the underlying network connection.
// After a read has timed out, the websocket connection state is corrupt and
// all future reads will return an error. A zero value for t means reads will
// not time out.
func (c *Conn) SetReadDeadline(t time.Time) error { _ = "STUB: not implemented"; return nil }

// SetReadLimit sets the maximum size in bytes for a message read from the peer. If a
// message exceeds the limit, the connection sends a close message to the peer
// and returns ErrReadLimit to the application.
func (c *Conn) SetReadLimit(limit int64) { _ = "STUB: not implemented"; return }

func (c *Conn) defaultCloseHandler(code int, _ string) error { _ = "STUB: not implemented"; return nil }

func (c *Conn) defaultPingHandler(message []byte) error { _ = "STUB: not implemented"; return nil }

//nolint:staticcheck

// SetPingHandler sets the handler for ping messages received from the peer.
// The appData argument to h is the PING message application data. The default
// ping handler sends a pong to the peer.
//
// The handler function is called from the NextReader, ReadMessage and message
// reader Read methods. The application must read the connection to process
// ping messages as described in the section on Control Messages above.
func (c *Conn) SetPingHandler(h func(appData []byte) error) { _ = "STUB: not implemented"; return }

func (c *Conn) defaultPongHandler(_ []byte) error {
	_ = "STUB: not implemented"

	// SetPongHandler sets the handler for pong messages received from the peer.
	// The appData argument to h is the PONG message application data. The default
	// pong handler does nothing.
	//
	// The handler function is called from the NextReader, ReadMessage and message
	// reader Read methods. The application must read the connection to process
	// pong messages as described in the section on Control Messages above.
	return nil
}

func (c *Conn) SetPongHandler(h func(appData []byte) error) { _ = "STUB: not implemented"; return }

// NetConn returns the underlying connection that is wrapped by c.
// Note that writing to or reading from this connection directly will corrupt the
// WebSocket connection.
func (c *Conn) NetConn() net.Conn {
	_ = "STUB: not implemented"

	// EnableWriteCompression enables and disables write compression of
	// subsequent text and binary messages. This function is a noop if
	// compression was not negotiated with the peer.
	return *new(net.Conn)
}

func (c *Conn) EnableWriteCompression(enable bool) { _ = "STUB: not implemented"; return }

// SetCompressionLevel sets the flate compression level for subsequent text and
// binary messages. This function is a noop if compression was not negotiated
// with the peer. See the compress/flate package for a description of
// compression levels.
func (c *Conn) SetCompressionLevel(level int) error { _ = "STUB: not implemented"; return nil }

// FormatCloseMessage formats closeCode and text as a WebSocket close message.
// An empty message is returned for code CloseNoStatusReceived.
func FormatCloseMessage(closeCode int, text string) []byte { _ = "STUB: not implemented"; return nil }

// Return empty message because it's illegal to send
// CloseNoStatusReceived. Return non-nil value in case application
// checks for nil.
