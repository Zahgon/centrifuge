package memstream

import (
	"container/list"
)

// Item to be kept inside stream.
type Item struct {
	Offset uint64
	Value  any
}

type AppVersion struct {
	// Version is an incremental version of state.
	Version uint64
	// Epoch is a string that identifies the epoch of version number.
	Epoch string
}

// Stream is a non-thread safe in-memory data structure that
// maintains a stream of values limited by size and provides
// methods to access a range of values from provided position.
type Stream struct {
	top     uint64
	list    *list.List
	index   map[uint64]*list.Element
	epoch   string
	version AppVersion
}

// New creates new Stream.
func New() *Stream { _ = "STUB: not implemented"; return nil }

// Add item to stream.
func (s *Stream) Add(v any, size int, version uint64, versionEpoch string) (uint64, error) {
	_ = "STUB: not implemented"
	return 0, nil
}

// Top returns top of stream.
func (s *Stream) Top() uint64 {
	_ = "STUB: not implemented"

	// Epoch returns epoch of stream.
	return 0
}

func (s *Stream) Epoch() string { _ = "STUB: not implemented"; return "" }

func (s *Stream) TopVersion() uint64 { _ = "STUB: not implemented"; return 0 }

func (s *Stream) TopVersionEpoch() string { _ = "STUB: not implemented"; return "" }

// Reset stream.
func (s *Stream) Reset() { _ = "STUB: not implemented"; return }

// Clear stream data.
func (s *Stream) Clear() { _ = "STUB: not implemented"; return }

// Get items since provided position.
// If seq is zero then elements since current first element in stream will be returned.
func (s *Stream) Get(offset uint64, useOffset bool, limit int, reverse bool) ([]Item, uint64, error) {
	_ = "STUB: not implemented"
	return nil, 0, nil
}
