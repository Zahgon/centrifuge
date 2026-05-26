package bpool

import (
	"io"
	"sync"
)

var (
	// Verify ByteBuffer implements io.Writer.
	_ io.Writer = &ByteBuffer{}
)

// ByteBuffer implements a simple byte buffer.
type ByteBuffer struct {
	// B is the underlying byte slice.
	B []byte
}

// Reset resets bb.
func (bb *ByteBuffer) Reset() {
	_ = "STUB: not implemented"

	// Write appends p to bb.
	return
}

func (bb *ByteBuffer) Write(p []byte) (int, error) { _ = "STUB: not implemented"; return 0, nil }

// pools contain pools for byte slices of various capacities.
var pools [19]sync.Pool

// maxBufferLength is the maximum length of an element that can be added to the Pool.
const maxBufferLength = 262144 // 2^18

// Log of base two, round up (for v > 0).
func nextLogBase2(v uint32) uint32 { _ = "STUB: not implemented"; return 0 }

// Log of base two, round down (for v > 0)
func prevLogBase2(num uint32) uint32 { _ = "STUB: not implemented"; return 0 }

// GetByteBuffer returns byte buffer with the given capacity.
func GetByteBuffer(length int) *ByteBuffer { _ = "STUB: not implemented"; return nil }

// PutByteBuffer returns bb to the pool.
func PutByteBuffer(bb *ByteBuffer) { _ = "STUB: not implemented"; return }

// drop.
