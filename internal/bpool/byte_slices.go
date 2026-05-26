package bpool

import (
	"sync"
)

const (
	// maxByteSlicesBufLength is the maximum length for pooled [][]byte buffers.
	maxByteSlicesBufLength = 4096 // 2^12
)

// ByteSlicesBuf wraps [][]byte to avoid allocations when using sync.Pool
type ByteSlicesBuf struct {
	B [][]byte
}

// byteSlicesBufPools contain pools for [][]byte slices of various capacities (power of 2).
// Supports up to 2^12 = 4096 items.
var byteSlicesBufPools [13]sync.Pool

// GetByteSlicesBuf returns a ByteSlicesBuf with capacity >= length
func GetByteSlicesBuf(length int) *ByteSlicesBuf { _ = "STUB: not implemented"; return nil }

// default

// PutByteSlicesBuf returns buf to the pool
func PutByteSlicesBuf(buf *ByteSlicesBuf) { _ = "STUB: not implemented"; return }

// drop oversized buffers

// Clear the buffer

// nextLogBase2ByteSlices returns log2(v) rounded up
func nextLogBase2ByteSlices(v uint32) uint32 { _ = "STUB: not implemented"; return 0 }

// prevLogBase2ByteSlices returns log2(v) rounded down
func prevLogBase2ByteSlices(v uint32) uint32 { _ = "STUB: not implemented"; return 0 }
