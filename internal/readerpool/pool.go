package readerpool

import (
	"bytes"
	"strings"
	"sync"
)

var stringReaderPool sync.Pool

// GetStringReader from pool.
func GetStringReader(data string) *strings.Reader { _ = "STUB: not implemented"; return nil }

// PutStringReader to pool.
func PutStringReader(reader *strings.Reader) { _ = "STUB: not implemented"; return }

var bytesReaderPool sync.Pool

// GetBytesReader from pool.
func GetBytesReader(data []byte) *bytes.Reader { _ = "STUB: not implemented"; return nil }

// PutBytesReader to pool.
func PutBytesReader(reader *bytes.Reader) { _ = "STUB: not implemented"; return }
