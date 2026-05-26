// Copyright 2017 The Gorilla WebSocket Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package websocket

import (
	"compress/flate"
	"io"
	"sync"
)

const (
	minCompressionLevel     = -2 // flate.HuffmanOnly not defined in Go < 1.6
	maxCompressionLevel     = flate.BestCompression
	defaultCompressionLevel = 1
)

var (
	flateWriterPools [maxCompressionLevel - minCompressionLevel + 1]sync.Pool
	flateReaderPool  = sync.Pool{New: func() interface{} {
		return flate.NewReader(nil)
	}}
)

func decompressNoContextTakeover(r io.Reader) io.ReadCloser {
	_ = "STUB: not implemented"

	// Add four bytes as specified in RFC
	return *new(io.ReadCloser)
}

// Add final block to squelch unexpected EOF error from flate reader.

func isValidCompressionLevel(level int) bool { _ = "STUB: not implemented"; return false }

func compressNoContextTakeover(w io.WriteCloser, level int) io.WriteCloser {
	_ = "STUB: not implemented"
	return *new(io.WriteCloser)
}

// truncWriter is an io.Writer that writes all but the last four bytes of the
// stream to another io.Writer.
type truncWriter struct {
	w io.WriteCloser
	n int
	p [4]byte
}

func (w *truncWriter) Write(p []byte) (int, error) {
	_ = "STUB: not implemented"

	// fill buffer first for simplicity.
	return 0, nil
}

type flateWriteWrapper struct {
	fw *flate.Writer
	tw *truncWriter
	p  *sync.Pool
}

func (w *flateWriteWrapper) Write(p []byte) (int, error) { _ = "STUB: not implemented"; return 0, nil }

func (w *flateWriteWrapper) Close() error { _ = "STUB: not implemented"; return nil }

type flateReadWrapper struct {
	fr io.ReadCloser
}

func (r *flateReadWrapper) Read(p []byte) (int, error) { _ = "STUB: not implemented"; return 0, nil }

// Preemptively place the reader back in the pool. This helps with
// scenarios where the application does not call NextReader() soon after
// this final read.

func (r *flateReadWrapper) Close() error { _ = "STUB: not implemented"; return nil }
