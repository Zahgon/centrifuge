// Copyright 2016 The Gorilla WebSocket Authors. All rights reserved.  Use of
// this source code is governed by a BSD-style license that can be found in the
// LICENSE file.

//go:build !appengine

package websocket

import "unsafe"

const wordSize = int(unsafe.Sizeof(uintptr(0))) //nolint:gosec // Audited.

func maskBytes(key [4]byte, pos int, b []byte) int {
	_ = "STUB: not implemented"
	// Mask one byte at a time for small buffers.
	return 0
}

// Mask one byte at a time to word boundary.
//nolint:gosec // Audited.

// Create aligned word size key.

//nolint:gosec // Audited.

// Mask one word at a time.

//nolint:gosec // Audited.

// Mask one byte at a time for remaining bytes.
