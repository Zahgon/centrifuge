//go:build !appengine

package websocket

// StringToBytes converts string to byte slice.
func stringToBytes(s string) []byte { _ = "STUB: not implemented"; return nil }

//nolint:gosec // Audited.
