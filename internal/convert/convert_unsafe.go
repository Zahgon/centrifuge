//go:build !appengine

package convert

// BytesToString converts byte slice to string.
func BytesToString(b []byte) string { _ = "STUB: not implemented"; return "" }

//nolint:gosec // Audited.

// StringToBytes converts string to byte slice.
func StringToBytes(s string) []byte { _ = "STUB: not implemented"; return nil }

//nolint:gosec // Audited.
