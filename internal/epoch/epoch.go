// Package epoch provides epoch string generation for stream position tracking.
package epoch

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// Generate creates a random 8-character epoch string using crypto/rand.
// With 52^8 ≈ 5.3×10^13 possible values, collision probability is negligible.
func Generate() string { _ = "STUB: not implemented"; return "" }
