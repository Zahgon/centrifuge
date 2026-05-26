package saferand

import (
	"math/rand"
	"sync"
)

// Rand is a concurrency-safe source of pseudo-random numbers. The Go
// stdlib's math/rand.Source is not concurrency-safe. The global source in
// math/rand would be concurrency safe (due to its internal use of
// lockedSource), but it is prone to inter-package interference with the PRNG
// state.
type Rand struct {
	mu sync.Mutex
	r  *rand.Rand
}

func New(seed int64) *Rand { _ = "STUB: not implemented"; return nil }

//nolint:gosec // Not used for security-sensitive purposes.

func (sr *Rand) Int63n(n int64) int64 { _ = "STUB: not implemented"; return 0 }

func (sr *Rand) Intn(n int) int { _ = "STUB: not implemented"; return 0 }
