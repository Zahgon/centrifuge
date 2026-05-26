package timers

import (
	"sync"
	"time"
)

var timerPool sync.Pool

// AcquireTimer from pool.
func AcquireTimer(d time.Duration) *time.Timer { _ = "STUB: not implemented"; return nil }

// ReleaseTimer to pool.
func ReleaseTimer(tm *time.Timer) {
	_ = "STUB: not implemented"

	// Collect possibly added time from the channel
	// If timer has been stopped and nobody collected its value.
	return
}
