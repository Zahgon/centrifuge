package cancelctx

import (
	"context"
	"time"
)

// customCancelContext wraps context and cancels as soon as channel closed.
type customCancelContext struct {
	context.Context
	ch <-chan struct{}
}

// Deadline not used.
func (c customCancelContext) Deadline() (time.Time, bool) {
	_ = "STUB: not implemented"
	return *

	// Done returns channel that will be closed as soon as connection closed.
	new(time.Time), false
}

func (c customCancelContext) Done() <-chan struct{} {
	_ = "STUB: not implemented"

	// Err returns context error.
	return nil
}

func (c customCancelContext) Err() error { _ = "STUB: not implemented"; return nil }

// New returns a wrapper context around original context that will
// be canceled on channel close.
func New(ctx context.Context, ch <-chan struct{}) context.Context {
	_ = "STUB: not implemented"
	return *new(context.Context)
}
