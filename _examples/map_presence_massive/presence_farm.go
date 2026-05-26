package main

import (
	"context"

	"github.com/centrifugal/centrifuge"
)

// presenceFarmConfig describes a synthetic map_clients presence load.
// Entries are pushed directly via the map broker — no real WebSocket
// clients — so we can stress the protocol with hundreds of thousands of
// entries on a single process.
//
// After the initial bulk fill, churn runs as paired "replace" events: each
// tick removes one random live entry and publishes one random non-live
// entry from the fixed pool. Population stays exactly at InitialCount, and
// every key is reused over time so each grid cell flickers with activity.
type presenceFarmConfig struct {
	Channel      string
	PoolSize     int // total stable id pool — keys are c_0..c_<PoolSize-1>
	InitialCount int
	ChurnPerSec  int
}

func runPresenceFarm(ctx context.Context, node *centrifuge.Node, cfg presenceFarmConfig) {
	_ = "STUB: not implemented"
	return
}

// ~97.5% full

// Shuffle 0..PoolSize-1 once. The first InitialCount become live, the
// rest become the not-live pool.

// popRandom removes a random element from a slice and returns it. Caller
// must hold mu.

// If everyone is live there's nothing to swap with — disable churn.
