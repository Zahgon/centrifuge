package main

import (
	"github.com/centrifugal/centrifuge"
)

// setupMapBroker creates either a memory or Redis map broker based on the
// --redis flag.
func setupMapBroker(node *centrifuge.Node, redisAddr string) (centrifuge.MapBroker, error) {
	_ = "STUB: not implemented"
	return *new(centrifuge.MapBroker), nil
}
