package filter

import "github.com/centrifugal/protocol"

func Eq(key, val string) *protocol.FilterNode { _ = "STUB: not implemented"; return nil }

func Neq(key, val string) *protocol.FilterNode { _ = "STUB: not implemented"; return nil }

func In(key string, vals ...string) *protocol.FilterNode { _ = "STUB: not implemented"; return nil }

func Nin(key string, vals ...string) *protocol.FilterNode { _ = "STUB: not implemented"; return nil }

func Gt(key, val string) *protocol.FilterNode { _ = "STUB: not implemented"; return nil }

func Gte(key, val string) *protocol.FilterNode { _ = "STUB: not implemented"; return nil }

func Lt(key, val string) *protocol.FilterNode { _ = "STUB: not implemented"; return nil }

func Lte(key, val string) *protocol.FilterNode { _ = "STUB: not implemented"; return nil }

func Contains(key, val string) *protocol.FilterNode { _ = "STUB: not implemented"; return nil }

func Starts(key, val string) *protocol.FilterNode { _ = "STUB: not implemented"; return nil }

func Ends(key, val string) *protocol.FilterNode { _ = "STUB: not implemented"; return nil }

func Exists(key string) *protocol.FilterNode { _ = "STUB: not implemented"; return nil }

func NotExists(key string) *protocol.FilterNode { _ = "STUB: not implemented"; return nil }

// And combines multiple FilterNode children with logical AND
func And(nodes ...*protocol.FilterNode) *protocol.FilterNode { _ = "STUB: not implemented"; return nil }

// Or combines multiple FilterNode children with logical OR
func Or(nodes ...*protocol.FilterNode) *protocol.FilterNode { _ = "STUB: not implemented"; return nil }

// Not negates a single FilterNode
func Not(node *protocol.FilterNode) *protocol.FilterNode { _ = "STUB: not implemented"; return nil }
