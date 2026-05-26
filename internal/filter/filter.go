package filter

import (
	"github.com/centrifugal/protocol"
)

// Node operations.
const (
	OpLeaf = "" // leaf node
	OpAnd  = "and"
	OpOr   = "or"
	OpNot  = "not"
)

// Leaf comparison operators.
const (
	CompareEQ         = "eq"
	CompareNotEQ      = "neq"
	CompareIn         = "in"
	CompareNotIn      = "nin"
	CompareExists     = "ex"
	CompareNotExists  = "nex"
	CompareStartsWith = "sw"
	CompareEndsWith   = "ew"
	CompareContains   = "ct"
	CompareGT         = "gt"
	CompareGTE        = "gte"
	CompareLT         = "lt"
	CompareLTE        = "lte"
)

// Match checks if the provided tags match the filter.
func Match(f *protocol.FilterNode, tags map[string]string) (bool, error) {
	_ = "STUB: not implemented"
	return false, nil
}

// numeric comparisons unified

// Validate ensures the filter tree is well-formed.
// Called at subscription time.
func Validate(f *protocol.FilterNode) error { _ = "STUB: not implemented"; return nil }

// Leaf must have a comparison operator.

// All leafs must have a key except exists/nex

// Hash computes filter hash.
func Hash(f *protocol.FilterNode) [32]byte { _ = "STUB: not implemented"; return nil }

// get canonical hash.
// SHA-256 hash.
