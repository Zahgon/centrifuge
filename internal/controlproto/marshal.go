package controlproto

import "github.com/centrifugal/centrifuge/internal/controlpb"

// Encoder ...
type Encoder interface {
	EncodeCommand(*controlpb.Command) ([]byte, error)
}

var _ Encoder = (*ProtobufEncoder)(nil)

// ProtobufEncoder ...
type ProtobufEncoder struct{}

// NewProtobufEncoder ...
func NewProtobufEncoder() *ProtobufEncoder { _ = "STUB: not implemented"; return nil }

// EncodeCommand ...
func (e *ProtobufEncoder) EncodeCommand(cmd *controlpb.Command) ([]byte, error) {
	_ = "STUB: not implemented"
	return nil, nil
}
