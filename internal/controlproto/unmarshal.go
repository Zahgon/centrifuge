package controlproto

import "github.com/centrifugal/centrifuge/internal/controlpb"

// Decoder ...
type Decoder interface {
	DecodeCommand([]byte) (*controlpb.Command, error)
}

var _ Decoder = (*ProtobufDecoder)(nil)

// ProtobufDecoder ...
type ProtobufDecoder struct{}

// NewProtobufDecoder ...
func NewProtobufDecoder() *ProtobufDecoder { _ = "STUB: not implemented"; return nil }

// DecodeCommand ...
func (e *ProtobufDecoder) DecodeCommand(data []byte) (*controlpb.Command, error) {
	_ = "STUB: not implemented"
	return nil, nil
}
