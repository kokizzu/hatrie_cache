package hatDataStructure

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	// AggregateStateEnvelopeWireVersion identifies the envelope encoding.
	AggregateStateEnvelopeWireVersion byte = 1
	// MaxAggregateStateEnvelopeBytes bounds one encoded partial aggregate.
	MaxAggregateStateEnvelopeBytes = 64 << 20
	// MaxAggregateStatePayloadBytes leaves room for the envelope header and
	// checksum while bounding allocations made by callers.
	MaxAggregateStatePayloadBytes = MaxAggregateStateEnvelopeBytes - 128
	// MaxAggregateStateKindBytes bounds the logical aggregate kind name.
	MaxAggregateStateKindBytes = 64

	// AggregateStateKindHyperLogLog identifies a HyperLogLog state payload.
	AggregateStateKindHyperLogLog = "hyperloglog"
	// AggregateStateKindCountMinSketch identifies a Count-Min Sketch payload.
	AggregateStateKindCountMinSketch = "count_min_sketch"
	// AggregateStateKindTopK identifies an approximate top-K payload.
	AggregateStateKindTopK = "top_k"
	// AggregateStateKindTDigest identifies an approximate quantile payload.
	AggregateStateKindTDigest = "tdigest"
	// AggregateStateKindArgMaxInt64 identifies a mergeable int64 arg-max state.
	AggregateStateKindArgMaxInt64 = "argmax_int64"
	// AggregateStateKindArgMinInt64 identifies a mergeable int64 arg-min state.
	AggregateStateKindArgMinInt64 = "argmin_int64"
)

var (
	// ErrAggregateStateEnvelopeInvalid indicates invalid envelope metadata.
	ErrAggregateStateEnvelopeInvalid = errors.New("hatriecache: aggregate state envelope is invalid")
	// ErrAggregateStateEnvelopeLimit indicates a payload or envelope over the
	// supported bounded size.
	ErrAggregateStateEnvelopeLimit = errors.New("hatriecache: aggregate state envelope limit exceeded")
	// ErrAggregateStateEnvelopeWire indicates malformed or unsupported wire data.
	ErrAggregateStateEnvelopeWire = errors.New("hatriecache: aggregate state envelope wire is invalid")
	// ErrAggregateStateKindMismatch indicates that a typed decoder received a
	// different aggregate kind.
	ErrAggregateStateKindMismatch = errors.New("hatriecache: aggregate state kind mismatch")
	// ErrAggregateStateVersionUnsupported indicates a typed state version that
	// the current decoder does not understand.
	ErrAggregateStateVersionUnsupported = errors.New("hatriecache: aggregate state version is unsupported")
)

var aggregateStateEnvelopeMagic = [4]byte{'H', 'A', 'G', '1'}

// AggregateStateEnvelope wraps a versioned partial aggregate payload for
// transfer between workers or nodes. Unknown kinds remain decodable at this
// layer so newer aggregate implementations can be rolled out independently.
type AggregateStateEnvelope struct {
	Kind    string `json:"kind"`
	Version uint64 `json:"version"`
	Payload []byte `json:"payload"`
}

// NewAggregateStateEnvelope constructs a validated envelope and copies the
// payload so callers can reuse their buffer after the call returns.
func NewAggregateStateEnvelope(kind string, version uint64, payload []byte) (AggregateStateEnvelope, error) {
	envelope := AggregateStateEnvelope{Kind: kind, Version: version, Payload: payload}
	if err := validateAggregateStateEnvelope(envelope); err != nil {
		return AggregateStateEnvelope{}, err
	}
	envelope.Payload = append([]byte(nil), payload...)
	return envelope, nil
}

// MarshalAggregateStateEnvelope encodes kind, version, and payload directly
// without retaining or copying the caller's payload before encoding.
func MarshalAggregateStateEnvelope(kind string, version uint64, payload []byte) ([]byte, error) {
	return MarshalAggregateStateEnvelopeWithPayload(kind, version, len(payload), func(encoded []byte) []byte {
		return append(encoded, payload...)
	})
}

// MarshalAggregateStateEnvelopeWithPayload encodes a payload directly into
// the final wire buffer. The callback must append exactly payloadLength bytes;
// this lets typed aggregate implementations avoid a temporary payload copy.
func MarshalAggregateStateEnvelopeWithPayload(kind string, version uint64, payloadLength int, appendPayload func([]byte) []byte) ([]byte, error) {
	if appendPayload == nil {
		return nil, fmt.Errorf("%w: payload writer is nil", ErrAggregateStateEnvelopeInvalid)
	}
	return marshalAggregateStateEnvelopeFields(kind, version, payloadLength, appendPayload)
}

// MarshalBinary encodes the envelope as deterministic HAG1 data. The checksum
// covers the complete header and payload, including kind and version.
func (envelope AggregateStateEnvelope) MarshalBinary() ([]byte, error) {
	if err := validateAggregateStateEnvelope(envelope); err != nil {
		return nil, err
	}
	return MarshalAggregateStateEnvelopeWithPayload(envelope.Kind, envelope.Version, len(envelope.Payload), func(encoded []byte) []byte {
		return append(encoded, envelope.Payload...)
	})
}

func marshalAggregateStateEnvelopeFields(kind string, version uint64, payloadLength int, appendPayload func([]byte) []byte) ([]byte, error) {
	if err := validateAggregateStateMetadata(kind, version); err != nil {
		return nil, err
	}
	if payloadLength < 0 || payloadLength > MaxAggregateStatePayloadBytes {
		return nil, ErrAggregateStateEnvelopeLimit
	}
	capacity := len(aggregateStateEnvelopeMagic) + 1 + aggregateStateUvarintLen(uint64(len(kind))) + len(kind) + aggregateStateUvarintLen(version) + aggregateStateUvarintLen(uint64(payloadLength)) + payloadLength + 4
	if capacity > MaxAggregateStateEnvelopeBytes {
		return nil, ErrAggregateStateEnvelopeLimit
	}
	encoded := make([]byte, 0, capacity)
	encoded = append(encoded, aggregateStateEnvelopeMagic[:]...)
	encoded = append(encoded, AggregateStateEnvelopeWireVersion)
	encoded = appendAggregateStateUvarint(encoded, uint64(len(kind)))
	encoded = append(encoded, kind...)
	encoded = appendAggregateStateUvarint(encoded, version)
	encoded = appendAggregateStateUvarint(encoded, uint64(payloadLength))
	payloadStart := len(encoded)
	encoded = appendPayload(encoded)
	if len(encoded) != payloadStart+payloadLength {
		return nil, fmt.Errorf("%w: payload writer produced %d bytes, want %d", ErrAggregateStateEnvelopeInvalid, len(encoded)-payloadStart, payloadLength)
	}
	if len(encoded)+4 > MaxAggregateStateEnvelopeBytes {
		return nil, ErrAggregateStateEnvelopeLimit
	}
	var checksum [4]byte
	binary.LittleEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(encoded))
	encoded = append(encoded, checksum[:]...)
	return encoded, nil
}

// UnmarshalAggregateStateEnvelope decodes and validates one complete HAG1
// envelope. It rejects truncation, trailing bytes, unsupported wire versions,
// invalid kinds, oversized payloads, and checksum failures.
func UnmarshalAggregateStateEnvelope(data []byte) (AggregateStateEnvelope, error) {
	if len(data) > MaxAggregateStateEnvelopeBytes || len(data) < len(aggregateStateEnvelopeMagic)+1 {
		return AggregateStateEnvelope{}, fmt.Errorf("%w: payload length %d is outside bounds", ErrAggregateStateEnvelopeWire, len(data))
	}
	if !bytes.Equal(data[:len(aggregateStateEnvelopeMagic)], aggregateStateEnvelopeMagic[:]) {
		return AggregateStateEnvelope{}, fmt.Errorf("%w: magic is not HAG1", ErrAggregateStateEnvelopeWire)
	}
	if data[len(aggregateStateEnvelopeMagic)] != AggregateStateEnvelopeWireVersion {
		return AggregateStateEnvelope{}, fmt.Errorf("%w: wire version %d is unsupported", ErrAggregateStateEnvelopeWire, data[len(aggregateStateEnvelopeMagic)])
	}

	offset := len(aggregateStateEnvelopeMagic) + 1
	kindLength, next, err := readAggregateStateUvarint(data, offset)
	if err != nil {
		return AggregateStateEnvelope{}, err
	}
	if kindLength == 0 || kindLength > MaxAggregateStateKindBytes || kindLength > uint64(len(data)-next) {
		return AggregateStateEnvelope{}, fmt.Errorf("%w: kind length %d is invalid", ErrAggregateStateEnvelopeWire, kindLength)
	}
	offset = next
	kind := string(data[offset : offset+int(kindLength)])
	offset += int(kindLength)

	version, next, err := readAggregateStateUvarint(data, offset)
	if err != nil {
		return AggregateStateEnvelope{}, err
	}
	offset = next
	payloadLength, next, err := readAggregateStateUvarint(data, offset)
	if err != nil {
		return AggregateStateEnvelope{}, err
	}
	offset = next
	if payloadLength > MaxAggregateStatePayloadBytes || offset > len(data) || len(data)-offset < 4 || payloadLength != uint64(len(data)-offset-4) {
		return AggregateStateEnvelope{}, fmt.Errorf("%w: payload length %d does not match wire size", ErrAggregateStateEnvelopeWire, payloadLength)
	}
	payloadEnd := offset + int(payloadLength)
	if crc32.ChecksumIEEE(data[:payloadEnd]) != binary.LittleEndian.Uint32(data[payloadEnd:]) {
		return AggregateStateEnvelope{}, fmt.Errorf("%w: checksum mismatch", ErrAggregateStateEnvelopeWire)
	}

	envelope := AggregateStateEnvelope{Kind: kind, Version: version, Payload: append([]byte(nil), data[offset:payloadEnd]...)}
	if err := validateAggregateStateEnvelope(envelope); err != nil {
		return AggregateStateEnvelope{}, fmt.Errorf("%w: %v", ErrAggregateStateEnvelopeWire, err)
	}
	return envelope, nil
}

func validateAggregateStateEnvelope(envelope AggregateStateEnvelope) error {
	if err := validateAggregateStateMetadata(envelope.Kind, envelope.Version); err != nil {
		return err
	}
	if len(envelope.Payload) > MaxAggregateStatePayloadBytes {
		return ErrAggregateStateEnvelopeLimit
	}
	return nil
}

func validateAggregateStateMetadata(kind string, version uint64) error {
	if len(kind) == 0 || len(kind) > MaxAggregateStateKindBytes {
		return fmt.Errorf("%w: kind length %d is invalid", ErrAggregateStateEnvelopeInvalid, len(kind))
	}
	for index := 0; index < len(kind); index++ {
		value := kind[index]
		if value < 'a' || value > 'z' {
			if value < '0' || value > '9' {
				if value != '.' && value != '_' && value != '-' {
					return fmt.Errorf("%w: kind contains unsupported byte %q", ErrAggregateStateEnvelopeInvalid, value)
				}
			}
		}
	}
	if version == 0 {
		return fmt.Errorf("%w: version must be positive", ErrAggregateStateEnvelopeInvalid)
	}
	return nil
}

func aggregateStateUvarintLen(value uint64) int {
	var encoded [binary.MaxVarintLen64]byte
	return binary.PutUvarint(encoded[:], value)
}

func appendAggregateStateUvarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	count := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:count]...)
}

func readAggregateStateUvarint(data []byte, offset int) (uint64, int, error) {
	if offset < 0 || offset >= len(data) {
		return 0, offset, fmt.Errorf("%w: truncated varint", ErrAggregateStateEnvelopeWire)
	}
	value, count := binary.Uvarint(data[offset:])
	if count == 0 {
		return 0, offset, fmt.Errorf("%w: truncated varint", ErrAggregateStateEnvelopeWire)
	}
	if count < 0 {
		return 0, offset, fmt.Errorf("%w: varint overflow", ErrAggregateStateEnvelopeWire)
	}
	return value, offset + count, nil
}
