package hatCache

import (
	"encoding/binary"
	"fmt"

	hatDataStructure "hatrie_cache/hat/hatDataStructure"
)

const countMinSketchAggregateStateVersion uint64 = 1

// MarshalAggregateState encodes the Count-Min Sketch as a versioned partial
// aggregate. Counters are transferred as raw little-endian uint32 values.
func (sketch CountMinSketch) MarshalAggregateState() ([]byte, error) {
	if err := validateCountMinSketchState(sketch); err != nil {
		return nil, err
	}
	payloadLength := aggregateStateUvarintLenForCountMin(sketch.width) + 1 + aggregateStateUvarintLenForCountMin(sketch.total) + len(sketch.counters)*4
	return hatDataStructure.MarshalAggregateStateEnvelopeWithPayload(hatDataStructure.AggregateStateKindCountMinSketch, countMinSketchAggregateStateVersion, payloadLength, func(payload []byte) []byte {
		payload = appendAggregateStateUvarintForCountMin(payload, sketch.width)
		payload = append(payload, sketch.depth)
		payload = appendAggregateStateUvarintForCountMin(payload, sketch.total)
		for _, counter := range sketch.counters {
			payload = append(payload, 0, 0, 0, 0)
			binary.LittleEndian.PutUint32(payload[len(payload)-4:], counter)
		}
		return payload
	})
}

// NewCountMinSketchFromAggregateState reconstructs a Count-Min Sketch from a
// checked versioned partial aggregate payload.
func NewCountMinSketchFromAggregateState(data []byte) (CountMinSketch, error) {
	envelope, err := hatDataStructure.UnmarshalAggregateStateEnvelope(data)
	if err != nil {
		return CountMinSketch{}, err
	}
	if envelope.Kind != hatDataStructure.AggregateStateKindCountMinSketch {
		return CountMinSketch{}, fmt.Errorf("%w: got %q want %q", hatDataStructure.ErrAggregateStateKindMismatch, envelope.Kind, hatDataStructure.AggregateStateKindCountMinSketch)
	}
	if envelope.Version != countMinSketchAggregateStateVersion {
		return CountMinSketch{}, fmt.Errorf("%w: got %d want %d", hatDataStructure.ErrAggregateStateVersionUnsupported, envelope.Version, countMinSketchAggregateStateVersion)
	}
	return unmarshalCountMinSketchAggregatePayload(envelope.Payload)
}

func unmarshalCountMinSketchAggregatePayload(payload []byte) (CountMinSketch, error) {
	width, offset, err := readCountMinAggregateStateUvarint(payload, 0)
	if err != nil {
		return CountMinSketch{}, err
	}
	if offset >= len(payload) {
		return CountMinSketch{}, fmt.Errorf("%w: missing depth", ErrCountMinSketchStateInvalid)
	}
	depth := payload[offset]
	offset++
	total, next, err := readCountMinAggregateStateUvarint(payload, offset)
	if err != nil {
		return CountMinSketch{}, err
	}
	offset = next
	raw := payload[offset:]
	if width == 0 && depth == 0 && total == 0 && len(raw) == 0 {
		return CountMinSketch{}, nil
	}
	if err := validateCountMinSketchShape(width, depth); err != nil {
		return CountMinSketch{}, err
	}
	expected := int(width * uint64(depth) * 4)
	if len(raw) != 0 && len(raw) != expected {
		return CountMinSketch{}, fmt.Errorf("%w: counter byte length=%d want=%d", ErrCountMinSketchStateInvalid, len(raw), expected)
	}
	out := CountMinSketch{width: width, depth: depth, total: total}
	if len(raw) == 0 {
		if total != 0 {
			return CountMinSketch{}, ErrCountMinSketchStateInvalid
		}
		return out, nil
	}
	out.counters = make([]uint32, expected/4)
	for index := range out.counters {
		out.counters[index] = binary.LittleEndian.Uint32(raw[index*4 : index*4+4])
	}
	if !countMinSketchRawHasCounters(raw) && total == 0 {
		out.counters = nil
		return out, nil
	}
	if err := validateCountMinSketchState(out); err != nil {
		return CountMinSketch{}, err
	}
	return out, nil
}

func appendAggregateStateUvarintForCountMin(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	count := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:count]...)
}

func aggregateStateUvarintLenForCountMin(value uint64) int {
	var encoded [binary.MaxVarintLen64]byte
	return binary.PutUvarint(encoded[:], value)
}

func readCountMinAggregateStateUvarint(data []byte, offset int) (uint64, int, error) {
	if offset < 0 || offset >= len(data) {
		return 0, offset, fmt.Errorf("%w: truncated varint", ErrCountMinSketchStateInvalid)
	}
	value, count := binary.Uvarint(data[offset:])
	if count == 0 {
		return 0, offset, fmt.Errorf("%w: truncated varint", ErrCountMinSketchStateInvalid)
	}
	if count < 0 {
		return 0, offset, fmt.Errorf("%w: varint overflow", ErrCountMinSketchStateInvalid)
	}
	return value, offset + count, nil
}
