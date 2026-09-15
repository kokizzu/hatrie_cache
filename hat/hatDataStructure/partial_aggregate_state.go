package hatDataStructure

import (
	"fmt"
)

const hyperLogLogAggregateStateVersion uint64 = 1

// MarshalAggregateState encodes the HyperLogLog state as a versioned partial
// aggregate. The payload uses raw register bytes instead of JSON/base64.
func (hll HyperLogLog) MarshalAggregateState() ([]byte, error) {
	if !hll.validMergeState() {
		return nil, ErrHyperLogLogStateInvalid
	}
	raw := hll.registers
	if hll.observations == 0 && !hyperLogLogRawHasRegisters(raw) {
		raw = nil
	}
	payloadLength := 1 + aggregateStateUvarintLen(hll.observations) + len(raw)
	return MarshalAggregateStateEnvelopeWithPayload(AggregateStateKindHyperLogLog, hyperLogLogAggregateStateVersion, payloadLength, func(payload []byte) []byte {
		payload = append(payload, hll.precision)
		payload = appendAggregateStateUvarint(payload, hll.observations)
		return append(payload, raw...)
	})
}

// NewHyperLogLogFromAggregateState reconstructs a HyperLogLog from a checked
// versioned partial aggregate payload.
func NewHyperLogLogFromAggregateState(data []byte) (HyperLogLog, error) {
	envelope, err := UnmarshalAggregateStateEnvelope(data)
	if err != nil {
		return HyperLogLog{}, err
	}
	if envelope.Kind != AggregateStateKindHyperLogLog {
		return HyperLogLog{}, fmt.Errorf("%w: got %q want %q", ErrAggregateStateKindMismatch, envelope.Kind, AggregateStateKindHyperLogLog)
	}
	if envelope.Version != hyperLogLogAggregateStateVersion {
		return HyperLogLog{}, fmt.Errorf("%w: got %d want %d", ErrAggregateStateVersionUnsupported, envelope.Version, hyperLogLogAggregateStateVersion)
	}
	return unmarshalHyperLogLogAggregatePayload(envelope.Payload)
}

func unmarshalHyperLogLogAggregatePayload(payload []byte) (HyperLogLog, error) {
	if len(payload) == 0 {
		return HyperLogLog{}, fmt.Errorf("%w: missing precision", ErrHyperLogLogStateInvalid)
	}
	precision := payload[0]
	offset := 1
	observations, next, err := readAggregateStateUvarint(payload, offset)
	if err != nil {
		return HyperLogLog{}, fmt.Errorf("%w: %v", ErrHyperLogLogStateInvalid, err)
	}
	offset = next
	raw := payload[offset:]
	if precision == 0 {
		if observations != 0 || len(raw) != 0 {
			return HyperLogLog{}, ErrHyperLogLogStateInvalid
		}
		return HyperLogLog{}, nil
	}
	if err := ValidateHyperLogLogPrecision(precision); err != nil {
		return HyperLogLog{}, err
	}
	expected := hyperLogLogRegisterCount(precision)
	if len(raw) != 0 && len(raw) != expected {
		return HyperLogLog{}, fmt.Errorf("%w: register length=%d want=%d", ErrHyperLogLogStateInvalid, len(raw), expected)
	}
	if len(raw) == 0 {
		if observations != 0 {
			return HyperLogLog{}, ErrHyperLogLogStateInvalid
		}
		return HyperLogLog{precision: precision}, nil
	}
	maxRank := hyperLogLogMaxRank(precision)
	nonZero := uint64(0)
	for _, register := range raw {
		if register > maxRank {
			return HyperLogLog{}, ErrHyperLogLogStateInvalid
		}
		if register != 0 {
			nonZero++
		}
	}
	if nonZero > observations || observations > 0 && nonZero == 0 {
		return HyperLogLog{}, ErrHyperLogLogStateInvalid
	}
	if nonZero == 0 {
		return HyperLogLog{precision: precision}, nil
	}
	out := HyperLogLog{precision: precision, observations: observations, registers: append([]uint8(nil), raw...)}
	out.rebuildSummary()
	return out, nil
}
