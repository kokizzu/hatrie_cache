package hatDataStructure

import (
	"encoding/binary"
	"fmt"
)

const (
	argExtremeInt64StateVersion     uint64 = 1
	argExtremeInt64StatePayloadSize        = 24
	argExtremeInt64StateSeen        byte   = 1
)

type argExtremeInt64State struct {
	argument int64
	value    int64
	seen     bool
}

func (state *argExtremeInt64State) observe(argument, value int64, maximize bool) {
	if !state.seen || argExtremeInt64CandidateWins(argument, value, state.argument, state.value, maximize) {
		state.argument = argument
		state.value = value
		state.seen = true
	}
}

func (state *argExtremeInt64State) merge(other argExtremeInt64State, maximize bool) {
	if other.seen {
		state.observe(other.argument, other.value, maximize)
	}
}

func (state argExtremeInt64State) result() (argument, value int64, ok bool) {
	return state.argument, state.value, state.seen
}

func argExtremeInt64CandidateWins(candidateArgument, candidateValue, currentArgument, currentValue int64, maximize bool) bool {
	if candidateValue != currentValue {
		if maximize {
			return candidateValue > currentValue
		}
		return candidateValue < currentValue
	}
	return candidateArgument < currentArgument
}

func marshalArgExtremeInt64State(kind string, state argExtremeInt64State) ([]byte, error) {
	return MarshalAggregateStateEnvelopeWithPayload(
		kind,
		argExtremeInt64StateVersion,
		argExtremeInt64StatePayloadSize,
		func(encoded []byte) []byte {
			var payload [argExtremeInt64StatePayloadSize]byte
			if state.seen {
				payload[0] = argExtremeInt64StateSeen
			}
			binary.LittleEndian.PutUint64(payload[8:16], uint64(state.argument))
			binary.LittleEndian.PutUint64(payload[16:24], uint64(state.value))
			return append(encoded, payload[:]...)
		},
	)
}

func decodeArgExtremeInt64State(data []byte, expectedKind string) (argExtremeInt64State, error) {
	envelope, err := UnmarshalAggregateStateEnvelope(data)
	if err != nil {
		return argExtremeInt64State{}, err
	}
	if envelope.Kind != expectedKind {
		return argExtremeInt64State{}, fmt.Errorf("%w: got %q want %q", ErrAggregateStateKindMismatch, envelope.Kind, expectedKind)
	}
	if envelope.Version != argExtremeInt64StateVersion {
		return argExtremeInt64State{}, fmt.Errorf("%w: got %d want %d", ErrAggregateStateVersionUnsupported, envelope.Version, argExtremeInt64StateVersion)
	}
	payload := envelope.Payload
	if len(payload) != argExtremeInt64StatePayloadSize {
		return argExtremeInt64State{}, fmt.Errorf("%w: arg-extreme payload length %d, want %d", ErrAggregateStateEnvelopeWire, len(payload), argExtremeInt64StatePayloadSize)
	}
	if payload[0] != 0 && payload[0] != argExtremeInt64StateSeen {
		return argExtremeInt64State{}, fmt.Errorf("%w: arg-extreme seen flag %d is invalid", ErrAggregateStateEnvelopeWire, payload[0])
	}
	for _, reserved := range payload[1:8] {
		if reserved != 0 {
			return argExtremeInt64State{}, fmt.Errorf("%w: arg-extreme reserved payload byte is non-zero", ErrAggregateStateEnvelopeWire)
		}
	}
	state := argExtremeInt64State{
		argument: int64(binary.LittleEndian.Uint64(payload[8:16])),
		value:    int64(binary.LittleEndian.Uint64(payload[16:24])),
		seen:     payload[0] == argExtremeInt64StateSeen,
	}
	if !state.seen && (state.argument != 0 || state.value != 0) {
		return argExtremeInt64State{}, fmt.Errorf("%w: empty arg-extreme state contains a candidate", ErrAggregateStateEnvelopeWire)
	}
	return state, nil
}

// ArgMaxInt64State retains the argument associated with the greatest int64
// value. Equal values use the lower argument, making updates and merges
// deterministic regardless of input partitioning. The zero value is empty.
type ArgMaxInt64State struct {
	state argExtremeInt64State
}

// Observe considers one argument/value pair.
func (state *ArgMaxInt64State) Observe(argument, value int64) {
	state.state.observe(argument, value, true)
}

// Merge combines another state without changing the result of a larger value.
func (state *ArgMaxInt64State) Merge(other ArgMaxInt64State) {
	state.state.merge(other.state, true)
}

// Result returns the selected argument, its value, and whether the state is
// non-empty.
func (state ArgMaxInt64State) Result() (argument, value int64, ok bool) {
	return state.state.result()
}

// MarshalAggregateState encodes the state as a bounded, checksummed HAG1
// envelope with a fixed-width payload.
func (state ArgMaxInt64State) MarshalAggregateState() ([]byte, error) {
	return marshalArgExtremeInt64State(AggregateStateKindArgMaxInt64, state.state)
}

// NewArgMaxInt64StateFromAggregateState decodes one HAG1 arg-max state.
func NewArgMaxInt64StateFromAggregateState(data []byte) (ArgMaxInt64State, error) {
	state, err := decodeArgExtremeInt64State(data, AggregateStateKindArgMaxInt64)
	if err != nil {
		return ArgMaxInt64State{}, err
	}
	return ArgMaxInt64State{state: state}, nil
}

// MergeAggregateState decodes and merges one arg-max state atomically.
func (state *ArgMaxInt64State) MergeAggregateState(data []byte) error {
	other, err := NewArgMaxInt64StateFromAggregateState(data)
	if err != nil {
		return err
	}
	state.Merge(other)
	return nil
}

// ArgMinInt64State retains the argument associated with the smallest int64
// value. Equal values use the lower argument, making updates and merges
// deterministic regardless of input partitioning. The zero value is empty.
type ArgMinInt64State struct {
	state argExtremeInt64State
}

// Observe considers one argument/value pair.
func (state *ArgMinInt64State) Observe(argument, value int64) {
	state.state.observe(argument, value, false)
}

// Merge combines another state without changing the result of a smaller value.
func (state *ArgMinInt64State) Merge(other ArgMinInt64State) {
	state.state.merge(other.state, false)
}

// Result returns the selected argument, its value, and whether the state is
// non-empty.
func (state ArgMinInt64State) Result() (argument, value int64, ok bool) {
	return state.state.result()
}

// MarshalAggregateState encodes the state as a bounded, checksummed HAG1
// envelope with a fixed-width payload.
func (state ArgMinInt64State) MarshalAggregateState() ([]byte, error) {
	return marshalArgExtremeInt64State(AggregateStateKindArgMinInt64, state.state)
}

// NewArgMinInt64StateFromAggregateState decodes one HAG1 arg-min state.
func NewArgMinInt64StateFromAggregateState(data []byte) (ArgMinInt64State, error) {
	state, err := decodeArgExtremeInt64State(data, AggregateStateKindArgMinInt64)
	if err != nil {
		return ArgMinInt64State{}, err
	}
	return ArgMinInt64State{state: state}, nil
}

// MergeAggregateState decodes and merges one arg-min state atomically.
func (state *ArgMinInt64State) MergeAggregateState(data []byte) error {
	other, err := NewArgMinInt64StateFromAggregateState(data)
	if err != nil {
		return err
	}
	state.Merge(other)
	return nil
}
