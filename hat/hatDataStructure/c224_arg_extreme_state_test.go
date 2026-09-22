package hatDataStructure

import (
	"bytes"
	"testing"
)

func TestC224ArgMaxInt64StateUsesDeterministicTieBreak(t *testing.T) {
	var state ArgMaxInt64State
	state.Observe(10, 5)
	state.Observe(2, 5)
	state.Observe(9, 4)
	state.Observe(8, 7)

	argument, value, ok := state.Result()
	if !ok || argument != 8 || value != 7 {
		t.Fatalf("Result() = (%d, %d, %t), want (8, 7, true)", argument, value, ok)
	}

	var left, right ArgMaxInt64State
	left.Observe(10, 5)
	right.Observe(2, 5)
	left.Merge(right)
	argument, value, ok = left.Result()
	if !ok || argument != 2 || value != 5 {
		t.Fatalf("Merge() = (%d, %d, %t), want (2, 5, true)", argument, value, ok)
	}
}

func TestC224ArgMinInt64StateUsesDeterministicTieBreak(t *testing.T) {
	var state ArgMinInt64State
	state.Observe(10, 5)
	state.Observe(2, 5)
	state.Observe(9, 4)
	state.Observe(8, 7)

	argument, value, ok := state.Result()
	if !ok || argument != 9 || value != 4 {
		t.Fatalf("Result() = (%d, %d, %t), want (9, 4, true)", argument, value, ok)
	}

	var left, right ArgMinInt64State
	left.Observe(10, 5)
	right.Observe(2, 5)
	left.Merge(right)
	argument, value, ok = left.Result()
	if !ok || argument != 2 || value != 5 {
		t.Fatalf("Merge() = (%d, %d, %t), want (2, 5, true)", argument, value, ok)
	}
}

func TestC224ArgExtremeInt64StateRoundTripAndValidation(t *testing.T) {
	var source ArgMaxInt64State
	source.Observe(42, 1700000000)
	wire, err := source.MarshalAggregateState()
	if err != nil {
		t.Fatalf("MarshalAggregateState() error = %v", err)
	}
	envelope, err := UnmarshalAggregateStateEnvelope(wire)
	if err != nil {
		t.Fatalf("UnmarshalAggregateStateEnvelope() error = %v", err)
	}
	if envelope.Kind != AggregateStateKindArgMaxInt64 || envelope.Version != 1 || len(envelope.Payload) != argExtremeInt64StatePayloadSize {
		t.Fatalf("envelope = kind %q version %d payload %d, want argmax_int64 v1 payload %d", envelope.Kind, envelope.Version, len(envelope.Payload), argExtremeInt64StatePayloadSize)
	}
	decoded, err := NewArgMaxInt64StateFromAggregateState(wire)
	if err != nil {
		t.Fatalf("NewArgMaxInt64StateFromAggregateState() error = %v", err)
	}
	argument, value, ok := decoded.Result()
	if !ok || argument != 42 || value != 1700000000 {
		t.Fatalf("decoded Result() = (%d, %d, %t), want (42, 1700000000, true)", argument, value, ok)
	}

	corrupted := append([]byte(nil), wire...)
	corrupted[len(corrupted)-1] ^= 1
	var receiver ArgMaxInt64State
	receiver.Observe(7, 9)
	if err := receiver.MergeAggregateState(corrupted); err == nil {
		t.Fatal("MergeAggregateState() accepted a corrupted wire")
	}
	argument, value, ok = receiver.Result()
	if !ok || argument != 7 || value != 9 {
		t.Fatalf("receiver after rejected merge = (%d, %d, %t), want (7, 9, true)", argument, value, ok)
	}

	var minimum ArgMinInt64State
	minimum.Observe(42, 1700000000)
	minimumWire, err := minimum.MarshalAggregateState()
	if err != nil {
		t.Fatalf("ArgMin MarshalAggregateState() error = %v", err)
	}
	if _, err := NewArgMaxInt64StateFromAggregateState(minimumWire); err == nil {
		t.Fatal("NewArgMaxInt64StateFromAggregateState() accepted an argmin state")
	}
	if bytes.Equal(wire, minimumWire) {
		t.Fatal("argmax and argmin wire states unexpectedly match")
	}
}

func TestC224ArgExtremeInt64StateEmpty(t *testing.T) {
	var maximum ArgMaxInt64State
	if argument, value, ok := maximum.Result(); ok || argument != 0 || value != 0 {
		t.Fatalf("empty argmax Result() = (%d, %d, %t), want (0, 0, false)", argument, value, ok)
	}
	var minimum ArgMinInt64State
	if argument, value, ok := minimum.Result(); ok || argument != 0 || value != 0 {
		t.Fatalf("empty argmin Result() = (%d, %d, %t), want (0, 0, false)", argument, value, ok)
	}
}

var c224ArgMaxInt64BenchmarkState = func() ArgMaxInt64State {
	var state ArgMaxInt64State
	state.Observe(42, 1700000000)
	return state
}()

var c224ArgMaxInt64BenchmarkWire, _ = c224ArgMaxInt64BenchmarkState.MarshalAggregateState()

func BenchmarkC224ArgMaxHAG1Marshal(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(c224ArgMaxInt64BenchmarkWire)))
	for i := 0; i < b.N; i++ {
		if _, err := c224ArgMaxInt64BenchmarkState.MarshalAggregateState(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkC224ArgMaxHAG1Unmarshal(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(c224ArgMaxInt64BenchmarkWire)))
	for i := 0; i < b.N; i++ {
		if _, err := NewArgMaxInt64StateFromAggregateState(c224ArgMaxInt64BenchmarkWire); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkC224ArgMaxHAG1Merge(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(c224ArgMaxInt64BenchmarkWire)))
	var state ArgMaxInt64State
	for i := 0; i < b.N; i++ {
		if err := state.MergeAggregateState(c224ArgMaxInt64BenchmarkWire); err != nil {
			b.Fatal(err)
		}
	}
}
