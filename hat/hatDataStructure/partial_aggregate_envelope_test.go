package hatDataStructure_test

import (
	"bytes"
	"testing"

	hatDataStructure "hatrie_cache/hat/hatDataStructure"
)

func TestAggregateStateEnvelopeRoundTripAndCorruptionCheck(t *testing.T) {
	payload := []byte{0, 1, 2, 3, 4}
	envelope, err := hatDataStructure.NewAggregateStateEnvelope("future.aggregate", 7, payload)
	if err != nil {
		t.Fatalf("NewAggregateStateEnvelope() error = %v", err)
	}
	payload[0] = 99

	wire, err := envelope.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	decoded, err := hatDataStructure.UnmarshalAggregateStateEnvelope(wire)
	if err != nil {
		t.Fatalf("UnmarshalAggregateStateEnvelope() error = %v", err)
	}
	if decoded.Kind != "future.aggregate" || decoded.Version != 7 || !bytes.Equal(decoded.Payload, []byte{0, 1, 2, 3, 4}) {
		t.Fatalf("decoded envelope = %#v, want original kind/version/payload", decoded)
	}

	corrupted := append([]byte(nil), wire...)
	corrupted[len(corrupted)-1] ^= 1
	if _, err := hatDataStructure.UnmarshalAggregateStateEnvelope(corrupted); err == nil {
		t.Fatal("UnmarshalAggregateStateEnvelope() accepted a corrupted checksum")
	}

	trailing := append(append([]byte(nil), wire...), 0)
	if _, err := hatDataStructure.UnmarshalAggregateStateEnvelope(trailing); err == nil {
		t.Fatal("UnmarshalAggregateStateEnvelope() accepted trailing bytes")
	}

	for _, invalid := range []hatDataStructure.AggregateStateEnvelope{
		{Kind: "", Version: 1},
		{Kind: "Future.aggregate", Version: 1},
		{Kind: "future.aggregate", Version: 0},
	} {
		if _, err := invalid.MarshalBinary(); err == nil {
			t.Fatalf("MarshalBinary(%#v) accepted invalid metadata", invalid)
		}
	}
}

func TestHyperLogLogAggregateStateRoundTrip(t *testing.T) {
	want, err := hatDataStructure.NewHyperLogLog(10)
	if err != nil {
		t.Fatalf("NewHyperLogLog() error = %v", err)
	}
	want.AddBytes([]byte("alpha"), []byte("beta"), []byte("gamma"), []byte("alpha"))

	wire, err := want.MarshalAggregateState()
	if err != nil {
		t.Fatalf("MarshalAggregateState() error = %v", err)
	}
	envelope, err := hatDataStructure.UnmarshalAggregateStateEnvelope(wire)
	if err != nil {
		t.Fatalf("UnmarshalAggregateStateEnvelope() error = %v", err)
	}
	if envelope.Kind != hatDataStructure.AggregateStateKindHyperLogLog || envelope.Version != 1 {
		t.Fatalf("envelope metadata = %#v, want HyperLogLog v1", envelope)
	}

	got, err := hatDataStructure.NewHyperLogLogFromAggregateState(wire)
	if err != nil {
		t.Fatalf("NewHyperLogLogFromAggregateState() error = %v", err)
	}
	if got.Info() != want.Info() || !bytes.Equal(got.RawRegisters(), want.RawRegisters()) {
		t.Fatalf("round trip info/registers = %#v/%v, want %#v/%v", got.Info(), got.RawRegisters(), want.Info(), want.RawRegisters())
	}

	wrongVersion, err := hatDataStructure.NewAggregateStateEnvelope(hatDataStructure.AggregateStateKindHyperLogLog, 2, envelope.Payload)
	if err != nil {
		t.Fatalf("NewAggregateStateEnvelope(wrong version) error = %v", err)
	}
	wrongWire, err := wrongVersion.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary(wrong version) error = %v", err)
	}
	if _, err := hatDataStructure.NewHyperLogLogFromAggregateState(wrongWire); err == nil {
		t.Fatal("NewHyperLogLogFromAggregateState() accepted an unsupported state version")
	}
}
