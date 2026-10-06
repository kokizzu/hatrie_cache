package hatDataStructure

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
)

func TestCHU03AggregateStateRegistryRoundTripsAndMerges(t *testing.T) {
	registry := NewAggregateStateRegistry()
	if err := registry.Register(chu03ByteStateCodec("demo.sum", 1)); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	original := []byte("left")
	wire, err := registry.Marshal("demo.sum", 1, original)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	original[0] = 'X'

	decoded, err := registry.Unmarshal(wire)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if decoded.Kind != "demo.sum" || decoded.Version != 1 {
		t.Fatalf("decoded metadata = %#v", decoded)
	}
	if got := decoded.Value.([]byte); !bytes.Equal(got, []byte("left")) {
		t.Fatalf("decoded value = %q, want left", got)
	}
	corrupted := append([]byte(nil), wire...)
	corrupted[len(corrupted)-1] ^= 0xff
	if got := decoded.Value.([]byte); !bytes.Equal(got, []byte("left")) {
		t.Fatalf("decoded value aliases wire = %q", got)
	}

	right, err := registry.Marshal("demo.sum", 1, []byte("right"))
	if err != nil {
		t.Fatalf("right Marshal() error = %v", err)
	}
	merged, err := registry.Merge(wire, right)
	if err != nil {
		t.Fatalf("Merge() error = %v", err)
	}
	mergedValue, err := registry.Unmarshal(merged)
	if err != nil {
		t.Fatalf("merged Unmarshal() error = %v", err)
	}
	if got := mergedValue.Value.([]byte); !bytes.Equal(got, []byte("leftright")) {
		t.Fatalf("merged value = %q, want leftright", got)
	}
}

func TestCHU03AggregateStateRegistryRejectsInvalidRegistrationAndUnknownStates(t *testing.T) {
	registry, err := NewAggregateStateRegistryWithLimit(1)
	if err != nil {
		t.Fatalf("NewAggregateStateRegistryWithLimit() error = %v", err)
	}
	if err := registry.Register(AggregateStateCodec{Kind: "demo.sum", Version: 1}); !errors.Is(err, ErrAggregateStateRegistryInvalid) {
		t.Fatalf("nil callbacks error = %v, want invalid", err)
	}
	codec := chu03ByteStateCodec("demo.sum", 1)
	if err := registry.Register(codec); err != nil {
		t.Fatalf("first Register() error = %v", err)
	}
	if err := registry.Register(codec); !errors.Is(err, ErrAggregateStateRegistryDuplicate) {
		t.Fatalf("duplicate Register() error = %v, want duplicate", err)
	}
	if err := registry.Register(chu03ByteStateCodec("demo.other", 1)); !errors.Is(err, ErrAggregateStateRegistryFull) {
		t.Fatalf("full Register() error = %v, want full", err)
	}
	if registry.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", registry.Len())
	}

	if _, err := registry.Marshal("missing", 1, []byte("x")); !errors.Is(err, ErrAggregateStateRegistryUnknown) {
		t.Fatalf("unknown Marshal() error = %v, want unknown", err)
	}
	wire, err := MarshalAggregateStateEnvelope("missing", 1, []byte("x"))
	if err != nil {
		t.Fatalf("MarshalAggregateStateEnvelope() error = %v", err)
	}
	if _, err := registry.Unmarshal(wire); !errors.Is(err, ErrAggregateStateRegistryUnknown) {
		t.Fatalf("unknown Unmarshal() error = %v, want unknown", err)
	}
	if _, err := NewAggregateStateRegistryWithLimit(0); !errors.Is(err, ErrAggregateStateRegistryInvalid) {
		t.Fatalf("zero registry limit error = %v, want invalid", err)
	}
}

func TestCHU03AggregateStateRegistryFencesVersionAndMergeKind(t *testing.T) {
	registry := NewAggregateStateRegistry()
	if err := registry.Register(chu03ByteStateCodec("demo.left", 1)); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(chu03ByteStateCodec("demo.right", 1)); err != nil {
		t.Fatal(err)
	}
	left, err := registry.Marshal("demo.left", 1, []byte("left"))
	if err != nil {
		t.Fatal(err)
	}
	right, err := registry.Marshal("demo.right", 1, []byte("right"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Merge(left, right); !errors.Is(err, ErrAggregateStateKindMismatch) {
		t.Fatalf("cross-kind Merge() error = %v, want kind mismatch", err)
	}

	versioned, err := MarshalAggregateStateEnvelope("demo.left", 2, []byte("future"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Unmarshal(versioned); !errors.Is(err, ErrAggregateStateRegistryUnknown) {
		t.Fatalf("unknown version Unmarshal() error = %v, want unknown", err)
	}
}

func chu03ByteStateCodec(kind string, version uint64) AggregateStateCodec {
	return AggregateStateCodec{
		Kind:    kind,
		Version: version,
		Encode: func(value any) ([]byte, error) {
			bytesValue, ok := value.([]byte)
			if !ok {
				return nil, fmt.Errorf("want []byte, got %T", value)
			}
			return append([]byte(nil), bytesValue...), nil
		},
		Decode: func(payload []byte) (any, error) {
			return append([]byte(nil), payload...), nil
		},
		Merge: func(left, right any) (any, error) {
			leftBytes, leftOK := left.([]byte)
			rightBytes, rightOK := right.([]byte)
			if !leftOK || !rightOK {
				return nil, fmt.Errorf("want []byte values, got %T and %T", left, right)
			}
			merged := make([]byte, 0, len(leftBytes)+len(rightBytes))
			merged = append(merged, leftBytes...)
			return append(merged, rightBytes...), nil
		},
	}
}
