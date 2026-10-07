package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"sync"
	"testing"
)

type chu03CounterState struct {
	Count uint64
}

func TestCHU03AggregateStateRegistryRoundTrip(t *testing.T) {
	registry := NewAggregateStateRegistry()
	if registry == nil {
		t.Fatal("NewAggregateStateRegistry() returned nil")
	}
	if err := registry.Register(chu03CounterCodec()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	encoded, err := registry.Marshal("counter", 1, chu03CounterState{Count: 42})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	envelope, value, err := registry.Unmarshal(encoded)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if envelope.Kind != "counter" || envelope.Version != 1 {
		t.Fatalf("envelope = %#v, want counter v1", envelope)
	}
	state, ok := value.(chu03CounterState)
	if !ok || state.Count != 42 {
		t.Fatalf("decoded value = %#v, want counter 42", value)
	}
}

func TestCHU03DefaultAggregateStateRegistryIncludesHyperLogLog(t *testing.T) {
	registry, err := NewDefaultAggregateStateRegistry()
	if err != nil {
		t.Fatalf("NewDefaultAggregateStateRegistry() error = %v", err)
	}
	if registry.CodecCount() < 2 {
		t.Fatalf("CodecCount() = %d, want built-in codecs", registry.CodecCount())
	}
	hll, err := NewHyperLogLog(10)
	if err != nil {
		t.Fatalf("NewHyperLogLog() error = %v", err)
	}
	encoded, err := registry.Marshal(AggregateStateKindHyperLogLog, 1, hll)
	if err != nil {
		t.Fatalf("Marshal(HLL) error = %v", err)
	}
	_, value, err := registry.Unmarshal(encoded)
	if err != nil {
		t.Fatalf("Unmarshal(HLL) error = %v", err)
	}
	if _, ok := value.(HyperLogLog); !ok {
		t.Fatalf("decoded HLL value = %T, want HyperLogLog", value)
	}
	digest, err := NewTDigestFromSnapshot(TDigestSnapshot{
		Compression: 100,
		Count:       1,
		Centroids:   []TDigestCentroid{{Mean: 42, Count: 1}},
	})
	if err != nil {
		t.Fatalf("NewTDigestFromSnapshot() error = %v", err)
	}
	encoded, err = registry.Marshal(AggregateStateKindTDigest, 1, digest)
	if err != nil {
		t.Fatalf("Marshal(t-digest) error = %v", err)
	}
	_, value, err = registry.Unmarshal(encoded)
	if err != nil {
		t.Fatalf("Unmarshal(t-digest) error = %v", err)
	}
	if _, ok := value.(TDigest); !ok {
		t.Fatalf("decoded t-digest value = %T, want TDigest", value)
	}
}

func TestCHU03AggregateStateRegistryRejectsInvalidAndUnknownCodecs(t *testing.T) {
	registry := NewAggregateStateRegistry()
	codec := chu03CounterCodec()
	if err := registry.Register(codec); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(codec); !errors.Is(err, ErrAggregateStateRegistryCodecExists) {
		t.Fatalf("duplicate Register() error = %v, want ErrAggregateStateRegistryCodecExists", err)
	}
	if _, err := registry.Marshal("missing", 1, chu03CounterState{}); !errors.Is(err, ErrAggregateStateRegistryCodecMissing) {
		t.Fatalf("unknown Marshal() error = %v, want ErrAggregateStateRegistryCodecMissing", err)
	}
	if _, _, err := registry.Unmarshal([]byte("bad")); !errors.Is(err, ErrAggregateStateEnvelopeWire) {
		t.Fatalf("malformed Unmarshal() error = %v, want envelope wire error", err)
	}
	if err := registry.Register(AggregateStateCodec{Kind: "bad", Version: 1}); !errors.Is(err, ErrAggregateStateRegistryCodecInvalid) {
		t.Fatalf("invalid Register() error = %v, want ErrAggregateStateRegistryCodecInvalid", err)
	}
	wrongEnvelope, err := MarshalAggregateStateEnvelope("other", 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := registry.Unmarshal(wrongEnvelope); !errors.Is(err, ErrAggregateStateRegistryCodecMissing) {
		t.Fatalf("unregistered envelope error = %v, want ErrAggregateStateRegistryCodecMissing", err)
	}
}

func TestCHU03AggregateStateRegistryRejectsCodecWireMismatch(t *testing.T) {
	registry := NewAggregateStateRegistry()
	codec := chu03CounterCodec()
	codec.Encode = func(any) ([]byte, error) {
		return MarshalAggregateStateEnvelope("wrong", 1, nil)
	}
	if err := registry.Register(codec); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Marshal("counter", 1, chu03CounterState{}); !errors.Is(err, ErrAggregateStateRegistryCodecMismatch) {
		t.Fatalf("wire mismatch error = %v, want ErrAggregateStateRegistryCodecMismatch", err)
	}
}

func TestCHU03AggregateStateRegistryConcurrentUse(t *testing.T) {
	registry := NewAggregateStateRegistry()
	if err := registry.Register(chu03CounterCodec()); err != nil {
		t.Fatal(err)
	}
	const workers = 8
	const rounds = 100
	var group sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for round := 0; round < rounds; round++ {
				encoded, err := registry.Marshal("counter", 1, chu03CounterState{Count: uint64(worker*round + 1)})
				if err != nil {
					t.Errorf("Marshal() error = %v", err)
					return
				}
				if _, _, err := registry.Unmarshal(encoded); err != nil {
					t.Errorf("Unmarshal() error = %v", err)
					return
				}
			}
		}(worker)
	}
	group.Wait()
}

func TestCHU03AggregateStateRegistryNilReceiver(t *testing.T) {
	var registry *AggregateStateRegistry
	if _, err := registry.Marshal("counter", 1, nil); !errors.Is(err, ErrAggregateStateRegistryNil) {
		t.Fatalf("nil Marshal() error = %v, want ErrAggregateStateRegistryNil", err)
	}
	if _, _, err := registry.Unmarshal(nil); !errors.Is(err, ErrAggregateStateRegistryNil) {
		t.Fatalf("nil Unmarshal() error = %v, want ErrAggregateStateRegistryNil", err)
	}
}

func chu03CounterCodec() AggregateStateCodec {
	return AggregateStateCodec{
		Kind:    "counter",
		Version: 1,
		Encode: func(value any) ([]byte, error) {
			state, ok := value.(chu03CounterState)
			if !ok {
				return nil, ErrAggregateStateRegistryValueMismatch
			}
			var payload [8]byte
			binary.LittleEndian.PutUint64(payload[:], state.Count)
			return MarshalAggregateStateEnvelope("counter", 1, payload[:])
		},
		Decode: func(data []byte) (any, error) {
			envelope, err := UnmarshalAggregateStateEnvelope(data)
			if err != nil {
				return nil, err
			}
			if len(envelope.Payload) != 8 {
				return nil, ErrAggregateStateRegistryValueMismatch
			}
			return chu03CounterState{Count: binary.LittleEndian.Uint64(envelope.Payload)}, nil
		},
	}
}
