package hatDataStructure_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"

	hatDataStructure "hatrie_cache/hat/hatDataStructure"
)

type registrySumState struct {
	Total int64
}

func encodeRegistrySumState(value interface{}) ([]byte, error) {
	state, ok := value.(registrySumState)
	if !ok {
		return nil, errors.New("registry sum expects registrySumState")
	}
	encoded := make([]byte, binary.MaxVarintLen64)
	return encoded[:binary.PutVarint(encoded, state.Total)], nil
}

func decodeRegistrySumState(encoded []byte) (interface{}, error) {
	total, size := binary.Varint(encoded)
	if size <= 0 || size != len(encoded) {
		return nil, errors.New("invalid registry sum state")
	}
	return registrySumState{Total: total}, nil
}

func newRegistrySumCodec(t testing.TB, version uint64) hatDataStructure.AggregateStateCodec {
	t.Helper()
	codec, err := hatDataStructure.NewAggregateStateCodec("sum", version, encodeRegistrySumState, decodeRegistrySumState)
	if err != nil {
		t.Fatalf("NewAggregateStateCodec() error = %v", err)
	}
	return codec
}

func TestAggregateStateRegistryRoundTripUsesHAG1(t *testing.T) {
	registry := hatDataStructure.NewAggregateStateRegistry()
	if err := registry.Register(newRegistrySumCodec(t, 7)); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	wire, err := registry.Encode("sum", 7, registrySumState{Total: 9})
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if !bytes.HasPrefix(wire, []byte("HAG1")) {
		t.Fatalf("wire prefix = %q, want HAG1", wire[:minRegistryState(len(wire), 4)])
	}
	envelope, value, err := registry.Decode(wire)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if envelope.Kind != "sum" || envelope.Version != 7 {
		t.Fatalf("envelope = %#v, want sum/v7", envelope)
	}
	if !reflect.DeepEqual(value, registrySumState{Total: 9}) {
		t.Fatalf("decoded value = %#v, want total 9", value)
	}
}

func TestAggregateStateRegistryRejectsMissingDuplicateAndVersionMismatch(t *testing.T) {
	registry := hatDataStructure.NewAggregateStateRegistry()
	codec := newRegistrySumCodec(t, 7)
	if err := registry.Register(codec); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(codec); !errors.Is(err, hatDataStructure.ErrAggregateStateCodecExists) {
		t.Fatalf("duplicate Register() error = %v, want exists", err)
	}
	if _, err := registry.Encode("missing", 1, registrySumState{}); !errors.Is(err, hatDataStructure.ErrAggregateStateCodecMissing) {
		t.Fatalf("missing Encode() error = %v, want missing", err)
	}
	if _, err := registry.Encode("sum", 8, registrySumState{}); !errors.Is(err, hatDataStructure.ErrAggregateStateCodecVersionMismatch) {
		t.Fatalf("version Encode() error = %v, want version mismatch", err)
	}
	wrongVersion, err := hatDataStructure.MarshalAggregateStateEnvelope("sum", 8, []byte{0})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := registry.Decode(wrongVersion); !errors.Is(err, hatDataStructure.ErrAggregateStateCodecVersionMismatch) {
		t.Fatalf("version Decode() error = %v, want version mismatch", err)
	}
	unknown, err := hatDataStructure.MarshalAggregateStateEnvelope("missing", 1, []byte{0})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := registry.Decode(unknown); !errors.Is(err, hatDataStructure.ErrAggregateStateCodecMissing) {
		t.Fatalf("unknown Decode() error = %v, want missing", err)
	}
}

func TestAggregateStateRegistryRejectsCorruptionAndCodecErrors(t *testing.T) {
	registry := hatDataStructure.NewAggregateStateRegistry()
	if err := registry.Register(newRegistrySumCodec(t, 1)); err != nil {
		t.Fatal(err)
	}
	wire, err := registry.Encode("sum", 1, registrySumState{Total: 4})
	if err != nil {
		t.Fatal(err)
	}
	corrupted := append([]byte(nil), wire...)
	corrupted[len(corrupted)-1] ^= 1
	if _, _, err := registry.Decode(corrupted); !errors.Is(err, hatDataStructure.ErrAggregateStateEnvelopeWire) {
		t.Fatalf("corrupt Decode() error = %v, want envelope wire error", err)
	}

	failing := hatDataStructure.NewAggregateStateRegistry()
	codec, err := hatDataStructure.NewAggregateStateCodec("failing", 1,
		func(interface{}) ([]byte, error) { return []byte{1}, nil },
		func([]byte) (interface{}, error) { return nil, errors.New("decode rejected") },
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := failing.Register(codec); err != nil {
		t.Fatal(err)
	}
	failingWire, err := failing.Encode("failing", 1, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := failing.Decode(failingWire); !errors.Is(err, hatDataStructure.ErrAggregateStateCodecDecode) {
		t.Fatalf("codec Decode() error = %v, want codec decode error", err)
	}
}

func TestAggregateStateRegistryRequiresCompleteCodec(t *testing.T) {
	for name, codec := range map[string]hatDataStructure.AggregateStateCodec{
		"missing kind":    {Version: 1, Encode: encodeRegistrySumState, Decode: decodeRegistrySumState},
		"missing version": {Kind: "sum", Encode: encodeRegistrySumState, Decode: decodeRegistrySumState},
		"missing encode":  {Kind: "sum", Version: 1, Decode: decodeRegistrySumState},
		"missing decode":  {Kind: "sum", Version: 1, Encode: encodeRegistrySumState},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := hatDataStructure.NewAggregateStateCodec(codec.Kind, codec.Version, codec.Encode, codec.Decode); !errors.Is(err, hatDataStructure.ErrAggregateStateCodecInvalid) {
				t.Fatalf("NewAggregateStateCodec() error = %v, want invalid", err)
			}
		})
	}
}

func TestAggregateStateRegistrySupportsConcurrentDecode(t *testing.T) {
	registry := hatDataStructure.NewAggregateStateRegistry()
	if err := registry.Register(newRegistrySumCodec(t, 1)); err != nil {
		t.Fatal(err)
	}
	wire, err := registry.Encode("sum", 1, registrySumState{Total: 3})
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for range 100 {
				_, value, err := registry.Decode(wire)
				if err != nil || value != (registrySumState{Total: 3}) {
					t.Errorf("Decode() = %#v/%v, want total 3", value, err)
					return
				}
			}
		}()
	}
	group.Wait()
}

func BenchmarkAggregateStateRegistry(b *testing.B) {
	registry := hatDataStructure.NewAggregateStateRegistry()
	if err := registry.Register(newRegistrySumCodec(b, 1)); err != nil {
		b.Fatal(err)
	}
	state := registrySumState{Total: 123456}
	payload, err := encodeRegistrySumState(state)
	if err != nil {
		b.Fatal(err)
	}
	directWire, err := hatDataStructure.MarshalAggregateStateEnvelope("sum", 1, payload)
	if err != nil {
		b.Fatal(err)
	}
	registryWire, err := registry.Encode("sum", 1, state)
	if err != nil {
		b.Fatal(err)
	}
	jsonWire, err := json.Marshal(struct {
		Kind    string
		Version uint64
		State   int64
	}{"sum", 1, state.Total})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(registryWire)), "registry-wire-bytes")
	b.ReportMetric(float64(len(jsonWire)), "json-bytes")
	b.Run("hag1-direct-encode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(directWire)))
		for range b.N {
			if _, err := hatDataStructure.MarshalAggregateStateEnvelope("sum", 1, payload); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("registry-encode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(registryWire)))
		for range b.N {
			if _, err := registry.Encode("sum", 1, state); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("hag1-direct-decode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(directWire)))
		for range b.N {
			if _, err := hatDataStructure.UnmarshalAggregateStateEnvelope(directWire); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("registry-decode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(registryWire)))
		for range b.N {
			if _, _, err := registry.Decode(registryWire); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("json-encode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(jsonWire)))
		for range b.N {
			if _, err := json.Marshal(struct {
				Kind    string
				Version uint64
				State   int64
			}{"sum", 1, state.Total}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("json-decode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(jsonWire)))
		for range b.N {
			var decoded struct {
				Kind    string
				Version uint64
				State   int64
			}
			if err := json.Unmarshal(jsonWire, &decoded); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func minRegistryState(left, right int) int {
	if left < right {
		return left
	}
	return right
}
