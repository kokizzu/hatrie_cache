package hatDataStructure

import (
	"bytes"
	"testing"
)

var (
	chu03RegistryBenchmarkWire  []byte
	chu03RegistryBenchmarkValue AggregateStateValue
)

func BenchmarkCHU03RegistryAggregateStateMarshal(b *testing.B) {
	registry := chu03BenchmarkRegistry(b)
	payload := bytes.Repeat([]byte("partial-state"), 32)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wire, err := registry.Marshal("demo.sum", 1, payload)
		if err != nil {
			b.Fatal(err)
		}
		chu03RegistryBenchmarkWire = wire
	}
	b.ReportMetric(float64(len(chu03RegistryBenchmarkWire)), "wire_bytes/op")
}

func BenchmarkCHU03RegistryAggregateStateUnmarshal(b *testing.B) {
	registry := chu03BenchmarkRegistry(b)
	payload := bytes.Repeat([]byte("partial-state"), 32)
	wire, err := registry.Marshal("demo.sum", 1, payload)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value, err := registry.Unmarshal(wire)
		if err != nil {
			b.Fatal(err)
		}
		chu03RegistryBenchmarkValue = value
	}
	b.ReportMetric(float64(len(wire)), "wire_bytes/op")
}

func BenchmarkCHU03RegistryAggregateStateMerge(b *testing.B) {
	registry := chu03BenchmarkRegistry(b)
	left, err := registry.Marshal("demo.sum", 1, []byte("left"))
	if err != nil {
		b.Fatal(err)
	}
	right, err := registry.Marshal("demo.sum", 1, []byte("right"))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		merged, err := registry.Merge(left, right)
		if err != nil {
			b.Fatal(err)
		}
		chu03RegistryBenchmarkWire = merged
	}
	b.ReportMetric(float64(len(chu03RegistryBenchmarkWire)), "wire_bytes/op")
}

func chu03BenchmarkRegistry(b *testing.B) *AggregateStateRegistry {
	b.Helper()
	registry := NewAggregateStateRegistry()
	if err := registry.Register(AggregateStateCodec{
		Kind:    "demo.sum",
		Version: 1,
		Encode: func(value any) ([]byte, error) {
			return value.([]byte), nil
		},
		Decode: func(payload []byte) (any, error) {
			return payload, nil
		},
		Merge: func(left, right any) (any, error) {
			merged := make([]byte, 0, len(left.([]byte))+len(right.([]byte)))
			merged = append(merged, left.([]byte)...)
			return append(merged, right.([]byte)...), nil
		},
	}); err != nil {
		b.Fatal(err)
	}
	return registry
}
