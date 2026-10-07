package hatDataStructure

import "testing"

var chu03RegistryBenchmarkSink any

func BenchmarkCHU03RegistryMarshal(b *testing.B) {
	registry := NewAggregateStateRegistry()
	if err := registry.Register(chu03CounterCodec()); err != nil {
		b.Fatal(err)
	}
	state := chu03CounterState{Count: 42}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err := registry.Marshal("counter", 1, state)
		if err != nil {
			b.Fatal(err)
		}
		chu03RegistryBenchmarkSink = encoded
	}
}

func BenchmarkCHU03EnvelopeMarshal(b *testing.B) {
	state := chu03CounterState{Count: 42}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var payload [8]byte
		payload[0] = byte(state.Count)
		encoded, err := MarshalAggregateStateEnvelope("counter", 1, payload[:])
		if err != nil {
			b.Fatal(err)
		}
		chu03RegistryBenchmarkSink = encoded
	}
}

func BenchmarkCHU03RegistryUnmarshal(b *testing.B) {
	registry := NewAggregateStateRegistry()
	if err := registry.Register(chu03CounterCodec()); err != nil {
		b.Fatal(err)
	}
	encoded, err := registry.Marshal("counter", 1, chu03CounterState{Count: 42})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_, value, err := registry.Unmarshal(encoded)
		if err != nil {
			b.Fatal(err)
		}
		chu03RegistryBenchmarkSink = value
	}
}
