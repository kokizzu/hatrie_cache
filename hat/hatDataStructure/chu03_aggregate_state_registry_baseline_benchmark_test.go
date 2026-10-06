package hatDataStructure

import (
	"bytes"
	"testing"
)

var (
	chu03BaselineEnvelopeWire []byte
	chu03BaselineEnvelope     AggregateStateEnvelope
)

func BenchmarkCHU03DirectAggregateStateEnvelopeMarshal(b *testing.B) {
	payload := bytes.Repeat([]byte("partial-state"), 32)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wire, err := MarshalAggregateStateEnvelope("demo.sum", 1, payload)
		if err != nil {
			b.Fatal(err)
		}
		chu03BaselineEnvelopeWire = wire
	}
}

func BenchmarkCHU03DirectAggregateStateEnvelopeUnmarshal(b *testing.B) {
	payload := bytes.Repeat([]byte("partial-state"), 32)
	wire, err := MarshalAggregateStateEnvelope("demo.sum", 1, payload)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		envelope, err := UnmarshalAggregateStateEnvelope(wire)
		if err != nil {
			b.Fatal(err)
		}
		chu03BaselineEnvelope = envelope
	}
}
