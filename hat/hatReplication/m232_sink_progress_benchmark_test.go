package hatReplication

import (
	"encoding/json"
	"testing"
)

func BenchmarkSinkProgressEmit(b *testing.B) {
	emitter, err := NewSinkProgressEmitter("orders", 0)
	if err != nil {
		b.Fatal(err)
	}
	record := ExactlyOnceUpsertSinkRecord{OutputID: "order-1", Key: []byte("1"), Value: []byte("paid")}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		record.Sequence = uint64(index + 1)
		if _, err := emitter.Emit([]ExactlyOnceUpsertSinkRecord{record}, record.Sequence); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSinkProgressMarshalBinary(b *testing.B) {
	envelope := m232BenchmarkEnvelope(b)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := envelope.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSinkProgressMarshalJSON(b *testing.B) {
	envelope := m232BenchmarkEnvelope(b)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := json.Marshal(envelope); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSinkProgressUnmarshalBinary(b *testing.B) {
	envelope := m232BenchmarkEnvelope(b)
	encoded, err := envelope.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := UnmarshalSinkProgressEnvelope(encoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSinkProgressUnmarshalJSON(b *testing.B) {
	envelope := m232BenchmarkEnvelope(b)
	encoded, err := json.Marshal(envelope)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		var decoded SinkProgressEnvelope
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func m232BenchmarkEnvelope(b *testing.B) SinkProgressEnvelope {
	b.Helper()
	records := make([]ExactlyOnceUpsertSinkRecord, 32)
	for index := range records {
		records[index] = ExactlyOnceUpsertSinkRecord{
			Sequence: uint64(index + 1),
			OutputID: "order-1",
			Key:      []byte("1"),
			Value:    []byte("paid"),
		}
	}
	return SinkProgressEnvelope{Source: "orders", Frontier: 64, Records: records}
}
