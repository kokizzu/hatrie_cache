package hatReplication

import (
	"encoding/json"
	"testing"
)

func BenchmarkExactlyOnceUpsertSinkBeginCommit(b *testing.B) {
	sink, err := NewExactlyOnceUpsertSink("orders")
	if err != nil {
		b.Fatal(err)
	}
	record := ExactlyOnceUpsertSinkRecord{OutputID: "order-1", Key: []byte("1"), Value: []byte("paid")}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		record.Sequence = uint64(index + 1)
		decision, err := sink.Begin(record)
		if err != nil || decision.Action != ExactlyOnceUpsertSinkApply {
			b.Fatalf("Begin() = %#v, %v", decision, err)
		}
		if _, err := sink.Commit(record.OutputID); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExactlyOnceUpsertSinkMarshalBinary(b *testing.B) {
	snapshot := m231BenchmarkPendingSnapshot(b)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := snapshot.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExactlyOnceUpsertSinkMarshalJSON(b *testing.B) {
	snapshot := m231BenchmarkPendingSnapshot(b)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := json.Marshal(snapshot); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExactlyOnceUpsertSinkUnmarshalBinary(b *testing.B) {
	snapshot := m231BenchmarkPendingSnapshot(b)
	encoded, err := snapshot.MarshalBinary()
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if _, err := UnmarshalExactlyOnceUpsertSinkSnapshot(encoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExactlyOnceUpsertSinkUnmarshalJSON(b *testing.B) {
	snapshot := m231BenchmarkPendingSnapshot(b)
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		var decoded ExactlyOnceUpsertSinkSnapshot
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func m231BenchmarkPendingSnapshot(b *testing.B) ExactlyOnceUpsertSinkSnapshot {
	b.Helper()
	return ExactlyOnceUpsertSinkSnapshot{
		Source:               "orders",
		HasCommittedSequence: true,
		CommittedSequence:    41,
		LastOutputID:         "order-41",
		Pending: &ExactlyOnceUpsertSinkRecord{
			Sequence: 42,
			OutputID: "order-42",
			Key:      []byte("order-42"),
			Value:    []byte("paid"),
		},
	}
}
