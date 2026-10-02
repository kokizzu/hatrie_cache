package hatReplication

import (
	"testing"
	"time"
)

var conflictIntrospectionBenchmarkSink ConflictIntrospectionRecord

func BenchmarkConflictIntrospectionRecord(b *testing.B) {
	log, err := NewConflictIntrospectionLog(ConflictIntrospectionLogOptions{
		Capacity: 4096,
		Now:      func() time.Time { return time.Unix(123, 456) },
	})
	if err != nil {
		b.Fatal(err)
	}
	input := ConflictIntrospectionInput{
		Space:   "orders",
		Key:     []byte("customer-42"),
		Left:    ConflictVersion{Timestamp: 10, NodeID: "node-a", Sequence: 1},
		Right:   ConflictVersion{Timestamp: 11, NodeID: "node-b", Sequence: 2},
		Winner:  ConflictVersion{Timestamp: 11, NodeID: "node-b", Sequence: 2},
		Outcome: ConflictIntrospectionRightWon,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		record, err := log.Record(input)
		if err != nil {
			b.Fatal(err)
		}
		conflictIntrospectionBenchmarkSink = record
	}
}

func BenchmarkConflictIntrospectionRead(b *testing.B) {
	log, err := NewConflictIntrospectionLog(ConflictIntrospectionLogOptions{Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	version := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	for index := 0; index < 1024; index++ {
		if _, err := log.Record(ConflictIntrospectionInput{
			Space:   "orders",
			Key:     []byte{byte(index), byte(index >> 8)},
			Left:    version,
			Right:   version,
			Winner:  version,
			Outcome: ConflictIntrospectionLeftWon,
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		records, _, err := log.Read(0, 32)
		if err != nil {
			b.Fatal(err)
		}
		if len(records) != 32 {
			b.Fatalf("Read() returned %d records, want 32", len(records))
		}
		conflictIntrospectionBenchmarkSink = records[0]
	}
}
