package hatReplication

import "testing"

func benchmarkConflictEventInput() ConflictEventInput {
	return ConflictEventInput{
		Space:    "orders",
		Key:      []byte("customer-00000000000000000000000000000001"),
		Left:     ConflictVersion{Timestamp: 10, NodeID: "node-a", Sequence: 1},
		Right:    ConflictVersion{Timestamp: 11, NodeID: "node-b", Sequence: 2},
		Winner:   ConflictVersion{Timestamp: 11, NodeID: "node-b", Sequence: 2},
		Decision: ConflictDecisionLastWriteWins,
	}
}

func BenchmarkConflictEventLogAppend(b *testing.B) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	input := benchmarkConflictEventInput()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := log.Append(input); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConflictEventLogRead(b *testing.B) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	input := benchmarkConflictEventInput()
	for index := 0; index < 1024; index++ {
		if _, err := log.Append(input); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		batch := log.Read(0, 64)
		if len(batch.Events) != 64 {
			b.Fatalf("Read() returned %d events, want 64", len(batch.Events))
		}
	}
}

func BenchmarkConflictEventLogMarshalBinary(b *testing.B) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	input := benchmarkConflictEventInput()
	for index := 0; index < 1024; index++ {
		if _, err := log.Append(input); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		wire, err := log.MarshalBinary()
		if err != nil || len(wire) == 0 {
			b.Fatalf("MarshalBinary() length = %d, error = %v", len(wire), err)
		}
	}
}
