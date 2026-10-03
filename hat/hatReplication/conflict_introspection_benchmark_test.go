package hatReplication

import "testing"

func BenchmarkConflictEventLogAppend(b *testing.B) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 4096})
	if err != nil {
		b.Fatal(err)
	}
	event := conflictBenchmarkEvent()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		event.KeyDigest[0] = 1
		event.KeyDigest[1] = byte(index)
		if _, err := log.Append(event); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConflictEventLogRead(b *testing.B) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 4096})
	if err != nil {
		b.Fatal(err)
	}
	event := conflictBenchmarkEvent()
	for index := 0; index < 1024; index++ {
		event.KeyDigest[0] = 1
		event.KeyDigest[1] = byte(index)
		if _, err := log.Append(event); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		events, err := log.ReadAfter(0, 1024)
		if err != nil || len(events) != 1024 {
			b.Fatalf("ReadAfter() events=%d err=%v", len(events), err)
		}
	}
}

func BenchmarkConflictEventLogMarshal(b *testing.B) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 4096})
	if err != nil {
		b.Fatal(err)
	}
	event := conflictBenchmarkEvent()
	for index := 0; index < 1024; index++ {
		event.KeyDigest[0] = 1
		event.KeyDigest[1] = byte(index)
		if _, err := log.Append(event); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err := log.MarshalBinary()
		if err != nil || len(encoded) == 0 {
			b.Fatalf("MarshalBinary() len=%d err=%v", len(encoded), err)
		}
	}
}

func conflictBenchmarkEvent() ConflictEvent {
	event := ConflictEvent{Space: "orders", WinnerSource: "node-a", LoserSource: "node-b", Decision: ConflictDecisionLastWriteWins, Timestamp: 42}
	event.KeyDigest[0] = 1
	return event
}
