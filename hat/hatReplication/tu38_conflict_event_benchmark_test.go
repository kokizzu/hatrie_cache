package hatReplication

import "testing"

func BenchmarkTU38ConflictResolveAndRecord(b *testing.B) {
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
	if err != nil {
		b.Fatal(err)
	}
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 100, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 101, NodeID: "node-b", Sequence: 1}
	key := []byte("order-42")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := registry.ResolveAndRecord(log, "orders", key, left, right); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU38ConflictEventRecord(b *testing.B) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 100, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 101, NodeID: "node-b", Sequence: 1}
	key := []byte("order-42")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := log.Record("orders", key, left, right, ConflictEventDecisionRight); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU38ConflictEventRecordParallel(b *testing.B) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 100, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 101, NodeID: "node-b", Sequence: 1}
	key := []byte("order-42")
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := log.Record("orders", key, left, right, ConflictEventDecisionRight); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkTU38ConflictEventSince(b *testing.B) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 100, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 101, NodeID: "node-b", Sequence: 1}
	for index := 0; index < 1024; index++ {
		if _, err := log.Record("orders", []byte("order"), left, right, ConflictEventDecisionRight); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		page, err := log.Since(900, 32)
		if err != nil || len(page.Events) != 32 {
			b.Fatalf("Since() = %d events, %v", len(page.Events), err)
		}
	}
}

func BenchmarkTU38ConflictEventEncode(b *testing.B) {
	log, err := NewConflictEventLog(ConflictEventLogOptions{Capacity: 64})
	if err != nil {
		b.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 100, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 101, NodeID: "node-b", Sequence: 1}
	for index := 0; index < 64; index++ {
		if _, err := log.Record("orders", []byte("order"), left, right, ConflictEventDecisionRight); err != nil {
			b.Fatal(err)
		}
	}
	snapshot := log.Snapshot()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := EncodeConflictEventSnapshot(snapshot); err != nil {
			b.Fatal(err)
		}
	}
}
