package hatReplication

import "testing"

func BenchmarkTU38ConflictInspectionRecord(b *testing.B) {
	log, err := NewConflictInspectionLog(1024)
	if err != nil {
		b.Fatal(err)
	}
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
	if err != nil {
		b.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := registry.ResolveAndRecord(log, "orders", "order-42", left, right); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU38ConflictInspectionRead(b *testing.B) {
	log, err := NewConflictInspectionLog(1024)
	if err != nil {
		b.Fatal(err)
	}
	version := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	for index := 0; index < 1024; index++ {
		if _, err := log.Record("orders", "order-42", ConflictPolicyLastWriteWins, version, version, version, ConflictInspectionWinner); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = log.Read(0, 64)
	}
}
