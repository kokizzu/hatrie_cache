package hatReplication

import "testing"

func BenchmarkTU38BaselineConflictResolution(b *testing.B) {
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
	if err != nil {
		b.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 100, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 101, NodeID: "node-b", Sequence: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := registry.Resolve("orders", left, right); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU38BaselineConflictResolutionParallel(b *testing.B) {
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
	if err != nil {
		b.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 100, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 101, NodeID: "node-b", Sequence: 1}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := registry.Resolve("orders", left, right); err != nil {
				b.Fatal(err)
			}
		}
	})
}
