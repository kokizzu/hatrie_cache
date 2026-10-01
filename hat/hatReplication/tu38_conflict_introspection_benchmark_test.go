package hatReplication

import "testing"

var benchmarkTU38Winner ConflictVersion

func BenchmarkTU38ConflictRegistry(b *testing.B) {
	left := ConflictVersion{Timestamp: 10, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 11, NodeID: "node-b", Sequence: 1}
	b.Run("disabled", func(b *testing.B) {
		registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			benchmarkTU38Winner, err = registry.Resolve("space", left, right)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("redacted-log", func(b *testing.B) {
		registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
		if err != nil {
			b.Fatal(err)
		}
		log, err := NewConflictEventLog(1024)
		if err != nil {
			b.Fatal(err)
		}
		registry.SetEventLog(log)
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			benchmarkTU38Winner, err = registry.Resolve("space", left, right)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
