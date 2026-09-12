package hatReplication

import "testing"

func BenchmarkConflictPolicyResolution(b *testing.B) {
	left := ConflictVersion{Timestamp: 10, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 11, NodeID: "node-b", Sequence: 1}
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{Mode: ConflictPolicyLastWriteWins})
	if err != nil {
		b.Fatal(err)
	}
	if err := registry.Set("priority", ConflictPolicy{Mode: ConflictPolicySourcePriority, SourcePriority: []string{"node-a", "node-b"}}); err != nil {
		b.Fatal(err)
	}

	b.Run("direct-lww", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if _, err := ResolveConflictVersion(left, right); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("registry-default", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if _, err := registry.Resolve("default", left, right); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("registry-priority", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			if _, err := registry.Resolve("priority", left, right); err != nil {
				b.Fatal(err)
			}
		}
	})
}
