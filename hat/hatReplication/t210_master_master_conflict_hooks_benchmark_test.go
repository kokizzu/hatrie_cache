//go:build t210

package hatReplication

import "testing"

func BenchmarkT210DefaultConflictResolve(b *testing.B) {
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{Mode: ConflictPolicyLastWriteWins})
	if err != nil {
		b.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 10, NodeID: "region-a", Sequence: 41}
	right := ConflictVersion{Timestamp: 11, NodeID: "region-b", Sequence: 9}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := registry.Resolve("orders", left, right); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT210HookedConflictResolve(b *testing.B) {
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{
		Mode: ConflictPolicyLastWriteWins,
		Hook: func(ConflictHookContext) (ConflictHookDecision, error) {
			return ConflictHookUsePolicy, nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 10, NodeID: "region-a", Sequence: 41}
	right := ConflictVersion{Timestamp: 11, NodeID: "region-b", Sequence: 9}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := registry.Resolve("orders", left, right); err != nil {
			b.Fatal(err)
		}
	}
}
