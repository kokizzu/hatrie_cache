package hatTopology_test

import (
	"testing"

	"hatrie_cache/hat/hatTopology"
)

func BenchmarkT204LeaderForKey(b *testing.B) {
	tests := []struct {
		name  string
		setup func(*hatTopology.ElectionStore) error
	}{
		{
			name: "AutomaticDefault",
		},
		{
			name: "OperatorOverride",
			setup: func(store *hatTopology.ElectionStore) error {
				return store.SetLeaderOverride(0, "node-b", "benchmark")
			},
		},
		{
			name: "RecoveryState",
			setup: func(store *hatTopology.ElectionStore) error {
				if err := store.SetLeaderOverride(0, "node-b", "benchmark"); err != nil {
					return err
				}
				return store.BeginRecovery(0, "benchmark")
			},
		},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			store := newT204ElectionStore()
			if test.setup != nil {
				if err := test.setup(store); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok := store.LeaderForKey("account:42"); !ok {
					b.Fatal("LeaderForKey() returned no route")
				}
			}
		})
	}
}
