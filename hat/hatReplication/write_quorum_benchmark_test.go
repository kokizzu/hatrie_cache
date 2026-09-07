package hatReplication_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func BenchmarkExecuteWriteQuorumThreeTargets(b *testing.B) {
	nodes := []string{"local", "east", "west"}
	write := func(context.Context, string) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if result, err := hatReplication.ExecuteWriteQuorum(context.Background(), nodes, 2, write); err != nil || !result.Decision.Satisfied {
			b.Fatalf("ExecuteWriteQuorum() = %#v/%v, want satisfied", result, err)
		}
	}
}
