package hatReplication

import (
	"context"
	"testing"
	"time"
)

func BenchmarkExecuteWriteQuorumUntilSatisfiedFastTargets(b *testing.B) {
	nodes := []string{"fast-a", "fast-b", "fast-c"}
	write := func(context.Context, string) error { return nil }
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteWriteQuorumUntilSatisfied(context.Background(), nodes, 2, write)
		if err != nil || !result.Decision.Satisfied {
			b.Fatalf("early fast quorum = %#v/%v, want satisfied", result, err)
		}
	}
}

func BenchmarkExecuteWriteQuorumUntilSatisfiedSlowTarget(b *testing.B) {
	nodes := []string{"fast-a", "fast-b", "slow"}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteWriteQuorumUntilSatisfied(context.Background(), nodes, 2, benchmarkSlowWrite)
		if err != nil || !result.Decision.Satisfied {
			b.Fatalf("early quorum = %#v/%v, want satisfied", result, err)
		}
	}
}

func BenchmarkExecuteWriteQuorumWaitAllWithSlowTarget(b *testing.B) {
	nodes := []string{"fast-a", "fast-b", "slow"}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteWriteQuorum(context.Background(), nodes, 2, benchmarkSlowWrite)
		if err != nil || !result.Decision.Satisfied {
			b.Fatalf("wait-all quorum = %#v/%v, want satisfied", result, err)
		}
	}
}

func benchmarkSlowWrite(ctx context.Context, node string) error {
	if node != "slow" {
		return nil
	}
	timer := time.NewTimer(100 * time.Microsecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
