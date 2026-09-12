package hatTopology_test

import (
	"context"
	"errors"
	"testing"
	"time"
)

var replicaHedgeBenchmarkError = errors.New("replica hedge benchmark miss")

var replicaHedgeBenchmarkCandidates = []string{
	"node-a",
	"node-b",
}

func replicaHedgeBenchmarkRead(ctx context.Context, candidate string) (string, error) {
	if candidate == "node-a" {
		timer := time.NewTimer(5 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			return "", replicaHedgeBenchmarkError
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return "ok", nil
}

func replicaHedgeBenchmarkSequential(ctx context.Context, candidates []string) (string, string, error) {
	for _, candidate := range candidates {
		value, err := replicaHedgeBenchmarkRead(ctx, candidate)
		if err == nil {
			return value, candidate, nil
		}
	}
	return "", "", replicaHedgeBenchmarkError
}

func BenchmarkReplicaReadSequential(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := replicaHedgeBenchmarkSequential(ctx, replicaHedgeBenchmarkCandidates); err != nil {
			b.Fatal(err)
		}
	}
}
