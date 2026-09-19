package hatSql

import (
	"context"
	"testing"
	"time"
)

func BenchmarkDifferentialTemporalJoinAdaptiveLoad(b *testing.B) {
	left, right := mU08TemporalJoinBenchmarkRows(1024)
	policy := &DifferentialTemporalJoinCompactionPolicy{
		MinUpdates:           1024,
		MaxStateBytes:        1 << 30,
		MaxFrontierAge:       time.Hour,
		EstimatedBytesPerRow: 128,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		join, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{
			MaxTimeDistance:  4,
			LeftKey:          func(row SQLRow) string { return row["group"].(string) },
			RightKey:         func(row SQLRow) string { return row["group"].(string) },
			CompactionPolicy: policy,
		})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := join.ApplyLeft(left); err != nil {
			b.Fatal(err)
		}
		if _, err := join.ApplyRight(right); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDifferentialTemporalJoinAdaptiveRecommendation(b *testing.B) {
	join := mU10BenchmarkJoin(b)
	left, right := mU08TemporalJoinBenchmarkRows(1024)
	if _, err := join.ApplyLeft(left); err != nil {
		b.Fatal(err)
	}
	if _, err := join.ApplyRight(right); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		recommendation := join.CompactionRecommendation(time.Now())
		if !recommendation.ShouldCompact {
			b.Fatal("adaptive recommendation unexpectedly disabled")
		}
	}
}

func BenchmarkDifferentialTemporalJoinAdaptiveCompact(b *testing.B) {
	left, right := mU08TemporalJoinBenchmarkRows(1024)
	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		join := mU10BenchmarkJoin(b)
		if _, err := join.ApplyLeft(left); err != nil {
			b.Fatal(err)
		}
		if _, err := join.ApplyRight(right); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		stats, recommendation, err := join.CompactIfNeeded(context.Background(), 1024, 1024, time.Now())
		if err != nil {
			b.Fatal(err)
		}
		if !recommendation.ShouldCompact || stats.RemovedLeft == 0 || stats.RemovedRight == 0 {
			b.Fatal("adaptive compaction removed no rows")
		}
	}
}

func BenchmarkDifferentialTemporalJoinAdaptiveCompactCancellable(b *testing.B) {
	left, right := mU08TemporalJoinBenchmarkRows(1024)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.ReportAllocs()
	for range b.N {
		b.StopTimer()
		join := mU10BenchmarkJoin(b)
		if _, err := join.ApplyLeft(left); err != nil {
			b.Fatal(err)
		}
		if _, err := join.ApplyRight(right); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		stats, recommendation, err := join.CompactIfNeeded(ctx, 1024, 1024, time.Now())
		if err != nil {
			b.Fatal(err)
		}
		if !recommendation.ShouldCompact || stats.RemovedLeft == 0 || stats.RemovedRight == 0 {
			b.Fatal("cancellable adaptive compaction removed no rows")
		}
	}
}

func mU10BenchmarkJoin(b *testing.B) *DifferentialTemporalJoin {
	b.Helper()
	join, err := NewDifferentialTemporalJoin(DifferentialTemporalJoinDefinition{
		MaxTimeDistance: 4,
		LeftKey:         func(row SQLRow) string { return row["group"].(string) },
		RightKey:        func(row SQLRow) string { return row["group"].(string) },
		CompactionPolicy: &DifferentialTemporalJoinCompactionPolicy{
			MinUpdates:           1,
			MaxStateBytes:        1 << 30,
			MaxFrontierAge:       time.Hour,
			EstimatedBytesPerRow: 128,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	return join
}
