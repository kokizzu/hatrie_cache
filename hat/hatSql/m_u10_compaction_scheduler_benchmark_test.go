package hatSql

import (
	"context"
	"testing"
	"time"
)

func BenchmarkDifferentialTemporalJoinCompactionSchedulerIdleTick(b *testing.B) {
	join := mU10BenchmarkJoin(b)
	scheduler := &DifferentialTemporalJoinCompactionScheduler{
		join:      join,
		frontiers: func(context.Context) (uint64, uint64, error) { return 0, 0, nil },
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		scheduler.tick(context.Background(), time.Now().UTC())
	}
}

func BenchmarkDifferentialTemporalJoinCompactionSchedulerCompactionTick(b *testing.B) {
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
		scheduler := &DifferentialTemporalJoinCompactionScheduler{
			join: join,
			frontiers: func(context.Context) (uint64, uint64, error) {
				return 1024, 1024, nil
			},
		}
		b.StartTimer()
		scheduler.tick(context.Background(), time.Now().UTC())
	}
}
