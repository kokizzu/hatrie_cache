package hatCache

import (
	"sync/atomic"
	"testing"
)

func BenchmarkTR013ImmediateCompactionControl(b *testing.B) {
	var calls atomic.Int64
	compact := func(LevelDBCompactionOptions) (LevelDBCompactionResult, error) {
		calls.Add(1)
		return LevelDBCompactionResult{}, nil
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := compact(LevelDBCompactionOptions{}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(calls.Load())/float64(b.N), "compactions/op")
}

func BenchmarkTR013DebtScheduler(b *testing.B) {
	var calls atomic.Int64
	scheduler, err := NewCompactionDebtScheduler(CompactionDebtSchedulerOptions{
		ThresholdBytes: 64 << 10,
		Compact: func(LevelDBCompactionOptions) (LevelDBCompactionResult, error) {
			calls.Add(1)
			return LevelDBCompactionResult{}, nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := scheduler.AddDebt(1024); err != nil {
			b.Fatal(err)
		}
		if _, _, err := scheduler.CompactIfDue(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(calls.Load())/float64(b.N), "compactions/op")
}

func BenchmarkTR013DebtAccountingBelowThreshold(b *testing.B) {
	scheduler, err := NewCompactionDebtScheduler(CompactionDebtSchedulerOptions{
		ThresholdBytes: 1 << 60,
		Compact: func(LevelDBCompactionOptions) (LevelDBCompactionResult, error) {
			return LevelDBCompactionResult{}, nil
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := scheduler.AddDebt(1024); err != nil {
			b.Fatal(err)
		}
	}
}
