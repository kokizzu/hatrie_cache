package hatSql

import (
	"context"
	"strconv"
	"testing"
	"time"
)

func BenchmarkCH007TTLSchedulerRunOnceNoop(b *testing.B) {
	clock := time.Unix(100, 0)
	table := newCH007TTLBenchmarkTable(b, TypedTableTTLOptions{
		Mode:     TypedTableTTLProcessingTime,
		Lifetime: time.Hour,
		Clock:    func() time.Time { return clock },
	})
	scheduler, err := NewTypedTableTTLScheduler(TypedTableTTLSchedulerOptions{
		Now: clockNow(clock),
	})
	if err != nil {
		b.Fatal(err)
	}
	if err := scheduler.Register("events", table); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		runs, err := scheduler.RunOnce(context.Background())
		if err != nil || len(runs) != 1 {
			b.Fatalf("RunOnce() = %#v/%v, want one run", runs, err)
		}
		ch007TTLBenchmarkSink += runs[0].Expired
	}
	b.StopTimer()
	_ = scheduler.Close()
}

func BenchmarkCH007TTLMarshalState(b *testing.B) {
	clock := time.Unix(100, 0)
	table := newCH007TTLBenchmarkTable(b, TypedTableTTLOptions{
		Mode:     TypedTableTTLProcessingTime,
		Lifetime: time.Hour,
		Clock:    func() time.Time { return clock },
	})
	for index := 0; index < ch007TTLBenchmarkRows; index++ {
		if _, err := table.Upsert("row-"+strconv.Itoa(index), []TypedTableValue{TypedInt64(clock.UnixNano()), TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded, err := table.MarshalTTLState()
		if err != nil {
			b.Fatal(err)
		}
		ch007TTLBenchmarkSink += len(encoded)
	}
}

func clockNow(now time.Time) func() time.Time {
	return func() time.Time { return now }
}
