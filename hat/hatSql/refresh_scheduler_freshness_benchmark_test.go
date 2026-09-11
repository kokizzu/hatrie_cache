package hatSql

import (
	"context"
	"testing"
	"time"
)

var managedRefreshFreshnessBenchmarkSink []ManagedRefreshStatus

func BenchmarkManagedRefreshSchedulerStatuses(b *testing.B) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	scheduler, err := NewManagedRefreshScheduler(ManagedRefreshSchedulerOptions{Now: func() time.Time { return now }})
	if err != nil {
		b.Fatal(err)
	}
	if err := scheduler.AddRollupWithOptions("metrics", ManagedRefreshTaskOptions{
		Every:        time.Minute,
		MaxStaleness: 5 * time.Minute,
	}, func(context.Context) error { return nil }); err != nil {
		b.Fatal(err)
	}
	if _, err := scheduler.RunDue(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		managedRefreshFreshnessBenchmarkSink = scheduler.StatusesAt(now)
	}
}
