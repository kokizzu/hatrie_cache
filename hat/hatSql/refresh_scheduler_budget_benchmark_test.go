package hatSql_test

import (
	"context"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkManagedRefreshSchedulerCycleBudget(b *testing.B) {
	for _, options := range []struct {
		name    string
		options hatSql.ManagedRefreshSchedulerOptions
	}{
		{name: "default", options: hatSql.ManagedRefreshSchedulerOptions{}},
		{name: "run_limited", options: hatSql.ManagedRefreshSchedulerOptions{MaxRunsPerCycle: 1}},
		{name: "budgeted", options: hatSql.ManagedRefreshSchedulerOptions{MaxRunsPerCycle: 1, MaxCycleDuration: time.Second}},
	} {
		b.Run(options.name, func(b *testing.B) {
			now := time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC)
			options.options.Now = func() time.Time { return now }
			scheduler, err := hatSql.NewManagedRefreshScheduler(options.options)
			if err != nil {
				b.Fatal(err)
			}
			if err := scheduler.AddRollup("refresh", time.Nanosecond, func(context.Context) error { return nil }); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				now = now.Add(time.Nanosecond)
				if _, err := scheduler.RunDue(context.Background()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
