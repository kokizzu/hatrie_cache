package hatSql

import (
	"context"
	"testing"
	"time"
)

func BenchmarkC233CPUTimeBudget(b *testing.B) {
	b.Run("off", func(b *testing.B) {
		control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		defer cancel()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := control.check(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("on", func(b *testing.B) {
		control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{
			MaxCPUTime:    time.Hour,
			CPUTimeSource: NewSQLCPUTimeSource(func() time.Duration { return 0 }),
		})
		if err != nil {
			b.Fatal(err)
		}
		defer cancel()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := control.check(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
