package hatSql

import (
	"context"
	"testing"
	"time"
)

var ch233CPUCheckSink error

func BenchmarkC233CPUCheckBaseline(b *testing.B) {
	control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	defer cancel()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch233CPUCheckSink = control.check()
	}
}

func BenchmarkC233CPUCheckCPUTime(b *testing.B) {
	control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{MaxCPUTime: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	defer cancel()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch233CPUCheckSink = control.check()
	}
}

func BenchmarkC233CPUCheckCPUTimeEvery1(b *testing.B) {
	control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{MaxCPUTime: time.Hour, CPUTimeCheckEvery: 1})
	if err != nil {
		b.Fatal(err)
	}
	defer cancel()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch233CPUCheckSink = control.check()
	}
}
