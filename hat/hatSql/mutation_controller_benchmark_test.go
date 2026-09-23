package hatSql

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
)

var mutationBenchmarkSink uint64

func BenchmarkMutationControllerSubmitWait(b *testing.B) {
	controller, err := NewMutationController(MutationControllerOptions{
		QueueCapacity:   128,
		Workers:         1,
		HistoryCapacity: 128,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer controller.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handle, err := controller.Submit(context.Background(), MutationSpec{
			ID:       "bench-" + strconv.Itoa(i),
			Priority: i & 3,
			Run: func(ctx context.Context, report MutationProgressReporter) error {
				progress := MutationProgress{Completed: 1, Total: 1}
				atomic.AddUint64(&mutationBenchmarkSink, progress.Completed)
				return report(progress)
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		if err := handle.Wait(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

func BenchmarkMutationDirectCallback(b *testing.B) {
	callback := func(ctx context.Context, report MutationProgressReporter) error {
		return report(MutationProgress{Completed: 1, Total: 1})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := callback(context.Background(), func(progress MutationProgress) error {
			atomic.AddUint64(&mutationBenchmarkSink, progress.Completed)
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}
