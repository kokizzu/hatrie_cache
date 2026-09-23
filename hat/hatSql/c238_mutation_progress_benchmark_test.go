package hatSql

import (
	"context"
	"testing"
)

func BenchmarkC238MutationSnapshot(b *testing.B) {
	controller, err := NewMutationController(MutationControllerOptions{Workers: 1, QueueCapacity: 1})
	if err != nil {
		b.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	handle, err := controller.Submit(context.Background(), MutationSpec{
		ID: "c238-benchmark",
		Run: func(ctx context.Context, report MutationProgressReporter) error {
			if err := report(MutationProgress{Completed: 2, Total: 10}); err != nil {
				return err
			}
			close(started)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	if err != nil {
		_ = controller.Close()
		b.Fatal(err)
	}
	<-started
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := handle.Snapshot(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	close(release)
	if err := handle.Wait(context.Background()); err != nil {
		b.Fatal(err)
	}
	if err := controller.Close(); err != nil {
		b.Fatal(err)
	}
}
