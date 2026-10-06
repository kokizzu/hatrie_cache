package hatStorage

import (
	"context"
	"testing"
)

type mu033ImmediateRetentionGate struct{}

func (mu033ImmediateRetentionGate) WaitUntilSafe(context.Context, string, uint64) error {
	return nil
}

func BenchmarkMU033CompactionControllerLegacy(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		controller, err := NewCompactionController(CompactionControllerOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if _, queued, err := controller.Submit(CompactionRequest{
			Target: "orders-part",
			Run:    func(context.Context) error { return nil },
		}); err != nil || !queued {
			b.Fatalf("Submit() = queued=%v err=%v", queued, err)
		}
		if _, err := controller.Run(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMU033CompactionControllerRetention(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		controller, err := NewCompactionController(CompactionControllerOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if _, queued, err := controller.Submit(CompactionRequest{
			Target:            "orders-part",
			RetentionGate:     mu033ImmediateRetentionGate{},
			RetentionFrontier: "orders",
			RetentionBoundary: 42,
			Run:               func(context.Context) error { return nil },
		}); err != nil || !queued {
			b.Fatalf("Submit() = queued=%v err=%v", queued, err)
		}
		if _, err := controller.Run(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
