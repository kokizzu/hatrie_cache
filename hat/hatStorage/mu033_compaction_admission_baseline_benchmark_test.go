package hatStorage

import (
	"context"
	"testing"
)

func BenchmarkMU33BaselineCompactionControllerSubmitRun(b *testing.B) {
	controller, err := NewCompactionController(CompactionControllerOptions{MaxPending: 1})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, queued, err := controller.Submit(CompactionRequest{
			Target: "target",
			Run:    func(context.Context) error { return nil },
		}); err != nil || !queued {
			b.Fatalf("Submit() = queued %v, err %v", queued, err)
		}
		if _, err := controller.Run(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
