package hatStorage

import (
	"context"
	"strconv"
	"testing"
)

var chu35CompactionRunSink CompactionRun

func BenchmarkCHU35CompactionControllerRun(b *testing.B) {
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		controller, err := NewCompactionController(CompactionControllerOptions{
			SchedulerOptions: CompactionSchedulerOptions{MaxConcurrent: 4},
			MaxPending:       64,
			HistoryCapacity:  64,
		})
		if err != nil {
			b.Fatal(err)
		}
		for task := 0; task < 64; task++ {
			_, accepted, err := controller.Submit(CompactionRequest{
				Target: strconv.Itoa(task),
				Run:    func(context.Context) error { return nil },
			})
			if err != nil || !accepted {
				b.Fatalf("Submit() = %v, %t", err, accepted)
			}
		}
		chu35CompactionRunSink, err = controller.Run(context.Background())
		if err != nil || chu35CompactionRunSink.Completed != 64 {
			b.Fatalf("Run() = %#v/%v", chu35CompactionRunSink, err)
		}
	}
}
