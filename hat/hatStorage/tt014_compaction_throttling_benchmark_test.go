package hatStorage_test

import (
	"context"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

var compactionControllerTT014Sink hatStorage.CompactionJob

func BenchmarkCompactionControllerPendingBytesTT014(b *testing.B) {
	for _, test := range []struct {
		name            string
		maxPendingBytes uint64
	}{
		{name: "budget-off", maxPendingBytes: 0},
		{name: "budget-on", maxPendingBytes: 64 * 1024},
	} {
		b.Run(test.name, func(b *testing.B) {
			callback := func(context.Context) error { return nil }
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				controller, err := hatStorage.NewCompactionController(hatStorage.CompactionControllerOptions{
					MaxPending:      64,
					MaxPendingBytes: test.maxPendingBytes,
				})
				if err != nil {
					b.Fatal(err)
				}
				for task := 0; task < 64; task++ {
					job, accepted, err := controller.Submit(hatStorage.CompactionRequest{
						Target:         strconv.Itoa(task),
						EstimatedBytes: 1024,
						Run:            callback,
					})
					if err != nil || !accepted {
						b.Fatalf("Submit(%d) = %#v, %t, %v", task, job, accepted, err)
					}
					compactionControllerTT014Sink = job
				}
			}
		})
	}
}
