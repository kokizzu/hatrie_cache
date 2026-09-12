package hatCache

import (
	"context"
	"testing"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

var benchmarkCommandStreamResponseSink *hatriecachev1.CommandResponse

func BenchmarkGRPCCommandStreamRequestIDs(b *testing.B) {
	const batchSize = 32
	for _, benchmark := range []struct {
		name    string
		workers int
		ids     bool
	}{
		{name: "LegacyOrdered", workers: 1},
		{name: "Multiplexed4", workers: 4, ids: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			client, stop := newGRPCBenchmarkClientWithOptions(b, CacheGRPCOptions{
				NodeName:             "bench",
				CommandStreamWorkers: benchmark.workers,
			})
			defer stop()
			stream, err := client.CommandStream(context.Background())
			if err != nil {
				b.Fatalf("CommandStream() error = %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for j := 0; j < batchSize; j++ {
					request := &hatriecachev1.CommandRequest{Command: "GET", Key: "benchmark:missing"}
					if benchmark.ids {
						request.RequestId = uint64(i*batchSize + j + 1)
					}
					if err := stream.Send(request); err != nil {
						b.Fatalf("CommandStream.Send(%d) error = %v", j, err)
					}
				}
				for j := 0; j < batchSize; j++ {
					response, err := stream.Recv()
					if err != nil {
						b.Fatalf("CommandStream.Recv(%d) error = %v", j, err)
					}
					benchmarkCommandStreamResponseSink = response
				}
			}
		})
	}
}
