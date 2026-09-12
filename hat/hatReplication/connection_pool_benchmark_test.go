package hatReplication

import (
	"context"
	"sync/atomic"
	"testing"
)

var connectionPoolBenchmarkDialSequence uint64
var connectionPoolBenchmarkCloseSequence uint64

func benchmarkConnectionDial(context.Context) (int, error) {
	return int(atomic.AddUint64(&connectionPoolBenchmarkDialSequence, 1)), nil
}

func benchmarkConnectionClose(connection int) error {
	atomic.AddUint64(&connectionPoolBenchmarkCloseSequence, uint64(connection))
	return nil
}

func BenchmarkConnectionPoolAcquireRelease(b *testing.B) {
	pool, err := NewConnectionPool(ConnectionPoolOptions[int]{
		MaxOpen: 1,
		MaxIdle: 1,
		Dial:    benchmarkConnectionDial,
		Close:   benchmarkConnectionClose,
	})
	if err != nil {
		b.Fatal(err)
	}
	connection, err := pool.Acquire(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	pool.Release(connection)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		connection, err := pool.Acquire(ctx)
		if err != nil {
			b.Fatal(err)
		}
		pool.Release(connection)
	}
}

func BenchmarkConnectionDirectDialClose(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		connection, err := benchmarkConnectionDial(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if err := benchmarkConnectionClose(connection); err != nil {
			b.Fatal(err)
		}
	}
}
