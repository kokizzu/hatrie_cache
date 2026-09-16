package hatSql

import (
	"context"
	"sync/atomic"
	"testing"
)

var sqlIndexRebuildQueueBenchmarkSink uint64

func BenchmarkSQLIndexRebuildDirectCallbackBaseline(b *testing.B) {
	run := func(context.Context, func(int, int)) error {
		atomic.AddUint64(&sqlIndexRebuildQueueBenchmarkSink, 1)
		return nil
	}
	ctx := context.Background()
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if err := run(ctx, nil); err != nil {
			b.Fatal(err)
		}
	}
}
