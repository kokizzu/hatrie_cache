package hatCache

import (
	"context"
	"testing"
	"time"
)

var sqlJSONIndexRebuildWorkerBenchmarkSink *SQLJSONIndexRebuildWorker

func BenchmarkSQLJSONIndexRebuildWorkerStartup(b *testing.B) {
	trie := CreateHatTrie()
	b.ReportAllocs()
	for b.Loop() {
		worker, err := trie.StartSQLJSONIndexRebuildWorker(context.Background(), time.Hour, nil)
		if err != nil {
			b.Fatal(err)
		}
		sqlJSONIndexRebuildWorkerBenchmarkSink = worker
		worker.Stop()
		worker.Wait()
	}
}
