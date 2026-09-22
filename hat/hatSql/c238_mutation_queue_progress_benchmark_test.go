package hatSql

import "testing"

func BenchmarkC238MutationQueueProgress(b *testing.B) {
	queue := newSQLMutationDependencyQueueBenchmarkFixture(b, 512)
	defer queue.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = queue.Progress()
	}
}
