package hatSql

import "testing"

func BenchmarkC238MutationQueueProgressBaseline(b *testing.B) {
	queue := newSQLMutationDependencyQueueBenchmarkFixture(b, 512)
	defer queue.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		total, pending, running, completed, failed := 0, 0, 0, 0, 0
		for _, task := range queue.Snapshot() {
			total++
			switch task.State {
			case SQLMutationTaskPending:
				pending++
			case SQLMutationTaskRunning:
				running++
			case SQLMutationTaskCompleted:
				completed++
			case SQLMutationTaskFailed:
				failed++
			}
		}
		_ = total - completed + pending + running + failed
	}
}
