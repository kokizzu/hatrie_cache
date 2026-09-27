package hatSql

import (
	"fmt"
	"path/filepath"
	"testing"
)

func newSQLMutationWorkerBenchmarkQueue(b *testing.B) *SQLMutationDependencyQueue {
	b.Helper()
	path := filepath.Join(b.TempDir(), "mutations.log")
	queue, err := OpenSQLMutationDependencyQueue(path, 128)
	if err != nil {
		b.Fatalf("OpenSQLMutationDependencyQueue() error = %v", err)
	}
	for index := 0; index < 64; index++ {
		if err := queue.Add(SQLMutationTask{ID: fmt.Sprintf("task-%02d", index)}); err != nil {
			_ = queue.Close()
			b.Fatalf("Add(%d) error = %v", index, err)
		}
	}
	return queue
}

func BenchmarkSQLMutationDependencyQueueManualProcessReady(b *testing.B) {
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		queue := newSQLMutationWorkerBenchmarkQueue(b)
		b.StartTimer()
		claimed, err := queue.ClaimReady(0)
		if err != nil {
			b.Fatalf("ClaimReady() error = %v", err)
		}
		for _, task := range claimed {
			if err := queue.Complete(task.ID, task.Attempt); err != nil {
				b.Fatalf("Complete(%q) error = %v", task.ID, err)
			}
		}
		b.StopTimer()
		if err := queue.Close(); err != nil {
			b.Fatalf("Close() error = %v", err)
		}
	}
}
