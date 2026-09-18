package hatSql

import (
	"path/filepath"
	"testing"
)

func BenchmarkSQLMutationDependencyQueueClaimReady(b *testing.B) {
	queue := newSQLMutationDependencyQueueBenchmarkFixture(b, 512)
	defer queue.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		claimed, err := queue.ClaimReady(64)
		if err != nil {
			b.Fatal(err)
		}
		if len(claimed) != 64 {
			b.Fatalf("ClaimReady() returned %d tasks, want 64", len(claimed))
		}
		if count, err := queue.RequeueRunning(); err != nil || count != 64 {
			b.Fatalf("RequeueRunning() = %d/%v, want 64/nil", count, err)
		}
	}
}

func BenchmarkSQLMutationDependencyQueueOpenReplay(b *testing.B) {
	path := filepath.Join(b.TempDir(), "mutations.log")
	queue, err := OpenSQLMutationDependencyQueue(path, 512)
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 256; index++ {
		if err := queue.Add(SQLMutationTask{ID: mutationDependencyBenchmarkID(index)}); err != nil {
			b.Fatal(err)
		}
	}
	if err := queue.Close(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		queue, err := OpenSQLMutationDependencyQueue(path, 512)
		if err != nil {
			b.Fatal(err)
		}
		if len(queue.Snapshot()) != 256 {
			b.Fatal("replayed queue has unexpected task count")
		}
		if err := queue.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func newSQLMutationDependencyQueueBenchmarkFixture(b *testing.B, taskCount int) *SQLMutationDependencyQueue {
	b.Helper()
	path := filepath.Join(b.TempDir(), "mutations.log")
	queue, err := OpenSQLMutationDependencyQueue(path, taskCount+1)
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < taskCount; index++ {
		if err := queue.Add(SQLMutationTask{ID: mutationDependencyBenchmarkID(index)}); err != nil {
			queue.Close()
			b.Fatal(err)
		}
	}
	return queue
}
