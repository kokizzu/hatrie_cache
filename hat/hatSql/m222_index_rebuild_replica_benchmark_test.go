package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkM222SingleQueueRebuildStatus(b *testing.B) {
	queue, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 0})
	if err != nil {
		b.Fatal(err)
	}
	defer queue.Close()
	if _, err := queue.Enqueue(hatSql.SQLIndexRebuildRequest{
		ID:   "status-single",
		Name: "people_projection",
		Run:  func(context.Context, hatSql.SQLIndexRebuildProgressFunc) error { return nil },
	}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := queue.Status("status-single"); !ok {
			b.Fatal("single queue status missing")
		}
	}
}

func BenchmarkM222ReplicaSetRebuildStatus(b *testing.B) {
	first, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 0})
	if err != nil {
		b.Fatal(err)
	}
	defer first.Close()
	second, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 0})
	if err != nil {
		b.Fatal(err)
	}
	defer second.Close()
	replicas, err := hatSql.NewSQLIndexRebuildReplicaSet(hatSql.SQLIndexRebuildReplicaSetOptions{
		Queues: []*hatSql.SQLIndexRebuildQueue{first, second},
		Quorum: 1,
	})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := replicas.Enqueue(hatSql.SQLIndexRebuildRequest{
		ID:   "status-replicated",
		Name: "people_projection",
		Run:  func(context.Context, hatSql.SQLIndexRebuildProgressFunc) error { return nil },
	}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := replicas.Status("status-replicated"); !ok {
			b.Fatal("replica set status missing")
		}
	}
}
