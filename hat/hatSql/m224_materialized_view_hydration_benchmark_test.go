package hatSql_test

import (
	"context"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkM224HydrationStatusRunning(b *testing.B) {
	views, queue, release := newM224HydrationBenchmarkView(b, true)
	defer queue.Close()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, ok := views.HydrationStatus("people_projection"); !ok {
			b.Fatal("running hydration status missing")
		}
	}
	b.StopTimer()
	release()
	flushM224HydrationBenchmarkBuild(b, queue)
}

func BenchmarkM224HydrationStatusReady(b *testing.B) {
	views, queue, _ := newM224HydrationBenchmarkView(b, false)
	defer queue.Close()
	if err := queue.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
	if err := queue.Flush(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, ok := views.HydrationStatus("people_projection"); !ok {
			b.Fatal("ready hydration status missing")
		}
	}
}

func newM224HydrationBenchmarkView(b *testing.B, pauseAtHalf bool) (*hatSql.MaterializedViews, *hatSql.SQLIndexRebuildQueue, func()) {
	rows := make([]hatSql.Row, 512)
	for index := range rows {
		rows[index] = hatSql.Row{"id": int64(index), "region": "sg"}
	}
	views := hatSql.NewMaterializedViews()
	resolver := &m219PointBuildResolver{rows: rows, version: "1"}
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_projection",
		Query:        "FROM CACHE('people') AS p SELECT p.id, p.region",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		b.Fatal(err)
	}
	queue, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 1})
	if err != nil {
		b.Fatal(err)
	}
	request := hatSql.MaterializedViewPointLookupBuildRequest{
		ID:       "people-region-benchmark",
		ViewName: "people_projection",
		Fields:   []string{"region"},
	}
	if pauseAtHalf {
		reachedHalf := make(chan struct{})
		release := make(chan struct{})
		request.Progress = func(completed, total int) {
			if completed == 256 && total == 512 {
				close(reachedHalf)
				<-release
			}
		}
		if _, err := views.EnqueuePointLookupBuild(queue, request); err != nil {
			queue.Close()
			b.Fatal(err)
		}
		if err := queue.Start(context.Background()); err != nil {
			queue.Close()
			b.Fatal(err)
		}
		select {
		case <-reachedHalf:
		case <-time.After(2 * time.Second):
			close(release)
			queue.Close()
			b.Fatal("timed out waiting for benchmark hydration progress")
		}
		return views, queue, func() {
			select {
			case <-release:
			default:
				close(release)
			}
		}
	}
	if _, err := views.EnqueuePointLookupBuild(queue, request); err != nil {
		queue.Close()
		b.Fatal(err)
	}
	return views, queue, func() {}
}

func flushM224HydrationBenchmarkBuild(b *testing.B, queue *hatSql.SQLIndexRebuildQueue) {
	if err := queue.Flush(context.Background()); err != nil {
		b.Fatal(err)
	}
}
