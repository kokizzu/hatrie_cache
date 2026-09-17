package hatSql

import (
	"context"
	"testing"
)

type mu017BenchmarkResolver struct{}

func (mu017BenchmarkResolver) ResolveSQLSource(_ string, key string) ([]Row, error) {
	if key != "people" {
		return nil, nil
	}
	return []Row{{"name": "Ada"}}, nil
}

func (mu017BenchmarkResolver) BeginSQLSnapshotAt(_ context.Context, _ uint64) (SQLSourceResolver, func(), error) {
	return mu017BenchmarkResolver{}, func() {}, nil
}

func newMU017BenchmarkRunner(b *testing.B) (*IncrementalProjectionRunner, *SQLSourceFrontierBarrier) {
	b.Helper()
	resolver := mu017BenchmarkResolver{}
	views := NewMaterializedViews()
	if _, err := views.Create(context.Background(), MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}, resolver, QueryOptions{}); err != nil {
		b.Fatal(err)
	}
	runner, err := NewIncrementalProjectionRunner(views, resolver, QueryOptions{}, IncrementalProjectionRunnerOptions{Name: "people_projection", Enabled: true})
	if err != nil {
		b.Fatal(err)
	}
	barrier, err := NewSQLSourceFrontierBarrierFromPartitions([]SQLSourceFrontierPartition{{Source: "people", Partition: "0"}})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := barrier.Observe(SQLSourceFrontier{Source: "people", Partition: "0", Frontier: 7}); err != nil {
		b.Fatal(err)
	}
	return runner, barrier
}

func BenchmarkMU017RebuildBaseline(b *testing.B) {
	runner, _ := newMU017BenchmarkRunner(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := runner.Rebuild(context.Background(), []string{"people"}, 7); err != nil {
			b.Fatal(err)
		}
	}
}
