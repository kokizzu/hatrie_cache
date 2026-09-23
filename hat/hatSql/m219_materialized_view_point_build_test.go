package hatSql_test

import (
	"context"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestM219MaterializedViewPointLookupBuildExposesFrontierAndPublishesAtomically(t *testing.T) {
	resolver := &m219PointBuildResolver{
		rows: []hatSql.Row{
			{"id": int64(1), "region": "sg", "name": "Ada"},
			{"id": int64(2), "region": "jp", "name": "Kai"},
			{"id": int64(3), "region": "us", "name": "Lin"},
			{"id": int64(4), "region": "sg", "name": "Mira"},
		},
		version: "1",
	}
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_projection",
		Query:        "FROM CACHE('people') AS p SELECT p.id, p.region, p.name",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	queue, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	status, err := views.EnqueuePointLookupBuild(queue, hatSql.MaterializedViewPointLookupBuildRequest{
		ID:       "people-region-v1",
		ViewName: "people_projection",
		Fields:   []string{"region"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if status.State != hatSql.SQLIndexRebuildQueued || status.Frontier != 0 {
		t.Fatalf("queued status = %#v, want queued frontier zero", status)
	}

	before, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM CACHE('people') AS p WHERE p.region = 'sg' SELECT p.id, p.region, p.name", resolver, hatSql.QueryOptions{ProjectionCatalog: views})
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Plan) != 1 || before.Plan[0].Node != "MATERIALIZED ARRANGEMENT SCAN" {
		t.Fatalf("pre-build plan = %#v, want arrangement scan", before.Plan)
	}

	if err := queue.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := queue.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, ok := queue.Status("people-region-v1")
	if !ok || status.State != hatSql.SQLIndexRebuildSucceeded || status.Completed != len(resolver.rows) || status.Total != len(resolver.rows) || status.Frontier != uint64(len(resolver.rows)) {
		t.Fatalf("completed status = %#v/%v, want complete frontier", status, ok)
	}

	rows, available, err := views.PointLookup("people_projection", "region", "sg")
	if err != nil || !available {
		t.Fatalf("PointLookup() = %#v/%v/%v, want available", rows, available, err)
	}
	want := []hatSql.Row{
		{"id": int64(1), "region": "sg", "name": "Ada"},
		{"id": int64(4), "region": "sg", "name": "Mira"},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("PointLookup() = %#v, want %#v", rows, want)
	}
	after, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM CACHE('people') AS p WHERE p.region = 'sg' SELECT p.id, p.region, p.name", resolver, hatSql.QueryOptions{ProjectionCatalog: views})
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Plan) != 1 || after.Plan[0].Node != "MATERIALIZED POINT LOOKUP" {
		t.Fatalf("post-build plan = %#v, want point lookup", after.Plan)
	}
}

func TestM219MaterializedViewPointLookupBuildRejectsStaleRevision(t *testing.T) {
	resolver := &m219PointBuildResolver{
		rows:    []hatSql.Row{{"id": int64(1), "region": "sg"}},
		version: "1",
	}
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_projection",
		Query:        "FROM CACHE('people') AS p SELECT p.id, p.region",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	queue, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	if _, err := views.EnqueuePointLookupBuild(queue, hatSql.MaterializedViewPointLookupBuildRequest{
		ID:       "people-region-stale",
		ViewName: "people_projection",
		Fields:   []string{"region"},
	}); err != nil {
		t.Fatal(err)
	}
	resolver.rows = []hatSql.Row{{"id": int64(2), "region": "sg"}}
	resolver.version = "2"
	if _, err := views.RefreshChanged(context.Background(), []string{"people"}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := queue.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := queue.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, ok := queue.Status("people-region-stale")
	if !ok || status.State != hatSql.SQLIndexRebuildFailed || status.Error == "" {
		t.Fatalf("stale build status = %#v/%v, want failed", status, ok)
	}
	if _, available, err := views.PointLookup("people_projection", "region", "sg"); err != nil || available {
		t.Fatalf("stale PointLookup() = available=%v error=%v, want unavailable", available, err)
	}
}

func TestM219MaterializedViewPointLookupBuildRejectsUnknownField(t *testing.T) {
	resolver := &m219PointBuildResolver{rows: []hatSql.Row{{"id": int64(1)}}, version: "1"}
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_projection",
		Query:        "FROM CACHE('people') SELECT id",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	queue, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	if _, err := views.EnqueuePointLookupBuild(queue, hatSql.MaterializedViewPointLookupBuildRequest{
		ID:       "people-missing",
		ViewName: "people_projection",
		Fields:   []string{"missing"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := queue.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := queue.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	status, ok := queue.Status("people-missing")
	if !ok || status.State != hatSql.SQLIndexRebuildFailed || status.Error == "" {
		t.Fatalf("unknown field status = %#v/%v, want failed", status, ok)
	}
}

type m219PointBuildResolver struct {
	rows    []hatSql.Row
	version string
}

func (resolver *m219PointBuildResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if name != "CACHE" || key != "people" {
		return nil, nil
	}
	return hatSql.CloneRows(resolver.rows), nil
}

func (resolver *m219PointBuildResolver) SQLSourceVersion(name, key string) (string, bool, error) {
	if name != "CACHE" || key != "people" {
		return "", false, nil
	}
	return resolver.version, true, nil
}
