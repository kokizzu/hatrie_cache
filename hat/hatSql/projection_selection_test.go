package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type projectionSelectionResolver struct {
	rows              []hatSql.Row
	version           string
	calls             int
	changeVersionRead bool
}

func (resolver *projectionSelectionResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if name != "CACHE" || key != "events" {
		return nil, nil
	}
	resolver.calls++
	if resolver.changeVersionRead {
		resolver.version = "2"
		resolver.changeVersionRead = false
	}
	return hatSql.CloneRows(resolver.rows), nil
}

func (resolver *projectionSelectionResolver) SQLSourceVersion(name, key string) (string, bool, error) {
	if name != "CACHE" || key != "events" {
		return "", false, nil
	}
	return resolver.version, true, nil
}

func TestProjectionCatalogServesFreshExactResultAndInvalidatesOnVersionChange(t *testing.T) {
	resolver := &projectionSelectionResolver{
		rows:    []hatSql.Row{{"name": "Ada"}},
		version: "1",
	}
	views := hatSql.NewMaterializedViews()
	query := "FROM CACHE('events') SELECT name"
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "event_names",
		Query:        query,
		Dependencies: []string{"events"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}

	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.QueryOptions{ProjectionCatalog: views})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["name"] != "Ada" {
		t.Fatalf("projection result = %#v", result.Rows)
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver calls on projection hit = %d, want 1 create call", resolver.calls)
	}
	if len(result.Plan) != 1 || result.Plan[0].Node != "PROJECTION HIT" {
		t.Fatalf("projection plan = %#v, want projection hit", result.Plan)
	}

	result.Rows[0]["name"] = "mutated"
	resolver.rows = []hatSql.Row{{"name": "Lin"}}
	resolver.version = "2"
	result, err = hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.QueryOptions{ProjectionCatalog: views})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["name"] != "Lin" {
		t.Fatalf("stale projection result = %#v", result.Rows)
	}
	if len(result.Plan) == 1 && result.Plan[0].Node == "PROJECTION HIT" {
		t.Fatalf("stale projection was served: %#v", result.Plan)
	}
	if _, err := views.RefreshChanged(context.Background(), []string{"events"}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	result, err = hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.QueryOptions{ProjectionCatalog: views})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["name"] != "Lin" || len(result.Plan) != 1 || result.Plan[0].Node != "PROJECTION HIT" {
		t.Fatalf("refreshed projection result = %#v, plan = %#v", result.Rows, result.Plan)
	}
}

func TestProjectionCatalogRequiresExactQueryAndSourceVersion(t *testing.T) {
	resolver := &projectionSelectionResolver{
		rows:    []hatSql.Row{{"name": "Ada"}},
		version: "1",
	}
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "event_names",
		Query:        "FROM CACHE('events') SELECT name",
		Dependencies: []string{"events"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM CACHE('events') SELECT name AS label", resolver, hatSql.QueryOptions{ProjectionCatalog: views})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["label"] != "Ada" {
		t.Fatalf("non-exact query result = %#v", result.Rows)
	}
	if resolver.calls != 2 {
		t.Fatalf("non-exact query resolver calls = %d, want create plus query", resolver.calls)
	}
}

func TestProjectionCatalogFallsBackWithoutVersionContract(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(name, key string) ([]hatSql.Row, error) {
		if name != "CACHE" || key != "events" {
			return nil, nil
		}
		return []hatSql.Row{{"name": "Ada"}}, nil
	})
	views := hatSql.NewMaterializedViews()
	query := "FROM CACHE('events') SELECT name"
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "event_names",
		Query:        query,
		Dependencies: []string{"events"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, hatSql.QueryOptions{ProjectionCatalog: views})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["name"] != "Ada" {
		t.Fatalf("fallback result = %#v", result.Rows)
	}
	if len(result.Plan) == 1 && result.Plan[0].Node == "PROJECTION HIT" {
		t.Fatalf("unversioned source served projection: %#v", result.Plan)
	}
}

func TestProjectionCatalogRejectsSourceChangeDuringBuild(t *testing.T) {
	resolver := &projectionSelectionResolver{
		rows:              []hatSql.Row{{"name": "Ada"}},
		version:           "1",
		changeVersionRead: true,
	}
	views := hatSql.NewMaterializedViews()
	_, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "event_names",
		Query:        "FROM CACHE('events') SELECT name",
		Dependencies: []string{"events"},
	}, resolver, hatSql.QueryOptions{})
	if err == nil {
		t.Fatal("Create() error = nil, want source-change rejection")
	}
}

func BenchmarkProjectionCatalog(b *testing.B) {
	resolver := &projectionSelectionResolver{
		rows: []hatSql.Row{
			{"name": "Ada"},
			{"name": "Lin"},
			{"name": "Bea"},
			{"name": "Cia"},
		},
		version: "1",
	}
	views := hatSql.NewMaterializedViews()
	query := "FROM CACHE('events') SELECT name"
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "event_names",
		Query:        query,
		Dependencies: []string{"events"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		b.Fatal(err)
	}
	b.Run("direct_query", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			if _, err := hatSql.ExecuteSQLQuery(query, resolver); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("projection_hit", func(b *testing.B) {
		options := hatSql.QueryOptions{ProjectionCatalog: views}
		b.ReportAllocs()
		for range b.N {
			if _, err := hatSql.ExecuteSQLQueryContext(context.Background(), query, resolver, options); err != nil {
				b.Fatal(err)
			}
		}
	})
}
