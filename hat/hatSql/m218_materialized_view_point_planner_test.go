package hatSql_test

import (
	"context"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type m218PointPlannerResolver struct {
	rows       []hatSql.Row
	version    string
	sourceRead int
}

func (resolver *m218PointPlannerResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if name != "CACHE" || key != "people" {
		return nil, nil
	}
	resolver.sourceRead++
	return hatSql.CloneRows(resolver.rows), nil
}

func (resolver *m218PointPlannerResolver) SQLSourceVersion(name, key string) (string, bool, error) {
	if name != "CACHE" || key != "people" {
		return "", false, nil
	}
	return resolver.version, true, nil
}

func TestM218MaterializedViewPointLookupPlannerUsesPosting(t *testing.T) {
	resolver := &m218PointPlannerResolver{
		rows: []hatSql.Row{
			{"id": int64(1), "region": "sg", "name": "Ada"},
			{"id": int64(2), "region": "jp", "name": "Kai"},
			{"id": int64(3), "region": "jp", "name": "Lin"},
			{"id": int64(4), "region": "jp", "name": "Mira"},
		},
		version: "1",
	}
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:              "people_projection",
		Query:             "FROM CACHE('people') AS p SELECT p.id, p.region, p.name",
		Dependencies:      []string{"people"},
		PointLookupFields: []string{"region"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}

	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM CACHE('people') AS p WHERE p.region = 'sg' SELECT p.id, p.region, p.name", resolver, hatSql.QueryOptions{ProjectionCatalog: views})
	if err != nil {
		t.Fatal(err)
	}
	want := []hatSql.Row{{"id": int64(1), "region": "sg", "name": "Ada"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("point-planned rows = %#v, want %#v", result.Rows, want)
	}
	if len(result.Plan) != 1 || result.Plan[0].Node != "MATERIALIZED POINT LOOKUP" {
		t.Fatalf("point planner plan = %#v, want materialized point lookup", result.Plan)
	}
	if resolver.sourceRead != 1 {
		t.Fatalf("source reads = %d, want only view creation read", resolver.sourceRead)
	}
}

func TestM218MaterializedViewPointLookupPlannerFallsBackWithoutPosting(t *testing.T) {
	resolver := &m218PointPlannerResolver{
		rows: []hatSql.Row{
			{"id": int64(1), "region": "sg"},
			{"id": int64(2), "region": "jp"},
		},
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

	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM CACHE('people') AS p WHERE p.region = 'sg' SELECT p.id, p.region", resolver, hatSql.QueryOptions{ProjectionCatalog: views})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["id"] != int64(1) {
		t.Fatalf("fallback rows = %#v, want one matching row", result.Rows)
	}
	if len(result.Plan) != 1 || result.Plan[0].Node != "MATERIALIZED ARRANGEMENT SCAN" {
		t.Fatalf("fallback plan = %#v, want materialized arrangement scan", result.Plan)
	}
	if resolver.sourceRead != 1 {
		t.Fatalf("source reads = %d, want only view creation read", resolver.sourceRead)
	}
}

func TestM218MaterializedViewPointLookupPlannerChoosesArrangementForDenseMatch(t *testing.T) {
	resolver := &m218PointPlannerResolver{
		rows: []hatSql.Row{
			{"id": int64(1), "region": "sg"},
			{"id": int64(2), "region": "sg"},
			{"id": int64(3), "region": "sg"},
		},
		version: "1",
	}
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:              "people_projection",
		Query:             "FROM CACHE('people') AS p SELECT p.id, p.region",
		Dependencies:      []string{"people"},
		PointLookupFields: []string{"region"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}

	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM CACHE('people') AS p WHERE p.region = 'sg' SELECT p.id, p.region", resolver, hatSql.QueryOptions{ProjectionCatalog: views})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != len(resolver.rows) {
		t.Fatalf("dense result rows = %#v, want %d rows", result.Rows, len(resolver.rows))
	}
	if len(result.Plan) != 1 || result.Plan[0].Node != "MATERIALIZED ARRANGEMENT SCAN" {
		t.Fatalf("dense planner plan = %#v, want materialized arrangement scan", result.Plan)
	}
	if resolver.sourceRead != 1 {
		t.Fatalf("source reads = %d, want only view creation read", resolver.sourceRead)
	}
}

func TestM218MaterializedViewPointLookupPlannerRejectsStaleSnapshot(t *testing.T) {
	resolver := &m218PointPlannerResolver{
		rows: []hatSql.Row{
			{"id": int64(1), "region": "sg"},
		},
		version: "1",
	}
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:              "people_projection",
		Query:             "FROM CACHE('people') AS p SELECT p.id, p.region",
		Dependencies:      []string{"people"},
		PointLookupFields: []string{"region"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	resolver.rows = []hatSql.Row{{"id": int64(2), "region": "sg"}}
	resolver.version = "2"

	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM CACHE('people') AS p WHERE p.region = 'sg' SELECT p.id, p.region", resolver, hatSql.QueryOptions{ProjectionCatalog: views})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["id"] != int64(2) {
		t.Fatalf("stale snapshot result = %#v, want refreshed source row", result.Rows)
	}
	if len(result.Plan) == 1 && (result.Plan[0].Node == "MATERIALIZED POINT LOOKUP" || result.Plan[0].Node == "MATERIALIZED ARRANGEMENT SCAN") {
		t.Fatalf("stale snapshot was served: %#v", result.Plan)
	}
	if resolver.sourceRead != 2 {
		t.Fatalf("source reads = %d, want view creation plus stale fallback query", resolver.sourceRead)
	}
}
