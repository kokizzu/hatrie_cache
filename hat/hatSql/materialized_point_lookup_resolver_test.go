package hatSql_test

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestM218MaterializedPointLookupResolverSelectsPointPlan(t *testing.T) {
	rows := []hatSql.Row{
		{"id": int64(1), "name": "Ada"},
		{"id": int64(2), "name": "Lin"},
		{"id": int64(3), "name": "Mira"},
	}
	resolver := hatSql.SourceResolverFunc(func(name, key string) ([]hatSql.Row, error) {
		if name == "CACHE" && key == "people" {
			return hatSql.CloneRows(rows), nil
		}
		return nil, nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT id, name",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := views.CreatePointLookupIndex(hatSql.MaterializedViewPointLookupDefinition{
		Name:     "people_by_id",
		ViewName: "people_view",
		Key: func(row hatSql.Row) (string, error) {
			id, ok := row["id"].(int64)
			if !ok {
				return "", fmt.Errorf("id is not int64")
			}
			return strconv.FormatInt(id, 10), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	pointResolver, err := hatSql.NewMaterializedViewPointLookupResolver(views, hatSql.MaterializedViewPointLookupSourceDefinition{
		IndexName:    "people_by_id",
		SourceKey:    "people_view",
		Field:        "id",
		ValueKey:     m218PointLookupValueKey,
		CollectStats: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := hatSql.ExecuteQueryParameters(context.Background(), `
FROM EXTERNAL('people_view') AS person
WHERE person.id = 2
SELECT person.id, person.name`, pointResolver, nil, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []hatSql.Row{{"id": int64(2), "name": "Lin"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("point lookup rows = %#v, want %#v", result.Rows, want)
	}

	result, err = hatSql.ExecuteQueryParameters(context.Background(), `
FROM EXTERNAL('people_view') AS person
WHERE person.id = 99
SELECT person.id, person.name`, pointResolver, nil, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 0 {
		t.Fatalf("point lookup miss rows = %#v, want empty", result.Rows)
	}

	result, err = hatSql.ExecuteQueryParameters(context.Background(), `
FROM EXTERNAL('people_view') AS person
WHERE person.id > 1
SELECT person.id, person.name`, pointResolver, nil, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []hatSql.Row{{"id": int64(2), "name": "Lin"}, {"id": int64(3), "name": "Mira"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("scan fallback rows = %#v, want %#v", result.Rows, want)
	}

	rows = []hatSql.Row{
		{"id": int64(4), "name": "Noa"},
	}
	if _, err := views.RefreshChanged(context.Background(), []string{"people"}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	result, err = hatSql.ExecuteQueryParameters(context.Background(), `
FROM EXTERNAL('people_view') AS person
WHERE person.id = 4
SELECT person.id, person.name`, pointResolver, nil, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []hatSql.Row{{"id": int64(4), "name": "Noa"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("refreshed point lookup rows = %#v, want %#v", result.Rows, want)
	}

	result, err = hatSql.ExecuteQueryParameters(context.Background(), `
FROM EXTERNAL('people_view') AS person
WHERE person.id = '4'
SELECT person.id, person.name`, pointResolver, nil, hatSql.QueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []hatSql.Row{{"id": int64(4), "name": "Noa"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("unsupported point lookup value rows = %#v, want %#v", result.Rows, want)
	}

	stats := pointResolver.Stats()
	if stats.PointLookups != 4 || stats.PointLookupHits != 2 || stats.PointLookupMisses != 1 || stats.SourceScans != 2 || stats.LookupFallbacks != 1 {
		t.Fatalf("planner stats = %#v, want four point probes (two hits), two scans, one fallback", stats)
	}
}

func m218PointLookupValueKey(value interface{}) (string, error) {
	id, ok := value.(int64)
	if !ok {
		return "", fmt.Errorf("point lookup value %T is not int64", value)
	}
	return strconv.FormatInt(id, 10), nil
}
