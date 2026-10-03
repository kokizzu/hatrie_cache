package hatSchema

import (
	"context"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestT026NamedIndexHintSelectsSpecificMaterializedIndex(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{
		{Name: "id"},
		{Name: "region"},
	})
	for _, row := range []Row{
		{"id": int64(1), "region": "sg"},
		{"id": int64(2), "region": "id"},
		{"id": int64(3), "region": "sg"},
	} {
		if _, err := source.Insert(row); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.BuildFunctionalIndex("region_copy", []string{"region"}, func(row Row) (interface{}, error) {
		return row["region"], nil
	}); err != nil {
		t.Fatal(err)
	}
	resolver := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"people": source}}
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), `
FROM CACHE('people') AS person
WHERE person.region = 'sg'
SELECT person.id ORDER BY person.id`, resolver, hatSql.SQLQueryOptions{
		IndexHint: hatSql.SQLIndexHint{
			Source: "person",
			Field:  "region",
			Index:  "region_copy",
			Mode:   hatSql.SQLIndexHintForce,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []hatSql.Row{{"id": int64(1)}, {"id": int64(3)}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("named forced-index rows = %#v, want %#v", result.Rows, want)
	}
}

func TestT026NamedIndexHintRejectsConditionalFunctionalIndex(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{
		{Name: "id"},
		{Name: "region"},
		{Name: "active"},
	})
	for _, row := range []Row{
		{"id": int64(1), "region": "sg", "active": true},
		{"id": int64(2), "region": "sg", "active": false},
	} {
		if _, err := source.Insert(row); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.BuildConditionalFunctionalIndex("active_region", []string{"region", "active"}, ConditionalFunctionalIndexOptions{
		Predicate: "active = true",
		Matches: func(row Row) (bool, error) {
			active, _ := row["active"].(bool)
			return active, nil
		},
	}, func(row Row) (interface{}, error) {
		return row["region"], nil
	}); err != nil {
		t.Fatal(err)
	}
	resolver := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"people": source}}
	_, err := hatSql.ExecuteSQLQueryContext(context.Background(), `
FROM CACHE('people') AS person
WHERE person.region = 'sg'
SELECT person.id`, resolver, hatSql.SQLQueryOptions{
		IndexHint: hatSql.SQLIndexHint{Source: "person", Field: "region", Index: "active_region", Mode: hatSql.SQLIndexHintForce},
	})
	if err == nil || err.Error() != `SQL named index "active_region" is unavailable` {
		t.Fatalf("conditional named hint error = %v", err)
	}
}

func TestT026NamedIndexHintRequiresMode(t *testing.T) {
	_, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM VALUES (1) AS values(region) SELECT region", nil, hatSql.SQLQueryOptions{
		IndexHint: hatSql.SQLIndexHint{Field: "region", Index: "region_copy"},
	})
	if err == nil || err.Error() != "SQL named index hint requires FORCE or FORBID" {
		t.Fatalf("named hint validation error = %v", err)
	}
}
