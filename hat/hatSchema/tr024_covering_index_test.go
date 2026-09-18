package hatSchema

import (
	"context"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestMaterializedSourceBuildsAndMaintainsCoveringIndex(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{
		{Name: "id", Identity: true},
		{Name: "region"},
		{Name: "name"},
		{Name: "secret"},
	})
	if _, err := source.Insert(Row{"region": "eu", "name": "Ada", "secret": "hidden"}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Insert(Row{"region": "us", "name": "Grace", "secret": "hidden-2"}); err != nil {
		t.Fatal(err)
	}

	report, err := source.BuildCoveringIndex("region", []string{"name"})
	if err != nil {
		t.Fatalf("BuildCoveringIndex() error = %v", err)
	}
	if report.Field != "region" || report.Rows != 2 || report.Attempts == 0 {
		t.Fatalf("BuildCoveringIndex() report = %#v", report)
	}
	if !source.HasIndex("region") || !source.HasCoveringIndex("region", []string{"region", "name"}) {
		t.Fatal("covering index was not installed")
	}
	if source.HasCoveringIndex("region", []string{"region", "secret"}) {
		t.Fatal("covering index claimed an uncovered field")
	}

	rows, available := source.LookupCovering("region", "eu", []string{"region", "name"})
	if !available || !reflect.DeepEqual(rows, []Row{{"region": "eu", "name": "Ada"}}) {
		t.Fatalf("LookupCovering() = %#v/%v", rows, available)
	}
	rows[0]["name"] = "mutated"
	rows, available = source.LookupCovering("region", "eu", []string{"name", "region"})
	if !available || rows[0]["name"] != "Ada" {
		t.Fatalf("covering lookup leaked mutable state: %#v/%v", rows, available)
	}

	superset := NewMaterializedSource([]DerivedColumn{{Name: "region"}, {Name: "name"}, {Name: "secret"}})
	if _, err := superset.Insert(Row{"region": "eu", "name": "Ada", "secret": "hidden"}); err != nil {
		t.Fatal(err)
	}
	if _, err := superset.BuildCoveringIndex("region", []string{"name", "secret"}); err != nil {
		t.Fatal(err)
	}
	rows, available = superset.LookupCovering("region", "eu", []string{"region", "name"})
	if !available || len(rows) != 1 || len(rows[0]) != 2 || rows[0]["secret"] != nil {
		t.Fatalf("covering lookup returned unrequested fields: %#v/%v", rows, available)
	}

	if _, err := source.Insert(Row{"region": "eu", "name": "Katherine", "secret": "hidden-3"}); err != nil {
		t.Fatal(err)
	}
	rows, available = source.LookupCovering("region", "eu", []string{"region", "name"})
	if !available || len(rows) != 2 || rows[1]["name"] != "Katherine" {
		t.Fatalf("covering index did not maintain insert: %#v/%v", rows, available)
	}
	fullRows := source.Lookup("region", "eu")
	if len(fullRows) != 2 || fullRows[0]["secret"] != "hidden" {
		t.Fatalf("covering index did not preserve regular lookup: %#v", fullRows)
	}
}

func TestMaterializedSourceSQLResolverUsesCoveringIndex(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{
		{Name: "id", Identity: true},
		{Name: "region"},
		{Name: "name"},
		{Name: "secret"},
	})
	if _, err := source.Insert(Row{"region": "eu", "name": "Ada", "secret": "hidden"}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.BuildCoveringIndex("region", []string{"name"}); err != nil {
		t.Fatal(err)
	}
	adapter := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"people": source}}
	rows, available, err := adapter.ResolveSQLCoveringSource("CACHE", "people", "region", "eu", []string{"region", "name"})
	if err != nil || !available || !reflect.DeepEqual(rows, []hatSql.Row{{"region": "eu", "name": "Ada"}}) {
		t.Fatalf("ResolveSQLCoveringSource() = %#v/%v/%v", rows, available, err)
	}
	if _, available, err := adapter.ResolveSQLCoveringSource("CACHE", "people", "region", "eu", []string{"region", "secret"}); err != nil || available {
		t.Fatalf("uncovered ResolveSQLCoveringSource() = %v/%v", available, err)
	}
}

func TestMaterializedSourceSQLQueryUsesCoveringIndex(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{
		{Name: "id", Identity: true},
		{Name: "region"},
		{Name: "name"},
		{Name: "secret"},
	})
	if _, err := source.Insert(Row{"region": "eu", "name": "Ada", "secret": "hidden"}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.BuildCoveringIndex("region", []string{"name"}); err != nil {
		t.Fatal(err)
	}
	adapter := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"people": source}}
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM CACHE('people') AS p WHERE p.region = 'eu' SELECT p.region, p.name", adapter, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	want := []hatSql.Row{{"region": "eu", "name": "Ada"}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("query rows = %#v, want %#v", result.Rows, want)
	}
}
