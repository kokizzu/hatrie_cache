package hatSql_test

import (
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type mu039LayoutResolver struct {
	rows   []hatSql.Row
	layout hatSql.SQLSourceLayout
}

func (resolver *mu039LayoutResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if name != "CACHE" || key != "events" {
		return nil, nil
	}
	return resolver.rows, nil
}

func (resolver *mu039LayoutResolver) ResolveSQLSourceLayout(name, key string) (hatSql.SQLSourceLayout, bool, error) {
	if name != "CACHE" || key != "events" {
		return hatSql.SQLSourceLayout{}, false, nil
	}
	return resolver.layout, true, nil
}

func TestMU039PartitionOrderDeclarationIsVisibleInExplain(t *testing.T) {
	resolver := &mu039LayoutResolver{
		rows: []hatSql.Row{{"region": "apac", "created_at": int64(2)}},
		layout: hatSql.SQLSourceLayout{
			PartitionBy: []string{"region"},
			OrderBy:     []hatSql.SQLSourceOrder{{Field: "created_at", Descending: true}},
		},
	}

	result, err := hatSql.ExecuteSQLQuery("EXPLAIN FROM CACHE('events') SELECT region, created_at ORDER BY created_at DESC", resolver)
	if err != nil {
		t.Fatalf("EXPLAIN error = %v", err)
	}
	if len(result.Plan) == 0 || len(result.Plan[0].Arrangements) != 1 || result.Plan[0].Arrangements[0].Layout == nil {
		t.Fatalf("EXPLAIN plan = %#v, want source layout arrangement", result.Plan)
	}
	want := &hatSql.SQLSourceLayout{
		PartitionBy: []string{"region"},
		OrderBy:     []hatSql.SQLSourceOrder{{Field: "created_at", Descending: true}},
	}
	if !reflect.DeepEqual(result.Plan[0].Arrangements[0].Layout, want) {
		t.Fatalf("EXPLAIN layout = %#v, want %#v", result.Plan[0].Arrangements[0].Layout, want)
	}
	if len(result.Columns) == 0 || result.Columns[len(result.Columns)-1] != "arrangements" {
		t.Fatalf("EXPLAIN columns = %#v, want arrangements column", result.Columns)
	}
	if _, ok := result.Rows[0]["arrangements"]; !ok {
		t.Fatalf("EXPLAIN row = %#v, want layout arrangement", result.Rows[0])
	}

	result.Plan[0].Arrangements[0].Layout.PartitionBy[0] = "mutated"
	again, err := hatSql.ExecuteSQLQuery("EXPLAIN FROM CACHE('events') SELECT region, created_at ORDER BY created_at DESC", resolver)
	if err != nil {
		t.Fatalf("second EXPLAIN error = %v", err)
	}
	if again.Plan[0].Arrangements[0].Layout.PartitionBy[0] != "region" {
		t.Fatalf("resolver layout was mutated through EXPLAIN result: %#v", again.Plan[0].Arrangements)
	}
}

func TestMU039CatalogResolverForwardsPartitionOrderDeclaration(t *testing.T) {
	resolver := &mu039LayoutResolver{
		layout: hatSql.SQLSourceLayout{PartitionBy: []string{"tenant"}},
	}
	result, err := hatSql.ExecuteSQLQuery("EXPLAIN FROM CACHE('events') SELECT tenant", hatSql.CatalogResolver{Source: resolver})
	if err != nil {
		t.Fatalf("CatalogResolver EXPLAIN error = %v", err)
	}
	if len(result.Plan) == 0 || len(result.Plan[0].Arrangements) != 1 || result.Plan[0].Arrangements[0].Layout == nil || !reflect.DeepEqual(result.Plan[0].Arrangements[0].Layout.PartitionBy, []string{"tenant"}) {
		t.Fatalf("CatalogResolver layout = %#v, want tenant partition", result.Plan)
	}
}

func TestMU039InvalidLayoutIsIgnoredWithoutBreakingExplain(t *testing.T) {
	resolver := &mu039LayoutResolver{
		layout: hatSql.SQLSourceLayout{
			PartitionBy: []string{"region", "REGION"},
		},
	}
	result, err := hatSql.ExecuteSQLQuery("EXPLAIN FROM CACHE('events') SELECT region", resolver)
	if err != nil {
		t.Fatalf("invalid-layout EXPLAIN error = %v", err)
	}
	if len(result.Plan) == 0 || len(result.Plan[0].Arrangements) != 0 {
		t.Fatalf("invalid layout was exposed: %#v", result.Plan)
	}
}

func TestMU039SessionPreservesLocalSourcePrecedence(t *testing.T) {
	resolver := &mu039LayoutResolver{layout: hatSql.SQLSourceLayout{PartitionBy: []string{"region"}}}
	session := hatSql.NewSQLSession(resolver)
	if err := session.CreateTemporaryTable("events", []hatSql.Row{{"region": "local"}}); err != nil {
		t.Fatalf("CreateTemporaryTable() error = %v", err)
	}
	result, err := hatSql.ExecuteSQLQuery("EXPLAIN FROM CACHE('events') SELECT region", session)
	if err != nil {
		t.Fatalf("session EXPLAIN error = %v", err)
	}
	if len(result.Plan) == 0 || len(result.Plan[0].Arrangements) != 0 {
		t.Fatalf("session leaked external layout over local table: %#v", result.Plan)
	}
}
