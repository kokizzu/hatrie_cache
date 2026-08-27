package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestParameterizedViewsCacheByArgumentsAndInvalidateDependencies(t *testing.T) {
	resolver := &testParameterizedViewResolver{rows: []hatSql.SQLRow{{"id": int64(1)}, {"id": int64(2)}}}
	views := hatSql.NewParameterizedViews()
	if err := views.Create(hatSql.ParameterizedViewDefinition{Name: "recent_orders", Query: `FROM CACHE($1) AS src WHERE src.id >= $2 SELECT src.id ORDER BY src.id`, Dependencies: []string{"orders"}}); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	first, err := views.Query(context.Background(), "recent_orders", []interface{}{"orders", int64(2)}, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if len(first.Rows) != 1 || first.Rows[0]["id"] != int64(2) {
		t.Fatalf("first result = %#v, want id 2", first.Rows)
	}
	resolver.rows = append(resolver.rows, hatSql.SQLRow{"id": int64(3)})
	cached, err := views.Query(context.Background(), "recent_orders", []interface{}{"orders", int64(2)}, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatalf("cached Query() error = %v", err)
	}
	if len(cached.Rows) != 1 {
		t.Fatalf("cached result = %#v, want unchanged snapshot", cached.Rows)
	}
	invalidated := views.Invalidate([]string{"orders"})
	if len(invalidated) != 1 || invalidated[0] != "recent_orders" {
		t.Fatalf("Invalidate() = %#v, want recent_orders", invalidated)
	}
	refreshed, err := views.Query(context.Background(), "recent_orders", []interface{}{"orders", int64(2)}, resolver, hatSql.QueryOptions{})
	if err != nil {
		t.Fatalf("refreshed Query() error = %v", err)
	}
	if len(refreshed.Rows) != 2 || refreshed.Rows[1]["id"] != int64(3) {
		t.Fatalf("refreshed result = %#v, want ids 2 and 3", refreshed.Rows)
	}
}

type testParameterizedViewResolver struct{ rows []hatSql.SQLRow }

func (resolver *testParameterizedViewResolver) ResolveSQLSource(name, key string) ([]hatSql.SQLRow, error) {
	if name != "CACHE" || key != "orders" {
		return nil, nil
	}
	return hatSql.CloneRows(resolver.rows), nil
}
