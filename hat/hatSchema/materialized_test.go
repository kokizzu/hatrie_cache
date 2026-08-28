package hatSchema

import (
	"context"
	"hatrie_cache/hat/hatSql"
	"testing"
)

func TestMaterializedSourceAppliesDerivedValuesAndIndexes(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{
		{Name: "id", Identity: true, Indexed: true},
		{Name: "first", Default: "Ada"},
		{Name: "last", Default: "Lovelace"},
		{Name: "full", Generated: func(row Row) (interface{}, error) { return row["first"].(string) + " " + row["last"].(string), nil }, Indexed: true},
	})
	row, err := source.Insert(Row{})
	if err != nil || row["id"] != int64(1) || row["full"] != "Ada Lovelace" {
		t.Fatalf("Insert() = %#v, %v", row, err)
	}
	rows := source.Lookup("full", "Ada Lovelace")
	if len(rows) != 1 || rows[0]["id"] != int64(1) {
		t.Fatalf("Lookup() = %#v", rows)
	}
}

func TestMaterializedSourceSQLResolverUsesGeneratedIndex(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id", Identity: true}, {Name: "name", Default: "Ada"}, {Name: "key", Generated: func(row Row) (interface{}, error) { return row["name"].(string) + "-1", nil }, Indexed: true}})
	if _, err := source.Insert(Row{}); err != nil {
		t.Fatal(err)
	}
	resolver := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"people": source}}
	result, err := hatSql.ExecuteSQLQueryParameters(context.Background(), `FROM CACHE('people') WHERE key = 'Ada-1' SELECT id`, resolver, nil, hatSql.SQLQueryOptions{})
	if err != nil || len(result.Rows) != 1 || result.Rows[0]["id"] != int64(1) {
		t.Fatalf("SQL query = %#v, %v", result, err)
	}
}

func TestMaterializedSourceNamedSequence(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id", Sequence: "orders"}})
	first, err := source.Insert(Row{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := source.Insert(Row{})
	if err != nil || first["id"] != int64(1) || second["id"] != int64(2) {
		t.Fatalf("sequence rows = %#v %#v, %v", first, second, err)
	}
}
