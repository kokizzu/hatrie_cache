package hatSchema

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTT024CrossFieldTextIndexUsesBothFields(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "title"}, {Name: "body"}})
	for _, row := range []Row{
		{"id": int64(1), "title": "quick brown release", "body": "unrelated"},
		{"id": int64(2), "title": "unrelated", "body": "lazy red fox"},
		{"id": int64(3), "title": "quick release", "body": "unrelated"},
		{"id": int64(4), "title": "quick brown release", "body": "lazy fox"},
	} {
		if _, err := source.Insert(row); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.BuildTextIndex("title"); err != nil {
		t.Fatal(err)
	}
	if _, err := source.BuildTextIndex("body"); err != nil {
		t.Fatal(err)
	}
	adapter := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"docs": source}}
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM CACHE('docs') AS doc WHERE CONTAINS_PHRASE(doc.title, 'quick brown') OR CONTAINS_PROXIMITY(doc.body, 'lazy fox', 1) SELECT doc.id ORDER BY doc.id", adapter, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertTT024IDs(t, result.Rows, []int64{1, 2, 4})
}

func TestTT024CrossFieldTextIndexRequiresEveryFieldIndex(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "title"}, {Name: "body"}})
	if _, err := source.Insert(Row{"id": int64(1), "title": "quick brown", "body": "lazy fox"}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.BuildTextIndex("title"); err != nil {
		t.Fatal(err)
	}
	adapter := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"docs": source}}
	rows, available, err := adapter.ResolveSQLTextProximityMultiFieldUnionSource("CACHE", "docs", []hatSql.SQLTextProximityFieldQuery{
		{Field: "title", Query: hatSql.SQLTextProximityQuery{Query: "quick brown"}},
		{Field: "body", Query: hatSql.SQLTextProximityQuery{Query: "lazy fox"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if available || rows != nil {
		t.Fatalf("partial multi-field index = %#v/%t, want unavailable", rows, available)
	}
}
