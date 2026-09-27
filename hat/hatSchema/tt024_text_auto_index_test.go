package hatSchema

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTT024MaterializedTextIndexAutoSelection(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "body"}})
	for _, row := range []Row{
		{"id": int64(1), "body": "alpha beta gamma"},
		{"id": int64(2), "body": "gamma alpha"},
		{"id": int64(3), "body": "alpha only"},
	} {
		if _, err := source.Insert(row); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.BuildTextIndex("body"); err != nil {
		t.Fatal(err)
	}

	adapter := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"docs": source}}
	indexRows, available, err := adapter.ResolveSQLTextSource("CACHE", "docs", "body", "alpha gamma")
	if err != nil || !available || len(indexRows) != 2 {
		t.Fatalf("adapter text lookup rows/availability/error = %#v/%t/%v", indexRows, available, err)
	}
	recorder := hatSql.NewSQLIndexUseRecorder()
	result, err := hatSql.ExecuteSQLQueryContext(
		context.Background(),
		"FROM CACHE('docs') AS doc WHERE CONTAINS(doc.body, 'alpha gamma') SELECT doc.id ORDER BY doc.id",
		adapter,
		hatSql.SQLQueryOptions{IndexUseRecorder: recorder},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 2 || result.Rows[0]["id"] != int64(1) || result.Rows[1]["id"] != int64(2) {
		t.Fatalf("CONTAINS rows = %#v, want ids 1 and 2", result.Rows)
	}
	report := recorder.Report([]hatSql.SQLIndexDefinition{{Key: "docs", Field: "body", Kind: "text"}})
	if len(report) != 1 || !report[0].Used {
		t.Fatalf("text index report = %#v, plan = %#v, want the automatic text index selected", report, result.Plan)
	}
}

func TestTT024MaterializedTextIndexFallsBackForEmptyQuery(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "body"}})
	if _, err := source.Insert(Row{"id": int64(1), "body": "alpha"}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.BuildTextIndex("body"); err != nil {
		t.Fatal(err)
	}

	adapter := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"docs": source}}
	_, available, err := adapter.ResolveSQLTextSource("CACHE", "docs", "body", "")
	if err != nil {
		t.Fatal(err)
	}
	if available {
		t.Fatal("empty CONTAINS query must fall back to the full predicate evaluator")
	}
}
