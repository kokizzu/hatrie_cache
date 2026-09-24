package hatSchema

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTT024TextIndexIsOptInAndPreservesSQLPhraseSemantics(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id", Indexed: true}, {Name: "body"}})
	for _, row := range []Row{
		{"id": int64(1), "body": "alpha beta gamma"},
		{"id": int64(2), "body": "alpha unrelated beta gamma"},
		{"id": int64(3), "body": "beta alpha beta gamma"},
	} {
		if _, err := source.Insert(row); err != nil {
			t.Fatal(err)
		}
	}
	adapter := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"docs": source}}

	if source.HasTextIndex("body") {
		t.Fatal("text index must be disabled by default")
	}
	if rows, available, err := adapter.ResolveSQLTextProximitySource("CACHE", "docs", "body", "alpha beta", 0); err != nil || available || rows != nil {
		t.Fatalf("unbuilt text index = %#v/%t/%v, want unavailable", rows, available, err)
	}

	before, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM CACHE('docs') AS doc WHERE CONTAINS_PHRASE(doc.body, 'alpha beta') SELECT doc.id", adapter, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertTT024IDs(t, before.Rows, []int64{1, 3})

	report, err := source.BuildTextIndex("body")
	if err != nil {
		t.Fatal(err)
	}
	if report.Rows != 3 || report.Tokens == 0 || report.Attempts == 0 || !source.HasTextIndex("body") {
		t.Fatalf("text index report = %#v", report)
	}

	after, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM CACHE('docs') AS doc WHERE CONTAINS_PHRASE(doc.body, 'alpha beta') SELECT doc.id", adapter, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertTT024IDs(t, after.Rows, []int64{1, 3})

	proximity, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM CACHE('docs') AS doc WHERE CONTAINS_PROXIMITY(doc.body, 'alpha gamma', 1) SELECT doc.id", adapter, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertTT024IDs(t, proximity.Rows, []int64{1, 3})
}

func TestTT024TextIndexMaintainsInsertAndUpsert(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id", Indexed: true}, {Name: "body"}})
	if _, err := source.BuildUniqueIndex("id"); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Insert(Row{"id": int64(1), "body": "alpha beta"}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.BuildTextIndex("body"); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Insert(Row{"id": int64(2), "body": "delta epsilon"}); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Upsert(Row{"id": int64(1), "body": "delta epsilon"}, MaterializedUpsertOptions{
		ConflictField: "id",
		OnConflict:    func(_, incoming Row) (Row, error) { return incoming, nil },
	}); err != nil {
		t.Fatal(err)
	}
	adapter := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"docs": source}}
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM CACHE('docs') AS doc WHERE CONTAINS_PHRASE(doc.body, 'delta epsilon') SELECT doc.id", adapter, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertTT024IDs(t, result.Rows, []int64{1, 2})
}

func TestTT024TextIndexPreservesRepeatedTokenPositions(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "body"}})
	for _, row := range []Row{
		{"id": int64(1), "body": "alpha beta alpha"},
		{"id": int64(2), "body": "alpha alpha beta"},
		{"id": int64(3), "body": "beta alpha alpha"},
	} {
		if _, err := source.Insert(row); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.BuildTextIndex("body"); err != nil {
		t.Fatal(err)
	}
	assertTT024IDs(t, sqlRows(source.LookupText("body", "alpha alpha", 0)), []int64{2, 3})
	assertTT024IDs(t, sqlRows(source.LookupText("body", "alpha alpha", 1)), []int64{1, 2, 3})
	if _, err := source.BuildTextIndex("missing"); err == nil {
		t.Fatal("missing text-index field must be rejected")
	}
}

func TestTT024TextIndexBinaryPersistenceRoundTripAndValidation(t *testing.T) {
	rows := []Row{
		{"id": int64(1), "body": "alpha beta gamma"},
		{"id": int64(2), "body": "alpha unrelated beta gamma"},
		{"id": int64(3), "body": "beta alpha beta gamma"},
	}
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "body"}})
	for _, row := range rows {
		if _, err := source.Insert(row); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := source.BuildTextIndex("body"); err != nil {
		t.Fatal(err)
	}
	wire, err := source.MarshalTextIndex("body")
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) == 0 {
		t.Fatal("text index persistence frame is empty")
	}
	t.Logf("text index persistence frame bytes = %d", len(wire))

	restored := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "body"}})
	for _, row := range rows {
		if _, err := restored.Insert(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := restored.RestoreTextIndex("body", wire); err != nil {
		t.Fatal(err)
	}
	if !restored.HasTextIndex("body") {
		t.Fatal("restored text index is not active")
	}
	assertTT024IDs(t, sqlRows(restored.LookupText("body", "alpha beta", 0)), []int64{1, 3})
	if _, err := restored.Insert(Row{"id": int64(4), "body": "alpha beta"}); err != nil {
		t.Fatal(err)
	}
	assertTT024IDs(t, sqlRows(restored.LookupText("body", "alpha beta", 0)), []int64{1, 3, 4})

	corrupted := append([]byte(nil), wire...)
	corrupted[len(corrupted)-1] ^= 1
	if err := restored.RestoreTextIndex("body", corrupted); err == nil {
		t.Fatal("corrupted text index frame restored without error")
	}

	drifted := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "body"}})
	for _, row := range rows {
		if _, err := drifted.Insert(row); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := drifted.Insert(Row{"id": int64(4), "body": "new document"}); err != nil {
		t.Fatal(err)
	}
	if err := drifted.RestoreTextIndex("body", wire); err == nil {
		t.Fatal("text index for a different source snapshot restored without error")
	}
}

func assertTT024IDs(t *testing.T, rows []hatSql.Row, want []int64) {
	t.Helper()
	if len(rows) != len(want) {
		t.Fatalf("row count = %d, want %d (%#v)", len(rows), len(want), rows)
	}
	for index, row := range rows {
		id, ok := row["id"].(int64)
		if !ok || id != want[index] {
			t.Fatalf("row %d id = %#v, want %d", index, row["id"], want[index])
		}
	}
}
