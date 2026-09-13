package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableSparsePrimaryMarkCacheReusesMarksAfterDataEviction(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "events",
		Columns: []hatSql.TypedTableColumn{{Name: "id", Kind: hatSql.TypedTableInt64}},
		ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
			Enabled:                   true,
			MaxBytes:                  1,
			MinReads:                  1,
			RowsPerSegment:            2,
			SparsePrimaryIndex:        true,
			SparsePrimaryField:        "id",
			SparsePrimaryMarkCache:    true,
			SparsePrimaryMarkMaxBytes: 1024,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index, value := range []int64{1, 2, 3, 4} {
		if _, err := table.Upsert(string(rune('a'+index)), []hatSql.TypedTableValue{hatSql.TypedInt64(value)}); err != nil {
			t.Fatal(err)
		}
	}
	fields := []string{"id"}
	if _, available, err := table.ResolveSQLColumnarSource("CACHE", "events", fields); err != nil || !available {
		t.Fatalf("ResolveSQLColumnarSource() = available %t, error %v", available, err)
	}
	batch, segments, available, err := table.BorrowSQLColumnarSourceSegments("CACHE", "events", fields)
	if err != nil || !available || segments == nil {
		t.Fatalf("BorrowSQLColumnarSourceSegments() = batch %#v, segments %#v, available %t, error %v; want retained marks", batch, segments, available, err)
	}
	if batch.Rows != 4 || segments.RowsPerSegment != 2 || segments.SparsePrimaryField != "id" || len(segments.Columns["id"]) != 2 {
		t.Fatalf("retained sparse marks = batch %#v, segments %#v; want four rows and two ordered marks", batch, segments)
	}

	if _, err := table.Upsert("b", []hatSql.TypedTableValue{hatSql.TypedInt64(-1)}); err != nil {
		t.Fatal(err)
	}
	if _, _, available, err := table.BorrowSQLColumnarSourceSegments("CACHE", "events", fields); err != nil || available {
		t.Fatalf("BorrowSQLColumnarSourceSegments() after mutation = available %t, error %v; want invalidated marks", available, err)
	}
	if _, available, err := table.ResolveSQLColumnarSource("CACHE", "events", fields); err != nil || !available {
		t.Fatalf("ResolveSQLColumnarSource() after mutation = available %t, error %v", available, err)
	}
	if _, _, available, err := table.BorrowSQLColumnarSourceSegments("CACHE", "events", fields); err != nil || available {
		t.Fatalf("BorrowSQLColumnarSourceSegments() after unordered rebuild = available %t, error %v; want no unsafe primary marks", available, err)
	}

	result, err := hatSql.ExecuteQueryParameters(context.Background(), "FROM CACHE('events') SELECT id WHERE id >= 1 AND id <= 4", table, nil, hatSql.QueryOptions{})
	if err != nil || len(result.Rows) != 3 {
		t.Fatalf("range query after mark invalidation = %#v, error %v; want three rows", result.Rows, err)
	}
}

func TestTypedTableSparsePrimaryMarkCacheIsOptIn(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name:    "events-disabled",
		Columns: []hatSql.TypedTableColumn{{Name: "id", Kind: hatSql.TypedTableInt64}},
		ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
			Enabled:            true,
			MaxBytes:           1,
			MinReads:           1,
			RowsPerSegment:     2,
			SparsePrimaryIndex: true,
			SparsePrimaryField: "id",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index, value := range []int64{1, 2, 3, 4} {
		if _, err := table.Upsert(string(rune('a'+index)), []hatSql.TypedTableValue{hatSql.TypedInt64(value)}); err != nil {
			t.Fatal(err)
		}
	}
	fields := []string{"id"}
	if _, _, err := table.ResolveSQLColumnarSource("CACHE", "events-disabled", fields); err != nil {
		t.Fatal(err)
	}
	if _, _, available, err := table.BorrowSQLColumnarSourceSegments("CACHE", "events-disabled", fields); err != nil || available {
		t.Fatalf("default mark cache = available %t, error %v; want disabled", available, err)
	}
}

func TestTypedTableSparsePrimaryMarkCacheEvictsLeastRecentlyUsed(t *testing.T) {
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "events-lru",
		Columns: []hatSql.TypedTableColumn{
			{Name: "id", Kind: hatSql.TypedTableInt64},
			{Name: "value", Kind: hatSql.TypedTableInt64},
		},
		ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
			Enabled:                   true,
			MaxBytes:                  1,
			MinReads:                  1,
			RowsPerSegment:            2,
			SparsePrimaryIndex:        true,
			SparsePrimaryField:        "id",
			SparsePrimaryMarkCache:    true,
			SparsePrimaryMarkMaxBytes: 228,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index, value := range []int64{1, 2, 3, 4} {
		if _, err := table.Upsert(string(rune('a'+index)), []hatSql.TypedTableValue{hatSql.TypedInt64(value), hatSql.TypedInt64(value * 10)}); err != nil {
			t.Fatal(err)
		}
	}
	fieldsA := []string{"id"}
	fieldsB := []string{"id", "value"}
	fieldsC := []string{"value", "id"}
	for _, fields := range [][]string{fieldsA, fieldsB} {
		if _, available, err := table.ResolveSQLColumnarSource("CACHE", "events-lru", fields); err != nil || !available {
			t.Fatalf("ResolveSQLColumnarSource(%v) = available %t, error %v", fields, available, err)
		}
	}
	if _, _, available, err := table.BorrowSQLColumnarSourceSegments("CACHE", "events-lru", fieldsA); err != nil || !available {
		t.Fatalf("touching fields A = available %t, error %v; want an LRU hit", available, err)
	}
	if _, available, err := table.ResolveSQLColumnarSource("CACHE", "events-lru", fieldsC); err != nil || !available {
		t.Fatalf("ResolveSQLColumnarSource(%v) = available %t, error %v", fieldsC, available, err)
	}
	if _, _, available, err := table.BorrowSQLColumnarSourceSegments("CACHE", "events-lru", fieldsA); err != nil || !available {
		t.Fatalf("fields A after eviction = available %t, error %v; want recently used mark retained", available, err)
	}
	if _, _, available, err := table.BorrowSQLColumnarSourceSegments("CACHE", "events-lru", fieldsB); err != nil || available {
		t.Fatalf("fields B after eviction = available %t, error %v; want least-recently-used mark evicted", available, err)
	}
	if _, _, available, err := table.BorrowSQLColumnarSourceSegments("CACHE", "events-lru", fieldsC); err != nil || !available {
		t.Fatalf("fields C after insertion = available %t, error %v; want newest mark retained", available, err)
	}
}
