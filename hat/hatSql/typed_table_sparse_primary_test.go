package hatSql

import "testing"

func TestTypedTableColumnarSparsePrimaryIndexRequiresOrderedValues(t *testing.T) {
	t.Parallel()
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "id", Kind: TypedTableInt64}},
		ColumnarCache: TypedTableColumnarCacheOptions{
			Enabled:            true,
			MinReads:           2,
			RowsPerSegment:     2,
			SparsePrimaryIndex: true,
			SparsePrimaryField: "id",
		},
	})
	if err != nil {
		t.Fatalf("NewTypedTable() error = %v", err)
	}
	for index, value := range []int64{1, 2, 3, 4} {
		if _, err := table.Upsert(string(rune('a'+index)), []TypedTableValue{TypedInt64(value)}); err != nil {
			t.Fatalf("Upsert() error = %v", err)
		}
	}
	fields := []string{"id"}
	for read := 0; read < 2; read++ {
		if _, found, err := table.ResolveSQLColumnarSource("CACHE", "events", fields); err != nil || !found {
			t.Fatalf("ResolveSQLColumnarSource() = found %t, error %v; want found", found, err)
		}
	}
	_, segments, found, err := table.BorrowSQLColumnarSourceSegments("CACHE", "events", fields)
	if err != nil || !found {
		t.Fatalf("BorrowSQLColumnarSourceSegments() = found %t, error %v; want found", found, err)
	}
	if segments == nil || segments.SparsePrimaryField != "id" {
		t.Fatalf("SparsePrimaryField = %#v, want id", segments)
	}

	if _, err := table.Upsert("b", []TypedTableValue{TypedInt64(-1)}); err != nil {
		t.Fatalf("out-of-order Upsert() error = %v", err)
	}
	for read := 0; read < 2; read++ {
		if _, found, err := table.ResolveSQLColumnarSource("CACHE", "events", fields); err != nil || !found {
			t.Fatalf("ResolveSQLColumnarSource() after update = found %t, error %v; want found", found, err)
		}
	}
	_, segments, found, err = table.BorrowSQLColumnarSourceSegments("CACHE", "events", fields)
	if err != nil || !found {
		t.Fatalf("BorrowSQLColumnarSourceSegments() after update = found %t, error %v; want found", found, err)
	}
	if segments == nil || segments.SparsePrimaryField != "" {
		t.Fatalf("SparsePrimaryField after out-of-order update = %#v, want empty", segments)
	}
}

func TestTypedTableColumnarSparsePrimaryIndexRejectsNulls(t *testing.T) {
	t.Parallel()
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "id", Kind: TypedTableInt64}},
		ColumnarCache: TypedTableColumnarCacheOptions{
			Enabled:            true,
			MinReads:           2,
			RowsPerSegment:     2,
			SparsePrimaryIndex: true,
			SparsePrimaryField: "id",
		},
	})
	if err != nil {
		t.Fatalf("NewTypedTable() error = %v", err)
	}
	for index, value := range []TypedTableValue{TypedInt64(1), TypedNull(), TypedInt64(3), TypedInt64(4)} {
		if _, err := table.Upsert(string(rune('a'+index)), []TypedTableValue{value}); err != nil {
			t.Fatalf("Upsert() error = %v", err)
		}
	}
	fields := []string{"id"}
	for read := 0; read < 2; read++ {
		if _, _, err := table.ResolveSQLColumnarSource("CACHE", "events", fields); err != nil {
			t.Fatalf("ResolveSQLColumnarSource() error = %v", err)
		}
	}
	_, segments, found, err := table.BorrowSQLColumnarSourceSegments("CACHE", "events", fields)
	if err != nil || !found {
		t.Fatalf("BorrowSQLColumnarSourceSegments() = found %t, error %v; want found", found, err)
	}
	if segments == nil || segments.SparsePrimaryField != "" {
		t.Fatalf("SparsePrimaryField with NULL = %#v, want empty", segments)
	}
}
