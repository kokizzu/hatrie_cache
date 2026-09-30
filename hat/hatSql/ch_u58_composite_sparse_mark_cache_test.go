package hatSql_test

import (
	"context"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestTypedTableCompositeSparsePrimaryMarkCacheRetainsTupleMarks(t *testing.T) {
	table := newCHU58CompositeSparseMarkTable(t, 2)
	fields := []string{"tenant", "id"}
	if _, available, err := table.ResolveSQLColumnarSource("CACHE", "events", fields); err != nil || !available {
		t.Fatalf("ResolveSQLColumnarSource() = available %t, error %v", available, err)
	}
	batch, segments, available, err := table.BorrowSQLColumnarSourceSegments("CACHE", "events", fields)
	if err != nil || !available || segments == nil {
		t.Fatalf("BorrowSQLColumnarSourceSegments() = batch %#v, segments %#v, available %t, error %v", batch, segments, available, err)
	}
	if batch.Rows != 4 {
		t.Fatalf("batch rows = %d, want 4", batch.Rows)
	}
	if want := []string{"tenant", "id"}; !reflect.DeepEqual(segments.SparsePrimaryFields, want) {
		t.Fatalf("SparsePrimaryFields = %#v, want %#v", segments.SparsePrimaryFields, want)
	}
	if want := []float64{1, 1, 2, 1}; !reflect.DeepEqual(segments.SparsePrimaryTupleMinimum, want) {
		t.Fatalf("tuple minimum = %#v, want %#v", segments.SparsePrimaryTupleMinimum, want)
	}
	if want := []float64{1, 2, 2, 2}; !reflect.DeepEqual(segments.SparsePrimaryTupleMaximum, want) {
		t.Fatalf("tuple maximum = %#v, want %#v", segments.SparsePrimaryTupleMaximum, want)
	}

	result, err := hatSql.ExecuteQueryParameters(context.Background(), "FROM CACHE('events') AS e WHERE e.tenant = 2 AND e.id >= 2 SELECT e.tenant, e.id", table, nil, hatSql.QueryOptions{})
	if err != nil {
		t.Fatalf("composite sparse-mark query error = %v", err)
	}
	want := []hatSql.Row{{"tenant": int64(2), "id": int64(2)}}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("composite sparse-mark rows = %#v, want %#v", result.Rows, want)
	}
}

func newCHU58CompositeSparseMarkTable(t testing.TB, rowsPerTenant int) *hatSql.TypedTable {
	t.Helper()
	table, err := hatSql.NewTypedTable(hatSql.TypedTableSchema{
		Name: "events",
		Columns: []hatSql.TypedTableColumn{
			{Name: "tenant", Kind: hatSql.TypedTableInt64},
			{Name: "id", Kind: hatSql.TypedTableInt64},
		},
		ColumnarCache: hatSql.TypedTableColumnarCacheOptions{
			Enabled:                   true,
			MaxBytes:                  1,
			MinReads:                  1,
			RowsPerSegment:            rowsPerTenant,
			SparsePrimaryIndex:        true,
			SparsePrimaryFields:       []string{"tenant", "id"},
			SparsePrimaryMarkCache:    true,
			SparsePrimaryMarkMaxBytes: 1 << 20,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for tenant := int64(1); tenant <= 2; tenant++ {
		for id := int64(1); id <= int64(rowsPerTenant); id++ {
			key := string(rune('a' + (tenant-1)*int64(rowsPerTenant) + id - 1))
			if _, err := table.Upsert(key, []hatSql.TypedTableValue{hatSql.TypedInt64(tenant), hatSql.TypedInt64(id)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	return table
}
