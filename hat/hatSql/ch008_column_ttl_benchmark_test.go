package hatSql

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkCH008NoTTLRows(b *testing.B) {
	table := newCH008NoTTLBenchmarkTable(b)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = table.Rows()
	}
}

func BenchmarkCH008RowTTLRows(b *testing.B) {
	table := newCH008BenchmarkTable(b)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = table.Rows()
	}
}

func BenchmarkCH008ColumnTTLRows(b *testing.B) {
	now := time.Unix(1700000000, 0)
	table, err := NewTypedTable(TypedTableSchema{
		Name: "ch008-column-benchmark",
		Columns: []TypedTableColumn{
			{Name: "id", Kind: TypedTableInt64},
			{Name: "region", Kind: TypedTableString, TTL: TypedTableTTLOptions{
				Mode: TypedTableTTLProcessingTime, Lifetime: time.Hour, Clock: func() time.Time { return now },
			}},
			{Name: "amount", Kind: TypedTableFloat64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 4096; index++ {
		if _, err := table.Upsert(
			"key-"+strconv.Itoa(index),
			[]TypedTableValue{TypedInt64(int64(index)), TypedString("region"), TypedFloat64(float64(index))},
		); err != nil {
			b.Fatal(err)
		}
	}
	now = now.Add(2 * time.Hour)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = table.Rows()
	}
}

func newCH008BenchmarkTable(b *testing.B) *TypedTable {
	return newCH008BenchmarkTableWithRowTTL(b, true)
}

func newCH008NoTTLBenchmarkTable(b *testing.B) *TypedTable {
	return newCH008BenchmarkTableWithRowTTL(b, false)
}

func newCH008BenchmarkTableWithRowTTL(b *testing.B, rowTTL bool) *TypedTable {
	b.Helper()
	now := time.Unix(1700000000, 0)
	columns := []TypedTableColumn{
		{Name: "id", Kind: TypedTableInt64},
		{Name: "region", Kind: TypedTableString},
		{Name: "amount", Kind: TypedTableFloat64},
	}
	schema := TypedTableSchema{Name: "ch008-benchmark", Columns: columns}
	if rowTTL {
		schema.TTL = TypedTableTTLOptions{
			Mode:     TypedTableTTLProcessingTime,
			Lifetime: time.Hour,
			Clock:    func() time.Time { return now },
		}
	}
	table, err := NewTypedTable(schema)
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 4096; index++ {
		if _, err := table.Upsert(
			"key-"+strconv.Itoa(index),
			[]TypedTableValue{TypedInt64(int64(index)), TypedString("region"), TypedFloat64(float64(index))},
		); err != nil {
			b.Fatal(err)
		}
	}
	return table
}
