package hatSql

import "testing"

var chu24TypedTableMemoryBudgetBenchmarkSink int64

func BenchmarkCHU24TypedTableUpsertDisabled(b *testing.B) {
	benchmarkCHU24TypedTableUpsert(b, 0)
}

func BenchmarkCHU24TypedTableUpsertEnabled(b *testing.B) {
	benchmarkCHU24TypedTableUpsert(b, 1<<20)
}

func benchmarkCHU24TypedTableUpsert(b *testing.B, maxBytes int64) {
	b.Helper()
	schema := TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableString}},
	}
	if maxBytes > 0 {
		schema.MemoryBudget = TypedTableMemoryBudgetOptions{MaxBytes: maxBytes}
	}
	table, err := NewTypedTable(schema)
	if err != nil {
		b.Fatal(err)
	}
	values := []TypedTableValue{TypedString("stable-value")}
	if _, err := table.Upsert("row", values); err != nil {
		b.Fatal(err)
	}
	table.changes = table.changes[:0]
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := table.Upsert("row", values); err != nil {
			b.Fatal(err)
		}
		table.changes = table.changes[:0]
	}
	b.StopTimer()
	chu24TypedTableMemoryBudgetBenchmarkSink = table.MemoryUsage().UsedBytes
}
