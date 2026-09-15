package hatSql

import "testing"

func BenchmarkMZ002TypedTableCompactNoHold(b *testing.B) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
	})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := table.Upsert("a", []TypedTableValue{TypedInt64(1)}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := table.CompactChangesThrough(0); err != nil {
			b.Fatal(err)
		}
	}
}
