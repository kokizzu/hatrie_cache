package hatSql

import "testing"

func BenchmarkMZ002TypedTableReadHoldLifecycle(b *testing.B) {
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
	hold, err := table.AcquireChangeReadHold(0)
	if err != nil {
		b.Fatal(err)
	}
	if err := hold.Release(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		hold, err := table.AcquireChangeReadHold(0)
		if err != nil {
			b.Fatal(err)
		}
		if err := hold.Release(); err != nil {
			b.Fatal(err)
		}
	}
}
