package hatSql

import "testing"

func BenchmarkTypedTableFieldUpdateReadUpsertBaseline(b *testing.B) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "events",
		Columns: []TypedTableColumn{{Name: "count", Kind: TypedTableInt64}, {Name: "label", Kind: TypedTableString}},
	})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := table.Upsert("event-0", []TypedTableValue{TypedInt64(0), TypedString("stable")}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		rows, err := table.ResolveSQLSource("CACHE", "events")
		if err != nil || len(rows) != 1 {
			b.Fatalf("ResolveSQLSource() = %d/%v", len(rows), err)
		}
		value, ok := rows[0]["count"].(int64)
		if !ok {
			b.Fatalf("count value = %#v", rows[0]["count"])
		}
		if _, err := table.Upsert("event-0", []TypedTableValue{TypedInt64(value + 1), TypedString("stable")}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedTableFieldUpdateDirect(b *testing.B) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "events",
		Columns: []TypedTableColumn{
			{Name: "count", Kind: TypedTableInt64},
			{Name: "label", Kind: TypedTableString},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := table.Upsert("event-0", []TypedTableValue{TypedInt64(0), TypedString("stable")}); err != nil {
		b.Fatal(err)
	}
	operations := []TypedTableUpdate{{Column: "count", Kind: TypedTableUpdateAdd, Value: TypedInt64(1)}}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := table.Update("event-0", operations); err != nil {
			b.Fatal(err)
		}
	}
}
