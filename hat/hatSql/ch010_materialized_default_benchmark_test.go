package hatSql

import "testing"

func BenchmarkCH010PlainUpsert(b *testing.B) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "plain-benchmark",
		Columns: []TypedTableColumn{
			{Name: "base", Kind: TypedTableInt64},
			{Name: "derived", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	values := []TypedTableValue{TypedInt64(0), TypedInt64(0)}
	benchmarkCH010Upserts(b, table, values)
}

func BenchmarkCH010MaterializedUpsert(b *testing.B) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "materialized-benchmark",
		Columns: []TypedTableColumn{
			{Name: "base", Kind: TypedTableInt64},
			{
				Name: "derived",
				Kind: TypedTableInt64,
				Generated: func(values []TypedTableValue) (TypedTableValue, error) {
					return TypedInt64(values[0].Int64 * 10), nil
				},
			},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	values := []TypedTableValue{TypedInt64(0), TypedInt64(0)}
	benchmarkCH010Upserts(b, table, values)
}

func BenchmarkCH010DefaultComputedUpsert(b *testing.B) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "default-computed-benchmark",
		Columns: []TypedTableColumn{
			{Name: "base", Kind: TypedTableInt64},
			{
				Name:          "derived",
				Kind:          TypedTableInt64,
				GeneratedMode: TypedTableGeneratedDefault,
				Generated: func(values []TypedTableValue) (TypedTableValue, error) {
					return TypedInt64(values[0].Int64 * 10), nil
				},
			},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	values := []TypedTableValue{TypedInt64(0), TypedNull()}
	benchmarkCH010Upserts(b, table, values)
}

func BenchmarkCH010DefaultPreservedUpsert(b *testing.B) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "default-preserved-benchmark",
		Columns: []TypedTableColumn{
			{Name: "base", Kind: TypedTableInt64},
			{
				Name:          "derived",
				Kind:          TypedTableInt64,
				GeneratedMode: TypedTableGeneratedDefault,
				Generated: func(values []TypedTableValue) (TypedTableValue, error) {
					return TypedInt64(values[0].Int64 * 10), nil
				},
			},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	values := []TypedTableValue{TypedInt64(0), TypedInt64(99)}
	benchmarkCH010Upserts(b, table, values)
}

func benchmarkCH010Upserts(b *testing.B, table *TypedTable, values []TypedTableValue) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		values[0] = TypedInt64(int64(index))
		if _, err := table.Upsert("row", values); err != nil {
			b.Fatal(err)
		}
	}
}
