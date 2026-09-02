package hatSql

import (
	"strconv"
	"testing"
)

func BenchmarkTypedTablePlainWrite(b *testing.B) {
	table := newTypedTableMVCCBenchmarkTable(b, false)
	values := []TypedTableValue{TypedString("value"), TypedInt64(1)}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := table.Upsert("hot", values); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedTableMVCCWrite(b *testing.B) {
	table := newTypedTableMVCCBenchmarkTable(b, true)
	values := []TypedTableValue{TypedString("value"), TypedInt64(1)}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := table.Upsert("hot", values); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedTablePlainRows(b *testing.B) {
	table := newTypedTableMVCCBenchmarkTable(b, false)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = table.Rows()
	}
}

func BenchmarkTypedTableMVCCSnapshotRows(b *testing.B) {
	table := newTypedTableMVCCBenchmarkTable(b, true)
	for index := 0; index < 100; index++ {
		if _, err := table.Upsert("hot", []TypedTableValue{TypedString("value"), TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	snapshot, err := table.Snapshot()
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = snapshot.Rows()
	}
}

func newTypedTableMVCCBenchmarkTable(b *testing.B, enabled bool) *TypedTable {
	b.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name: "users",
		MVCC: TypedTableMVCCOptions{Enabled: enabled},
		Columns: []TypedTableColumn{
			{Name: "name", Kind: TypedTableString},
			{Name: "score", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 1000; index++ {
		if _, err := table.Upsert("row"+strconv.Itoa(index), []TypedTableValue{TypedString("value"), TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	return table
}
