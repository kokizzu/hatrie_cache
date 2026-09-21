package hatSql

import (
	"strconv"
	"testing"
)

var m211FrontierBoundsBaselineSink int

func BenchmarkM211TypedTableSnapshotAtBaseline(b *testing.B) {
	table := newM211BenchmarkTable(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		snapshot, err := table.SnapshotAt(1)
		if err != nil {
			b.Fatal(err)
		}
		m211FrontierBoundsBaselineSink = len(snapshot.keys)
	}
}

func newM211BenchmarkTable(b *testing.B) *TypedTable {
	b.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name: "items",
		MVCC: TypedTableMVCCOptions{Enabled: true},
		Columns: []TypedTableColumn{
			{Name: "value", Kind: TypedTableString},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 256; index++ {
		if _, err := table.Upsert("item"+strconv.Itoa(index), []TypedTableValue{TypedString("value")}); err != nil {
			b.Fatal(err)
		}
	}
	return table
}
