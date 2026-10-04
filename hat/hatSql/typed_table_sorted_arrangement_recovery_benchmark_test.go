package hatSql

import (
	"fmt"
	"testing"
)

func BenchmarkTypedTableSortedArrangementCheckpointCapture(b *testing.B) {
	arrangement, _, _ := benchmarkTypedTableSortedArrangementCheckpointFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := arrangement.CaptureCheckpoint(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedTableSortedArrangementCheckpointRestore(b *testing.B) {
	arrangement, _, checkpoint := benchmarkTypedTableSortedArrangementCheckpointFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := arrangement.RestoreCheckpoint(checkpoint); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkTypedTableSortedArrangementCheckpointFixture(b *testing.B) (*TypedTableSortedArrangement, *TypedTable, TypedTableSortedArrangementCheckpoint) {
	b.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name: "sorted_checkpoint_benchmark",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "score", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 1024; index++ {
		key := fmt.Sprintf("row-%04d", index)
		team := fmt.Sprintf("team-%03d", index%64)
		if _, err := table.Upsert(key, []TypedTableValue{TypedString(team), TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	arrangement, err := NewTypedTableSortedArrangement(table, TypedTableSortedArrangementDefinition{Field: "team"})
	if err != nil {
		b.Fatal(err)
	}
	checkpoint, err := arrangement.CaptureCheckpoint()
	if err != nil {
		b.Fatal(err)
	}
	return arrangement, table, checkpoint
}
