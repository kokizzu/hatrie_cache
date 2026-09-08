package hatSql

import (
	"fmt"
	"testing"
)

func BenchmarkTypedTableSortedArrangementAppendBulkApply(b *testing.B) {
	table, err := NewTypedTable(TypedTableSchema{
		Name: "sorted_append_benchmark",
		Columns: []TypedTableColumn{
			{Name: "team", Kind: TypedTableString},
			{Name: "score", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := range 4096 {
		key := fmt.Sprintf("key-%05d", index)
		if _, err := table.Upsert(key, []TypedTableValue{TypedString(fmt.Sprintf("team-%05d", index)), TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	arrangement, err := NewTypedTableSortedArrangement(table, TypedTableSortedArrangementDefinition{Field: "team"})
	if err != nil {
		b.Fatal(err)
	}
	changes := make([]TypedTableChange, 256)
	for index := range changes {
		value := 4096 + index
		changes[index] = TypedTableChange{
			Sequence:  uint64(4097 + index),
			Operation: "INSERT",
			Key:       fmt.Sprintf("key-%05d", value),
			After:     []TypedTableValue{TypedString(fmt.Sprintf("team-%05d", value)), TypedInt64(int64(value))},
		}
	}
	baseOrderLength := len(arrangement.order)
	baseCheckpoint := arrangement.checkpoint
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		for _, change := range changes {
			delete(arrangement.entries, change.Key)
			delete(arrangement.positions, change.Key)
		}
		arrangement.order = arrangement.order[:baseOrderLength]
		arrangement.checkpoint = baseCheckpoint
		b.StartTimer()
		if err := arrangement.Apply(changes); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}
