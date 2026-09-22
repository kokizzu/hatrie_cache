package hatSql

import (
	"fmt"
	"testing"
)

var c239PartMergeMetricsBaselineSink uint64

func newC239PartMergeMetricsBaselineTable(b *testing.B) *TypedTable {
	b.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name:          "events",
		Columns:       []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
		PatchParts:    TypedTablePatchOptions{Enabled: true, MergeThreshold: 1024},
		StorageEvents: TypedTableStorageEventLogOptions{Enabled: true, Capacity: 16},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 512; index++ {
		if _, err := table.Upsert(fmt.Sprintf("key-%d", index), []TypedTableValue{TypedInt64(int64(index))}); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := table.Delete("key-0"); err != nil {
		b.Fatal(err)
	}
	if err := table.CompactPatchParts(); err != nil {
		b.Fatal(err)
	}
	if _, err := table.Delete("key-1"); err != nil {
		b.Fatal(err)
	}
	return table
}

func BenchmarkC239PartMergeMetricsBaseline(b *testing.B) {
	table := newC239PartMergeMetricsBaselineTable(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		events, ok := table.StorageEvents(0)
		if !ok {
			b.Fatal("storage events disabled")
		}
		var pending, rowsRead, rowsWritten, deleted uint64
		for _, event := range events {
			pending = uint64(event.PendingDeletes)
			if event.Kind != TypedTableStorageEventPatchPartMerged {
				continue
			}
			rowsRead += uint64(event.PhysicalRowsBefore)
			rowsWritten += uint64(event.PhysicalRowsAfter)
			deleted += uint64(event.DeletedRows)
		}
		c239PartMergeMetricsBaselineSink += pending + rowsRead + rowsWritten + deleted
	}
}
