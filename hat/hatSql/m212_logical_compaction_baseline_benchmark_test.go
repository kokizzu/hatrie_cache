package hatSql

import (
	"strconv"
	"testing"
)

var m212CompactionBenchmarkSink uint64

func BenchmarkM212PhysicalCompactionBaseline(b *testing.B) {
	table, frontier := newM212CompactionBenchmarkTable(b)
	originalHeads := m212OriginalVersionHeads(table)
	var retainedNodes int
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := table.CompactMVCCThrough(frontier); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		m212CompactionBenchmarkSink = table.mvcc.compactedThrough
		retainedNodes = m212RetainedVersionNodes(table)
		m212ResetCompactionBenchmarkTable(table, originalHeads)
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(float64(retainedNodes), "retained-version-nodes")
}

func newM212CompactionBenchmarkTable(b *testing.B) (*TypedTable, uint64) {
	b.Helper()
	const (
		keyCount       = 256
		versionsPerKey = 16
	)
	table, err := NewTypedTable(TypedTableSchema{
		Name: "m212_compaction",
		MVCC: TypedTableMVCCOptions{Enabled: true},
		Columns: []TypedTableColumn{
			{Name: "value", Kind: TypedTableString},
			{Name: "version", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	for version := 0; version < versionsPerKey; version++ {
		for key := 0; key < keyCount; key++ {
			if _, err := table.Upsert("row"+strconv.Itoa(key), []TypedTableValue{
				TypedString("value"),
				TypedInt64(int64(version)),
			}); err != nil {
				b.Fatal(err)
			}
		}
	}
	return table, table.sequence - keyCount
}

func m212OriginalVersionHeads(table *TypedTable) map[string]*typedTableMVCCVersion {
	original := make(map[string]*typedTableMVCCVersion, len(table.mvcc.heads))
	for key, head := range table.mvcc.heads {
		original[key] = head
	}
	return original
}

func m212ResetCompactionBenchmarkTable(table *TypedTable, originalHeads map[string]*typedTableMVCCVersion) {
	table.mu.Lock()
	for key, head := range originalHeads {
		table.mvcc.heads[key] = head
	}
	table.mvcc.compactedThrough = 0
	table.mvcc.physicalCompactedThrough = 0
	table.mu.Unlock()
}
