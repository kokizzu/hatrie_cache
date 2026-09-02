package hatSql

import (
	"fmt"
	"strconv"
	"testing"
)

func benchmarkTypedTablePatchTable(b *testing.B, enabled bool, rows int) (*TypedTable, []string) {
	b.Helper()
	options := TypedTablePatchOptions{Enabled: enabled, MergeThreshold: rows + 1}
	table, err := NewTypedTable(TypedTableSchema{
		Name:       "events",
		PatchParts: options,
		Columns: []TypedTableColumn{
			{Name: "value", Kind: TypedTableInt64},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	keys := make([]string, rows)
	for i := range keys {
		keys[i] = "key-" + strconv.Itoa(i)
		if _, err := table.Upsert(keys[i], []TypedTableValue{TypedInt64(int64(i))}); err != nil {
			b.Fatal(err)
		}
	}
	return table, keys
}

func BenchmarkTypedTableDeleteReinsert(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "PhysicalDelete"
		if enabled {
			name = "LogicalDelete"
		}
		b.Run(name, func(b *testing.B) {
			table, keys := benchmarkTypedTablePatchTable(b, enabled, 10_000)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := keys[i%len(keys)]
				if _, err := table.Delete(key); err != nil {
					b.Fatal(err)
				}
				if _, err := table.Upsert(key, []TypedTableValue{TypedInt64(int64(i))}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTypedTableRowsAfterHalfDeletes(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "PhysicalDelete"
		if enabled {
			name = "LogicalDelete"
		}
		b.Run(name, func(b *testing.B) {
			table, keys := benchmarkTypedTablePatchTable(b, enabled, 10_000)
			for i := 0; i < len(keys); i += 2 {
				if _, err := table.Delete(keys[i]); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if rows := table.Rows(); len(rows) != len(keys)/2 {
					b.Fatalf("rows = %d, want %d", len(rows), len(keys)/2)
				}
			}
		})
	}
}

func BenchmarkTypedTablePatchCompaction(b *testing.B) {
	for _, rows := range []int{1_000, 10_000} {
		b.Run(fmt.Sprintf("Rows%d", rows), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				table, keys := benchmarkTypedTablePatchTable(b, true, rows)
				for j := 0; j < len(keys); j += 2 {
					if _, err := table.Delete(keys[j]); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				if err := table.CompactPatchParts(); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
			}
		})
	}
}
