package hatSql

import (
	"strconv"
	"testing"
)

func BenchmarkCH005PatchState(b *testing.B) {
	source := ch005BenchmarkPatchTable(b)
	encoded, err := source.MarshalPatchState()
	if err != nil {
		b.Fatal(err)
	}
	restored := ch005BenchmarkPatchTable(b)
	b.Run("Marshal", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(encoded)), "snapshot_bytes")
		var sink []byte
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			sink, err = source.MarshalPatchState()
			if err != nil {
				b.Fatal(err)
			}
		}
		_ = sink
	})
	b.Run("Restore", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(len(encoded)), "snapshot_bytes")
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if err := restored.RestorePatchState(encoded); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func ch005BenchmarkPatchTable(b *testing.B) *TypedTable {
	b.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name:       "events",
		PatchParts: TypedTablePatchOptions{Enabled: true, MergeThreshold: 10000},
		Columns:    []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
	})
	if err != nil {
		b.Fatal(err)
	}
	for key := 0; key < 4096; key++ {
		if _, err := table.Upsert(strconv.Itoa(key), []TypedTableValue{TypedInt64(int64(key))}); err != nil {
			b.Fatal(err)
		}
	}
	for key := 0; key < 4096; key += 4 {
		if _, err := table.Delete(strconv.Itoa(key)); err != nil {
			b.Fatal(err)
		}
	}
	return table
}
