package hatSql

import (
	"fmt"
	"testing"
)

var ch005CompactPatchSnapshotBenchmarkSink []byte

func BenchmarkCH005PatchSnapshotMarshal(b *testing.B) {
	table := ch005CompactPatchSnapshotFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoded, err := table.MarshalPatchState()
		if err != nil {
			b.Fatal(err)
		}
		ch005CompactPatchSnapshotBenchmarkSink = encoded
	}
	b.StopTimer()
	b.SetBytes(int64(len(ch005CompactPatchSnapshotBenchmarkSink)))
}

func BenchmarkCH005PatchSnapshotRestore(b *testing.B) {
	source := ch005CompactPatchSnapshotFixture(b)
	encoded, err := source.MarshalPatchState()
	if err != nil {
		b.Fatal(err)
	}
	target := ch005CompactPatchSnapshotFixture(b)
	b.SetBytes(int64(len(encoded)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := target.RestorePatchState(encoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH005PatchSnapshotMarshalCold(b *testing.B) {
	table := ch005CompactPatchSnapshotFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table.mu.Lock()
		table.patchStateKeyLayoutGeneration++
		table.mu.Unlock()
		encoded, err := table.MarshalPatchState()
		if err != nil {
			b.Fatal(err)
		}
		ch005CompactPatchSnapshotBenchmarkSink = encoded
	}
	b.StopTimer()
	b.SetBytes(int64(len(ch005CompactPatchSnapshotBenchmarkSink)))
}

func BenchmarkCH005PatchSnapshotRestoreCold(b *testing.B) {
	source := ch005CompactPatchSnapshotFixture(b)
	encoded, err := source.MarshalPatchState()
	if err != nil {
		b.Fatal(err)
	}
	target := ch005CompactPatchSnapshotFixture(b)
	b.SetBytes(int64(len(encoded)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		target.mu.Lock()
		target.patchStateKeyLayoutGeneration++
		target.mu.Unlock()
		if err := target.RestorePatchState(encoded); err != nil {
			b.Fatal(err)
		}
	}
}

func ch005CompactPatchSnapshotFixture(tb testing.TB) *TypedTable {
	tb.Helper()
	table, err := NewTypedTable(TypedTableSchema{
		Name: "compact_snapshot",
		PatchParts: TypedTablePatchOptions{
			Enabled:        true,
			MergeThreshold: 1 << 30,
		},
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableInt64}},
	})
	if err != nil {
		tb.Fatal(err)
	}
	for index := 0; index < 4096; index++ {
		key := fmt.Sprintf("customer-region-%08d-immutable-key", index)
		if _, err := table.Upsert(key, []TypedTableValue{TypedInt64(int64(index))}); err != nil {
			tb.Fatal(err)
		}
	}
	for index := 0; index < 4096; index += 5 {
		key := fmt.Sprintf("customer-region-%08d-immutable-key", index)
		if _, err := table.Delete(key); err != nil {
			tb.Fatal(err)
		}
	}
	return table
}
