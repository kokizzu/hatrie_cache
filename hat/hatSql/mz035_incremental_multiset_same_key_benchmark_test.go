package hatSql

import "testing"

func benchmarkMZ035SameKeyBatchFixture() []DifferentialRow {
	updates := make([]DifferentialRow, 0, 64)
	row := Row{"id": int64(1), "value": "alpha"}
	for index := 0; index < 32; index++ {
		updates = append(updates,
			DifferentialRow{Key: "a", Time: uint64(index*2 + 1), Diff: 1, Row: row},
			DifferentialRow{Key: "a", Time: uint64(index*2 + 2), Diff: -1, Row: row},
		)
	}
	return updates
}

func BenchmarkMZ035SameKeyBatch(b *testing.B) {
	updates := benchmarkMZ035SameKeyBatchFixture()
	b.SetBytes(int64(len(updates)))
	for _, benchmark := range []struct {
		name  string
		apply func(*IncrementalMultiset, []DifferentialRow) ([]DifferentialRow, error)
	}{
		{name: "GenericMap", apply: (*IncrementalMultiset).applyGenericBatch},
		{name: "SameKeyFastPath", apply: (*IncrementalMultiset).applySameKeyBatch},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			multiset := NewIncrementalMultiset()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				if _, err := benchmark.apply(multiset, updates); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
