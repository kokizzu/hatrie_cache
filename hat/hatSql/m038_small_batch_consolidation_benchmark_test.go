package hatSql

import "testing"

func benchmarkMZ038SmallBatchFixture(keyCount, updatesPerKey int) []DifferentialRow {
	updates := make([]DifferentialRow, 0, keyCount*updatesPerKey)
	for index := 0; index < keyCount; index++ {
		key := string(rune('a' + index))
		for update := 0; update < updatesPerKey; update++ {
			updates = append(updates, DifferentialRow{
				Key:  key,
				Time: uint64(index*updatesPerKey + update + 1),
				Diff: 1,
				Row:  Row{"id": int64(index), "value": key},
			})
		}
	}
	return updates
}

func BenchmarkMZ038SmallBatchConsolidation(b *testing.B) {
	for _, shape := range []struct {
		name          string
		keyCount      int
		updatesPerKey int
	}{
		{name: "TwoKeys", keyCount: 2, updatesPerKey: 2},
		{name: "FourKeys", keyCount: 4, updatesPerKey: 2},
		{name: "EightKeys", keyCount: 8, updatesPerKey: 1},
	} {
		updates := benchmarkMZ038SmallBatchFixture(shape.keyCount, shape.updatesPerKey)
		b.Run(shape.name, func(b *testing.B) {
			b.SetBytes(int64(len(updates)))
			b.Run("GenericMap", func(b *testing.B) {
				b.ReportAllocs()
				multiset := NewIncrementalMultiset()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					if _, err := multiset.applyGenericBatch(updates); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("SmallSlice", func(b *testing.B) {
				b.ReportAllocs()
				multiset := NewIncrementalMultiset()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					if _, err := multiset.applySmallBatch(updates); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
