package hatDataStructure

import "testing"

func BenchmarkT229MemtxUpsertBaseline(b *testing.B) {
	table, err := NewMemtxTable[int](MemtxTableOptions{Capacity: 1})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < b.N; index++ {
		if _, err := table.Upsert(1, index); err != nil {
			b.Fatal(err)
		}
	}
}
