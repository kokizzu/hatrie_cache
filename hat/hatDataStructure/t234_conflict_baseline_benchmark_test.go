package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func BenchmarkT234RegularTransactionWriteBaseline(b *testing.B) {
	space, err := hatDataStructure.NewSpace(t232SpaceOptions(hatDataStructure.SpaceEngineMemtx))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tx, err := space.BeginTransaction()
		if err != nil {
			b.Fatal(err)
		}
		if err := tx.Put("key", []byte("value")); err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}
