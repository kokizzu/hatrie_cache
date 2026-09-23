package hatDataStructure_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func BenchmarkT233RegularTransactionReadBaseline(b *testing.B) {
	space, err := hatDataStructure.NewSpace(t232SpaceOptions(hatDataStructure.SpaceEngineMemtx))
	if err != nil {
		b.Fatal(err)
	}
	for _, key := range t232BenchmarkKeys {
		if err := space.Put(key, []byte("value")); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tx, err := space.BeginTransaction()
		if err != nil {
			b.Fatal(err)
		}
		for _, key := range t232BenchmarkKeys {
			if _, ok := tx.Get(key); !ok {
				b.Fatalf("missing baseline key %q", key)
			}
		}
		if err := tx.Rollback(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT233MVCCTransactionRead(b *testing.B) {
	space, err := hatDataStructure.NewSpace(t232SpaceOptions(hatDataStructure.SpaceEngineMemtx))
	if err != nil {
		b.Fatal(err)
	}
	for _, key := range t232BenchmarkKeys {
		if err := space.Put(key, []byte("value")); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tx, err := space.BeginMVCCTransaction()
		if err != nil {
			b.Fatal(err)
		}
		for _, key := range t232BenchmarkKeys {
			if _, ok := tx.Get(key); !ok {
				b.Fatalf("missing MVCC key %q", key)
			}
		}
		if err := tx.Rollback(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT233MVCCTransactionYield(b *testing.B) {
	space, err := hatDataStructure.NewSpace(t232SpaceOptions(hatDataStructure.SpaceEngineMemtx))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tx, err := space.BeginMVCCTransaction()
		if err != nil {
			b.Fatal(err)
		}
		if err := tx.Yield(context.Background()); err != nil {
			b.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			b.Fatal(err)
		}
	}
}
