package hatDataStructure_test

import (
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func BenchmarkT234RegularTransactionWriteVinylBaseline(b *testing.B) {
	space, err := newT234BenchmarkSpace(true)
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

func BenchmarkT234ConflictTransactionWrite(b *testing.B) {
	benchmarkT234ConflictTransactionWrite(b, false)
}

func BenchmarkT234ConflictTransactionWriteVinyl(b *testing.B) {
	benchmarkT234ConflictTransactionWrite(b, true)
}

func benchmarkT234ConflictTransactionWrite(b *testing.B, vinyl bool) {
	space, err := newT234BenchmarkSpace(vinyl)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tx, err := space.BeginConflictDetectingTransaction()
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

func BenchmarkT234ConflictTransactionRejected(b *testing.B) {
	space, err := newT234BenchmarkSpace(false)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		first, err := space.BeginConflictDetectingTransaction()
		if err != nil {
			b.Fatal(err)
		}
		second, err := space.BeginConflictDetectingTransaction()
		if err != nil {
			b.Fatal(err)
		}
		if err := first.Put("conflict", []byte("first")); err != nil {
			b.Fatal(err)
		}
		if err := second.Put("conflict", []byte("second")); err != nil {
			b.Fatal(err)
		}
		if err := first.Commit(); err != nil {
			b.Fatal(err)
		}
		if err := second.Commit(); err == nil {
			b.Fatal("expected conflict")
		}
		if err := second.Rollback(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT234DirectVinylPutDefault(b *testing.B) {
	space, err := newT234BenchmarkSpace(true)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := space.Put("key", []byte("value")); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT234DirectVinylDeleteMissingDefault(b *testing.B) {
	space, err := newT234BenchmarkSpace(true)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := space.Delete("missing"); err != nil {
			b.Fatal(err)
		}
	}
}

func newT234BenchmarkSpace(vinyl bool) (*hatDataStructure.Space, error) {
	if vinyl {
		return hatDataStructure.NewSpace(t232SpaceOptions(hatDataStructure.SpaceEngineVinyl))
	}
	return hatDataStructure.NewSpace(t232SpaceOptions(hatDataStructure.SpaceEngineMemtx))
}
