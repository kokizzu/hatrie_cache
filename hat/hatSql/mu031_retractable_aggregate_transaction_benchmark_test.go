package hatSql

import "testing"

func BenchmarkMU031AggregateTransactionAdd(b *testing.B) {
	tx, err := NewSQLAggregateTransaction(&mu031BenchmarkSum{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := tx.Add(int64(1)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkMU031AggregateTransactionCreate(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		tx, err := NewSQLAggregateTransaction(&mu031BenchmarkSum{})
		if err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}
