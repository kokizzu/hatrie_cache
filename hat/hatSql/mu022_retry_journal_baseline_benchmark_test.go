package hatSql

import "testing"

var mu022OffsetAdvanceBenchmarkSink bool

func BenchmarkMU022BeforeSourceTransactionAdvance(b *testing.B) {
	tracker := NewSQLSourceOffsetTracker()
	transaction := SQLSourceTransaction{
		ID: "orders-tx",
		Offsets: []SQLSourceOffset{{
			Source:    "orders",
			Partition: "0",
		}},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		transaction.Offsets[0].Offset = uint64(index + 1)
		accepted, err := tracker.AdvanceTransaction(transaction)
		if err != nil {
			b.Fatal(err)
		}
		mu022OffsetAdvanceBenchmarkSink = accepted
	}
}
