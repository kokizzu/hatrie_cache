package hatReplication

import "testing"

func BenchmarkGlobalTimestampOracleReserveOne(b *testing.B) {
	oracle, err := NewGlobalTimestampOracle(1, 0)
	if err != nil {
		b.Fatal(err)
	}
	request := GlobalTimestampRequest{Term: 1, NodeID: "node-a", NodeEpoch: 1, Count: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		request.Sequence = uint64(index + 1)
		request.Observed = int64(index)
		if _, err := oracle.Reserve(request); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGlobalTimestampLeasePoolNext(b *testing.B) {
	oracle, err := NewGlobalTimestampOracle(1, 0)
	if err != nil {
		b.Fatal(err)
	}
	pool, err := NewGlobalTimestampLeasePool(GlobalTimestampLeasePoolOptions{
		Term:      1,
		NodeID:    "node-a",
		NodeEpoch: 1,
		BatchSize: DefaultGlobalTimestampLeaseBatchSize,
		Reserve:   oracle.Reserve,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := pool.Next(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	stats := pool.Stats()
	b.ReportMetric(float64(stats.Reservations)/float64(b.N), "reservations/op")
}
