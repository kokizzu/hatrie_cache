package hatReplication

import "testing"

var globalTimestampBenchmarkSink int64

func BenchmarkGlobalTimestampOracle(b *testing.B) {
	b.Run("reserve_one", func(b *testing.B) {
		oracle, err := NewGlobalTimestampOracle(1, 0)
		if err != nil {
			b.Fatal(err)
		}
		request := GlobalTimestampRequest{Term: 1, NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Count: 1}
		if _, err := oracle.Reserve(request); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			request.Sequence = uint64(index + 2)
			grant, err := oracle.Reserve(request)
			if err != nil {
				b.Fatal(err)
			}
			globalTimestampBenchmarkSink = grant.End
		}
	})

	b.Run("reserve_range", func(b *testing.B) {
		oracle, err := NewGlobalTimestampOracle(1, 0)
		if err != nil {
			b.Fatal(err)
		}
		request := GlobalTimestampRequest{Term: 1, NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Count: 1024}
		if _, err := oracle.Reserve(request); err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			request.Sequence = uint64(index + 2)
			grant, err := oracle.Reserve(request)
			if err != nil {
				b.Fatal(err)
			}
			globalTimestampBenchmarkSink = grant.End
		}
	})

	b.Run("leased_next", func(b *testing.B) {
		oracle, err := NewGlobalTimestampOracle(1, 0)
		if err != nil {
			b.Fatal(err)
		}
		request := GlobalTimestampRequest{Term: 1, NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Count: 1024}
		grant, err := oracle.Reserve(request)
		if err != nil {
			b.Fatal(err)
		}
		lease, err := NewGlobalTimestampLease(grant)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			value, ok := lease.Next()
			if !ok {
				request.Sequence++
				grant, err := oracle.Reserve(request)
				if err != nil {
					b.Fatal(err)
				}
				if err := lease.Reset(grant); err != nil {
					b.Fatal(err)
				}
				value, ok = lease.Next()
			}
			if !ok {
				b.Fatal("timestamp lease unexpectedly exhausted")
			}
			globalTimestampBenchmarkSink = value
		}
	})
}
