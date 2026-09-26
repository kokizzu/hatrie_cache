package hatCache

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func BenchmarkM033cGlobalTimestampReserveDirect(b *testing.B) {
	oracle, err := hatReplication.NewGlobalTimestampOracle(1, 0)
	if err != nil {
		b.Fatal(err)
	}
	request := hatReplication.GlobalTimestampRequest{Term: 1, NodeID: "node-a", NodeEpoch: 1, Count: 32}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		request.Sequence = uint64(index + 1)
		if _, err := oracle.Reserve(request); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM033cGlobalTimestampReserveGRPC(b *testing.B) {
	oracle, err := hatReplication.NewGlobalTimestampOracle(1, 0)
	if err != nil {
		b.Fatal(err)
	}
	conn := newM033cGlobalTimestampGRPCServer(b, CacheGRPCOptions{
		ReplicationAuthToken: "replication-secret",
		GlobalTimestampReserve: func(_ context.Context, request hatReplication.GlobalTimestampRequest) (hatReplication.GlobalTimestampGrant, error) {
			return oracle.Reserve(request)
		},
	})
	client := newM033cGlobalTimestampGRPCClient(b, conn, "replication-secret")
	request := hatReplication.GlobalTimestampRequest{Term: 1, NodeID: "node-a", NodeEpoch: 1, Count: 32}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		request.Sequence = uint64(index + 1)
		if _, err := client.Reserve(context.Background(), request); err != nil {
			b.Fatal(err)
		}
	}
}
