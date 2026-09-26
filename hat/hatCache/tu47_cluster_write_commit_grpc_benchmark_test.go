package hatCache

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"hatrie_cache/hat/hatReplication"
)

func BenchmarkTU047ClusterWriteCommitGRPC(b *testing.B) {
	proposal := hatReplication.ClusterWriteCommitProposal{
		TransactionID: "tx-grpc-benchmark",
		Sequence:      7,
		FenceToken:    11,
		PayloadDigest: [32]byte{1, 2, 3},
	}
	b.Run("direct_participant_phases", func(b *testing.B) {
		participant, err := hatReplication.NewClusterWriteCommitParticipant(hatReplication.ClusterWriteCommitParticipantOptions{MaxRecords: 1})
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := participant.Prepare(proposal); err != nil {
				b.Fatal(err)
			}
			if _, err := participant.Commit(proposal); err != nil {
				b.Fatal(err)
			}
		}
	})

	client, cleanup := newTU047ClusterWriteCommitBenchmarkClient(b)
	b.Run("grpc_participant_phases", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if err := client.Prepare(context.Background(), proposal); err != nil {
				b.Fatal(err)
			}
			if err := client.Commit(context.Background(), proposal); err != nil {
				b.Fatal(err)
			}
		}
	})
	cleanup()
}

func newTU047ClusterWriteCommitBenchmarkClient(b *testing.B) (*ClusterWriteCommitGRPCClient, func()) {
	b.Helper()
	participant, err := hatReplication.NewClusterWriteCommitParticipant(hatReplication.ClusterWriteCommitParticipantOptions{MaxRecords: 1})
	if err != nil {
		b.Fatal(err)
	}
	server := NewCacheGRPCServer(nil, CacheGRPCOptions{
		ReplicationAuthToken:          "replication-secret",
		ClusterWriteCommitParticipant: participant,
	})
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	RegisterCacheGRPCServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()
	ctx, cancel := context.WithCancel(context.Background())
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cancel()
		grpcServer.Stop()
		_ = listener.Close()
		b.Fatal(err)
	}
	client := NewClusterWriteCommitGRPCClient(conn, "replication-secret")
	return client, func() {
		_ = conn.Close()
		cancel()
		grpcServer.Stop()
		_ = listener.Close()
	}
}
