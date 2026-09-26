package hatCache

import (
	"context"
	"net"
	"reflect"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"hatrie_cache/hat/hatTopology"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func TestC153gPartitionOwnershipConsensusGRPCVoteAndCollector(t *testing.T) {
	authenticator, err := hatTopology.NewPartitionOwnershipConsensusAuthenticator("cluster-key-v1", []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	expected := hatTopology.PartitionOwnership{
		ShardID:             7,
		Primary:             "node-a",
		Replicas:            []string{"node-b", "node-c"},
		TopologyFingerprint: "topology-v7",
		FencingToken:        19,
	}
	conn, stop := newC153gOwnershipGRPCConnection(t, CacheGRPCOptions{
		NodeName:             "node-b",
		ReplicationAuthToken: "replication-secret",
		PartitionOwnershipConsensusVote: func(ctx context.Context, ownership hatTopology.PartitionOwnership) (hatTopology.PartitionOwnershipConsensusVote, error) {
			if err := ctx.Err(); err != nil {
				return hatTopology.PartitionOwnershipConsensusVote{}, err
			}
			return authenticator.Sign(hatTopology.PartitionOwnershipConsensusVote{
				NodeID:    "node-b",
				Ownership: ownership,
				Accepted:  true,
			})
		},
		PartitionOwnershipConsensusAuthenticator: authenticator,
	})
	defer stop()

	client := NewPartitionOwnershipConsensusGRPCClient(conn, "replication-secret")
	vote, err := client.Fetch(context.Background(), expected)
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if vote.NodeID != "node-b" || !vote.Accepted || !reflect.DeepEqual(vote.Ownership, expected) {
		t.Fatalf("vote = %#v, want signed node-b acceptance for %#v", vote, expected)
	}

	collection, err := CollectPartitionOwnershipConsensusOverGRPC(
		context.Background(),
		hatTopology.TopologyConsensusPolicy{Voters: []string{"node-b"}, Required: 1},
		expected,
		func(context.Context, string) (*PartitionOwnershipConsensusGRPCClient, error) {
			return client, nil
		},
		hatTopology.PartitionOwnershipConsensusCollectorOptions{Authenticator: authenticator},
	)
	if err != nil {
		t.Fatalf("CollectPartitionOwnershipConsensusOverGRPC() error = %v", err)
	}
	if !collection.Decision.Satisfied || len(collection.Votes) != 1 || len(collection.Failures) != 0 {
		t.Fatalf("collection = %#v, want one satisfied authenticated vote", collection)
	}
}

func TestC153gPartitionOwnershipConsensusGRPCAuthAndOptIn(t *testing.T) {
	expected := hatTopology.PartitionOwnership{
		ShardID:             1,
		Primary:             "node-a",
		TopologyFingerprint: "topology-v1",
		FencingToken:        2,
	}

	conn, stop := newC153gOwnershipGRPCConnection(t, CacheGRPCOptions{NodeName: "node-b"})
	client := NewPartitionOwnershipConsensusGRPCClient(conn, "")
	_, err := client.Fetch(context.Background(), expected)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("disabled RPC error = %v, want Unavailable", err)
	}
	stop()

	authenticator, err := hatTopology.NewPartitionOwnershipConsensusAuthenticator("cluster-key-v1", []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	conn, stop = newC153gOwnershipGRPCConnection(t, CacheGRPCOptions{
		NodeName:             "node-b",
		ReplicationAuthToken: "replication-secret",
		PartitionOwnershipConsensusVote: func(context.Context, hatTopology.PartitionOwnership) (hatTopology.PartitionOwnershipConsensusVote, error) {
			return hatTopology.PartitionOwnershipConsensusVote{NodeID: "node-b", Ownership: expected, Accepted: true}, nil
		},
		PartitionOwnershipConsensusAuthenticator: authenticator,
	})
	defer stop()
	badClient := NewPartitionOwnershipConsensusGRPCClient(conn, "wrong-secret")
	_, err = badClient.Fetch(context.Background(), expected)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("wrong replication token error = %v, want Unauthenticated", err)
	}
	goodClient := NewPartitionOwnershipConsensusGRPCClient(conn, "replication-secret")
	_, err = goodClient.Fetch(context.Background(), expected)
	if status.Code(err) != codes.Internal {
		t.Fatalf("unsigned handler vote error = %v, want Internal", err)
	}
}

func TestC153gPartitionOwnershipConsensusGRPCRejectsMalformedRequestAndImpersonation(t *testing.T) {
	expected := hatTopology.PartitionOwnership{
		ShardID:             1,
		Primary:             "node-a",
		TopologyFingerprint: "topology-v1",
		FencingToken:        2,
	}
	called := false
	conn, stop := newC153gOwnershipGRPCConnection(t, CacheGRPCOptions{
		NodeName: "node-b",
		PartitionOwnershipConsensusVote: func(context.Context, hatTopology.PartitionOwnership) (hatTopology.PartitionOwnershipConsensusVote, error) {
			called = true
			return hatTopology.PartitionOwnershipConsensusVote{NodeID: "node-attacker", Ownership: expected, Accepted: true}, nil
		},
	})
	defer stop()
	rawClient := hatriecachev1.NewCacheServiceClient(conn)
	_, err := rawClient.PartitionOwnershipConsensusVote(context.Background(), &hatriecachev1.PartitionOwnershipConsensusVoteRequest{Primary: "node-a"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("malformed request error = %v, want InvalidArgument", err)
	}
	if called {
		t.Fatal("malformed request invoked vote handler")
	}
	_, err = rawClient.PartitionOwnershipConsensusVote(context.Background(), partitionOwnershipConsensusGRPCRequest(expected))
	if status.Code(err) != codes.Internal {
		t.Fatalf("impersonated vote error = %v, want Internal", err)
	}
}

func newC153gOwnershipGRPCConnection(t testing.TB, options CacheGRPCOptions) (*grpc.ClientConn, func()) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	RegisterCacheGRPCServer(grpcServer, NewCacheGRPCServer(nil, options))
	go func() { _ = grpcServer.Serve(listener) }()
	dialer := func(context.Context, string) (net.Conn, error) { return listener.Dial() }
	conn, err := grpc.DialContext(context.Background(), "bufnet", grpc.WithContextDialer(dialer), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatal(err)
	}
	return conn, func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = listener.Close()
	}
}
