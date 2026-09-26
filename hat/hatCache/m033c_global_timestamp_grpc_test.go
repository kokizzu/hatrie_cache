package hatCache

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"hatrie_cache/hat/hatReplication"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func TestM033cGlobalTimestampReserveGRPCIsRetrySafeAndBoundToRequest(t *testing.T) {
	oracle, err := hatReplication.NewGlobalTimestampOracle(7, 10)
	if err != nil {
		t.Fatal(err)
	}
	server := newM033cGlobalTimestampGRPCServer(t, CacheGRPCOptions{
		ReplicationAuthToken: "replication-secret",
		GlobalTimestampReserve: func(_ context.Context, request hatReplication.GlobalTimestampRequest) (hatReplication.GlobalTimestampGrant, error) {
			return oracle.Reserve(request)
		},
	})
	client := newM033cGlobalTimestampGRPCClient(t, server, "replication-secret")
	request := hatReplication.GlobalTimestampRequest{Term: 7, NodeID: "node-a", NodeEpoch: 3, Sequence: 1, Observed: 11, Count: 4}
	first, err := client.Reserve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Term != request.Term || first.NodeID != request.NodeID || first.Start != 12 || first.End != 15 || first.Count != request.Count {
		t.Fatalf("first grant = %#v, want bound [12,15] grant", first)
	}
	retry, err := client.Reserve(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if retry != first {
		t.Fatalf("retry grant = %#v, want %#v", retry, first)
	}
}

func TestM033cGlobalTimestampReserveGRPCRejectsDisabledAndUnauthorized(t *testing.T) {
	for name, options := range map[string]CacheGRPCOptions{
		"disabled": {},
		"wrong token": {
			ReplicationAuthToken: "replication-secret",
			GlobalTimestampReserve: func(context.Context, hatReplication.GlobalTimestampRequest) (hatReplication.GlobalTimestampGrant, error) {
				return hatReplication.GlobalTimestampGrant{}, nil
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			server := newM033cGlobalTimestampGRPCServer(t, options)
			token := "replication-secret"
			if name == "wrong token" {
				token = "wrong-secret"
			}
			client := newM033cGlobalTimestampGRPCClient(t, server, token)
			_, err := client.Reserve(context.Background(), hatReplication.GlobalTimestampRequest{Term: 1, NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Count: 1})
			if name == "disabled" {
				if status.Code(err) != codes.Unavailable {
					t.Fatalf("error = %v, want Unavailable", err)
				}
				return
			}
			if status.Code(err) != codes.Unauthenticated {
				t.Fatalf("error = %v, want Unauthenticated", err)
			}
		})
	}
}

func TestM033cGlobalTimestampReserveGRPCRejectsMalformedRequestBeforeHandler(t *testing.T) {
	calls := 0
	server := newM033cGlobalTimestampGRPCServer(t, CacheGRPCOptions{
		ReplicationAuthToken: "replication-secret",
		GlobalTimestampReserve: func(context.Context, hatReplication.GlobalTimestampRequest) (hatReplication.GlobalTimestampGrant, error) {
			calls++
			return hatReplication.GlobalTimestampGrant{}, nil
		},
	})
	client := newM033cGlobalTimestampGRPCClient(t, server, "replication-secret")
	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-hatrie-replication-token", "replication-secret")
	_, err := client.client.GlobalTimestampReserve(ctx, &hatriecachev1.GlobalTimestampReserveRequest{Term: 1, NodeId: "node-a", NodeEpoch: 1, Sequence: 0, Count: 1})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("error = %v, want InvalidArgument", err)
	}
	if calls != 0 {
		t.Fatalf("handler calls = %d, want 0", calls)
	}
}

func TestM033cGlobalTimestampReserveGRPCRejectsImpersonatedGrant(t *testing.T) {
	server := newM033cGlobalTimestampGRPCServer(t, CacheGRPCOptions{
		ReplicationAuthToken: "replication-secret",
		GlobalTimestampReserve: func(context.Context, hatReplication.GlobalTimestampRequest) (hatReplication.GlobalTimestampGrant, error) {
			return hatReplication.GlobalTimestampGrant{Term: 1, NodeID: "other-node", NodeEpoch: 1, Sequence: 1, Start: 1, End: 1, Count: 1}, nil
		},
	})
	client := newM033cGlobalTimestampGRPCClient(t, server, "replication-secret")
	_, err := client.Reserve(context.Background(), hatReplication.GlobalTimestampRequest{Term: 1, NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Count: 1})
	if status.Code(err) != codes.Internal {
		t.Fatalf("error = %v, want Internal grant mismatch", err)
	}
}

func newM033cGlobalTimestampGRPCServer(t testing.TB, options CacheGRPCOptions) *grpc.ClientConn {
	t.Helper()
	server := NewCacheGRPCServer(&HatTrie{}, options)
	grpcServer := grpc.NewServer()
	hatriecachev1.RegisterCacheServiceServer(grpcServer, server)
	listener := bufconn.Listen(1024 * 1024)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.GracefulStop()
		_ = listener.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func newM033cGlobalTimestampGRPCClient(t testing.TB, conn *grpc.ClientConn, token string) *GlobalTimestampReserveGRPCClient {
	t.Helper()
	return NewGlobalTimestampReserveGRPCClient(conn, token)
}
