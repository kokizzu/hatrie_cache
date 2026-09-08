package hatCache

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"hatrie_cache/hat/hatCommand"
	"hatrie_cache/hat/hatGrpc"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func TestCacheGRPCServerProtocolGatePreservesLegacyDefault(t *testing.T) {
	server := NewCacheGRPCServer(newTestTrie(t), CacheGRPCOptions{
		ProtocolVersions: hatCommand.ProtocolVersionRange{Min: 1, Max: 2},
	})
	response, err := server.Health(context.Background(), &hatriecachev1.HealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() == "" {
		t.Fatalf("legacy health response status = %q, want non-empty", response.GetStatus())
	}
}

func TestCacheGRPCServerProtocolGateRejectsIncompatibleVersion(t *testing.T) {
	server := NewCacheGRPCServer(newTestTrie(t), CacheGRPCOptions{
		ProtocolVersions: hatCommand.ProtocolVersionRange{Min: 1, Max: 2},
	})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(hatGrpc.ProtocolVersionMetadataKey, "3"))
	_, err := server.Health(ctx, &hatriecachev1.HealthRequest{})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("incompatible health error = %v, want FailedPrecondition", err)
	}
}

func TestCacheGRPCServerProtocolGateAcceptsOverlappingRange(t *testing.T) {
	server := NewCacheGRPCServer(newTestTrie(t), CacheGRPCOptions{
		ProtocolVersions: hatCommand.ProtocolVersionRange{Min: 1, Max: 2},
	})
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(hatGrpc.ProtocolVersionMetadataKey, "1-4"))
	response, err := server.Health(ctx, &hatriecachev1.HealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() == "" {
		t.Fatalf("overlap health response status = %q, want non-empty", response.GetStatus())
	}
}

func TestCacheGRPCServerProtocolGateNegotiatesThroughTransport(t *testing.T) {
	client, stop := newTestGRPCClient(t, newTestTrie(t), CacheGRPCOptions{
		ProtocolVersions: hatCommand.ProtocolVersionRange{Min: 1, Max: 2},
	})
	defer stop()
	ctx := hatGrpc.AppendProtocolVersionRange(context.Background(), hatCommand.ProtocolVersionRange{Min: 1, Max: 4})
	var header metadata.MD
	response, err := client.Health(ctx, &hatriecachev1.HealthRequest{}, grpc.Header(&header))
	if err != nil {
		t.Fatal(err)
	}
	if response.GetStatus() == "" {
		t.Fatalf("transport health response status = %q, want non-empty", response.GetStatus())
	}
	if header.Get(hatGrpc.ProtocolVersionMetadataKey)[0] != "2" || header.Get(hatGrpc.ProtocolSupportedMetadataKey)[0] != "1-2" {
		t.Fatalf("transport protocol headers = %#v, want selected 2 and supported 1-2", header)
	}
}
