package hatGrpc_test

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"hatrie_cache/hat/hatGrpc"
)

func TestPublicGRPCClientExportsProtoMessagesAndService(t *testing.T) {
	request := &hatGrpc.CommandRequest{
		Command:        "SETSTR",
		Key:            "name",
		Value:          "ivi",
		IdempotencyKey: "request-1",
		BinaryValue:    []byte("payload"),
	}
	wire, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	decoded := new(hatGrpc.CommandRequest)
	if err := proto.Unmarshal(wire, decoded); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(request, decoded) {
		t.Fatalf("protobuf round trip = %#v, want %#v", decoded, request)
	}
	if hatGrpc.ScalarCommand_SCALAR_COMMAND_GET == hatGrpc.ScalarCommand_SCALAR_COMMAND_SET_STRING {
		t.Fatal("public scalar command constants are not distinct")
	}
	if hatGrpc.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP == hatGrpc.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP {
		t.Fatal("public structured command constants are not distinct")
	}
	var _ hatGrpc.CacheServiceClient
	var _ hatGrpc.CacheServiceServer = publicServer{}
	if hatGrpc.NewCacheServiceClient(nil) == nil {
		t.Fatal("NewCacheServiceClient(nil) returned nil")
	}
	if hatGrpc.FileCacheProto == nil {
		t.Fatal("public protobuf file descriptor is nil")
	}
}

type publicServer struct {
	hatGrpc.UnimplementedCacheServiceServer
}

func TestPublicGRPCClientExportsAllBatchMessageTypes(t *testing.T) {
	messages := []proto.Message{
		&hatGrpc.HealthRequest{},
		&hatGrpc.HealthResponse{},
		&hatGrpc.StatsRequest{},
		&hatGrpc.StatsResponse{},
		&hatGrpc.EntriesRequest{},
		&hatGrpc.Entry{},
		&hatGrpc.EntriesResponse{},
		&hatGrpc.CommandRequest{},
		&hatGrpc.CommandResponse{},
		&hatGrpc.CommandBatchRequest{},
		&hatGrpc.CommandBatchResponse{},
		&hatGrpc.SnapshotRequest{},
		&hatGrpc.ReplicationRequest{},
		&hatGrpc.ReplicationResponse{},
		&hatGrpc.ReplicationStreamBatch{},
		&hatGrpc.ReplicationStreamAck{},
		&hatGrpc.ReplicationQueue{},
		&hatGrpc.ReplicationTarget{},
		&hatGrpc.TopologyRequest{},
		&hatGrpc.UpdateTopologyRequest{},
		&hatGrpc.TopologyResponse{},
		&hatGrpc.ClusterTopology{},
		&hatGrpc.TopologyNode{},
		&hatGrpc.TopologyShard{},
		&hatGrpc.TopologyBucketRange{},
		&hatGrpc.TopologyRoute{},
		&hatGrpc.ElectionRequest{},
		&hatGrpc.UpdateElectionRequest{},
		&hatGrpc.ElectionResponse{},
		&hatGrpc.ElectionStatus{},
		&hatGrpc.ElectionNodeStatus{},
		&hatGrpc.ElectionLeader{},
		&hatGrpc.ElectionKeyRoute{},
		&hatGrpc.ScalarBatchRequest{},
		&hatGrpc.ScalarBatchResponse{},
		&hatGrpc.StructuredBatchRequest{},
		&hatGrpc.StructuredBatchResponse{},
	}
	if len(messages) != 37 {
		t.Fatalf("public message count = %d, want 37", len(messages))
	}
}

func BenchmarkPublicGRPCClientProtoMarshal(b *testing.B) {
	request := &hatGrpc.CommandRequest{
		Command:        "SETSTR",
		Key:            "name",
		Value:          "ivi",
		IdempotencyKey: "request-1",
		BinaryValue:    []byte("payload"),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := proto.Marshal(request); err != nil {
			b.Fatal(err)
		}
	}
}
