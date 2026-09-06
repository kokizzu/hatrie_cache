package hatGrpc

import (
	"google.golang.org/grpc"

	generated "hatrie_cache/internal/gen/hatriecache/v1"
)

// The public aliases keep the generated protobuf contract importable without
// requiring applications to import the repository's internal package.
type CacheServiceClient = generated.CacheServiceClient
type CacheServiceServer = generated.CacheServiceServer
type UnimplementedCacheServiceServer = generated.UnimplementedCacheServiceServer
type UnsafeCacheServiceServer = generated.UnsafeCacheServiceServer

type CacheService_CommandStreamClient = generated.CacheService_CommandStreamClient
type CacheService_CommandStreamServer = generated.CacheService_CommandStreamServer
type CacheService_CommandBatchStreamClient = generated.CacheService_CommandBatchStreamClient
type CacheService_CommandBatchStreamServer = generated.CacheService_CommandBatchStreamServer
type CacheService_ScalarBatchStreamClient = generated.CacheService_ScalarBatchStreamClient
type CacheService_ScalarBatchStreamServer = generated.CacheService_ScalarBatchStreamServer
type CacheService_StructuredBatchStreamClient = generated.CacheService_StructuredBatchStreamClient
type CacheService_StructuredBatchStreamServer = generated.CacheService_StructuredBatchStreamServer
type CacheService_ReplicationStreamClient = generated.CacheService_ReplicationStreamClient
type CacheService_ReplicationStreamServer = generated.CacheService_ReplicationStreamServer

type ScalarCommand = generated.ScalarCommand
type ScalarResultStatus = generated.ScalarResultStatus
type ScalarValueKind = generated.ScalarValueKind
type StructuredCommand = generated.StructuredCommand

type HealthRequest = generated.HealthRequest
type HealthResponse = generated.HealthResponse
type StatsRequest = generated.StatsRequest
type StatsResponse = generated.StatsResponse
type EntriesRequest = generated.EntriesRequest
type Entry = generated.Entry
type EntriesResponse = generated.EntriesResponse
type CommandRequest = generated.CommandRequest
type CommandResponse = generated.CommandResponse
type CommandBatchRequest = generated.CommandBatchRequest
type CommandBatchResponse = generated.CommandBatchResponse
type SnapshotRequest = generated.SnapshotRequest
type ReplicationRequest = generated.ReplicationRequest
type ReplicationResponse = generated.ReplicationResponse
type ReplicationStreamBatch = generated.ReplicationStreamBatch
type ReplicationStreamAck = generated.ReplicationStreamAck
type ReplicationQueue = generated.ReplicationQueue
type ReplicationTarget = generated.ReplicationTarget
type TopologyRequest = generated.TopologyRequest
type UpdateTopologyRequest = generated.UpdateTopologyRequest
type TopologyResponse = generated.TopologyResponse
type ClusterTopology = generated.ClusterTopology
type TopologyNode = generated.TopologyNode
type TopologyShard = generated.TopologyShard
type TopologyBucketRange = generated.TopologyBucketRange
type TopologyRoute = generated.TopologyRoute
type ElectionRequest = generated.ElectionRequest
type UpdateElectionRequest = generated.UpdateElectionRequest
type ElectionResponse = generated.ElectionResponse
type ElectionStatus = generated.ElectionStatus
type ElectionNodeStatus = generated.ElectionNodeStatus
type ElectionLeader = generated.ElectionLeader
type ElectionKeyRoute = generated.ElectionKeyRoute
type ScalarBatchRequest = generated.ScalarBatchRequest
type ScalarBatchResponse = generated.ScalarBatchResponse
type StructuredBatchRequest = generated.StructuredBatchRequest
type StructuredBatchResponse = generated.StructuredBatchResponse

const (
	ScalarCommand_SCALAR_COMMAND_UNSPECIFIED = generated.ScalarCommand_SCALAR_COMMAND_UNSPECIFIED
	ScalarCommand_SCALAR_COMMAND_GET         = generated.ScalarCommand_SCALAR_COMMAND_GET
	ScalarCommand_SCALAR_COMMAND_EXISTS      = generated.ScalarCommand_SCALAR_COMMAND_EXISTS
	ScalarCommand_SCALAR_COMMAND_SET_STRING  = generated.ScalarCommand_SCALAR_COMMAND_SET_STRING
	ScalarCommand_SCALAR_COMMAND_SET_COUNTER = generated.ScalarCommand_SCALAR_COMMAND_SET_COUNTER
	ScalarCommand_SCALAR_COMMAND_INCREMENT   = generated.ScalarCommand_SCALAR_COMMAND_INCREMENT
	ScalarCommand_SCALAR_COMMAND_DELETE      = generated.ScalarCommand_SCALAR_COMMAND_DELETE

	ScalarResultStatus_SCALAR_RESULT_STATUS_UNSPECIFIED      = generated.ScalarResultStatus_SCALAR_RESULT_STATUS_UNSPECIFIED
	ScalarResultStatus_SCALAR_RESULT_STATUS_OK               = generated.ScalarResultStatus_SCALAR_RESULT_STATUS_OK
	ScalarResultStatus_SCALAR_RESULT_STATUS_NOT_FOUND        = generated.ScalarResultStatus_SCALAR_RESULT_STATUS_NOT_FOUND
	ScalarResultStatus_SCALAR_RESULT_STATUS_INVALID_KEY      = generated.ScalarResultStatus_SCALAR_RESULT_STATUS_INVALID_KEY
	ScalarResultStatus_SCALAR_RESULT_STATUS_INVALID_ARGUMENT = generated.ScalarResultStatus_SCALAR_RESULT_STATUS_INVALID_ARGUMENT
	ScalarResultStatus_SCALAR_RESULT_STATUS_COUNTER_OVERFLOW = generated.ScalarResultStatus_SCALAR_RESULT_STATUS_COUNTER_OVERFLOW
	ScalarResultStatus_SCALAR_RESULT_STATUS_INTERNAL_ERROR   = generated.ScalarResultStatus_SCALAR_RESULT_STATUS_INTERNAL_ERROR

	ScalarValueKind_SCALAR_VALUE_KIND_NONE    = generated.ScalarValueKind_SCALAR_VALUE_KIND_NONE
	ScalarValueKind_SCALAR_VALUE_KIND_BYTES   = generated.ScalarValueKind_SCALAR_VALUE_KIND_BYTES
	ScalarValueKind_SCALAR_VALUE_KIND_INTEGER = generated.ScalarValueKind_SCALAR_VALUE_KIND_INTEGER
	ScalarValueKind_SCALAR_VALUE_KIND_BOOLEAN = generated.ScalarValueKind_SCALAR_VALUE_KIND_BOOLEAN

	StructuredCommand_STRUCTURED_COMMAND_UNSPECIFIED   = generated.StructuredCommand_STRUCTURED_COMMAND_UNSPECIFIED
	StructuredCommand_STRUCTURED_COMMAND_PUT_MAP       = generated.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP
	StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP      = generated.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP
	StructuredCommand_STRUCTURED_COMMAND_TAKE_MAP      = generated.StructuredCommand_STRUCTURED_COMMAND_TAKE_MAP
	StructuredCommand_STRUCTURED_COMMAND_PUSH_SLICE    = generated.StructuredCommand_STRUCTURED_COMMAND_PUSH_SLICE
	StructuredCommand_STRUCTURED_COMMAND_POP_SLICE     = generated.StructuredCommand_STRUCTURED_COMMAND_POP_SLICE
	StructuredCommand_STRUCTURED_COMMAND_SHIFT_SLICE   = generated.StructuredCommand_STRUCTURED_COMMAND_SHIFT_SLICE
	StructuredCommand_STRUCTURED_COMMAND_HEAD_SLICE    = generated.StructuredCommand_STRUCTURED_COMMAND_HEAD_SLICE
	StructuredCommand_STRUCTURED_COMMAND_TAIL_SLICE    = generated.StructuredCommand_STRUCTURED_COMMAND_TAIL_SLICE
	StructuredCommand_STRUCTURED_COMMAND_ADD_SET       = generated.StructuredCommand_STRUCTURED_COMMAND_ADD_SET
	StructuredCommand_STRUCTURED_COMMAND_REMOVE_SET    = generated.StructuredCommand_STRUCTURED_COMMAND_REMOVE_SET
	StructuredCommand_STRUCTURED_COMMAND_HAS_SET       = generated.StructuredCommand_STRUCTURED_COMMAND_HAS_SET
	StructuredCommand_STRUCTURED_COMMAND_GET_SET       = generated.StructuredCommand_STRUCTURED_COMMAND_GET_SET
	StructuredCommand_STRUCTURED_COMMAND_PUSH_PRIORITY = generated.StructuredCommand_STRUCTURED_COMMAND_PUSH_PRIORITY
	StructuredCommand_STRUCTURED_COMMAND_PEEK_PRIORITY = generated.StructuredCommand_STRUCTURED_COMMAND_PEEK_PRIORITY
	StructuredCommand_STRUCTURED_COMMAND_POP_PRIORITY  = generated.StructuredCommand_STRUCTURED_COMMAND_POP_PRIORITY
	StructuredCommand_STRUCTURED_COMMAND_GET_PRIORITY  = generated.StructuredCommand_STRUCTURED_COMMAND_GET_PRIORITY
)

// NewCacheServiceClient constructs a client for the public protobuf contract.
func NewCacheServiceClient(cc grpc.ClientConnInterface) CacheServiceClient {
	return generated.NewCacheServiceClient(cc)
}

// RegisterCacheServiceServer registers an implementation of the public
// protobuf service contract.
func RegisterCacheServiceServer(s grpc.ServiceRegistrar, srv CacheServiceServer) {
	generated.RegisterCacheServiceServer(s, srv)
}

var (
	// ScalarCommand_name maps wire enum values to their protobuf names.
	ScalarCommand_name = generated.ScalarCommand_name
	// ScalarCommand_value maps protobuf enum names to wire enum values.
	ScalarCommand_value = generated.ScalarCommand_value
	// ScalarResultStatus_name maps wire enum values to their protobuf names.
	ScalarResultStatus_name = generated.ScalarResultStatus_name
	// ScalarResultStatus_value maps protobuf enum names to wire enum values.
	ScalarResultStatus_value = generated.ScalarResultStatus_value
	// ScalarValueKind_name maps wire enum values to their protobuf names.
	ScalarValueKind_name = generated.ScalarValueKind_name
	// ScalarValueKind_value maps protobuf enum names to wire enum values.
	ScalarValueKind_value = generated.ScalarValueKind_value
	// StructuredCommand_name maps wire enum values to their protobuf names.
	StructuredCommand_name = generated.StructuredCommand_name
	// StructuredCommand_value maps protobuf enum names to wire enum values.
	StructuredCommand_value = generated.StructuredCommand_value

	// CacheService_ServiceDesc is the gRPC service descriptor for CacheService.
	CacheService_ServiceDesc = generated.CacheService_ServiceDesc
	// File_proto_hatriecache_v1_cache_proto is the protobuf file descriptor.
	File_proto_hatriecache_v1_cache_proto = generated.File_proto_hatriecache_v1_cache_proto

	// CacheServiceServiceDesc is an idiomatic alias for CacheService_ServiceDesc.
	CacheServiceServiceDesc = generated.CacheService_ServiceDesc
	// FileCacheProto is an idiomatic alias for File_proto_hatriecache_v1_cache_proto.
	FileCacheProto = generated.File_proto_hatriecache_v1_cache_proto
)
