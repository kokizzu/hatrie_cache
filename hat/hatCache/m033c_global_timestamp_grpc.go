package hatCache

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"hatrie_cache/hat/hatReplication"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

const (
	maxGlobalTimestampReserveNodeID = 256
	maxGlobalTimestampReserveCount  = 1 << 20
)

var (
	// ErrGlobalTimestampReserveGRPCGrantMismatch means the coordinator
	// returned a grant that is not bound to the request that produced it.
	ErrGlobalTimestampReserveGRPCGrantMismatch = errors.New("hatriecache: global timestamp gRPC grant does not match request")
)

// GlobalTimestampReserveHandler serves one validated, idempotent timestamp
// reservation request. The caller owns consensus, leadership, and durable
// oracle state; this hook only exposes that state over an authenticated RPC.
type GlobalTimestampReserveHandler func(context.Context, hatReplication.GlobalTimestampRequest) (hatReplication.GlobalTimestampGrant, error)

// GlobalTimestampReserveGRPCClient adapts one gRPC connection to the global
// timestamp reservation API. The caller owns connection lifetime and TLS.
type GlobalTimestampReserveGRPCClient struct {
	client           hatriecachev1.CacheServiceClient
	replicationToken string
}

// NewGlobalTimestampReserveGRPCClient creates a reservation client over conn.
func NewGlobalTimestampReserveGRPCClient(conn grpc.ClientConnInterface, replicationToken string) *GlobalTimestampReserveGRPCClient {
	if conn == nil {
		return &GlobalTimestampReserveGRPCClient{}
	}
	return &GlobalTimestampReserveGRPCClient{
		client:           hatriecachev1.NewCacheServiceClient(conn),
		replicationToken: strings.TrimSpace(replicationToken),
	}
}

// Reserve requests a globally ordered range and verifies that the response is
// bound to every request identity and range field before returning it.
func (client *GlobalTimestampReserveGRPCClient) Reserve(ctx context.Context, request hatReplication.GlobalTimestampRequest) (hatReplication.GlobalTimestampGrant, error) {
	if client == nil || client.client == nil {
		return hatReplication.GlobalTimestampGrant{}, errors.New("hatriecache: global timestamp gRPC client is not configured")
	}
	normalized, err := normalizeGlobalTimestampReserveRequest(request)
	if err != nil {
		return hatReplication.GlobalTimestampGrant{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if client.replicationToken != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-hatrie-replication-token", client.replicationToken)
	}
	response, err := client.client.GlobalTimestampReserve(ctx, &hatriecachev1.GlobalTimestampReserveRequest{
		Term:      normalized.Term,
		NodeId:    normalized.NodeID,
		NodeEpoch: normalized.NodeEpoch,
		Sequence:  normalized.Sequence,
		Observed:  normalized.Observed,
		Count:     normalized.Count,
	})
	if err != nil {
		return hatReplication.GlobalTimestampGrant{}, err
	}
	if response == nil {
		return hatReplication.GlobalTimestampGrant{}, errors.New("hatriecache: global timestamp gRPC response is empty")
	}
	grant := hatReplication.GlobalTimestampGrant{
		Term:      response.GetTerm(),
		NodeID:    response.GetNodeId(),
		NodeEpoch: response.GetNodeEpoch(),
		Sequence:  response.GetSequence(),
		Start:     response.GetStart(),
		End:       response.GetEnd(),
		Count:     response.GetCount(),
	}
	if err := validateGlobalTimestampReserveGrant(normalized, grant); err != nil {
		return hatReplication.GlobalTimestampGrant{}, err
	}
	return grant, nil
}

// GlobalTimestampReserve creates an opt-in authenticated reservation RPC.
func (server *CacheGRPCServer) GlobalTimestampReserve(ctx context.Context, request *hatriecachev1.GlobalTimestampReserveRequest) (*hatriecachev1.GlobalTimestampReserveResponse, error) {
	ctx = grpcContext(ctx)
	if err := server.requireReplicationAuthorized(ctx); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if server.options.GlobalTimestampReserve == nil {
		return nil, status.Error(codes.Unavailable, "global timestamp reservation is not configured")
	}
	normalized, err := globalTimestampReserveRequestFromProto(request)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	grant, err := server.options.GlobalTimestampReserve(ctx, normalized)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if err := validateGlobalTimestampReserveGrant(normalized, grant); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &hatriecachev1.GlobalTimestampReserveResponse{
		Term:      grant.Term,
		NodeId:    grant.NodeID,
		NodeEpoch: grant.NodeEpoch,
		Sequence:  grant.Sequence,
		Start:     grant.Start,
		End:       grant.End,
		Count:     grant.Count,
	}, nil
}

func globalTimestampReserveRequestFromProto(request *hatriecachev1.GlobalTimestampReserveRequest) (hatReplication.GlobalTimestampRequest, error) {
	if request == nil {
		return hatReplication.GlobalTimestampRequest{}, errors.New("global timestamp reservation request is required")
	}
	return normalizeGlobalTimestampReserveRequest(hatReplication.GlobalTimestampRequest{
		Term:      request.GetTerm(),
		NodeID:    request.GetNodeId(),
		NodeEpoch: request.GetNodeEpoch(),
		Sequence:  request.GetSequence(),
		Observed:  request.GetObserved(),
		Count:     request.GetCount(),
	})
}

func normalizeGlobalTimestampReserveRequest(request hatReplication.GlobalTimestampRequest) (hatReplication.GlobalTimestampRequest, error) {
	request.NodeID = strings.TrimSpace(request.NodeID)
	if request.Term == 0 || request.NodeID == "" || len(request.NodeID) > maxGlobalTimestampReserveNodeID || request.NodeEpoch == 0 || request.Sequence == 0 || request.Observed < 0 || request.Count == 0 || request.Count > maxGlobalTimestampReserveCount {
		return hatReplication.GlobalTimestampRequest{}, fmt.Errorf("global timestamp reservation request is invalid")
	}
	return request, nil
}

func validateGlobalTimestampReserveGrant(request hatReplication.GlobalTimestampRequest, grant hatReplication.GlobalTimestampGrant) error {
	if grant.Term != request.Term || grant.NodeID != request.NodeID || grant.NodeEpoch != request.NodeEpoch || grant.Sequence != request.Sequence || grant.Count != request.Count || grant.Start <= 0 || grant.End < grant.Start {
		return ErrGlobalTimestampReserveGRPCGrantMismatch
	}
	span := uint64(grant.End-grant.Start) + 1
	if span != grant.Count {
		return ErrGlobalTimestampReserveGRPCGrantMismatch
	}
	return nil
}
