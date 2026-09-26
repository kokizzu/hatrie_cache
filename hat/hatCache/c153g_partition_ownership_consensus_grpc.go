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
	"hatrie_cache/hat/hatTopology"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

const (
	maxPartitionOwnershipConsensusGRPCReplicas = 4096
	maxPartitionOwnershipConsensusGRPCString   = 1 << 20
)

// PartitionOwnershipConsensusVoteHandler evaluates one ownership proposal on
// the local node. The handler owns the policy decision and may sign the vote
// with hatTopology.PartitionOwnershipConsensusAuthenticator.
type PartitionOwnershipConsensusVoteHandler func(context.Context, hatTopology.PartitionOwnership) (hatTopology.PartitionOwnershipConsensusVote, error)

// PartitionOwnershipConsensusVoteAuthenticator is the minimal verification
// contract accepted by the gRPC server. The concrete HMAC authenticator in
// hatTopology satisfies it without coupling CacheGRPCOptions to one key type.
type PartitionOwnershipConsensusVoteAuthenticator interface {
	Verify(hatTopology.PartitionOwnershipConsensusVote) error
}

// PartitionOwnershipConsensusGRPCClient adapts one CacheService connection to
// the transport-neutral ownership vote collector. The caller owns conn.
type PartitionOwnershipConsensusGRPCClient struct {
	client           hatriecachev1.CacheServiceClient
	replicationToken string
}

// PartitionOwnershipConsensusGRPCClientFactory creates a client for one
// named voter. Factories are caller-owned and may reuse one connection per
// node.
type PartitionOwnershipConsensusGRPCClientFactory func(context.Context, string) (*PartitionOwnershipConsensusGRPCClient, error)

// NewPartitionOwnershipConsensusGRPCClient creates a vote client over conn.
// The replication token is sent only as internal gRPC metadata when nonempty.
func NewPartitionOwnershipConsensusGRPCClient(conn grpc.ClientConnInterface, replicationToken string) *PartitionOwnershipConsensusGRPCClient {
	if conn == nil {
		return &PartitionOwnershipConsensusGRPCClient{}
	}
	return &PartitionOwnershipConsensusGRPCClient{
		client:           hatriecachev1.NewCacheServiceClient(conn),
		replicationToken: strings.TrimSpace(replicationToken),
	}
}

// Fetch obtains one vote for expected. Response validation is deliberately
// compatible with rejected votes that carry a valid current ownership
// snapshot; the quorum collector binds the response identity to its voter.
func (client *PartitionOwnershipConsensusGRPCClient) Fetch(ctx context.Context, expected hatTopology.PartitionOwnership) (hatTopology.PartitionOwnershipConsensusVote, error) {
	if client == nil || client.client == nil {
		return hatTopology.PartitionOwnershipConsensusVote{}, errors.New("hatriecache: partition ownership consensus gRPC client is not configured")
	}
	if err := validatePartitionOwnershipConsensusGRPCMetadata(expected); err != nil {
		return hatTopology.PartitionOwnershipConsensusVote{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if client.replicationToken != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-hatrie-replication-token", client.replicationToken)
	}
	response, err := client.client.PartitionOwnershipConsensusVote(ctx, partitionOwnershipConsensusGRPCRequest(expected))
	if err != nil {
		return hatTopology.PartitionOwnershipConsensusVote{}, err
	}
	vote, err := partitionOwnershipConsensusGRPCVoteFromProto(response)
	if err != nil {
		return hatTopology.PartitionOwnershipConsensusVote{}, status.Error(codes.Internal, err.Error())
	}
	return vote, nil
}

// CollectPartitionOwnershipConsensusOverGRPC reuses the bounded generic
// collector with one caller-owned gRPC client factory per voter.
func CollectPartitionOwnershipConsensusOverGRPC(
	ctx context.Context,
	policy hatTopology.TopologyConsensusPolicy,
	expected hatTopology.PartitionOwnership,
	factory PartitionOwnershipConsensusGRPCClientFactory,
	options hatTopology.PartitionOwnershipConsensusCollectorOptions,
) (hatTopology.PartitionOwnershipConsensusCollection, error) {
	if factory == nil {
		return hatTopology.PartitionOwnershipConsensusCollection{}, fmt.Errorf("%w: gRPC client factory is required", hatTopology.ErrPartitionOwnershipConsensusCollectorInvalid)
	}
	return hatTopology.CollectPartitionOwnershipConsensus(
		ctx,
		policy,
		expected,
		func(fetchContext context.Context, voter string, ownership hatTopology.PartitionOwnership) (hatTopology.PartitionOwnershipConsensusVote, error) {
			client, err := factory(fetchContext, voter)
			if err != nil {
				return hatTopology.PartitionOwnershipConsensusVote{}, err
			}
			if client == nil {
				return hatTopology.PartitionOwnershipConsensusVote{}, fmt.Errorf("hatriecache: nil ownership consensus gRPC client for %q", voter)
			}
			return client.Fetch(fetchContext, ownership)
		},
		options,
	)
}

func (server *CacheGRPCServer) PartitionOwnershipConsensusVote(ctx context.Context, request *hatriecachev1.PartitionOwnershipConsensusVoteRequest) (*hatriecachev1.PartitionOwnershipConsensusVoteResponse, error) {
	ctx = grpcContext(ctx)
	if err := server.requireReplicationAuthorized(ctx); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if server.options.PartitionOwnershipConsensusVote == nil {
		return nil, status.Error(codes.Unavailable, "partition ownership consensus vote is not configured")
	}
	ownership, err := partitionOwnershipConsensusGRPCOwnershipFromProto(request)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	vote, err := server.options.PartitionOwnershipConsensusVote(ctx, ownership)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	nodeID := strings.TrimSpace(server.options.NodeName)
	if vote.NodeID != nodeID {
		return nil, status.Error(codes.Internal, "partition ownership consensus vote node identity does not match server")
	}
	if err := validatePartitionOwnershipConsensusGRPCVote(ownership, vote); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if authenticator := server.options.PartitionOwnershipConsensusAuthenticator; authenticator != nil {
		if err := authenticator.Verify(vote); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return partitionOwnershipConsensusGRPCVoteToProto(vote), nil
}

func partitionOwnershipConsensusGRPCRequest(ownership hatTopology.PartitionOwnership) *hatriecachev1.PartitionOwnershipConsensusVoteRequest {
	return &hatriecachev1.PartitionOwnershipConsensusVoteRequest{
		ShardId:             ownership.ShardID,
		Primary:             ownership.Primary,
		Replicas:            append([]string(nil), ownership.Replicas...),
		TopologyFingerprint: ownership.TopologyFingerprint,
		FencingToken:        ownership.FencingToken,
	}
}

func partitionOwnershipConsensusGRPCOwnershipFromProto(request *hatriecachev1.PartitionOwnershipConsensusVoteRequest) (hatTopology.PartitionOwnership, error) {
	if request == nil {
		return hatTopology.PartitionOwnership{}, errors.New("partition ownership consensus vote request is required")
	}
	if len(request.GetReplicas()) > maxPartitionOwnershipConsensusGRPCReplicas {
		return hatTopology.PartitionOwnership{}, fmt.Errorf("partition ownership consensus replica count exceeds %d", maxPartitionOwnershipConsensusGRPCReplicas)
	}
	ownership := hatTopology.PartitionOwnership{
		ShardID:             request.GetShardId(),
		Primary:             request.GetPrimary(),
		Replicas:            append([]string(nil), request.GetReplicas()...),
		TopologyFingerprint: request.GetTopologyFingerprint(),
		FencingToken:        request.GetFencingToken(),
	}
	if err := validatePartitionOwnershipConsensusGRPCMetadata(ownership); err != nil {
		return hatTopology.PartitionOwnership{}, err
	}
	return ownership, nil
}

func partitionOwnershipConsensusGRPCVoteToProto(vote hatTopology.PartitionOwnershipConsensusVote) *hatriecachev1.PartitionOwnershipConsensusVoteResponse {
	return &hatriecachev1.PartitionOwnershipConsensusVoteResponse{
		NodeId:              vote.NodeID,
		Accepted:            vote.Accepted,
		ShardId:             vote.Ownership.ShardID,
		Primary:             vote.Ownership.Primary,
		Replicas:            append([]string(nil), vote.Ownership.Replicas...),
		TopologyFingerprint: vote.Ownership.TopologyFingerprint,
		FencingToken:        vote.Ownership.FencingToken,
		KeyId:               vote.KeyID,
		Signature:           append([]byte(nil), vote.Signature...),
	}
}

func partitionOwnershipConsensusGRPCVoteFromProto(response *hatriecachev1.PartitionOwnershipConsensusVoteResponse) (hatTopology.PartitionOwnershipConsensusVote, error) {
	if response == nil {
		return hatTopology.PartitionOwnershipConsensusVote{}, errors.New("partition ownership consensus vote response is required")
	}
	if len(response.GetReplicas()) > maxPartitionOwnershipConsensusGRPCReplicas {
		return hatTopology.PartitionOwnershipConsensusVote{}, fmt.Errorf("partition ownership consensus replica count exceeds %d", maxPartitionOwnershipConsensusGRPCReplicas)
	}
	vote := hatTopology.PartitionOwnershipConsensusVote{
		NodeID: response.GetNodeId(),
		Ownership: hatTopology.PartitionOwnership{
			ShardID:             response.GetShardId(),
			Primary:             response.GetPrimary(),
			Replicas:            append([]string(nil), response.GetReplicas()...),
			TopologyFingerprint: response.GetTopologyFingerprint(),
			FencingToken:        response.GetFencingToken(),
		},
		Accepted:  response.GetAccepted(),
		KeyID:     response.GetKeyId(),
		Signature: append([]byte(nil), response.GetSignature()...),
	}
	if err := validatePartitionOwnershipConsensusGRPCVoteMetadata(vote); err != nil {
		return hatTopology.PartitionOwnershipConsensusVote{}, err
	}
	return vote, nil
}

func validatePartitionOwnershipConsensusGRPCMetadata(ownership hatTopology.PartitionOwnership) error {
	if len(ownership.Primary) > maxPartitionOwnershipConsensusGRPCString || len(ownership.TopologyFingerprint) > maxPartitionOwnershipConsensusGRPCString {
		return errors.New("partition ownership consensus metadata string is too large")
	}
	for _, replica := range ownership.Replicas {
		if len(replica) > maxPartitionOwnershipConsensusGRPCString {
			return errors.New("partition ownership consensus replica name is too large")
		}
	}
	_, err := hatTopology.EvaluatePartitionOwnershipConsensus(
		hatTopology.TopologyConsensusPolicy{Voters: []string{"validation-node"}, Required: 1},
		ownership,
		[]hatTopology.PartitionOwnershipConsensusVote{{NodeID: "validation-node", Ownership: ownership}},
	)
	return err
}

func validatePartitionOwnershipConsensusGRPCVote(ownership hatTopology.PartitionOwnership, vote hatTopology.PartitionOwnershipConsensusVote) error {
	if err := validatePartitionOwnershipConsensusGRPCMetadata(ownership); err != nil {
		return err
	}
	return validatePartitionOwnershipConsensusGRPCVoteMetadata(vote)
}

func validatePartitionOwnershipConsensusGRPCVoteMetadata(vote hatTopology.PartitionOwnershipConsensusVote) error {
	nodeID := strings.TrimSpace(vote.NodeID)
	if nodeID == "" {
		return errors.New("partition ownership consensus vote node is required")
	}
	if len(vote.NodeID) > maxPartitionOwnershipConsensusGRPCString || len(vote.KeyID) > maxPartitionOwnershipConsensusGRPCString {
		return errors.New("partition ownership consensus vote string is too large")
	}
	if err := validatePartitionOwnershipConsensusGRPCMetadata(vote.Ownership); err != nil {
		return err
	}
	_, err := hatTopology.EvaluatePartitionOwnershipConsensus(
		hatTopology.TopologyConsensusPolicy{Voters: []string{nodeID}, Required: 1},
		vote.Ownership,
		[]hatTopology.PartitionOwnershipConsensusVote{vote},
	)
	return err
}
