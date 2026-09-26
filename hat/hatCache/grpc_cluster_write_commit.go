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

// ClusterWriteCommitGRPCClient adapts one gRPC connection to the phase
// callbacks accepted by the transport-neutral cluster write coordinator.
// Authentication and TLS remain owned by the caller's connection; the token is
// optional and is sent only as replication metadata when configured.
type ClusterWriteCommitGRPCClient struct {
	client           hatriecachev1.CacheServiceClient
	replicationToken string
}

// ClusterWriteCommitGRPCClientFactory creates one reusable phase client for a
// participant node. The caller owns the connection lifetime.
type ClusterWriteCommitGRPCClientFactory func(context.Context, string) (*ClusterWriteCommitGRPCClient, error)

const clusterWriteCommitPayloadDigestSize = 32

// ExecuteClusterWriteCommitOverGRPC binds the existing transport-neutral
// coordinator to one reusable gRPC client per participant. Connections are
// created before prepare starts and are never created again for later phases.
// TLS, dialing, and connection cleanup remain caller-owned through factory.
func ExecuteClusterWriteCommitOverGRPC(
	ctx context.Context,
	nodes []string,
	proposal hatReplication.ClusterWriteCommitProposal,
	factory ClusterWriteCommitGRPCClientFactory,
) (hatReplication.ClusterWriteCommitResult, error) {
	if ctx == nil || factory == nil {
		return hatReplication.ClusterWriteCommitResult{Proposal: proposal}, hatReplication.ErrClusterWriteCommitInvalid
	}
	if len(nodes) == 0 || len(nodes) > hatReplication.MaxClusterWriteCommitNodes {
		return hatReplication.ClusterWriteCommitResult{Proposal: proposal}, hatReplication.ErrClusterWriteCommitInvalid
	}
	normalizedNodes := make([]string, len(nodes))
	clients := make(map[string]*ClusterWriteCommitGRPCClient, len(nodes))
	for index, node := range nodes {
		node = strings.TrimSpace(node)
		if node == "" {
			return hatReplication.ClusterWriteCommitResult{Proposal: proposal}, hatReplication.ErrClusterWriteCommitInvalid
		}
		if _, exists := clients[node]; exists {
			return hatReplication.ClusterWriteCommitResult{Proposal: proposal}, hatReplication.ErrClusterWriteCommitInvalid
		}
		client, err := factory(ctx, node)
		if err != nil {
			return hatReplication.ClusterWriteCommitResult{Proposal: proposal}, err
		}
		if client == nil {
			return hatReplication.ClusterWriteCommitResult{Proposal: proposal}, fmt.Errorf("hatriecache: nil cluster write commit client for %q", node)
		}
		normalizedNodes[index] = node
		clients[node] = client
	}
	lookup := func(node string) (*ClusterWriteCommitGRPCClient, error) {
		client := clients[node]
		if client == nil {
			return nil, fmt.Errorf("hatriecache: cluster write commit client for %q is not configured", node)
		}
		return client, nil
	}
	return hatReplication.ExecuteClusterWriteCommit(
		ctx,
		normalizedNodes,
		proposal,
		func(ctx context.Context, node string, proposal hatReplication.ClusterWriteCommitProposal) error {
			client, err := lookup(node)
			if err != nil {
				return err
			}
			return client.Prepare(ctx, proposal)
		},
		func(ctx context.Context, node string, proposal hatReplication.ClusterWriteCommitProposal) error {
			client, err := lookup(node)
			if err != nil {
				return err
			}
			return client.Commit(ctx, proposal)
		},
		func(ctx context.Context, node string, proposal hatReplication.ClusterWriteCommitProposal) error {
			client, err := lookup(node)
			if err != nil {
				return err
			}
			return client.Abort(ctx, proposal)
		},
	)
}

// NewClusterWriteCommitGRPCClient creates a phase client over conn. The
// connection may be shared with other CacheService calls.
func NewClusterWriteCommitGRPCClient(conn grpc.ClientConnInterface, replicationToken string) *ClusterWriteCommitGRPCClient {
	if conn == nil {
		return &ClusterWriteCommitGRPCClient{}
	}
	return &ClusterWriteCommitGRPCClient{
		client:           hatriecachev1.NewCacheServiceClient(conn),
		replicationToken: strings.TrimSpace(replicationToken),
	}
}

// Prepare records a proposal on the remote participant without making it
// visible.
func (client *ClusterWriteCommitGRPCClient) Prepare(ctx context.Context, proposal hatReplication.ClusterWriteCommitProposal) error {
	return client.call(ctx, hatriecachev1.ClusterWriteCommitPhase_CLUSTER_WRITE_COMMIT_PHASE_PREPARE, proposal)
}

// Commit makes a previously prepared proposal visible on the remote
// participant.
func (client *ClusterWriteCommitGRPCClient) Commit(ctx context.Context, proposal hatReplication.ClusterWriteCommitProposal) error {
	return client.call(ctx, hatriecachev1.ClusterWriteCommitPhase_CLUSTER_WRITE_COMMIT_PHASE_COMMIT, proposal)
}

// Abort releases a previously prepared proposal on the remote participant.
func (client *ClusterWriteCommitGRPCClient) Abort(ctx context.Context, proposal hatReplication.ClusterWriteCommitProposal) error {
	return client.call(ctx, hatriecachev1.ClusterWriteCommitPhase_CLUSTER_WRITE_COMMIT_PHASE_ABORT, proposal)
}

func (client *ClusterWriteCommitGRPCClient) call(ctx context.Context, phase hatriecachev1.ClusterWriteCommitPhase, proposal hatReplication.ClusterWriteCommitProposal) error {
	if client == nil || client.client == nil {
		return errors.New("hatriecache: cluster write commit gRPC client is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if client.replicationToken != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-hatrie-replication-token", client.replicationToken)
	}
	request := &hatriecachev1.ClusterWriteCommitRequest{
		TransactionId: proposal.TransactionID,
		Sequence:      proposal.Sequence,
		FenceToken:    proposal.FenceToken,
		PayloadDigest: append([]byte(nil), proposal.PayloadDigest[:]...),
		Phase:         phase,
	}
	response, err := client.client.ClusterWriteCommit(ctx, request)
	if err != nil {
		return err
	}
	if response == nil || !response.GetOk() {
		if response == nil || strings.TrimSpace(response.GetMessage()) == "" {
			return errors.New("hatriecache: remote cluster write commit phase failed")
		}
		return errors.New(response.GetMessage())
	}
	if response.GetPhase() != phase {
		return fmt.Errorf("hatriecache: cluster write commit response phase %s does not match request phase %s", response.GetPhase(), phase)
	}
	return nil
}

func (server *CacheGRPCServer) ClusterWriteCommit(ctx context.Context, request *hatriecachev1.ClusterWriteCommitRequest) (*hatriecachev1.ClusterWriteCommitResponse, error) {
	ctx = grpcContext(ctx)
	if err := server.requireReplicationAuthorized(ctx); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if server.options.ClusterWriteCommitParticipant == nil {
		return nil, status.Error(codes.Unavailable, "cluster write commit participant is not configured")
	}
	proposal, err := clusterWriteCommitProposalFromProto(request)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	response := &hatriecachev1.ClusterWriteCommitResponse{
		Phase: request.GetPhase(),
	}
	switch request.GetPhase() {
	case hatriecachev1.ClusterWriteCommitPhase_CLUSTER_WRITE_COMMIT_PHASE_PREPARE:
		_, err = server.options.ClusterWriteCommitParticipant.Prepare(proposal)
	case hatriecachev1.ClusterWriteCommitPhase_CLUSTER_WRITE_COMMIT_PHASE_COMMIT:
		_, err = server.options.ClusterWriteCommitParticipant.Commit(proposal)
	case hatriecachev1.ClusterWriteCommitPhase_CLUSTER_WRITE_COMMIT_PHASE_ABORT:
		_, err = server.options.ClusterWriteCommitParticipant.Abort(proposal)
	default:
		return nil, status.Error(codes.InvalidArgument, "cluster write commit phase is required")
	}
	if err != nil {
		response.Message = err.Error()
		return response, nil
	}
	response.Ok = true
	return response, nil
}

func clusterWriteCommitProposalFromProto(request *hatriecachev1.ClusterWriteCommitRequest) (hatReplication.ClusterWriteCommitProposal, error) {
	if request == nil {
		return hatReplication.ClusterWriteCommitProposal{}, errors.New("cluster write commit request is required")
	}
	digest := request.GetPayloadDigest()
	if len(digest) != clusterWriteCommitPayloadDigestSize {
		return hatReplication.ClusterWriteCommitProposal{}, fmt.Errorf("cluster write commit payload digest must be %d bytes", clusterWriteCommitPayloadDigestSize)
	}
	proposal := hatReplication.ClusterWriteCommitProposal{
		TransactionID: request.GetTransactionId(),
		Sequence:      request.GetSequence(),
		FenceToken:    request.GetFenceToken(),
	}
	copy(proposal.PayloadDigest[:], digest)
	return proposal, nil
}
