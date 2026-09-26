package hatCache

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"hatrie_cache/hat/hatReplication"
)

func TestTU047ClusterWriteCommitGRPCParticipantPhases(t *testing.T) {
	participant, err := hatReplication.NewClusterWriteCommitParticipant(hatReplication.ClusterWriteCommitParticipantOptions{MaxRecords: 8})
	if err != nil {
		t.Fatal(err)
	}
	server := NewCacheGRPCServer(nil, CacheGRPCOptions{
		ReplicationAuthToken:          "replication-secret",
		ClusterWriteCommitParticipant: participant,
	})
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	RegisterCacheGRPCServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := NewClusterWriteCommitGRPCClient(conn, "replication-secret")
	proposal := hatReplication.ClusterWriteCommitProposal{
		TransactionID: "tx-grpc-1",
		Sequence:      7,
		FenceToken:    11,
		PayloadDigest: [32]byte{1, 2, 3},
	}
	if err := client.Prepare(ctx, proposal); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if record, ok := participant.Status(proposal.TransactionID); !ok || record.Phase != hatReplication.ClusterWriteCommitParticipantPrepared {
		t.Fatalf("prepared participant status = %#v/%v", record, ok)
	}
	status, found, err := client.Status(ctx, proposal.TransactionID)
	if err != nil {
		t.Fatalf("Status() error = %v", err)
	}
	if !found || status.Proposal != proposal || status.Phase != hatReplication.ClusterWriteCommitParticipantPrepared {
		t.Fatalf("Status() = %#v/%v, want prepared proposal", status, found)
	}
	if _, found, err := client.Status(ctx, "tx-grpc-missing"); err != nil || found {
		t.Fatalf("Status(missing) = %t/%v, want false/nil", found, err)
	}
	if err := client.Commit(ctx, proposal); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if record, ok := participant.Status(proposal.TransactionID); !ok || record.Phase != hatReplication.ClusterWriteCommitParticipantCommitted {
		t.Fatalf("committed participant status = %#v/%v", record, ok)
	}
	status, found, err = client.Status(ctx, proposal.TransactionID)
	if err != nil {
		t.Fatalf("committed Status() error = %v", err)
	}
	if !found || status.Proposal != proposal || status.Phase != hatReplication.ClusterWriteCommitParticipantCommitted {
		t.Fatalf("committed Status() = %#v/%v, want committed proposal", status, found)
	}

	abortProposal := proposal
	abortProposal.TransactionID = "tx-grpc-2"
	if err := client.Prepare(ctx, abortProposal); err != nil {
		t.Fatalf("second Prepare() error = %v", err)
	}
	if err := client.Abort(ctx, abortProposal); err != nil {
		t.Fatalf("Abort() error = %v", err)
	}
	if record, ok := participant.Status(abortProposal.TransactionID); !ok || record.Phase != hatReplication.ClusterWriteCommitParticipantAborted {
		t.Fatalf("aborted participant status = %#v/%v", record, ok)
	}

	coordinatorProposal := proposal
	coordinatorProposal.TransactionID = "tx-grpc-coordinator"
	result, err := ExecuteClusterWriteCommitOverGRPC(ctx, []string{"node-a"}, coordinatorProposal, func(context.Context, string) (*ClusterWriteCommitGRPCClient, error) {
		return client, nil
	})
	if err != nil || !result.Committed || result.CommittedCount != 1 {
		t.Fatalf("ExecuteClusterWriteCommitOverGRPC() = %#v/%v", result, err)
	}
}

func TestTU047ClusterWriteCommitGRPCRequiresReplicationAuthorization(t *testing.T) {
	participant, err := hatReplication.NewClusterWriteCommitParticipant(hatReplication.ClusterWriteCommitParticipantOptions{})
	if err != nil {
		t.Fatal(err)
	}
	server := NewCacheGRPCServer(nil, CacheGRPCOptions{
		ReplicationAuthToken:          "replication-secret",
		ClusterWriteCommitParticipant: participant,
	})
	listener := bufconn.Listen(1 << 20)
	grpcServer := grpc.NewServer()
	RegisterCacheGRPCServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := NewClusterWriteCommitGRPCClient(conn, "wrong-secret")
	err = client.Prepare(ctx, hatReplication.ClusterWriteCommitProposal{TransactionID: "tx-unauthorized"})
	if err == nil {
		t.Fatal("Prepare() error = nil, want authorization failure")
	}
	if _, ok := participant.Status("tx-unauthorized"); ok {
		t.Fatal("unauthorized prepare changed participant state")
	}
	if _, ok, err := client.Status(ctx, "tx-unauthorized"); err == nil || ok {
		t.Fatalf("unauthorized Status() = %t/%v, want authorization failure", ok, err)
	}
}
