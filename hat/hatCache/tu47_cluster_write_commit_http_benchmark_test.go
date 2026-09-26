package hatCache

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"

	"hatrie_cache/hat/hatReplication"
)

func BenchmarkTU047HTTPTransport(b *testing.B) {
	participant, err := hatReplication.NewClusterWriteCommitParticipant(hatReplication.ClusterWriteCommitParticipantOptions{MaxRecords: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	server := httptest.NewServer(&ClusterWriteCommitHTTPHandler{Participant: participant})
	defer server.Close()
	client := NewClusterWriteCommitHTTPClient(server.URL, server.Client(), "")
	factory := func(context.Context, string) (*ClusterWriteCommitHTTPClient, error) {
		return client, nil
	}
	nodes := []string{server.URL}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		proposal := hatReplication.ClusterWriteCommitProposal{
			TransactionID: fmt.Sprintf("benchmark-http-%d", index),
			Sequence:      uint64(index + 1),
		}
		proposal.PayloadDigest[0] = byte(index)
		result, err := ExecuteClusterWriteCommitOverHTTP(context.Background(), nodes, proposal, factory)
		if err != nil || !result.Committed {
			b.Fatalf("result = %#v/%v", result, err)
		}
	}
}

func BenchmarkTU047DirectCoordinatorBaseline(b *testing.B) {
	nodes := []string{"node-a"}
	proposal := hatReplication.ClusterWriteCommitProposal{}
	callback := func(context.Context, string, hatReplication.ClusterWriteCommitProposal) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		proposal.TransactionID = fmt.Sprintf("benchmark-direct-%d", index)
		proposal.Sequence = uint64(index + 1)
		proposal.PayloadDigest[0] = byte(index)
		result, err := hatReplication.ExecuteClusterWriteCommit(context.Background(), nodes, proposal, callback, callback, callback)
		if err != nil || !result.Committed {
			b.Fatalf("result = %#v/%v", result, err)
		}
	}
}
