package hatStorage_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

var chu33BenchmarkSink bool

type chu33NoopPublishStore struct{}

func (chu33NoopPublishStore) PublishRemotePart(_ context.Context, _ hatStorage.RemotePartReference, _ uint64) error {
	return nil
}

func BenchmarkCHU33DirectRemotePartPublish(b *testing.B) {
	proposal := chu33Proposal(b)
	store := chu33NoopPublishStore{}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := store.PublishRemotePart(context.Background(), proposal.Reference, proposal.FencingToken); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCHU33QuorumRemotePartPublish(b *testing.B) {
	proposal := chu33Proposal(b)
	votes := []hatStorage.RemotePartPublicationVote{
		chu33Vote(proposal, "node-a", true),
		chu33Vote(proposal, "node-b", true),
		chu33Vote(proposal, "node-c", false),
	}
	store := chu33NoopPublishStore{}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		result, err := hatStorage.PublishRemotePartAfterQuorum(context.Background(), store, proposal, votes, 0)
		if err != nil {
			b.Fatal(err)
		}
		chu33BenchmarkSink = result.Published
	}
}
