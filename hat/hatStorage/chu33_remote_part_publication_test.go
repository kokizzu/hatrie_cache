package hatStorage_test

import (
	"context"
	"errors"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

var chu33PublishErr = errors.New("publish failed")

type chu33PublishStore struct {
	calls     int
	reference hatStorage.RemotePartReference
	token     uint64
	err       error
}

func (store *chu33PublishStore) PublishRemotePart(_ context.Context, reference hatStorage.RemotePartReference, fencingToken uint64) error {
	store.calls++
	store.reference = reference
	store.token = fencingToken
	return store.err
}

func chu33Reference(t testing.TB) hatStorage.RemotePartReference {
	t.Helper()
	reference, err := hatStorage.NewRemotePartReference(
		"s3://bucket/parts/publication",
		"parts/publication.json",
		"sha256:publication",
		128,
	)
	if err != nil {
		t.Fatalf("NewRemotePartReference() error = %v", err)
	}
	return reference
}

func chu33Proposal(t testing.TB) hatStorage.RemotePartPublicationProposal {
	t.Helper()
	proposal, err := hatStorage.NewRemotePartPublicationProposal(chu33Reference(t), hatStorage.RemotePartPublicationOptions{
		PublicationID:               "publication-1",
		ExpectedManifestFingerprint: "manifest-v7",
		FencingToken:                42,
		Voters:                      []string{"node-a", "node-b", "node-c"},
		Required:                    2,
	})
	if err != nil {
		t.Fatalf("NewRemotePartPublicationProposal() error = %v", err)
	}
	return proposal
}

func chu33Vote(proposal hatStorage.RemotePartPublicationProposal, nodeID string, accepted bool) hatStorage.RemotePartPublicationVote {
	return hatStorage.RemotePartPublicationVote{
		NodeID:                      nodeID,
		PublicationID:               proposal.PublicationID,
		ExpectedManifestFingerprint: proposal.ExpectedManifestFingerprint,
		CandidateFingerprint:        proposal.CandidateFingerprint(),
		FencingToken:                proposal.FencingToken,
		Accepted:                    accepted,
	}
}

func TestCHU33DoesNotPublishBeforePartQuorum(t *testing.T) {
	proposal := chu33Proposal(t)
	store := &chu33PublishStore{}
	result, err := hatStorage.PublishRemotePartAfterQuorum(context.Background(), store, proposal, []hatStorage.RemotePartPublicationVote{
		chu33Vote(proposal, "node-a", true),
		chu33Vote(proposal, "node-b", false),
	}, 41)
	if !errors.Is(err, hatStorage.ErrRemotePartPublicationQuorumUnsatisfied) {
		t.Fatalf("PublishRemotePartAfterQuorum() error = %v, want quorum error", err)
	}
	if result.Published || result.Decision.Satisfied {
		t.Fatalf("unsatisfied result = %#v, want unpublished", result)
	}
	if store.calls != 0 {
		t.Fatalf("publish calls = %d, want 0", store.calls)
	}
}

func TestCHU33PublishesAfterMatchingQuorumAndFencing(t *testing.T) {
	proposal := chu33Proposal(t)
	store := &chu33PublishStore{}
	result, err := hatStorage.PublishRemotePartAfterQuorum(context.Background(), store, proposal, []hatStorage.RemotePartPublicationVote{
		chu33Vote(proposal, "node-c", true),
		chu33Vote(proposal, "node-a", true),
	}, 41)
	if err != nil {
		t.Fatalf("PublishRemotePartAfterQuorum() error = %v", err)
	}
	if !result.Published || !result.Decision.Satisfied || len(result.Decision.Acknowledged) != 2 {
		t.Fatalf("published result = %#v, want satisfied publication", result)
	}
	if store.calls != 1 || store.token != 42 || store.reference.ObjectURI() != proposal.Reference.ObjectURI() {
		t.Fatalf("store call = %d token=%d uri=%q, want one fenced publication", store.calls, store.token, store.reference.ObjectURI())
	}
}

func TestCHU33RejectsStaleOrMismatchedVotesBeforeNetwork(t *testing.T) {
	proposal := chu33Proposal(t)
	store := &chu33PublishStore{}
	if _, err := hatStorage.PublishRemotePartAfterQuorum(context.Background(), store, proposal, []hatStorage.RemotePartPublicationVote{
		chu33Vote(proposal, "node-a", true),
		chu33Vote(proposal, "node-b", true),
	}, 42); !errors.Is(err, hatStorage.ErrRemotePartPublicationStale) {
		t.Fatalf("stale proposal error = %v, want stale error", err)
	}
	if store.calls != 0 {
		t.Fatalf("stale publish calls = %d, want 0", store.calls)
	}

	badVote := chu33Vote(proposal, "node-a", true)
	badVote.CandidateFingerprint = "different-part"
	if _, err := hatStorage.EvaluateRemotePartPublication(proposal, []hatStorage.RemotePartPublicationVote{
		badVote,
		chu33Vote(proposal, "node-b", true),
	}); !errors.Is(err, hatStorage.ErrRemotePartPublicationInvalid) {
		t.Fatalf("mismatched vote error = %v, want invalid error", err)
	}
}

func TestCHU33CancellationAndStoreFailureDoNotClaimVisibility(t *testing.T) {
	proposal := chu33Proposal(t)
	votes := []hatStorage.RemotePartPublicationVote{
		chu33Vote(proposal, "node-a", true),
		chu33Vote(proposal, "node-b", true),
	}
	store := &chu33PublishStore{err: chu33PublishErr}
	result, err := hatStorage.PublishRemotePartAfterQuorum(context.Background(), store, proposal, votes, 0)
	if !errors.Is(err, chu33PublishErr) || result.Published {
		t.Fatalf("store failure result=%#v err=%v, want unpublished wrapped failure", result, err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	store = &chu33PublishStore{}
	result, err = hatStorage.PublishRemotePartAfterQuorum(canceled, store, proposal, votes, 0)
	if !errors.Is(err, context.Canceled) || result.Published || store.calls != 0 {
		t.Fatalf("canceled result=%#v err=%v calls=%d, want no publication", result, err, store.calls)
	}
}

func TestCHU33ProposalRequiresBoundedVotersAndPart(t *testing.T) {
	_, err := hatStorage.NewRemotePartPublicationProposal(hatStorage.RemotePartReference{}, hatStorage.RemotePartPublicationOptions{
		PublicationID:               "publication-1",
		ExpectedManifestFingerprint: "manifest-v7",
		FencingToken:                1,
		Voters:                      []string{"node-a"},
	})
	if !errors.Is(err, hatStorage.ErrRemotePartPublicationInvalid) {
		t.Fatalf("invalid reference proposal error = %v, want invalid error", err)
	}
}
