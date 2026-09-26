package hatTopology

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func c153fTestOwnership() PartitionOwnership {
	return PartitionOwnership{
		ShardID:             9,
		Primary:             "node-a",
		Replicas:            []string{"node-b", "node-c"},
		TopologyFingerprint: "topology-9",
		FencingToken:        11,
	}
}

func c153fTestPolicy(required int) TopologyConsensusPolicy {
	return TopologyConsensusPolicy{
		Voters:   []string{"node-d", "node-b", "node-a", "node-c"},
		Required: required,
	}
}

func c153fTestVote(voter string, ownership PartitionOwnership) PartitionOwnershipConsensusVote {
	return PartitionOwnershipConsensusVote{NodeID: voter, Ownership: ownership, Accepted: true}
}

func TestCollectPartitionOwnershipConsensusBoundsConcurrencyAndSortsResult(t *testing.T) {
	policy := c153fTestPolicy(4)
	expected := c153fTestOwnership()
	var active atomic.Int32
	var maximum atomic.Int32

	result, err := CollectPartitionOwnershipConsensus(context.Background(), policy, expected, func(ctx context.Context, voter string, ownership PartitionOwnership) (PartitionOwnershipConsensusVote, error) {
		current := active.Add(1)
		for {
			previous := maximum.Load()
			if current <= previous || maximum.CompareAndSwap(previous, current) {
				break
			}
		}
		defer active.Add(-1)
		select {
		case <-ctx.Done():
			return PartitionOwnershipConsensusVote{}, ctx.Err()
		case <-time.After(2 * time.Millisecond):
		}
		return c153fTestVote(voter, ownership), nil
	}, PartitionOwnershipConsensusCollectorOptions{MaxConcurrent: 2})
	if err != nil {
		t.Fatalf("CollectPartitionOwnershipConsensus() error = %v", err)
	}
	if !result.Decision.Satisfied {
		t.Fatalf("decision = %#v, want satisfied", result.Decision)
	}
	if got, want := maximum.Load(), int32(2); got > want {
		t.Fatalf("maximum concurrency = %d, want <= %d", got, want)
	}
	if got, want := result.Decision.Acknowledged, []string{"node-a", "node-b", "node-c", "node-d"}; !equalStrings(got, want) {
		t.Fatalf("acknowledged = %#v, want %#v", got, want)
	}
	if got, want := len(result.Votes), 4; got != want {
		t.Fatalf("valid vote count = %d, want %d", got, want)
	}
}

func TestCollectPartitionOwnershipConsensusStopsAfterQuorum(t *testing.T) {
	policy := c153fTestPolicy(1)
	expected := c153fTestOwnership()
	var canceled atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	result, err := CollectPartitionOwnershipConsensus(ctx, policy, expected, func(ctx context.Context, voter string, ownership PartitionOwnership) (PartitionOwnershipConsensusVote, error) {
		if voter == "node-a" {
			return c153fTestVote(voter, ownership), nil
		}
		select {
		case <-ctx.Done():
			canceled.Add(1)
			return PartitionOwnershipConsensusVote{}, ctx.Err()
		case <-time.After(time.Second):
			return c153fTestVote(voter, ownership), nil
		}
	}, PartitionOwnershipConsensusCollectorOptions{MaxConcurrent: 4})
	if err != nil {
		t.Fatalf("CollectPartitionOwnershipConsensus() error = %v", err)
	}
	if !result.Decision.Satisfied {
		t.Fatalf("decision = %#v, want satisfied", result.Decision)
	}
	if canceled.Load() == 0 {
		t.Fatal("collector did not cancel in-flight vote requests after quorum")
	}
}

func TestCollectPartitionOwnershipConsensusAuthenticatesAndBindsVoter(t *testing.T) {
	policy := c153fTestPolicy(4)
	expected := c153fTestOwnership()
	authenticator, err := NewPartitionOwnershipConsensusAuthenticator("key-1", []byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatalf("NewPartitionOwnershipConsensusAuthenticator() error = %v", err)
	}

	result, err := CollectPartitionOwnershipConsensus(context.Background(), policy, expected, func(_ context.Context, voter string, ownership PartitionOwnership) (PartitionOwnershipConsensusVote, error) {
		vote := c153fTestVote(voter, ownership)
		if voter == "node-a" {
			vote.NodeID = "node-b"
			return vote, nil
		}
		return authenticator.Sign(vote)
	}, PartitionOwnershipConsensusCollectorOptions{MaxConcurrent: 4, Authenticator: authenticator})
	if err != nil {
		t.Fatalf("CollectPartitionOwnershipConsensus() error = %v", err)
	}
	if result.Decision.Satisfied {
		t.Fatalf("decision = %#v, mismatched voter must not satisfy quorum", result.Decision)
	}
	if got, want := len(result.Failures), 1; got != want {
		t.Fatalf("failure count = %d, want %d", got, want)
	}
	if got, want := result.Failures[0].NodeID, "node-a"; got != want {
		t.Fatalf("failure node = %q, want %q", got, want)
	}
}

func TestCollectPartitionOwnershipConsensusReturnsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := CollectPartitionOwnershipConsensus(ctx, c153fTestPolicy(2), c153fTestOwnership(), func(context.Context, string, PartitionOwnership) (PartitionOwnershipConsensusVote, error) {
		t.Fatal("fetcher called after context cancellation")
		return PartitionOwnershipConsensusVote{}, nil
	}, PartitionOwnershipConsensusCollectorOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

func TestCollectPartitionOwnershipConsensusRejectsInvalidOptions(t *testing.T) {
	_, err := CollectPartitionOwnershipConsensus(context.Background(), c153fTestPolicy(2), c153fTestOwnership(), func(context.Context, string, PartitionOwnership) (PartitionOwnershipConsensusVote, error) {
		return PartitionOwnershipConsensusVote{}, nil
	}, PartitionOwnershipConsensusCollectorOptions{MaxConcurrent: -1})
	if !errors.Is(err, ErrPartitionOwnershipConsensusCollectorInvalid) {
		t.Fatalf("error = %v, want ErrPartitionOwnershipConsensusCollectorInvalid", err)
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
