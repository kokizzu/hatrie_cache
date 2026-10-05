package hatTopology

import (
	"errors"
	"testing"
	"time"
)

type tu12TopologyProvider struct {
	topology ClusterTopology
}

func (provider tu12TopologyProvider) TopologySnapshot() ClusterTopology {
	return provider.topology
}

func TestElectionStoreProposeFailoverBuildsFencedReplicaPromotion(t *testing.T) {
	now := time.Unix(100, 0)
	provider := tu12TopologyProvider{topology: ClusterTopology{
		Version:      Version,
		Mode:         TopologyModeSharded,
		Self:         "node-a",
		FencingToken: 7,
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "a"},
			{ID: "node-b", Address: "b"},
			{ID: "node-c", Address: "c"},
		},
		Shards: []TopologyShard{{ID: 1, Primary: "node-a", Replicas: []string{"node-b", "node-c"}}},
	}}
	store := NewElectionStore(provider, ElectionOptions{Timeout: time.Minute, Now: func() time.Time { return now }})
	if err := store.MarkOffline("node-a"); err != nil {
		t.Fatalf("MarkOffline() error = %v", err)
	}

	proposal, err := store.ProposeFailover(1, FailoverOptions{})
	if err != nil {
		t.Fatalf("ProposeFailover() error = %v", err)
	}
	if proposal.PreviousPrimary != "node-a" || proposal.CandidatePrimary != "node-b" {
		t.Fatalf("proposal = %#v, want node-a -> node-b", proposal)
	}
	if proposal.Commit.Topology.FencingToken != 8 {
		t.Fatalf("proposal fencing token = %d, want 8", proposal.Commit.Topology.FencingToken)
	}
	if proposal.Commit.Topology.Shards[0].Primary != "node-b" {
		t.Fatalf("candidate primary = %q, want node-b", proposal.Commit.Topology.Shards[0].Primary)
	}
	if proposal.Commit.ExpectedFingerprint != provider.topology.Fingerprint() {
		t.Fatalf("expected fingerprint = %q, want current topology fingerprint", proposal.Commit.ExpectedFingerprint)
	}
	if err := proposal.Commit.Validate(provider.topology); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if provider.topology.FencingToken != 7 || provider.topology.Shards[0].Primary != "node-a" {
		t.Fatalf("provider topology mutated: %#v", provider.topology)
	}
}

func TestElectionStoreProposeFailoverRejectsUnsafeRequests(t *testing.T) {
	now := time.Unix(100, 0)
	newStore := func(t *testing.T) *ElectionStore {
		t.Helper()
		provider := tu12TopologyProvider{topology: ClusterTopology{
			Version: Version, Mode: TopologyModeSharded,
			Nodes:  []TopologyNode{{ID: "node-a"}, {ID: "node-b"}, {ID: "node-c"}},
			Shards: []TopologyShard{{ID: 1, Primary: "node-a", Replicas: []string{"node-b", "node-c"}}},
		}}
		return NewElectionStore(provider, ElectionOptions{Timeout: time.Minute, Now: func() time.Time { return now }})
	}

	store := newStore(t)
	if _, err := store.ProposeFailover(1, FailoverOptions{}); !errors.Is(err, ErrFailoverPrimaryHealthy) {
		t.Fatalf("healthy primary error = %v, want %v", err, ErrFailoverPrimaryHealthy)
	}

	store = newStore(t)
	if err := store.MarkOffline("node-a"); err != nil {
		t.Fatalf("MarkOffline(primary) error = %v", err)
	}
	if err := store.MarkOffline("node-b"); err != nil {
		t.Fatalf("MarkOffline(replica) error = %v", err)
	}
	if err := store.MarkOffline("node-c"); err != nil {
		t.Fatalf("MarkOffline(replica) error = %v", err)
	}
	if _, err := store.ProposeFailover(1, FailoverOptions{}); !errors.Is(err, ErrFailoverNoCandidate) {
		t.Fatalf("no candidate error = %v, want %v", err, ErrFailoverNoCandidate)
	}

	store = newStore(t)
	if err := store.MarkOffline("node-a"); err != nil {
		t.Fatalf("MarkOffline(primary) error = %v", err)
	}
	if err := store.MarkOffline("node-c"); err != nil {
		t.Fatalf("MarkOffline(replica) error = %v", err)
	}
	if _, err := store.ProposeFailover(1, FailoverOptions{MinHealthyOwners: 2}); !errors.Is(err, ErrFailoverInsufficientHealthyOwners) {
		t.Fatalf("healthy-owner threshold error = %v, want %v", err, ErrFailoverInsufficientHealthyOwners)
	}
	if _, err := store.ProposeFailover(1, FailoverOptions{MinHealthyOwners: -1}); !errors.Is(err, ErrFailoverInvalid) {
		t.Fatalf("negative threshold error = %v, want %v", err, ErrFailoverInvalid)
	}
}

func TestElectionStoreProposeFailoverSupportsFullReplicaAndRejectsTokenOverflow(t *testing.T) {
	now := time.Unix(100, 0)
	provider := tu12TopologyProvider{topology: ClusterTopology{
		Version:      Version,
		Mode:         TopologyModeFullReplica,
		Self:         "node-a",
		FencingToken: 9,
		Nodes:        []TopologyNode{{ID: "node-a"}, {ID: "node-b"}},
	}}
	store := NewElectionStore(provider, ElectionOptions{Timeout: time.Minute, Now: func() time.Time { return now }})
	if err := store.MarkOffline("node-a"); err != nil {
		t.Fatalf("MarkOffline() error = %v", err)
	}
	proposal, err := store.ProposeFailover(0, FailoverOptions{})
	if err != nil {
		t.Fatalf("full replica ProposeFailover() error = %v", err)
	}
	if proposal.CandidatePrimary != "node-b" || proposal.Commit.Topology.Self != "node-b" || proposal.Commit.Topology.FencingToken != 10 {
		t.Fatalf("full replica proposal = %#v, want node-b/token 10", proposal)
	}

	provider.topology.FencingToken = ^uint64(0)
	store = NewElectionStore(provider, ElectionOptions{Timeout: time.Minute, Now: func() time.Time { return now }})
	if err := store.MarkOffline("node-a"); err != nil {
		t.Fatalf("MarkOffline(overflow) error = %v", err)
	}
	if _, err := store.ProposeFailover(0, FailoverOptions{}); !errors.Is(err, ErrFailoverInvalid) {
		t.Fatalf("overflow error = %v, want %v", err, ErrFailoverInvalid)
	}
}
