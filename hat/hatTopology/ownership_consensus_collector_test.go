package hatTopology

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestPartitionOwnershipConsensusCollector(t *testing.T) {
	expected := PartitionOwnership{
		ShardID:             7,
		Primary:             "node-a",
		Replicas:            []string{"node-b"},
		TopologyFingerprint: "topology-fingerprint",
		FencingToken:        11,
	}
	collector, err := NewPartitionOwnershipConsensusCollector(TopologyConsensusPolicy{
		Voters: []string{"node-c", "node-a", "node-b"},
	}, expected)
	if err != nil {
		t.Fatalf("NewPartitionOwnershipConsensusCollector() error = %v", err)
	}

	done, err := collector.AddVote(PartitionOwnershipConsensusVote{NodeID: "node-c", Accepted: true, Ownership: expected})
	if err != nil || done {
		t.Fatalf("first AddVote() = done %v, error %v, want pending", done, err)
	}
	if _, err := collector.Decision(); !errors.Is(err, ErrPartitionOwnershipConsensusPending) {
		t.Fatalf("pending Decision() error = %v, want ErrPartitionOwnershipConsensusPending", err)
	}

	done, err = collector.AddVote(PartitionOwnershipConsensusVote{NodeID: "node-a", Accepted: true, Ownership: expected})
	if err != nil || !done {
		t.Fatalf("quorum AddVote() = done %v, error %v, want terminal", done, err)
	}
	decision, err := collector.Decision()
	if err != nil {
		t.Fatalf("Decision() error = %v", err)
	}
	if !decision.Satisfied || decision.Required != 2 {
		t.Fatalf("decision = %#v, want satisfied majority", decision)
	}
	want := []string{"node-a", "node-c"}
	if !reflect.DeepEqual(decision.Acknowledged, want) {
		t.Fatalf("acknowledged = %v, want %v", decision.Acknowledged, want)
	}
	decision.Acknowledged[0] = "mutated"
	again, err := collector.Decision()
	if err != nil || !reflect.DeepEqual(again.Acknowledged, want) {
		t.Fatalf("Decision() snapshot isolation = %v/%v, want %v/nil", again.Acknowledged, err, want)
	}
	if _, err := collector.AddVote(PartitionOwnershipConsensusVote{NodeID: "node-b", Accepted: true, Ownership: expected}); !errors.Is(err, ErrPartitionOwnershipConsensusClosed) {
		t.Fatalf("closed AddVote() error = %v, want ErrPartitionOwnershipConsensusClosed", err)
	}
	if err := collector.Reset(); err != nil {
		t.Fatalf("Reset() error = %v", err)
	}
	if collector.Complete() {
		t.Fatal("collector remained complete after Reset")
	}
}

func TestPartitionOwnershipConsensusCollectorStopsWhenQuorumImpossible(t *testing.T) {
	expected := PartitionOwnership{ShardID: 1, Primary: "node-a", TopologyFingerprint: "fingerprint"}
	collector, err := NewPartitionOwnershipConsensusCollector(TopologyConsensusPolicy{
		Voters:   []string{"node-a", "node-b", "node-c"},
		Required: 2,
	}, expected)
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range []string{"node-a", "node-b"} {
		done, err := collector.AddVote(PartitionOwnershipConsensusVote{NodeID: node, Ownership: expected})
		if err != nil {
			t.Fatal(err)
		}
		if node == "node-b" && !done {
			t.Fatal("collector remained pending after quorum became impossible")
		}
	}
	decision, err := collector.Decision()
	if err != nil {
		t.Fatal(err)
	}
	if decision.Satisfied || len(decision.Rejected) != 2 {
		t.Fatalf("decision = %#v, want unsatisfied with two rejections", decision)
	}
}

func TestPartitionOwnershipConsensusCollectorAcceptsConcurrentVotes(t *testing.T) {
	expected := PartitionOwnership{ShardID: 3, Primary: "node-a", TopologyFingerprint: "fingerprint"}
	collector, err := NewPartitionOwnershipConsensusCollector(TopologyConsensusPolicy{
		Voters:   []string{"node-a", "node-b", "node-c", "node-d"},
		Required: 3,
	}, expected)
	if err != nil {
		t.Fatal(err)
	}
	votes := []PartitionOwnershipConsensusVote{
		{NodeID: "node-d", Accepted: true, Ownership: expected},
		{NodeID: "node-b", Accepted: true, Ownership: expected},
		{NodeID: "node-a", Accepted: true, Ownership: expected},
		{NodeID: "node-c", Accepted: true, Ownership: expected},
	}
	errorsCh := make(chan error, len(votes))
	var group sync.WaitGroup
	for _, vote := range votes {
		vote := vote
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := collector.AddVote(vote)
			errorsCh <- err
		}()
	}
	group.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil && !errors.Is(err, ErrPartitionOwnershipConsensusClosed) {
			t.Fatalf("concurrent AddVote() error = %v", err)
		}
	}
	decision, err := collector.Decision()
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Satisfied || len(decision.Acknowledged) != 3 {
		t.Fatalf("decision = %#v, want satisfied with three acknowledgements", decision)
	}
}
