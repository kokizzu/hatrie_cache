package hatCache

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"hatrie_cache/hat/hatTopology"
)

func topologyForCommit(fencingToken uint64) ClusterTopology {
	return ClusterTopology{
		Version:      1,
		Mode:         TopologyModeFullReplica,
		FencingToken: fencingToken,
		Nodes: []TopologyNode{
			{ID: "node-a", Address: "http://node-a"},
			{ID: "node-b", Address: "http://node-b"},
			{ID: "node-c", Address: "http://node-c"},
		},
	}
}

func TestTopologyStoreApplyConsensusCommitIsAtomicAndRetrySafe(t *testing.T) {
	store, err := NewTopologyStore(topologyForCommit(1))
	if err != nil {
		t.Fatal(err)
	}
	expected := store.Fingerprint()
	commit, err := hatTopology.NewTopologyCommit(expected, topologyForCommit(2))
	if err != nil {
		t.Fatal(err)
	}
	candidate := commit.CandidateFingerprint()
	decision, err := hatTopology.EvaluateTopologyConsensus(
		hatTopology.TopologyConsensusPolicy{Voters: []string{"node-a", "node-b", "node-c"}},
		expected,
		candidate,
		[]hatTopology.TopologyConsensusVote{
			{NodeID: "node-a", ExpectedFingerprint: expected, CandidateFingerprint: candidate, Accepted: true},
			{NodeID: "node-b", ExpectedFingerprint: expected, CandidateFingerprint: candidate, Accepted: true},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.ApplyConsensusCommit(commit, decision)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Applied || result.AlreadyApplied || result.CurrentFingerprint != candidate {
		t.Fatalf("first commit result = %+v", result)
	}
	if store.FencingToken() != 2 {
		t.Fatalf("fencing token = %d, want 2", store.FencingToken())
	}

	retry, err := store.ApplyConsensusCommit(commit, decision)
	if err != nil {
		t.Fatal(err)
	}
	if retry.Applied || !retry.AlreadyApplied || retry.CurrentFingerprint != candidate {
		t.Fatalf("retry result = %+v", retry)
	}
}

func TestTopologyCommitJSONRoundTripPreservesProposal(t *testing.T) {
	commit, err := hatTopology.NewTopologyCommit("current", topologyForCommit(2))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(commit)
	if err != nil {
		t.Fatal(err)
	}
	var decoded hatTopology.TopologyCommit
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ExpectedFingerprint != commit.ExpectedFingerprint || decoded.CandidateFingerprint() != commit.CandidateFingerprint() {
		t.Fatalf("decoded commit = %+v, want %+v", decoded, commit)
	}
}

func TestTopologyStoreApplyCommitRejectsConflictsAndStaleFences(t *testing.T) {
	store, err := NewTopologyStore(topologyForCommit(1))
	if err != nil {
		t.Fatal(err)
	}
	wrong, err := hatTopology.NewTopologyCommit("wrong-fingerprint", topologyForCommit(2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyCommit(wrong); !errors.Is(err, hatTopology.ErrTopologyCommitConflict) {
		t.Fatalf("wrong expected fingerprint error = %v", err)
	}
	if store.FencingToken() != 1 {
		t.Fatalf("fencing token changed after conflict: %d", store.FencingToken())
	}

	staleTopology := topologyForCommit(1)
	staleTopology.Nodes[0].Region = "asia"
	stale, err := hatTopology.NewTopologyCommit(store.Fingerprint(), staleTopology)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyCommit(stale); !errors.Is(err, hatTopology.ErrTopologyCommitStale) {
		t.Fatalf("stale fence error = %v", err)
	}
}

func TestEvaluateTopologyConsensusBindsVotesAndUsesMajorityDefault(t *testing.T) {
	decision, err := hatTopology.EvaluateTopologyConsensus(
		hatTopology.TopologyConsensusPolicy{Voters: []string{"node-c", "node-a", "node-b"}},
		"current",
		"candidate",
		[]hatTopology.TopologyConsensusVote{
			{NodeID: "node-a", ExpectedFingerprint: "current", CandidateFingerprint: "candidate", Accepted: true},
			{NodeID: "node-b", ExpectedFingerprint: "other", CandidateFingerprint: "candidate", Accepted: true},
			{NodeID: "node-c", ExpectedFingerprint: "current", CandidateFingerprint: "candidate", Accepted: false},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Satisfied || decision.Required != 2 {
		t.Fatalf("decision = %+v, want unsatisfied majority", decision)
	}
	if len(decision.Acknowledged) != 1 || decision.Acknowledged[0] != "node-a" {
		t.Fatalf("acknowledgements = %v", decision.Acknowledged)
	}
	if len(decision.Rejected) != 2 || decision.Rejected[0] != "node-b" || decision.Rejected[1] != "node-c" {
		t.Fatalf("rejections = %v", decision.Rejected)
	}
}

func TestTopologyStoreApplyConsensusCommitRejectsUnboundDecision(t *testing.T) {
	store, err := NewTopologyStore(topologyForCommit(1))
	if err != nil {
		t.Fatal(err)
	}
	commit, err := hatTopology.NewTopologyCommit(store.Fingerprint(), topologyForCommit(2))
	if err != nil {
		t.Fatal(err)
	}
	decision := hatTopology.TopologyConsensusDecision{
		ExpectedFingerprint:  store.Fingerprint(),
		CandidateFingerprint: "different",
		Voters:               []string{"node-a", "node-b", "node-c"},
		Required:             2,
		Acknowledged:         []string{"node-a", "node-b"},
		Satisfied:            true,
	}
	if _, err := store.ApplyConsensusCommit(commit, decision); !errors.Is(err, hatTopology.ErrTopologyConsensusInvalid) {
		t.Fatalf("unbound decision error = %v", err)
	}
	if store.FencingToken() != 1 {
		t.Fatalf("fencing token changed after invalid decision: %d", store.FencingToken())
	}
}

func TestTopologyStoreApplyCommitPersistsAndReloads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "topology.json")
	store, err := OpenTopologyStore(path, topologyForCommit(1))
	if err != nil {
		t.Fatal(err)
	}
	commit, err := hatTopology.NewTopologyCommit(store.Fingerprint(), topologyForCommit(2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ApplyCommit(commit); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadTopology(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Fingerprint() != commit.CandidateFingerprint() || loaded.FencingToken != 2 {
		t.Fatalf("loaded topology = %+v, want candidate", loaded)
	}
}

func TestTopologyStoreApplyCommitConcurrentRetriesInstallOnce(t *testing.T) {
	store, err := NewTopologyStore(topologyForCommit(1))
	if err != nil {
		t.Fatal(err)
	}
	commit, err := hatTopology.NewTopologyCommit(store.Fingerprint(), topologyForCommit(2))
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan TopologyCommitResult, 4)
	errorsCh := make(chan error, 4)
	var group sync.WaitGroup
	for i := 0; i < 4; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := store.ApplyCommit(commit)
			results <- result
			errorsCh <- err
		}()
	}
	group.Wait()
	close(results)
	close(errorsCh)
	applied := 0
	alreadyApplied := 0
	for result := range results {
		if result.Applied {
			applied++
		}
		if result.AlreadyApplied {
			alreadyApplied++
		}
	}
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if applied != 1 || alreadyApplied != 3 {
		t.Fatalf("applied = %d, already applied = %d, want 1 and 3", applied, alreadyApplied)
	}
}
