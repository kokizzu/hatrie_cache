package hatTopology_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"hatrie_cache/hat/hatTopology"
)

func TestTU13MembershipRequiresQuorumAndSurvivesReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "membership.json")
	log, err := hatTopology.NewMembershipLog(hatTopology.SingleNodeTopology("node-a", "http://a"), hatTopology.MembershipLogOptions{
		Path:       path,
		MaxRecords: 8,
	})
	if err != nil {
		t.Fatalf("NewMembershipLog() error = %v", err)
	}

	proposal, err := log.ProposeJoin(hatTopology.TopologyNode{ID: "node-b", Address: "http://b", Region: "asia"})
	if err != nil {
		t.Fatalf("ProposeJoin() error = %v", err)
	}
	if err := log.Commit(proposal, hatTopology.TopologyConsensusDecision{}); !errors.Is(err, hatTopology.ErrTopologyConsensusUnsatisfied) {
		t.Fatalf("Commit() without quorum error = %v, want ErrTopologyConsensusUnsatisfied", err)
	}

	decision := tu13Decision(proposal, "node-a")
	if err := log.Commit(proposal, decision); err != nil {
		t.Fatalf("Commit(join) error = %v", err)
	}
	if got := len(log.Current().Nodes); got != 2 {
		t.Fatalf("joined node count = %d, want 2", got)
	}

	reloaded, err := hatTopology.LoadMembershipLog(path, hatTopology.MembershipLogOptions{MaxRecords: 8})
	if err != nil {
		t.Fatalf("LoadMembershipLog() error = %v", err)
	}
	if got := len(reloaded.Current().Nodes); got != 2 {
		t.Fatalf("reloaded node count = %d, want 2", got)
	}
	if got := len(reloaded.Records()); got != 1 {
		t.Fatalf("reloaded record count = %d, want 1", got)
	}

	leave, err := reloaded.ProposeLeave("node-b")
	if err != nil {
		t.Fatalf("ProposeLeave() error = %v", err)
	}
	if err := reloaded.Commit(leave, tu13Decision(leave, "node-a")); err != nil {
		t.Fatalf("Commit(leave) error = %v", err)
	}
	reloadedAgain, err := hatTopology.LoadMembershipLog(path, hatTopology.MembershipLogOptions{MaxRecords: 8})
	if err != nil {
		t.Fatalf("LoadMembershipLog(after leave) error = %v", err)
	}
	if got := len(reloadedAgain.Current().Nodes); got != 1 {
		t.Fatalf("reloaded node count after leave = %d, want 1", got)
	}
	if got := reloadedAgain.Current().FencingToken; got != 2 {
		t.Fatalf("fencing token after two changes = %d, want 2", got)
	}
}

func TestTU13MembershipRejectsStaleAndUnsafeChanges(t *testing.T) {
	log, err := hatTopology.NewMembershipLog(hatTopology.ClusterTopology{
		Mode:  hatTopology.TopologyModeSharded,
		Nodes: []hatTopology.TopologyNode{{ID: "node-a"}, {ID: "node-b"}},
		Shards: []hatTopology.TopologyShard{{
			ID:       1,
			Primary:  "node-a",
			Replicas: []string{"node-b"},
		}},
	}, hatTopology.MembershipLogOptions{})
	if err != nil {
		t.Fatalf("NewMembershipLog() error = %v", err)
	}
	if _, err := log.ProposeJoin(hatTopology.TopologyNode{ID: "node-a"}); !errors.Is(err, hatTopology.ErrMembershipNodeExists) {
		t.Fatalf("duplicate join error = %v, want ErrMembershipNodeExists", err)
	}
	if _, err := log.ProposeLeave("node-a"); !errors.Is(err, hatTopology.ErrMembershipNodeOwnsShard) {
		t.Fatalf("unsafe leave error = %v, want ErrMembershipNodeOwnsShard", err)
	}

	first, err := log.ProposeJoin(hatTopology.TopologyNode{ID: "node-c"})
	if err != nil {
		t.Fatalf("first ProposeJoin() error = %v", err)
	}
	second, err := log.ProposeJoin(hatTopology.TopologyNode{ID: "node-d"})
	if err != nil {
		t.Fatalf("second ProposeJoin() error = %v", err)
	}
	if err := log.Commit(second, tu13Decision(second, "node-a")); err != nil {
		t.Fatalf("Commit(second) error = %v", err)
	}
	if err := log.Commit(first, tu13Decision(first, "node-a")); !errors.Is(err, hatTopology.ErrTopologyCommitConflict) {
		t.Fatalf("stale Commit() error = %v, want ErrTopologyCommitConflict", err)
	}
}

func TestTU13MembershipRollsBackWhenDurabilityFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "membership.json")
	log, err := hatTopology.NewMembershipLog(hatTopology.SingleNodeTopology("node-a", ""), hatTopology.MembershipLogOptions{Path: path})
	if err != nil {
		t.Fatalf("NewMembershipLog() error = %v", err)
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatalf("Mkdir(%q) error = %v", path, err)
	}
	proposal, err := log.ProposeJoin(hatTopology.TopologyNode{ID: "node-b"})
	if err != nil {
		t.Fatalf("ProposeJoin() error = %v", err)
	}
	if err := log.Commit(proposal, tu13Decision(proposal, "node-a")); err == nil {
		t.Fatal("Commit() succeeded through an unavailable durable path")
	}
	if got := len(log.Current().Nodes); got != 1 {
		t.Fatalf("node count after failed commit = %d, want 1", got)
	}
	if got := len(log.Records()); got != 0 {
		t.Fatalf("record count after failed commit = %d, want 0", got)
	}
}

func TestTU13MembershipBoundsAndCopiesAuditHistory(t *testing.T) {
	log, err := hatTopology.NewMembershipLog(hatTopology.SingleNodeTopology("node-a", ""), hatTopology.MembershipLogOptions{MaxRecords: 1})
	if err != nil {
		t.Fatalf("NewMembershipLog() error = %v", err)
	}
	join, err := log.ProposeJoin(hatTopology.TopologyNode{ID: "node-b"})
	if err != nil {
		t.Fatalf("ProposeJoin() error = %v", err)
	}
	if err := log.Commit(join, tu13Decision(join, "node-a")); err != nil {
		t.Fatalf("Commit(join) error = %v", err)
	}
	leave, err := log.ProposeLeave("node-b")
	if err != nil {
		t.Fatalf("ProposeLeave() error = %v", err)
	}
	if err := log.Commit(leave, tu13Decision(leave, "node-a")); err != nil {
		t.Fatalf("Commit(leave) error = %v", err)
	}
	records := log.Records()
	if len(records) != 1 || records[0].Generation != 2 {
		t.Fatalf("bounded records = %#v, want one generation-2 record", records)
	}
	records[0].Consensus.Voters[0] = "mutated"
	if got := log.Records()[0].Consensus.Voters[0]; got == "mutated" {
		t.Fatal("Records() exposed mutable consensus storage")
	}
	if got := log.Snapshot().Topology.Fingerprint(); got != records[0].CandidateFingerprint {
		t.Fatalf("final record fingerprint = %q, current topology fingerprint = %q", records[0].CandidateFingerprint, got)
	}
}

func tu13Decision(proposal hatTopology.MembershipProposal, voter string) hatTopology.TopologyConsensusDecision {
	decision, err := hatTopology.EvaluateTopologyConsensus(
		hatTopology.TopologyConsensusPolicy{Voters: []string{voter}, Required: 1},
		proposal.Commit.ExpectedFingerprint,
		proposal.Commit.CandidateFingerprint(),
		[]hatTopology.TopologyConsensusVote{{
			NodeID:               voter,
			ExpectedFingerprint:  proposal.Commit.ExpectedFingerprint,
			CandidateFingerprint: proposal.Commit.CandidateFingerprint(),
			Accepted:             true,
		}},
	)
	if err != nil {
		panic(err)
	}
	return decision
}
