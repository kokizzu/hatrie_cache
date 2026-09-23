package hatReplication

import (
	"errors"
	"sync"
	"testing"
)

func TestReplicaRecoveryEvictionRejectsOldIncarnationAndPlansRejoin(t *testing.T) {
	protocol, err := NewReplicaRecoveryProtocol(ReplicaRecoveryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	initial, err := protocol.Admit(ReplicaRecoveryNode{
		NodeID:              "node-a",
		Incarnation:         1,
		LastAppliedSequence: 100,
		StorageGeneration:   7,
	}, 0, 1)
	if err != nil {
		t.Fatalf("Admit() error = %v", err)
	}
	if initial.Generation != 1 || initial.FencingToken != 1 || initial.Nodes[0].State != ReplicaRecoveryStateActive {
		t.Fatalf("initial snapshot = %+v", initial)
	}

	evicted, err := protocol.Evict("node-a", initial.Generation, 2)
	if err != nil {
		t.Fatalf("Evict() error = %v", err)
	}
	if evicted.Generation != 2 || evicted.FencingToken != 2 || evicted.Nodes[0].State != ReplicaRecoveryStateEvicted {
		t.Fatalf("evicted snapshot = %+v", evicted)
	}

	source := ReplicaRecoverySource{
		CurrentJournalSequence: 110,
		RetainedFromSequence:   100,
		StorageGeneration:      7,
	}
	if _, err := protocol.EvaluateRejoin(ReplicaRejoinRequest{
		NodeID:              "node-a",
		Incarnation:         1,
		LastAppliedSequence: 105,
		StorageGeneration:   7,
	}, source); !errors.Is(err, ErrReplicaRecoveryStaleIncarnation) {
		t.Fatalf("old incarnation error = %v", err)
	}

	plan, err := protocol.EvaluateRejoin(ReplicaRejoinRequest{
		NodeID:              "node-a",
		Incarnation:         2,
		LastAppliedSequence: 105,
		StorageGeneration:   7,
	}, source)
	if err != nil {
		t.Fatalf("EvaluateRejoin() error = %v", err)
	}
	if plan.Decision != ReplicaRecoveryDecisionResume || plan.RequiredSequence != 105 || plan.ExpectedGeneration != evicted.Generation {
		t.Fatalf("rejoin plan = %+v", plan)
	}

	committed, err := protocol.CommitRejoin(plan, 110, 7, 3)
	if err != nil {
		t.Fatalf("CommitRejoin() error = %v", err)
	}
	if committed.Generation != 3 || committed.FencingToken != 3 || committed.Nodes[0].State != ReplicaRecoveryStateActive || committed.Nodes[0].Incarnation != 2 || committed.Nodes[0].LastAppliedSequence != 110 {
		t.Fatalf("committed snapshot = %+v", committed)
	}
	if _, err := protocol.EvaluateRejoin(ReplicaRejoinRequest{
		NodeID:              "node-a",
		Incarnation:         1,
		LastAppliedSequence: 110,
		StorageGeneration:   7,
	}, source); !errors.Is(err, ErrReplicaRecoveryStaleIncarnation) {
		t.Fatalf("replayed old incarnation error = %v", err)
	}
}

func TestReplicaRecoveryRequiresBootstrapForStaleStateAndRejectsFutureState(t *testing.T) {
	protocol, err := NewReplicaRecoveryProtocol(ReplicaRecoveryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := protocol.Admit(ReplicaRecoveryNode{
		NodeID:              "node-a",
		Incarnation:         4,
		LastAppliedSequence: 100,
		StorageGeneration:   7,
	}, 0, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := protocol.EvaluateRejoin(ReplicaRejoinRequest{
		NodeID:              "node-a",
		Incarnation:         4,
		LastAppliedSequence: 99,
		StorageGeneration:   7,
	}, ReplicaRecoverySource{CurrentJournalSequence: 110, RetainedFromSequence: 90, StorageGeneration: 7}); !errors.Is(err, ErrReplicaRecoveryStaleState) {
		t.Fatalf("older same-incarnation state error = %v", err)
	}
	if _, err := protocol.EvaluateRejoin(ReplicaRejoinRequest{
		NodeID:              "node-a",
		Incarnation:         4,
		LastAppliedSequence: 111,
		StorageGeneration:   7,
	}, ReplicaRecoverySource{CurrentJournalSequence: 110, RetainedFromSequence: 90, StorageGeneration: 7}); !errors.Is(err, ErrReplicaRecoveryFutureState) {
		t.Fatalf("future state error = %v", err)
	}
	plan, err := protocol.EvaluateRejoin(ReplicaRejoinRequest{
		NodeID:              "node-a",
		Incarnation:         4,
		LastAppliedSequence: 100,
		StorageGeneration:   7,
	}, ReplicaRecoverySource{CurrentJournalSequence: 110, RetainedFromSequence: 101, StorageGeneration: 7})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Decision != ReplicaRecoveryDecisionBootstrap || plan.Reason == "" {
		t.Fatalf("stale WAL plan = %+v", plan)
	}
}

func TestReplicaRecoveryUnknownNodeBootstrapsAndStalePlansCannotCommit(t *testing.T) {
	protocol, err := NewReplicaRecoveryProtocol(ReplicaRecoveryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := protocol.EvaluateRejoin(ReplicaRejoinRequest{
		NodeID:              "node-new",
		Incarnation:         1,
		LastAppliedSequence: 0,
		StorageGeneration:   7,
	}, ReplicaRecoverySource{CurrentJournalSequence: 20, RetainedFromSequence: 0, StorageGeneration: 7})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Decision != ReplicaRecoveryDecisionBootstrap || plan.RequiredSequence != 20 {
		t.Fatalf("unknown-node plan = %+v", plan)
	}
	committed, err := protocol.CommitRejoin(plan, 20, 7, 1)
	if err != nil {
		t.Fatalf("CommitRejoin() error = %v", err)
	}
	if committed.Nodes[0].State != ReplicaRecoveryStateActive || committed.Nodes[0].LastAppliedSequence != 20 {
		t.Fatalf("unknown-node commit = %+v", committed)
	}
	if _, err := protocol.CommitRejoin(plan, 20, 7, 2); !errors.Is(err, ErrReplicaRecoveryGeneration) {
		t.Fatalf("stale plan commit error = %v", err)
	}
	if _, err := protocol.Evict("node-new", committed.Generation, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := protocol.EvaluateRejoin(ReplicaRejoinRequest{
		NodeID:              "node-new",
		Incarnation:         2,
		LastAppliedSequence: 20,
		StorageGeneration:   8,
	}, ReplicaRecoverySource{CurrentJournalSequence: 25, RetainedFromSequence: 20, StorageGeneration: 7}); !errors.Is(err, ErrReplicaRecoveryFutureState) {
		t.Fatalf("future storage generation error = %v", err)
	}
}

func TestReplicaRecoveryConcurrentReadDecisionsRemainConsistent(t *testing.T) {
	protocol, err := NewReplicaRecoveryProtocol(ReplicaRecoveryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := protocol.Admit(ReplicaRecoveryNode{
		NodeID:              "node-a",
		Incarnation:         1,
		LastAppliedSequence: 100,
		StorageGeneration:   7,
	}, 0, 1); err != nil {
		t.Fatal(err)
	}
	request := ReplicaRejoinRequest{NodeID: "node-a", Incarnation: 1, LastAppliedSequence: 100, StorageGeneration: 7}
	source := ReplicaRecoverySource{CurrentJournalSequence: 110, RetainedFromSequence: 90, StorageGeneration: 7}
	var group sync.WaitGroup
	for index := 0; index < 32; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			plan, err := protocol.EvaluateRejoin(request, source)
			if err != nil || plan.Decision != ReplicaRecoveryDecisionResume {
				t.Errorf("EvaluateRejoin() = %+v, %v", plan, err)
			}
			snapshot := protocol.Snapshot()
			if len(snapshot.Nodes) != 1 || snapshot.Nodes[0].State != ReplicaRecoveryStateActive {
				t.Errorf("Snapshot() = %+v", snapshot)
			}
		}()
	}
	group.Wait()
}
