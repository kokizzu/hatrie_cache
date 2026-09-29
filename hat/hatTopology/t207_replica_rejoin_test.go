package hatTopology

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestT207MembershipJournalEvictsAndRejoinsReplica(t *testing.T) {
	journal, err := NewMembershipJournal(MembershipJournalOptions{MaxHistory: 8})
	if err != nil {
		t.Fatalf("NewMembershipJournal() error = %v", err)
	}
	joinPrimary := MembershipChange{
		Operation:          MembershipOperationJoin,
		OperationID:        "join-primary",
		ExpectedGeneration: 0,
		FencingToken:       1,
		Node:               TopologyNode{ID: "node-a", Address: "a:8000", Role: "primary"},
	}
	if _, err := journal.Apply(joinPrimary); err != nil {
		t.Fatalf("Apply(join-primary) error = %v", err)
	}
	joinReplica := MembershipChange{
		Operation:          MembershipOperationJoin,
		OperationID:        "join-replica",
		ExpectedGeneration: 1,
		FencingToken:       2,
		Node:               TopologyNode{ID: "node-b", Address: "b:8000", Role: "replica"},
	}
	if _, err := journal.Apply(joinReplica); err != nil {
		t.Fatalf("Apply(join-replica) error = %v", err)
	}

	evict := MembershipChange{
		Operation:          MembershipOperationEvict,
		OperationID:        "evict-replica-1",
		ExpectedGeneration: 2,
		FencingToken:       3,
		Node:               TopologyNode{ID: "node-b"},
	}
	evictRecord, err := journal.Apply(evict)
	if err != nil {
		t.Fatalf("Apply(evict) error = %v", err)
	}
	if evictRecord.Operation != MembershipOperationEvict || journal.Snapshot().Generation != 3 {
		t.Fatalf("eviction record/state = %#v/%#v", evictRecord, journal.Snapshot())
	}
	if got := journal.Snapshot().Nodes; len(got) != 1 || got[0].ID != "node-a" {
		t.Fatalf("after eviction nodes = %#v, want only node-a", got)
	}

	rejoin := MembershipChange{
		Operation:          MembershipOperationRejoin,
		OperationID:        "rejoin-replica-1",
		ExpectedGeneration: 3,
		FencingToken:       4,
		Node:               TopologyNode{ID: "node-b", Address: "b-new:8000", Role: "replica"},
	}
	rejoinRecord, err := journal.Apply(rejoin)
	if err != nil {
		t.Fatalf("Apply(rejoin) error = %v", err)
	}
	if rejoinRecord.Operation != MembershipOperationRejoin || journal.Snapshot().Generation != 4 {
		t.Fatalf("rejoin record/state = %#v/%#v", rejoinRecord, journal.Snapshot())
	}
	if got := journal.Snapshot().Nodes; len(got) != 2 || got[1].ID != "node-b" || got[1].Address != "b-new:8000" {
		t.Fatalf("after rejoin nodes = %#v, want refreshed node-b", got)
	}
	if idempotent, err := journal.Apply(rejoin); err != nil || idempotent != rejoinRecord {
		t.Fatalf("idempotent rejoin = %#v/%v, want %#v/nil", idempotent, err, rejoinRecord)
	}

	records, err := journal.Replay(2, 0)
	if err != nil {
		t.Fatalf("Replay(after=2) error = %v", err)
	}
	if len(records) != 2 || records[0].Operation != MembershipOperationEvict || records[1].Operation != MembershipOperationRejoin {
		t.Fatalf("Replay(after=2) = %#v, want evict/rejoin", records)
	}
}

func TestT207MembershipJournalRejectsWrongOperationAndPersistsRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "membership.hmm")
	journal, err := OpenMembershipJournal(path, MembershipJournalOptions{MaxHistory: 8})
	if err != nil {
		t.Fatalf("OpenMembershipJournal() error = %v", err)
	}
	join := MembershipChange{
		Operation:          MembershipOperationJoin,
		OperationID:        "join-a",
		ExpectedGeneration: 0,
		FencingToken:       10,
		Node:               TopologyNode{ID: "node-a", Address: "a:8000"},
	}
	if _, err := journal.Apply(join); err != nil {
		t.Fatalf("Apply(join) error = %v", err)
	}
	if _, err := journal.Apply(MembershipChange{
		Operation:          MembershipOperationEvict,
		OperationID:        "evict-missing",
		ExpectedGeneration: 1,
		FencingToken:       11,
		Node:               TopologyNode{ID: "node-missing"},
	}); !errors.Is(err, ErrMembershipJournalNodeMissing) {
		t.Fatalf("evict missing error = %v, want node missing", err)
	}
	if _, err := journal.Apply(MembershipChange{
		Operation:          MembershipOperationRejoin,
		OperationID:        "rejoin-existing",
		ExpectedGeneration: 1,
		FencingToken:       11,
		Node:               TopologyNode{ID: "node-a"},
	}); !errors.Is(err, ErrMembershipJournalNodeExists) {
		t.Fatalf("rejoin existing error = %v, want node exists", err)
	}

	if _, err := journal.Apply(MembershipChange{
		Operation:          MembershipOperationEvict,
		OperationID:        "evict-a",
		ExpectedGeneration: 1,
		FencingToken:       11,
		Node:               TopologyNode{ID: "node-a"},
	}); !errors.Is(err, ErrMembershipJournalLastNode) {
		t.Fatalf("evict last node error = %v, want last-node error", err)
	}
	if restored, err := OpenMembershipJournal(path, MembershipJournalOptions{MaxHistory: 8}); err != nil {
		t.Fatalf("OpenMembershipJournal(restored) error = %v", err)
	} else if snapshot := restored.Snapshot(); snapshot.Generation != 1 || len(snapshot.Nodes) != 1 || snapshot.Nodes[0].ID != "node-a" {
		t.Fatalf("restored snapshot = %#v, want original join only", snapshot)
	}
}

func BenchmarkT207EvictRejoin(b *testing.B) {
	benchmarkT207MembershipTransition(b, MembershipOperationEvict, MembershipOperationRejoin)
}

func BenchmarkT207LeaveJoinControl(b *testing.B) {
	benchmarkT207MembershipTransition(b, MembershipOperationLeave, MembershipOperationJoin)
}

func benchmarkT207MembershipTransition(b *testing.B, removeOperation, addOperation string) {
	b.Helper()
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		journal, err := NewMembershipJournal(MembershipJournalOptions{MaxHistory: 8})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := journal.Apply(MembershipChange{
			Operation: MembershipOperationJoin, OperationID: "join-a", FencingToken: 1,
			Node: TopologyNode{ID: "node-a"},
		}); err != nil {
			b.Fatal(err)
		}
		if _, err := journal.Apply(MembershipChange{
			Operation: MembershipOperationJoin, OperationID: "join-b", ExpectedGeneration: 1, FencingToken: 2,
			Node: TopologyNode{ID: "node-b"},
		}); err != nil {
			b.Fatal(err)
		}
		if _, err := journal.Apply(MembershipChange{
			Operation: removeOperation, OperationID: "remove-b", ExpectedGeneration: 2, FencingToken: 3,
			Node: TopologyNode{ID: "node-b"},
		}); err != nil {
			b.Fatal(err)
		}
		if _, err := journal.Apply(MembershipChange{
			Operation: addOperation, OperationID: "add-b", ExpectedGeneration: 3, FencingToken: 4,
			Node: TopologyNode{ID: "node-b", Address: "b-new:8000", Role: "replica"},
		}); err != nil {
			b.Fatal(err)
		}
	}
}
