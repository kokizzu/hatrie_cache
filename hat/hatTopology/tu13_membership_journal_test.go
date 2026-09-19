package hatTopology

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestMembershipJournalJoinLeaveGenerationAndIdempotency(t *testing.T) {
	journal, err := NewMembershipJournal(MembershipJournalOptions{MaxHistory: 4})
	if err != nil {
		t.Fatalf("NewMembershipJournal() error = %v", err)
	}
	joinA := MembershipChange{
		Operation:          MembershipOperationJoin,
		OperationID:        "join-a",
		ExpectedGeneration: 0,
		FencingToken:       1,
		Node:               TopologyNode{ID: "node-a", Address: "a:8000", Role: "primary"},
	}
	recordA, err := journal.Apply(joinA)
	if err != nil {
		t.Fatalf("Apply(join-a) error = %v", err)
	}
	if recordA.Sequence != 1 || recordA.Generation != 1 {
		t.Fatalf("join-a record = %#v, want sequence/generation 1", recordA)
	}
	idempotent, err := journal.Apply(joinA)
	if err != nil || idempotent != recordA {
		t.Fatalf("idempotent Apply(join-a) = %#v/%v, want %#v/nil", idempotent, err, recordA)
	}

	if _, err := journal.Apply(MembershipChange{
		Operation:          MembershipOperationJoin,
		OperationID:        "join-b",
		ExpectedGeneration: 1,
		FencingToken:       2,
		Node:               TopologyNode{ID: "node-b", Address: "b:8000", Role: "replica"},
	}); err != nil {
		t.Fatalf("Apply(join-b) error = %v", err)
	}
	leave, err := journal.Apply(MembershipChange{
		Operation:          MembershipOperationLeave,
		OperationID:        "leave-a",
		ExpectedGeneration: 2,
		FencingToken:       3,
		Node:               TopologyNode{ID: "node-a"},
	})
	if err != nil {
		t.Fatalf("Apply(leave-a) error = %v", err)
	}
	if leave.Sequence != 3 || leave.Generation != 3 {
		t.Fatalf("leave-a record = %#v, want sequence/generation 3", leave)
	}
	snapshot := journal.Snapshot()
	if snapshot.Generation != 3 || snapshot.FencingToken != 3 || len(snapshot.Nodes) != 1 || snapshot.Nodes[0].ID != "node-b" {
		t.Fatalf("snapshot = %#v, want node-b at generation 3", snapshot)
	}
	records, err := journal.Replay(1, 0)
	if err != nil || len(records) != 2 || records[0].OperationID != "join-b" || records[1].OperationID != "leave-a" {
		t.Fatalf("Replay(after=1) = %#v/%v, want join-b/leave-a", records, err)
	}
	if _, err := journal.Apply(MembershipChange{
		Operation:          MembershipOperationLeave,
		OperationID:        "leave-b",
		ExpectedGeneration: 3,
		FencingToken:       4,
		Node:               TopologyNode{ID: "node-b"},
	}); !errors.Is(err, ErrMembershipJournalLastNode) {
		t.Fatalf("Apply(leave-last) error = %v, want last-node error", err)
	}
}

func TestMembershipJournalRejectsStaleGenerationFenceAndOperationConflicts(t *testing.T) {
	journal, err := NewMembershipJournal(MembershipJournalOptions{})
	if err != nil {
		t.Fatalf("NewMembershipJournal() error = %v", err)
	}
	change := MembershipChange{
		Operation:          MembershipOperationJoin,
		OperationID:        "join-a",
		ExpectedGeneration: 0,
		FencingToken:       1,
		Node:               TopologyNode{ID: "node-a"},
	}
	if _, err := journal.Apply(change); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	change.OperationID = "join-b"
	change.Node.ID = "node-b"
	if _, err := journal.Apply(change); !errors.Is(err, ErrMembershipJournalGenerationConflict) {
		t.Fatalf("stale generation error = %v, want generation conflict", err)
	}
	change.ExpectedGeneration = 1
	if _, err := journal.Apply(change); !errors.Is(err, ErrMembershipJournalFenceStale) {
		t.Fatalf("stale fence error = %v, want fence stale", err)
	}
	conflict := change
	conflict.OperationID = "join-a"
	conflict.FencingToken = 2
	conflict.Node.ID = "node-b"
	if _, err := journal.Apply(conflict); !errors.Is(err, ErrMembershipJournalOperationConflict) {
		t.Fatalf("operation conflict error = %v, want operation conflict", err)
	}
}

func TestMembershipJournalPersistsAndRestoresAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "membership.hmm")
	journal, err := OpenMembershipJournal(path, MembershipJournalOptions{MaxHistory: 8})
	if err != nil {
		t.Fatalf("OpenMembershipJournal(new) error = %v", err)
	}
	if _, err := journal.Apply(MembershipChange{
		Operation:          MembershipOperationJoin,
		OperationID:        "join-a",
		ExpectedGeneration: 0,
		FencingToken:       11,
		Node:               TopologyNode{ID: "node-a", Region: "east"},
	}); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	restored, err := OpenMembershipJournal(path, MembershipJournalOptions{MaxHistory: 8})
	if err != nil {
		t.Fatalf("OpenMembershipJournal(restore) error = %v", err)
	}
	if got := restored.Snapshot(); got.Generation != 1 || got.FencingToken != 11 || len(got.Nodes) != 1 || got.Nodes[0].Region != "east" || len(got.Records) != 1 {
		t.Fatalf("restored snapshot = %#v, want one durable record", got)
	}
	data, err := restored.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	data[len(data)-1] ^= 0x80
	if err := restored.UnmarshalBinary(data); !errors.Is(err, ErrMembershipJournalChecksum) {
		t.Fatalf("corrupt UnmarshalBinary() error = %v, want checksum", err)
	}
	if got := restored.Snapshot(); got.Generation != 1 || len(got.Nodes) != 1 {
		t.Fatalf("state changed after corrupt restore: %#v", got)
	}
}

func TestMembershipJournalBoundsHistoryAndRejectsInvalidChanges(t *testing.T) {
	journal, err := NewMembershipJournal(MembershipJournalOptions{MaxHistory: 2})
	if err != nil {
		t.Fatalf("NewMembershipJournal() error = %v", err)
	}
	for index, node := range []string{"node-a", "node-b", "node-c"} {
		if _, err := journal.Apply(MembershipChange{
			Operation:          MembershipOperationJoin,
			OperationID:        "join-" + node,
			ExpectedGeneration: uint64(index),
			FencingToken:       uint64(index + 1),
			Node:               TopologyNode{ID: node},
		}); err != nil {
			t.Fatalf("Apply(%s) error = %v", node, err)
		}
	}
	if got := journal.Snapshot(); got.CompactedThrough != 1 || len(got.Records) != 2 {
		t.Fatalf("bounded snapshot = %#v, want compacted sequence 1 and two records", got)
	}
	if _, err := journal.Replay(0, 0); !errors.Is(err, ErrMembershipJournalHistoryGap) {
		t.Fatalf("Replay(history gap) error = %v, want history gap", err)
	}
	if _, err := journal.Apply(MembershipChange{Operation: "update", OperationID: "bad", ExpectedGeneration: 3, FencingToken: 4, Node: TopologyNode{ID: "node-x"}}); !errors.Is(err, ErrMembershipJournalInvalidChange) {
		t.Fatalf("invalid operation error = %v, want invalid change", err)
	}
	if _, err := journal.Apply(MembershipChange{Operation: MembershipOperationLeave, OperationID: "missing", ExpectedGeneration: 3, FencingToken: 4, Node: TopologyNode{ID: "node-x"}}); !errors.Is(err, ErrMembershipJournalNodeMissing) {
		t.Fatalf("missing node error = %v, want node missing", err)
	}
}
