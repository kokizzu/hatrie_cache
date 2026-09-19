package hatTopology

import "testing"

func BenchmarkTU13MembershipSnapshot(b *testing.B) {
	journal, err := NewMembershipJournal(MembershipJournalOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := journal.Apply(MembershipChange{
		Operation:          MembershipOperationJoin,
		OperationID:        "join-a",
		ExpectedGeneration: 0,
		FencingToken:       1,
		Node:               TopologyNode{ID: "node-a", Address: "a:8000", Role: "primary"},
	}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		snapshot := journal.Snapshot()
		if len(snapshot.Nodes) != 1 || snapshot.Generation != 1 {
			b.Fatal("membership snapshot lost state")
		}
	}
}

func BenchmarkTU13MembershipMarshal(b *testing.B) {
	journal, err := NewMembershipJournal(MembershipJournalOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := journal.Apply(MembershipChange{
		Operation:          MembershipOperationJoin,
		OperationID:        "join-a",
		ExpectedGeneration: 0,
		FencingToken:       1,
		Node:               TopologyNode{ID: "node-a", Address: "a:8000", Role: "primary"},
	}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		data, err := journal.MarshalBinary()
		if err != nil || len(data) == 0 {
			b.Fatalf("MarshalBinary() = %d bytes/%v", len(data), err)
		}
	}
}
