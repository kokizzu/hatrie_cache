package hatTopology

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

func durableMembershipInitialTopology() ClusterTopology {
	return ClusterTopology{
		Mode:  TopologyModeFullReplica,
		Self:  "node-a",
		Nodes: []TopologyNode{{ID: "node-a", Address: "127.0.0.1:9001", Role: "primary"}},
	}
}

func TestDurableMembershipPersistsGenerationsAndReplays(t *testing.T) {
	path := filepath.Join(t.TempDir(), "membership.json")
	store, err := OpenDurableMembershipStore(path, durableMembershipInitialTopology())
	if err != nil {
		t.Fatalf("OpenDurableMembershipStore() error = %v", err)
	}
	initial := store.Snapshot()
	if initial.Generation != 0 || initial.FencingToken != 0 || initial.Topology.FencingToken != 0 || len(initial.Topology.Nodes) != 1 {
		t.Fatalf("initial snapshot = %+v", initial)
	}

	joined, err := store.Apply(MembershipChange{
		ID:                 "join-node-b",
		Kind:               MembershipJoin,
		ExpectedGeneration: 0,
		FencingToken:       1,
		Node:               TopologyNode{ID: "node-b", Address: "127.0.0.1:9002", Role: "replica"},
	})
	if err != nil {
		t.Fatalf("join error = %v", err)
	}
	if joined.Replayed || joined.Snapshot.Generation != 1 || joined.Snapshot.FencingToken != 1 || joined.Snapshot.Topology.FencingToken != 1 || len(joined.Snapshot.Topology.Nodes) != 2 {
		t.Fatalf("join result = %+v", joined)
	}

	left, err := store.Apply(MembershipChange{
		ID:                 "leave-node-b",
		Kind:               MembershipLeave,
		ExpectedGeneration: 1,
		FencingToken:       2,
		NodeID:             "node-b",
	})
	if err != nil {
		t.Fatalf("leave error = %v", err)
	}
	if left.Snapshot.Generation != 2 || left.Snapshot.FencingToken != 2 || left.Snapshot.Topology.FencingToken != 2 || len(left.Snapshot.Topology.Nodes) != 1 {
		t.Fatalf("leave result = %+v", left)
	}
	replayedLeave, err := store.Apply(MembershipChange{
		ID:                 "leave-node-b",
		Kind:               MembershipLeave,
		ExpectedGeneration: 1,
		FencingToken:       2,
		NodeID:             "node-b",
	})
	if err != nil {
		t.Fatalf("leave retry error = %v", err)
	}
	if !replayedLeave.Replayed || replayedLeave.Snapshot.Generation != 2 {
		t.Fatalf("leave retry result = %+v", replayedLeave)
	}

	reopened, err := OpenDurableMembershipStore(path, ClusterTopology{})
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	got := reopened.Snapshot()
	if got.Generation != 2 || got.FencingToken != 2 || got.Topology.FencingToken != 2 || len(got.Topology.Nodes) != 1 || got.Topology.Nodes[0].ID != "node-a" {
		t.Fatalf("reopened snapshot = %+v", got)
	}
	history := reopened.History()
	if len(history) != 2 || history[0].Kind != MembershipJoin || history[1].Kind != MembershipLeave {
		t.Fatalf("history = %#v", history)
	}
}

func TestDurableMembershipCarriesInitialFencingToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "membership.json")
	initial := durableMembershipInitialTopology()
	initial.FencingToken = 7
	store, err := OpenDurableMembershipStore(path, initial)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot := store.Snapshot(); snapshot.FencingToken != 7 || snapshot.Topology.FencingToken != 7 {
		t.Fatalf("initial fencing snapshot = %+v", snapshot)
	}
	result, err := store.Apply(MembershipChange{
		ID:           "join-node-b",
		Kind:         MembershipJoin,
		FencingToken: 8,
		Node:         TopologyNode{ID: "node-b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Snapshot.FencingToken != 8 || result.Snapshot.Topology.FencingToken != 8 {
		t.Fatalf("updated fencing snapshot = %+v", result.Snapshot)
	}
}

func TestDurableMembershipRejectsStaleAndUnsafeChanges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "membership.json")
	store, err := OpenDurableMembershipStore(path, durableMembershipInitialTopology())
	if err != nil {
		t.Fatal(err)
	}
	join := MembershipChange{ID: "join-node-b", Kind: MembershipJoin, FencingToken: 1, Node: TopologyNode{ID: "node-b"}}
	if _, err := store.Apply(join); err != nil {
		t.Fatalf("initial join error = %v", err)
	}
	join.ExpectedGeneration = 0
	join.ID = "join-node-c"
	join.Node.ID = "node-c"
	join.FencingToken = 2
	if _, err := store.Apply(join); !errors.Is(err, ErrMembershipGenerationConflict) {
		t.Fatalf("stale generation error = %v, want ErrMembershipGenerationConflict", err)
	}

	join.ExpectedGeneration = 1
	join.ID = "join-node-c-stale-token"
	join.FencingToken = 1
	if _, err := store.Apply(join); !errors.Is(err, ErrMembershipFencingStale) {
		t.Fatalf("stale fencing error = %v, want ErrMembershipFencingStale", err)
	}
}

func TestDurableMembershipRetriesByChangeIDWithoutNewGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "membership.json")
	store, err := OpenDurableMembershipStore(path, durableMembershipInitialTopology())
	if err != nil {
		t.Fatal(err)
	}
	change := MembershipChange{
		ID:           "join-node-b",
		Kind:         MembershipJoin,
		FencingToken: 1,
		Node:         TopologyNode{ID: "node-b", Address: "127.0.0.1:9002"},
	}
	first, err := store.Apply(change)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Apply(change)
	if err != nil {
		t.Fatalf("retry error = %v", err)
	}
	if !second.Replayed || second.Record.Generation != first.Record.Generation || second.Snapshot.Generation != 1 {
		t.Fatalf("retry result = %+v, first = %+v", second, first)
	}
	change.Node.Address = "different"
	if _, err := store.Apply(change); !errors.Is(err, ErrMembershipChangeConflict) {
		t.Fatalf("conflicting retry error = %v, want ErrMembershipChangeConflict", err)
	}

	leave := MembershipChange{
		ID:                 "leave-node-b",
		Kind:               MembershipLeave,
		ExpectedGeneration: 1,
		FencingToken:       2,
		Node:               TopologyNode{ID: "node-b"},
	}
	firstLeave, err := store.Apply(leave)
	if err != nil {
		t.Fatalf("leave error = %v", err)
	}
	secondLeave, err := store.Apply(leave)
	if err != nil {
		t.Fatalf("leave retry error = %v", err)
	}
	if !secondLeave.Replayed || firstLeave.Record.Generation != secondLeave.Record.Generation {
		t.Fatalf("leave retry result = %+v, first = %+v", secondLeave, firstLeave)
	}
}

func TestDurableMembershipProtectsReferencedAndSelfNodes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "membership.json")
	topology := durableMembershipInitialTopology()
	topology.Mode = TopologyModeSharded
	topology.Self = "node-z"
	topology.Nodes = append(topology.Nodes, TopologyNode{ID: "node-z", Address: "127.0.0.1:9003", Role: "replica"})
	topology.Shards = []TopologyShard{{ID: 1, Primary: "node-a"}}
	store, err := OpenDurableMembershipStore(path, topology)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		id   string
		want error
	}{
		{name: "referenced", id: "node-a", want: ErrMembershipNodeReferenced},
		{name: "self", id: "node-z", want: ErrMembershipSelfLeave},
	} {
		t.Run(test.name, func(t *testing.T) {
			change := MembershipChange{ID: "leave-" + test.name, Kind: MembershipLeave, FencingToken: 1, NodeID: test.id}
			_, err := store.Apply(change)
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}

	join, err := store.Apply(MembershipChange{ID: "join-node-b", Kind: MembershipJoin, FencingToken: 1, Node: TopologyNode{ID: "node-b"}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(join.Snapshot.Topology.Shards, topology.Shards) {
		t.Fatalf("join changed shard ownership: %#v", join.Snapshot.Topology.Shards)
	}
}

func TestDurableMembershipRejectsCorruptAndMismatchedState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "membership.json")
	if err := writeJSONFileAtomic(path, map[string]any{"version": 999}); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDurableMembershipStore(path, ClusterTopology{}); !errors.Is(err, ErrMembershipCorrupt) {
		t.Fatalf("version error = %v, want ErrMembershipCorrupt", err)
	}

	validPath := filepath.Join(t.TempDir(), "valid.json")
	if _, err := OpenDurableMembershipStore(validPath, durableMembershipInitialTopology()); err != nil {
		t.Fatal(err)
	}
	other := durableMembershipInitialTopology()
	other.Nodes[0].Address = "different"
	if _, err := OpenDurableMembershipStore(validPath, other); !errors.Is(err, ErrMembershipInitialMismatch) {
		t.Fatalf("initial mismatch error = %v, want ErrMembershipInitialMismatch", err)
	}
}
