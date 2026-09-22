package hatReplication

import (
	"errors"
	"reflect"
	"testing"
)

func TestShardConsensusMetadataFencesOwnersAndVotes(t *testing.T) {
	store, err := NewShardConsensusMetadataStore("region-a")
	if err != nil {
		t.Fatal(err)
	}
	first := ShardConsensusMetadata{
		Shard:                   "region-a",
		Owner:                   "node-a",
		FencingToken:            2,
		Term:                    1,
		VotedFor:                "node-a",
		CommitIndex:             10,
		AppliedIndex:            8,
		Frontier:                7,
		ConfigurationGeneration: 2,
	}
	got, err := store.Commit(first)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, first) {
		t.Fatalf("first metadata changed: want=%+v got=%+v", first, got)
	}

	ownerConflict := first
	ownerConflict.Owner = "node-b"
	if _, err := store.Commit(ownerConflict); !errors.Is(err, ErrShardConsensusMetadataOwnerConflict) {
		t.Fatalf("same fencing token should reject a different owner, got %v", err)
	}
	stale := first
	stale.FencingToken = 1
	if _, err := store.Commit(stale); !errors.Is(err, ErrShardConsensusMetadataStale) {
		t.Fatalf("lower fencing token should be stale, got %v", err)
	}
	voteConflict := first
	voteConflict.VotedFor = "node-b"
	if _, err := store.Commit(voteConflict); !errors.Is(err, ErrShardConsensusMetadataVoteConflict) {
		t.Fatalf("same term should not change a vote, got %v", err)
	}
	regressed := first
	regressed.CommitIndex--
	if _, err := store.Commit(regressed); !errors.Is(err, ErrShardConsensusMetadataStale) {
		t.Fatalf("regressed commit index should be stale, got %v", err)
	}

	takeover := ShardConsensusMetadata{
		Shard:                   "region-a",
		Owner:                   "node-b",
		FencingToken:            3,
		Term:                    2,
		VotedFor:                "node-b",
		CommitIndex:             11,
		AppliedIndex:            9,
		Frontier:                8,
		ConfigurationGeneration: 3,
	}
	if _, err := store.Commit(takeover); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(first); !errors.Is(err, ErrShardConsensusMetadataStale) {
		t.Fatalf("old owner should remain fenced after takeover, got %v", err)
	}
}

func TestShardConsensusMetadataSnapshotRoundTripRetainsFencingState(t *testing.T) {
	store, err := NewShardConsensusMetadataStore("region-a")
	if err != nil {
		t.Fatal(err)
	}
	want := ShardConsensusMetadata{
		Shard:                   "region-a",
		Owner:                   "node-a",
		FencingToken:            4,
		Term:                    9,
		VotedFor:                "node-a",
		CommitIndex:             80,
		AppliedIndex:            75,
		Frontier:                70,
		ConfigurationGeneration: 6,
	}
	if _, err := store.Commit(want); err != nil {
		t.Fatal(err)
	}
	encoded, err := store.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalShardConsensusMetadata(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, want) {
		t.Fatalf("metadata changed across binary round trip: want=%+v got=%+v", want, decoded)
	}
	restored, err := NewShardConsensusMetadataStoreFromSnapshot(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if got := restored.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("restored metadata differs: want=%+v got=%+v", want, got)
	}

	next := want
	next.Owner = "node-b"
	next.FencingToken = 5
	next.Term = 10
	next.VotedFor = "node-b"
	if _, err := restored.Commit(next); err != nil {
		t.Fatal(err)
	}
}

func TestShardConsensusMetadataRestoreIsAtomicAndRejectsCorruption(t *testing.T) {
	store, err := NewShardConsensusMetadataStore("region-a")
	if err != nil {
		t.Fatal(err)
	}
	current := ShardConsensusMetadata{
		Shard:        "region-a",
		Owner:        "node-a",
		FencingToken: 1,
		Term:         1,
		VotedFor:     "node-a",
	}
	if _, err := store.Commit(current); err != nil {
		t.Fatal(err)
	}
	encoded, err := current.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	for _, malformed := range [][]byte{
		encoded[:len(encoded)-1],
		append(append([]byte(nil), encoded...), 0),
		[]byte("invalid"),
	} {
		if _, err := UnmarshalShardConsensusMetadata(malformed); !errors.Is(err, ErrShardConsensusMetadataSnapshotInvalid) {
			t.Errorf("expected invalid snapshot error, got %v", err)
		}
	}
	invalid := current
	invalid.AppliedIndex = invalid.CommitIndex + 1
	if err := store.Restore(invalid); !errors.Is(err, ErrShardConsensusMetadataInvalid) {
		t.Fatalf("expected invalid restore error, got %v", err)
	}
	if got := store.Snapshot(); !reflect.DeepEqual(got, current) {
		t.Fatalf("invalid restore changed state: want=%+v got=%+v", current, got)
	}
}

func TestShardConsensusMetadataRejectsInvalidTransitions(t *testing.T) {
	if _, err := NewShardConsensusMetadataStore(""); !errors.Is(err, ErrShardConsensusMetadataInvalid) {
		t.Fatalf("empty shard should be rejected, got %v", err)
	}
	store, err := NewShardConsensusMetadataStore("region-a")
	if err != nil {
		t.Fatal(err)
	}
	invalid := ShardConsensusMetadata{
		Shard:        "region-a",
		Owner:        "node-a",
		FencingToken: 1,
		Term:         0,
	}
	if _, err := store.Commit(invalid); !errors.Is(err, ErrShardConsensusMetadataInvalid) {
		t.Fatalf("zero term should be rejected, got %v", err)
	}
	invalid.Term = 1
	invalid.AppliedIndex = 2
	invalid.CommitIndex = 1
	if _, err := store.Commit(invalid); !errors.Is(err, ErrShardConsensusMetadataInvalid) {
		t.Fatalf("applied index beyond commit index should be rejected, got %v", err)
	}
}
