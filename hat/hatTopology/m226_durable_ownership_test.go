package hatTopology_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"hatrie_cache/hat/hatTopology"
)

func TestM226DurablePartitionOwnershipRoundTripAndMonotoneCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ownership.meta")
	store, err := hatTopology.OpenDurablePartitionOwnershipStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Snapshot(); ok {
		t.Fatal("new store has a snapshot, want empty")
	}
	decision := m226OwnershipDecision(11, "node-a", "topology-1")
	record, err := store.Commit(1, 100, decision)
	if err != nil {
		t.Fatalf("first Commit() error = %v", err)
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".hatrie-ownership-") {
			t.Fatalf("temporary ownership file remains after commit: %q", entry.Name())
		}
	}
	if record.Sequence != 1 || record.Frontier != 100 || record.Decision.Ownership.FencingToken != 11 || !record.Decision.Satisfied {
		t.Fatalf("first record = %#v", record)
	}
	record.Decision.Voters[0] = "mutated"
	record.Decision.Ownership.Replicas[0] = "mutated"
	snapshot, ok := store.Snapshot()
	if !ok || snapshot.Decision.Voters[0] != "node-a" || snapshot.Decision.Ownership.Replicas[0] != "node-b" {
		t.Fatalf("Snapshot() ownership = %#v, want detached copy", snapshot)
	}

	reopened, err := hatTopology.OpenDurablePartitionOwnershipStore(path)
	if err != nil {
		t.Fatal(err)
	}
	restored, ok := reopened.Snapshot()
	if !ok || restored.Sequence != 1 || restored.Frontier != 100 || restored.Decision.Ownership.Primary != "node-a" {
		t.Fatalf("reopened Snapshot() = %#v, want persisted record", restored)
	}
	if _, err := reopened.Commit(2, 101, m226OwnershipDecision(11, "node-a", "topology-1")); err != nil {
		t.Fatalf("monotone Commit() error = %v", err)
	}
	if _, err := reopened.Commit(2, 102, m226OwnershipDecision(11, "node-a", "topology-1")); !errors.Is(err, hatTopology.ErrDurablePartitionOwnershipSequence) {
		t.Fatalf("duplicate sequence error = %v, want %v", err, hatTopology.ErrDurablePartitionOwnershipSequence)
	}
	if _, err := reopened.Commit(3, 99, m226OwnershipDecision(11, "node-a", "topology-1")); !errors.Is(err, hatTopology.ErrDurablePartitionOwnershipFrontier) {
		t.Fatalf("regressed frontier error = %v, want %v", err, hatTopology.ErrDurablePartitionOwnershipFrontier)
	}
}

func TestM226DurablePartitionOwnershipRejectsInvalidAndStaleDecisions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ownership.meta")
	store, err := hatTopology.OpenDurablePartitionOwnershipStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(0, 1, m226OwnershipDecision(11, "node-a", "topology-1")); !errors.Is(err, hatTopology.ErrDurablePartitionOwnershipSequence) {
		t.Fatalf("zero sequence error = %v, want %v", err, hatTopology.ErrDurablePartitionOwnershipSequence)
	}
	unsatisfied := m226OwnershipDecision(11, "node-a", "topology-1")
	unsatisfied.Satisfied = false
	if _, err := store.Commit(1, 1, unsatisfied); !errors.Is(err, hatTopology.ErrPartitionOwnershipConsensusUnsatisfied) {
		t.Fatalf("unsatisfied decision error = %v, want %v", err, hatTopology.ErrPartitionOwnershipConsensusUnsatisfied)
	}
	if _, err := store.Commit(1, 1, m226OwnershipDecision(11, "node-a", "topology-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(2, 2, m226OwnershipDecision(10, "node-c", "topology-0")); !errors.Is(err, hatTopology.ErrDurablePartitionOwnershipStale) {
		t.Fatalf("stale fencing error = %v, want %v", err, hatTopology.ErrDurablePartitionOwnershipStale)
	}
	wrongShard := m226OwnershipDecision(11, "node-a", "topology-1")
	wrongShard.Ownership.ShardID = 8
	if _, err := store.Commit(2, 2, wrongShard); !errors.Is(err, hatTopology.ErrDurablePartitionOwnershipShard) {
		t.Fatalf("wrong shard error = %v, want %v", err, hatTopology.ErrDurablePartitionOwnershipShard)
	}
}

func TestM226DurablePartitionOwnershipDetectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ownership.meta")
	store, err := hatTopology.OpenDurablePartitionOwnershipStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(1, 1, m226OwnershipDecision(11, "node-a", "topology-1")); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := hatTopology.OpenDurablePartitionOwnershipStore(path); !errors.Is(err, hatTopology.ErrDurablePartitionOwnershipChecksum) {
		t.Fatalf("corrupt open error = %v, want %v", err, hatTopology.ErrDurablePartitionOwnershipChecksum)
	}
}

func TestM226DurablePartitionOwnershipRejectsMalformedRecords(t *testing.T) {
	for name, data := range map[string][]byte{
		"empty":     nil,
		"truncated": []byte("HPO1\x01"),
		"oversized": make([]byte, 1<<20+1),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ownership.meta")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := hatTopology.OpenDurablePartitionOwnershipStore(path); !errors.Is(err, hatTopology.ErrDurablePartitionOwnershipInvalid) {
				t.Fatalf("malformed open error = %v, want %v", err, hatTopology.ErrDurablePartitionOwnershipInvalid)
			}
		})
	}
}

func m226OwnershipDecision(fencingToken uint64, primary, fingerprint string) hatTopology.PartitionOwnershipConsensusDecision {
	return hatTopology.PartitionOwnershipConsensusDecision{
		Ownership: hatTopology.PartitionOwnership{
			ShardID:             7,
			Primary:             primary,
			Replicas:            []string{"node-b"},
			TopologyFingerprint: fingerprint,
			FencingToken:        fencingToken,
		},
		Voters:       []string{"node-a", "node-b", "node-c"},
		Required:     2,
		Acknowledged: []string{"node-a", "node-b"},
		Satisfied:    true,
	}
}
