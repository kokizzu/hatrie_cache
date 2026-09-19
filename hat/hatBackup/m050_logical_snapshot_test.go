package hatBackup

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestM050LogicalSnapshotCanonicalRoundTrip(t *testing.T) {
	createdAt := time.Unix(1_700_000_000, 123).UTC()
	manifest := LogicalSnapshotManifest{
		Version:           LogicalSnapshotManifestVersion,
		SnapshotID:        "snapshot-1",
		CreatedAt:         createdAt,
		BundleBackupID:    "backup-7",
		StorageGeneration: 3,
		JournalSequence:   42,
		SourceOffsets: []SourceOffsetCheckpoint{
			{SourceID: "orders", Partition: "1", Offset: 8, Epoch: 2},
			{SourceID: "orders", Partition: "0", Offset: 9, Epoch: 2},
		},
		Frontiers: []FrontierCheckpoint{
			{ID: "orders", Lower: 40, Upper: 42, Generation: 4},
			{ID: "users", Lower: 41, Upper: 42, Generation: 5},
		},
		Subscriptions: []SubscriptionCheckpoint{
			{ID: "users-sub", FrontierID: "users", AsOf: 41, AckedSequence: 42},
		},
	}

	normalized, err := manifest.Normalize()
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if got := normalized.SourceOffsets[0].Partition; got != "0" {
		t.Fatalf("source order = %q, want 0", got)
	}

	encoded, err := manifest.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	repeated, err := manifest.MarshalBinary()
	if err != nil {
		t.Fatalf("second MarshalBinary() error = %v", err)
	}
	if !bytes.Equal(encoded, repeated) {
		t.Fatal("MarshalBinary() is not deterministic")
	}

	decoded, err := DecodeLogicalSnapshotManifest(encoded)
	if err != nil {
		t.Fatalf("DecodeLogicalSnapshotManifest() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, normalized) {
		t.Fatalf("decoded manifest = %#v, want %#v", decoded, normalized)
	}
}

func TestM050LogicalSnapshotValidationAndRestorePlan(t *testing.T) {
	manifest := validM050Manifest()
	plan, err := PlanLogicalSnapshotRestore(manifest, LogicalSnapshotJournalCoverage{
		HasEntries:    true,
		FirstSequence: 42,
		LastSequence:  47,
	})
	if err != nil {
		t.Fatalf("PlanLogicalSnapshotRestore() error = %v", err)
	}
	wantSteps := []LogicalSnapshotRestoreStep{
		RestoreImmutableStorage,
		RestoreSourceOffsets,
		RestoreFrontiers,
		RestoreSubscriptions,
		ReplayJournal,
	}
	if !reflect.DeepEqual(plan.Steps, wantSteps) || plan.ReplayFrom != 43 || plan.ReplayThrough != 47 {
		t.Fatalf("restore plan = %#v, want steps %#v and replay 43..47", plan, wantSteps)
	}

	_, err = PlanLogicalSnapshotRestore(manifest, LogicalSnapshotJournalCoverage{
		HasEntries:    true,
		FirstSequence: 44,
		LastSequence:  47,
	})
	if !errors.Is(err, ErrLogicalSnapshotHistoryGap) {
		t.Fatalf("history gap error = %v, want ErrLogicalSnapshotHistoryGap", err)
	}
}

func TestM050LogicalSnapshotRejectsInvalidInputWithoutMutation(t *testing.T) {
	invalid := validM050Manifest()
	invalid.Subscriptions[0].FrontierID = "missing"
	if _, err := invalid.Normalize(); !errors.Is(err, ErrLogicalSnapshotInvalid) {
		t.Fatalf("invalid subscription error = %v, want ErrLogicalSnapshotInvalid", err)
	}

	destination := validM050Manifest()
	payload, err := destination.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	corrupt := append([]byte(nil), payload...)
	corrupt[len(corrupt)-1] ^= 0xff
	before := destination
	if err := destination.UnmarshalBinary(corrupt); !errors.Is(err, ErrLogicalSnapshotInvalid) {
		t.Fatalf("UnmarshalBinary(corrupt) error = %v, want ErrLogicalSnapshotInvalid", err)
	}
	if !reflect.DeepEqual(destination, before) {
		t.Fatal("UnmarshalBinary(corrupt) mutated the destination")
	}

	duplicate := validM050Manifest()
	duplicate.SourceOffsets = append(duplicate.SourceOffsets, duplicate.SourceOffsets[0])
	if _, err := duplicate.Normalize(); !errors.Is(err, ErrLogicalSnapshotInvalid) {
		t.Fatalf("duplicate source offset error = %v, want ErrLogicalSnapshotInvalid", err)
	}
}

func validM050Manifest() LogicalSnapshotManifest {
	return LogicalSnapshotManifest{
		Version:           LogicalSnapshotManifestVersion,
		SnapshotID:        "snapshot-1",
		CreatedAt:         time.Unix(1_700_000_000, 0).UTC(),
		BundleBackupID:    "backup-7",
		StorageGeneration: 3,
		JournalSequence:   42,
		SourceOffsets:     []SourceOffsetCheckpoint{{SourceID: "orders", Partition: "0", Offset: 9}},
		Frontiers:         []FrontierCheckpoint{{ID: "orders", Lower: 40, Upper: 42, Generation: 4}},
		Subscriptions:     []SubscriptionCheckpoint{{ID: "orders-sub", FrontierID: "orders", AsOf: 40, AckedSequence: 42}},
	}
}
