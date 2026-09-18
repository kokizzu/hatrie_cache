//go:build tu36_backup_rotation

package hatCache

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTU36BackupRotationPolicyFirstBackupIsDue(t *testing.T) {
	policy := BackupRotationPolicy{JournalSequenceDelta: 100}
	decision, err := policy.Decide(time.Unix(100, 0), BackupRotationState{})
	if err != nil {
		t.Fatalf("Decide() error = %v", err)
	}
	if !decision.Due || decision.Reason != BackupRotationReasonInitial {
		t.Fatalf("decision = %#v, want initial backup due", decision)
	}
}

func TestTU36BackupRotationPolicyJournalDeltaHonorsMinimumInterval(t *testing.T) {
	policy := BackupRotationPolicy{
		MinimumInterval:      time.Hour,
		JournalSequenceDelta: 100,
	}
	last := time.Unix(100, 0)

	decision, err := policy.Decide(last.Add(30*time.Minute), BackupRotationState{
		HasBackup:              true,
		LastCreatedAt:          last,
		LastJournalSequence:    10,
		CurrentJournalSequence: 110,
	})
	if err != nil {
		t.Fatalf("early Decide() error = %v", err)
	}
	if decision.Due {
		t.Fatalf("early decision = %#v, want not due", decision)
	}

	decision, err = policy.Decide(last.Add(time.Hour), BackupRotationState{
		HasBackup:              true,
		LastCreatedAt:          last,
		LastJournalSequence:    10,
		CurrentJournalSequence: 110,
	})
	if err != nil {
		t.Fatalf("due Decide() error = %v", err)
	}
	if !decision.Due || decision.Reason != BackupRotationReasonJournalDelta {
		t.Fatalf("decision = %#v, want journal-delta due", decision)
	}
}

func TestTU36BackupRotationPolicyMaximumIntervalTriggers(t *testing.T) {
	policy := BackupRotationPolicy{
		MinimumInterval:      time.Hour,
		MaximumInterval:      2 * time.Hour,
		JournalSequenceDelta: 100,
	}
	last := time.Unix(100, 0)
	decision, err := policy.Decide(last.Add(2*time.Hour), BackupRotationState{
		HasBackup:              true,
		LastCreatedAt:          last,
		LastJournalSequence:    10,
		CurrentJournalSequence: 10,
	})
	if err != nil {
		t.Fatalf("Decide() error = %v", err)
	}
	if !decision.Due || decision.Reason != BackupRotationReasonMaximumInterval {
		t.Fatalf("decision = %#v, want maximum-interval due", decision)
	}
}

func TestTU36BackupRotationPolicyRejectsRegressedJournal(t *testing.T) {
	policy := BackupRotationPolicy{JournalSequenceDelta: 1}
	_, err := policy.Decide(time.Unix(101, 0), BackupRotationState{
		HasBackup:              true,
		LastCreatedAt:          time.Unix(100, 0),
		LastJournalSequence:    10,
		CurrentJournalSequence: 9,
	})
	if !errors.Is(err, ErrBackupRotationJournalRegressed) {
		t.Fatalf("Decide() error = %v, want %v", err, ErrBackupRotationJournalRegressed)
	}
}

func TestTU36BackupRotationPolicyRejectsInvalidOptions(t *testing.T) {
	tests := []BackupRotationPolicy{
		{MinimumInterval: -time.Second, MaximumInterval: time.Hour},
		{MaximumInterval: time.Second, MinimumInterval: 2 * time.Second},
		{Retain: -1, MaximumInterval: time.Hour},
		{RetainBytes: -1, MaximumInterval: time.Hour},
		{},
	}
	for index, policy := range tests {
		if err := policy.Validate(); err == nil {
			t.Errorf("case %d Validate() error = nil", index)
		}
	}
}

func TestTU36BackupRepositoryRetainedBytesCountsUniqueObjects(t *testing.T) {
	manifests := map[string]BackupBundleManifest{
		"latest": {Files: []BackupBundleFile{
			{SHA256: "shared", Size: 7},
			{SHA256: "latest", Size: 11},
		}},
		"parent": {Files: []BackupBundleFile{
			{SHA256: "shared", Size: 7},
			{SHA256: "parent", Size: 13},
		}},
	}
	if got := backupRepositoryRetainedBytes(manifests); got != 31 {
		t.Fatalf("backupRepositoryRetainedBytes() = %d, want 31", got)
	}
}

func TestTU36IncrementalBackupRepositoryIfDueSkipsAndThenRotates(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("rotation:key", "value")
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	repository := filepath.Join(t.TempDir(), "repository")
	createdAt := time.Unix(100, 0)
	options := BackupBundleOptions{
		Mode:            BackupModePebbleIncremental,
		PersistentStore: store,
		DirtyTracker:    NewLevelDBDirtyTracker(),
		CreatedAt:       createdAt,
	}
	policy := BackupRotationPolicy{MaximumInterval: time.Hour, Retain: 2}
	first, err := CreateIncrementalBackupRepositoryIfDue(repository, trie, nil, options, policy)
	if err != nil {
		t.Fatalf("first rotation error = %v", err)
	}
	if !first.Created || first.Decision.Reason != BackupRotationReasonInitial {
		t.Fatalf("first rotation result = %#v, want created initial backup", first)
	}

	options.CreatedAt = createdAt.Add(30 * time.Minute)
	second, err := CreateIncrementalBackupRepositoryIfDue(repository, trie, nil, options, policy)
	if err != nil {
		t.Fatalf("early rotation error = %v", err)
	}
	if second.Created || second.Decision.Reason != BackupRotationReasonNotDue || second.Manifest.BackupID != first.Manifest.BackupID {
		t.Fatalf("early rotation result = %#v, want unchanged latest", second)
	}

	options.CreatedAt = createdAt.Add(time.Hour)
	third, err := CreateIncrementalBackupRepositoryIfDue(repository, trie, nil, options, policy)
	if err != nil {
		t.Fatalf("maximum-interval rotation error = %v", err)
	}
	if !third.Created || third.Decision.Reason != BackupRotationReasonMaximumInterval || third.Manifest.BackupID == first.Manifest.BackupID {
		t.Fatalf("maximum-interval rotation result = %#v", third)
	}
}

func TestTU36IncrementalBackupRepositoryByteRetentionKeepsLatest(t *testing.T) {
	trie := newTestTrie(t)
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	tracker := NewLevelDBDirtyTracker()
	repository := filepath.Join(t.TempDir(), "repository")
	for index := 0; index < 3; index++ {
		trie.UpsertString("rotation:value", string(rune('a'+index)))
		tracker.Mark("rotation:value")
		if _, err := CreateBackupBundle(repository, trie, nil, BackupBundleOptions{
			Mode:                  BackupModePebbleIncremental,
			PersistentStore:       store,
			DirtyTracker:          tracker,
			RepositoryRetain:      4,
			RepositoryRetainBytes: 1,
		}); err != nil {
			t.Fatalf("backup %d error = %v", index, err)
		}
	}
	entries, err := os.ReadDir(filepath.Join(repository, backupRepositoryManifestsPath))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("byte-budget retained manifests = %d, want latest only", len(entries))
	}
	if _, err := VerifyBackupPath(repository); err != nil {
		t.Fatalf("VerifyBackupPath(byte-budget repository) error = %v", err)
	}
}
