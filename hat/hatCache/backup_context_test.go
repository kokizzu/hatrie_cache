package hatCache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCreateIncrementalBackupRepositoryWithContextCancellationPreservesLatest(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("name", "before-cancel")
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	tracker := NewLevelDBDirtyTracker()
	repository := filepath.Join(t.TempDir(), "repository")

	base, err := CreateIncrementalBackupRepositoryWithContext(context.Background(), repository, trie, nil, BackupBundleOptions{
		PersistentStore: store,
		DirtyTracker:    tracker,
	})
	if err != nil {
		t.Fatalf("CreateIncrementalBackupRepositoryWithContext(base) error = %v", err)
	}

	trie.UpsertString("name", "after-cancel")
	tracker.Mark("name")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := CreateIncrementalBackupRepositoryWithContext(ctx, repository, trie, nil, BackupBundleOptions{
		PersistentStore: store,
		DirtyTracker:    tracker,
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled backup error = %v, want context.Canceled", err)
	}

	latest, err := VerifyBackupPath(repository)
	if err != nil {
		t.Fatalf("VerifyBackupPath(after cancellation) error = %v", err)
	}
	if latest.BackupID != base.BackupID {
		t.Fatalf("latest backup after cancellation = %q, want %q", latest.BackupID, base.BackupID)
	}
	if _, err := os.Stat(filepath.Join(repository, backupRepositoryLatestPath)); err != nil {
		t.Fatalf("latest marker after cancellation: %v", err)
	}

	resumed, err := CreateIncrementalBackupRepositoryWithContext(context.Background(), repository, trie, nil, BackupBundleOptions{
		PersistentStore: store,
		DirtyTracker:    tracker,
	})
	if err != nil {
		t.Fatalf("CreateIncrementalBackupRepositoryWithContext(resume) error = %v", err)
	}
	if resumed.ParentBackupID != base.BackupID {
		t.Fatalf("resumed parent backup = %q, want %q", resumed.ParentBackupID, base.BackupID)
	}
}

func TestCreateBackupBundleWithContextCancellationDoesNotCreateBundle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	bundlePath := filepath.Join(t.TempDir(), "backup.hc")
	_, err := CreateBackupBundleWithContext(ctx, bundlePath, newTestTrie(t), nil, BackupBundleOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled bundle error = %v, want context.Canceled", err)
	}
	if _, err := os.Stat(bundlePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled bundle stat error = %v, want not exist", err)
	}
}
