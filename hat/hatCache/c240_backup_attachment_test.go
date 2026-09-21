package hatCache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestC240BackupReadOnlyAttachmentQueriesSnapshotBundle(t *testing.T) {
	source := newTestTrie(t)
	source.UpsertString("jobs", `[{"id":1,"state":"queued"}]`)
	bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	manifest, err := CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON})
	if err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}

	attachment, err := OpenBackupReadOnlyAttachment(bundlePath)
	if err != nil {
		t.Fatalf("OpenBackupReadOnlyAttachment() error = %v", err)
	}
	if attachment == nil {
		t.Fatal("OpenBackupReadOnlyAttachment() returned nil attachment")
	}
	if got := attachment.Manifest(); got.Mode != manifest.Mode || got.Snapshot != manifest.Snapshot {
		t.Fatalf("Manifest() = %#v, want mode/snapshot from %#v", got, manifest)
	}
	if _, exposesMutableTrie := attachment.Resolver().(*HatTrie); exposesMutableTrie {
		t.Fatal("Resolver() exposed the mutable HatTrie")
	}
	stagingPath := attachment.staging

	rows, err := attachment.ResolveSQLSource("CACHE", "jobs")
	if err != nil {
		t.Fatalf("ResolveSQLSource() error = %v", err)
	}
	if len(rows) != 1 || rows[0]["id"] != float64(1) || rows[0]["state"] != "queued" {
		t.Fatalf("attached rows = %#v, want the snapshot row", rows)
	}
	queryRows := 0
	err = ExecuteSQLQueryRows(context.Background(), "FROM CACHE('jobs') AS job SELECT job.id, job.state", attachment.Resolver(), nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		queryRows++
		if row["id"] != float64(1) {
			t.Fatalf("query row = %#v, want id 1", row)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
	}
	if queryRows != 1 {
		t.Fatalf("query rows = %d, want 1", queryRows)
	}

	source.UpsertString("jobs", `[{"id":2,"state":"running"}]`)
	rows, err = attachment.ResolveSQLSource("CACHE", "jobs")
	if err != nil {
		t.Fatalf("ResolveSQLSource() after source mutation error = %v", err)
	}
	if len(rows) != 1 || rows[0]["id"] != float64(1) {
		t.Fatalf("attached rows changed with live source mutation = %#v", rows)
	}

	if err := attachment.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := os.Stat(stagingPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging path after Close() error = %v, want not-exist", err)
	}
	if err := attachment.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want idempotent close", err)
	}
	if _, err := attachment.ResolveSQLSource("CACHE", "jobs"); !errors.Is(err, ErrBackupReadOnlyAttachmentClosed) {
		t.Fatalf("ResolveSQLSource() after Close() error = %v, want %v", err, ErrBackupReadOnlyAttachmentClosed)
	}
}

func TestC240BackupReadOnlyAttachmentRejectsCorruptBundle(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[{"id":1}]`)
	bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, trie, nil, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	data[0] ^= 0xff
	corruptPath := filepath.Join(t.TempDir(), "corrupt.tar.gz")
	if err := os.WriteFile(corruptPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenBackupReadOnlyAttachment(corruptPath); err == nil {
		t.Fatal("OpenBackupReadOnlyAttachment(corrupt) error = nil, want rejection")
	}
}

func TestC240BackupReadOnlyAttachmentRejectsUnsupportedBackupMode(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[{"id":3,"state":"checkpointed"}]`)
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	bundlePath := filepath.Join(t.TempDir(), "checkpoint.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, trie, nil, BackupBundleOptions{Mode: BackupModePebbleCheckpoint, PersistentStore: store}); err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}
	attachment, err := OpenBackupReadOnlyAttachment(bundlePath)
	if err != nil {
		t.Fatalf("OpenBackupReadOnlyAttachment(checkpoint) error = %v", err)
	}
	defer attachment.Close()
	rows, err := attachment.ResolveSQLSource("CACHE", "jobs")
	if err != nil {
		t.Fatalf("ResolveSQLSource(checkpoint) error = %v", err)
	}
	if len(rows) != 1 || rows[0]["id"] != float64(3) {
		t.Fatalf("checkpoint rows = %#v, want id 3", rows)
	}
}

func TestC240BackupReadOnlyAttachmentQueriesIncrementalRepository(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[{"id":4,"state":"repository"}]`)
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	repository := filepath.Join(t.TempDir(), "repository")
	manifest, err := CreateBackupBundle(repository, trie, nil, BackupBundleOptions{
		Mode:            BackupModePebbleIncremental,
		PersistentStore: store,
		DirtyTracker:    NewLevelDBDirtyTracker(),
	})
	if err != nil {
		t.Fatalf("CreateBackupBundle(repository) error = %v", err)
	}

	attachment, err := OpenBackupReadOnlyAttachmentWithOptions(repository, BackupReadOnlyAttachmentOptions{BackupID: manifest.BackupID})
	if err != nil {
		t.Fatalf("OpenBackupReadOnlyAttachment(repository) error = %v", err)
	}
	defer attachment.Close()
	rows, err := attachment.ResolveSQLSource("CACHE", "jobs")
	if err != nil {
		t.Fatalf("ResolveSQLSource(repository) error = %v", err)
	}
	if len(rows) != 1 || rows[0]["id"] != float64(4) {
		t.Fatalf("repository rows = %#v, want id 4", rows)
	}
}

func BenchmarkC240BackupAttachmentOpenAndQuery(b *testing.B) {
	source := CreateHatTrie()
	defer source.Destroy()
	source.UpsertString("jobs", `[{"id":1,"state":"queued"},{"id":2,"state":"running"}]`)
	bundlePath := filepath.Join(b.TempDir(), "backup.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON}); err != nil {
		b.Fatalf("CreateBackupBundle() error = %v", err)
	}
	query := "FROM CACHE('jobs') AS job SELECT job.id, job.state"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		attachment, err := OpenBackupReadOnlyAttachment(bundlePath)
		if err != nil {
			b.Fatalf("OpenBackupReadOnlyAttachment() error = %v", err)
		}
		rows := 0
		err = ExecuteSQLQueryRows(context.Background(), query, attachment.Resolver(), nil, SQLQueryOptions{}, func(_ []string, _ SQLRow) error {
			rows++
			return nil
		})
		closeErr := attachment.Close()
		if err != nil {
			b.Fatalf("ExecuteSQLQueryRows() error = %v", err)
		}
		if closeErr != nil {
			b.Fatalf("Close() error = %v", closeErr)
		}
		if rows != 2 {
			b.Fatalf("query rows = %d, want 2", rows)
		}
	}
}
