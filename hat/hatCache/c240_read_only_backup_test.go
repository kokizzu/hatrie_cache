package hatCache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestReadOnlyBackupAttachmentSnapshotBundle(t *testing.T) {
	source := CreateHatTrie()
	defer source.Destroy()
	source.UpsertString("name", "from-backup")
	source.UpsertString("region:sg", "south-east")
	source.UpsertString("people", `[{"value":"from-backup"}]`)

	bundlePath := filepath.Join(t.TempDir(), "snapshot.tar.gz")
	manifest, err := CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{
		Mode:           BackupModeSnapshot,
		SnapshotFormat: SnapshotFormatGzipBinary,
	})
	if err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}

	attachment, err := OpenReadOnlyBackup(bundlePath, ReadOnlyBackupOptions{})
	if err != nil {
		t.Fatalf("OpenReadOnlyBackup() error = %v", err)
	}
	defer attachment.Close()
	if got := attachment.Manifest().Mode; got != manifest.Mode {
		t.Fatalf("Manifest().Mode = %q, want %q", got, manifest.Mode)
	}

	value, ok, err := attachment.GetStringChecked("name")
	if err != nil {
		t.Fatalf("GetStringChecked() error = %v", err)
	}
	if !ok || value != "from-backup" {
		t.Fatalf("GetStringChecked() = %q, %v; want from-backup, true", value, ok)
	}
	keys, err := attachment.KeysWithPrefixChecked("region:", true)
	if err != nil {
		t.Fatalf("KeysWithPrefixChecked() error = %v", err)
	}
	if len(keys) != 1 || keys[0] != "region:sg" {
		t.Fatalf("KeysWithPrefixChecked() = %#v, want [region:sg]", keys)
	}

	result, err := attachment.QuerySQL(context.Background(), "FROM CACHE('people') AS person SELECT person.value", nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("QuerySQL() error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["value"] != "from-backup" {
		t.Fatalf("QuerySQL() rows = %#v, want one backup value", result.Rows)
	}

	source.UpsertString("name", "live-value")
	value, ok, err = attachment.GetStringChecked("name")
	if err != nil {
		t.Fatalf("GetStringChecked() after source mutation error = %v", err)
	}
	if !ok || value != "from-backup" {
		t.Fatalf("attached value after source mutation = %q, %v; want from-backup, true", value, ok)
	}
}

func TestReadOnlyBackupAttachmentPebbleCheckpointBundle(t *testing.T) {
	source := CreateHatTrie()
	defer source.Destroy()
	source.UpsertString("name", "checkpoint-value")
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Save(source); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	bundlePath := filepath.Join(t.TempDir(), "checkpoint.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{
		Mode:            BackupModePebbleCheckpoint,
		PersistentStore: store,
	}); err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}

	attachment, err := OpenReadOnlyBackup(bundlePath, ReadOnlyBackupOptions{})
	if err != nil {
		t.Fatalf("OpenReadOnlyBackup() error = %v", err)
	}
	defer attachment.Close()
	value, ok, err := attachment.GetStringChecked("name")
	if err != nil {
		t.Fatalf("GetStringChecked() error = %v", err)
	}
	if !ok || value != "checkpoint-value" {
		t.Fatalf("GetStringChecked() = %q, %v; want checkpoint-value, true", value, ok)
	}
}

func TestReadOnlyBackupAttachmentIncrementalRepository(t *testing.T) {
	source := CreateHatTrie()
	defer source.Destroy()
	source.UpsertString("name", "repository-value")
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	repository := filepath.Join(t.TempDir(), "repository")
	if _, err := CreateBackupBundle(repository, source, nil, BackupBundleOptions{
		Mode:            BackupModePebbleIncremental,
		PersistentStore: store,
		DirtyTracker:    NewLevelDBDirtyTracker(),
	}); err != nil {
		t.Fatalf("CreateBackupBundle() error = %v", err)
	}

	attachment, err := OpenReadOnlyBackup(repository, ReadOnlyBackupOptions{})
	if err != nil {
		t.Fatalf("OpenReadOnlyBackup() error = %v", err)
	}
	defer attachment.Close()
	value, ok, err := attachment.GetStringChecked("name")
	if err != nil {
		t.Fatalf("GetStringChecked() error = %v", err)
	}
	if !ok || value != "repository-value" {
		t.Fatalf("GetStringChecked() = %q, %v; want repository-value, true", value, ok)
	}
}

func TestReadOnlyBackupAttachmentNativeCheckpointDirectory(t *testing.T) {
	source := CreateHatTrie()
	defer source.Destroy()
	source.UpsertString("name", "native-checkpoint")
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Save(source); err != nil {
		t.Fatal(err)
	}
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint")
	if err := store.SaveCheckpointWithJournalSequence(source, checkpointPath, 0); err != nil {
		t.Fatalf("SaveCheckpointWithJournalSequence() error = %v", err)
	}

	attachment, err := OpenReadOnlyBackup(checkpointPath, ReadOnlyBackupOptions{})
	if err != nil {
		t.Fatalf("OpenReadOnlyBackup() error = %v", err)
	}
	defer attachment.Close()
	if got := attachment.GetString("name"); got != "native-checkpoint" {
		t.Fatalf("GetString() = %q, want native-checkpoint", got)
	}
}

func TestReadOnlyBackupAttachmentRejectsWritesAfterClose(t *testing.T) {
	source := CreateHatTrie()
	defer source.Destroy()
	source.UpsertString("name", "value")
	bundlePath := filepath.Join(t.TempDir(), "snapshot.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{Mode: BackupModeSnapshot}); err != nil {
		t.Fatal(err)
	}

	attachment, err := OpenReadOnlyBackup(bundlePath, ReadOnlyBackupOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := attachment.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := attachment.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, _, err := attachment.GetStringChecked("name"); !errors.Is(err, ErrReadOnlyBackupClosed) {
		t.Fatalf("GetStringChecked() after Close() error = %v, want %v", err, ErrReadOnlyBackupClosed)
	}
}

func BenchmarkReadOnlyBackupAttachment(b *testing.B) {
	bundlePath := c240BenchmarkSnapshotBundle(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		attachment, err := OpenReadOnlyBackup(bundlePath, ReadOnlyBackupOptions{})
		if err != nil {
			b.Fatal(err)
		}
		if attachment.GetString("name") != "benchmark-value" {
			b.Fatal("attached benchmark value changed")
		}
		if err := attachment.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadOnlyBackupRestoreAndLoad(b *testing.B) {
	bundlePath := c240BenchmarkSnapshotBundle(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		dataDir, err := os.MkdirTemp(b.TempDir(), "restore-*")
		if err != nil {
			b.Fatal(err)
		}
		if _, err := RestoreBackupBundle(bundlePath, dataDir, BackupBundleRestoreOptions{}); err != nil {
			b.Fatal(err)
		}
		trie := CreateHatTrie()
		if err := trie.LoadSnapshot(filepath.Join(dataDir, "snapshot.hc")); err != nil {
			trie.Destroy()
			b.Fatal(err)
		}
		if trie.GetString("name") != "benchmark-value" {
			trie.Destroy()
			b.Fatal("restored benchmark value changed")
		}
		trie.Destroy()
		if err := os.RemoveAll(dataDir); err != nil {
			b.Fatal(err)
		}
	}
}

func c240BenchmarkSnapshotBundle(b *testing.B) string {
	b.Helper()
	source := CreateHatTrie()
	source.UpsertString("name", "benchmark-value")
	for index := 0; index < 128; index++ {
		source.UpsertString(filepath.Join("item", string(rune('a'+index))), "value")
	}
	bundlePath := filepath.Join(b.TempDir(), "benchmark.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, source, nil, BackupBundleOptions{Mode: BackupModeSnapshot}); err != nil {
		source.Destroy()
		b.Fatal(err)
	}
	source.Destroy()
	return bundlePath
}
