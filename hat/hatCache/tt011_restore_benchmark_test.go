package hatCache

import (
	"path/filepath"
	"strconv"
	"testing"
)

func benchmarkTT011Bundle(b *testing.B) (string, uint64) {
	b.Helper()
	source := CreateHatTrie()
	defer source.Destroy()
	journal, err := OpenCommandJournal(filepath.Join(b.TempDir(), "commands.journal"))
	if err != nil {
		b.Fatal(err)
	}
	if response := journal.ExecuteCommand(source, CacheCommandRequest{Command: "SETSTR", Key: "state", Value: "snapshot"}); !response.OK {
		b.Fatalf("snapshot SETSTR response = %#v", response)
	}
	baseBundlePath := filepath.Join(b.TempDir(), "base.tar.gz")
	if _, err := CreateBackupBundle(baseBundlePath, source, journal, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON}); err != nil {
		b.Fatalf("CreateBackupBundle() error = %v", err)
	}
	for index := 0; index < 64; index++ {
		if response := journal.ExecuteCommand(source, CacheCommandRequest{Command: "SETSTR", Key: "tail-" + strconv.Itoa(index), Value: "value"}); !response.OK {
			b.Fatalf("tail SETSTR response = %#v", response)
		}
	}
	journalPath := journal.path
	if err := journal.Close(); err != nil {
		b.Fatalf("Close() error = %v", err)
	}
	return rebuildBackupBundleWithJournal(b, baseBundlePath, journalPath), 33
}

func BenchmarkTT011RestoreDefault(b *testing.B) {
	bundlePath, _ := benchmarkTT011Bundle(b)
	root := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := RestoreBackupBundle(bundlePath, filepath.Join(root, strconv.Itoa(index)), BackupBundleRestoreOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
