package hatCache

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func BenchmarkCreateHotBackupBundle(b *testing.B) {
	benchmarkHotBackupBundlePath(b, true)
}

func BenchmarkCreateBlockingBackupBundle(b *testing.B) {
	benchmarkHotBackupBundlePath(b, false)
}

func benchmarkHotBackupBundlePath(b *testing.B, hot bool) {
	trie := newTestTrie(b)
	journal, err := OpenCommandJournalWithFormat(filepath.Join(b.TempDir(), "commands.journal"), CommandJournalFormatJSON)
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !response.OK {
		b.Fatalf("SETSTR response = %#v", response)
	}
	root := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		path := filepath.Join(root, "backup-"+strconv.Itoa(index)+".tar.gz")
		if hot {
			if _, err := CreateHotBackupBundle(path, trie, journal, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON}); err != nil {
				b.Fatal(err)
			}
		} else if _, err := CreateBackupBundle(path, trie, journal, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON}); err != nil {
			b.Fatal(err)
		}
		if err := os.Remove(path); err != nil {
			b.Fatal(err)
		}
	}
}
