package hatCache

import (
	"os"
	"path/filepath"
	"testing"

	"hatrie_cache/internal/jsonwire"
)

func BenchmarkCHU50BackupCreateSnapshot(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	journal, err := OpenCommandJournal(filepath.Join(b.TempDir(), "commands.journal"))
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !response.OK {
		b.Fatalf("ExecuteCommand() response = %#v", response)
	}
	bundlePath := filepath.Join(b.TempDir(), "backup.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, trie, journal, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON}); err != nil {
		b.Fatal(err)
	}
	info, err := os.Stat(bundlePath)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := CreateBackupBundle(bundlePath, trie, journal, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(info.Size()), "bundle-bytes")
}

func BenchmarkCHU50BackupVerifySnapshot(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	journal, err := OpenCommandJournal(filepath.Join(b.TempDir(), "commands.journal"))
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !response.OK {
		b.Fatalf("ExecuteCommand() response = %#v", response)
	}
	bundlePath := filepath.Join(b.TempDir(), "backup.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, trie, journal, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := VerifyBackupBundle(bundlePath); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCHU50BackupCreatePebbleCheckpoint(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	journal, err := OpenCommandJournal(filepath.Join(b.TempDir(), "commands.journal"))
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()
	for _, request := range []CacheCommandRequest{
		{Command: "SETSTR", Key: "name", Value: "ivi"},
		{Command: "SETINT", Key: "count", Value: "42"},
	} {
		if response := journal.ExecuteCommand(trie, request); !response.OK {
			b.Fatalf("ExecuteCommand(%s) response = %#v", request.Command, response)
		}
	}
	store, err := OpenPebbleStore(filepath.Join(b.TempDir(), "live.pebble"))
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	bundlePath := filepath.Join(b.TempDir(), "backup.tar.gz")
	if _, err := CreateBackupBundle(bundlePath, trie, journal, BackupBundleOptions{
		Mode:            BackupModePebbleCheckpoint,
		PersistentStore: store,
	}); err != nil {
		b.Fatal(err)
	}
	info, err := os.Stat(bundlePath)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := CreateBackupBundle(bundlePath, trie, journal, BackupBundleOptions{
			Mode:            BackupModePebbleCheckpoint,
			PersistentStore: store,
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(info.Size()), "bundle-bytes")
}

func BenchmarkCHU50BackupManifestMarshal(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	journal, err := OpenCommandJournal(filepath.Join(b.TempDir(), "commands.journal"))
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !response.OK {
		b.Fatalf("ExecuteCommand() response = %#v", response)
	}
	manifest, err := CreateBackupBundle(filepath.Join(b.TempDir(), "backup.tar.gz"), trie, journal, BackupBundleOptions{SnapshotFormat: SnapshotFormatJSON})
	if err != nil {
		b.Fatal(err)
	}
	data, err := jsonwire.Marshal(manifest)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		data, err := jsonwire.Marshal(manifest)
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(len(data)))
	}
	b.StopTimer()
	b.ReportMetric(float64(len(data)), "manifest-bytes")
}
