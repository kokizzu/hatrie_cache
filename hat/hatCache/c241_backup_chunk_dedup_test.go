package hatCache

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestC241IncrementalBackupRepositoryStoresAndRestoresChunkedFiles(t *testing.T) {
	trie := newTestTrie(t)
	for index := 0; index < 256; index++ {
		trie.UpsertString("c241:key:"+strconv.Itoa(index), strings.Repeat(string(rune('a'+index%26)), 8192))
	}
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	tracker := NewLevelDBDirtyTracker()
	repository := filepath.Join(t.TempDir(), "repository")
	options := BackupBundleOptions{
		Mode:                BackupModePebbleIncremental,
		PersistentStore:     store,
		DirtyTracker:        tracker,
		RepositoryRetain:    4,
		RepositoryChunkSize: 4096,
	}

	base, err := CreateBackupBundle(repository, trie, nil, options)
	if err != nil {
		t.Fatalf("CreateBackupBundle(base repository) error = %v", err)
	}
	if !hasChunkedBackupFile(base.Files) {
		t.Fatalf("base repository has no chunked file: %#v", base.Files)
	}

	updated := strings.Repeat("z", 8192)
	trie.UpsertString("c241:key:7", updated)
	tracker.Mark("c241:key:7")
	second, err := CreateBackupBundle(repository, trie, nil, options)
	if err != nil {
		t.Fatalf("CreateBackupBundle(incremental repository) error = %v", err)
	}
	if !second.Incremental || second.ParentBackupID != base.BackupID {
		t.Fatalf("incremental repository manifest = %#v, base = %#v", second, base)
	}
	if second.ReusedObjects == 0 || second.ReusedObjectBytes == 0 || second.NewObjectBytes >= backupRepositoryLogicalBytes(second.Files) {
		t.Fatalf("chunk reuse metrics = %#v", second)
	}

	dataDir := filepath.Join(t.TempDir(), "restored")
	report, err := RestoreBackupBundle(repository, dataDir, BackupBundleRestoreOptions{})
	if err != nil {
		t.Fatalf("RestoreBackupBundle(repository) error = %v", err)
	}
	if report.BackupID != second.BackupID {
		t.Fatalf("repository restore report = %#v", report)
	}
	restoredStore, err := OpenPersistentStore(report.Store)
	if err != nil {
		t.Fatal(err)
	}
	defer restoredStore.Close()
	restored := newTestTrie(t)
	if _, err := restoredStore.Load(restored); err != nil {
		t.Fatal(err)
	}
	if got := restored.GetString("c241:key:7"); got != updated {
		t.Fatalf("restored updated value length = %d, want %d", len(got), len(updated))
	}
	resumeDir := filepath.Join(t.TempDir(), "resume")
	resumed, err := materializeBackupRepositoryWithResume(repository, second.BackupID, resumeDir, true)
	if err != nil {
		t.Fatalf("materializeBackupRepositoryWithResume() error = %v", err)
	}
	if resumed.BackupID != second.BackupID {
		t.Fatalf("resumed manifest = %#v, want backup %s", resumed, second.BackupID)
	}
	if tracker.Pending() != 0 {
		t.Fatalf("dirty tracker pending = %d, want 0 after committed backup", tracker.Pending())
	}
}

func hasChunkedBackupFile(files []BackupBundleFile) bool {
	for _, file := range files {
		if len(file.Chunks) > 1 {
			return true
		}
	}
	return false
}

func TestC241BackupRepositoryValidatesChunkCoverageAndSize(t *testing.T) {
	chunkA := c241BackupChunk(0, "aaaa")
	chunkB := c241BackupChunk(4, "bbbb")
	valid := BackupBundleManifest{
		RepositoryChunkSize: 4096,
		Files:               []BackupBundleFile{{Path: "cache.leveldb/data", Size: 8, Chunks: []BackupBundleChunk{chunkA, chunkB}}},
	}
	if err := validateBackupRepositoryManifestChunks(valid); err != nil {
		t.Fatalf("valid chunk manifest rejected: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*BackupBundleManifest)
	}{
		{name: "gap", mutate: func(manifest *BackupBundleManifest) { manifest.Files[0].Chunks[1].Offset = 5 }},
		{name: "short coverage", mutate: func(manifest *BackupBundleManifest) { manifest.Files[0].Chunks[1].Size = 3 }},
		{name: "invalid hash", mutate: func(manifest *BackupBundleManifest) { manifest.Files[0].Chunks[0].SHA256 = "not-a-hash" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifest := valid
			manifest.Files = append([]BackupBundleFile(nil), valid.Files...)
			manifest.Files[0].Chunks = append([]BackupBundleChunk(nil), valid.Files[0].Chunks...)
			test.mutate(&manifest)
			if err := validateBackupRepositoryManifestChunks(manifest); err == nil {
				t.Fatal("malformed chunk manifest accepted")
			}
		})
	}
}

func TestC241BackupRepositoryChunkSizeValidation(t *testing.T) {
	if got, err := normalizeBackupRepositoryChunkSize(0); err != nil || got != DefaultBackupRepositoryChunkSize {
		t.Fatalf("default chunk size = %d, %v", got, err)
	}
	for _, size := range []int{backupRepositoryMinChunkSize, backupRepositoryMaxChunkSize} {
		if _, err := normalizeBackupRepositoryChunkSize(size); err != nil {
			t.Fatalf("chunk size %d rejected: %v", size, err)
		}
	}
	if got, err := normalizeBackupRepositoryChunkSize(DisableBackupRepositoryChunking); err != nil || got != 0 {
		t.Fatalf("disabled chunk size = %d, %v", got, err)
	}
	for _, size := range []int{DisableBackupRepositoryChunking - 1, backupRepositoryMinChunkSize - 1, backupRepositoryMaxChunkSize + 1} {
		if _, err := normalizeBackupRepositoryChunkSize(size); err == nil {
			t.Fatalf("chunk size %d accepted", size)
		}
	}
}

func TestC241BackupRepositoryRetainedBytesCountsUniqueChunks(t *testing.T) {
	first := c241BackupChunk(0, "aaaa")
	second := c241BackupChunk(4, "bbbb")
	third := c241BackupChunk(8, "cccc")
	manifests := map[string]BackupBundleManifest{
		"one": {Files: []BackupBundleFile{{Path: "one", Size: 8, Chunks: []BackupBundleChunk{first, second}}}},
		"two": {Files: []BackupBundleFile{{Path: "two", Size: 8, Chunks: []BackupBundleChunk{second, third}}}},
	}
	if got := backupRepositoryRetainedBytes(manifests); got != 12 {
		t.Fatalf("backupRepositoryRetainedBytes() = %d, want 12", got)
	}
}

func c241BackupChunk(offset int64, data string) BackupBundleChunk {
	sum := sha256.Sum256([]byte(data))
	return BackupBundleChunk{Offset: offset, Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
}
