package hatCache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"hatrie_cache/hat/hatBackup"
	"hatrie_cache/internal/jsonwire"
)

func TestC241IncrementalRepositoryDefaultChunkDeduplication(t *testing.T) {
	const payloadSize = 4 << 20
	baseData := c241Payload(payloadSize, 0x11)
	changedData := append([]byte(nil), baseData...)
	for index := (payloadSize / 2) - 32; index < (payloadSize/2)+32; index++ {
		changedData[index] ^= 0x7f
	}

	root := t.TempDir()
	if err := ensureBackupRepository(root); err != nil {
		t.Fatal(err)
	}
	base := BackupBundleManifest{
		Version: BackupBundleVersion,
		Files:   []BackupBundleFile{c241BackupFile("cache.leveldb/large.sst", baseData)},
	}
	if err := storeBackupRepositoryObjects(context.Background(), root, &base, []backupBundlePayloadFile{{name: base.Files[0].Path, data: baseData}}, DefaultBackupRepositoryChunkSize); err != nil {
		t.Fatal(err)
	}
	changed := BackupBundleManifest{
		Version: BackupBundleVersion,
		Files:   []BackupBundleFile{c241BackupFile("cache.leveldb/large.sst", changedData)},
	}
	if err := storeBackupRepositoryObjects(context.Background(), root, &changed, []backupBundlePayloadFile{{name: changed.Files[0].Path, data: changedData}}, DefaultBackupRepositoryChunkSize); err != nil {
		t.Fatal(err)
	}

	chunks := changed.Files[0].Chunks
	if len(chunks) < 2 {
		t.Fatalf("default repository chunk count = %d, want at least 2", len(chunks))
	}
	if changed.NewObjectBytes >= int64(len(changedData)) {
		t.Fatalf("changed snapshot wrote %d bytes, want less than full payload %d", changed.NewObjectBytes, len(changedData))
	}
}

func TestC241IncrementalRepositoryChunkedRoundTripAndCorruptionDetection(t *testing.T) {
	const payloadSize = 3<<20 + 123
	data := c241Payload(payloadSize, 0x33)
	root := t.TempDir()
	if err := ensureBackupRepository(root); err != nil {
		t.Fatal(err)
	}
	manifest := BackupBundleManifest{
		Version: BackupBundleVersion,
		Mode:    BackupModePebbleIncremental,
		Files:   []BackupBundleFile{c241BackupFile("cache.leveldb/large.sst", data)},
	}
	sourcePath := filepath.Join(root, "source.sst")
	if err := os.WriteFile(sourcePath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := storeBackupRepositoryObjects(context.Background(), root, &manifest, []backupBundlePayloadFile{{name: manifest.Files[0].Path, path: sourcePath}}, DefaultBackupRepositoryChunkSize); err != nil {
		t.Fatal(err)
	}
	manifest = c241PublishRepositoryManifest(t, root, manifest)

	destination := filepath.Join(t.TempDir(), "restored")
	if _, err := materializeBackupRepository(root, manifest.BackupID, destination); err != nil {
		t.Fatalf("materializeBackupRepository() error = %v", err)
	}
	restored, err := os.ReadFile(filepath.Join(destination, filepath.FromSlash(manifest.Files[0].Path)))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored, data) {
		t.Fatal("chunked repository restore changed payload")
	}

	resumeDestination := filepath.Join(t.TempDir(), "resume")
	resumePath := filepath.Join(resumeDestination, filepath.FromSlash(manifest.Files[0].Path))
	if err := os.MkdirAll(filepath.Dir(resumePath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(resumePath, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := materializeBackupRepositoryWithResume(root, manifest.BackupID, resumeDestination, true); err != nil {
		t.Fatalf("materializeBackupRepositoryWithResume() error = %v", err)
	}
	resumed, err := os.ReadFile(resumePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(resumed, data) {
		t.Fatal("chunked resume restore changed payload")
	}

	chunk := manifest.Files[0].Chunks[0]
	objectPath, err := backupRepositoryObjectPath(root, chunk.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(objectPath, make([]byte, int(chunk.Size)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := materializeBackupRepository(root, manifest.BackupID, filepath.Join(t.TempDir(), "corrupt")); err == nil {
		t.Fatal("corrupted chunk restored without error")
	}
}

func TestC241IncrementalRepositoryChunkingCanUseLegacyWholeFileObjects(t *testing.T) {
	data := c241Payload(2<<20, 0x44)
	root := t.TempDir()
	if err := ensureBackupRepository(root); err != nil {
		t.Fatal(err)
	}
	manifest := BackupBundleManifest{
		Version: BackupBundleVersion,
		Files:   []BackupBundleFile{c241BackupFile("cache.leveldb/large.sst", data)},
	}
	if err := storeBackupRepositoryObjects(context.Background(), root, &manifest, []backupBundlePayloadFile{{name: manifest.Files[0].Path, data: data}}, BackupRepositoryChunkingDisabled); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Files[0].Chunks) != 0 || manifest.NewObjectBytes != int64(len(data)) {
		t.Fatalf("legacy repository storage = %#v", manifest)
	}
}

func TestC241IncrementalRepositoryChunkSizeDefaultsAndValidation(t *testing.T) {
	if got, err := normalizeBackupRepositoryChunkSize(0); err != nil || got != DefaultBackupRepositoryChunkSize {
		t.Fatalf("default chunk size = %d, error = %v", got, err)
	}
	if got, err := normalizeBackupRepositoryChunkSize(BackupRepositoryChunkingDisabled); err != nil || got != BackupRepositoryChunkingDisabled {
		t.Fatalf("disabled chunk size = %d, error = %v", got, err)
	}
	if _, err := normalizeBackupRepositoryChunkSize(backupRepositoryMinChunkSize - 1); err == nil {
		t.Fatal("sub-minimum chunk size accepted")
	}
	file := c241BackupFile("payload", []byte("payload"))
	file.Chunks = []hatBackup.BundleChunk{{Offset: 1, Size: file.Size, SHA256: file.SHA256}}
	if err := hatBackup.ValidateBundleFileChunks(file); err == nil {
		t.Fatal("non-contiguous chunk metadata accepted")
	}
}

func TestC241PublicRepositoryChunkSizeOptionAndLegacyFallback(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("c241:large", string(c241Payload(128<<10, 0x81)))
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	tracker := NewLevelDBDirtyTracker()

	chunked, err := CreateBackupBundle(filepath.Join(t.TempDir(), "chunked"), trie, nil, BackupBundleOptions{
		Mode:                BackupModePebbleIncremental,
		PersistentStore:     store,
		DirtyTracker:        tracker,
		RepositoryChunkSize: backupRepositoryMinChunkSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	foundChunkedFile := false
	for _, file := range chunked.Files {
		if len(file.Chunks) > 0 {
			foundChunkedFile = true
			break
		}
	}
	if !foundChunkedFile {
		t.Fatalf("custom chunk size produced no chunked file: %#v", chunked.Files)
	}

	legacy, err := CreateBackupBundle(filepath.Join(t.TempDir(), "legacy"), trie, nil, BackupBundleOptions{
		Mode:                  BackupModePebbleIncremental,
		PersistentStore:       store,
		DirtyTracker:          tracker,
		RepositoryChunkSize:   BackupRepositoryChunkingDisabled,
		RepositoryRetain:      2,
		RepositoryRetainBytes: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range legacy.Files {
		if len(file.Chunks) != 0 {
			t.Fatalf("legacy fallback unexpectedly chunked %s", file.Path)
		}
	}
}

func TestC241BackupChainAndRetentionAccountForChunkObjects(t *testing.T) {
	const chunkSize = 1 << 20
	chunkA := c241Payload(chunkSize, 0x51)
	chunkB := c241Payload(chunkSize, 0x61)
	chunkC := c241Payload(chunkSize, 0x71)
	baseData := append(append([]byte(nil), chunkA...), chunkB...)
	childData := append(append([]byte(nil), chunkA...), chunkC...)
	baseFile := c241ChunkedFile("cache.leveldb/data", baseData, chunkSize)
	childFile := c241ChunkedFile("cache.leveldb/data", childData, chunkSize)
	base := hatBackup.BundleManifest{
		Version:           hatBackup.BundleVersion,
		Mode:              hatBackup.ModePebbleIncremental,
		BackupID:          "base",
		StorageBackend:    "pebble",
		StorageFormat:     "binary",
		StorageIdentity:   "identity",
		StorageGeneration: 1,
		Store:             "cache.leveldb",
		Files:             []hatBackup.BundleFile{baseFile},
	}
	child := base
	child.BackupID = "child"
	child.ParentBackupID = "base"
	child.Incremental = true
	child.JournalSequence = 1
	child.Files = []hatBackup.BundleFile{childFile}

	retainedBytes := backupRepositoryRetainedBytes(map[string]BackupBundleManifest{
		base.BackupID:  base,
		child.BackupID: child,
	})
	if retainedBytes != 3*chunkSize {
		t.Fatalf("retained physical bytes = %d, want %d", retainedBytes, 3*chunkSize)
	}
	plan, err := hatBackup.PlanBackupRetention([]hatBackup.BundleManifest{base, child}, child.BackupID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.KeepObjectHashes) != 2 || len(plan.DeleteObjectHashes) != 1 {
		t.Fatalf("chunk retention plan = %#v", plan)
	}
}

func c241Payload(size int, value byte) []byte {
	data := make([]byte, size)
	for index := range data {
		data[index] = value + byte(index%23)
	}
	return data
}

func c241BackupFile(path string, data []byte) BackupBundleFile {
	sum := sha256Bytes(data)
	return BackupBundleFile{Path: path, Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
}

func c241ChunkedFile(path string, data []byte, chunkSize int) hatBackup.BundleFile {
	file := c241BackupFile(path, data)
	file.Chunks = make([]hatBackup.BundleChunk, 0, (len(data)+chunkSize-1)/chunkSize)
	for offset := 0; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}
		sum := sha256Bytes(data[offset:end])
		file.Chunks = append(file.Chunks, hatBackup.BundleChunk{Offset: int64(offset), Size: int64(end - offset), SHA256: hex.EncodeToString(sum[:])})
	}
	return file
}

func c241PublishRepositoryManifest(t *testing.T, root string, manifest BackupBundleManifest) BackupBundleManifest {
	t.Helper()
	backupID, err := backupRepositoryManifestID(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifest.BackupID = backupID
	data, err := jsonwire.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, backupRepositoryManifestsPath, backupID+".json")
	if err := os.WriteFile(manifestPath, append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, backupRepositoryLatestPath), []byte(backupID+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func sha256Bytes(data []byte) [32]byte {
	return sha256.Sum256(data)
}
