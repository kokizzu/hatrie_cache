package hatCache

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotExportResumesWithoutDuplicatingBytes(t *testing.T) {
	trie := newTestTrie(t)
	defer trie.Destroy()

	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	for _, key := range []string{"resume:a", "resume:b", "resume:c"} {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SET", Key: key, Value: key + "-value"})
		if !response.OK {
			t.Fatalf("ExecuteCommand(%q) response = %#v", key, response)
		}
	}

	dir := t.TempDir()
	targetPath := filepath.Join(dir, "snapshot.hc")
	checkpointPath := filepath.Join(dir, "snapshot.resume.json")
	sourcePath := snapshotExportSourcePath(targetPath)
	partialPath := snapshotExportPartialPath(targetPath)

	sourceFile, err := os.OpenFile(sourcePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := journal.WriteSnapshotWithManifest(trie, sourceFile, SnapshotFormatBinary)
	closeErr := sourceFile.Close()
	if err != nil {
		t.Fatalf("WriteSnapshotWithManifest() error = %v", err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	sourceBytes, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	offset := len(sourceBytes) / 2
	if offset == 0 || offset == len(sourceBytes) {
		t.Fatalf("snapshot size = %d, want a resumable non-edge offset", len(sourceBytes))
	}
	tailEnd := offset + 7
	if tailEnd > len(sourceBytes) {
		tailEnd = len(sourceBytes)
	}
	if err := os.WriteFile(partialPath, sourceBytes[:tailEnd], 0o600); err != nil {
		t.Fatal(err)
	}
	prefixDigest := sha256.Sum256(sourceBytes[:offset])
	checkpoint := snapshotExportCheckpoint{
		Version:      snapshotExportCheckpointVersion,
		TargetPath:   targetPath,
		SourcePath:   sourcePath,
		PartialPath:  partialPath,
		Format:       SnapshotFormatBinary,
		Manifest:     manifest,
		BytesWritten: int64(offset),
		PrefixSHA256: hex.EncodeToString(prefixDigest[:]),
	}
	if err := writeSnapshotExportCheckpoint(checkpointPath, checkpoint); err != nil {
		t.Fatalf("writeSnapshotExportCheckpoint() error = %v", err)
	}

	report, err := journal.WriteSnapshotWithResumableExport(trie, targetPath, SnapshotExportOptions{
		Format:         SnapshotFormatBinary,
		CheckpointPath: checkpointPath,
	})
	if err != nil {
		t.Fatalf("WriteSnapshotWithResumableExport() error = %v", err)
	}
	if !report.Resumed {
		t.Fatal("resumable export report.Resumed = false, want true")
	}
	if report.Manifest != manifest {
		t.Fatalf("resumed manifest = %#v, want %#v", report.Manifest, manifest)
	}

	got, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, sourceBytes) {
		t.Fatalf("resumed snapshot bytes differ: got %d bytes, want %d", len(got), len(sourceBytes))
	}
	if err := VerifySnapshotManifest(targetPath, manifest); err != nil {
		t.Fatalf("VerifySnapshotManifest() error = %v", err)
	}
	for _, path := range []string{checkpointPath, sourcePath, partialPath} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("temporary export path %q stat error = %v, want not exist", path, err)
		}
	}

	restored := newTestTrie(t)
	defer restored.Destroy()
	if _, err := restored.LoadSnapshotWithMetadata(targetPath); err != nil {
		t.Fatalf("LoadSnapshotWithMetadata() error = %v", err)
	}
	for _, key := range []string{"resume:a", "resume:b", "resume:c"} {
		if got := restored.GetString(key); got != key+"-value" {
			t.Fatalf("restored %q = %q, want %q", key, got, key+"-value")
		}
	}
}

func TestSnapshotExportFreshPublishesAndCleansSidecars(t *testing.T) {
	trie := newTestTrie(t)
	defer trie.Destroy()

	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SET", Key: "fresh", Value: "value"})
	if !response.OK {
		t.Fatalf("ExecuteCommand() response = %#v", response)
	}

	targetPath := filepath.Join(t.TempDir(), "snapshot.json.gz")
	report, err := journal.WriteSnapshotWithResumableExport(trie, targetPath, SnapshotExportOptions{
		Format: SnapshotFormatGzipBinary,
	})
	if err != nil {
		t.Fatalf("WriteSnapshotWithResumableExport() error = %v", err)
	}
	if report.Resumed {
		t.Fatal("fresh export report.Resumed = true, want false")
	}
	if report.BytesWritten != report.Manifest.SizeBytes || report.BytesWritten == 0 {
		t.Fatalf("fresh export bytes = %d, manifest size = %d", report.BytesWritten, report.Manifest.SizeBytes)
	}
	if err := VerifySnapshotManifest(targetPath, report.Manifest); err != nil {
		t.Fatalf("VerifySnapshotManifest() error = %v", err)
	}
	for _, path := range []string{
		targetPath + ".resume.json",
		snapshotExportSourcePath(targetPath),
		snapshotExportPartialPath(targetPath),
	} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("temporary export path %q stat error = %v, want not exist", path, err)
		}
	}
}

func TestSnapshotExportRejectsSymlinkedPartial(t *testing.T) {
	trie := newTestTrie(t)
	defer trie.Destroy()

	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SET", Key: "symlink", Value: "value"})
	if !response.OK {
		t.Fatalf("ExecuteCommand() response = %#v", response)
	}

	dir := t.TempDir()
	targetPath := filepath.Join(dir, "snapshot.hc")
	checkpointPath := filepath.Join(dir, "snapshot.resume.json")
	sourcePath := snapshotExportSourcePath(targetPath)
	partialPath := snapshotExportPartialPath(targetPath)
	sourceFile, err := os.OpenFile(sourcePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := journal.WriteSnapshotWithManifest(trie, sourceFile, SnapshotFormatBinary)
	if closeErr := sourceFile.Close(); err != nil {
		t.Fatalf("WriteSnapshotWithManifest() error = %v", err)
	} else if closeErr != nil {
		t.Fatal(closeErr)
	}
	sentinelPath := filepath.Join(dir, "sentinel")
	sentinel := []byte("do-not-overwrite")
	if err := os.WriteFile(sentinelPath, sentinel, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(sentinelPath, partialPath); err != nil {
		t.Skipf("symlink not supported: %v", err)
	}
	checkpoint := snapshotExportCheckpoint{
		Version:      snapshotExportCheckpointVersion,
		TargetPath:   targetPath,
		SourcePath:   sourcePath,
		PartialPath:  partialPath,
		Format:       SnapshotFormatBinary,
		Manifest:     manifest,
		PrefixSHA256: snapshotExportPrefixDigest(nil),
	}
	if err := writeSnapshotExportCheckpoint(checkpointPath, checkpoint); err != nil {
		t.Fatalf("writeSnapshotExportCheckpoint() error = %v", err)
	}
	if _, err := journal.WriteSnapshotWithResumableExport(trie, targetPath, SnapshotExportOptions{
		Format:         SnapshotFormatBinary,
		CheckpointPath: checkpointPath,
	}); !errors.Is(err, ErrSnapshotExportCheckpointInvalid) {
		t.Fatalf("WriteSnapshotWithResumableExport(symlink) error = %v, want checkpoint invalid", err)
	}
	got, err := os.ReadFile(sentinelPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, sentinel) {
		t.Fatalf("symlink target changed: got %q, want %q", got, sentinel)
	}
}
