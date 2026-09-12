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

func TestCommandJournalWriteSnapshotWithManifest(t *testing.T) {
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

	response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SET", Key: "name", Value: "ivi"})
	if !response.OK {
		t.Fatalf("ExecuteCommand() response = %#v", response)
	}

	var snapshot bytes.Buffer
	manifest, err := journal.WriteSnapshotWithManifest(trie, &snapshot, SnapshotFormatBinary)
	if err != nil {
		t.Fatalf("WriteSnapshotWithManifest() error = %v", err)
	}
	wantDigest := sha256.Sum256(snapshot.Bytes())
	if manifest.JournalSequence != journal.Sequence() || manifest.JournalSequence == 0 {
		t.Fatalf("manifest sequence = %d, journal sequence = %d", manifest.JournalSequence, journal.Sequence())
	}
	if manifest.Format != SnapshotFormatBinary {
		t.Fatalf("manifest format = %q, want %q", manifest.Format, SnapshotFormatBinary)
	}
	if manifest.SizeBytes != int64(snapshot.Len()) {
		t.Fatalf("manifest size = %d, snapshot size = %d", manifest.SizeBytes, snapshot.Len())
	}
	if manifest.SHA256 != hex.EncodeToString(wantDigest[:]) {
		t.Fatalf("manifest SHA256 = %q, want %q", manifest.SHA256, hex.EncodeToString(wantDigest[:]))
	}

	path := filepath.Join(t.TempDir(), "snapshot.hc")
	if err := os.WriteFile(path, snapshot.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifySnapshotManifest(path, manifest); err != nil {
		t.Fatalf("VerifySnapshotManifest() error = %v", err)
	}
	restored := newTestTrie(t)
	defer restored.Destroy()
	metadata, err := restored.LoadSnapshotWithMetadata(path)
	if err != nil {
		t.Fatalf("LoadSnapshotWithMetadata() error = %v", err)
	}
	if metadata.JournalSequence != manifest.JournalSequence || restored.GetString("name") != "ivi" {
		t.Fatalf("restored metadata/value = %#v/%q, manifest = %#v", metadata, restored.GetString("name"), manifest)
	}

	corrupted := append([]byte(nil), snapshot.Bytes()...)
	corrupted[len(corrupted)/2]++
	if err := os.WriteFile(path, corrupted, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := VerifySnapshotManifest(path, manifest); !errors.Is(err, ErrSnapshotManifestMismatch) {
		t.Fatalf("VerifySnapshotManifest(corrupted) error = %v, want manifest mismatch", err)
	}
}
