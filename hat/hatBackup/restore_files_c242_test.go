package hatBackup

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestCopyRestoreFilesCopiesChecksummedFilesWithBoundedWorkers(t *testing.T) {
	sourceDir := t.TempDir()
	destinationDir := t.TempDir()
	files := make([]RestoreFile, 8)
	for index := range files {
		data := make([]byte, 4096+index*257)
		for offset := range data {
			data[offset] = byte(index + offset)
		}
		source := filepath.Join(sourceDir, "source", filepath.Base(filepath.Join("file", string(rune('a'+index)))))
		if err := os.MkdirAll(filepath.Dir(source), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(source, data, 0o600); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		files[index] = RestoreFile{
			Source:      source,
			Destination: filepath.Join(destinationDir, "nested", string(rune('a'+index)), "payload"),
			Size:        int64(len(data)),
			SHA256:      hex.EncodeToString(sum[:]),
		}
	}

	if err := CopyRestoreFiles(files, RestoreFileOptions{MaxConcurrency: 3}); err != nil {
		t.Fatalf("CopyRestoreFiles() error = %v", err)
	}
	for _, file := range files {
		data, err := os.ReadFile(file.Destination)
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(data)) != file.Size {
			t.Fatalf("restored size for %s = %d, want %d", file.Destination, len(data), file.Size)
		}
	}
}

func TestCopyRestoreFilesRejectsInvalidConcurrency(t *testing.T) {
	if err := CopyRestoreFiles(nil, RestoreFileOptions{MaxConcurrency: -1}); err == nil {
		t.Fatal("CopyRestoreFiles() error = nil, want invalid concurrency error")
	}
	if err := CopyRestoreFiles(nil, RestoreFileOptions{MaxConcurrency: MaxRestoreFileConcurrency + 1}); err == nil {
		t.Fatal("CopyRestoreFiles() error = nil, want excessive concurrency error")
	}
}

func TestCopyRestoreFilesRemovesPartialDestinationAfterChecksumFailure(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(source, []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "nested", "destination")
	if err := CopyRestoreFiles([]RestoreFile{{
		Source:      source,
		Destination: destination,
		Size:        int64(len("payload")),
		SHA256:      "0000000000000000000000000000000000000000000000000000000000000000",
	}}, RestoreFileOptions{}); err == nil {
		t.Fatal("CopyRestoreFiles() error = nil, want checksum mismatch")
	}
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		t.Fatalf("partial destination stat error = %v, want not exist", err)
	}
}

func TestCopyRestoreFilesPreflightsDeclarationsBeforeCopy(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(source, []byte("payload"), 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("payload"))
	validDestination := filepath.Join(t.TempDir(), "valid")
	files := []RestoreFile{
		{Source: source, Destination: validDestination, Size: int64(len("payload")), SHA256: hex.EncodeToString(sum[:])},
		{Source: "", Destination: filepath.Join(t.TempDir(), "invalid"), Size: 0, SHA256: hex.EncodeToString(sum[:])},
	}
	if err := CopyRestoreFiles(files, RestoreFileOptions{}); err == nil {
		t.Fatal("CopyRestoreFiles() error = nil, want invalid declaration")
	}
	if _, err := os.Stat(validDestination); !os.IsNotExist(err) {
		t.Fatalf("preflight destination stat error = %v, want not exist", err)
	}
}
