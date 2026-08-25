package hatBackup

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRestoreDestinationPublishesVerifiedStaging(t *testing.T) {
	source := t.TempDir()
	target := filepath.Join(t.TempDir(), "restored")
	destination, err := PrepareRestoreDestination(source, target, false)
	if err != nil {
		t.Fatalf("PrepareRestoreDestination() error = %v", err)
	}
	defer destination.Cleanup()
	path := filepath.Join(destination.StagingPath(), "data")
	if err := os.WriteFile(path, []byte("value"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if err := SyncRestoreTree(destination.StagingPath()); err != nil {
		t.Fatalf("SyncRestoreTree() error = %v", err)
	}
	if err := destination.Publish(false); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	data, err := os.ReadFile(filepath.Join(target, "data"))
	if err != nil || string(data) != "value" {
		t.Fatalf("published data = %q/%v, want value/nil", data, err)
	}
}

func TestPrepareRestoreDestinationRejectsSourceOverlap(t *testing.T) {
	source := t.TempDir()
	if _, err := PrepareRestoreDestination(source, filepath.Join(source, "restore"), false); err == nil {
		t.Fatal("PrepareRestoreDestination() error = nil, want source overlap rejection")
	}
}
