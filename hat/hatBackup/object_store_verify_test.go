package hatBackup_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"hatrie_cache/hat/hatBackup"
)

func TestObjectStoreTargetVerifyChecksEveryPayload(t *testing.T) {
	ctx := context.Background()
	sourceStore := newObjectStoreMemory()
	source := t.TempDir()
	writeObjectStoreFile(t, source+"/part-a/rows.bin", []byte("row-a\nrow-b\n"))
	sourceTarget, err := hatBackup.NewObjectStoreTarget(sourceStore, "region-a/backup-1")
	if err != nil {
		t.Fatalf("NewObjectStoreTarget() error = %v", err)
	}
	if _, err := sourceTarget.Backup(ctx, source, hatBackup.BundleManifest{}); err != nil {
		t.Fatalf("Backup() error = %v", err)
	}

	destinationStore := newObjectStoreMemory()
	const sourcePrefix = "region-a/backup-1/"
	const destinationPrefix = "region-b/backup-1/"
	sourceStore.mu.Lock()
	for key, data := range sourceStore.objects {
		if strings.HasPrefix(key, sourcePrefix) {
			copiedKey := destinationPrefix + strings.TrimPrefix(key, sourcePrefix)
			destinationStore.objects[copiedKey] = append([]byte(nil), data...)
		}
	}
	sourceStore.mu.Unlock()

	target, err := hatBackup.NewObjectStoreTarget(destinationStore, "region-b/backup-1")
	if err != nil {
		t.Fatalf("NewObjectStoreTarget() error = %v", err)
	}
	manifest, err := target.Verify(ctx)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if len(manifest.Files) != 1 || manifest.Files[0].Path != "part-a/rows.bin" {
		t.Fatalf("Verify() manifest = %#v", manifest)
	}
	restored := filepath.Join(t.TempDir(), "restored")
	if _, err := target.Restore(ctx, restored, false); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	data, err := os.ReadFile(filepath.Join(restored, "part-a", "rows.bin"))
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if string(data) != "row-a\nrow-b\n" {
		t.Fatalf("restored data = %q", data)
	}

	if err := destinationStore.Put(ctx, "region-b/backup-1/part-a/rows.bin", bytes.NewReader([]byte("tampered")), int64(len("tampered"))); err != nil {
		t.Fatalf("tamper Put() error = %v", err)
	}
	if _, err := target.Verify(ctx); err == nil || errors.Is(err, hatBackup.ErrObjectStoreManifestInvalid) {
		t.Fatalf("Verify() tampered error = %v, want payload checksum error", err)
	}
}
