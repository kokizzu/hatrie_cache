package hatBackup_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatBackup"
)

func TestObjectStoreEncryptedBackupRoundTripsAndSupportsKeyRotation(t *testing.T) {
	ctx := context.Background()
	store := newObjectStoreMemory()
	oldKey := bytes.Repeat([]byte{0x11}, 32)
	newKey := bytes.Repeat([]byte{0x22}, 32)
	source := t.TempDir()
	payload := bytes.Repeat([]byte("row-value\n"), 7000)
	writeObjectStoreFile(t, filepath.Join(source, "part-a", "rows.bin"), payload)

	oldTarget, err := hatBackup.NewObjectStoreTargetWithOptions(store, "backup-old", hatBackup.ObjectStoreTargetOptions{
		EncryptionKeys:        []hatBackup.ObjectStoreEncryptionKey{{ID: "key-old", Key: oldKey}},
		ActiveEncryptionKeyID: "key-old",
	})
	if err != nil {
		t.Fatalf("NewObjectStoreTargetWithOptions() error = %v", err)
	}
	manifest, err := oldTarget.Backup(ctx, source, hatBackup.BundleManifest{Mode: hatBackup.ModeSnapshot, Snapshot: "snapshot-encrypted"})
	if err != nil {
		t.Fatalf("Backup() error = %v", err)
	}
	if manifest.Encryption == nil || manifest.Encryption.KeyID != "key-old" {
		t.Fatalf("Backup() encryption metadata = %+v, want key-old", manifest.Encryption)
	}
	if manifest.Encryption.Algorithm != hatBackup.ObjectStoreEncryptionAlgorithm {
		t.Fatalf("Backup() algorithm = %q, want %q", manifest.Encryption.Algorithm, hatBackup.ObjectStoreEncryptionAlgorithm)
	}
	if len(manifest.Files) != 1 || manifest.Files[0].Size != int64(len(payload)) || manifest.Files[0].SHA256 != objectStoreSHA256(string(payload)) {
		t.Fatalf("Backup() file metadata = %+v, want plaintext size/checksum", manifest.Files)
	}
	if raw := store.objects["backup-old/manifest.json"]; bytes.Contains(raw, []byte(`"files"`)) {
		t.Fatal("encrypted manifest exposes plaintext JSON")
	}
	if raw := store.objects["backup-old/part-a/rows.bin"]; bytes.Equal(raw, payload) || bytes.Contains(raw, payload) {
		t.Fatal("encrypted payload exposes plaintext bytes")
	}

	rotatedTarget, err := hatBackup.NewObjectStoreTargetWithOptions(store, "backup-old", hatBackup.ObjectStoreTargetOptions{
		EncryptionKeys: []hatBackup.ObjectStoreEncryptionKey{
			{ID: "key-old", Key: oldKey},
			{ID: "key-new", Key: newKey},
		},
		ActiveEncryptionKeyID: "key-new",
	})
	if err != nil {
		t.Fatalf("rotated NewObjectStoreTargetWithOptions() error = %v", err)
	}
	restored := filepath.Join(t.TempDir(), "restored-old")
	if got, err := rotatedTarget.Restore(ctx, restored, false); err != nil || got.Encryption == nil || got.Encryption.KeyID != "key-old" {
		t.Fatalf("Restore() manifest/error = %+v/%v, want old key", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(restored, "part-a", "rows.bin")); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("restored payload/error = %q/%v, want original", got, err)
	}
	if _, err := rotatedTarget.Verify(ctx); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}

	newTarget, err := hatBackup.NewObjectStoreTargetWithOptions(store, "backup-new", hatBackup.ObjectStoreTargetOptions{
		EncryptionKeys: []hatBackup.ObjectStoreEncryptionKey{
			{ID: "key-old", Key: oldKey},
			{ID: "key-new", Key: newKey},
		},
		ActiveEncryptionKeyID: "key-new",
	})
	if err != nil {
		t.Fatalf("new-key target error = %v", err)
	}
	newManifest, err := newTarget.Backup(ctx, source, hatBackup.BundleManifest{})
	if err != nil {
		t.Fatalf("new-key Backup() error = %v", err)
	}
	if newManifest.Encryption == nil || newManifest.Encryption.KeyID != "key-new" {
		t.Fatalf("new-key encryption metadata = %+v, want key-new", newManifest.Encryption)
	}

	wrongTarget, err := hatBackup.NewObjectStoreTargetWithOptions(store, "backup-old", hatBackup.ObjectStoreTargetOptions{
		EncryptionKeys:        []hatBackup.ObjectStoreEncryptionKey{{ID: "key-old", Key: bytes.Repeat([]byte{0x33}, 32)}},
		ActiveEncryptionKeyID: "key-old",
	})
	if err != nil {
		t.Fatalf("wrong-key target error = %v", err)
	}
	wrongDestination := filepath.Join(t.TempDir(), "wrong-key")
	if _, err := wrongTarget.Restore(ctx, wrongDestination, false); err == nil {
		t.Fatal("Restore() accepted a wrong encryption key")
	}
	if _, err := os.Stat(wrongDestination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("wrong-key restore destination state, stat error = %v", err)
	}
}

func TestObjectStoreEncryptedBackupRejectsTamperedPayloadBeforePublishing(t *testing.T) {
	ctx := context.Background()
	store := newObjectStoreMemory()
	key := bytes.Repeat([]byte{0x44}, 32)
	target, err := hatBackup.NewObjectStoreTargetWithOptions(store, "backup", hatBackup.ObjectStoreTargetOptions{
		EncryptionKeys:        []hatBackup.ObjectStoreEncryptionKey{{ID: "key-1", Key: key}},
		ActiveEncryptionKeyID: "key-1",
	})
	if err != nil {
		t.Fatalf("NewObjectStoreTargetWithOptions() error = %v", err)
	}
	source := t.TempDir()
	writeObjectStoreFile(t, filepath.Join(source, "rows.bin"), []byte("secure rows"))
	if _, err := target.Backup(ctx, source, hatBackup.BundleManifest{}); err != nil {
		t.Fatalf("Backup() error = %v", err)
	}
	raw := append([]byte(nil), store.objects["backup/rows.bin"]...)
	store.objects["backup/rows.bin"] = append(append([]byte(nil), raw...), 0)
	if _, err := target.Verify(ctx); err == nil {
		t.Fatal("Verify() accepted encrypted payload with trailing bytes")
	}
	raw[len(raw)-1] ^= 1
	store.objects["backup/rows.bin"] = raw
	if _, err := target.Verify(ctx); err == nil {
		t.Fatal("Verify() accepted tampered encrypted payload")
	}
	destination := filepath.Join(t.TempDir(), "restored")
	if _, err := target.Restore(ctx, destination, false); err == nil {
		t.Fatal("Restore() accepted tampered encrypted payload")
	}
	if _, err := os.Stat(destination); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tampered restore destination state, stat error = %v", err)
	}
}

func TestEncryptedTargetRestoresLegacyBundle(t *testing.T) {
	ctx := context.Background()
	store := newObjectStoreMemory()
	source := t.TempDir()
	payload := []byte("legacy payload")
	writeObjectStoreFile(t, filepath.Join(source, "rows.bin"), payload)
	legacy, err := hatBackup.NewObjectStoreTarget(store, "legacy")
	if err != nil {
		t.Fatalf("legacy target error = %v", err)
	}
	if _, err := legacy.Backup(ctx, source, hatBackup.BundleManifest{}); err != nil {
		t.Fatalf("legacy Backup() error = %v", err)
	}
	encrypted, err := hatBackup.NewObjectStoreTargetWithOptions(store, "legacy", hatBackup.ObjectStoreTargetOptions{
		EncryptionKeys:        []hatBackup.ObjectStoreEncryptionKey{{ID: "key-1", Key: bytes.Repeat([]byte{0x66}, 32)}},
		ActiveEncryptionKeyID: "key-1",
	})
	if err != nil {
		t.Fatalf("encrypted target error = %v", err)
	}
	destination := filepath.Join(t.TempDir(), "restored")
	manifest, err := encrypted.Restore(ctx, destination, false)
	if err != nil {
		t.Fatalf("encrypted target legacy Restore() error = %v", err)
	}
	if manifest.Encryption != nil {
		t.Fatalf("legacy manifest encryption metadata = %+v, want nil", manifest.Encryption)
	}
	got, err := os.ReadFile(filepath.Join(destination, "rows.bin"))
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("legacy restored payload/error = %q/%v, want original", got, err)
	}
}

func TestObjectStoreTargetEncryptionOptionsValidateKeyRing(t *testing.T) {
	cases := []hatBackup.ObjectStoreTargetOptions{
		{EncryptionKeys: []hatBackup.ObjectStoreEncryptionKey{{ID: "key", Key: []byte("short")}}, ActiveEncryptionKeyID: "key"},
		{EncryptionKeys: []hatBackup.ObjectStoreEncryptionKey{{ID: "key", Key: bytes.Repeat([]byte{1}, 32)}}, ActiveEncryptionKeyID: "missing"},
		{EncryptionKeys: []hatBackup.ObjectStoreEncryptionKey{{ID: "key", Key: bytes.Repeat([]byte{1}, 32)}, {ID: "key", Key: bytes.Repeat([]byte{2}, 32)}}, ActiveEncryptionKeyID: "key"},
	}
	for index, options := range cases {
		if _, err := hatBackup.NewObjectStoreTargetWithOptions(newObjectStoreMemory(), "backup", options); err == nil {
			t.Errorf("case %d accepted invalid encryption options", index)
		}
	}
}

func BenchmarkObjectStoreTargetEncryptedBackup(b *testing.B) {
	store := newObjectStoreMemory()
	source := b.TempDir()
	writeObjectStoreFile(b, filepath.Join(source, "part", "rows.bin"), bytes.Repeat([]byte("row-value\n"), 1024))
	target, err := hatBackup.NewObjectStoreTargetWithOptions(store, "bench", hatBackup.ObjectStoreTargetOptions{
		EncryptionKeys:        []hatBackup.ObjectStoreEncryptionKey{{ID: "bench-key", Key: bytes.Repeat([]byte{0x55}, 32)}},
		ActiveEncryptionKeyID: "bench-key",
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := target.Backup(context.Background(), source, hatBackup.BundleManifest{}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(len(store.objects["bench/part/rows.bin"])), "stored-bytes")
}

func BenchmarkObjectStoreTargetRestore(b *testing.B) {
	benchmarkObjectStoreTargetRestore(b, false)
}

func BenchmarkObjectStoreTargetEncryptedRestore(b *testing.B) {
	benchmarkObjectStoreTargetRestore(b, true)
}

func benchmarkObjectStoreTargetRestore(b *testing.B, encrypted bool) {
	store := newObjectStoreMemory()
	source := b.TempDir()
	writeObjectStoreFile(b, filepath.Join(source, "part", "rows.bin"), bytes.Repeat([]byte("row-value\n"), 1024))
	var target *hatBackup.ObjectStoreTarget
	var err error
	if encrypted {
		target, err = hatBackup.NewObjectStoreTargetWithOptions(store, "bench", hatBackup.ObjectStoreTargetOptions{
			EncryptionKeys:        []hatBackup.ObjectStoreEncryptionKey{{ID: "bench-key", Key: bytes.Repeat([]byte{0x55}, 32)}},
			ActiveEncryptionKeyID: "bench-key",
		})
	} else {
		target, err = hatBackup.NewObjectStoreTarget(store, "bench")
	}
	if err != nil {
		b.Fatal(err)
	}
	if _, err := target.Backup(context.Background(), source, hatBackup.BundleManifest{}); err != nil {
		b.Fatal(err)
	}
	destinationRoot := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		destination := filepath.Join(destinationRoot, strconv.Itoa(i))
		if _, err := target.Restore(context.Background(), destination, false); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(len(store.objects["bench/part/rows.bin"])), "stored-bytes")
}
