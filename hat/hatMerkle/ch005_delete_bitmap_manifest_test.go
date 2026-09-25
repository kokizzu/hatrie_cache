package hatMerkle

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

func TestCH005DeleteBitmapBindsToManifestAndValidatesSnapshot(t *testing.T) {
	part := []byte("immutable-part")
	snapshot := []byte("delete-bitmap-snapshot")
	bitmap, err := BuildPartDeleteBitmap(snapshot, 128, 7)
	if err != nil {
		t.Fatalf("BuildPartDeleteBitmap() error = %v", err)
	}
	manifest := PartManifest{Checksum: ChecksumPart(part), DeleteBitmap: &bitmap}
	if err := manifest.Validate(part); err != nil {
		t.Fatalf("PartManifest.Validate() error = %v", err)
	}
	if err := bitmap.Verify(snapshot); err != nil {
		t.Fatalf("PartDeleteBitmap.Verify() error = %v", err)
	}
	corrupt := append([]byte(nil), snapshot...)
	corrupt[len(corrupt)-1] ^= 1
	if err := bitmap.Verify(corrupt); !errors.Is(err, ErrPartDeleteBitmapMismatch) {
		t.Fatalf("corrupt bitmap error = %v, want %v", err, ErrPartDeleteBitmapMismatch)
	}

	changed := bitmap
	changed.DeletedCount++
	if manifest.Equal(PartManifest{Checksum: ChecksumPart(part), DeleteBitmap: &changed}) {
		t.Fatal("manifest equality ignored delete bitmap metadata")
	}
}

func TestCH005DeleteBitmapCatalogRoundTrip(t *testing.T) {
	snapshot := []byte("delete-bitmap-snapshot")
	bitmap, err := BuildPartDeleteBitmap(snapshot, 64, 3)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewPartCatalog(PartCatalogOptions{MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	entry := PartCatalogEntry{
		Name:     "part-0001",
		Location: "part-0001",
		Manifest: PartManifest{Checksum: ChecksumPart([]byte("part")), DeleteBitmap: &bitmap},
	}
	if err := catalog.Attach(entry, func(PartCatalogEntry) error { return nil }); err != nil {
		t.Fatalf("Attach() error = %v", err)
	}
	encoded, err := catalog.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	restored, err := RestorePartCatalog(encoded, PartCatalogOptions{MaxEntries: 2})
	if err != nil {
		t.Fatalf("RestorePartCatalog() error = %v", err)
	}
	got, ok := restored.Get("part-0001")
	if !ok || got.Manifest.DeleteBitmap == nil {
		t.Fatalf("restored delete bitmap = %#v, want metadata", got.Manifest.DeleteBitmap)
	}
	if *got.Manifest.DeleteBitmap != bitmap {
		t.Fatalf("restored delete bitmap = %#v, want %#v", *got.Manifest.DeleteBitmap, bitmap)
	}
	got.Manifest.DeleteBitmap.DeletedCount = 0
	fresh, ok := restored.Get("part-0001")
	if !ok || fresh.Manifest.DeleteBitmap == nil || fresh.Manifest.DeleteBitmap.DeletedCount != bitmap.DeletedCount {
		t.Fatal("catalog returned a mutable delete bitmap descriptor")
	}
}

func TestCH005DeleteBitmapRejectsInvalidCounts(t *testing.T) {
	if _, err := BuildPartDeleteBitmap([]byte("snapshot"), 2, 3); !errors.Is(err, ErrPartDeleteBitmapInvalid) {
		t.Fatalf("invalid counts error = %v, want %v", err, ErrPartDeleteBitmapInvalid)
	}
	invalid := PartDeleteBitmap{RowCount: 2, DeletedCount: 3, Snapshot: ChecksumPart([]byte("snapshot"))}
	if err := invalid.Validate(); !errors.Is(err, ErrPartDeleteBitmapInvalid) {
		t.Fatalf("invalid metadata error = %v, want %v", err, ErrPartDeleteBitmapInvalid)
	}
}

func TestCH005RestoreLegacyV1CatalogWithoutDeleteBitmap(t *testing.T) {
	catalog, err := NewPartCatalog(PartCatalogOptions{MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Attach(PartCatalogEntry{
		Name:     "part-0001",
		Location: "part-0001",
		Manifest: PartManifest{Checksum: ChecksumPart([]byte("part"))},
	}, func(PartCatalogEntry) error { return nil }); err != nil {
		t.Fatal(err)
	}
	current, err := catalog.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	payload, _, err := unwrapPartCatalogPersistencePayload(current)
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) == 0 || payload[len(payload)-1] != 0 {
		t.Fatalf("v2 payload does not end in an empty bitmap flag: %x", payload)
	}
	payload = payload[:len(payload)-1]
	legacy := make([]byte, partCatalogPersistenceHeaderBytes+len(payload))
	copy(legacy[:4], partCatalogPersistenceMagic[:])
	binary.LittleEndian.PutUint16(legacy[4:6], partCatalogPersistenceLegacyVersion)
	binary.LittleEndian.PutUint32(legacy[8:12], uint32(len(payload)))
	binary.LittleEndian.PutUint32(legacy[12:16], crc32.Checksum(payload, partCatalogPersistenceCRC32CTable))
	copy(legacy[partCatalogPersistenceHeaderBytes:], payload)
	restored, err := RestorePartCatalog(legacy, PartCatalogOptions{MaxEntries: 1})
	if err != nil {
		t.Fatalf("RestorePartCatalog(v1) error = %v", err)
	}
	entry, ok := restored.Get("part-0001")
	if !ok || entry.Manifest.DeleteBitmap != nil {
		t.Fatalf("legacy entry = %#v, want no delete bitmap", entry)
	}
}

func TestCH005CatalogRejectsInvalidDeleteBitmapFlag(t *testing.T) {
	catalog, err := NewPartCatalog(PartCatalogOptions{MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Attach(PartCatalogEntry{
		Name:     "part-0001",
		Location: "part-0001",
		Manifest: PartManifest{Checksum: ChecksumPart([]byte("part"))},
	}, func(PartCatalogEntry) error { return nil }); err != nil {
		t.Fatal(err)
	}
	encoded, err := catalog.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), encoded...)
	corrupt[len(corrupt)-1] = 2
	payload := corrupt[partCatalogPersistenceHeaderBytes:]
	binary.LittleEndian.PutUint32(corrupt[12:16], crc32.Checksum(payload, partCatalogPersistenceCRC32CTable))
	if _, err := RestorePartCatalog(corrupt, PartCatalogOptions{MaxEntries: 1}); !errors.Is(err, ErrPartCatalogSerializationInvalid) {
		t.Fatalf("invalid bitmap flag error = %v, want %v", err, ErrPartCatalogSerializationInvalid)
	}
}
