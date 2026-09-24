package hatMerkle

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestMU38PartCatalogBinaryRoundTripPreservesLifecycle(t *testing.T) {
	oldPayload := []byte("old immutable part payload")
	activePayload := []byte("active immutable part payload")
	oldManifest, err := BuildPartManifest(oldPayload, []PartColumnRange{
		{Name: "prefix", Offset: 0, Size: 3},
		{Name: "suffix", Offset: 3, Size: uint64(len(oldPayload) - 3)},
	})
	if err != nil {
		t.Fatal(err)
	}
	activeManifest := PartManifest{Checksum: ChecksumPart(activePayload)}
	payloads := map[string][]byte{
		"part-old":    oldPayload,
		"part-active": activePayload,
	}
	verify := func(entry PartCatalogEntry) error {
		payload, ok := payloads[entry.Location]
		if !ok {
			return errors.New("unknown part location")
		}
		return entry.Manifest.Validate(payload)
	}

	catalog, err := NewPartCatalog(PartCatalogOptions{MaxEntries: 8})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Attach(PartCatalogEntry{
		Name:     "part-old",
		Location: "part-old",
		Manifest: oldManifest,
	}, verify); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Attach(PartCatalogEntry{
		Name:     "part-active",
		Location: "part-active",
		Manifest: activeManifest,
	}, verify); err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Detach("part-old"); err != nil {
		t.Fatal(err)
	}

	activeBefore, quarantinedBefore := catalog.Snapshot()
	encoded, err := catalog.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	encodedAgain, err := catalog.MarshalBinary()
	if err != nil {
		t.Fatalf("second MarshalBinary() error = %v", err)
	}
	if !bytes.Equal(encoded, encodedAgain) {
		t.Fatal("MarshalBinary() is not deterministic")
	}

	restored, err := RestorePartCatalog(encoded, PartCatalogOptions{MaxEntries: 8})
	if err != nil {
		t.Fatalf("RestorePartCatalog() error = %v", err)
	}
	activeAfter, quarantinedAfter := restored.Snapshot()
	if !reflect.DeepEqual(activeAfter, activeBefore) {
		t.Fatalf("active snapshot after restore = %#v, want %#v", activeAfter, activeBefore)
	}
	if !reflect.DeepEqual(quarantinedAfter, quarantinedBefore) {
		t.Fatalf("quarantine snapshot after restore = %#v, want %#v", quarantinedAfter, quarantinedBefore)
	}
	if restored.Generation() != catalog.Generation() {
		t.Fatalf("restored generation = %d, want %d", restored.Generation(), catalog.Generation())
	}
	reencoded, err := restored.MarshalBinary()
	if err != nil {
		t.Fatalf("restored MarshalBinary() error = %v", err)
	}
	if !bytes.Equal(reencoded, encoded) {
		t.Fatal("restored catalog did not retain canonical bytes")
	}
}

func TestMU38PartCatalogBinaryRejectsCorruptionAndBounds(t *testing.T) {
	catalog, err := NewPartCatalog(PartCatalogOptions{MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("part")
	if err := catalog.Attach(PartCatalogEntry{
		Name:     "part",
		Location: "part",
		Manifest: PartManifest{Checksum: ChecksumPart(payload)},
	}, func(entry PartCatalogEntry) error {
		return entry.Manifest.Validate(payload)
	}); err != nil {
		t.Fatal(err)
	}
	encoded, err := catalog.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	mutated := append([]byte(nil), encoded...)
	mutated[len(mutated)-1] ^= 0xff
	for _, test := range []struct {
		name string
		data []byte
	}{
		{name: "truncated", data: encoded[:len(encoded)-1]},
		{name: "checksum mismatch", data: mutated},
		{name: "trailing bytes", data: append(append([]byte(nil), encoded...), 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := RestorePartCatalog(test.data, PartCatalogOptions{MaxEntries: 2}); !errors.Is(err, ErrPartCatalogSerializationInvalid) {
				t.Fatalf("RestorePartCatalog() error = %v, want ErrPartCatalogSerializationInvalid", err)
			}
		})
	}
	if _, err := RestorePartCatalog(encoded, PartCatalogOptions{MaxEntries: 0}); err != nil {
		t.Fatalf("RestorePartCatalog() with default capacity error = %v", err)
	}
	if _, err := RestorePartCatalog(encoded, PartCatalogOptions{MaxEntries: 0}); err != nil {
		t.Fatalf("second RestorePartCatalog() error = %v", err)
	}
}

func TestMU38PartCatalogSaveLoadUsesAtomicPrivateFile(t *testing.T) {
	payload := []byte("persisted part")
	catalog, err := NewPartCatalog(PartCatalogOptions{MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Attach(PartCatalogEntry{
		Name:     "part",
		Location: "parts/part",
		Manifest: PartManifest{Checksum: ChecksumPart(payload)},
	}, func(entry PartCatalogEntry) error {
		return entry.Manifest.Validate(payload)
	}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "catalog.bin")
	if err := catalog.Save(path); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("catalog mode = %o, want 600", got)
	}
	loaded, err := LoadPartCatalog(path, PartCatalogOptions{MaxEntries: 2})
	if err != nil {
		t.Fatalf("LoadPartCatalog() error = %v", err)
	}
	loadedActive, loadedQuarantined := loaded.Snapshot()
	originalActive, originalQuarantined := catalog.Snapshot()
	if !reflect.DeepEqual(loadedActive, originalActive) || !reflect.DeepEqual(loadedQuarantined, originalQuarantined) {
		t.Fatal("LoadPartCatalog() snapshot differs from saved catalog")
	}
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(path) {
		t.Fatalf("temporary save files remain: %#v", entries)
	}
}

func TestMU38PartCatalogPersistenceRejectsNilAndMissingPath(t *testing.T) {
	var catalog *PartCatalog
	if _, err := catalog.MarshalBinary(); !errors.Is(err, ErrPartCatalogNil) {
		t.Fatalf("nil MarshalBinary() error = %v", err)
	}
	if err := catalog.Save("catalog.bin"); !errors.Is(err, ErrPartCatalogNil) {
		t.Fatalf("nil Save() error = %v", err)
	}
	if _, err := LoadPartCatalog(filepath.Join(t.TempDir(), "missing"), PartCatalogOptions{}); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing LoadPartCatalog() error = %v", err)
	}
}

func TestMU38PartCatalogPersistenceRejectsUnsupportedHeaderAndOversizedCount(t *testing.T) {
	basePayload := make([]byte, 16)
	binary.LittleEndian.PutUint32(basePayload[8:12], 0)
	binary.LittleEndian.PutUint32(basePayload[12:16], 0)
	valid, err := wrapPartCatalogPersistencePayload(basePayload)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		data []byte
	}{
		{name: "unsupported version", data: mutateMU38Header(valid, func(data []byte) { data[4]++ })},
		{name: "unsupported flags", data: mutateMU38Header(valid, func(data []byte) { data[6] = 1 })},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := RestorePartCatalog(test.data, PartCatalogOptions{}); !errors.Is(err, ErrPartCatalogSerializationInvalid) {
				t.Fatalf("RestorePartCatalog() error = %v, want ErrPartCatalogSerializationInvalid", err)
			}
		})
	}
	oversizedPayload := append([]byte(nil), basePayload...)
	binary.LittleEndian.PutUint32(oversizedPayload[8:12], uint32(partCatalogPersistenceMaxEntries+1))
	oversized, err := wrapPartCatalogPersistencePayload(oversizedPayload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RestorePartCatalog(oversized, PartCatalogOptions{}); !errors.Is(err, ErrPartCatalogSerializationInvalid) {
		t.Fatalf("oversized RestorePartCatalog() error = %v, want ErrPartCatalogSerializationInvalid", err)
	}
}

func mutateMU38Header(data []byte, mutate func([]byte)) []byte {
	mutated := append([]byte(nil), data...)
	mutate(mutated)
	return mutated
}
