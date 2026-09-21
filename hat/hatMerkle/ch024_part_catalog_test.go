package hatMerkle

import (
	"errors"
	"os"
	"testing"
)

func TestCH024PartCatalogDetachesAndAttachesVerifiedReplacement(t *testing.T) {
	oldPayload := []byte("old immutable part")
	newPayload := []byte("new immutable part")
	oldManifest := PartManifest{Checksum: ChecksumPart(oldPayload)}
	newManifest := PartManifest{Checksum: ChecksumPart(newPayload)}
	payloads := map[string][]byte{
		"part-0001.old": oldPayload,
		"part-0001.new": newPayload,
	}
	verify := func(entry PartCatalogEntry) error {
		payload, ok := payloads[entry.Location]
		if !ok {
			return errors.New("unknown part location")
		}
		return entry.Manifest.Validate(payload)
	}

	catalog, err := NewPartCatalog(PartCatalogOptions{MaxEntries: 4})
	if err != nil {
		t.Fatalf("NewPartCatalog() error = %v", err)
	}
	old := PartCatalogEntry{Name: "part-0001", Location: "part-0001.old", Manifest: oldManifest}
	if err := catalog.Attach(old, verify); err != nil {
		t.Fatalf("Attach(old) error = %v", err)
	}
	if got, ok := catalog.Get("part-0001"); !ok || got.Location != old.Location {
		t.Fatalf("Get(old) = %#v, %v", got, ok)
	}

	detached, err := catalog.Detach("part-0001")
	if err != nil {
		t.Fatalf("Detach() error = %v", err)
	}
	if detached.Location != old.Location || catalog.Len() != 0 {
		t.Fatalf("Detach() = %#v, active length = %d", detached, catalog.Len())
	}
	if got, ok := catalog.GetQuarantined("part-0001"); !ok || got.Location != old.Location {
		t.Fatalf("GetQuarantined(old) = %#v, %v", got, ok)
	}

	bad := PartCatalogEntry{Name: "part-0001", Location: "part-0001.new", Manifest: oldManifest}
	if err := catalog.Attach(bad, verify); !errors.Is(err, ErrPartManifestMismatch) {
		t.Fatalf("Attach(corrupt replacement) error = %v, want manifest mismatch", err)
	}
	if _, ok := catalog.Get("part-0001"); ok {
		t.Fatal("failed replacement became active")
	}
	replacement := PartCatalogEntry{Name: "part-0001", Location: "part-0001.new", Manifest: newManifest}
	if err := catalog.Attach(replacement, verify); err != nil {
		t.Fatalf("Attach(replacement) error = %v", err)
	}
	if got, ok := catalog.Get("part-0001"); !ok || got.Location != replacement.Location {
		t.Fatalf("Get(replacement) = %#v, %v", got, ok)
	}
	if !catalog.RemoveQuarantined("part-0001") {
		t.Fatal("RemoveQuarantined() = false, want true")
	}
	if _, ok := catalog.GetQuarantined("part-0001"); ok {
		t.Fatal("quarantined part remained after removal")
	}
}

func TestCH024PartCatalogRestoreAndBoundedLifecycle(t *testing.T) {
	payload := []byte("restore me")
	entry := PartCatalogEntry{Name: "part", Location: "part", Manifest: PartManifest{Checksum: ChecksumPart(payload)}}
	verify := func(candidate PartCatalogEntry) error {
		return candidate.Manifest.Validate(payload)
	}
	catalog, err := NewPartCatalog(PartCatalogOptions{MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Attach(entry, verify); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Attach(entry, verify); !errors.Is(err, ErrPartCatalogAlreadyAttached) {
		t.Fatalf("duplicate Attach() error = %v", err)
	}
	if _, err := catalog.Detach("part"); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Restore("part", verify); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if _, ok := catalog.GetQuarantined("part"); ok {
		t.Fatal("Restore() left an entry in quarantine")
	}
	if _, err := catalog.Detach("missing"); !errors.Is(err, ErrPartCatalogNotFound) {
		t.Fatalf("missing Detach() error = %v", err)
	}
}

func TestCH024VerifyImmutablePartFilePreservesOffsetAndDetectsCorruption(t *testing.T) {
	payload := []byte("file-backed immutable part")
	file, err := os.CreateTemp(t.TempDir(), "part-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(payload); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	checksum := ChecksumPart(payload)
	catalog, err := NewPartCatalog(PartCatalogOptions{MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.AttachFile(PartCatalogEntry{
		Name:     "file-part",
		Location: file.Name(),
		Manifest: PartManifest{Checksum: checksum},
	}, file); err != nil {
		t.Fatalf("AttachFile() error = %v", err)
	}
	if got, ok := catalog.Get("file-part"); !ok || got.Location != file.Name() {
		t.Fatalf("AttachFile() catalog entry = %#v, %v", got, ok)
	}
	written, err := VerifyImmutablePartFile(file, checksum)
	if err != nil || written != int64(len(payload)) {
		t.Fatalf("VerifyImmutablePartFile() = %d, %v", written, err)
	}
	offset, err := file.Seek(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if offset != 0 {
		t.Fatalf("verification moved file offset to %d", offset)
	}
	if _, err := file.WriteAt([]byte("X"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyImmutablePartFile(file, checksum); !errors.Is(err, ErrInvalidPartChecksum) {
		t.Fatalf("corrupt verification error = %v, want ErrInvalidPartChecksum", err)
	}
}
