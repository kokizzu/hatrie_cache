package hatStorage_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"hatrie_cache/hat/hatStorage"
)

func TestMU38ImmutablePartManifestRoundTripIsDeterministic(t *testing.T) {
	t.Parallel()
	first := mu38Manifest(t, "manifest-1", 7, time.Date(2026, 10, 2, 4, 5, 6, 700, time.FixedZone("SGT", 8*60*60)), []hatStorage.ImmutableDataPart{
		mu38Part(t, "part-b", "eu", "m", "z", 2),
		mu38Part(t, "part-a", "eu", "a", "m", 1),
	})
	second := first
	second.Parts = []hatStorage.ImmutableDataPart{first.Parts[1], first.Parts[0]}

	firstPayload, err := first.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	secondPayload, err := second.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() reordered error = %v", err)
	}
	if string(firstPayload) != string(secondPayload) {
		t.Fatal("MarshalBinary() changed when input parts were reordered")
	}

	decoded, err := hatStorage.DecodeImmutablePartManifest(firstPayload)
	if err != nil {
		t.Fatalf("DecodeImmutablePartManifest() error = %v", err)
	}
	normalized, err := first.Normalize()
	if err != nil {
		t.Fatalf("Normalize() error = %v", err)
	}
	if !reflect.DeepEqual(decoded.Parts, normalized.Parts) {
		t.Fatalf("decoded parts = %#v, want %#v", decoded.Parts, normalized.Parts)
	}
	if !decoded.CreatedAt.Equal(normalized.CreatedAt) || decoded.Generation != normalized.Generation || decoded.ManifestID != normalized.ManifestID {
		t.Fatalf("decoded manifest metadata = %#v, want %#v", decoded, normalized)
	}
	references, err := decoded.RemotePartReferences()
	if err != nil {
		t.Fatalf("RemotePartReferences() error = %v", err)
	}
	if len(references) != 2 {
		t.Fatalf("reachable references = %d, want 2", len(references))
	}
}

func TestMU38ImmutablePartManifestRejectsDuplicateAndInvalidParts(t *testing.T) {
	t.Parallel()
	part := mu38Part(t, "part-a", "eu", "a", "m", 1)
	manifest := mu38Manifest(t, "manifest-1", 1, time.Unix(10, 0), []hatStorage.ImmutableDataPart{part, part})
	if _, err := manifest.MarshalBinary(); !errors.Is(err, hatStorage.ErrImmutablePartManifestInvalid) {
		t.Fatalf("duplicate part error = %v, want ErrImmutablePartManifestInvalid", err)
	}

	manifest = mu38Manifest(t, "manifest-1", 1, time.Unix(10, 0), []hatStorage.ImmutableDataPart{{
		PartID:      "part-a",
		PartitionID: "eu",
		Reference:   hatStorage.RemotePartMetadata{ObjectURI: "file:///unsafe", LocalMetadataPath: "part.json", Checksum: "sha256:bad"},
	}})
	if _, err := manifest.MarshalBinary(); !errors.Is(err, hatStorage.ErrImmutablePartManifestInvalid) {
		t.Fatalf("invalid reference error = %v, want ErrImmutablePartManifestInvalid", err)
	}
}

func TestMU38ImmutablePartCatalogPublishesAndReportsTransition(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "parts.manifest")
	catalog, err := hatStorage.NewImmutablePartCatalog(path)
	if err != nil {
		t.Fatalf("NewImmutablePartCatalog() error = %v", err)
	}
	if current, err := catalog.Load(); err != nil {
		t.Fatalf("Load() empty error = %v", err)
	} else if current.ManifestID != "" || len(current.Parts) != 0 {
		t.Fatalf("empty catalog = %#v", current)
	}

	first := mu38Manifest(t, "manifest-1", 1, time.Unix(10, 0), []hatStorage.ImmutableDataPart{
		mu38Part(t, "part-a", "eu", "a", "m", 1),
		mu38Part(t, "part-b", "eu", "m", "z", 2),
	})
	publication, err := catalog.Publish(first)
	if err != nil {
		t.Fatalf("Publish(first) error = %v", err)
	}
	if publication.Previous.ManifestID != "" || len(publication.Added) != 2 || len(publication.Retired) != 0 {
		t.Fatalf("first publication = %#v", publication)
	}

	second := mu38Manifest(t, "manifest-2", 2, time.Unix(20, 0), []hatStorage.ImmutableDataPart{
		mu38Part(t, "part-b", "eu", "m", "z", 2),
		mu38Part(t, "part-c", "eu", "z", "zz", 3),
	})
	publication, err = catalog.Publish(second)
	if err != nil {
		t.Fatalf("Publish(second) error = %v", err)
	}
	if len(publication.Added) != 1 || publication.Added[0].PartID != "part-c" || len(publication.Retired) != 1 || publication.Retired[0].PartID != "part-a" {
		t.Fatalf("second publication = %#v", publication)
	}
	if got, err := catalog.Load(); err != nil {
		t.Fatalf("Load() published error = %v", err)
	} else if got.ManifestID != second.ManifestID || len(got.Parts) != 2 {
		t.Fatalf("loaded current = %#v", got)
	}

	if _, err := catalog.Publish(first); !errors.Is(err, hatStorage.ErrImmutablePartManifestStale) {
		t.Fatalf("stale publication error = %v, want ErrImmutablePartManifestStale", err)
	}
}

func TestMU38ImmutablePartCatalogRejectsCorruptManifest(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "parts.manifest")
	if err := os.WriteFile(path, []byte("not-a-manifest"), 0o600); err != nil {
		t.Fatalf("write corrupt manifest: %v", err)
	}
	catalog, err := hatStorage.NewImmutablePartCatalog(path)
	if err != nil {
		t.Fatalf("NewImmutablePartCatalog() error = %v", err)
	}
	if _, err := catalog.Load(); !errors.Is(err, hatStorage.ErrImmutablePartManifestInvalid) {
		t.Fatalf("corrupt Load() error = %v, want ErrImmutablePartManifestInvalid", err)
	}
}

func TestMU38ImmutablePartCatalogRejectsPartMutationAndKeepsPrevious(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "parts.manifest")
	catalog, err := hatStorage.NewImmutablePartCatalog(path)
	if err != nil {
		t.Fatalf("NewImmutablePartCatalog() error = %v", err)
	}
	first := mu38Manifest(t, "manifest-1", 1, time.Unix(10, 0), []hatStorage.ImmutableDataPart{
		mu38Part(t, "part-a", "eu", "a", "m", 1),
	})
	if _, err := catalog.Publish(first); err != nil {
		t.Fatalf("Publish(first) error = %v", err)
	}
	mutated := mu38Manifest(t, "manifest-2", 2, time.Unix(20, 0), []hatStorage.ImmutableDataPart{
		mu38Part(t, "part-a", "eu", "a", "n", 1),
	})
	if _, err := catalog.Publish(mutated); !errors.Is(err, hatStorage.ErrImmutablePartManifestPartConflict) {
		t.Fatalf("mutated publication error = %v, want ErrImmutablePartManifestPartConflict", err)
	}
	loaded, err := catalog.Load()
	if err != nil {
		t.Fatalf("Load() after rejected publication error = %v", err)
	}
	if loaded.ManifestID != first.ManifestID || loaded.Generation != first.Generation {
		t.Fatalf("current after rejected publication = %#v, want %#v", loaded, first)
	}
}

func TestMU38ManifestReferencesFeedRemotePartGC(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	manifest := mu38Manifest(t, "manifest-1", 1, now, []hatStorage.ImmutableDataPart{
		mu38Part(t, "part-live", "eu", "a", "m", 1),
	})
	references, err := manifest.RemotePartReferences()
	if err != nil {
		t.Fatalf("RemotePartReferences() error = %v", err)
	}
	plan, err := hatStorage.PlanRemotePartGarbageCollection(references, []hatStorage.RemotePartGCObject{
		{ObjectURI: references[0].ObjectURI(), SizeBytes: references[0].SizeBytes(), LastModified: now.Add(-2 * time.Hour)},
		{ObjectURI: "s3://bucket/parts/retired", SizeBytes: 42, LastModified: now.Add(-2 * time.Hour)},
	}, hatStorage.RemotePartGCOptions{Now: now, MinAge: time.Hour})
	if err != nil {
		t.Fatalf("PlanRemotePartGarbageCollection() error = %v", err)
	}
	if len(plan.Candidates) != 1 || plan.Candidates[0].ObjectURI != "s3://bucket/parts/retired" || plan.ReclaimableBytes != 42 {
		t.Fatalf("GC plan = %#v, want one retired object and 42 bytes", plan)
	}
}

func mu38Manifest(t *testing.T, id string, generation uint64, createdAt time.Time, parts []hatStorage.ImmutableDataPart) hatStorage.ImmutablePartManifest {
	t.Helper()
	return hatStorage.ImmutablePartManifest{
		Version:    hatStorage.ImmutablePartManifestVersion,
		ManifestID: id,
		Generation: generation,
		CreatedAt:  createdAt,
		Parts:      parts,
	}
}

func mu38Part(t *testing.T, id, partition, lower, upper string, rowCount uint64) hatStorage.ImmutableDataPart {
	t.Helper()
	reference, err := hatStorage.NewRemotePartReference("s3://bucket/parts/"+id, "parts/"+id+".json", "sha256:"+id, rowCount*10)
	if err != nil {
		t.Fatalf("NewRemotePartReference() error = %v", err)
	}
	return hatStorage.ImmutableDataPart{
		PartID:      id,
		PartitionID: partition,
		Generation:  1,
		RowCount:    rowCount,
		LowerBound:  lower,
		UpperBound:  upper,
		Reference:   reference.Metadata(),
	}
}
