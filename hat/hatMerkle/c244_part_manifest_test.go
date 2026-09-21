package hatMerkle

import (
	"errors"
	"testing"
)

func TestC244LocalPartManifestRequiresPartAndColumnChecksums(t *testing.T) {
	data := []byte("key=001|value=alpha|meta=active")
	ranges := []PartColumnRange{
		{Name: "key", Offset: 0, Size: 7},
		{Name: "value", Offset: 7, Size: 12},
		{Name: "meta", Offset: 19, Size: 12},
	}
	manifest, err := BuildPartManifest(data, ranges)
	if err != nil {
		t.Fatalf("BuildPartManifest() error = %v", err)
	}
	if !manifest.Equal(manifest) {
		t.Fatal("manifest should equal itself")
	}
	if err := manifest.Validate(data); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	changedPart := append([]byte(nil), data...)
	changedPart[0] = 'x'
	if err := manifest.Validate(changedPart); !errors.Is(err, ErrPartManifestMismatch) {
		t.Fatalf("changed part error = %v, want ErrPartManifestMismatch", err)
	}

	changedColumn := manifest
	changedColumn.Columns = append([]PartColumnChecksum(nil), manifest.Columns...)
	changedColumn.Columns[1].Checksum = ChecksumPart([]byte("different"))
	if err := changedColumn.Validate(data); !errors.Is(err, ErrPartColumnChecksumMismatch) {
		t.Fatalf("changed column error = %v, want ErrPartColumnChecksumMismatch", err)
	}
	if manifest.Equal(changedColumn) {
		t.Fatal("different column checksum must not be reusable")
	}

	reordered := manifest
	reordered.Columns = append([]PartColumnChecksum(nil), manifest.Columns...)
	reordered.Columns[0], reordered.Columns[1] = reordered.Columns[1], reordered.Columns[0]
	if manifest.Equal(reordered) {
		t.Fatal("different column order must not be reusable")
	}
}

func TestC244LocalPartManifestRejectsInvalidRanges(t *testing.T) {
	data := []byte("0123456789")
	for name, ranges := range map[string][]PartColumnRange{
		"empty name":     {{Name: "", Offset: 0, Size: 1}},
		"out of bounds":  {{Name: "value", Offset: 9, Size: 2}},
		"overlap":        {{Name: "a", Offset: 0, Size: 4}, {Name: "b", Offset: 3, Size: 2}},
		"duplicate name": {{Name: "a", Offset: 0, Size: 2}, {Name: "a", Offset: 2, Size: 2}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := BuildPartManifest(data, ranges); !errors.Is(err, ErrPartManifestInvalid) {
				t.Fatalf("BuildPartManifest() error = %v, want ErrPartManifestInvalid", err)
			}
		})
	}
}
