package hatMerkle

import (
	"errors"
	"fmt"
	"strings"
)

var (
	ErrPartManifestInvalid        = errors.New("hatriecache: invalid immutable part manifest")
	ErrPartManifestMismatch       = errors.New("hatriecache: immutable part checksum does not match")
	ErrPartColumnChecksumMismatch = errors.New("hatriecache: immutable part column checksum does not match")
)

// PartColumnRange identifies one non-overlapping byte range in an immutable
// part. The range order is retained in the resulting manifest so callers can
// use schema order without a second name-to-column lookup.
type PartColumnRange struct {
	Name   string
	Offset uint64
	Size   uint64
}

// PartColumnChecksum records the expected checksum for one part column.
type PartColumnChecksum struct {
	Name     string
	Offset   uint64
	Size     uint64
	Checksum PartChecksum
}

// PartManifest is the identity and integrity metadata for an immutable part.
// A cache can compare manifests with Equal before reusing an entry, then call
// Validate when bytes have entered or may have changed in local storage.
type PartManifest struct {
	Checksum     PartChecksum
	Columns      []PartColumnChecksum
	DeleteBitmap *PartDeleteBitmap
}

// BuildPartManifest computes a whole-part checksum and independent checksums
// for the supplied column ranges. Ranges must be in bounds, non-overlapping,
// and have unique non-empty names.
func BuildPartManifest(data []byte, columns []PartColumnRange) (PartManifest, error) {
	if err := validatePartColumnRanges(len(data), columns); err != nil {
		return PartManifest{}, err
	}
	manifest := PartManifest{
		Checksum: ChecksumPart(data),
		Columns:  make([]PartColumnChecksum, len(columns)),
	}
	for index, column := range columns {
		name := strings.TrimSpace(column.Name)
		start := int(column.Offset)
		end := start + int(column.Size)
		manifest.Columns[index] = PartColumnChecksum{
			Name:     name,
			Offset:   column.Offset,
			Size:     column.Size,
			Checksum: ChecksumPart(data[start:end]),
		}
	}
	return manifest, nil
}

// Equal reports whether two manifests identify the same part and the same
// ordered set of column ranges and checksums. It performs no allocations.
func (manifest PartManifest) Equal(other PartManifest) bool {
	if manifest.Checksum != other.Checksum || len(manifest.Columns) != len(other.Columns) {
		return false
	}
	if !equalPartDeleteBitmap(manifest.DeleteBitmap, other.DeleteBitmap) {
		return false
	}
	for index, column := range manifest.Columns {
		if column != other.Columns[index] {
			return false
		}
	}
	return true
}

// Validate verifies the whole part and every recorded column before local
// bytes are reused. The whole-part check catches changes outside the recorded
// columns; the column checks identify a corrupted or mismatched column.
func (manifest PartManifest) Validate(data []byte) error {
	if manifest.DeleteBitmap != nil {
		if err := manifest.DeleteBitmap.Validate(); err != nil {
			return err
		}
	}
	if err := validatePartColumnChecksums(len(data), manifest.Columns); err != nil {
		return err
	}
	if !VerifyPartChecksum(data, manifest.Checksum) {
		return ErrPartManifestMismatch
	}
	for _, column := range manifest.Columns {
		start := int(column.Offset)
		end := start + int(column.Size)
		if !VerifyPartChecksum(data[start:end], column.Checksum) {
			return fmt.Errorf("%w: %s", ErrPartColumnChecksumMismatch, column.Name)
		}
	}
	return nil
}

func equalPartDeleteBitmap(left, right *PartDeleteBitmap) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func validatePartColumnRanges(dataSize int, columns []PartColumnRange) error {
	for index, column := range columns {
		if strings.TrimSpace(column.Name) == "" || column.Offset > uint64(dataSize) || column.Size > uint64(dataSize)-column.Offset {
			return fmt.Errorf("%w: column %d range", ErrPartManifestInvalid, index)
		}
		for previous := 0; previous < index; previous++ {
			other := columns[previous]
			if strings.TrimSpace(other.Name) == strings.TrimSpace(column.Name) {
				return fmt.Errorf("%w: duplicate column name %q", ErrPartManifestInvalid, column.Name)
			}
			if rangesOverlap(other.Offset, other.Size, column.Offset, column.Size) {
				return fmt.Errorf("%w: overlapping columns %q and %q", ErrPartManifestInvalid, other.Name, column.Name)
			}
		}
	}
	return nil
}

func validatePartColumnChecksums(dataSize int, columns []PartColumnChecksum) error {
	for index, column := range columns {
		if strings.TrimSpace(column.Name) == "" || column.Offset > uint64(dataSize) || column.Size > uint64(dataSize)-column.Offset {
			return fmt.Errorf("%w: column %d range", ErrPartManifestInvalid, index)
		}
		for previous := 0; previous < index; previous++ {
			other := columns[previous]
			if other.Name == column.Name {
				return fmt.Errorf("%w: duplicate column name %q", ErrPartManifestInvalid, column.Name)
			}
			if rangesOverlap(other.Offset, other.Size, column.Offset, column.Size) {
				return fmt.Errorf("%w: overlapping columns %q and %q", ErrPartManifestInvalid, other.Name, column.Name)
			}
		}
	}
	return nil
}

func rangesOverlap(leftOffset, leftSize, rightOffset, rightSize uint64) bool {
	if leftSize == 0 || rightSize == 0 {
		return false
	}
	return leftOffset < rightOffset+rightSize && rightOffset < leftOffset+leftSize
}
