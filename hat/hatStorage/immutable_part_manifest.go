package hatStorage

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// ImmutablePartManifestVersion is the current persisted manifest contract.
	ImmutablePartManifestVersion uint64 = 1
	// MaxImmutablePartManifestParts bounds one manifest and its restore memory.
	MaxImmutablePartManifestParts = 1_000_000
	// MaxImmutablePartManifestBytes bounds one encoded manifest before it is
	// written or decoded.
	MaxImmutablePartManifestBytes = 64 << 20

	immutablePartManifestCodecVersion = 1
	immutablePartManifestHeaderSize   = 5
	immutablePartManifestDigestSize   = sha256.Size
	maxImmutablePartManifestString    = 1 << 20
)

var (
	immutablePartManifestMagic = [4]byte{'H', 'I', 'P', '1'}

	// ErrImmutablePartManifestInvalid indicates malformed or inconsistent
	// manifest metadata or bytes.
	ErrImmutablePartManifestInvalid = errors.New("hatriecache: immutable part manifest is invalid")
	// ErrImmutablePartManifestStale indicates a publication older than or equal
	// to the currently published generation.
	ErrImmutablePartManifestStale = errors.New("hatriecache: immutable part manifest generation is stale")
	// ErrImmutablePartManifestPartConflict indicates that an immutable part ID
	// was reused for different bytes or placement metadata.
	ErrImmutablePartManifestPartConflict = errors.New("hatriecache: immutable part identity conflicts with published metadata")
)

// ImmutableDataPart describes one immutable data object. The object must be
// uploaded and checksum-verified before its containing manifest is published.
// LowerBound and UpperBound are opaque caller-owned keys for pruning; the
// manifest stores them without interpreting their encoding.
type ImmutableDataPart struct {
	PartID      string             `json:"part_id"`
	PartitionID string             `json:"partition_id"`
	Generation  uint64             `json:"generation"`
	RowCount    uint64             `json:"row_count"`
	LowerBound  string             `json:"lower_bound,omitempty"`
	UpperBound  string             `json:"upper_bound,omitempty"`
	Reference   RemotePartMetadata `json:"reference"`
}

// ImmutablePartManifest is a deterministic set of immutable data parts. A
// manifest is published as one atomic metadata file; its objects are not
// deleted automatically when a newer manifest retires them.
type ImmutablePartManifest struct {
	Version    uint64              `json:"version"`
	ManifestID string              `json:"manifest_id"`
	Generation uint64              `json:"generation"`
	CreatedAt  time.Time           `json:"created_at"`
	Parts      []ImmutableDataPart `json:"parts"`
}

// ImmutablePartPublication is the result of publishing one manifest. Added
// parts are now reachable from the current manifest; retired parts are safe
// to consider for a separately reviewed, retention-aware GC plan.
type ImmutablePartPublication struct {
	Previous ImmutablePartManifest `json:"previous"`
	Current  ImmutablePartManifest `json:"current"`
	Added    []ImmutableDataPart   `json:"added"`
	Retired  []ImmutableDataPart   `json:"retired"`
}

// ImmutablePartCatalog atomically persists the current immutable-part
// manifest. A catalog serializes writers in one process. Cross-process
// ownership and object-store upload/delete authorization remain caller-owned.
type ImmutablePartCatalog struct {
	mu      sync.Mutex
	path    string
	loaded  bool
	current ImmutablePartManifest
}

// NewImmutablePartCatalog validates a manifest path. The parent directory is
// created by the first publication.
func NewImmutablePartCatalog(path string) (*ImmutablePartCatalog, error) {
	path, err := normalizeImmutablePartManifestPath(path)
	if err != nil {
		return nil, err
	}
	return &ImmutablePartCatalog{path: path}, nil
}

// Normalize validates and returns an independent deterministic manifest copy.
func (manifest ImmutablePartManifest) Normalize() (ImmutablePartManifest, error) {
	if manifest.Version != ImmutablePartManifestVersion {
		return ImmutablePartManifest{}, immutablePartManifestError("unsupported version %d", manifest.Version)
	}
	manifestID, err := normalizeImmutablePartManifestString("manifest ID", manifest.ManifestID, false)
	if err != nil {
		return ImmutablePartManifest{}, err
	}
	if manifest.Generation == 0 {
		return ImmutablePartManifest{}, immutablePartManifestError("generation must be positive")
	}
	if manifest.CreatedAt.IsZero() {
		return ImmutablePartManifest{}, immutablePartManifestError("created time is required")
	}
	if len(manifest.Parts) > MaxImmutablePartManifestParts {
		return ImmutablePartManifest{}, immutablePartManifestError("part count exceeds %d", MaxImmutablePartManifestParts)
	}

	normalized := ImmutablePartManifest{
		Version:    manifest.Version,
		ManifestID: manifestID,
		Generation: manifest.Generation,
		CreatedAt:  manifest.CreatedAt.UTC(),
		Parts:      make([]ImmutableDataPart, 0, len(manifest.Parts)),
	}
	seenPartIDs := make(map[string]struct{}, len(manifest.Parts))
	for _, input := range manifest.Parts {
		partID, err := normalizeImmutablePartManifestString("part ID", input.PartID, false)
		if err != nil {
			return ImmutablePartManifest{}, err
		}
		partitionID, err := normalizeImmutablePartManifestString("partition ID", input.PartitionID, false)
		if err != nil {
			return ImmutablePartManifest{}, err
		}
		if _, exists := seenPartIDs[partID]; exists {
			return ImmutablePartManifest{}, immutablePartManifestError("duplicate part ID %q", partID)
		}
		seenPartIDs[partID] = struct{}{}
		if input.Generation == 0 || input.Generation > manifest.Generation {
			return ImmutablePartManifest{}, immutablePartManifestError("part %q generation is outside manifest generation", partID)
		}
		lowerBound, err := normalizeImmutablePartManifestString("lower bound", input.LowerBound, true)
		if err != nil {
			return ImmutablePartManifest{}, err
		}
		upperBound, err := normalizeImmutablePartManifestString("upper bound", input.UpperBound, true)
		if err != nil {
			return ImmutablePartManifest{}, err
		}
		reference, err := NewRemotePartReference(
			input.Reference.ObjectURI,
			input.Reference.LocalMetadataPath,
			input.Reference.Checksum,
			input.Reference.SizeBytes,
		)
		if err != nil {
			return ImmutablePartManifest{}, immutablePartManifestError("part %q reference: %v", partID, err)
		}
		normalized.Parts = append(normalized.Parts, ImmutableDataPart{
			PartID:      partID,
			PartitionID: partitionID,
			Generation:  input.Generation,
			RowCount:    input.RowCount,
			LowerBound:  lowerBound,
			UpperBound:  upperBound,
			Reference:   reference.Metadata(),
		})
	}
	sort.Slice(normalized.Parts, func(left, right int) bool {
		if normalized.Parts[left].PartitionID != normalized.Parts[right].PartitionID {
			return normalized.Parts[left].PartitionID < normalized.Parts[right].PartitionID
		}
		return normalized.Parts[left].PartID < normalized.Parts[right].PartID
	})
	return normalized, nil
}

// MarshalBinary encodes a compact, deterministic, checksummed manifest.
func (manifest ImmutablePartManifest) MarshalBinary() ([]byte, error) {
	normalized, err := manifest.normalizeForMarshal()
	if err != nil {
		return nil, err
	}
	payload := make([]byte, 0, immutablePartManifestHeaderSize+256+len(normalized.Parts)*160)
	payload = append(payload, immutablePartManifestMagic[:]...)
	payload = append(payload, immutablePartManifestCodecVersion)
	payload = appendImmutablePartUvarint(payload, normalized.Version)
	payload = appendImmutablePartUvarint(payload, normalized.Generation)
	payload = appendImmutablePartVarint(payload, normalized.CreatedAt.UnixNano())
	payload = appendImmutablePartString(payload, normalized.ManifestID)
	payload = appendImmutablePartUvarint(payload, uint64(len(normalized.Parts)))
	for _, part := range normalized.Parts {
		payload = appendImmutablePartString(payload, part.PartID)
		payload = appendImmutablePartString(payload, part.PartitionID)
		payload = appendImmutablePartUvarint(payload, part.Generation)
		payload = appendImmutablePartUvarint(payload, part.RowCount)
		payload = appendImmutablePartString(payload, part.LowerBound)
		payload = appendImmutablePartString(payload, part.UpperBound)
		payload = appendImmutablePartString(payload, part.Reference.ObjectURI)
		payload = appendImmutablePartString(payload, part.Reference.LocalMetadataPath)
		payload = appendImmutablePartUvarint(payload, part.Reference.SizeBytes)
		payload = appendImmutablePartString(payload, part.Reference.Checksum)
	}
	if len(payload)+immutablePartManifestDigestSize > MaxImmutablePartManifestBytes {
		return nil, immutablePartManifestError("encoded size exceeds %d bytes", MaxImmutablePartManifestBytes)
	}
	digest := sha256.Sum256(payload)
	return append(payload, digest[:]...), nil
}

func (manifest ImmutablePartManifest) normalizeForMarshal() (ImmutablePartManifest, error) {
	if err := validateCanonicalImmutablePartManifest(manifest); err == nil {
		return manifest, nil
	}
	return manifest.Normalize()
}

// DecodeImmutablePartManifest decodes and validates a binary manifest.
func DecodeImmutablePartManifest(payload []byte) (ImmutablePartManifest, error) {
	if len(payload) < immutablePartManifestHeaderSize+immutablePartManifestDigestSize || len(payload) > MaxImmutablePartManifestBytes {
		return ImmutablePartManifest{}, ErrImmutablePartManifestInvalid
	}
	bodyEnd := len(payload) - immutablePartManifestDigestSize
	if !bytes.Equal(payload[:4], immutablePartManifestMagic[:]) || payload[4] != immutablePartManifestCodecVersion {
		return ImmutablePartManifest{}, ErrImmutablePartManifestInvalid
	}
	expected := sha256.Sum256(payload[:bodyEnd])
	if subtle.ConstantTimeCompare(expected[:], payload[bodyEnd:]) != 1 {
		return ImmutablePartManifest{}, ErrImmutablePartManifestInvalid
	}
	offset := immutablePartManifestHeaderSize
	version, err := readImmutablePartUvarint(payload[:bodyEnd], &offset)
	if err != nil {
		return ImmutablePartManifest{}, err
	}
	generation, err := readImmutablePartUvarint(payload[:bodyEnd], &offset)
	if err != nil {
		return ImmutablePartManifest{}, err
	}
	createdUnixNano, err := readImmutablePartVarint(payload[:bodyEnd], &offset)
	if err != nil {
		return ImmutablePartManifest{}, err
	}
	manifestID, err := readImmutablePartString(payload[:bodyEnd], &offset)
	if err != nil {
		return ImmutablePartManifest{}, err
	}
	partCount, err := readImmutablePartUvarint(payload[:bodyEnd], &offset)
	if err != nil || partCount > MaxImmutablePartManifestParts {
		return ImmutablePartManifest{}, ErrImmutablePartManifestInvalid
	}
	parts := make([]ImmutableDataPart, 0, int(partCount))
	for index := uint64(0); index < partCount; index++ {
		partID, err := readImmutablePartString(payload[:bodyEnd], &offset)
		if err != nil {
			return ImmutablePartManifest{}, err
		}
		partitionID, err := readImmutablePartString(payload[:bodyEnd], &offset)
		if err != nil {
			return ImmutablePartManifest{}, err
		}
		partGeneration, err := readImmutablePartUvarint(payload[:bodyEnd], &offset)
		if err != nil {
			return ImmutablePartManifest{}, err
		}
		rowCount, err := readImmutablePartUvarint(payload[:bodyEnd], &offset)
		if err != nil {
			return ImmutablePartManifest{}, err
		}
		lowerBound, err := readImmutablePartString(payload[:bodyEnd], &offset)
		if err != nil {
			return ImmutablePartManifest{}, err
		}
		upperBound, err := readImmutablePartString(payload[:bodyEnd], &offset)
		if err != nil {
			return ImmutablePartManifest{}, err
		}
		objectURI, err := readImmutablePartString(payload[:bodyEnd], &offset)
		if err != nil {
			return ImmutablePartManifest{}, err
		}
		localMetadataPath, err := readImmutablePartString(payload[:bodyEnd], &offset)
		if err != nil {
			return ImmutablePartManifest{}, err
		}
		sizeBytes, err := readImmutablePartUvarint(payload[:bodyEnd], &offset)
		if err != nil {
			return ImmutablePartManifest{}, err
		}
		checksum, err := readImmutablePartString(payload[:bodyEnd], &offset)
		if err != nil {
			return ImmutablePartManifest{}, err
		}
		parts = append(parts, ImmutableDataPart{
			PartID:      partID,
			PartitionID: partitionID,
			Generation:  partGeneration,
			RowCount:    rowCount,
			LowerBound:  lowerBound,
			UpperBound:  upperBound,
			Reference: RemotePartMetadata{
				ObjectURI:         objectURI,
				LocalMetadataPath: localMetadataPath,
				SizeBytes:         sizeBytes,
				Checksum:          checksum,
			},
		})
	}
	if offset != bodyEnd {
		return ImmutablePartManifest{}, ErrImmutablePartManifestInvalid
	}
	decoded := ImmutablePartManifest{
		Version:    version,
		ManifestID: manifestID,
		Generation: generation,
		CreatedAt:  time.Unix(0, createdUnixNano).UTC(),
		Parts:      parts,
	}
	if err := validateCanonicalImmutablePartManifest(decoded); err == nil {
		return decoded, nil
	}
	return decoded.Normalize()
}

// UnmarshalBinary decodes without mutating the receiver on failure.
func (manifest *ImmutablePartManifest) UnmarshalBinary(payload []byte) error {
	if manifest == nil {
		return ErrImmutablePartManifestInvalid
	}
	decoded, err := DecodeImmutablePartManifest(payload)
	if err != nil {
		return err
	}
	*manifest = decoded
	return nil
}

// RemotePartReferences returns independent validated references for object
// store reachability and the existing remote-part GC planner.
func (manifest ImmutablePartManifest) RemotePartReferences() ([]RemotePartReference, error) {
	normalized, err := manifest.Normalize()
	if err != nil {
		return nil, err
	}
	references := make([]RemotePartReference, 0, len(normalized.Parts))
	for _, part := range normalized.Parts {
		reference, err := NewRemotePartReference(
			part.Reference.ObjectURI,
			part.Reference.LocalMetadataPath,
			part.Reference.Checksum,
			part.Reference.SizeBytes,
		)
		if err != nil {
			return nil, immutablePartManifestError("part %q reference: %v", part.PartID, err)
		}
		references = append(references, reference)
	}
	return references, nil
}

// DiffImmutablePartManifests computes the safe publication transition. A
// reused part ID must describe identical immutable metadata in both manifests.
func DiffImmutablePartManifests(previous, current ImmutablePartManifest) (ImmutablePartPublication, error) {
	var normalizedPrevious ImmutablePartManifest
	var err error
	if !isZeroImmutablePartManifest(previous) {
		normalizedPrevious, err = previous.Normalize()
		if err != nil {
			return ImmutablePartPublication{}, err
		}
	}
	normalizedCurrent, err := current.Normalize()
	if err != nil {
		return ImmutablePartPublication{}, err
	}
	if normalizedPrevious.ManifestID != "" && normalizedCurrent.Generation <= normalizedPrevious.Generation {
		return ImmutablePartPublication{}, fmt.Errorf("%w: generation %d is not newer than %d", ErrImmutablePartManifestStale, normalizedCurrent.Generation, normalizedPrevious.Generation)
	}
	previousByID := make(map[string]ImmutableDataPart, len(normalizedPrevious.Parts))
	for _, part := range normalizedPrevious.Parts {
		previousByID[part.PartID] = part
	}
	currentByID := make(map[string]ImmutableDataPart, len(normalizedCurrent.Parts))
	publication := ImmutablePartPublication{
		Previous: cloneImmutablePartManifest(normalizedPrevious),
		Current:  cloneImmutablePartManifest(normalizedCurrent),
	}
	for _, part := range normalizedCurrent.Parts {
		currentByID[part.PartID] = part
		previous, exists := previousByID[part.PartID]
		if !exists {
			publication.Added = append(publication.Added, part)
			continue
		}
		if !sameImmutableDataPart(previous, part) {
			return ImmutablePartPublication{}, fmt.Errorf("%w: part %q", ErrImmutablePartManifestPartConflict, part.PartID)
		}
	}
	for _, part := range normalizedPrevious.Parts {
		if _, remains := currentByID[part.PartID]; !remains {
			publication.Retired = append(publication.Retired, part)
		}
	}
	return publication, nil
}

func isZeroImmutablePartManifest(manifest ImmutablePartManifest) bool {
	return manifest.Version == 0 && manifest.ManifestID == "" && manifest.Generation == 0 && manifest.CreatedAt.IsZero() && len(manifest.Parts) == 0
}

// Load returns the current manifest. A missing catalog is an empty catalog.
func (catalog *ImmutablePartCatalog) Load() (ImmutablePartManifest, error) {
	if catalog == nil {
		return ImmutablePartManifest{}, ErrImmutablePartManifestInvalid
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if !catalog.loaded {
		current, err := LoadImmutablePartManifest(catalog.path)
		if err != nil {
			return ImmutablePartManifest{}, err
		}
		catalog.current = current
		catalog.loaded = true
	}
	return cloneImmutablePartManifest(catalog.current), nil
}

// Publish atomically makes a newer manifest current and reports the exact
// transition. It does not upload or delete any object.
func (catalog *ImmutablePartCatalog) Publish(manifest ImmutablePartManifest) (ImmutablePartPublication, error) {
	if catalog == nil {
		return ImmutablePartPublication{}, ErrImmutablePartManifestInvalid
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if !catalog.loaded {
		current, err := LoadImmutablePartManifest(catalog.path)
		if err != nil {
			return ImmutablePartPublication{}, err
		}
		catalog.current = current
		catalog.loaded = true
	}
	publication, err := DiffImmutablePartManifests(catalog.current, manifest)
	if err != nil {
		return ImmutablePartPublication{}, err
	}
	if err := SaveImmutablePartManifest(catalog.path, publication.Current); err != nil {
		return ImmutablePartPublication{}, err
	}
	catalog.current = cloneImmutablePartManifest(publication.Current)
	return cloneImmutablePartPublication(publication), nil
}

// SaveImmutablePartManifest atomically writes a validated manifest with
// restrictive file permissions and a directory sync.
func SaveImmutablePartManifest(path string, manifest ImmutablePartManifest) error {
	normalizedPath, err := normalizeImmutablePartManifestPath(path)
	if err != nil {
		return err
	}
	payload, err := manifest.MarshalBinary()
	if err != nil {
		return err
	}
	if err := rejectImmutablePartManifestPath(normalizedPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	directory := filepath.Dir(normalizedPath)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return fmt.Errorf("hatriecache: create immutable part manifest directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".hatrie-immutable-part-manifest-*.tmp")
	if err != nil {
		return fmt.Errorf("hatriecache: create immutable part manifest staging file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("hatriecache: secure immutable part manifest staging file: %w", err)
	}
	if _, err := temporary.Write(payload); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("hatriecache: write immutable part manifest staging file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("hatriecache: sync immutable part manifest staging file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("hatriecache: close immutable part manifest staging file: %w", err)
	}
	if err := os.Rename(temporaryPath, normalizedPath); err != nil {
		return fmt.Errorf("hatriecache: publish immutable part manifest: %w", err)
	}
	return syncImmutablePartManifestDirectory(directory)
}

// LoadImmutablePartManifest reads and validates one manifest. A missing path
// returns the zero manifest so first publication needs no special bootstrap.
func LoadImmutablePartManifest(path string) (ImmutablePartManifest, error) {
	normalizedPath, err := normalizeImmutablePartManifestPath(path)
	if err != nil {
		return ImmutablePartManifest{}, err
	}
	if err := rejectImmutablePartManifestPath(normalizedPath); err != nil {
		if os.IsNotExist(err) {
			return ImmutablePartManifest{}, nil
		}
		return ImmutablePartManifest{}, err
	}
	payload, err := os.ReadFile(normalizedPath)
	if err != nil {
		return ImmutablePartManifest{}, fmt.Errorf("hatriecache: read immutable part manifest: %w", err)
	}
	manifest, err := DecodeImmutablePartManifest(payload)
	if err != nil {
		return ImmutablePartManifest{}, err
	}
	return manifest, nil
}

func normalizeImmutablePartManifestPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", immutablePartManifestError("path is required")
	}
	path = filepath.Clean(path)
	if path == "." || path == string(filepath.Separator) {
		return "", immutablePartManifestError("path must be a file")
	}
	return path, nil
}

func rejectImmutablePartManifestPath(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return immutablePartManifestError("path is a symlink")
	}
	if !info.Mode().IsRegular() {
		return immutablePartManifestError("path is not a regular file")
	}
	return nil
}

func syncImmutablePartManifestDirectory(directory string) error {
	handle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("hatriecache: open immutable part manifest directory: %w", err)
	}
	defer handle.Close()
	if err := handle.Sync(); err != nil {
		return fmt.Errorf("hatriecache: sync immutable part manifest directory: %w", err)
	}
	return nil
}

func normalizeImmutablePartManifestString(label, value string, allowEmpty bool) (string, error) {
	if allowEmpty && value == "" {
		return "", nil
	}
	if value == "" || strings.TrimSpace(value) != value || len(value) > maxImmutablePartManifestString || strings.IndexByte(value, 0) >= 0 {
		return "", immutablePartManifestError("%s is empty, oversized, or not canonical", label)
	}
	return value, nil
}

func immutablePartManifestError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrImmutablePartManifestInvalid, fmt.Sprintf(format, args...))
}

func validateCanonicalImmutablePartManifest(manifest ImmutablePartManifest) error {
	if manifest.Version != ImmutablePartManifestVersion || manifest.Generation == 0 || manifest.CreatedAt.IsZero() || manifest.CreatedAt.Location() != time.UTC || len(manifest.Parts) > MaxImmutablePartManifestParts {
		return ErrImmutablePartManifestInvalid
	}
	if _, err := normalizeImmutablePartManifestString("manifest ID", manifest.ManifestID, false); err != nil {
		return err
	}
	previousPartition := ""
	previousPartID := ""
	for index, part := range manifest.Parts {
		if _, err := normalizeImmutablePartManifestString("part ID", part.PartID, false); err != nil {
			return err
		}
		if _, err := normalizeImmutablePartManifestString("partition ID", part.PartitionID, false); err != nil {
			return err
		}
		if index > 0 && (part.PartitionID < previousPartition || (part.PartitionID == previousPartition && part.PartID <= previousPartID)) {
			return ErrImmutablePartManifestInvalid
		}
		if part.Generation == 0 || part.Generation > manifest.Generation {
			return ErrImmutablePartManifestInvalid
		}
		if _, err := normalizeImmutablePartManifestString("lower bound", part.LowerBound, true); err != nil {
			return err
		}
		if _, err := normalizeImmutablePartManifestString("upper bound", part.UpperBound, true); err != nil {
			return err
		}
		if err := validateCanonicalImmutablePartReference(part.Reference); err != nil {
			return ErrImmutablePartManifestInvalid
		}
		previousPartition = part.PartitionID
		previousPartID = part.PartID
	}
	return nil
}

func validateCanonicalImmutablePartReference(metadata RemotePartMetadata) error {
	if !isCanonicalRemotePartGCURI(metadata.ObjectURI) || metadata.LocalMetadataPath == "" || filepath.IsAbs(metadata.LocalMetadataPath) || strings.IndexByte(metadata.LocalMetadataPath, 0) >= 0 || filepath.Clean(metadata.LocalMetadataPath) != metadata.LocalMetadataPath || metadata.LocalMetadataPath == "." || metadata.LocalMetadataPath == ".." || strings.HasPrefix(metadata.LocalMetadataPath, ".."+string(filepath.Separator)) || metadata.Checksum == "" {
		return ErrImmutablePartManifestInvalid
	}
	return nil
}

func appendImmutablePartString(payload []byte, value string) []byte {
	payload = appendImmutablePartUvarint(payload, uint64(len(value)))
	return append(payload, value...)
}

func appendImmutablePartUvarint(payload []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	size := binary.PutUvarint(encoded[:], value)
	return append(payload, encoded[:size]...)
}

func appendImmutablePartVarint(payload []byte, value int64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	size := binary.PutVarint(encoded[:], value)
	return append(payload, encoded[:size]...)
}

func readImmutablePartUvarint(payload []byte, offset *int) (uint64, error) {
	if offset == nil || *offset < 0 || *offset >= len(payload) {
		return 0, ErrImmutablePartManifestInvalid
	}
	value, size := binary.Uvarint(payload[*offset:])
	if size <= 0 {
		return 0, ErrImmutablePartManifestInvalid
	}
	*offset += size
	return value, nil
}

func readImmutablePartVarint(payload []byte, offset *int) (int64, error) {
	if offset == nil || *offset < 0 || *offset >= len(payload) {
		return 0, ErrImmutablePartManifestInvalid
	}
	value, size := binary.Varint(payload[*offset:])
	if size <= 0 {
		return 0, ErrImmutablePartManifestInvalid
	}
	*offset += size
	return value, nil
}

func readImmutablePartString(payload []byte, offset *int) (string, error) {
	if offset == nil || *offset < 0 || *offset > len(payload) {
		return "", ErrImmutablePartManifestInvalid
	}
	length, size := binary.Uvarint(payload[*offset:])
	if size <= 0 || size > len(payload)-*offset || length > maxImmutablePartManifestString || length > uint64(len(payload)-*offset-size) {
		return "", ErrImmutablePartManifestInvalid
	}
	*offset += size
	end := *offset + int(length)
	value := string(payload[*offset:end])
	*offset = end
	return value, nil
}

func cloneImmutablePartManifest(manifest ImmutablePartManifest) ImmutablePartManifest {
	manifest.Parts = append([]ImmutableDataPart(nil), manifest.Parts...)
	return manifest
}

func cloneImmutablePartPublication(publication ImmutablePartPublication) ImmutablePartPublication {
	publication.Previous = cloneImmutablePartManifest(publication.Previous)
	publication.Current = cloneImmutablePartManifest(publication.Current)
	publication.Added = append([]ImmutableDataPart(nil), publication.Added...)
	publication.Retired = append([]ImmutableDataPart(nil), publication.Retired...)
	return publication
}

func sameImmutableDataPart(left, right ImmutableDataPart) bool {
	return left.PartID == right.PartID &&
		left.PartitionID == right.PartitionID &&
		left.Generation == right.Generation &&
		left.RowCount == right.RowCount &&
		left.LowerBound == right.LowerBound &&
		left.UpperBound == right.UpperBound &&
		left.Reference == right.Reference
}
