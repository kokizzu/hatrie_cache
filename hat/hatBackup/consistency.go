package hatBackup

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
)

// BuildBundleConsistency derives the deterministic part/WAL binding for a
// manifest whose immutable parts were captured at JournalSequence. The
// manifest's existing Files field remains the source of truth for payload
// extraction; this contract makes its relationship to the mutation boundary
// explicit.
func BuildBundleConsistency(manifest BundleManifest) (*BundleConsistency, error) {
	return BuildBundleConsistencyAtPartSequence(manifest, manifest.JournalSequence)
}

// BuildBundleConsistencyAtPartSequence derives a binding for a manifest whose
// immutable parts were captured at partSequence and whose journal may extend
// through JournalSequence. It is used for point-in-time or partition-filtered
// derived backups where those boundaries intentionally differ.
func BuildBundleConsistencyAtPartSequence(manifest BundleManifest, partSequence uint64) (*BundleConsistency, error) {
	return buildBundleConsistency(manifest, partSequence)
}

func buildBundleConsistency(manifest BundleManifest, partSequence uint64) (*BundleConsistency, error) {
	if partSequence > manifest.JournalSequence {
		return nil, errors.New("hatriecache: backup consistency part sequence is after journal sequence")
	}
	if err := validateBundleConsistencyFiles(manifest.Files); err != nil {
		return nil, err
	}
	parts := make([]BundlePart, 0, len(manifest.Files))
	var journalFile BundleFile
	journalFound := false
	for _, file := range manifest.Files {
		if manifest.Journal != "" && file.Path == manifest.Journal {
			journalFile = file
			journalFound = true
			continue
		}
		parts = append(parts, BundlePart{
			Path:   file.Path,
			Kind:   bundlePartKind(manifest, file.Path),
			Size:   file.Size,
			SHA256: file.SHA256,
		})
	}
	if len(parts) == 0 {
		return nil, errors.New("hatriecache: backup consistency requires at least one immutable part")
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Path < parts[j].Path })

	if manifest.Snapshot != "" && !bundleConsistencyHasPart(parts, manifest.Snapshot) {
		return nil, fmt.Errorf("hatriecache: backup consistency is missing snapshot part %s", manifest.Snapshot)
	}
	if manifest.Store != "" && !bundleConsistencyHasStorePart(parts, manifest.Store) {
		return nil, fmt.Errorf("hatriecache: backup consistency is missing storage part under %s", manifest.Store)
	}

	consistency := &BundleConsistency{
		Version:         BundleConsistencyVersion,
		PartSequence:    partSequence,
		JournalSequence: manifest.JournalSequence,
		Parts:           parts,
	}
	if manifest.Journal != "" {
		if !journalFound {
			return nil, fmt.Errorf("hatriecache: backup consistency is missing journal file %s", manifest.Journal)
		}
		consistency.Journal = &BundleJournalBoundary{
			Path:     journalFile.Path,
			Format:   manifest.JournalFormat,
			Sequence: manifest.JournalSequence,
			Size:     journalFile.Size,
			SHA256:   journalFile.SHA256,
		}
	} else if manifest.JournalFormat != "" {
		return nil, errors.New("hatriecache: backup consistency has journal format without journal file")
	}
	consistency.Digest = bundleConsistencyDigest(*consistency)
	return consistency, nil
}

// ValidateBundleConsistency validates a manifest's explicit part/WAL binding.
// A nil consistency block is accepted for compatibility with older backups.
func ValidateBundleConsistency(manifest BundleManifest) error {
	if manifest.Consistency == nil {
		return nil
	}
	consistency := manifest.Consistency
	if consistency.Version != BundleConsistencyVersion {
		return fmt.Errorf("hatriecache: unsupported backup consistency version %d", consistency.Version)
	}
	if consistency.PartSequence > consistency.JournalSequence || consistency.JournalSequence != manifest.JournalSequence {
		return errors.New("hatriecache: backup consistency sequence mismatch")
	}
	if err := validateBundleConsistencyFiles(manifest.Files); err != nil {
		return err
	}
	if len(consistency.Parts) == 0 {
		return errors.New("hatriecache: backup consistency requires at least one immutable part")
	}
	for index, part := range consistency.Parts {
		if err := validateBundleConsistencyPath(part.Path); err != nil {
			return err
		}
		if index > 0 && consistency.Parts[index-1].Path >= part.Path {
			return errors.New("hatriecache: backup consistency parts are not sorted uniquely")
		}
		if part.Size < 0 {
			return fmt.Errorf("hatriecache: negative backup part size for %s", part.Path)
		}
		if err := validateBundleConsistencyHash(part.SHA256); err != nil {
			return fmt.Errorf("hatriecache: backup part %s: %w", part.Path, err)
		}
		file, ok := bundleConsistencyFileByPath(manifest.Files, part.Path, manifest.Journal)
		if !ok {
			return fmt.Errorf("hatriecache: backup consistency part %s is not declared", part.Path)
		}
		if bundlePartKind(manifest, part.Path) != part.Kind || file.Size != part.Size || file.SHA256 != part.SHA256 {
			return fmt.Errorf("hatriecache: backup consistency part %s does not match its file declaration", part.Path)
		}
	}
	expectedParts := 0
	for _, file := range manifest.Files {
		if manifest.Journal != "" && file.Path == manifest.Journal {
			continue
		}
		expectedParts++
		if !bundleConsistencyHasPart(consistency.Parts, file.Path) {
			return fmt.Errorf("hatriecache: backup consistency is missing part %s", file.Path)
		}
	}
	if len(consistency.Parts) != expectedParts {
		return errors.New("hatriecache: backup consistency part count mismatch")
	}
	if manifest.Journal == "" {
		if consistency.Journal != nil {
			return errors.New("hatriecache: backup consistency declares a journal without a manifest journal")
		}
	} else {
		if consistency.Journal == nil {
			return errors.New("hatriecache: backup consistency is missing the journal boundary")
		}
		journalFile, ok := bundleConsistencyFileByPath(manifest.Files, manifest.Journal, "")
		if !ok {
			return fmt.Errorf("hatriecache: backup consistency journal %s is not declared", manifest.Journal)
		}
		boundary := consistency.Journal
		if boundary.Path != manifest.Journal || boundary.Format != manifest.JournalFormat || boundary.Sequence != manifest.JournalSequence || boundary.Size != journalFile.Size || boundary.SHA256 != journalFile.SHA256 {
			return errors.New("hatriecache: backup consistency journal boundary mismatch")
		}
	}
	if consistency.Digest != bundleConsistencyDigest(*consistency) {
		return errors.New("hatriecache: backup consistency digest mismatch")
	}
	return nil
}

func validateBundleConsistencyFiles(files []BundleFile) error {
	for index, file := range files {
		if err := validateBundleConsistencyPath(file.Path); err != nil {
			return err
		}
		for _, previous := range files[:index] {
			if previous.Path == file.Path {
				return fmt.Errorf("hatriecache: duplicate backup file declaration %s", file.Path)
			}
		}
		if file.Size < 0 {
			return fmt.Errorf("hatriecache: negative backup file size for %s", file.Path)
		}
		if err := validateBundleConsistencyHash(file.SHA256); err != nil {
			return fmt.Errorf("hatriecache: backup file %s: %w", file.Path, err)
		}
	}
	return nil
}

func bundleConsistencyFileByPath(files []BundleFile, target string, excluded string) (BundleFile, bool) {
	for _, file := range files {
		if file.Path == target && file.Path != excluded {
			return file, true
		}
	}
	return BundleFile{}, false
}

func validateBundleConsistencyPath(value string) error {
	if value == "" {
		return errors.New("hatriecache: backup consistency contains an empty file path")
	}
	if path.IsAbs(value) || path.Clean(value) != value || value == "." || strings.HasPrefix(value, "../") {
		return fmt.Errorf("hatriecache: unsafe backup file path %q", value)
	}
	if value == "manifest.json" {
		return errors.New("hatriecache: backup consistency cannot cover manifest.json")
	}
	return nil
}

func validateBundleConsistencyHash(value string) error {
	if len(value) != sha256.Size*2 {
		return errors.New("backup file checksum must be a SHA-256 hex digest")
	}
	if _, err := hex.DecodeString(value); err != nil || strings.ToLower(value) != value {
		return errors.New("backup file checksum must be lowercase SHA-256 hex")
	}
	return nil
}

func bundlePartKind(manifest BundleManifest, filePath string) string {
	if manifest.Snapshot != "" && filePath == manifest.Snapshot {
		return BundlePartKindSnapshot
	}
	if manifest.Store != "" && (filePath == manifest.Store || strings.HasPrefix(filePath, manifest.Store+"/")) {
		return BundlePartKindStorage
	}
	if strings.HasSuffix(filePath, ".backend") {
		return BundlePartKindMetadata
	}
	return BundlePartKindPayload
}

func bundleConsistencyHasPart(parts []BundlePart, target string) bool {
	for _, part := range parts {
		if part.Path == target {
			return true
		}
	}
	return false
}

func bundleConsistencyHasStorePart(parts []BundlePart, store string) bool {
	prefix := store + "/"
	for _, part := range parts {
		if part.Path == store || strings.HasPrefix(part.Path, prefix) {
			return true
		}
	}
	return false
}

func bundleConsistencyDigest(consistency BundleConsistency) string {
	consistency.Digest = ""
	data, _ := json.Marshal(consistency)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
