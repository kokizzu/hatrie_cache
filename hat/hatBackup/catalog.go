package hatBackup

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const backupManifestCatalogLogHeader = "hatrie-backup-catalog-v2\n"
const backupManifestCatalogLogHeaderV1 = "hatrie-backup-catalog-v1\n"
const backupManifestCatalogVersion = 1
const backupManifestCatalogChecksumLength = sha256.Size * 2

type backupManifestCatalogFormat uint8

const (
	backupManifestCatalogLog backupManifestCatalogFormat = iota
	backupManifestCatalogLegacyLog
	backupManifestCatalogLegacyJSON
)

// BackupManifestCatalog is a durable, atomically published catalog of
// incremental backup manifests. It gives operators one recoverable source of
// truth for chain planning instead of requiring an external manifest slice.
// One catalog instance serializes its own writers; callers sharing a path
// across processes should still provide process-level ownership.
type BackupManifestCatalog struct {
	mu        sync.Mutex
	path      string
	loaded    bool
	format    backupManifestCatalogFormat
	manifests []BundleManifest
	ids       map[string]struct{}
}

type backupManifestCatalogEnvelope struct {
	Version   int              `json:"version"`
	Manifests []BundleManifest `json:"manifests"`
}

// NewBackupManifestCatalog validates a catalog file path. Parent directories
// are created by the first Replace or Append operation.
func NewBackupManifestCatalog(path string) (*BackupManifestCatalog, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("hatriecache: backup manifest catalog path is required")
	}
	path = filepath.Clean(path)
	if path == "." || path == string(filepath.Separator) {
		return nil, errors.New("hatriecache: backup manifest catalog path must be a file")
	}
	return &BackupManifestCatalog{path: path}, nil
}

// Load returns independent manifest copies in catalog order. A missing catalog
// is treated as an empty catalog, which makes first-start initialization safe.
func (catalog *BackupManifestCatalog) Load() ([]BundleManifest, error) {
	if catalog == nil {
		return nil, errors.New("hatriecache: backup manifest catalog is nil")
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	return catalog.refreshLocked()
}

// Append validates and durably adds one manifest. Incremental manifests must
// reference a parent already present in the catalog.
func (catalog *BackupManifestCatalog) Append(manifest BundleManifest) error {
	if catalog == nil {
		return errors.New("hatriecache: backup manifest catalog is nil")
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	if !catalog.loaded {
		if _, err := catalog.refreshLocked(); err != nil {
			return err
		}
	}
	if err := validateBackupChainManifest(manifest); err != nil {
		return err
	}
	if _, exists := catalog.ids[manifest.BackupID]; exists {
		return fmt.Errorf("hatriecache: duplicate backup id %q", manifest.BackupID)
	}
	if manifest.ParentBackupID != "" {
		if _, exists := catalog.ids[manifest.ParentBackupID]; !exists {
			return fmt.Errorf("hatriecache: backup %q parent %q is not in the catalog", manifest.BackupID, manifest.ParentBackupID)
		}
	}
	manifest = cloneBackupManifest(manifest)
	if catalog.format != backupManifestCatalogLog {
		candidate := append(cloneBackupManifests(catalog.manifests), manifest)
		if err := catalog.writeLocked(candidate); err != nil {
			return err
		}
	} else if err := catalog.appendLogRecordLocked(manifest); err != nil {
		return err
	}
	catalog.manifests = append(catalog.manifests, manifest)
	catalog.ids[manifest.BackupID] = struct{}{}
	catalog.format = backupManifestCatalogLog
	return nil
}

// Replace validates and atomically publishes the complete manifest catalog.
// An empty replacement is valid and removes all chain metadata without
// touching backup payload objects.
func (catalog *BackupManifestCatalog) Replace(manifests []BundleManifest) error {
	if catalog == nil {
		return errors.New("hatriecache: backup manifest catalog is nil")
	}
	catalog.mu.Lock()
	defer catalog.mu.Unlock()
	cloned := cloneBackupManifests(manifests)
	if err := validateBackupManifestCatalog(cloned); err != nil {
		return err
	}
	if err := catalog.writeLocked(cloned); err != nil {
		return err
	}
	catalog.setLoadedLocked(cloned, backupManifestCatalogLog)
	return nil
}

// Plan validates and resolves one persisted chain ending at latestID.
func (catalog *BackupManifestCatalog) Plan(latestID string) (BackupChainPlan, error) {
	if catalog == nil {
		return BackupChainPlan{}, errors.New("hatriecache: backup manifest catalog is nil")
	}
	manifests, err := catalog.Load()
	if err != nil {
		return BackupChainPlan{}, err
	}
	return PlanBackupChain(manifests, latestID)
}

func (catalog *BackupManifestCatalog) refreshLocked() ([]BundleManifest, error) {
	manifests, format, err := readBackupManifestCatalog(catalog.path)
	if err != nil {
		return nil, err
	}
	catalog.setLoadedLocked(manifests, format)
	return cloneBackupManifests(manifests), nil
}

func (catalog *BackupManifestCatalog) setLoadedLocked(manifests []BundleManifest, format backupManifestCatalogFormat) {
	catalog.manifests = cloneBackupManifests(manifests)
	catalog.ids = make(map[string]struct{}, len(manifests))
	for _, manifest := range manifests {
		catalog.ids[manifest.BackupID] = struct{}{}
	}
	catalog.format = format
	catalog.loaded = true
}

func readBackupManifestCatalog(path string) ([]BundleManifest, backupManifestCatalogFormat, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, backupManifestCatalogLog, nil
	}
	if err != nil {
		return nil, backupManifestCatalogLog, fmt.Errorf("hatriecache: read backup manifest catalog: %w", err)
	}
	if bytes.HasPrefix(data, []byte(backupManifestCatalogLogHeader)) {
		manifests, err := parseBackupManifestCatalogLog(data, true)
		return manifests, backupManifestCatalogLog, err
	}
	if bytes.HasPrefix(data, []byte(backupManifestCatalogLogHeaderV1)) {
		manifests, err := parseBackupManifestCatalogLog(data, false)
		return manifests, backupManifestCatalogLegacyLog, err
	}
	var envelope backupManifestCatalogEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, backupManifestCatalogLegacyJSON, fmt.Errorf("hatriecache: decode backup manifest catalog: %w", err)
	}
	if envelope.Version != backupManifestCatalogVersion {
		return nil, backupManifestCatalogLegacyJSON, fmt.Errorf("hatriecache: unsupported backup manifest catalog version %d", envelope.Version)
	}
	if err := validateBackupManifestCatalog(envelope.Manifests); err != nil {
		return nil, backupManifestCatalogLegacyJSON, err
	}
	return cloneBackupManifests(envelope.Manifests), backupManifestCatalogLegacyJSON, nil
}

func parseBackupManifestCatalogLog(data []byte, checksummed bool) ([]BundleManifest, error) {
	headerLength := len(backupManifestCatalogLogHeader)
	if !checksummed {
		headerLength = len(backupManifestCatalogLogHeaderV1)
	}
	payload := data[headerLength:]
	if len(payload) == 0 {
		return nil, nil
	}
	if payload[len(payload)-1] != '\n' {
		return nil, errors.New("hatriecache: backup manifest catalog has an incomplete final record")
	}
	lines := bytes.Split(payload[:len(payload)-1], []byte{'\n'})
	manifests := make([]BundleManifest, 0, len(lines))
	for index, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			return nil, fmt.Errorf("hatriecache: backup manifest catalog has an empty record at line %d", index+1)
		}
		if checksummed {
			if len(line) < backupManifestCatalogChecksumLength+1 || line[backupManifestCatalogChecksumLength] != ' ' {
				return nil, fmt.Errorf("hatriecache: backup manifest catalog record %d has an invalid checksum frame", index+1)
			}
			var expected [sha256.Size]byte
			if _, err := hex.Decode(expected[:], line[:backupManifestCatalogChecksumLength]); err != nil {
				return nil, fmt.Errorf("hatriecache: backup manifest catalog record %d has an invalid checksum: %w", index+1, err)
			}
			payload := line[backupManifestCatalogChecksumLength+1:]
			actual := sha256.Sum256(payload)
			if subtle.ConstantTimeCompare(expected[:], actual[:]) != 1 {
				return nil, fmt.Errorf("hatriecache: backup manifest catalog record %d checksum mismatch", index+1)
			}
			line = payload
		}
		var manifest BundleManifest
		if err := json.Unmarshal(line, &manifest); err != nil {
			return nil, fmt.Errorf("hatriecache: decode backup manifest catalog record %d: %w", index+1, err)
		}
		manifests = append(manifests, manifest)
	}
	if err := validateBackupManifestCatalog(manifests); err != nil {
		return nil, err
	}
	return manifests, nil
}

func validateBackupManifestCatalog(manifests []BundleManifest) error {
	ids := make(map[string]struct{}, len(manifests))
	byID := make(map[string]BundleManifest, len(manifests))
	for _, manifest := range manifests {
		if err := validateBackupChainManifest(manifest); err != nil {
			return err
		}
		if _, exists := ids[manifest.BackupID]; exists {
			return fmt.Errorf("hatriecache: duplicate backup id %q", manifest.BackupID)
		}
		ids[manifest.BackupID] = struct{}{}
		byID[manifest.BackupID] = manifest
	}
	for _, manifest := range manifests {
		if manifest.ParentBackupID == "" {
			continue
		}
		if _, exists := ids[manifest.ParentBackupID]; !exists {
			return fmt.Errorf("hatriecache: backup %q parent %q is not in the catalog", manifest.BackupID, manifest.ParentBackupID)
		}
	}
	for backupID := range ids {
		seen := make(map[string]struct{})
		currentID := backupID
		for currentID != "" {
			if _, exists := seen[currentID]; exists {
				return fmt.Errorf("hatriecache: backup manifest catalog contains a parent cycle at %q", currentID)
			}
			seen[currentID] = struct{}{}
			currentID = byID[currentID].ParentBackupID
		}
	}
	return nil
}

func (catalog *BackupManifestCatalog) writeLocked(manifests []BundleManifest) error {
	if err := rejectBackupManifestCatalogPath(catalog.path); err != nil {
		return err
	}
	data, err := encodeBackupManifestCatalogLog(manifests)
	if err != nil {
		return err
	}
	return atomicWriteBackupManifestCatalog(catalog.path, data)
}

func (catalog *BackupManifestCatalog) appendLogRecordLocked(manifest BundleManifest) error {
	if err := rejectBackupManifestCatalogPath(catalog.path); err != nil {
		return err
	}
	directory := filepath.Dir(catalog.path)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return fmt.Errorf("hatriecache: create backup manifest catalog directory: %w", err)
	}
	record, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("hatriecache: encode backup manifest catalog record: %w", err)
	}
	info, statErr := os.Lstat(catalog.path)
	newFile := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !newFile {
		return fmt.Errorf("hatriecache: inspect backup manifest catalog path: %w", statErr)
	}
	if !newFile && !info.Mode().IsRegular() {
		return errors.New("hatriecache: backup manifest catalog path is not a regular file")
	}
	file, err := os.OpenFile(catalog.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("hatriecache: open backup manifest catalog: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return fmt.Errorf("hatriecache: secure backup manifest catalog: %w", err)
	}
	if newFile {
		if _, err := io.WriteString(file, backupManifestCatalogLogHeader); err != nil {
			_ = file.Close()
			return fmt.Errorf("hatriecache: write backup manifest catalog header: %w", err)
		}
	} else if err := verifyBackupManifestCatalogLogHeader(file); err != nil {
		_ = file.Close()
		return err
	}
	var checksum [backupManifestCatalogChecksumLength]byte
	digest := sha256.Sum256(record)
	hex.Encode(checksum[:], digest[:])
	if _, err := file.Write(checksum[:]); err != nil {
		_ = file.Close()
		return fmt.Errorf("hatriecache: append backup manifest catalog checksum: %w", err)
	}
	if _, err := file.Write([]byte{' '}); err != nil {
		_ = file.Close()
		return fmt.Errorf("hatriecache: append backup manifest catalog checksum separator: %w", err)
	}
	if _, err := file.Write(record); err != nil {
		_ = file.Close()
		return fmt.Errorf("hatriecache: append backup manifest catalog record: %w", err)
	}
	if _, err := file.Write([]byte{'\n'}); err != nil {
		_ = file.Close()
		return fmt.Errorf("hatriecache: append backup manifest catalog record terminator: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("hatriecache: sync backup manifest catalog: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("hatriecache: close backup manifest catalog: %w", err)
	}
	if newFile {
		if err := syncBackupManifestCatalogDirectory(directory); err != nil {
			return err
		}
	}
	return nil
}

func verifyBackupManifestCatalogLogHeader(file *os.File) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("hatriecache: seek backup manifest catalog: %w", err)
	}
	header := make([]byte, len(backupManifestCatalogLogHeader))
	if _, err := io.ReadFull(file, header); err != nil {
		return fmt.Errorf("hatriecache: read backup manifest catalog header: %w", err)
	}
	if string(header) != backupManifestCatalogLogHeader {
		return errors.New("hatriecache: backup manifest catalog has an invalid log header")
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("hatriecache: seek backup manifest catalog end: %w", err)
	}
	return nil
}

func encodeBackupManifestCatalogLog(manifests []BundleManifest) ([]byte, error) {
	var data bytes.Buffer
	data.WriteString(backupManifestCatalogLogHeader)
	for index, manifest := range manifests {
		record, err := json.Marshal(manifest)
		if err != nil {
			return nil, fmt.Errorf("hatriecache: encode backup manifest catalog record %d: %w", index+1, err)
		}
		var checksum [backupManifestCatalogChecksumLength]byte
		digest := sha256.Sum256(record)
		hex.Encode(checksum[:], digest[:])
		data.Write(checksum[:])
		data.WriteByte(' ')
		data.Write(record)
		data.WriteByte('\n')
	}
	return data.Bytes(), nil
}

func atomicWriteBackupManifestCatalog(path string, data []byte) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return fmt.Errorf("hatriecache: create backup manifest catalog directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".hatrie-backup-catalog-*.tmp")
	if err != nil {
		return fmt.Errorf("hatriecache: create backup manifest catalog staging file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("hatriecache: secure backup manifest catalog staging file: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("hatriecache: write backup manifest catalog staging file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("hatriecache: sync backup manifest catalog staging file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("hatriecache: close backup manifest catalog staging file: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("hatriecache: publish backup manifest catalog: %w", err)
	}
	return syncBackupManifestCatalogDirectory(directory)
}

func rejectBackupManifestCatalogPath(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("hatriecache: inspect backup manifest catalog path: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return errors.New("hatriecache: backup manifest catalog path is a symlink")
	}
	if !info.Mode().IsRegular() {
		return errors.New("hatriecache: backup manifest catalog path is not a regular file")
	}
	return nil
}

func syncBackupManifestCatalogDirectory(directory string) error {
	handle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("hatriecache: open backup manifest catalog directory: %w", err)
	}
	defer handle.Close()
	if err := handle.Sync(); err != nil {
		return fmt.Errorf("hatriecache: sync backup manifest catalog directory: %w", err)
	}
	return nil
}

func cloneBackupManifests(input []BundleManifest) []BundleManifest {
	if input == nil {
		return nil
	}
	cloned := make([]BundleManifest, len(input))
	for index, manifest := range input {
		cloned[index] = cloneBackupManifest(manifest)
	}
	return cloned
}
