package hatCache

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"hatrie_cache/hat/hatBackup"
)

// ErrBackupReadOnlyAttachmentClosed reports use of an attachment after Close.
var ErrBackupReadOnlyAttachmentClosed = errors.New("hatriecache: read-only backup attachment is closed")

// BackupReadOnlyAttachmentOptions controls backup attachment materialization.
// BackupID selects one incremental repository snapshot; an empty value uses
// the repository's latest pointer. MaxPartConcurrency controls repository
// object materialization and zero uses the restore default.
type BackupReadOnlyAttachmentOptions struct {
	BackupID           string
	MaxPartConcurrency int
}

// BackupReadOnlyAttachment exposes an immutable backup snapshot to SQL.
// Callers must close it when finished so its private staging files are
// removed. The attachment never opens or mutates the live database path.
type BackupReadOnlyAttachment struct {
	mu       sync.RWMutex
	trie     *HatTrie
	store    *PebbleStore
	manifest BackupBundleManifest
	staging  string
	closed   bool
}

var _ SQLSourceResolver = (*BackupReadOnlyAttachment)(nil)

// OpenBackupReadOnlyAttachment opens a snapshot, Pebble checkpoint, or
// incremental backup repository for read-only SQL queries.
func OpenBackupReadOnlyAttachment(path string) (*BackupReadOnlyAttachment, error) {
	return OpenBackupReadOnlyAttachmentWithOptions(path, BackupReadOnlyAttachmentOptions{})
}

// OpenBackupReadOnlyAttachmentWithOptions is the configurable form of
// OpenBackupReadOnlyAttachment.
func OpenBackupReadOnlyAttachmentWithOptions(path string, options BackupReadOnlyAttachmentOptions) (*BackupReadOnlyAttachment, error) {
	path = strings.TrimSpace(path)
	options.BackupID = strings.TrimSpace(options.BackupID)
	if path == "" {
		return nil, errors.New("hatriecache: backup attachment path is required")
	}
	if err := (hatBackup.RestoreFileOptions{MaxConcurrency: options.MaxPartConcurrency}).Validate(); err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.IsDir() {
		if !fileExists(filepath.Join(path, backupRepositoryDescriptorPath)) {
			return nil, errors.New("hatriecache: backup attachment directory is not an incremental repository")
		}
		return openBackupReadOnlyRepository(path, options)
	}
	return openBackupReadOnlyBundle(path, options)
}

func openBackupReadOnlyBundle(path string, options BackupReadOnlyAttachmentOptions) (*BackupReadOnlyAttachment, error) {
	manifest, err := readBackupBundleManifest(path)
	if err != nil {
		return nil, err
	}
	mode := backupBundleManifestMode(manifest)
	if mode != BackupModeSnapshot && mode != BackupModePebbleCheckpoint {
		return nil, fmt.Errorf("hatriecache: unsupported backup attachment mode %q", mode)
	}
	staging, err := os.MkdirTemp("", "hatrie-cache-backup-attachment-*")
	if err != nil {
		return nil, err
	}
	removeStaging := true
	defer func() {
		if removeStaging {
			_ = os.RemoveAll(staging)
		}
	}()
	if err := extractBackupBundleFiles(path, staging, manifest.Files); err != nil {
		return nil, err
	}
	attachment, err := openBackupReadOnlyStaged(staging, manifest)
	if err != nil {
		return nil, err
	}
	removeStaging = false
	return attachment, nil
}

func openBackupReadOnlyRepository(path string, options BackupReadOnlyAttachmentOptions) (*BackupReadOnlyAttachment, error) {
	if err := verifyBackupRepositoryDescriptor(path); err != nil {
		return nil, err
	}
	manifest, err := readBackupRepositoryManifest(path, options.BackupID)
	if err != nil {
		return nil, err
	}
	staging, err := os.MkdirTemp("", "hatrie-cache-backup-attachment-*")
	if err != nil {
		return nil, err
	}
	removeStaging := true
	defer func() {
		if removeStaging {
			_ = os.RemoveAll(staging)
		}
	}()
	if _, err := materializeBackupRepositoryWithConcurrency(path, manifest.BackupID, staging, false, options.MaxPartConcurrency); err != nil {
		return nil, err
	}
	attachment, err := openBackupReadOnlyStaged(staging, manifest)
	if err != nil {
		return nil, err
	}
	removeStaging = false
	return attachment, nil
}

func openBackupReadOnlyStaged(staging string, manifest BackupBundleManifest) (*BackupReadOnlyAttachment, error) {
	attachment := &BackupReadOnlyAttachment{manifest: cloneBackupAttachmentManifest(manifest), staging: staging}
	mode := backupBundleManifestMode(manifest)
	switch mode {
	case BackupModeSnapshot:
		trie, err := loadBackupAttachmentSnapshot(staging, manifest)
		if err != nil {
			return nil, err
		}
		attachment.trie = trie
	case BackupModePebbleCheckpoint, BackupModePebbleIncremental:
		trie, store, err := loadBackupAttachmentStore(staging, manifest)
		if err != nil {
			return nil, err
		}
		attachment.trie = trie
		attachment.store = store
	default:
		return nil, fmt.Errorf("hatriecache: unsupported backup attachment mode %q", mode)
	}
	return attachment, nil
}

func loadBackupAttachmentSnapshot(root string, manifest BackupBundleManifest) (*HatTrie, error) {
	if manifest.Snapshot == "" {
		return nil, errors.New("hatriecache: backup attachment manifest missing snapshot")
	}
	trie := CreateHatTrie()
	metadata, err := trie.LoadSnapshotWithMetadata(filepath.Join(root, filepath.FromSlash(manifest.Snapshot)))
	if err != nil {
		trie.Destroy()
		return nil, err
	}
	if manifest.Consistency != nil && metadata.JournalSequence != manifest.Consistency.PartSequence {
		trie.Destroy()
		return nil, fmt.Errorf("hatriecache: snapshot journal sequence %d does not match backup consistency part boundary %d", metadata.JournalSequence, manifest.Consistency.PartSequence)
	}
	if _, err := validateBackupPartitionMetadataAgainstTrie(trie, manifest.Partition); err != nil {
		trie.Destroy()
		return nil, err
	}
	if manifest.Journal != "" {
		journalPath := filepath.Join(root, filepath.FromSlash(manifest.Journal))
		journalReport, entries, err := verifyJournalFileWithEntries(manifest.Journal, journalPath)
		if err != nil {
			trie.Destroy()
			return nil, err
		}
		if manifest.Consistency != nil && manifest.JournalSequence == manifest.Consistency.JournalSequence && (manifest.Consistency.Journal == nil || journalReport.LastSequence != manifest.Consistency.JournalSequence) {
			trie.Destroy()
			return nil, fmt.Errorf("hatriecache: journal last sequence %d does not match backup consistency boundary %d", journalReport.LastSequence, manifest.Consistency.JournalSequence)
		}
		journalFormat := DefaultCommandJournalFormat
		if manifest.JournalFormat != "" {
			journalFormat, err = ParseCommandJournalFormat(manifest.JournalFormat)
			if err != nil {
				trie.Destroy()
				return nil, err
			}
		}
		journal, err := OpenCommandJournalWithFormat(journalPath, journalFormat)
		if err != nil {
			trie.Destroy()
			return nil, err
		}
		_, replayErr := journal.Replay(trie, metadata.JournalSequence)
		closeErr := journal.Close()
		if replayErr != nil || closeErr != nil {
			trie.Destroy()
			return nil, errors.Join(replayErr, closeErr)
		}
		if _, err := validateBackupPartitionMetadataAgainstJournalEntries(nil, entries, manifest.Partition); err != nil {
			trie.Destroy()
			return nil, err
		}
		if _, err := validateBackupPartitionMetadataAgainstTrie(trie, manifest.Partition); err != nil {
			trie.Destroy()
			return nil, err
		}
	}
	return trie, nil
}

func loadBackupAttachmentStore(root string, manifest BackupBundleManifest) (*HatTrie, *PebbleStore, error) {
	if manifest.Store == "" || manifest.StorageBackend != string(StorageBackendPebble) {
		return nil, nil, errors.New("hatriecache: backup attachment requires a Pebble store payload")
	}
	format := DefaultStorageFormat
	if strings.TrimSpace(manifest.StorageFormat) != "" {
		parsed, err := ParseStorageFormat(manifest.StorageFormat)
		if err != nil {
			return nil, nil, err
		}
		format = parsed
	}
	store, err := openPebbleStoreReadOnlyWithFormat(filepath.Join(root, filepath.FromSlash(manifest.Store)), format)
	if err != nil {
		return nil, nil, err
	}
	trie := CreateHatTrie()
	if _, err := store.Load(trie); err != nil {
		trie.Destroy()
		_ = store.Close()
		return nil, nil, err
	}
	return trie, store, nil
}

// Resolver returns the read-only SQL resolver for this attachment.
func (attachment *BackupReadOnlyAttachment) Resolver() SQLSourceResolver {
	return attachment
}

// Manifest returns a detached copy of the backup manifest.
func (attachment *BackupReadOnlyAttachment) Manifest() BackupBundleManifest {
	if attachment == nil {
		return BackupBundleManifest{}
	}
	attachment.mu.RLock()
	defer attachment.mu.RUnlock()
	return cloneBackupAttachmentManifest(attachment.manifest)
}

// ResolveSQLSource implements SQLSourceResolver without exposing attachment
// storage or any mutation method on the detached trie.
func (attachment *BackupReadOnlyAttachment) ResolveSQLSource(name string, key string) ([]SQLRow, error) {
	if attachment == nil {
		return nil, ErrBackupReadOnlyAttachmentClosed
	}
	attachment.mu.RLock()
	defer attachment.mu.RUnlock()
	if attachment.closed || attachment.trie == nil {
		return nil, ErrBackupReadOnlyAttachmentClosed
	}
	return attachment.trie.ResolveSQLSource(name, key)
}

// Close releases the detached trie, any read-only store handle, and staging
// files. It is safe to call more than once.
func (attachment *BackupReadOnlyAttachment) Close() error {
	if attachment == nil {
		return nil
	}
	attachment.mu.Lock()
	defer attachment.mu.Unlock()
	if attachment.closed {
		return nil
	}
	attachment.closed = true
	var errs []error
	if attachment.store != nil {
		errs = append(errs, attachment.store.Close())
		attachment.store = nil
	}
	if attachment.trie != nil {
		attachment.trie.Destroy()
		attachment.trie = nil
	}
	if attachment.staging != "" {
		errs = append(errs, os.RemoveAll(attachment.staging))
		attachment.staging = ""
	}
	return errors.Join(errs...)
}

func cloneBackupAttachmentManifest(input BackupBundleManifest) BackupBundleManifest {
	output := input
	output.Files = append([]BackupBundleFile(nil), input.Files...)
	output.KeyPrefixes = append([]string(nil), input.KeyPrefixes...)
	output.NewObjectHashes = append([]string(nil), input.NewObjectHashes...)
	output.ReusedObjectHashes = append([]string(nil), input.ReusedObjectHashes...)
	output.Partition = cloneBackupPartitionMetadata(input.Partition)
	if input.Consistency != nil {
		consistency := *input.Consistency
		consistency.Parts = append([]hatBackup.BundlePart(nil), input.Consistency.Parts...)
		if input.Consistency.Journal != nil {
			journal := *input.Consistency.Journal
			consistency.Journal = &journal
		}
		output.Consistency = &consistency
	}
	return output
}
