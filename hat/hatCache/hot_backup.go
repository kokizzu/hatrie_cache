package hatCache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"hatrie_cache/hat/hatBackup"
	"hatrie_cache/internal/jsonwire"
)

// HotBackupResult is the published bundle plus the exact streamed snapshot
// identity captured before the bundle was packaged.
type HotBackupResult struct {
	Bundle   BackupBundleManifest
	Snapshot SnapshotManifest
}

// CreateHotBackupBundle creates a snapshot-mode bundle without holding the
// journal mutex while snapshot bytes are streamed. The returned snapshot and
// bundle share one exact journal boundary; the bundled journal is a checkpoint
// at that boundary and later mutations remain available from the live journal.
func CreateHotBackupBundle(path string, trie *HatTrie, journal *CommandJournal, options BackupBundleOptions) (HotBackupResult, error) {
	return CreateHotBackupBundleWithContext(context.Background(), path, trie, journal, options)
}

// CreateHotBackupBundleWithContext is the cancellable form of
// CreateHotBackupBundle. Publication is atomic, and cancellation before the
// final rename leaves the previous bundle untouched.
func CreateHotBackupBundleWithContext(ctx context.Context, path string, trie *HatTrie, journal *CommandJournal, options BackupBundleOptions) (HotBackupResult, error) {
	ctx = normalizeBackupContext(ctx)
	if err := checkBackupContext(ctx); err != nil {
		return HotBackupResult{}, err
	}
	cleanPath := filepath.Clean(path)
	if path == "" || cleanPath == "." {
		return HotBackupResult{}, errors.New("hatriecache: hot backup bundle path is required")
	}
	path = cleanPath
	if trie == nil {
		return HotBackupResult{}, ErrNilHatTrie
	}
	if journal == nil {
		return HotBackupResult{}, ErrNilCommandJournal
	}
	mode, err := ParseBackupMode(string(options.Mode))
	if err != nil {
		return HotBackupResult{}, err
	}
	if mode == BackupModeAuto {
		mode = BackupModeSnapshot
	}
	if mode != BackupModeSnapshot {
		return HotBackupResult{}, fmt.Errorf("hatriecache: hot backup supports snapshot mode only, got %q", mode)
	}
	if len(options.KeyPrefixes) > 0 || options.PartitionLocal {
		return HotBackupResult{}, errors.New("hatriecache: hot backup does not support filtered snapshots")
	}
	partition, err := normalizeBackupPartitionMetadata(options.Partition)
	if err != nil {
		return HotBackupResult{}, err
	}
	format, err := ParseSnapshotFormat(string(options.SnapshotFormat))
	if err != nil {
		return HotBackupResult{}, err
	}
	createdAt := options.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	createdAt = createdAt.UTC()

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return HotBackupResult{}, err
	}
	tmpDir, err := os.MkdirTemp(dir, filepath.Base(path)+".hot-work-*")
	if err != nil {
		return HotBackupResult{}, err
	}
	defer os.RemoveAll(tmpDir)
	if err := checkBackupContext(ctx); err != nil {
		return HotBackupResult{}, err
	}

	snapshotPath := filepath.Join(tmpDir, backupBundleSnapshotPath)
	var snapshotManifest SnapshotManifest
	var snapshotLease *CommandJournalBackupRetentionLease
	if err := writeFileAtomicStream(snapshotPath, func(writer io.Writer) error {
		var err error
		snapshotManifest, snapshotLease, err = journal.WriteSnapshotWithManifestAndBackupRetentionLease(trie, backupContextWriter{ctx: ctx, Writer: writer}, format)
		return err
	}); err != nil {
		if snapshotLease != nil {
			snapshotLease.Release()
		}
		return HotBackupResult{}, err
	}
	defer snapshotLease.Release()
	if err := checkBackupContext(ctx); err != nil {
		return HotBackupResult{}, err
	}

	journal.mu.Lock()
	if journal.closed {
		journal.mu.Unlock()
		return HotBackupResult{}, ErrCommandJournalClosed
	}
	journalFormat := journal.format
	journal.mu.Unlock()

	snapshotFile, err := backupBundleFileInfo(backupBundleSnapshotPath, snapshotPath)
	if err != nil {
		return HotBackupResult{}, err
	}
	if snapshotFile.Size != snapshotManifest.SizeBytes || snapshotFile.SHA256 != snapshotManifest.SHA256 {
		return HotBackupResult{}, fmt.Errorf("hatriecache: hot snapshot identity changed while packaging")
	}

	var journalBuffer bytes.Buffer
	if snapshotManifest.JournalSequence > 0 {
		if err := writeCommandJournalEntry(&journalBuffer, commandJournalEntry{
			Version:    commandJournalVersion,
			Sequence:   snapshotManifest.JournalSequence,
			Checkpoint: true,
		}, journalFormat); err != nil {
			return HotBackupResult{}, err
		}
	}
	journalData := journalBuffer.Bytes()
	manifest := BackupBundleManifest{
		Version:         BackupBundleVersion,
		CreatedAt:       createdAt,
		Mode:            BackupModeSnapshot,
		Snapshot:        backupBundleSnapshotPath,
		SnapshotFormat:  string(format),
		Journal:         backupBundleJournalPath,
		JournalFormat:   string(journalFormat),
		JournalSequence: snapshotManifest.JournalSequence,
		Partition:       cloneBackupPartitionMetadata(partition),
		Files: []BackupBundleFile{
			snapshotFile,
			backupBundleBytesInfo(backupBundleJournalPath, journalData),
		},
		RestoreHint: "extract snapshot.hc and commands.journal into DATA_DIR, then start with SNAPSHOT_PATH=DATA_DIR/snapshot.hc JOURNAL_PATH=DATA_DIR/commands.journal",
	}
	manifest.Consistency, err = hatBackup.BuildBundleConsistency(manifest)
	if err != nil {
		return HotBackupResult{}, err
	}
	manifestData, err := jsonwire.Marshal(manifest)
	if err != nil {
		return HotBackupResult{}, err
	}
	manifestData = append(manifestData, '\n')
	if err := checkBackupContext(ctx); err != nil {
		return HotBackupResult{}, err
	}
	payloads := []backupBundlePayloadFile{
		{name: backupBundleSnapshotPath, path: snapshotPath},
		{name: backupBundleJournalPath, data: journalData},
	}
	if err := writeFileAtomicStream(path, func(writer io.Writer) error {
		return writeBackupBundlePayloadTarGzip(backupContextWriter{ctx: ctx, Writer: writer}, payloads, manifestData, createdAt)
	}); err != nil {
		return HotBackupResult{}, err
	}
	return HotBackupResult{Bundle: manifest, Snapshot: snapshotManifest}, nil
}
