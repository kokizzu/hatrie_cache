package hatCache

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
)

var errSelectivePartitionRestoreJournalReplay = errors.New("hatriecache: selective partition restore requires a checkpoint-only journal")

func filterRestoredSnapshotByPartition(root string, manifest BackupBundleManifest, selector *BackupPartitionMetadata) error {
	if selector == nil || len(selector.KeyPrefixes) == 0 {
		return nil
	}
	if manifest.Snapshot == "" {
		return errors.New("hatriecache: selective partition restore requires a snapshot")
	}
	format, err := ParseSnapshotFormat(manifest.SnapshotFormat)
	if err != nil {
		return err
	}
	snapshotPath := filepath.Join(root, filepath.FromSlash(manifest.Snapshot))
	loaded := CreateHatTrie()
	defer loaded.Destroy()
	metadata, err := loaded.LoadSnapshotWithMetadata(snapshotPath)
	if err != nil {
		return err
	}
	return writeFileAtomicStream(snapshotPath, func(writer io.Writer) error {
		return loaded.writeSnapshotWithKeyFilter(writer, metadata.JournalSequence, format, func(key string) bool {
			return backupPartitionKeyCoveredByPrefix(key, selector.KeyPrefixes)
		})
	})
}

func validatePartitionRestoreJournal(root string, manifest BackupBundleManifest) error {
	if manifest.Journal == "" {
		return nil
	}
	if manifest.Journal != backupBundleJournalPath {
		return fmt.Errorf("hatriecache: selective partition restore does not support journal path %q", manifest.Journal)
	}
	if _, err := ParseCommandJournalFormat(manifest.JournalFormat); err != nil {
		return fmt.Errorf("hatriecache: selective partition restore journal format: %w", err)
	}
	journalPath := filepath.Join(root, filepath.FromSlash(manifest.Journal))
	entryCount := 0
	var firstEntry commandJournalEntry
	if _, err := scanCommandJournalEntries(journalPath, func(entry commandJournalEntry) error {
		if entryCount == 0 {
			firstEntry = entry
			entryCount = 1
			return nil
		}
		return errSelectivePartitionRestoreJournalReplay
	}); err != nil {
		if errors.Is(err, errSelectivePartitionRestoreJournalReplay) {
			return err
		}
		return fmt.Errorf("hatriecache: selective partition restore journal validation: %w", err)
	}
	if manifest.JournalSequence == 0 {
		if entryCount == 0 {
			return nil
		}
		return errSelectivePartitionRestoreJournalReplay
	}
	if entryCount != 1 || !firstEntry.Checkpoint || firstEntry.Sequence != manifest.JournalSequence {
		return errSelectivePartitionRestoreJournalReplay
	}
	return nil
}
