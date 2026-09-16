package hatCache

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
)

func truncateRestoredJournalAtSequence(root string, manifest BackupBundleManifest, maxSequence, snapshotSequence uint64) (uint64, error) {
	if maxSequence == 0 {
		return manifest.JournalSequence, nil
	}
	if maxSequence < snapshotSequence {
		return 0, fmt.Errorf("hatriecache: point-in-time sequence %d is before snapshot checkpoint %d", maxSequence, snapshotSequence)
	}
	if manifest.Journal == "" {
		if maxSequence != snapshotSequence {
			return 0, fmt.Errorf("hatriecache: point-in-time sequence %d requires a journal tail after snapshot checkpoint %d", maxSequence, snapshotSequence)
		}
		return snapshotSequence, nil
	}
	if manifest.Journal != backupBundleJournalPath {
		return 0, fmt.Errorf("hatriecache: point-in-time restore does not support journal path %q", manifest.Journal)
	}
	if maxSequence >= manifest.JournalSequence {
		return manifest.JournalSequence, nil
	}
	format, err := ParseCommandJournalFormat(manifest.JournalFormat)
	if err != nil {
		return 0, fmt.Errorf("hatriecache: point-in-time restore journal format: %w", err)
	}
	journalPath := filepath.Join(root, filepath.FromSlash(manifest.Journal))
	reached := maxSequence == snapshotSequence
	if err := writeFileAtomicStream(journalPath, func(writer io.Writer) error {
		if _, err := scanCommandJournalEntries(journalPath, func(entry commandJournalEntry) error {
			if entry.Sequence > maxSequence {
				return nil
			}
			if entry.Sequence == maxSequence {
				reached = true
			}
			return writeCommandJournalEntry(writer, entry, format)
		}); err != nil {
			return err
		}
		if !reached {
			return fmt.Errorf("hatriecache: point-in-time sequence %d is not present in journal", maxSequence)
		}
		return nil
	}); err != nil {
		return 0, fmt.Errorf("hatriecache: truncate journal at sequence %d: %w", maxSequence, err)
	}
	return maxSequence, nil
}

func validatePointInTimeRestore(manifest BackupBundleManifest, maxSequence uint64) error {
	if maxSequence == 0 {
		return nil
	}
	if backupBundleManifestMode(manifest) != BackupModeSnapshot {
		return errors.New("hatriecache: point-in-time restore requires a snapshot backup")
	}
	if manifest.Snapshot == "" {
		return errors.New("hatriecache: point-in-time restore requires a snapshot")
	}
	if maxSequence > manifest.JournalSequence {
		return fmt.Errorf("hatriecache: point-in-time sequence %d is after backup sequence %d", maxSequence, manifest.JournalSequence)
	}
	return nil
}
