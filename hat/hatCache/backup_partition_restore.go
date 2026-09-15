package hatCache

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

func filterRestoredSnapshotByPartition(root string, manifest BackupBundleManifest, selector *BackupPartitionMetadata) (uint64, error) {
	if selector == nil || len(selector.KeyPrefixes) == 0 {
		return manifest.JournalSequence, nil
	}
	if manifest.Snapshot == "" {
		return 0, errors.New("hatriecache: selective partition restore requires a snapshot")
	}
	format, err := ParseSnapshotFormat(manifest.SnapshotFormat)
	if err != nil {
		return 0, err
	}
	snapshotPath := filepath.Join(root, filepath.FromSlash(manifest.Snapshot))
	loaded := CreateHatTrie()
	defer loaded.Destroy()
	metadata, err := loaded.LoadSnapshotWithMetadata(snapshotPath)
	if err != nil {
		return 0, err
	}
	journalSequence, err := filterPartitionJournalTail(root, manifest, selector, loaded, metadata.JournalSequence)
	if err != nil {
		return 0, err
	}
	if err := writeFileAtomicStream(snapshotPath, func(writer io.Writer) error {
		return loaded.writeSnapshotWithKeyFilter(writer, journalSequence, format, func(key string) bool {
			return backupPartitionKeyCoveredByPrefix(key, selector.KeyPrefixes)
		})
	}); err != nil {
		return 0, err
	}
	return journalSequence, nil
}

func filterPartitionJournalTail(root string, manifest BackupBundleManifest, selector *BackupPartitionMetadata, trie *HatTrie, snapshotSequence uint64) (uint64, error) {
	if manifest.Journal == "" {
		return snapshotSequence, nil
	}
	if manifest.Journal != backupBundleJournalPath {
		return 0, fmt.Errorf("hatriecache: selective partition restore does not support journal path %q", manifest.Journal)
	}
	format, err := ParseCommandJournalFormat(manifest.JournalFormat)
	if err != nil {
		return 0, fmt.Errorf("hatriecache: selective partition restore journal format: %w", err)
	}
	journalPath := filepath.Join(root, filepath.FromSlash(manifest.Journal))
	finalSequence := snapshotSequence
	if _, err := scanCommandJournalEntries(journalPath, func(entry commandJournalEntry) error {
		if entry.Sequence > finalSequence {
			finalSequence = entry.Sequence
		}
		if entry.Checkpoint {
			if entry.Sequence > snapshotSequence {
				return fmt.Errorf("hatriecache: selective partition restore journal checkpoint %d is newer than snapshot sequence %d", entry.Sequence, snapshotSequence)
			}
			return nil
		}
		if entry.Sequence <= snapshotSequence {
			return nil
		}
		key, err := selectivePartitionJournalRequestKey(entry)
		if err != nil {
			return err
		}
		if !backupPartitionKeyCoveredByPrefix(key, selector.KeyPrefixes) {
			return nil
		}
		if err := executeCommandForReplay(trie, entry.Request); err != nil {
			return fmt.Errorf("hatriecache: selective partition restore journal entry %d failed: %w", entry.Sequence, err)
		}
		return nil
	}); err != nil {
		return 0, err
	}
	if err := writeCommandJournalCheckpointWithFormat(journalPath, finalSequence, format); err != nil {
		return 0, fmt.Errorf("hatriecache: selective partition restore journal checkpoint: %w", err)
	}
	return finalSequence, nil
}

func selectivePartitionJournalRequestKey(entry commandJournalEntry) (string, error) {
	if entry.Outbox != nil {
		return "", errors.New("hatriecache: selective partition restore does not support journal outbox entries")
	}
	if strings.TrimSpace(entry.Request.IdempotencyKey) != "" {
		return "", errors.New("hatriecache: selective partition restore does not support idempotent journal replay")
	}
	request := entry.Request
	command := strings.ToUpper(strings.TrimSpace(request.Command))
	switch command {
	case "SET", "SETSTR", "SETX", "SETSTRX", "SETINT", "SETINTX", "INC", "DEL", "EXPIRE", "EXPIREAT":
	default:
		return "", fmt.Errorf("hatriecache: selective partition restore cannot safely filter journal command %q", command)
	}
	if strings.TrimSpace(request.Key) == "" || len(request.Values) != 0 || len(request.Pairs) != 0 || len(request.Batch) != 0 || request.Subkey != "" || request.Priority != nil {
		return "", fmt.Errorf("hatriecache: selective partition restore cannot safely filter journal command %q payload", command)
	}
	if !commandShouldJournal(request) {
		return "", fmt.Errorf("hatriecache: selective partition restore cannot safely filter invalid journal command %q", command)
	}
	return strings.TrimSpace(request.Key), nil
}
