package hatCache

import (
	"errors"
	"io"
	"path/filepath"
)

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
