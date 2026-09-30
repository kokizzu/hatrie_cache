package hatCache

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

func filterRestoredPebbleStoreByPartition(root string, manifest BackupBundleManifest, selector *BackupPartitionMetadata) (int, error) {
	if selector == nil || len(selector.KeyPrefixes) == 0 {
		return 0, errors.New("hatriecache: selective Pebble restore requires key prefixes")
	}
	if manifest.Store != backupBundleStorePath || manifest.StorageBackend != string(StorageBackendPebble) {
		return 0, errors.New("hatriecache: selective partition restore requires a Pebble checkpoint")
	}
	format, err := ParseStorageFormat(manifest.StorageFormat)
	if err != nil {
		return 0, err
	}
	storePath := filepath.Join(root, filepath.FromSlash(manifest.Store))
	sourceStore, err := openPebbleStoreReadOnlyWithFormat(storePath, format)
	if err != nil {
		return 0, err
	}
	source := CreateHatTrie()
	defer source.Destroy()
	_, loadErr := sourceStore.Load(source)
	closeErr := sourceStore.Close()
	if loadErr != nil {
		return 0, loadErr
	}
	if closeErr != nil {
		return 0, closeErr
	}

	filterDir, err := os.MkdirTemp(root, ".selective-partition-*")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(filterDir)
	snapshotPath := filepath.Join(filterDir, backupBundleSnapshotPath)
	if err := writeFileAtomicStream(snapshotPath, func(writer io.Writer) error {
		return source.writeSnapshotWithKeyFilter(writer, manifest.JournalSequence, SnapshotFormatBinary, func(key string) bool {
			return backupPartitionKeyCoveredByPrefix(key, selector.KeyPrefixes)
		})
	}); err != nil {
		return 0, err
	}
	filtered := CreateHatTrie()
	defer filtered.Destroy()
	if _, err := filtered.LoadSnapshotWithMetadata(snapshotPath); err != nil {
		return 0, err
	}

	livePath, err := os.MkdirTemp(root, ".cache.leveldb.selective-live-*")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(livePath)
	checkpointPath, err := os.MkdirTemp(root, ".cache.leveldb.selective-checkpoint-*")
	if err != nil {
		return 0, err
	}
	if err := os.RemoveAll(checkpointPath); err != nil {
		return 0, err
	}
	defer os.RemoveAll(checkpointPath)

	filteredStore, err := OpenPebbleStoreWithFormat(livePath, format)
	if err != nil {
		return 0, err
	}
	if err := filteredStore.SaveCheckpointWithJournalSequence(filtered, checkpointPath, manifest.JournalSequence); err != nil {
		_ = filteredStore.Close()
		return 0, err
	}
	if err := filteredStore.Close(); err != nil {
		return 0, err
	}
	if err := validatePebbleCheckpointForAdoption(checkpointPath, format); err != nil {
		return 0, err
	}

	store, err := OpenPebbleStoreWithFormat(storePath, format)
	if err != nil {
		return 0, err
	}
	adoptErr := store.AdoptCheckpoint(checkpointPath)
	closeErr = store.Close()
	if adoptErr != nil {
		return 0, adoptErr
	}
	if closeErr != nil {
		return 0, closeErr
	}
	return filtered.Size(), nil
}
