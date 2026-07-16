package hatriecache

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	json "github.com/goccy/go-json"
)

type BackupBundleRestoreOptions struct {
	Overwrite bool
}

type BackupBundleRestoreReport struct {
	OK              bool   `json:"ok"`
	Bundle          string `json:"bundle"`
	DataDir         string `json:"data_dir"`
	Snapshot        string `json:"snapshot"`
	Journal         string `json:"journal,omitempty"`
	JournalSequence uint64 `json:"journal_sequence"`
	RecoveredKeys   int    `json:"recovered_keys"`
}

func RestoreBackupBundle(bundlePath string, dataDir string, options BackupBundleRestoreOptions) (BackupBundleRestoreReport, error) {
	bundlePath = strings.TrimSpace(bundlePath)
	dataDir = strings.TrimSpace(dataDir)
	if bundlePath == "" {
		return BackupBundleRestoreReport{}, errors.New("hatriecache: backup bundle path is required")
	}
	if dataDir == "" {
		return BackupBundleRestoreReport{}, errors.New("hatriecache: restore data dir is required")
	}
	files, err := readBackupBundle(bundlePath)
	if err != nil {
		return BackupBundleRestoreReport{}, err
	}
	manifestData, ok := files[backupBundleManifestPath]
	if !ok {
		return BackupBundleRestoreReport{}, errors.New("hatriecache: backup bundle missing manifest.json")
	}
	var manifest BackupBundleManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return BackupBundleRestoreReport{}, err
	}
	if manifest.Version != BackupBundleVersion {
		return BackupBundleRestoreReport{}, fmt.Errorf("hatriecache: unsupported backup bundle version %d", manifest.Version)
	}
	for _, file := range manifest.Files {
		data, ok := files[file.Path]
		if !ok {
			return BackupBundleRestoreReport{}, fmt.Errorf("hatriecache: backup bundle missing %s", file.Path)
		}
		if err := verifyBackupFileChecksum(file, data); err != nil {
			return BackupBundleRestoreReport{}, err
		}
	}
	doctor, err := VerifyBackupBundle(bundlePath)
	if err != nil {
		return BackupBundleRestoreReport{}, err
	}
	if err := ensureRestoreDataDir(dataDir, options.Overwrite); err != nil {
		return BackupBundleRestoreReport{}, err
	}
	snapshotData, ok := files[manifest.Snapshot]
	if !ok {
		return BackupBundleRestoreReport{}, fmt.Errorf("hatriecache: backup bundle missing %s", manifest.Snapshot)
	}
	snapshotPath := filepath.Join(dataDir, backupBundleSnapshotPath)
	if err := os.WriteFile(snapshotPath, snapshotData, 0o600); err != nil {
		return BackupBundleRestoreReport{}, err
	}
	journalPath := ""
	if manifest.Journal != "" {
		journalData, ok := files[manifest.Journal]
		if !ok {
			return BackupBundleRestoreReport{}, fmt.Errorf("hatriecache: backup bundle missing %s", manifest.Journal)
		}
		journalPath = filepath.Join(dataDir, backupBundleJournalPath)
		if err := os.WriteFile(journalPath, journalData, 0o600); err != nil {
			return BackupBundleRestoreReport{}, err
		}
	}
	return BackupBundleRestoreReport{
		OK:              true,
		Bundle:          bundlePath,
		DataDir:         dataDir,
		Snapshot:        snapshotPath,
		Journal:         journalPath,
		JournalSequence: manifest.JournalSequence,
		RecoveredKeys:   doctor.RecoveredKeys,
	}, nil
}

func ensureRestoreDataDir(path string, overwrite bool) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	if len(entries) > 0 && !overwrite {
		return fmt.Errorf("hatriecache: restore data dir is not empty: %s", path)
	}
	return nil
}
