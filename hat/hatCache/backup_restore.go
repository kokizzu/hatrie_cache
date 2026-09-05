package hatCache

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"hatrie_cache/hat/hatBackup"
)

type BackupBundleRestoreOptions = hatBackup.RestoreOptions
type BackupBundleRestoreReport = hatBackup.RestoreReport
type RestoreRehearsalOptions = hatBackup.RehearsalOptions

type RestoreRehearsalReport struct {
	OK                    bool                           `json:"ok"`
	Source                string                         `json:"source"`
	SourceKind            string                         `json:"source_kind"`
	WorkDir               string                         `json:"work_dir,omitempty"`
	WorkDirKept           bool                           `json:"work_dir_kept"`
	RestoredDir           string                         `json:"restored_dir"`
	RecoveredKeys         int                            `json:"recovered_keys"`
	JournalSequence       uint64                         `json:"journal_sequence,omitempty"`
	SourceStateChecksum   string                         `json:"source_state_checksum,omitempty"`
	RestoredStateChecksum string                         `json:"restored_state_checksum,omitempty"`
	StateChecksumsMatch   bool                           `json:"state_checksums_match"`
	Backup                BackupDoctorReport             `json:"backup"`
	Restored              BackupDoctorReport             `json:"restored"`
	Runtime               *RestoreRehearsalRuntimeReport `json:"runtime,omitempty"`
}

type RestoreRehearsalRuntimeReport struct {
	OK     bool                       `json:"ok"`
	Addr   string                     `json:"addr"`
	Health *MonitoringHealth          `json:"health,omitempty"`
	Stats  *CacheStats                `json:"stats,omitempty"`
	Gets   []RestoreRehearsalGetCheck `json:"gets,omitempty"`
}

type RestoreRehearsalGetCheck = hatBackup.RehearsalGetCheck

func RestoreBackupBundle(bundlePath string, dataDir string, options BackupBundleRestoreOptions) (BackupBundleRestoreReport, error) {
	bundlePath = strings.TrimSpace(bundlePath)
	dataDir = strings.TrimSpace(dataDir)
	if bundlePath == "" {
		return BackupBundleRestoreReport{}, errors.New("hatriecache: backup bundle path is required")
	}
	if dataDir == "" {
		return BackupBundleRestoreReport{}, errors.New("hatriecache: restore data dir is required")
	}
	if info, err := os.Stat(bundlePath); err == nil && info.IsDir() && fileExists(filepath.Join(bundlePath, backupRepositoryDescriptorPath)) {
		return RestoreBackupRepository(bundlePath, "", dataDir, options)
	} else if err != nil {
		return BackupBundleRestoreReport{}, err
	}
	manifest, err := readBackupBundleManifest(bundlePath)
	if err != nil {
		return BackupBundleRestoreReport{}, err
	}
	mode := backupBundleManifestMode(manifest)
	if mode != BackupModeSnapshot && mode != BackupModePebbleCheckpoint {
		return BackupBundleRestoreReport{}, fmt.Errorf("hatriecache: unsupported backup bundle restore mode %q", mode)
	}
	destination, err := prepareRestoreDestination(bundlePath, dataDir, options.Overwrite)
	if err != nil {
		return BackupBundleRestoreReport{}, err
	}
	defer destination.Cleanup()
	if err := extractBackupBundleFiles(bundlePath, destination.StagingPath(), manifest.Files); err != nil {
		return BackupBundleRestoreReport{}, err
	}
	var doctor BackupDoctorReport
	switch mode {
	case BackupModeSnapshot:
		doctor, err = verifySnapshotBackupRoot(bundlePath, "bundle", manifest, destination.StagingPath())
	case BackupModePebbleCheckpoint:
		doctor, err = verifyPebbleBackupRoot(bundlePath, "bundle", manifest, destination.StagingPath())
	}
	if err != nil {
		return BackupBundleRestoreReport{}, err
	}
	if err := syncRestoreTree(destination.StagingPath()); err != nil {
		return BackupBundleRestoreReport{}, err
	}
	if err := publishRestoreDestination(destination, options.Overwrite); err != nil {
		return BackupBundleRestoreReport{}, err
	}
	snapshotPath := ""
	if manifest.Snapshot != "" {
		snapshotPath = filepath.Join(dataDir, filepath.FromSlash(manifest.Snapshot))
	}
	storePath := ""
	if manifest.Store != "" {
		storePath = filepath.Join(dataDir, filepath.FromSlash(manifest.Store))
	}
	journalPath := ""
	if manifest.Journal != "" {
		journalPath = filepath.Join(dataDir, filepath.FromSlash(manifest.Journal))
	}
	return BackupBundleRestoreReport{
		OK:                  true,
		Bundle:              bundlePath,
		DataDir:             dataDir,
		Mode:                mode,
		Snapshot:            snapshotPath,
		Store:               storePath,
		StorageBackend:      manifest.StorageBackend,
		Journal:             journalPath,
		Partition:           cloneBackupPartitionMetadata(manifest.Partition),
		PartitionValidation: cloneBackupPartitionValidation(doctor.PartitionValidation),
		JournalSequence:     manifest.JournalSequence,
		RecoveredKeys:       doctor.RecoveredKeys,
	}, nil
}

func RestoreBackupRepository(repositoryPath string, backupID string, dataDir string, options BackupBundleRestoreOptions) (BackupBundleRestoreReport, error) {
	repositoryPath = strings.TrimSpace(repositoryPath)
	dataDir = strings.TrimSpace(dataDir)
	if repositoryPath == "" {
		return BackupBundleRestoreReport{}, errors.New("hatriecache: backup repository path is required")
	}
	if dataDir == "" {
		return BackupBundleRestoreReport{}, errors.New("hatriecache: restore data dir is required")
	}
	if err := verifyBackupRepositoryDescriptor(repositoryPath); err != nil {
		return BackupBundleRestoreReport{}, err
	}
	manifest, err := readBackupRepositoryManifest(repositoryPath, backupID)
	if err != nil {
		return BackupBundleRestoreReport{}, err
	}
	destination, err := prepareRestoreDestination(repositoryPath, dataDir, options.Overwrite)
	if err != nil {
		return BackupBundleRestoreReport{}, err
	}
	defer destination.Cleanup()
	if _, err := materializeBackupRepository(repositoryPath, manifest.BackupID, destination.StagingPath()); err != nil {
		return BackupBundleRestoreReport{}, err
	}
	doctor, err := verifyPebbleBackupRoot(repositoryPath, "repository", manifest, destination.StagingPath())
	if err != nil {
		return BackupBundleRestoreReport{}, err
	}
	if err := syncRestoreTree(destination.StagingPath()); err != nil {
		return BackupBundleRestoreReport{}, err
	}
	if err := publishRestoreDestination(destination, options.Overwrite); err != nil {
		return BackupBundleRestoreReport{}, err
	}
	storePath := filepath.Join(dataDir, filepath.FromSlash(manifest.Store))
	journalPath := ""
	if manifest.Journal != "" {
		journalPath = filepath.Join(dataDir, filepath.FromSlash(manifest.Journal))
	}
	return BackupBundleRestoreReport{
		OK:                  true,
		Bundle:              repositoryPath,
		DataDir:             dataDir,
		BackupID:            manifest.BackupID,
		Mode:                BackupModePebbleIncremental,
		Store:               storePath,
		StorageBackend:      manifest.StorageBackend,
		Journal:             journalPath,
		Partition:           cloneBackupPartitionMetadata(manifest.Partition),
		PartitionValidation: cloneBackupPartitionValidation(doctor.PartitionValidation),
		JournalSequence:     manifest.JournalSequence,
		RecoveredKeys:       doctor.RecoveredKeys,
	}, nil
}

// StageBackupRepository checksum-verifies and durably materializes one
// repository manifest without opening its Pebble store. Recovery callers can
// use the subsequent eager load as the single semantic validation pass.
func StageBackupRepository(repositoryPath string, backupID string, dataDir string) (BackupBundleManifest, error) {
	repositoryPath = strings.TrimSpace(repositoryPath)
	dataDir = strings.TrimSpace(dataDir)
	if repositoryPath == "" {
		return BackupBundleManifest{}, errors.New("hatriecache: backup repository path is required")
	}
	if dataDir == "" {
		return BackupBundleManifest{}, errors.New("hatriecache: restore data dir is required")
	}
	if err := verifyBackupRepositoryDescriptor(repositoryPath); err != nil {
		return BackupBundleManifest{}, err
	}
	manifest, err := readBackupRepositoryManifest(repositoryPath, backupID)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	destination, err := prepareRestoreDestination(repositoryPath, dataDir, false)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	defer destination.Cleanup()
	if _, err := materializeBackupRepository(repositoryPath, manifest.BackupID, destination.StagingPath()); err != nil {
		return BackupBundleManifest{}, err
	}
	if err := syncRestoreTree(destination.StagingPath()); err != nil {
		return BackupBundleManifest{}, err
	}
	if err := publishRestoreDestination(destination, false); err != nil {
		return BackupBundleManifest{}, err
	}
	return manifest, nil
}

// StagePebbleCheckpointBundle checksum-verifies and durably materializes a
// native checkpoint without loading its records. Callers must open and load the
// installed store before exposing it to traffic.
func StagePebbleCheckpointBundle(bundlePath string, dataDir string) (BackupBundleManifest, error) {
	bundlePath = strings.TrimSpace(bundlePath)
	dataDir = strings.TrimSpace(dataDir)
	if bundlePath == "" {
		return BackupBundleManifest{}, errors.New("hatriecache: backup bundle path is required")
	}
	if dataDir == "" {
		return BackupBundleManifest{}, errors.New("hatriecache: restore data dir is required")
	}
	manifest, err := readBackupBundleManifest(bundlePath)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	if backupBundleManifestMode(manifest) != BackupModePebbleCheckpoint || manifest.Store != backupBundleStorePath || manifest.StorageBackend != string(StorageBackendPebble) {
		return BackupBundleManifest{}, errors.New("hatriecache: bundle is not a Pebble checkpoint")
	}
	destination, err := prepareRestoreDestination(bundlePath, dataDir, false)
	if err != nil {
		return BackupBundleManifest{}, err
	}
	defer destination.Cleanup()
	if err := extractBackupBundleFiles(bundlePath, destination.StagingPath(), manifest.Files); err != nil {
		return BackupBundleManifest{}, err
	}
	if err := syncRestoreTree(destination.StagingPath()); err != nil {
		return BackupBundleManifest{}, err
	}
	if err := publishRestoreDestination(destination, false); err != nil {
		return BackupBundleManifest{}, err
	}
	return manifest, nil
}

type restoreDestination = hatBackup.RestoreDestination

func prepareRestoreDestination(source string, dataDir string, overwrite bool) (restoreDestination, error) {
	return hatBackup.PrepareRestoreDestination(source, dataDir, overwrite)
}

func rejectRestoreSymlinkComponents(path string) error {
	return hatBackup.RejectRestoreSymlinkComponents(path)
}

func validateRestorePathSeparation(source string, target string) error {
	return hatBackup.ValidateRestorePathSeparation(source, target)
}

func validateRestoreTarget(target string, overwrite bool) (bool, error) {
	return hatBackup.ValidateRestoreTarget(target, overwrite)
}

func publishRestoreDestination(destination restoreDestination, overwrite bool) error {
	return destination.Publish(overwrite)
}

func syncRestoreTree(root string) error {
	return hatBackup.SyncRestoreTree(root)
}

func RehearseRestore(path string, options RestoreRehearsalOptions) (RestoreRehearsalReport, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return RestoreRehearsalReport{}, errors.New("hatriecache: restore rehearsal path is required")
	}
	backup, err := VerifyBackupPath(path)
	if err != nil {
		return RestoreRehearsalReport{}, err
	}

	workDir := strings.TrimSpace(options.WorkDir)
	workDirKept := options.KeepWorkDir || workDir != ""
	if workDir == "" {
		workDir, err = os.MkdirTemp("", "hatrie-cache-restore-rehearsal-*")
		if err != nil {
			return RestoreRehearsalReport{}, err
		}
	} else if err := os.MkdirAll(workDir, 0o700); err != nil {
		return RestoreRehearsalReport{}, err
	}
	if !workDirKept {
		defer os.RemoveAll(workDir)
	}

	restoredDir := filepath.Join(workDir, "data")
	switch backup.Kind {
	case "bundle", "repository":
		if _, err := RestoreBackupBundle(path, restoredDir, BackupBundleRestoreOptions{}); err != nil {
			return RestoreRehearsalReport{}, err
		}
	case "directory":
		if err := ensureRestoreDataDir(restoredDir, false); err != nil {
			return RestoreRehearsalReport{}, err
		}
		if err := copyBackupDirectory(path, restoredDir); err != nil {
			return RestoreRehearsalReport{}, err
		}
	default:
		return RestoreRehearsalReport{}, fmt.Errorf("hatriecache: unsupported backup kind %q", backup.Kind)
	}

	restored, err := VerifyBackupDirectory(restoredDir)
	if err != nil {
		return RestoreRehearsalReport{}, err
	}
	if backup.StateChecksum == "" || restored.StateChecksum == "" {
		return RestoreRehearsalReport{}, errors.New("hatriecache: restore rehearsal state checksum missing")
	}
	if backup.StateChecksum != restored.StateChecksum {
		return RestoreRehearsalReport{}, fmt.Errorf("hatriecache: restore rehearsal state checksum mismatch: source=%s restored=%s", backup.StateChecksum, restored.StateChecksum)
	}
	return RestoreRehearsalReport{
		OK:                    true,
		Source:                path,
		SourceKind:            backup.Kind,
		WorkDir:               workDir,
		WorkDirKept:           workDirKept,
		RestoredDir:           restoredDir,
		RecoveredKeys:         restored.RecoveredKeys,
		JournalSequence:       restored.JournalSequence,
		SourceStateChecksum:   backup.StateChecksum,
		RestoredStateChecksum: restored.StateChecksum,
		StateChecksumsMatch:   true,
		Backup:                backup,
		Restored:              restored,
	}, nil
}

func ensureRestoreDataDir(path string, overwrite bool) error {
	return hatBackup.EnsureRestoreDataDir(path, overwrite)
}

func copyBackupDirectory(source string, destination string) error {
	return hatBackup.CopyBackupDirectory(source, destination)
}

func sameOrChildPath(parent string, child string) bool {
	return hatBackup.SameOrChildPath(parent, child)
}
