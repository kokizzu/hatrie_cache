package hatriecache

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type BackupBundleRestoreOptions struct {
	Overwrite bool
}

type BackupBundleRestoreReport struct {
	OK                  bool                       `json:"ok"`
	Bundle              string                     `json:"bundle"`
	DataDir             string                     `json:"data_dir"`
	BackupID            string                     `json:"backup_id,omitempty"`
	Mode                BackupMode                 `json:"mode,omitempty"`
	Snapshot            string                     `json:"snapshot"`
	Store               string                     `json:"store,omitempty"`
	StorageBackend      string                     `json:"storage_backend,omitempty"`
	Journal             string                     `json:"journal,omitempty"`
	Partition           *BackupPartitionMetadata   `json:"partition,omitempty"`
	PartitionValidation *BackupPartitionValidation `json:"partition_validation,omitempty"`
	JournalSequence     uint64                     `json:"journal_sequence"`
	RecoveredKeys       int                        `json:"recovered_keys"`
}

type RestoreRehearsalOptions struct {
	WorkDir     string
	KeepWorkDir bool
}

type RestoreRehearsalReport struct {
	OK              bool                           `json:"ok"`
	Source          string                         `json:"source"`
	SourceKind      string                         `json:"source_kind"`
	WorkDir         string                         `json:"work_dir,omitempty"`
	WorkDirKept     bool                           `json:"work_dir_kept"`
	RestoredDir     string                         `json:"restored_dir"`
	RecoveredKeys   int                            `json:"recovered_keys"`
	JournalSequence uint64                         `json:"journal_sequence,omitempty"`
	Backup          BackupDoctorReport             `json:"backup"`
	Restored        BackupDoctorReport             `json:"restored"`
	Runtime         *RestoreRehearsalRuntimeReport `json:"runtime,omitempty"`
}

type RestoreRehearsalRuntimeReport struct {
	OK     bool                       `json:"ok"`
	Addr   string                     `json:"addr"`
	Health *MonitoringHealth          `json:"health,omitempty"`
	Stats  *CacheStats                `json:"stats,omitempty"`
	Gets   []RestoreRehearsalGetCheck `json:"gets,omitempty"`
}

type RestoreRehearsalGetCheck struct {
	Key      string  `json:"key"`
	OK       bool    `json:"ok"`
	Value    string  `json:"value,omitempty"`
	Expected *string `json:"expected,omitempty"`
	Error    string  `json:"error,omitempty"`
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
	defer os.RemoveAll(destination.staging)
	if err := extractBackupBundleFiles(bundlePath, destination.staging, manifest.Files); err != nil {
		return BackupBundleRestoreReport{}, err
	}
	var doctor BackupDoctorReport
	switch mode {
	case BackupModeSnapshot:
		doctor, err = verifySnapshotBackupRoot(bundlePath, "bundle", manifest, destination.staging)
	case BackupModePebbleCheckpoint:
		doctor, err = verifyPebbleBackupRoot(bundlePath, "bundle", manifest, destination.staging)
	}
	if err != nil {
		return BackupBundleRestoreReport{}, err
	}
	if err := syncRestoreTree(destination.staging); err != nil {
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
	defer os.RemoveAll(destination.staging)
	if _, err := materializeBackupRepository(repositoryPath, manifest.BackupID, destination.staging); err != nil {
		return BackupBundleRestoreReport{}, err
	}
	doctor, err := verifyPebbleBackupRoot(repositoryPath, "repository", manifest, destination.staging)
	if err != nil {
		return BackupBundleRestoreReport{}, err
	}
	if err := syncRestoreTree(destination.staging); err != nil {
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
	defer os.RemoveAll(destination.staging)
	if _, err := materializeBackupRepository(repositoryPath, manifest.BackupID, destination.staging); err != nil {
		return BackupBundleManifest{}, err
	}
	if err := syncRestoreTree(destination.staging); err != nil {
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
	defer os.RemoveAll(destination.staging)
	if err := extractBackupBundleFiles(bundlePath, destination.staging, manifest.Files); err != nil {
		return BackupBundleManifest{}, err
	}
	if err := syncRestoreTree(destination.staging); err != nil {
		return BackupBundleManifest{}, err
	}
	if err := publishRestoreDestination(destination, false); err != nil {
		return BackupBundleManifest{}, err
	}
	return manifest, nil
}

type restoreDestination struct {
	target  string
	staging string
}

func prepareRestoreDestination(source string, dataDir string, overwrite bool) (restoreDestination, error) {
	target, err := filepath.Abs(dataDir)
	if err != nil {
		return restoreDestination{}, err
	}
	target = filepath.Clean(target)
	parent := filepath.Dir(target)
	if target == parent {
		return restoreDestination{}, errors.New("hatriecache: restore data dir must not be a filesystem root")
	}
	if err := validateRestorePathSeparation(source, target); err != nil {
		return restoreDestination{}, err
	}
	if err := rejectRestoreSymlinkComponents(parent); err != nil {
		return restoreDestination{}, err
	}
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return restoreDestination{}, err
	}
	if err := rejectRestoreSymlinkComponents(parent); err != nil {
		return restoreDestination{}, err
	}
	if _, err := validateRestoreTarget(target, overwrite); err != nil {
		return restoreDestination{}, err
	}
	staging, err := os.MkdirTemp(parent, "."+filepath.Base(target)+".restore-stage-*")
	if err != nil {
		return restoreDestination{}, err
	}
	return restoreDestination{target: target, staging: staging}, nil
}

func rejectRestoreSymlinkComponents(path string) error {
	volume := filepath.VolumeName(path)
	root := volume + string(os.PathSeparator)
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return err
	}
	current := root
	for _, component := range strings.Split(relative, string(os.PathSeparator)) {
		if component == "" || component == "." {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("hatriecache: restore data dir path contains symlink: %s", current)
		}
	}
	return nil
}

func validateRestorePathSeparation(source string, target string) error {
	sourceAbs, err := filepath.Abs(source)
	if err != nil {
		return err
	}
	sourceAbs = filepath.Clean(sourceAbs)
	if evaluated, err := filepath.EvalSymlinks(sourceAbs); err == nil {
		sourceAbs = filepath.Clean(evaluated)
	}
	if sameOrChildPath(target, sourceAbs) || sameOrChildPath(sourceAbs, target) {
		return fmt.Errorf("hatriecache: restore source and data dir must not overlap: %s", target)
	}
	return nil
}

func validateRestoreTarget(target string, overwrite bool) (bool, error) {
	info, err := os.Lstat(target)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return false, fmt.Errorf("hatriecache: restore data dir must not be a symlink: %s", target)
	}
	if !info.IsDir() {
		return false, fmt.Errorf("hatriecache: restore data dir is not a directory: %s", target)
	}
	entries, err := os.ReadDir(target)
	if err != nil {
		return false, err
	}
	if len(entries) > 0 && !overwrite {
		return false, fmt.Errorf("hatriecache: restore data dir is not empty: %s", target)
	}
	return true, nil
}

func publishRestoreDestination(destination restoreDestination, overwrite bool) error {
	exists, err := validateRestoreTarget(destination.target, overwrite)
	if err != nil {
		return err
	}
	parent := filepath.Dir(destination.target)
	if !exists {
		if err := os.Rename(destination.staging, destination.target); err != nil {
			return err
		}
		if err := syncDirectory(parent); err != nil {
			rollbackErr := os.Rename(destination.target, destination.staging)
			if rollbackErr != nil {
				return fmt.Errorf("hatriecache: sync published restore: %w; rollback failed: %v", err, rollbackErr)
			}
			return err
		}
		return nil
	}
	oldPath, err := os.MkdirTemp(parent, "."+filepath.Base(destination.target)+".restore-old-*")
	if err != nil {
		return err
	}
	if err := os.Remove(oldPath); err != nil {
		return err
	}
	if err := os.Rename(destination.target, oldPath); err != nil {
		return err
	}
	if err := os.Rename(destination.staging, destination.target); err != nil {
		rollbackErr := os.Rename(oldPath, destination.target)
		if rollbackErr != nil {
			return fmt.Errorf("hatriecache: publish restore: %w; rollback failed: %v", err, rollbackErr)
		}
		return err
	}
	if err := syncDirectory(parent); err != nil {
		rollbackErr := rollbackPublishedRestore(destination, oldPath)
		if rollbackErr != nil {
			return fmt.Errorf("hatriecache: sync published restore: %w; rollback failed: %v", err, rollbackErr)
		}
		return err
	}
	if err := os.RemoveAll(oldPath); err != nil {
		return err
	}
	return syncDirectory(parent)
}

func rollbackPublishedRestore(destination restoreDestination, oldPath string) error {
	if err := os.Rename(destination.target, destination.staging); err != nil {
		return err
	}
	if err := os.Rename(oldPath, destination.target); err != nil {
		restoreErr := os.Rename(destination.staging, destination.target)
		if restoreErr != nil {
			return fmt.Errorf("restore old directory: %w; restoring new directory also failed: %v", err, restoreErr)
		}
		return err
	}
	return syncDirectory(filepath.Dir(destination.target))
}

func syncRestoreTree(root string) error {
	directories := make([]string, 0, 8)
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("hatriecache: restore staging contains symlink: %s", path)
		}
		if entry.IsDir() {
			directories = append(directories, path)
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("hatriecache: restore staging contains non-regular file: %s", path)
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		syncErr := file.Sync()
		closeErr := file.Close()
		if syncErr != nil {
			return syncErr
		}
		return closeErr
	})
	if err != nil {
		return err
	}
	sort.Slice(directories, func(left int, right int) bool {
		return strings.Count(directories[left], string(os.PathSeparator)) > strings.Count(directories[right], string(os.PathSeparator))
	})
	for _, directory := range directories {
		if err := syncDirectory(directory); err != nil {
			return err
		}
	}
	return nil
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
	return RestoreRehearsalReport{
		OK:              true,
		Source:          path,
		SourceKind:      backup.Kind,
		WorkDir:         workDir,
		WorkDirKept:     workDirKept,
		RestoredDir:     restoredDir,
		RecoveredKeys:   restored.RecoveredKeys,
		JournalSequence: restored.JournalSequence,
		Backup:          backup,
		Restored:        restored,
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
	if overwrite {
		for _, entry := range entries {
			if err := os.RemoveAll(filepath.Join(path, entry.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

func copyBackupDirectory(source string, destination string) error {
	sourceAbs, err := filepath.Abs(source)
	if err != nil {
		return err
	}
	destinationAbs, err := filepath.Abs(destination)
	if err != nil {
		return err
	}
	if sameOrChildPath(sourceAbs, destinationAbs) {
		return fmt.Errorf("hatriecache: restore rehearsal work dir must not be inside backup directory: %s", destination)
	}
	if sameOrChildPath(destinationAbs, sourceAbs) {
		return fmt.Errorf("hatriecache: restore rehearsal work dir must not contain backup directory: %s", destination)
	}
	return copyDirectoryContents(sourceAbs, destinationAbs)
}

func sameOrChildPath(parent string, child string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)))
}

func copyDirectoryContents(source string, destination string) error {
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		sourcePath := filepath.Join(source, entry.Name())
		destinationPath := filepath.Join(destination, entry.Name())
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("hatriecache: restore rehearsal refuses symlink in backup directory: %s", sourcePath)
		}
		if info.IsDir() {
			if err := os.MkdirAll(destinationPath, info.Mode().Perm()); err != nil {
				return err
			}
			if err := copyDirectoryContents(sourcePath, destinationPath); err != nil {
				return err
			}
			continue
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("hatriecache: restore rehearsal refuses non-regular backup file: %s", sourcePath)
		}
		if err := copyRegularFile(sourcePath, destinationPath, info.Mode().Perm()); err != nil {
			return err
		}
	}
	return nil
}

func copyRegularFile(source string, destination string, mode os.FileMode) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}
