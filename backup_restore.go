package hatriecache

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	if backupBundleManifestMode(manifest) == BackupModePebbleCheckpoint {
		return restorePebbleCheckpointBundle(bundlePath, dataDir, options, manifest)
	}
	doctor, err := VerifyBackupBundle(bundlePath)
	if err != nil {
		return BackupBundleRestoreReport{}, err
	}
	if err := ensureRestoreDataDir(dataDir, options.Overwrite); err != nil {
		return BackupBundleRestoreReport{}, err
	}
	if err := extractBackupBundleFiles(bundlePath, dataDir, manifest.Files); err != nil {
		return BackupBundleRestoreReport{}, err
	}
	snapshotPath := filepath.Join(dataDir, filepath.FromSlash(manifest.Snapshot))
	journalPath := ""
	if manifest.Journal != "" {
		journalPath = filepath.Join(dataDir, filepath.FromSlash(manifest.Journal))
	}
	return BackupBundleRestoreReport{
		OK:                  true,
		Bundle:              bundlePath,
		DataDir:             dataDir,
		Mode:                backupBundleManifestMode(manifest),
		Snapshot:            snapshotPath,
		Journal:             journalPath,
		Partition:           cloneBackupPartitionMetadata(manifest.Partition),
		PartitionValidation: cloneBackupPartitionValidation(doctor.PartitionValidation),
		JournalSequence:     manifest.JournalSequence,
		RecoveredKeys:       doctor.RecoveredKeys,
	}, nil
}

func RestoreBackupRepository(repositoryPath string, backupID string, dataDir string, options BackupBundleRestoreOptions) (BackupBundleRestoreReport, error) {
	doctor, err := VerifyBackupRepository(repositoryPath, backupID)
	if err != nil {
		return BackupBundleRestoreReport{}, err
	}
	manifest, err := readBackupRepositoryManifest(repositoryPath, doctor.BackupID)
	if err != nil {
		return BackupBundleRestoreReport{}, err
	}
	if err := ensureRestoreDataDir(dataDir, options.Overwrite); err != nil {
		return BackupBundleRestoreReport{}, err
	}
	if _, err := materializeBackupRepository(repositoryPath, manifest.BackupID, dataDir); err != nil {
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

func restorePebbleCheckpointBundle(bundlePath string, dataDir string, options BackupBundleRestoreOptions, manifest BackupBundleManifest) (BackupBundleRestoreReport, error) {
	doctor, err := VerifyBackupBundle(bundlePath)
	if err != nil {
		return BackupBundleRestoreReport{}, err
	}
	if err := ensureRestoreDataDir(dataDir, options.Overwrite); err != nil {
		return BackupBundleRestoreReport{}, err
	}
	if err := extractBackupBundleFiles(bundlePath, dataDir, manifest.Files); err != nil {
		return BackupBundleRestoreReport{}, err
	}
	storePath := filepath.Join(dataDir, filepath.FromSlash(manifest.Store))
	journalPath := ""
	if manifest.Journal != "" {
		journalPath = filepath.Join(dataDir, filepath.FromSlash(manifest.Journal))
	}
	return BackupBundleRestoreReport{
		OK:                  true,
		Bundle:              bundlePath,
		DataDir:             dataDir,
		Mode:                BackupModePebbleCheckpoint,
		Store:               storePath,
		StorageBackend:      manifest.StorageBackend,
		Journal:             journalPath,
		Partition:           cloneBackupPartitionMetadata(manifest.Partition),
		PartitionValidation: cloneBackupPartitionValidation(doctor.PartitionValidation),
		JournalSequence:     manifest.JournalSequence,
		RecoveredKeys:       doctor.RecoveredKeys,
	}, nil
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
	case "bundle":
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
