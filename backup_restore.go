package hatriecache

import (
	"errors"
	"fmt"
	"io"
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

type RestoreRehearsalOptions struct {
	WorkDir     string
	KeepWorkDir bool
}

type RestoreRehearsalReport struct {
	OK              bool               `json:"ok"`
	Source          string             `json:"source"`
	SourceKind      string             `json:"source_kind"`
	WorkDir         string             `json:"work_dir,omitempty"`
	WorkDirKept     bool               `json:"work_dir_kept"`
	RestoredDir     string             `json:"restored_dir"`
	RecoveredKeys   int                `json:"recovered_keys"`
	JournalSequence uint64             `json:"journal_sequence,omitempty"`
	Backup          BackupDoctorReport `json:"backup"`
	Restored        BackupDoctorReport `json:"restored"`
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
